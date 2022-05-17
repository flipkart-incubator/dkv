package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path"
	"strings"
	"syscall"

	"github.com/flipkart-incubator/dkv/internal/discovery"
	"github.com/flipkart-incubator/dkv/internal/master"
	"github.com/flipkart-incubator/dkv/internal/opts"
	"github.com/flipkart-incubator/dkv/internal/slave"
	"github.com/flipkart-incubator/dkv/internal/stats"
	"github.com/flipkart-incubator/dkv/internal/stats/aggregate"
	"github.com/flipkart-incubator/dkv/internal/storage"
	"github.com/flipkart-incubator/dkv/internal/storage/badger"
	"github.com/flipkart-incubator/dkv/internal/storage/rocksdb"
	"github.com/flipkart-incubator/dkv/internal/sync"
	"github.com/flipkart-incubator/dkv/pkg/health"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
	nexus_api "github.com/flipkart-incubator/nexus/pkg/api"
	nexus "github.com/flipkart-incubator/nexus/pkg/raft"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	flag "github.com/spf13/pflag"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"net/http/pprof"
)

type dkvSrvrRole string

const (
	noRole        dkvSrvrRole = "none"
	masterRole    dkvSrvrRole = "master"
	slaveRole     dkvSrvrRole = "slave"
	discoveryRole dkvSrvrRole = "discovery"

	defBlockCacheSize     = 3 << 30
	discoveryServerConfig = "serverConfig"
	discoveryClientConfig = "clientConfig"
)

var (
	//Config file used for reading and using configs
	cfgFile string

	//appConfig
	config opts.Config

	//nexus flags
	nexusLogDirFlag, nexusSnapDirFlag *flag.Flag

	// Logging vars
	verboseLogging bool
	accessLogger   *zap.Logger
	dkvLogger      *zap.Logger

	// Other vars
	pprofEnable   bool
	statsCli      stats.Client
	promRegistry  prometheus.Registerer
	statsStreamer *stats.StatStreamer

	discoveryClient        discovery.Client
	statAggregatorRegistry *aggregate.StatAggregatorRegistry
)

func init() {
	initializeFlags()
}

func initializeFlags() {
	flag.CommandLine.ParseErrorsWhitelist = flag.ParseErrorsWhitelist{UnknownFlags: true}
	flag.StringVarP(&cfgFile, "config", "c", "", "config file (default is /etc/default/dkvsrv.yaml)")
	flag.BoolVarP(&verboseLogging, "verbose", "v", false, fmt.Sprintf("Enable verbose logging.\nBy default, only warnings and errors are logged. (default %v)", verboseLogging))
	flag.BoolVarP(&pprofEnable, "pprof", "p", false, "Enable pprof profiling")
}

func main() {

	//load config
	flag.Parse()
	config.Init(cfgFile)
	config.Print()

	setupDKVLogger()
	setupAccessLogger()
	setFlagsForNexusDirs()
	setupStats()
	go setupHttpServer()

	kvs, cp, ca, br := newKVStore()
	grpcSrvr, lstnr := newGrpcServerListener()
	defer grpcSrvr.GracefulStop()
	srvrRole := toDKVSrvrRole(config.DbRole)
	//srvrRole.printFlags()

	// Create the region info which is passed to DKVServer
	nodeAddr, err := nodeAddress(config.ListenAddr)
	if err != nil {
		log.Panicf("Failed to parse GRPC Listen Address %v.", err)
	}

	//HTTP listen Address
	nodeHTTPAddr, err := nodeAddress(config.HttpListenAddr)
	if err != nil {
		log.Panicf("Failed to parse HTTP Listen Address %v.", err)
	}

	regionInfo := &serverpb.RegionInfo{
		DcID:            config.DcID,
		NodeAddress:     nodeAddr.Host,
		HttpAddress:     nodeHTTPAddr.Host,
		Database:        config.Database,
		VBucket:         config.VBucket,
		Status:          serverpb.RegionStatus_INACTIVE,
		MasterHost:      nil,
		NexusClusterUrl: nil,
	}

	serveropts := &opts.ServerOpts{
		Logger:                    dkvLogger,
		HealthCheckTickerInterval: opts.DefaultHealthCheckTickterInterval, //to be exposed later via app.conf
		StatsCli:                  statsCli,
		PrometheusRegistry:        promRegistry,
	}

	if srvrRole != noRole && srvrRole != discoveryRole {
		var err error
		discoveryClient, err = newDiscoveryClient()
		if err != nil {
			log.Panicf("Failed to start Discovery Client %v.", err)
		}
		// Currently statusPropagator and clusterInfoGetter are same instances hence closing just one
		// but ideally this information should be abstracted from main and we should call close on both
		defer discoveryClient.Close()
	}

	switch srvrRole {
	case noRole:
		dkvSvc := master.NewStandaloneService(kvs, nil, br, regionInfo, serveropts)
		defer dkvSvc.Close()
		serverpb.RegisterDKVServer(grpcSrvr, dkvSvc)
		serverpb.RegisterDKVBackupRestoreServer(grpcSrvr, dkvSvc)
		health.RegisterHealthServer(grpcSrvr, dkvSvc)
	case masterRole, discoveryRole:
		if cp == nil {
			log.Panicf("Storage engine %s is not supported for DKV master role.", config.DbEngine)
		}
		var dkvSvc master.DKVService
		if haveFlagsWithPrefix("nexus") {
			dkvSvc = master.NewDistributedService(kvs, cp, br, newDKVReplicator(kvs), regionInfo, serveropts)
			serverpb.RegisterDKVClusterServer(grpcSrvr, dkvSvc.(master.DKVClusterService))
		} else {
			dkvSvc = master.NewStandaloneService(kvs, cp, br, regionInfo, serveropts)
			serverpb.RegisterDKVBackupRestoreServer(grpcSrvr, dkvSvc)
		}
		defer dkvSvc.Close()
		serverpb.RegisterDKVServer(grpcSrvr, dkvSvc)
		serverpb.RegisterDKVReplicationServer(grpcSrvr, dkvSvc)
		health.RegisterHealthServer(grpcSrvr, dkvSvc)

		// Discovery servers can be only configured if node started as master.
		if srvrRole == discoveryRole {
			err := registerDiscoveryServer(grpcSrvr, dkvSvc)
			if err != nil {
				log.Panicf("Failed to start Discovery Service %v.", err)
			}
		} else {
			// Currently nodes can be either discovery server or client. This will change when a node supports multiple regions
			discoveryClient.RegisterRegion(dkvSvc)
		}
	case slaveRole:
		// TODO - construct replConfig from region level config described in LLD
		maxNumChanges := uint32(10000)
		replConfig := &slave.ReplicationConfig{
			MaxNumChngs:          maxNumChanges,
			ReplPollInterval:     config.ReplPollInterval,
			MaxActiveReplLag:     uint64(maxNumChanges * 10),
			MaxActiveReplElapsed: uint64(config.ReplPollInterval.Seconds()) * 10,
			ReplMasterAddr:       config.ReplicationMasterAddr,
		}
		dkvSvc, _ := slave.NewService(kvs, ca, regionInfo, replConfig, discoveryClient, serveropts)
		defer dkvSvc.Close()
		serverpb.RegisterDKVServer(grpcSrvr, dkvSvc)
		health.RegisterHealthServer(grpcSrvr, dkvSvc)
		discoveryClient.RegisterRegion(dkvSvc)
	default:
		panic("Invalid 'dbRole'. Allowed values are none|master|slave|discovery.")
	}
	go grpcSrvr.Serve(lstnr)
	sig := <-setupSignalHandler()
	log.Printf("[WARN] Caught signal: %v. Shutting down...\n", sig)
}

func setupAccessLogger() {
	accessLogger = zap.NewNop()
	if config.AccessLog != "" {
		accessLoggerConfig := zap.Config{
			Level:         zap.NewAtomicLevelAt(zap.InfoLevel),
			Development:   false,
			Encoding:      "console",
			DisableCaller: true,

			EncoderConfig: zapcore.EncoderConfig{
				TimeKey:        "ts",
				LevelKey:       "level",
				NameKey:        "logger",
				CallerKey:      "caller",
				MessageKey:     "msg",
				StacktraceKey:  "stacktrace",
				LineEnding:     zapcore.DefaultLineEnding,
				EncodeLevel:    zapcore.LowercaseLevelEncoder,
				EncodeTime:     zapcore.ISO8601TimeEncoder,
				EncodeDuration: zapcore.StringDurationEncoder,
				EncodeCaller:   zapcore.ShortCallerEncoder,
			},

			OutputPaths:      []string{config.AccessLog},
			ErrorOutputPaths: []string{config.AccessLog},
		}
		if lg, err := accessLoggerConfig.Build(); err != nil {
			log.Printf("[WARN] Unable to configure access logger. Error: %v\n", err)
		} else {
			accessLogger = lg
		}
	}
}

func setupDKVLogger() {
	dkvLoggerConfig := zap.Config{
		Development:   false,
		Encoding:      "console",
		DisableCaller: true,

		EncoderConfig: zapcore.EncoderConfig{
			TimeKey:        "ts",
			LevelKey:       "level",
			NameKey:        "logger",
			CallerKey:      "caller",
			MessageKey:     "msg",
			LineEnding:     zapcore.DefaultLineEnding,
			EncodeLevel:    zapcore.LowercaseLevelEncoder,
			EncodeTime:     zapcore.ISO8601TimeEncoder,
			EncodeDuration: zapcore.StringDurationEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder,
		},
		OutputPaths:      []string{"stderr"},
		ErrorOutputPaths: []string{"stderr"},
	}

	if verboseLogging {
		dkvLoggerConfig.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
		dkvLoggerConfig.EncoderConfig.StacktraceKey = "stacktrace"
	} else {
		logLevel := zap.WarnLevel
		if config.LogLevel != "" {
			logLevel.Set(config.LogLevel)
		}
		dkvLoggerConfig.Level = zap.NewAtomicLevelAt(logLevel)
	}

	if lg, err := dkvLoggerConfig.Build(); err != nil {
		log.Printf("[WARN] Unable to configure DKV logger. Error: %v\n", err)
		dkvLogger = zap.NewNop()
	} else {
		dkvLogger = lg
	}
}

func newGrpcServerListener() (*grpc.Server, net.Listener) {
	grpcSrvr := grpc.NewServer(
		grpc.StreamInterceptor(grpc_zap.StreamServerInterceptor(accessLogger)),
		grpc.UnaryInterceptor(grpc_zap.UnaryServerInterceptor(accessLogger)),
	)
	reflection.Register(grpcSrvr)
	return grpcSrvr, newListener()
}

func newListener() (lis net.Listener) {
	var err error
	if lis, err = net.Listen("tcp", config.ListenAddr); err != nil {
		log.Panicf("failed to listen: %v", err)
		return
	}
	return
}

func setupSignalHandler() <-chan os.Signal {
	signals := []os.Signal{syscall.SIGINT, syscall.SIGQUIT, syscall.SIGSTOP, syscall.SIGTERM}
	stopChan := make(chan os.Signal, len(signals))
	signal.Notify(stopChan, signals...)
	return stopChan
}

func haveFlagsWithPrefix(prefix string) bool {
	res := false
	flag.Visit(func(f *flag.Flag) {
		if strings.HasPrefix(f.Name, prefix) {
			res = true
		}
	})
	return res
}

func toDKVSrvrRole(role string) dkvSrvrRole {
	return dkvSrvrRole(strings.TrimSpace(strings.ToLower(role)))
}

func setFlagsForNexusDirs() {

	nexusLogDirFlag, nexusSnapDirFlag = flag.Lookup("nexus-log-dir"), flag.Lookup("nexus-snap-dir")
	if nexusLogDirFlag.Value.String() == "" {
		nexusLogDirFlag.Value.Set(path.Join(config.DbFolder, "logs"))
	}
	if nexusSnapDirFlag.Value.String() == "" {
		nexusSnapDirFlag.Value.Set(path.Join(config.DbFolder, "snap"))
	}
}

func setupStats() {
	if config.StatsdAddr != "" {
		statsCli = stats.NewStatsDClient(config.StatsdAddr, "dkv.")
	} else {
		statsCli = stats.NewNoOpClient()
	}
	promRegistry = stats.NewPromethousRegistry()
	statsStreamer = stats.NewStatStreamer()
	statAggregatorRegistry = aggregate.NewStatAggregatorRegistry()
	go statsStreamer.Run()
}

func newKVStore() (storage.KVStore, storage.ChangePropagator, storage.ChangeApplier, storage.Backupable) {
	slg := dkvLogger.Sugar()
	defer slg.Sync()

	if err := os.MkdirAll(config.DbFolder, 0777); err != nil {
		slg.Fatalf("Unable to create DB folder at %s. Error: %v.", config.DbFolder, err)
	}

	dataDir := path.Join(config.DbFolder, "data")
	slg.Infof("Using %s as data directory", dataDir)

	sstDir := path.Join(config.DbFolder, "sst")
	if err := os.MkdirAll(sstDir, 0777); err != nil {
		slg.Fatalf("Unable to create sst folder at %s. Error: %v.", config.DbFolder, err)
	}

	switch config.DbEngine {
	case "rocksdb":
		rocksDb, err := rocksdb.OpenDB(dataDir,
			rocksdb.WithSSTDir(sstDir),
			rocksdb.WithSyncWrites(),
			rocksdb.WithCacheSize(config.BlockCacheSize),
			rocksdb.WithRocksDBConfig(config.DbEngineIni),
			rocksdb.WithLogger(dkvLogger),
			rocksdb.WithStats(statsCli),
			rocksdb.WithPromStats(promRegistry))
		if err != nil {
			dkvLogger.Panic("RocksDB engine init failed", zap.Error(err))
		}
		return rocksDb, rocksDb, rocksDb, rocksDb
	case "badger":
		var badgerDb badger.DB
		var err error
		bdbOpts := []badger.DBOption{
			badger.WithSSTDir(sstDir),
			badger.WithSyncWrites(),
			badger.WithCacheSize(config.BlockCacheSize),
			badger.WithBadgerConfig(config.DbEngineIni),
			badger.WithLogger(dkvLogger),
			badger.WithStats(statsCli),
			badger.WithPromStats(promRegistry),
		}
		if config.DisklessMode {
			bdbOpts = append(bdbOpts, badger.WithInMemory())
		} else {
			bdbOpts = append(bdbOpts, badger.WithDBDir(dataDir))
		}
		badgerDb, err = badger.OpenDB(bdbOpts...)
		if err != nil {
			dkvLogger.Panic("Badger engine init failed", zap.Error(err))
		}
		return badgerDb, badgerDb, badgerDb, badgerDb
	default:
		slg.Panicf("Unknown storage engine: %s", config.DbEngine)
		return nil, nil, nil, nil
	}
}

func mkdirNexusDirs() {
	if err := os.MkdirAll(nexusLogDirFlag.Value.String(), 0777); err != nil {
		log.Panicf("Unable to create Nexus logDir. Error: %v", err)
	}
	if err := os.MkdirAll(nexusSnapDirFlag.Value.String(), 0777); err != nil {
		log.Panicf("Unable to create Nexus snapDir. Error: %v", err)
	}
}

func newDKVReplicator(kvs storage.KVStore) nexus_api.RaftReplicator {
	mkdirNexusDirs()
	replStore := sync.NewDKVReplStore(kvs)
	nexusOpts := nexus.OptionsFromFlags()
	nexusOpts = append(nexusOpts, nexus.StatsDAddr(config.StatsdAddr))
	if nexusRepl, err := nexus_api.NewRaftReplicator(replStore, nexusOpts...); err != nil {
		panic(err)
	} else {
		nexusRepl.Start()
		return nexusRepl
	}
}

func registerDiscoveryServer(grpcSrvr *grpc.Server, dkvService master.DKVService) error {
	discoveryService, err := discovery.NewDiscoveryService(dkvService, dkvLogger, &config.DiscoveryConfig.ServerConfig)
	if err != nil {
		return err
	}
	serverpb.RegisterDKVDiscoveryServer(grpcSrvr, discoveryService)
	return nil
}

func newDiscoveryClient() (discovery.Client, error) {
	client, err := discovery.NewDiscoveryClient(&config.DiscoveryConfig.ClientConfig, dkvLogger)
	if err != nil {
		return nil, err
	}
	return client, nil

}

func nodeAddress(listenAddress string) (*url.URL, error) {
	ip, port, err := net.SplitHostPort(listenAddress)
	if err != nil {
		return nil, err
	}

	if ip == "0.0.0.0" {
		//get interface ip.
		addrs, err := net.InterfaceAddrs()
		if err != nil {
			return nil, err
		}
		for _, address := range addrs {
			// check the address type and if it is not a loopback the display it
			if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
				if ipnet.IP.To4() != nil {
					ip = ipnet.IP.String()
				}
			}
		}
	}

	ep := url.URL{Host: fmt.Sprintf("%s:%s", ip, port)}
	return &ep, nil
}

func setupHttpServer() {
	router := mux.NewRouter()
	router.Handle("/metrics", promhttp.Handler())
	router.HandleFunc("/metrics/json", jsonMetricHandler)

	router.HandleFunc("/metrics/stream", statsStreamHandler)
	if toDKVSrvrRole(config.DbRole) == masterRole {
		// Should be enabled only for discovery server ?
		router.HandleFunc("/metrics/cluster", clusterMetricsHandler)
	}

	//Pprof
	if pprofEnable {
		log.Printf("[INFO] Enabling pprof...\n")
		router.HandleFunc("/debug/pprof/", pprof.Index)
		router.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		router.HandleFunc("/debug/pprof/profile", pprof.Profile)
		router.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		router.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}

	http.Handle("/", router)
	http.ListenAndServe(config.HttpListenAddr, nil)
}

func jsonMetricHandler(w http.ResponseWriter, r *http.Request) {
	metrics, err := stats.GetMetrics()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(metrics)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

}

func statsStreamHandler(w http.ResponseWriter, r *http.Request) {
	if f, ok := w.(http.Flusher); !ok {
		http.Error(w, "Streaming unsupported!", http.StatusInternalServerError)
		return
	} else {
		// Set the headers related to event streaming.
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("Access-Control-Allow-Origin", r.Header.Get("Origin"))
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")

		statChannel := make(chan stats.DKVMetrics, 5)
		channelId := statsStreamer.Register(statChannel)
		defer func() {
			io.Copy(ioutil.Discard, r.Body)
			r.Body.Close()
		}()
		// Listen to the closing of the http connection via the CloseNotifier
		log.Printf("[INFO] Starting Metrics Stream %v\n", channelId)
		for {
			select {
			case stat := <-statChannel:
				statJson, _ := json.Marshal(stat)
				fmt.Fprintf(w, "data: %s\n\n", statJson)
				f.Flush()
			case <-r.Context().Done():
				statsStreamer.DeRegister(channelId)
				log.Printf("[INFO] Closing Metics Stream %v\n", channelId)
				return
			}
		}
	}
}

func clusterMetricsHandler(w http.ResponseWriter, r *http.Request) {
	regions, err := discoveryClient.GetClusterStatus("", "")
	if err != nil {
		http.Error(w, "Unable to discover peers!", http.StatusInternalServerError)
		return
	}
	if f, ok := w.(http.Flusher); !ok {
		http.Error(w, "Streaming unsupported!", http.StatusInternalServerError)
		return
	} else {
		// Set the headers related to event streaming.
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("Access-Control-Allow-Origin", r.Header.Get("Origin"))
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")

		statChannel := make(chan map[string]*stats.DKVMetrics, 5)
		channelId := statAggregatorRegistry.Register(regions, func(region *serverpb.RegionInfo) string { return region.Database }, statChannel)
		defer func() {
			io.Copy(ioutil.Discard, r.Body)
			r.Body.Close()
		}()

		// Listen to the closing of the http connection via the CloseNotifier
		log.Printf("[INFO] Starting ClusterMetics Stream %v\n", channelId)
		for {
			select {
			case stat := <-statChannel:
				statJson, _ := json.Marshal(stat)
				fmt.Fprintf(w, "data: %s\n\n", statJson)
				f.Flush()
			case <-r.Context().Done():
				log.Printf("[INFO] Closing ClusterMetics Stream %v\n", channelId)
				statAggregatorRegistry.DeRegister(channelId)
				return
			}
		}
	}
}
