package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path"
	"strings"
	"syscall"
	"time"

	"github.com/flipkart-incubator/dkv/internal/master"
	"github.com/flipkart-incubator/dkv/internal/slave"
	"github.com/flipkart-incubator/dkv/internal/stats"
	"github.com/flipkart-incubator/dkv/internal/storage"
	"github.com/flipkart-incubator/dkv/internal/storage/badger"
	"github.com/flipkart-incubator/dkv/internal/storage/rocksdb"
	"github.com/flipkart-incubator/dkv/internal/sync"
	"github.com/flipkart-incubator/dkv/pkg/ctl"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
	nexus_api "github.com/flipkart-incubator/nexus/pkg/api"
	nexus "github.com/flipkart-incubator/nexus/pkg/raft"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	_ "net/http/pprof"
)

var (
	disklessMode     bool
	dbEngine         string
	dbEngineIni      string
	dbFolder         string
	dbListenAddr     string
	httpServerAddr   string
	dbRole           string
	statsdAddr       string
	replMasterAddr   string
	replPollInterval time.Duration
	blockCacheSize   uint64

	// Logging vars
	dbAccessLog    string
	verboseLogging bool
	accessLogger   *zap.Logger
	dkvLogger      *zap.Logger
	pprofEnable    bool

	nexusLogDirFlag, nexusSnapDirFlag *flag.Flag

	statsCli stats.Client
)

func init() {
	flag.BoolVar(&disklessMode, "diskless", false, fmt.Sprintf("Enables diskless mode where data is stored entirely in memory.\nAvailable on Badger for standalone and slave roles. (default %v)", disklessMode))
	flag.StringVar(&dbFolder, "db-folder", "/tmp/dkvsrv", "DB folder path for storing data files")
	flag.StringVar(&dbListenAddr, "listen-addr", "0.0.0.0:8080", "Address on which the DKV service binds")
	flag.StringVar(&httpServerAddr,"http-server-addr","0.0.0.0:8181","Address on which the http service binds")
	flag.StringVar(&dbEngine, "db-engine", "rocksdb", "Underlying DB engine for storing data - badger|rocksdb")
	flag.StringVar(&dbEngineIni, "db-engine-ini", "", "An .ini file for configuring the underlying storage engine. Refer badger.ini or rocks.ini for more details.")
	flag.StringVar(&dbRole, "role", "none", "DB role of this node - none|master|slave")
	flag.StringVar(&statsdAddr, "statsd-addr", "", "StatsD service address in host:port format")
	flag.StringVar(&replMasterAddr, "repl-master-addr", "", "Service address of DKV master node for replication")
	flag.DurationVar(&replPollInterval, "repl-poll-interval", 5*time.Second, "Interval used for polling changes from master. Eg., 10s, 5ms, 2h, etc.")
	flag.StringVar(&dbAccessLog, "access-log", "", "File for logging DKV accesses eg., stdout, stderr, /tmp/access.log")
	flag.BoolVar(&verboseLogging, "verbose", false, fmt.Sprintf("Enable verbose logging.\nBy default, only warnings and errors are logged. (default %v)", verboseLogging))
	flag.Uint64Var(&blockCacheSize, "block-cache-size", defBlockCacheSize, "Amount of cache (in bytes) to set aside for data blocks. A value of 0 disables block caching altogether.")
	flag.BoolVar(&pprofEnable, "pprof", false, "Enable pprof profiling")
	setDKVDefaultsForNexusDirs()
}

type dkvSrvrRole string

const (
	noRole     dkvSrvrRole = "none"
	masterRole             = "master"
	slaveRole              = "slave"
)

const defBlockCacheSize = 3 << 30

func main() {
	flag.Parse()
	validateFlags()
	setupDKVLogger()
	setupAccessLogger()
	setFlagsForNexusDirs()
	setupStats()
	go setupHttpServer()

	if pprofEnable {
		go func() {
			log.Printf("[INFO] Starting pprof on port 6060\n")
			log.Println(http.ListenAndServe("0.0.0.0:6060", nil))
		}()
	}

	kvs, cp, ca, br := newKVStore()
	grpcSrvr, lstnr := newGrpcServerListener()
	defer grpcSrvr.GracefulStop()
	srvrRole := toDKVSrvrRole(dbRole)
	srvrRole.printFlags()

	switch srvrRole {
	case noRole:
		dkvSvc := master.NewStandaloneService(kvs, nil, br, dkvLogger, statsCli)
		defer dkvSvc.Close()
		serverpb.RegisterDKVServer(grpcSrvr, dkvSvc)
		serverpb.RegisterDKVBackupRestoreServer(grpcSrvr, dkvSvc)
	case masterRole:
		if cp == nil {
			log.Panicf("Storage engine %s is not supported for DKV master role.", dbEngine)
		}
		var dkvSvc master.DKVService
		if haveFlagsWithPrefix("nexus") {
			dkvSvc = master.NewDistributedService(kvs, cp, br, newDKVReplicator(kvs), dkvLogger, statsCli)
			serverpb.RegisterDKVClusterServer(grpcSrvr, dkvSvc.(master.DKVClusterService))
		} else {
			dkvSvc = master.NewStandaloneService(kvs, cp, br, dkvLogger, statsCli)
			serverpb.RegisterDKVBackupRestoreServer(grpcSrvr, dkvSvc)
		}
		defer dkvSvc.Close()
		serverpb.RegisterDKVServer(grpcSrvr, dkvSvc)
		serverpb.RegisterDKVReplicationServer(grpcSrvr, dkvSvc)
	case slaveRole:
		// TODO: Check if authority override option is needed for slaves
		// while they connect with masters
		if replCli, err := ctl.NewInSecureDKVClient(replMasterAddr, ""); err != nil {
			panic(err)
		} else {
			defer replCli.Close()
			dkvSvc, _ := slave.NewService(kvs, ca, replCli, replPollInterval, dkvLogger, statsCli)
			defer dkvSvc.Close()
			serverpb.RegisterDKVServer(grpcSrvr, dkvSvc)
		}
	default:
		panic("Invalid 'dbRole'. Allowed values are none|master|slave.")
	}
	go grpcSrvr.Serve(lstnr)
	sig := <-setupSignalHandler()
	log.Printf("[WARN] Caught signal: %v. Shutting down...\n", sig)
}

func validateFlags() {
	if dbListenAddr != "" && strings.IndexRune(dbListenAddr, ':') < 0 {
		log.Panicf("given listen address: %s is invalid, must be in host:port format", dbListenAddr)
	}
	if replMasterAddr != "" && strings.IndexRune(replMasterAddr, ':') < 0 {
		log.Panicf("given master address: %s for replication is invalid, must be in host:port format", replMasterAddr)
	}
	if statsdAddr != "" && strings.IndexRune(statsdAddr, ':') < 0 {
		log.Panicf("given StatsD address: %s is invalid, must be in host:port format", statsdAddr)
	}

	if disklessMode && strings.ToLower(dbEngine) == "rocksdb" {
		log.Panicf("diskless is available only on Badger storage")
	}

	if strings.ToLower(dbRole) == slaveRole && replMasterAddr == "" {
		log.Panicf("repl-master-addr must be given in slave mode")
	}
	if dbEngineIni != "" {
		if _, err := os.Stat(dbEngineIni); err != nil && os.IsNotExist(err) {
			log.Panicf("given storage configuration file: %s does not exist", dbEngineIni)
		}
	}
}

func setupAccessLogger() {
	accessLogger = zap.NewNop()
	if dbAccessLog != "" {
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

			OutputPaths:      []string{dbAccessLog},
			ErrorOutputPaths: []string{dbAccessLog},
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
		dkvLoggerConfig.Level = zap.NewAtomicLevelAt(zap.WarnLevel)
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
		grpc.ChainStreamInterceptor(grpc_prometheus.StreamServerInterceptor,grpc_zap.StreamServerInterceptor(accessLogger)),
		grpc.ChainUnaryInterceptor(grpc_prometheus.UnaryServerInterceptor,grpc_zap.UnaryServerInterceptor(accessLogger)),
	)
	reflection.Register(grpcSrvr)
	grpc_prometheus.Register(grpcSrvr)
	return grpcSrvr, newListener()
}

func newListener() (lis net.Listener) {
	var err error
	if lis, err = net.Listen("tcp", dbListenAddr); err != nil {
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

func printFlagsWithoutPrefix(prefixes ...string) {
	flag.VisitAll(func(f *flag.Flag) {
		shouldPrint := true
		for _, pf := range prefixes {
			if strings.HasPrefix(f.Name, pf) {
				shouldPrint = false
				break
			}
		}
		if shouldPrint {
			log.Printf("%s (%s): %v\n", f.Name, f.Usage, f.Value)
		}
	})
}

func printFlagsWithPrefix(prefixes ...string) {
	flag.VisitAll(func(f *flag.Flag) {
		for _, pf := range prefixes {
			if strings.HasPrefix(f.Name, pf) {
				log.Printf("%s (%s): %v\n", f.Name, f.Usage, f.Value)
			}
		}
	})
}

func toDKVSrvrRole(role string) dkvSrvrRole {
	return dkvSrvrRole(strings.TrimSpace(strings.ToLower(role)))
}

func (role dkvSrvrRole) printFlags() {
	log.Println("Launching DKV server with following flags:")
	switch role {
	case noRole:
		printFlagsWithPrefix("db")
	case masterRole:
		if haveFlagsWithPrefix("nexus") {
			printFlagsWithPrefix("db", "nexus")
		} else {
			printFlagsWithPrefix("db")
		}
	case slaveRole:
		printFlagsWithPrefix("db", "repl")
	}
	printFlagsWithoutPrefix("db", "repl", "nexus")
}

func setDKVDefaultsForNexusDirs() {
	nexusLogDirFlag, nexusSnapDirFlag = flag.Lookup("nexus-log-dir"), flag.Lookup("nexus-snap-dir")
	dbPath := flag.Lookup("db-folder").DefValue
	nexusLogDirFlag.DefValue, nexusSnapDirFlag.DefValue = path.Join(dbPath, "logs"), path.Join(dbPath, "snap")
	nexusLogDirFlag.Value.Set("")
	nexusSnapDirFlag.Value.Set("")
}

func setFlagsForNexusDirs() {
	if nexusLogDirFlag.Value.String() == "" {
		nexusLogDirFlag.Value.Set(path.Join(dbFolder, "logs"))
	}
	if nexusSnapDirFlag.Value.String() == "" {
		nexusSnapDirFlag.Value.Set(path.Join(dbFolder, "snap"))
	}
}

func setupStats() {
	if statsdAddr != "" {
		statsCli = stats.NewStatsDClient(statsdAddr, "dkv.")
	} else {
		statsCli = stats.NewNoOpClient()
	}
}

func newKVStore() (storage.KVStore, storage.ChangePropagator, storage.ChangeApplier, storage.Backupable) {
	slg := dkvLogger.Sugar()
	defer slg.Sync()

	if err := os.MkdirAll(dbFolder, 0777); err != nil {
		slg.Fatalf("Unable to create DB folder at %s. Error: %v.", dbFolder, err)
	}

	dataDir := path.Join(dbFolder, "data")
	slg.Infof("Using %s as data directory", dataDir)

	sstDir := path.Join(dbFolder, "sst")
	if err := os.MkdirAll(sstDir, 0777); err != nil {
		slg.Fatalf("Unable to create sst folder at %s. Error: %v.", dbFolder, err)
	}

	switch dbEngine {
	case "rocksdb":
		rocksDb, err := rocksdb.OpenDB(dataDir,
			rocksdb.WithSSTDir(sstDir),
			rocksdb.WithSyncWrites(),
			rocksdb.WithCacheSize(blockCacheSize),
			rocksdb.WithRocksDBConfig(dbEngineIni),
			rocksdb.WithLogger(dkvLogger),
			rocksdb.WithStats(statsCli))
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
			badger.WithCacheSize(blockCacheSize),
			badger.WithBadgerConfig(dbEngineIni),
			badger.WithLogger(dkvLogger),
			badger.WithStats(statsCli),
		}
		if disklessMode {
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
		slg.Panicf("Unknown storage engine: %s", dbEngine)
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
	nexusOpts = append(nexusOpts, nexus.StatsDAddr(statsdAddr))
	if nexusRepl, err := nexus_api.NewRaftReplicator(replStore, nexusOpts...); err != nil {
		panic(err)
	} else {
		nexusRepl.Start()
		return nexusRepl
	}
}

func setupHttpServer() {
	router := mux.NewRouter()
	router.Handle("/metrics/prometheus", promhttp.Handler())
	router.HandleFunc("/metrics/json", jsonMetricHandler)
	router.HandleFunc("/metrics/stream",statsStreamHandler)
	prometheus.Unregister(prometheus.NewGoCollector())
	http.Handle("/", router)
	http.ListenAndServe(httpServerAddr, nil)
}

func jsonMetricHandler(w http.ResponseWriter, r *http.Request){
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(stats.GetMetrics())
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

		defer func() {
			ioutil.ReadAll(r.Body)
			r.Body.Close()
		}()

		// Listen to the closing of the http connection via the CloseNotifier
		notify := w.(http.CloseNotifier).CloseNotify()
		for ;; {
			select {
			case <-notify :
				return
			default:
				statJson, _ := json.Marshal(stats.GetMetrics())
				fmt.Fprintf(w, "data: %s\n\n", statJson)
				f.Flush()
				time.Sleep(time.Second)
			}
		}
	}
}