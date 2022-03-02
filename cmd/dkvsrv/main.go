package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path"
	"strings"
	"syscall"
	"time"

	"github.com/flipkart-incubator/dkv/internal/discovery"
	"github.com/flipkart-incubator/dkv/internal/master"
	"github.com/flipkart-incubator/dkv/internal/opts"
	"github.com/flipkart-incubator/dkv/internal/slave"
	"github.com/flipkart-incubator/dkv/internal/stats"
	"github.com/flipkart-incubator/dkv/internal/storage"
	"github.com/flipkart-incubator/dkv/internal/storage/badger"
	"github.com/flipkart-incubator/dkv/internal/storage/rocksdb"
	"github.com/flipkart-incubator/dkv/internal/sync"
	"github.com/flipkart-incubator/dkv/pkg/health"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
	nexus_api "github.com/flipkart-incubator/nexus/pkg/api"
	nexus "github.com/flipkart-incubator/nexus/pkg/raft"

	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"gopkg.in/ini.v1"

	_ "net/http/pprof"
)

var (
	// Logging vars
	verboseLogging bool
	accessLogger   *zap.Logger
	dkvLogger      *zap.Logger
	pprofEnable    bool

	nexusLogDirFlag, nexusSnapDirFlag *flag.Flag

	statsCli stats.Client

	//Config file used for reading and using configs
	cfgFile string

	config Config
)

type Config struct {
	// region level configuration.
	DisklessMode           bool   `mapstructure:"diskless"`
	NodeName               string `mapstructure:"node-name"`
	DbEngine               string `mapstructure:"db-engine"`
	DbEngineIni            string `mapstructure:"db-engine-ini"`
	DbRole                 string `mapstructure:"role"`
	ReplPollIntervalString string `mapstructure:"repl-poll-interval"`
	BlockCacheSize         uint64 `mapstructure:"block-cache-size"`
	DcID                   string `mapstructure:"dc-id"`
	Database               string `mapstructure:"database"`
	VBucket                string `mapstructure:"vbucket"`

	// Node level configuration common for all regions in the node
	DbFolder     string `mapstructure:"db-folder"`
	DbListenAddr string `mapstructure:"listen-addr"`
	StatsdAddr   string `mapstructure:"statsd-addr"`

	//Service discovery related params
	DiscoveryServiceConfig string `mapstructure:"discovery-service-config"`

	// Temporary variables to be removed once https://github.com/flipkart-incubator/dkv/issues/82 is fixed
	// The above issue causes replication issues during master switch due to inconsistent change numbers
	// Thus enabling hardcoded masters to not degrade current behaviour
	ReplicationMasterAddr string `mapstructure:"repl-master-addr"`
	DisableAutoMasterDisc bool   `mapstructure:"disable-auto-master-disc"`

	// Logging vars
	DbAccessLog string `mapstructure:"access-log"`

	ReplPollInterval time.Duration
}

func init() {
	initializeFlags()
	readConfigDataWithViper()
	overrideFlagsWithConfigData()
	setDKVDefaultsForNexusDirs()
}

func initializeFlags() {
	flag.StringVar(&cfgFile, "config", "", "config file (default is $HOME/dkvconfig.yaml)")
	flag.StringVar(&config.NodeName, "node-name", "", "name of the server node")
	flag.StringVar(&config.DbListenAddr, "listen-addr", "0.0.0.0:8080", "Address on which the DKV service binds")
	flag.StringVar(&config.Database, "database", "default", "Database identifier")
	flag.BoolVar(&verboseLogging, "verbose", false, fmt.Sprintf("Enable verbose logging.\nBy default, only warnings and errors are logged. (default %v)", verboseLogging))
	flag.BoolVar(&pprofEnable, "pprof", false, "Enable pprof profiling")
	flag.Parse()
}

func readConfigDataWithViper() {
	if cfgFile != "" {
		fmt.Println("file used " + cfgFile)
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := os.UserHomeDir()
		if err != nil {
			log.Panicf("Failed to read os variable %v.", err)
		}
		// Search config in home directory with name "dkvconfig.json"
		viper.AddConfigPath(home)
		viper.SetConfigType("json")
		viper.SetConfigName("dkvconfig")
	}
	viper.AutomaticEnv()
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
	unmarshallViper()
}

func unmarshallViper() {
	viper.Unmarshal(&config)
	//Handling time duration variable unmarshalling
	if config.ReplPollIntervalString != "" {
		replicationPollInterval, err := time.ParseDuration(config.ReplPollIntervalString)
		if err != nil {
			log.Panicf("Failed to read Replication poll iterval value from config %v", err)
		}
		config.ReplPollInterval = replicationPollInterval
	}
	//Append node name to default db folder location
	if config.NodeName != "" {
		config.DbFolder = path.Join(config.DbFolder, config.NodeName, "data")
	}
}

func overrideFlagsWithConfigData() {
	flag.VisitAll(func(f *flag.Flag) {
		// Apply the viper config value to the flag when the flag is not set and viper has a value
		if viper.IsSet(f.Name) {
			val := viper.Get(f.Name)
			flag.Set(f.Name, fmt.Sprintf("%v", val))
		}
	})
}

type dkvSrvrRole string

const (
	noRole        dkvSrvrRole = "none"
	masterRole    dkvSrvrRole = "master"
	slaveRole     dkvSrvrRole = "slave"
	discoveryRole dkvSrvrRole = "discovery"
)

const defBlockCacheSize = 3 << 30

const (
	discoveryServerConfig = "serverConfig"
	discoveryClientConfig = "clientConfig"
)

func main() {
	flag.Parse()
	validateFlags()
	setupDKVLogger()
	setupAccessLogger()
	setFlagsForNexusDirs()
	setupStats()
	printFlagsWithoutPrefix()

	if pprofEnable {
		go func() {
			log.Printf("[INFO] Starting pprof on port 6060\n")
			log.Println(http.ListenAndServe("0.0.0.0:6060", nil))
		}()
	}

	kvs, cp, ca, br := newKVStore()
	grpcSrvr, lstnr := newGrpcServerListener()
	defer grpcSrvr.GracefulStop()
	srvrRole := toDKVSrvrRole(config.DbRole)
	//srvrRole.printFlags()

	// Create the region info which is passed to DKVServer
	nodeAddr, err := nodeAddress()
	if err != nil {
		log.Panicf("Failed to detect IP Address %v.", err)
	}
	regionInfo := &serverpb.RegionInfo{
		DcID:            config.DcID,
		NodeAddress:     nodeAddr.Host,
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
	}

	var discoveryClient discovery.Client
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
			MaxNumChngs:           maxNumChanges,
			ReplPollInterval:      config.ReplPollInterval,
			MaxActiveReplLag:      uint64(maxNumChanges * 10),
			MaxActiveReplElapsed:  uint64(config.ReplPollInterval.Seconds()) * 10,
			DisableAutoMasterDisc: config.DisableAutoMasterDisc,
			ReplMasterAddr:        config.ReplicationMasterAddr,
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

func validateFlags() {
	if config.DbListenAddr != "" && strings.IndexRune(config.DbListenAddr, ':') < 0 {
		log.Panicf("given listen address: %s is invalid, must be in host:port format", config.DbListenAddr)
	}
	if config.StatsdAddr != "" && strings.IndexRune(config.StatsdAddr, ':') < 0 {
		log.Panicf("given StatsD address: %s is invalid, must be in host:port format", config.StatsdAddr)
	}

	if config.DisklessMode && strings.ToLower(config.DbEngine) == "rocksdb" {
		log.Panicf("diskless is available only on Badger storage")
	}

	if config.DbEngineIni != "" {
		if _, err := os.Stat(config.DbEngineIni); err != nil && os.IsNotExist(err) {
			log.Panicf("given storage configuration file: %s does not exist", config.DbEngineIni)
		}
	}

	if config.DbRole == "slave" && config.DisableAutoMasterDisc {
		if config.ReplicationMasterAddr == "" || strings.IndexRune(config.ReplicationMasterAddr, ':') < 0 {
			log.Panicf("given master address: %s for replication is invalid, must be in host:port format", config.ReplicationMasterAddr)
		}
	}
}

func setupAccessLogger() {
	accessLogger = zap.NewNop()
	if config.DbAccessLog != "" {
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

			OutputPaths:      []string{config.DbAccessLog},
			ErrorOutputPaths: []string{config.DbAccessLog},
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
		grpc.StreamInterceptor(grpc_zap.StreamServerInterceptor(accessLogger)),
		grpc.UnaryInterceptor(grpc_zap.UnaryServerInterceptor(accessLogger)),
	)
	reflection.Register(grpcSrvr)
	return grpcSrvr, newListener()
}

func newListener() (lis net.Listener) {
	var err error
	if lis, err = net.Listen("tcp", config.DbListenAddr); err != nil {
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
	case masterRole, discoveryRole:
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
	dbPath := config.DbFolder
	nexusLogDirFlag.DefValue, nexusSnapDirFlag.DefValue = path.Join(dbPath, "logs"), path.Join(dbPath, "snap")
	nexusLogDirFlag.Value.Set("")
	nexusSnapDirFlag.Value.Set("")
}

func setFlagsForNexusDirs() {
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
			badger.WithCacheSize(config.BlockCacheSize),
			badger.WithBadgerConfig(config.DbEngineIni),
			badger.WithLogger(dkvLogger),
			badger.WithStats(statsCli),
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
	iniConfig, err := ini.Load(config.DiscoveryServiceConfig)
	if err != nil {
		return fmt.Errorf("unable to load discovery service configuration from given file: %s, error: %v", config.DiscoveryServiceConfig, err)
	}
	if discoveryServerSection, err := iniConfig.GetSection(discoveryServerConfig); err == nil {
		discoverySrvConfig, err := discovery.NewDiscoverConfigFromIni(discoveryServerSection)
		if err != nil {
			return err
		}
		discoveryService, err := discovery.NewDiscoveryService(dkvService, dkvLogger, discoverySrvConfig)
		if err != nil {
			return err
		}
		serverpb.RegisterDKVDiscoveryServer(grpcSrvr, discoveryService)
		return nil
	} else {
		return fmt.Errorf("started as discovery server but can't load the section %s in file %s, error: %v",
			discoveryServerConfig, config.DiscoveryServiceConfig, err)
	}
}

func newDiscoveryClient() (discovery.Client, error) {
	iniConfig, err := ini.Load(config.DiscoveryServiceConfig)
	if err != nil {
		return nil, fmt.Errorf("unable to load discovery service configuration from given file: %s, error: %v", config.DiscoveryServiceConfig, err)
	}

	if discoveryClientSection, err := iniConfig.GetSection(discoveryClientConfig); err == nil {
		clientConfig, err := discovery.NewDiscoveryClientConfigFromIni(discoveryClientSection)
		if err != nil {
			return nil, err
		}
		client, err := discovery.NewDiscoveryClient(clientConfig, dkvLogger)
		if err != nil {
			return nil, err
		}
		return client, nil
	} else {
		return nil, fmt.Errorf("can't load discovery client configuration from section %s in file %s, error: %v",
			discoveryClientConfig, config.DiscoveryServiceConfig, err)
	}

}

func nodeAddress() (*url.URL, error) {
	ip, port, err := net.SplitHostPort(config.DbListenAddr)
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
