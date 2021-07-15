package main

import (
	"flag"
	"fmt"
	"github.com/flipkart-incubator/dkv/internal/discovery"
	"github.com/flipkart-incubator/dkv/internal/utils/name_resolvers"
	"gopkg.in/ini.v1"
	"log"
	"net"
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
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
	nexus_api "github.com/flipkart-incubator/nexus/pkg/api"
	nexus "github.com/flipkart-incubator/nexus/pkg/raft"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var (
	// region level configuration.
	// TODO - move them to config file to setup multiple regions in a node
	disklessMode     bool
	dbEngine         string
	dbEngineIni      string
	dbRole           string
	replPollInterval time.Duration
	blockCacheSize   uint64
	dcID             string
	database         string
	vBucket          string

	// Node level configuration common for all regions in the node
	dbFolder     string
	dbListenAddr string
	statsdAddr   string

	// Service discovery related params
	isDiscoverySrv bool
	discoveryConf  string

	// Temporary variables to be removed once https://github.com/flipkart-incubator/dkv/issues/82 is fixed
	// The above issue causes replication issues during master switch due to inconsistent change numbers
	// Thus enabling hardcoded masters to not degrade current behaviour
	replMasterAddr        string
	disableAutoMasterDisc bool

	// Logging vars
	dbAccessLog    string
	verboseLogging bool
	accessLogger   *zap.Logger
	dkvLogger      *zap.Logger

	nexusLogDirFlag, nexusSnapDirFlag *flag.Flag

	statsCli stats.Client
)

func init() {
	flag.BoolVar(&disklessMode, "diskless", false, fmt.Sprintf("Enables diskless mode where data is stored entirely in memory.\nAvailable on Badger for standalone and slave roles. (default %v)", disklessMode))
	flag.StringVar(&dbFolder, "db-folder", "/tmp/dkvsrv", "DB folder path for storing data files")
	flag.StringVar(&dbListenAddr, "listen-addr", "0.0.0.0:8080", "Address on which the DKV service binds")
	flag.StringVar(&dbEngine, "db-engine", "rocksdb", "Underlying DB engine for storing data - badger|rocksdb")
	flag.StringVar(&dbEngineIni, "db-engine-ini", "", "An .ini file for configuring the underlying storage engine. Refer badger.ini or rocks.ini for more details.")
	flag.StringVar(&dbRole, "role", "none", "DB role of this node - none|master|slave")
	flag.BoolVar(&isDiscoverySrv, "use-as-discovery-srv", false, "Use this node as discovery server")
	flag.StringVar(&discoveryConf, "discovery-service-config", "", "A .ini file for configuring discovery service parameters")
	flag.StringVar(&statsdAddr, "statsd-addr", "", "StatsD service address in host:port format")
	flag.DurationVar(&replPollInterval, "repl-poll-interval", 5*time.Second, "Interval used for polling changes from master. Eg., 10s, 5ms, 2h, etc.")
	flag.StringVar(&dbAccessLog, "access-log", "", "File for logging DKV accesses eg., stdout, stderr, /tmp/access.log")
	flag.BoolVar(&verboseLogging, "verbose", false, fmt.Sprintf("Enable verbose logging.\nBy default, only warnings and errors are logged. (default %v)", verboseLogging))
	flag.Uint64Var(&blockCacheSize, "block-cache-size", defBlockCacheSize, "Amount of cache (in bytes) to set aside for data blocks. A value of 0 disables block caching altogether.")
	flag.StringVar(&dcID, "dc-id", "default", "DC / Availability zone identifier")
	flag.StringVar(&database, "database", "default", "Database identifier")
	flag.StringVar(&vBucket, "vBucket", "default", "vBucket identifier")
	flag.StringVar(&replMasterAddr, "repl-master-addr", "", "Service address of DKV master node for replication")
	flag.BoolVar(&disableAutoMasterDisc, "disable-auto-master-disc", false, "Disable automated master discovery. Suggested to set to true until https://github.com/flipkart-incubator/dkv/issues/82 is fixed")
	setDKVDefaultsForNexusDirs()
}

type dkvSrvrRole string

const (
	noRole     dkvSrvrRole = "none"
	masterRole             = "master"
	slaveRole              = "slave"
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

	kvs, cp, ca, br := newKVStore()
	grpcSrvr, lstnr := newGrpcServerListener()
	defer grpcSrvr.GracefulStop()
	srvrRole := toDKVSrvrRole(dbRole)
	srvrRole.printFlags()
	name_resolvers.Initialise()

	// Create the region info which is passed to DKVServer
	regionInfo := &serverpb.RegionInfo{
		DcID:            dcID,
		NodeAddress:     dbListenAddr,
		Database:        database,
		VBucket:         vBucket,
		Status:          serverpb.RegionStatus_INACTIVE,
		MasterHost:      nil,
		NexusClusterUrl: nil,
	}

	var discoveryClient discovery.Client
	if srvrRole != noRole && !isDiscoverySrv {
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
		dkvSvc := master.NewStandaloneService(kvs, nil, br, dkvLogger, statsCli, regionInfo)
		defer dkvSvc.Close()
		serverpb.RegisterDKVServer(grpcSrvr, dkvSvc)
		serverpb.RegisterDKVBackupRestoreServer(grpcSrvr, dkvSvc)
	case masterRole:
		if cp == nil {
			log.Panicf("Storage engine %s is not supported for DKV master role.", dbEngine)
		}
		var dkvSvc master.DKVService
		if haveFlagsWithPrefix("nexus") {
			dkvSvc = master.NewDistributedService(kvs, cp, br, newDKVReplicator(kvs), dkvLogger, statsCli, regionInfo)
			serverpb.RegisterDKVClusterServer(grpcSrvr, dkvSvc.(master.DKVClusterService))
		} else {
			dkvSvc = master.NewStandaloneService(kvs, cp, br, dkvLogger, statsCli, regionInfo)
			serverpb.RegisterDKVBackupRestoreServer(grpcSrvr, dkvSvc)
		}
		defer dkvSvc.Close()
		serverpb.RegisterDKVServer(grpcSrvr, dkvSvc)
		serverpb.RegisterDKVReplicationServer(grpcSrvr, dkvSvc)

		// Discovery servers can be only configured if node started as master.
		if isDiscoverySrv {
			err := registerDiscoveryServer(dkvSvc, grpcSrvr)
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
			ReplPollInterval:      replPollInterval,
			MaxActiveReplLag:      uint64(maxNumChanges * 10),
			MaxActiveReplElapsed:  uint64(replPollInterval.Seconds()) * 10,
			DisableAutoMasterDisc: disableAutoMasterDisc,
			ReplMasterAddr:        replMasterAddr,
		}

		dkvSvc, _ := slave.NewService(kvs, ca, dkvLogger, statsCli, regionInfo, replConfig, discoveryClient)
		defer dkvSvc.Close()
		serverpb.RegisterDKVServer(grpcSrvr, dkvSvc)
		discoveryClient.RegisterRegion(dkvSvc)
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
	if statsdAddr != "" && strings.IndexRune(statsdAddr, ':') < 0 {
		log.Panicf("given StatsD address: %s is invalid, must be in host:port format", statsdAddr)
	}

	if disklessMode && strings.ToLower(dbEngine) == "rocksdb" {
		log.Panicf("diskless is available only on Badger storage")
	}

	if dbEngineIni != "" {
		if _, err := os.Stat(dbEngineIni); err != nil && os.IsNotExist(err) {
			log.Panicf("given storage configuration file: %s does not exist", dbEngineIni)
		}
	}

	if disableAutoMasterDisc == true {
		if replMasterAddr == "" || strings.IndexRune(replMasterAddr, ':') < 0 {
			log.Panicf("given master address: %s for replication is invalid, must be in host:port format", replMasterAddr)
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
		grpc.StreamInterceptor(grpc_zap.StreamServerInterceptor(accessLogger)),
		grpc.UnaryInterceptor(grpc_zap.UnaryServerInterceptor(accessLogger)),
	)
	reflection.Register(grpcSrvr)
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
	switch dbEngine {
	case "rocksdb":
		sstDir := path.Join(dbFolder, "sst")
		if err := os.MkdirAll(sstDir, 0777); err != nil {
			slg.Fatalf("Unable to create sst folder at %s. Error: %v.", dbFolder, err)
		}

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

func registerDiscoveryServer(dkvService master.DKVService, grpcSrvr *grpc.Server) error {
	iniConfig, _ := ini.Load(discoveryConf)
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
			discoveryServerConfig, discoveryConf, err)
	}
}

func newDiscoveryClient() (discovery.Client, error) {
	iniConfig, err := ini.Load(discoveryConf)
	if err != nil {
		return nil, fmt.Errorf("unable to load discovery service configuration from given file: %s, error: %v", discoveryConf, err)
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
		return nil, fmt.Errorf("Can't load discovery client configuration from section %s in file %s, error: %v",
			discoveryClientConfig, discoveryConf, err)
	}

}
