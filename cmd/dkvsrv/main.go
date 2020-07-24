package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path"
	"strings"
	"syscall"

	"github.com/flipkart-incubator/dkv/internal/ctl"
	"github.com/flipkart-incubator/dkv/internal/server/master"
	"github.com/flipkart-incubator/dkv/internal/server/slave"
	"github.com/flipkart-incubator/dkv/internal/server/storage"
	"github.com/flipkart-incubator/dkv/internal/server/storage/badger"
	"github.com/flipkart-incubator/dkv/internal/server/storage/rocksdb"
	"github.com/flipkart-incubator/dkv/internal/server/sync"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
	nexus_api "github.com/flipkart-incubator/nexus/pkg/api"
	nexus "github.com/flipkart-incubator/nexus/pkg/raft"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
)

var (
	dbAccessLog      string
	dbEngine         string
	dbFolder         string
	dbListenAddr     string
	dbRole           string
	replMasterAddr   string
	replPollInterval uint

	nexusLogDirFlag, nexusSnapDirFlag *flag.Flag
)

func init() {
	flag.StringVar(&dbFolder, "dbFolder", "/tmp/dkvsrv", "DB folder path for storing data files")
	flag.StringVar(&dbListenAddr, "dbListenAddr", "127.0.0.1:8080", "Address on which the DKV service binds")
	flag.StringVar(&dbEngine, "dbEngine", "rocksdb", "Underlying DB engine for storing data - badger|rocksdb")
	flag.StringVar(&dbRole, "dbRole", "none", "DB role of this node - none|master|slave")
	flag.StringVar(&replMasterAddr, "replMasterAddr", "", "Service address of DKV master node for replication")
	flag.UintVar(&replPollInterval, "replPollInterval", 5, "Interval (in seconds) used by the replication poller of this node")
	flag.StringVar(&dbAccessLog, "dbAccessLog", "", "File for logging DKV accesses eg., stdout, stderr, /tmp/access.log")
	setDKVDefaultsForNexusDirs()
}

type dkvSrvrRole string

const (
	noRole     dkvSrvrRole = "none"
	masterRole             = "master"
	slaveRole              = "slave"
)

func main() {
	flag.Parse()
	setFlagsForNexusDirs()

	kvs, cp, ca, br := newKVStore()
	grpcSrvr, lstnr := newGrpcServerListener()
	defer grpcSrvr.GracefulStop()
	srvrRole := toDKVSrvrRole(dbRole)
	srvrRole.printFlags()

	switch srvrRole {
	case noRole:
		dkvSvc := master.NewStandaloneService(kvs, nil, br)
		defer dkvSvc.Close()
		serverpb.RegisterDKVServer(grpcSrvr, dkvSvc)
		serverpb.RegisterDKVBackupRestoreServer(grpcSrvr, dkvSvc)
	case masterRole:
		if cp == nil {
			panic(fmt.Sprintf("Storage engine %s is not supported for DKV master role.", dbEngine))
		}
		var dkvSvc master.DKVService
		if haveFlagsWithPrefix("nexus") {
			dkvSvc = master.NewDistributedService(kvs, cp, br, newDKVReplicator(kvs))
			serverpb.RegisterDKVClusterServer(grpcSrvr, dkvSvc.(master.DKVClusterService))
		} else {
			dkvSvc = master.NewStandaloneService(kvs, cp, br)
			serverpb.RegisterDKVBackupRestoreServer(grpcSrvr, dkvSvc)
		}
		defer dkvSvc.Close()
		serverpb.RegisterDKVServer(grpcSrvr, dkvSvc)
		serverpb.RegisterDKVReplicationServer(grpcSrvr, dkvSvc)
	case slaveRole:
		if replCli, err := ctl.NewInSecureDKVClient(replMasterAddr); err != nil {
			panic(err)
		} else {
			defer replCli.Close()
			dkvSvc, _ := slave.NewService(kvs, ca, replCli, replPollInterval)
			defer dkvSvc.Close()
			serverpb.RegisterDKVServer(grpcSrvr, dkvSvc)
		}
	default:
		panic("Invalid 'dbRole'. Allowed values are none|master|slave.")
	}
	go grpcSrvr.Serve(lstnr)
	sig := <-setupSignalHandler()
	fmt.Printf("[WARN] Caught signal: %v. Shutting down...\n", sig)
}

var accessLoggerConfig = zap.Config{
	Level: zap.NewAtomicLevelAt(zap.InfoLevel),

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
}

func newGrpcServerListener() (*grpc.Server, net.Listener) {
	accessLogger := zap.NewNop()
	if dbAccessLog != "" {
		accessLoggerConfig.OutputPaths = []string{dbAccessLog}
		accessLoggerConfig.ErrorOutputPaths = []string{dbAccessLog}
		if lg, err := accessLoggerConfig.Build(); err != nil {
			panic(err)
		} else {
			accessLogger = lg
		}
	}
	grpcSrvr := grpc.NewServer(
		grpc.StreamInterceptor(grpc_zap.StreamServerInterceptor(accessLogger)),
		grpc.UnaryInterceptor(grpc_zap.UnaryServerInterceptor(accessLogger)),
	)
	return grpcSrvr, newListener()
}

func newListener() net.Listener {
	if lis, err := net.Listen("tcp", dbListenAddr); err != nil {
		panic(fmt.Sprintf("failed to listen: %v", err))
	} else {
		return lis
	}
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

func printFlagsWithPrefix(prefixes ...string) {
	fmt.Println("Launching DKV server with following flags:")
	flag.VisitAll(func(f *flag.Flag) {
		for _, pf := range prefixes {
			if strings.HasPrefix(f.Name, pf) {
				fmt.Printf("%s (%s): %v\n", f.Name, f.Usage, f.Value)
			}
		}
	})
}

func toDKVSrvrRole(role string) dkvSrvrRole {
	return dkvSrvrRole(strings.TrimSpace(strings.ToLower(dbRole)))
}

func (role dkvSrvrRole) printFlags() {
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
}

func setDKVDefaultsForNexusDirs() {
	nexusLogDirFlag, nexusSnapDirFlag = flag.Lookup("nexusLogDir"), flag.Lookup("nexusSnapDir")
	dbPath := flag.Lookup("dbFolder").DefValue
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

const cacheSize = 3 << 30

func newKVStore() (storage.KVStore, storage.ChangePropagator, storage.ChangeApplier, storage.Backupable) {
	if err := os.MkdirAll(dbFolder, 0777); err != nil {
		panic(err)
	}

	dbDir := path.Join(dbFolder, "data")
	switch dbEngine {
	case "rocksdb":
		rocksDb := rocksdb.OpenDB(dbDir, cacheSize)
		return rocksDb, rocksDb, rocksDb, rocksDb
	case "badger":
		badgerDb := badger.OpenDB(dbDir)
		return badgerDb, nil, badgerDb, badgerDb
	default:
		panic(fmt.Sprintf("Unknown storage engine: %s", dbEngine))
	}
}

func mkdirNexusDirs() {
	if err := os.MkdirAll(nexusLogDirFlag.Value.String(), 0777); err != nil {
		panic(fmt.Sprintf("Unable to create Nexus logDir. Error: %v", err))
	}
	if err := os.MkdirAll(nexusSnapDirFlag.Value.String(), 0777); err != nil {
		panic(fmt.Sprintf("Unable to create Nexus snapDir. Error: %v", err))
	}
}

func newDKVReplicator(kvs storage.KVStore) nexus_api.RaftReplicator {
	mkdirNexusDirs()
	replStore := sync.NewDKVReplStore(kvs)
	nexusOpts := nexus.OptionsFromFlags()
	if nexusRepl, err := nexus_api.NewRaftReplicator(replStore, nexusOpts...); err != nil {
		panic(err)
	} else {
		nexusRepl.Start()
		return nexusRepl
	}
}
