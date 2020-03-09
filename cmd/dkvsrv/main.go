package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
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
	"google.golang.org/grpc"
)

var (
	dbEngine         string
	dbFolder         string
	dbListenAddr     string
	dbRole           string
	replMasterAddr   string
	replPollInterval uint
)

func init() {
	flag.StringVar(&dbFolder, "dbFolder", "/tmp/dkvsrv", "DB folder path for storing data files")
	flag.StringVar(&dbListenAddr, "dbListenAddr", "127.0.0.1:8080", "Address on which the DKV service binds")
	flag.StringVar(&dbEngine, "dbEngine", "rocksdb", "Underlying DB engine for storing data - badger|rocksdb")
	flag.StringVar(&dbRole, "dbRole", "none", "DB role of this node - none|master|slave")
	flag.StringVar(&replMasterAddr, "replMasterAddr", "", "Service address of DKV master node for replication")
	flag.UintVar(&replPollInterval, "replPollInterval", 5, "Interval (in seconds) used by the replication poller of this node")
}

type dkvSrvrRole string

const (
	None   dkvSrvrRole = "none"
	Master             = "master"
	Slave              = "slave"
)

func toDKVSrvrRole(role string) dkvSrvrRole {
	return dkvSrvrRole(strings.TrimSpace(strings.ToLower(dbRole)))
}

func main() {
	flag.Parse()

	kvs, cp, ca := newKVStore()
	grpcSrvr, lstnr := newGrpcServerListener()
	defer grpcSrvr.GracefulStop()
	switch toDKVSrvrRole(dbRole) {
	case None:
		printFlagsWithPrefix("db")
		dkvSvc := master.NewStandaloneService(kvs, nil)
		defer dkvSvc.Close()
		serverpb.RegisterDKVServer(grpcSrvr, dkvSvc)
	case Master:
		if cp == nil {
			panic(fmt.Sprintf("Storage engine %s is not supported for DKV master role.", dbEngine))
		}
		var dkvSvc master.DKVService
		if haveFlagsWithPrefix("nexus") {
			printFlagsWithPrefix("db", "nexus")
			dkvSvc = master.NewDistributedService(kvs, cp, newDKVReplicator(kvs))
		} else {
			printFlagsWithPrefix("db")
			dkvSvc = master.NewStandaloneService(kvs, cp)
		}
		defer dkvSvc.Close()
		serverpb.RegisterDKVServer(grpcSrvr, dkvSvc)
		serverpb.RegisterDKVReplicationServer(grpcSrvr, dkvSvc)
	case Slave:
		printFlagsWithPrefix("db", "repl")
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

func newGrpcServerListener() (*grpc.Server, net.Listener) {
	return grpc.NewServer(), newListener()
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

const cacheSize = 3 << 30

func newKVStore() (storage.KVStore, storage.ChangePropagator, storage.ChangeApplier) {
	switch dbEngine {
	case "rocksdb":
		rocks_db := rocksdb.OpenDB(dbFolder, cacheSize)
		return rocks_db, rocks_db, rocks_db
	case "badger":
		badger_db := badger.OpenDB(dbFolder)
		return badger_db, nil, badger_db
	default:
		panic(fmt.Sprintf("Unknown storage engine: %s", dbEngine))
	}
}

func newDKVReplicator(kvs storage.KVStore) nexus_api.RaftReplicator {
	replStore := sync.NewDKVReplStore(kvs)
	if nexusRepl, err := nexus_api.NewRaftReplicator(replStore, nexus.OptionsFromFlags()...); err != nil {
		panic(err)
	} else {
		nexusRepl.Start()
		return nexusRepl
	}
}
