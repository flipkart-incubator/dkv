package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/flipkart-incubator/dkv/internal/server/master"
	"github.com/flipkart-incubator/dkv/internal/server/storage"
	"github.com/flipkart-incubator/dkv/internal/server/storage/badger"
	"github.com/flipkart-incubator/dkv/internal/server/storage/redis"
	"github.com/flipkart-incubator/dkv/internal/server/storage/rocksdb"
	"github.com/flipkart-incubator/dkv/internal/server/sync"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
	nexus_api "github.com/flipkart-incubator/nexus/pkg/api"
	nexus "github.com/flipkart-incubator/nexus/pkg/raft"
	"google.golang.org/grpc"
)

var (
	dbEngine     string
	dbFolder     string
	dbListenAddr string
	redisPort    int
	redisDBIndex int
)

func init() {
	flag.StringVar(&dbFolder, "dbFolder", "/tmp/dkvsrv", "DB folder path for storing data files")
	flag.StringVar(&dbListenAddr, "dbListenAddr", "127.0.0.1:8080", "Address on which the DKV service binds")
	flag.StringVar(&dbEngine, "dbEngine", "rocksdb", "Underlying DB engine for storing data - badger|rocksdb")
	flag.IntVar(&redisPort, "redisPort", 6379, "Redis port")
	flag.IntVar(&redisDBIndex, "redisDBIndex", 0, "Redis DB Index")
}

func main() {
	flag.Parse()
	nexusMode := haveFlagsWithPrefix("nexus")
	printFlags(nexusMode)

	kvs, cdc := newKVStore()
	dkvSvc := newDKVService(nexusMode, kvs, cdc)
	grpcSrvr := newDKVGrpcServer(dkvSvc)
	sig := <-setupSignalHandler()
	fmt.Printf("[WARN] Caught signal: %v. Shutting down...\n", sig)
	dkvSvc.Close()
	grpcSrvr.GracefulStop()
}

type serviceMode bool

const (
	standalone  serviceMode = false
	distributed             = true
)

func newDKVService(svcMode bool, kvs storage.KVStore, cdc storage.ChangePropagator) master.DKVService {
	var dkvSvc master.DKVService
	switch serviceMode(svcMode) {
	case standalone:
		dkvSvc = master.NewStandaloneService(kvs, cdc)
	case distributed:
		dkvSvc = master.NewDistributedService(kvs, cdc, newDKVReplicator(kvs))
	}
	return dkvSvc
}

func newDKVGrpcServer(dkvSvc master.DKVService) *grpc.Server {
	grpcSrvr := grpc.NewServer()
	serverpb.RegisterDKVServer(grpcSrvr, dkvSvc)
	serverpb.RegisterDKVReplicationServer(grpcSrvr, dkvSvc)
	lstnr := newListener()
	go grpcSrvr.Serve(lstnr)
	return grpcSrvr
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

func printFlags(nexusMode bool) {
	fmt.Println("Launching DKV server with following flags:")
	flag.VisitAll(func(f *flag.Flag) {
		if strings.HasPrefix(f.Name, "test.") || (!nexusMode && strings.HasPrefix(f.Name, "nexus")) {
			return
		} else {
			fmt.Printf("%s (%s): %v\n", f.Name, f.Usage, f.Value)
		}
	})
	fmt.Println()
}

const cacheSize = 3 << 30

func newKVStore() (storage.KVStore, storage.ChangePropagator) {
	switch dbEngine {
	case "rocksdb":
		rocks_db := rocksdb.OpenDB(dbFolder, cacheSize)
		return rocks_db, rocks_db
	case "badger":
		return badger.OpenDB(dbFolder), nil
	case "redis":
		return redis.OpenDB(redisPort, redisDBIndex), nil
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
