package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/flipkart-incubator/dkv/internal/server/api"
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
	engine       string
	dbFolder     string
	dkvSvcPort   uint
	redisPort    int
	redisDBIndex int
)

func init() {
	flag.StringVar(&engine, "storage", "rocksdb", "Storage engine to use - badger|rocksdb")
	flag.StringVar(&dbFolder, "dbFolder", "/tmp/dkvsrv", "DB folder path")
	flag.UintVar(&dkvSvcPort, "dkvSvcPort", 8080, "DKV service port")
	flag.IntVar(&redisPort, "redisPort", 6379, "Redis port")
	flag.IntVar(&redisDBIndex, "redisDBIndex", 0, "Redis DB Index")
}

func main() {
	flag.Parse()
	nexus_mode := haveFlagsWithPrefix("nexus")
	printFlags(nexus_mode)

	dkv_svc := newDKVService(nexus_mode, newKVStore())
	grpc_srvr := newDKVGrpcServer(dkv_svc)
	sig := <-setupSignalHandler()
	fmt.Printf("[WARN] Caught signal: %v. Shutting down...\n", sig)
	dkv_svc.Close()
	grpc_srvr.GracefulStop()
}

type serviceMode bool

const (
	standalone  serviceMode = false
	distributed             = true
)

func newDKVService(svcMode bool, kvs storage.KVStore) api.DKVService {
	var dkv_svc api.DKVService
	switch serviceMode(svcMode) {
	case standalone:
		dkv_svc = api.NewStandaloneService(kvs)
	case distributed:
		dkv_svc = api.NewDistributedService(kvs, newDKVReplicator(kvs))
	}
	return dkv_svc
}

func newDKVGrpcServer(dkvSvc serverpb.DKVServer) *grpc.Server {
	grpc_srvr := grpc.NewServer()
	serverpb.RegisterDKVServer(grpc_srvr, dkvSvc)
	lstnr := newListener(dkvSvcPort)
	go grpc_srvr.Serve(lstnr)
	return grpc_srvr
}

func newListener(port uint) net.Listener {
	if lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port)); err != nil {
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

func newKVStore() storage.KVStore {
	switch engine {
	case "rocksdb":
		return rocksdb.OpenDB(dbFolder, cacheSize)
	case "badger":
		return badger.OpenDB(dbFolder)
	case "redis":
		return redis.OpenDB(redisPort, redisDBIndex)
	default:
		panic(fmt.Sprintf("Unknown storage engine: %s", engine))
	}
}

func newDKVReplicator(kvs storage.KVStore) nexus_api.RaftReplicator {
	repl_store := sync.NewDKVReplStore(kvs)
	if nexus_repl, err := nexus_api.NewRaftReplicator(repl_store, nexus.OptionsFromFlags()...); err != nil {
		panic(err)
	} else {
		nexus_repl.Start()
		return nexus_repl
	}
}
