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

const (
	cacheSize = 3 << 30
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

func newGRPCServer(dkvSvc api.DKVService) *grpc.Server {
	grpc_srvr := grpc.NewServer()
	serverpb.RegisterDKVServer(grpc_srvr, dkvSvc)
	return grpc_srvr
}

func newListener(port uint) net.Listener {
	if lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port)); err != nil {
		panic(fmt.Sprintf("failed to listen: %v", err))
	} else {
		return lis
	}
}

func main() {
	flag.Parse()
	printFlags()

	dkv_svc := api.NewDistributedDKVService(newReplicator(newKVStore()))
	grpc_srvr, lstnr := newGRPCServer(dkv_svc), newListener(dkvSvcPort)
	go func() { grpc_srvr.Serve(lstnr) }()
	sig := <-setupSignalHandler()
	fmt.Printf("[WARN] Caught signal: %v. Shutting down...\n", sig)
	dkv_svc.Close()
	grpc_srvr.GracefulStop()
}

func setupSignalHandler() <-chan os.Signal {
	signals := []os.Signal{syscall.SIGINT, syscall.SIGQUIT, syscall.SIGSTOP, syscall.SIGTERM}
	stopChan := make(chan os.Signal, len(signals))
	signal.Notify(stopChan, signals...)
	return stopChan
}

func printFlags() {
	fmt.Println("Launching DKV server with following flags:")
	flag.VisitAll(func(f *flag.Flag) {
		if !strings.HasPrefix(f.Name, "test.") {
			fmt.Printf("%s (%s): %v\n", f.Name, f.Usage, f.Value)
		}
	})
	fmt.Println()
}

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

func newReplicator(kvs storage.KVStore) nexus_api.RaftReplicator {
	if repl_store, err := sync.NewDKVReplStore(kvs); err != nil {
		panic(err)
	} else {
		if nexus_repl, err := nexus_api.NewRaftReplicator(repl_store, nexus.OptionsFromFlags()...); err != nil {
			panic(err)
		} else {
			return nexus_repl
		}
	}
}
