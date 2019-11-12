package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/flipkart-incubator/dkv/internal/server/api"
	"github.com/flipkart-incubator/dkv/internal/server/storage"
	"github.com/flipkart-incubator/dkv/internal/server/storage/badger"
	"github.com/flipkart-incubator/dkv/internal/server/storage/redis"
	"github.com/flipkart-incubator/dkv/internal/server/storage/rocksdb"
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

func main() {
	flag.Parse()
	printFlags()

	dkvSvc := newDKVService()
	grpcSrvr, lstnr := dkvSvc.NewGRPCServer(), dkvSvc.NewListener()
	go func() { grpcSrvr.Serve(lstnr) }()
	sig := <-setupSignalHandler(dkvSvc, grpcSrvr)
	fmt.Printf("[WARN] Caught signal: %v. Shutting down...\n", sig)
	dkvSvc.Close()
	grpcSrvr.GracefulStop()
}

func setupSignalHandler(dkvSvc *api.DKVService, grpcSrvr *grpc.Server) <-chan os.Signal {
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

func newDKVService() *api.DKVService {
	var kvs storage.KVStore
	switch engine {
	case "rocksdb":
		kvs = rocksdb.OpenDB(dbFolder, cacheSize)
	case "badger":
		kvs = badger.OpenDB(dbFolder)
	case "redis":
		kvs = redis.OpenDB(redisPort, redisDBIndex)
	default:
		panic(fmt.Sprintf("Unknown storage engine: %s", engine))
	}
	return api.NewDKVService(dkvSvcPort, kvs)
}
