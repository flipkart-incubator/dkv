package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/bojand/ghz/printer"
	"github.com/bojand/ghz/runner"
	"github.com/flipkart-incubator/dkv/internal/ctl"
	"github.com/flipkart-incubator/dkv/internal/server/api"
	"github.com/flipkart-incubator/dkv/internal/server/storage"
	"github.com/flipkart-incubator/dkv/internal/server/storage/badger"
	"github.com/flipkart-incubator/dkv/internal/server/storage/rocksdb"
	"github.com/flipkart-incubator/dkv/tools/bench"
)

const (
	createDBFolderIfMissing = true
	cacheSize               = 3 << 30
)

var (
	dkvCli       *ctl.DKVClient
	parallelism  uint
	totalNumKeys uint
	engine       string
	dbFolder     string
	dkvSvcPort   uint
	dkvSvcHost   string
)

func init() {
	flag.StringVar(&dbFolder, "dbFolder", "/tmp/dkvbench", "DB folder path")
	flag.StringVar(&dkvSvcHost, "dkvSvcHost", "localhost", "DKV service host")
	flag.UintVar(&dkvSvcPort, "dkvSvcPort", 8080, "DKV service port")
	flag.StringVar(&engine, "storage", "rocksdb", "Storage engine to use")
	flag.UintVar(&parallelism, "parallelism", 2, "Number of parallel entities per core")
	flag.UintVar(&totalNumKeys, "totalNumKeys", 1000, "Total number of keys")
}

func launchBenchmark(bm bench.Benchmark) {
	report, err := runner.Run(
		bm.ApiName(),
		fmt.Sprintf("%s:%d", dkvSvcHost, dkvSvcPort),
		runner.WithProtoFile("./pkg/serverpb/api.proto", []string{}),
		runner.WithData(bm.CreateRequests(totalNumKeys)),
		runner.WithInsecure(true),
		runner.WithCPUs(8),
		runner.WithConcurrency(parallelism),
		runner.WithConnections(1),
		runner.WithTotalRequests(totalNumKeys),
	)

	if err != nil {
		panic(err)
	}

	printer := printer.ReportPrinter{
		Out:    os.Stdout,
		Report: report,
	}

	printer.Print("summary")
}

func main() {
	flag.Parse()
	printFlags()
	go serveDKV()
	sleepInSecs(3)

	launchBenchmark(&bench.PutNewKeysBenchmark{})
	launchBenchmark(&bench.PutModifyKeysBenchmark{})
	launchBenchmark(&bench.GetHotKeysBenchmark{})
}

func printFlags() {
	fmt.Println("Launching benchmarks with following flags:")
	flag.VisitAll(func(f *flag.Flag) {
		if !strings.HasPrefix(f.Name, "test.") {
			fmt.Printf("%s (%s): %v\n", f.Name, f.Usage, f.Value)
		}
	})
	fmt.Println()
}

func serveDKV() {
	if err := exec.Command("rm", "-rf", dbFolder).Run(); err != nil {
		panic(err)
	}
	var kvs storage.KVStore
	switch engine {
	case "rocksdb":
		kvs = serveRocksDBDKV()
	case "badger":
		kvs = serverBadgerDKV()
	default:
		panic(fmt.Sprintf("Unknown storage engine: %s", engine))
	}
	svc := api.NewDKVService(dkvSvcPort, kvs)
	svc.Serve()
}

func serverBadgerDKV() storage.KVStore {
	opts := badger.NewDefaultOptions(dbFolder)
	if kvs, err := badger.OpenStore(opts); err != nil {
		panic(err)
	} else {
		return kvs
	}
}

func serveRocksDBDKV() storage.KVStore {
	opts := rocksdb.NewDefaultOptions()
	opts.CreateDBFolderIfMissing(createDBFolderIfMissing).DBFolder(dbFolder).CacheSize(cacheSize)
	if kvs, err := rocksdb.OpenStore(opts); err != nil {
		panic(err)
	} else {
		return kvs
	}
}

func sleepInSecs(duration int) {
	<-time.After(time.Duration(duration) * time.Second)
}
