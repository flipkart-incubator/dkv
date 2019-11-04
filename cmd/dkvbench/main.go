package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/bojand/ghz/printer"
	"github.com/bojand/ghz/runner"
	"github.com/flipkart-incubator/dkv/internal/ctl"
	"github.com/flipkart-incubator/dkv/tools/bench"
)

const (
	createDBFolderIfMissing = true
	cacheSize               = 3 << 30
	redisPort               = 6379
	redisDBIndex            = 3
)

var (
	dkvCli       *ctl.DKVClient
	parallelism  uint
	totalNumKeys uint
	dkvSvcPort   uint
	dkvSvcHost   string
)

func init() {
	flag.StringVar(&dkvSvcHost, "dkvSvcHost", "localhost", "DKV service host")
	flag.UintVar(&dkvSvcPort, "dkvSvcPort", 8080, "DKV service port")
	flag.UintVar(&parallelism, "parallelism", 2, "Number of requests to run concurrently")
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

	launchBenchmark(bench.DefaultPutNewKeysBenchmark())
	launchBenchmark(bench.DefaultPutModifyKeysBenchmark())
	launchBenchmark(bench.DefaultGetHotKeysBenchmark())
	launchBenchmark(bench.DefaultMultiGetHotKeysBenchmark())
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
