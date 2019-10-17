package main

import (
	"flag"
	"fmt"
	"strings"

	"github.com/flipkart-incubator/dkv/internal/server/api"
	"github.com/flipkart-incubator/dkv/internal/server/storage"
	"github.com/flipkart-incubator/dkv/internal/server/storage/badger"
	"github.com/flipkart-incubator/dkv/internal/server/storage/rocksdb"
)

const (
	createDBFolderIfMissing = true
	cacheSize               = 3 << 30
)

var (
	engine     string
	dbFolder   string
	dkvSvcPort uint
)

func init() {
	flag.StringVar(&engine, "storage", "rocksdb", "Storage engine to use - badger|rocksdb")
	flag.StringVar(&dbFolder, "dbFolder", "/tmp/dkvsrv", "DB folder path")
	flag.UintVar(&dkvSvcPort, "dkvSvcPort", 8080, "DKV service port")
}

func main() {
	flag.Parse()
	printFlags()
	serveDKV()
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

func serveDKV() {
	var kvs storage.KVStore
	switch engine {
	case "rocksdb":
		kvs = serveRocksDBDKV()
	case "badger":
		kvs = serveBadgerDKV()
	default:
		panic(fmt.Sprintf("Unknown storage engine: %s", engine))
	}
	svc := api.NewDKVService(dkvSvcPort, kvs)
	svc.Serve()
}

func serveBadgerDKV() storage.KVStore {
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
