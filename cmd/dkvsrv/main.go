package main

import (
	"log"

	"github.com/flipkart-incubator/dkv/internal/server/api"
	"github.com/flipkart-incubator/dkv/internal/server/storage/rocksdb"
)

func main() {
	opts := rocksdb.NewDefaultOptions()
	opts.CreateDBFolderIfMissing(true).DBFolder("/tmp/dkv/").CacheSize(3 << 30)
	if kvs, err := rocksdb.OpenStore(opts); err != nil {
		panic(err)
	} else {
		port := 8080
		svc := api.NewDKVService(port, kvs)
		log.Printf("Listening on port %d...", port)
		svc.Serve()
	}
}
