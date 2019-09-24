package main

import (
	"log"

	"github.com/flipkart-incubator/dkv/internal/server/api"
	"github.com/flipkart-incubator/dkv/internal/server/storage"
)

func main() {
	opts := storage.NewDefaultRocksDBOptions()
	opts.CreateDBFolderIfMissing(true).DBFolder("/tmp/dkv/").CacheSize(3 << 30)
	if kvs, err := storage.OpenRocksDBStore(opts); err != nil {
		panic(err)
	} else {
		port := 8080
		svc := api.NewDKVService(port, kvs)
		log.Printf("Listening on port %d...", port)
		svc.Serve()
	}
}
