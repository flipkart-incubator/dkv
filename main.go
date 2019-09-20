package main

import (
	"fmt"

	"github.com/flipkart-incubator/dkv/internal/server/storage"
)

func main() {
	fmt.Print("Opening DB...")
	opts := storage.NewDefaultOptions()
	opts.CreateDBFolderIfMissing(true).DBFolder("/tmp/dkv/").CacheSize(3 << 30)
	if kvs, err := storage.OpenKVStore(opts); err != nil {
		panic(err)
	} else {
		fmt.Println("DONE")
		key, value := []byte("hello"), []byte("world")
		if err := kvs.Put(key, value); err != nil {
			panic(err)
		} else {
			if val, err := kvs.Get(key); err != nil {
				panic(err)
			} else {
				fmt.Println(string(val))
			}
		}
	}
}
