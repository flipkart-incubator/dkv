package main

import "fmt"
import "github.com/tecbot/gorocksdb"

func main() {
	fmt.Print("Opening DB...")
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(gorocksdb.NewLRUCache(3 << 30))
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)
	if db, err := gorocksdb.OpenDb(opts, "/tmp/dkv"); err != nil {
		panic(err)
	} else {
		fmt.Println("DONE")
		ro := gorocksdb.NewDefaultReadOptions()
		wo := gorocksdb.NewDefaultWriteOptions()
		if err := db.Put(wo, []byte("foo"), []byte("bar")); err != nil {
			panic(err)
		}
		if value, err := db.Get(ro, []byte("foo")); err != nil {
			panic(err)
		} else {
			defer value.Free()
			fmt.Println(string(value.Data()))
		}
	}
}
