package storage

import (
	"github.com/tecbot/gorocksdb"
)

type RocksDBStore struct {
	db *gorocksdb.DB
}

type Options struct {
	blockTableOpts *gorocksdb.BlockBasedTableOptions
	rocksDBOpts    *gorocksdb.Options
	folderName     string
}

func NewDefaultOptions() *Options {
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	return &Options{blockTableOpts: bbto, rocksDBOpts: opts}
}

func (this *Options) CacheSize(size uint64) *Options {
	this.blockTableOpts.SetBlockCache(gorocksdb.NewLRUCache(size))
	return this
}

func (this *Options) CreateDBFolderIfMissing(flag bool) *Options {
	this.rocksDBOpts.SetCreateIfMissing(flag)
	return this
}

func (this *Options) DBFolder(name string) *Options {
	this.folderName = name
	return this
}

func OpenKVStore(opts *Options) (*RocksDBStore, error) {
	if db, err := gorocksdb.OpenDb(opts.rocksDBOpts, opts.folderName); err != nil {
		return nil, err
	} else {
		return &RocksDBStore{db}, nil
	}
}

func (rdb *RocksDBStore) Put(key []byte, value []byte) error {
	wo := gorocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	return rdb.db.Put(wo, key, value)
}

func (rdb *RocksDBStore) Get(key []byte) ([]byte, error) {
	ro := gorocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	if value, err := rdb.db.Get(ro, key); err != nil {
		return nil, err
	} else {
		src := value.Data()
		var res []byte
		for i := 0; i < value.Size(); i++ {
			res = append(res, src[i])
		}
		value.Free()
		return res, nil
	}
}
