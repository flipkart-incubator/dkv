package rocksdb

import (
	"github.com/tecbot/gorocksdb"
)

type RocksDBStore struct {
	db *gorocksdb.DB
}

type RocksDBOptions struct {
	blockTableOpts *gorocksdb.BlockBasedTableOptions
	rocksDBOpts    *gorocksdb.Options
	folderName     string
}

func NewDefaultOptions() *RocksDBOptions {
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	return &RocksDBOptions{blockTableOpts: bbto, rocksDBOpts: opts}
}

func (this *RocksDBOptions) CacheSize(size uint64) *RocksDBOptions {
	this.blockTableOpts.SetBlockCache(gorocksdb.NewLRUCache(size))
	return this
}

func (this *RocksDBOptions) CreateDBFolderIfMissing(flag bool) *RocksDBOptions {
	this.rocksDBOpts.SetCreateIfMissing(flag)
	return this
}

func (this *RocksDBOptions) DBFolder(name string) *RocksDBOptions {
	this.folderName = name
	return this
}

func OpenStore(opts *RocksDBOptions) (*RocksDBStore, error) {
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
