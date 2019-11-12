package rocksdb

import (
	"github.com/flipkart-incubator/dkv/internal/server/storage"
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

func OpenDB(dbFolder string, cacheSize uint64) storage.KVStore {
	opts := NewDefaultOptions()
	opts.CreateDBFolderIfMissing(true).DBFolder(dbFolder).CacheSize(cacheSize)
	if kvs, err := OpenStore(opts); err != nil {
		panic(err)
	} else {
		return kvs
	}
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

func (rdb *RocksDBStore) Close() error {
	rdb.db.Close()
	return nil
}

func (rdb *RocksDBStore) Put(key []byte, value []byte) *storage.Result {
	wo := gorocksdb.NewDefaultWriteOptions()
	wo.SetSync(true)
	defer wo.Destroy()
	err := rdb.db.Put(wo, key, value)
	return &storage.Result{err}
}

func toReadResult(value *gorocksdb.Slice) *storage.ReadResult {
	src := value.Data()
	var res []byte
	for i := 0; i < value.Size(); i++ {
		res = append(res, src[i])
	}
	return storage.NewReadResultWithValue(res)
}

func (rdb *RocksDBStore) getSingleKey(ro *gorocksdb.ReadOptions, key []byte) *storage.ReadResult {
	if value, err := rdb.db.Get(ro, key); err != nil {
		return storage.NewReadResultWithError(err)
	} else {
		return toReadResult(value)
	}
}

func (rdb *RocksDBStore) getMultipleKeys(ro *gorocksdb.ReadOptions, keys [][]byte) []*storage.ReadResult {
	var results []*storage.ReadResult
	if values, err := rdb.db.MultiGet(ro, keys...); err != nil {
		results = append(results, storage.NewReadResultWithError(err))
	} else {
		for _, value := range values {
			results = append(results, toReadResult(value))
			value.Free()
		}
	}
	return results
}

func (rdb *RocksDBStore) Get(keys ...[]byte) []*storage.ReadResult {
	ro := gorocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	numKeys := len(keys)
	var results []*storage.ReadResult
	switch {
	case numKeys == 1:
		results = append(results, rdb.getSingleKey(ro, keys[0]))
	case numKeys > 1:
		results = rdb.getMultipleKeys(ro, keys)
	default:
		results = nil
	}
	return results
}
