package badger

import (
	"github.com/dgraph-io/badger"
	"github.com/flipkart-incubator/dkv/internal/server/storage"
)

type BadgerDBStore struct {
	db *badger.DB
}

type BadgerDBOptions struct {
	opts badger.Options
}

func OpenDB(dbFolder string) storage.KVStore {
	opts := NewDefaultOptions(dbFolder)
	if kvs, err := OpenStore(opts); err != nil {
		panic(err)
	} else {
		return kvs
	}
}

func NewDefaultOptions(dbFolder string) *BadgerDBOptions {
	opts := badger.DefaultOptions(dbFolder).WithSyncWrites(true).WithLogger(nil)
	return &BadgerDBOptions{opts: opts}
}

func (this *BadgerDBOptions) ValueFolder(folder string) *BadgerDBOptions {
	this.opts.WithValueDir(folder)
	return this
}

func OpenStore(badgerDBOpts *BadgerDBOptions) (*BadgerDBStore, error) {
	if db, err := badger.Open(badgerDBOpts.opts); err != nil {
		return nil, err
	} else {
		return &BadgerDBStore{db}, nil
	}
}

func (this *BadgerDBStore) Put(key []byte, value []byte) *storage.Result {
	err := this.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
	return &storage.Result{err}
}

func (this *BadgerDBStore) Get(keys ...[]byte) []*storage.ReadResult {
	var results []*storage.ReadResult
	err := this.db.View(func(txn *badger.Txn) error {
		for _, key := range keys {
			if item, err := txn.Get(key); err != nil {
				results = append(results, storage.NewReadResultWithError(err))
			} else {
				if value, err := item.ValueCopy(nil); err != nil {
					results = append(results, storage.NewReadResultWithError(err))
				} else {
					results = append(results, storage.NewReadResultWithValue(value))
				}
			}
		}
		return nil
	})
	if err != nil {
		results = append(results, storage.NewReadResultWithError(err))
	}
	return results
}
