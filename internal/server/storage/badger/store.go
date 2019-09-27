package badger

import (
	"github.com/dgraph-io/badger"
)

type BadgerDBStore struct {
	db *badger.DB
}

type BadgerDBOptions struct {
	opts badger.Options
}

func NewDefaultOptions(dbFolder string) *BadgerDBOptions {
	opts := badger.DefaultOptions(dbFolder).WithSyncWrites(false).WithLogger(nil)
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

func (this *BadgerDBStore) Put(key []byte, value []byte) error {
	return this.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

func (this *BadgerDBStore) Get(key []byte) ([]byte, error) {
	var value []byte
	err := this.db.View(func(txn *badger.Txn) error {
		if item, err := txn.Get(key); err == nil {
			value, _ = item.ValueCopy(nil)
		}
		return nil
	})
	return value, err
}
