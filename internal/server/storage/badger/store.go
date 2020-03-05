package badger

import (
	"encoding/binary"

	"github.com/dgraph-io/badger"
	"github.com/flipkart-incubator/dkv/internal/server/storage"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
)

type badgerDBStore struct {
	storage.KVStore
	storage.ChangeApplier
	db *badger.DB
}

type badgerDBOpts struct {
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

func NewDefaultOptions(dbFolder string) *badgerDBOpts {
	opts := badger.DefaultOptions(dbFolder).WithSyncWrites(true).WithLogger(nil)
	return &badgerDBOpts{opts: opts}
}

func (bdb *badgerDBOpts) ValueFolder(folder string) *badgerDBOpts {
	bdb.opts.WithValueDir(folder)
	return bdb
}

func OpenStore(badgerDBOpts *badgerDBOpts) (*badgerDBStore, error) {
	if db, err := badger.Open(badgerDBOpts.opts); err != nil {
		return nil, err
	} else {
		return &badgerDBStore{db: db}, nil
	}
}

func (bdb *badgerDBStore) Close() error {
	return bdb.db.Close()
}

func (bdb *badgerDBStore) Put(key []byte, value []byte) error {
	return bdb.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

func (bdb *badgerDBStore) Get(keys ...[]byte) ([][]byte, error) {
	var results [][]byte
	err := bdb.db.View(func(txn *badger.Txn) error {
		for _, key := range keys {
			if item, err := txn.Get(key); err != nil {
				return err
			} else {
				if value, err := item.ValueCopy(nil); err != nil {
					return err
				} else {
					results = append(results, value)
				}
			}
		}
		return nil
	})
	return results, err
}

const ChangeNumberKey = "_dkv_meta::ChangeNumber"

func (bdb *badgerDBStore) GetLatestAppliedChangeNumber() (uint64, error) {
	var chng_num uint64
	err := bdb.db.View(func(txn *badger.Txn) error {
		chng_num_val, err := txn.Get([]byte(ChangeNumberKey))
		switch {
		case err == badger.ErrKeyNotFound:
			chng_num = 0
		case err != nil:
			return err
		default:
			if err := chng_num_val.Value(func(v []byte) error {
				chng_num = binary.BigEndian.Uint64(v)
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	return chng_num, err
}

func (bdb *badgerDBStore) SaveChanges(changes []*serverpb.ChangeRecord) (uint64, error) {
	var appld_chng_num uint64
	var last_error error

	for _, chng := range changes {
		// Create a new badger transaction for the current change
		chng_trxn := bdb.db.NewTransaction(true)
		defer chng_trxn.Discard()

		// Load the current change number
		chng_num_val, err := chng_trxn.Get([]byte(ChangeNumberKey))
		var curr_chng_num uint64
		switch {
		case err == badger.ErrKeyNotFound:
			curr_chng_num = 0
		case err != nil:
			last_error = err
		default:
			if err := chng_num_val.Value(func(v []byte) error {
				curr_chng_num = binary.BigEndian.Uint64(v)
				return nil
			}); err != nil {
				last_error = err
			}
		}
		if last_error != nil {
			break
		}

		// Loop through every transaction record of the current change and
		// apply the operation to the current badger transaction
		for _, trxn_rec := range chng.Trxns {
			switch trxn_rec.Type {
			case serverpb.TrxnRecord_Put:
				if last_error = chng_trxn.Set(trxn_rec.Key, trxn_rec.Value); last_error != nil {
					break
				}
			case serverpb.TrxnRecord_Delete:
				if last_error = chng_trxn.Delete(trxn_rec.Key); last_error != nil {
					break
				}
			}
		}
		if last_error != nil {
			break
		}

		// Increment and set the change number in the same badger transaction
		curr_chng_num = curr_chng_num + 1
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], curr_chng_num)
		if last_error = chng_trxn.Set([]byte(ChangeNumberKey), buf[:]); last_error != nil {
			break
		}

		// Commit the badger transaction
		if last_error = chng_trxn.Commit(); last_error != nil {
			break
		} else {
			appld_chng_num = chng.ChangeNumber
		}
	}
	return appld_chng_num, last_error
}
