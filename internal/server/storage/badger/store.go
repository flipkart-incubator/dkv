package badger

import (
	"encoding/binary"

	"github.com/dgraph-io/badger"
	"github.com/flipkart-incubator/dkv/internal/server/storage"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
)

// DB interface represents the capabilities exposed
// by the underlying implmentation based on Badger engine.
type DB interface {
	storage.KVStore
	storage.ChangeApplier
}

type badgerDB struct {
	db *badger.DB
}

type badgerDBOpts struct {
	opts badger.Options
}

// OpenDB initializes a new instance of BadgerDB with default
// options. It uses the given folder for storing the data files.
func OpenDB(dbFolder string) DB {
	opts := newDefaultOptions(dbFolder)
	if kvs, err := openStore(opts); err != nil {
		panic(err)
	} else {
		return kvs
	}
}

func newDefaultOptions(dbFolder string) *badgerDBOpts {
	opts := badger.DefaultOptions(dbFolder).WithSyncWrites(true).WithLogger(nil)
	return &badgerDBOpts{opts: opts}
}

func (bdb *badgerDBOpts) ValueFolder(folder string) *badgerDBOpts {
	bdb.opts.WithValueDir(folder)
	return bdb
}

func openStore(badgerDBOpts *badgerDBOpts) (*badgerDB, error) {
	db, err := badger.Open(badgerDBOpts.opts)
	if err == nil {
		return &badgerDB{db: db}, nil
	}
	return nil, err
}

func (bdb *badgerDB) Close() error {
	return bdb.db.Close()
}

func (bdb *badgerDB) Put(key []byte, value []byte) error {
	return bdb.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

func (bdb *badgerDB) Get(keys ...[]byte) ([][]byte, error) {
	var results [][]byte
	err := bdb.db.View(func(txn *badger.Txn) error {
		for _, key := range keys {
			item, err := txn.Get(key)
			if err != nil {
				return err
			}
			value, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			results = append(results, value)
		}
		return nil
	})
	return results, err
}

const changeNumberKey = "_dkv_meta::ChangeNumber"

func (bdb *badgerDB) GetLatestAppliedChangeNumber() (uint64, error) {
	var chngNum uint64
	err := bdb.db.View(func(txn *badger.Txn) error {
		chngNumVal, err := txn.Get([]byte(changeNumberKey))
		switch {
		case err == badger.ErrKeyNotFound:
			chngNum = 0
		case err != nil:
			return err
		default:
			if err := chngNumVal.Value(func(v []byte) error {
				chngNum = binary.BigEndian.Uint64(v)
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	return chngNum, err
}

func (bdb *badgerDB) SaveChanges(changes []*serverpb.ChangeRecord) (uint64, error) {
	var appldChngNum uint64
	var lastErr error

	for _, chng := range changes {
		// Create a new badger transaction for the current change
		chngTrxn := bdb.db.NewTransaction(true)
		defer chngTrxn.Discard()

		// Load the current change number
		chngNumVal, err := chngTrxn.Get([]byte(changeNumberKey))
		var currChngNum uint64
		switch {
		case err == badger.ErrKeyNotFound:
			currChngNum = 0
		case err != nil:
			lastErr = err
		default:
			if err := chngNumVal.Value(func(v []byte) error {
				currChngNum = binary.BigEndian.Uint64(v)
				return nil
			}); err != nil {
				lastErr = err
			}
		}
		if lastErr != nil {
			break
		}

		// Loop through every transaction record of the current change and
		// apply the operation to the current badger transaction
		for _, trxnRec := range chng.Trxns {
			switch trxnRec.Type {
			case serverpb.TrxnRecord_Put:
				if lastErr = chngTrxn.Set(trxnRec.Key, trxnRec.Value); lastErr != nil {
					break
				}
			case serverpb.TrxnRecord_Delete:
				if lastErr = chngTrxn.Delete(trxnRec.Key); lastErr != nil {
					break
				}
			}
		}
		if lastErr != nil {
			break
		}

		// Increment and set the change number in the same badger transaction
		currChngNum = currChngNum + 1
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], currChngNum)
		if lastErr = chngTrxn.Set([]byte(changeNumberKey), buf[:]); lastErr != nil {
			break
		}

		// Commit the badger transaction for the current change
		if lastErr = chngTrxn.Commit(); lastErr != nil {
			break
		} else {
			appldChngNum = chng.ChangeNumber
		}
	}
	return appldChngNum, lastErr
}
