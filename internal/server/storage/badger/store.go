package badger

import (
	"bufio"
	"encoding/binary"
	"errors"
	"os"
	"path"
	"strings"
	"sync/atomic"

	"github.com/dgraph-io/badger"
	"github.com/flipkart-incubator/dkv/internal/server/storage"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
)

// DB interface represents the capabilities exposed
// by the underlying implmentation based on Badger engine.
type DB interface {
	storage.KVStore
	storage.Backupable
	storage.ChangeApplier
}

type badgerDB struct {
	db   *badger.DB
	opts *Opts

	// Indicates a global mutation like backup and restore that
	// require exclusivity. Shall be manipulated using atomics.
	globalMutation uint32
}

// Opts holds the various options required for configuring
// the Badger storage engine.
type Opts struct {
	opts badger.Options
}

// OpenDB initializes a new instance of BadgerDB with default
// options. It uses the given folder for storing the data files.
func OpenDB(dbFolder string) DB {
	opts := NewOptions(dbFolder)
	if kvs, err := openStore(opts); err != nil {
		panic(err)
	} else {
		return kvs
	}
}

// NewOptions initializes an instance of BadgerDB options with
// default settings. It can be used to customize specific parameters
// of the underlying Badger storage engine.
func NewOptions(dbFolder string) *Opts {
	opts := badger.DefaultOptions(dbFolder).WithSyncWrites(true).WithLogger(nil)
	return &Opts{opts}
}

func openStore(bdbOpts *Opts) (*badgerDB, error) {
	db, err := badger.Open(bdbOpts.opts)
	if err != nil {
		return nil, err
	}
	return &badgerDB{db, bdbOpts, 0}, nil
}

func (bdb *badgerDB) Close() error {
	bdb.db.Close()
	return nil
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

const backupBufSize = 64 << 20

func (bdb *badgerDB) BackupTo(file string) error {
	if err := checksForBackup(file); err != nil {
		return err
	}
	// Prevent any other backups or restores
	if err := bdb.beginGlobalMutation(); err != nil {
		return err
	}
	defer bdb.endGlobalMutation()

	bf, err := os.Create(path.Clean(file))
	if err != nil {
		return err
	}

	defer bf.Close()
	bw := bufio.NewWriterSize(bf, backupBufSize)
	if _, err = bdb.db.Backup(bw, 0); err != nil {
		return err
	}

	if err = bw.Flush(); err != nil {
		return err
	}

	return bf.Sync()
}

const (
	tempDirPrefx     = "badger-restore-"
	tempDirValPrefx  = "badger-restore-val-"
	maxPendingWrites = 256
)

func (bdb *badgerDB) RestoreFrom(file string) (err error) {
	// 1. Prevent any other backups or restores
	err = bdb.beginGlobalMutation()
	if err != nil {
		return err
	}
	defer bdb.endGlobalMutation()

	// 2. Close the current DB to prevent further mutations
	bdb.db.Close()

	// 3. In any case, reopen the current DB
	defer func() {
		if finalDB, openErr := openStore(bdb.opts); openErr != nil {
			err = openErr
		} else {
			*bdb = *finalDB
		}
	}()

	// 4. Check for the given restore file validity
	err = checksForRestore(file)
	if err != nil {
		return err
	}

	// 5. Open the given restore file
	f, err := os.Open(file)
	if err != nil {
		return err
	}
	defer f.Close()

	// 6. Create temp folder for the restored data
	restoreFolder, err := storage.CreateTempFolder(tempDirPrefx)
	if err != nil {
		return err
	}

	// 7. Create a temp badger DB pointing to the temp folder
	restoredDB, err := openStore(NewOptions(restoreFolder))
	if err != nil {
		return err
	}

	// 8. Restore data in the file onto the temp badger DB
	err = restoredDB.db.Load(f, maxPendingWrites)
	if err != nil {
		return err
	}

	// 9. Close the temp badger DB
	restoredDB.db.Close()

	// 10. Move the temp folders to the actual locations
	err = storage.RenameFolder(restoreFolder, bdb.opts.opts.Dir)

	// Plain return due to defer function above
	return
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

var globalMutationError = errors.New("Another global keyspace mutation is in progress")

func (bdb *badgerDB) hasGlobalMutation() bool {
	return atomic.LoadUint32(&bdb.globalMutation) == 1
}

func (bdb *badgerDB) beginGlobalMutation() error {
	if atomic.CompareAndSwapUint32(&bdb.globalMutation, 0, 1) {
		return nil
	}
	return globalMutationError
}

func (bdb *badgerDB) endGlobalMutation() error {
	if atomic.CompareAndSwapUint32(&bdb.globalMutation, 1, 0) {
		return nil
	}
	return globalMutationError
}

func checksForBackup(bckpPath string) error {
	if len(strings.TrimSpace(bckpPath)) == 0 {
		return errors.New("valid path must be provided")
	}

	_, err := os.Stat(bckpPath)
	if err == nil {
		return errors.New("require a new file for badger backup")
	}
	_, err = os.Stat(path.Dir(bckpPath))
	return err
}

func checksForRestore(rstrPath string) error {
	switch fi, err := os.Stat(rstrPath); {
	case err != nil:
		return err
	case fi.IsDir():
		return errors.New("require a file for badger restore")
	default:
		return nil
	}
}
