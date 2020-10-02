package badger

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"os"
	"path"
	"strings"
	"sync/atomic"

	"github.com/dgraph-io/badger"
	badger_pb "github.com/dgraph-io/badger/pb"
	"github.com/flipkart-incubator/dkv/internal/server/stats"
	"github.com/flipkart-incubator/dkv/internal/server/storage"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
	"go.uber.org/zap"
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
	opts *bdgrOpts

	// Indicates a global mutation like backup and restore that
	// require exclusivity. Shall be manipulated using atomics.
	globalMutation uint32
}

type bdgrOpts struct {
	opts     badger.Options
	lgr      *zap.Logger
	statsCli stats.Client
}

// DBOption is used to configure the Badger
// storage engine.
type DBOption func(*bdgrOpts)

// WithLogger is used to inject a ZAP logger instance.
func WithLogger(lgr *zap.Logger) DBOption {
	return func(opts *bdgrOpts) {
		if lgr != nil {
			opts.lgr = lgr
		} else {
			opts.lgr = zap.NewNop()
		}
	}
}

// WithStats is used to inject a metrics client.
func WithStats(statsCli stats.Client) DBOption {
	return func(opts *bdgrOpts) {
		if statsCli != nil {
			opts.statsCli = statsCli
		} else {
			opts.statsCli = stats.NewNoOpClient()
		}
	}
}

// WithSyncWrites configures Badger to ensure every
// write is flushed to disk before acking back.
func WithSyncWrites() DBOption {
	return func(opts *bdgrOpts) {
		opts.opts.WithSyncWrites(true)
	}
}

// WithoutSyncWrites configures Badger to prevent
// flush to disk for every write.
func WithoutSyncWrites() DBOption {
	return func(opts *bdgrOpts) {
		opts.opts.WithSyncWrites(false)
	}
}

// WithoutDBInternalLogging configures Badger to
// prevent any internal logging to occur.
func WithoutDBInternalLogging() DBOption {
	return func(opts *bdgrOpts) {
		opts.opts.WithLogger(nil)
	}
}

// OpenDB initializes a new instance of BadgerDB with the specified
// options. It uses the given folder for storing the data files.
func OpenDB(dbFolder string, dbOpts ...DBOption) (kvs DB, err error) {
	bdgrDBOpts := badger.DefaultOptions(dbFolder)
	opts := &bdgrOpts{opts: bdgrDBOpts}
	for _, dbOpt := range dbOpts {
		dbOpt(opts)
	}
	return openStore(opts)
}

func openStore(bdbOpts *bdgrOpts) (*badgerDB, error) {
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

func (bdb *badgerDB) GetSnapshot() ([]byte, error) {
	// TODO: Check if any options need to be set on stream
	strm := bdb.db.NewStream()
	snap := make(map[string][]byte)
	strm.Send = func(list *badger_pb.KVList) error {
		for _, kv := range list.Kv {
			snap[string(kv.Key)] = kv.Value
		}
		return nil
	}
	if err := strm.Orchestrate(context.Background()); err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(snap)
	return buf.Bytes(), err
}

func (bdb *badgerDB) PutSnapshot(snap []byte) error {
	buf := bytes.NewBuffer(snap)
	data := make(map[string][]byte)
	if err := gob.NewDecoder(buf).Decode(&data); err != nil {
		return err
	}

	return bdb.db.Update(func(txn *badger.Txn) error {
		for key, val := range data {
			if err := txn.Set([]byte(key), val); err != nil {
				return err
			}
		}
		return nil
	})
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

func (bdb *badgerDB) RestoreFrom(file string) (st storage.KVStore, ba storage.Backupable, cp storage.ChangePropagator, ca storage.ChangeApplier, err error) {
	// Setup return vars
	st, ba, cp, ca = bdb, bdb, nil, bdb

	// Prevent any other backups or restores
	err = bdb.beginGlobalMutation()
	if err != nil {
		return
	}
	defer bdb.endGlobalMutation()

	// In any case, reopen a new DB
	defer func() {
		if finalDB, openErr := openStore(bdb.opts); openErr != nil {
			err = openErr
		} else {
			st, ba, cp, ca = finalDB, finalDB, nil, finalDB
		}
	}()

	// Check for the given restore file validity
	err = checksForRestore(file)
	if err != nil {
		return
	}

	// Open the given restore file
	f, err := os.Open(file)
	if err != nil {
		return
	}
	defer f.Close()

	// Create temp folder for the restored data
	restoreDir, err := storage.CreateTempFolder(tempDirPrefx)
	if err != nil {
		return
	}

	// Create a temp badger DB pointing to the temp folder
	cloneOpts := *bdb.opts
	cloneOpts.opts = cloneOpts.opts.WithDir(restoreDir).WithValueDir(restoreDir)
	restoredDB, err := openStore(&cloneOpts)
	if err != nil {
		return
	}

	// Restore data in the file onto the temp badger DB
	err = restoredDB.db.Load(f, maxPendingWrites)
	if err != nil {
		return
	}

	// Close the temp badger DB
	restoredDB.db.Close()

	// Move the temp folders to the actual locations
	err = storage.RenameFolder(restoreDir, bdb.opts.opts.Dir)

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

type iter struct {
	itOpts  storage.IterationOptions
	txn     *badger.Txn
	it      *badger.Iterator
	iterErr error
}

func (bdbIter *iter) HasNext() bool {
	if kp, prsnt := bdbIter.itOpts.KeyPrefix(); prsnt {
		if bdbIter.it.ValidForPrefix(kp) {
			return true
		}
		if bdbIter.it.Valid() {
			bdbIter.it.Next()
			return bdbIter.HasNext()
		}
		return false
	}
	return bdbIter.it.Valid()
}

func (bdbIter *iter) Next() ([]byte, []byte) {
	defer bdbIter.it.Next()
	item := bdbIter.it.Item()
	key := item.KeyCopy(nil)
	val, err := item.ValueCopy(nil)
	if err != nil {
		bdbIter.iterErr = err
	}
	return key, val
}

func (bdbIter *iter) Err() error {
	return bdbIter.iterErr
}

func (bdbIter *iter) Close() error {
	bdbIter.it.Close()
	bdbIter.txn.Discard()
	return nil
}

func (bdb *badgerDB) newIter(itOpts storage.IterationOptions) *iter {
	txn := bdb.db.NewTransaction(false)
	it := txn.NewIterator(badger.DefaultIteratorOptions)

	if sk, prsnt := itOpts.StartKey(); prsnt {
		it.Seek(sk)
	} else {
		it.Rewind()
	}
	return &iter{itOpts, txn, it, nil}
}

func (bdb *badgerDB) Iterate(iterOpts storage.IterationOptions) storage.Iterator {
	return bdb.newIter(iterOpts)
}

var errGlobalMutation = errors.New("Another global keyspace mutation is in progress")

func (bdb *badgerDB) hasGlobalMutation() bool {
	return atomic.LoadUint32(&bdb.globalMutation) == 1
}

func (bdb *badgerDB) beginGlobalMutation() error {
	if atomic.CompareAndSwapUint32(&bdb.globalMutation, 0, 1) {
		return nil
	}
	return errGlobalMutation
}

func (bdb *badgerDB) endGlobalMutation() error {
	if atomic.CompareAndSwapUint32(&bdb.globalMutation, 1, 0) {
		return nil
	}
	return errGlobalMutation
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
