package rocksdb

import (
	"errors"
	"sync/atomic"

	"github.com/flipkart-incubator/dkv/internal/server/storage"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
	"github.com/tecbot/gorocksdb"
)

// DB interface represents the capabilities exposed
// by the underlying implmentation based on RocksDB engine.
type DB interface {
	storage.KVStore
	storage.Backupable
	storage.ChangePropagator
	storage.ChangeApplier
}

type rocksDB struct {
	db   *gorocksdb.DB
	opts *Opts

	// Indicates a global mutation like backup and restore that
	// require exclusivity. Shall be manipulated using atomics.
	globalMutation uint32
}

// Opts holds the various options required for configuring
// the RocksDB storage engine.
type Opts struct {
	blockTableOpts *gorocksdb.BlockBasedTableOptions
	rocksDBOpts    *gorocksdb.Options
	restoreOpts    *gorocksdb.RestoreOptions
	folderName     string
}

// OpenDB initializes a new instance of RocksDB with default
// options. It uses the given folder for storing the data files.
func OpenDB(dbFolder string, cacheSize uint64) DB {
	opts := NewOptions()
	opts.CreateDBFolderIfMissing(true).DBFolder(dbFolder).CacheSize(cacheSize)
	if kvs, err := openStore(opts); err != nil {
		panic(err)
	} else {
		return kvs
	}
}

// NewOptions initializes an instance of RocksDB options with
// default settings. It can be used to customize specific parameters
// of the underlying RocksDB storage engine.
func NewOptions() *Opts {
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	rstOpts := gorocksdb.NewRestoreOptions()
	return &Opts{blockTableOpts: bbto, rocksDBOpts: opts, restoreOpts: rstOpts}
}

// CacheSize can be used to set the RocksDB block cache size.
func (rdbOpts *Opts) CacheSize(size uint64) *Opts {
	rdbOpts.blockTableOpts.SetBlockCache(gorocksdb.NewLRUCache(size))
	return rdbOpts
}

// CreateDBFolderIfMissing can be used to direct RocksDB to create
// the database folder if its missing.
func (rdbOpts *Opts) CreateDBFolderIfMissing(flag bool) *Opts {
	rdbOpts.rocksDBOpts.SetCreateIfMissing(flag)
	return rdbOpts
}

// DBFolder allows for the RocksDB database folder location to be set.
func (rdbOpts *Opts) DBFolder(name string) *Opts {
	rdbOpts.folderName = name
	return rdbOpts
}

func (rdbOpts *Opts) destroy() {
	rdbOpts.blockTableOpts.Destroy()
	rdbOpts.rocksDBOpts.Destroy()
	rdbOpts.restoreOpts.Destroy()
}

func openStore(opts *Opts) (*rocksDB, error) {
	db, err := gorocksdb.OpenDb(opts.rocksDBOpts, opts.folderName)
	if err != nil {
		return nil, err
	}
	return &rocksDB{db, opts, 0}, nil
}

func (rdb *rocksDB) Close() error {
	rdb.opts.destroy()
	rdb.db.Close()
	return nil
}

func (rdb *rocksDB) Put(key []byte, value []byte) error {
	wo := gorocksdb.NewDefaultWriteOptions()
	wo.SetSync(true)
	defer wo.Destroy()
	return rdb.db.Put(wo, key, value)
}

func (rdb *rocksDB) Get(keys ...[]byte) ([][]byte, error) {
	ro := gorocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	switch numKeys := len(keys); {
	case numKeys == 1:
		val, err := rdb.getSingleKey(ro, keys[0])
		return [][]byte{val}, err
	default:
		return rdb.getMultipleKeys(ro, keys)
	}
}

func (rdb *rocksDB) beginGlobalMutation() error {
	if atomic.CompareAndSwapUint32(&rdb.globalMutation, 0, 1) {
		return nil
	}
	return errors.New("Another global keyspace mutation is in progress")
}

func (rdb *rocksDB) endGlobalMutation() error {
	if atomic.CompareAndSwapUint32(&rdb.globalMutation, 1, 0) {
		return nil
	}
	return errors.New("Another global keyspace mutation is in progress")
}

func (rdb *rocksDB) BackupTo(folder string) error {
	// Prevent any other backups or restores
	if err := rdb.beginGlobalMutation(); err != nil {
		return err
	}
	defer rdb.endGlobalMutation()

	be, err := rdb.openBackupEngine(folder)
	if err != nil {
		return err
	}
	defer be.Close()
	defer be.PurgeOldBackups(1)
	return be.CreateNewBackupFlush(rdb.db, true)
}

const tempDirPrefx = "rocksdb-restore-"

func (rdb *rocksDB) RestoreFrom(folder string) error {
	// Prevent any other backups or restores
	if err := rdb.beginGlobalMutation(); err != nil {
		return err
	}
	defer rdb.endGlobalMutation()

	// 1. Open backup engine for the given folder
	be, err := rdb.openBackupEngine(folder)
	if err != nil {
		return err
	}
	defer be.Close()

	// 2. Create a temp folder where the data is restored into
	restoreFolder, err := storage.CreateTempFolder(tempDirPrefx)
	if err != nil {
		return err
	}

	// 3. Perform the restoration onto the temp folder using the backup engine
	// TODO: Should this always be latest backup ?
	err = be.RestoreDBFromLatestBackup(restoreFolder, restoreFolder, rdb.opts.restoreOpts)
	if err != nil {
		return err
	}

	// 4. Close the current underlying DB and remove its db folder contents
	rdb.db.Close()

	// 5. Rename the temp folder as the actual db folder
	dbFolder := rdb.opts.folderName
	err = storage.RenameFolder(restoreFolder, dbFolder)
	if err != nil {
		return err
	}

	// 6. Open the underlying DB and switch current pointer
	restoredDB, err := openStore(rdb.opts)
	if err != nil {
		return err
	}
	*rdb = *restoredDB
	return nil
}

func (rdb *rocksDB) GetLatestCommittedChangeNumber() (uint64, error) {
	return rdb.db.GetLatestSequenceNumber(), nil
}

func (rdb *rocksDB) LoadChanges(fromChangeNumber uint64, maxChanges int) ([]*serverpb.ChangeRecord, error) {
	chngIter, err := rdb.db.GetUpdatesSince(fromChangeNumber)
	if err != nil {
		return nil, err
	}
	defer chngIter.Destroy()
	i, chngs := 0, make([]*serverpb.ChangeRecord, maxChanges)
	for i < maxChanges && chngIter.Valid() {
		wb, chngNum := chngIter.GetBatch()
		defer wb.Destroy()
		chngs[i] = toChangeRecord(wb, chngNum)
		i++
		chngIter.Next()
	}
	return chngs[0:i:i], nil
}

func (rdb *rocksDB) GetLatestAppliedChangeNumber() (uint64, error) {
	return rdb.db.GetLatestSequenceNumber(), nil
}

func (rdb *rocksDB) SaveChanges(changes []*serverpb.ChangeRecord) (uint64, error) {
	wo := gorocksdb.NewDefaultWriteOptions()
	wo.SetSync(true)
	defer wo.Destroy()
	appldChngNum := uint64(0)
	for _, chng := range changes {
		wb := gorocksdb.WriteBatchFrom(chng.SerialisedForm)
		defer wb.Destroy()
		err := rdb.db.Write(wo, wb)
		if err != nil {
			return appldChngNum, err
		}
		appldChngNum = chng.ChangeNumber
	}
	return appldChngNum, nil
}

func toChangeRecord(writeBatch *gorocksdb.WriteBatch, changeNum uint64) *serverpb.ChangeRecord {
	chngRec := &serverpb.ChangeRecord{}
	chngRec.ChangeNumber = changeNum
	dataBts := writeBatch.Data()
	dataBtsCopy := byteArrayCopy(dataBts, len(dataBts))
	chngRec.SerialisedForm = dataBtsCopy
	chngRec.NumberOfTrxns = uint32(writeBatch.Count())
	wbIter := NewWriteBatchIterator(dataBtsCopy)
	var trxns []*serverpb.TrxnRecord
	for wbIter.Next() {
		wbr := wbIter.Record()
		trxns = append(trxns, toTrxnRecord(wbr))
	}
	chngRec.Trxns = trxns
	return chngRec
}

func (rdb *rocksDB) openBackupEngine(folder string) (*gorocksdb.BackupEngine, error) {
	opts := rdb.opts.rocksDBOpts
	return gorocksdb.OpenBackupEngine(opts, folder)
}

func toTrxnRecord(wbr *gorocksdb.WriteBatchRecord) *serverpb.TrxnRecord {
	trxnRec := &serverpb.TrxnRecord{}
	switch wbr.Type {
	case gorocksdb.WriteBatchDeletionRecord:
		trxnRec.Type = serverpb.TrxnRecord_Delete
	case gorocksdb.WriteBatchValueRecord:
		trxnRec.Type = serverpb.TrxnRecord_Put
	default:
		trxnRec.Type = serverpb.TrxnRecord_Unknown
	}
	trxnRec.Key, trxnRec.Value = wbr.Key, wbr.Value
	return trxnRec
}

func byteArrayCopy(src []byte, dstLen int) []byte {
	dst := make([]byte, dstLen)
	copy(dst, src)
	return dst
}

func toByteArray(value *gorocksdb.Slice) []byte {
	src := value.Data()
	res := byteArrayCopy(src, value.Size())
	return res
}

func (rdb *rocksDB) getSingleKey(ro *gorocksdb.ReadOptions, key []byte) ([]byte, error) {
	value, err := rdb.db.Get(ro, key)
	if err != nil {
		return nil, err
	}
	return toByteArray(value), nil
}

func (rdb *rocksDB) getMultipleKeys(ro *gorocksdb.ReadOptions, keys [][]byte) ([][]byte, error) {
	values, err := rdb.db.MultiGet(ro, keys...)
	if err != nil {
		return nil, err
	}
	var results [][]byte
	for _, value := range values {
		results = append(results, toByteArray(value))
		value.Free()
	}
	return results, nil
}
