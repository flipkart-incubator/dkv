package rocksdb

import (
	"errors"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync/atomic"
	"time"

	"github.com/flipkart-incubator/dkv/internal/stats"
	"github.com/flipkart-incubator/dkv/internal/storage"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
	"github.com/tecbot/gorocksdb"
	"go.uber.org/zap"
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
	opts *rocksDBOpts

	// Indicates a global mutation like backup and restore that
	// require exclusivity. Shall be manipulated using atomics.
	globalMutation uint32
}

type rocksDBOpts struct {
	blockTableOpts *gorocksdb.BlockBasedTableOptions
	rocksDBOpts    *gorocksdb.Options
	restoreOpts    *gorocksdb.RestoreOptions
	folderName     string
	lgr            *zap.Logger
	statsCli       stats.Client
}

// DBOption is used to configure the RocksDB
// storage engine.
type DBOption func(*rocksDBOpts)

// WithLogger is used to inject a ZAP logger instance.
func WithLogger(lgr *zap.Logger) DBOption {
	return func(opts *rocksDBOpts) {
		if lgr != nil {
			opts.lgr = lgr
		} else {
			opts.lgr = zap.NewNop()
		}
	}
}

// WithStats is used to inject a metrics client.
func WithStats(statsCli stats.Client) DBOption {
	return func(opts *rocksDBOpts) {
		if statsCli != nil {
			opts.statsCli = statsCli
		} else {
			opts.statsCli = stats.NewNoOpClient()
		}
	}
}

// WithCacheSize is used to set the block cache size.
func WithCacheSize(size uint64) DBOption {
	return func(opts *rocksDBOpts) {
		opts.blockTableOpts.SetBlockCache(gorocksdb.NewLRUCache(size))
	}
}

// OpenDB initializes a new instance of RocksDB with specified
// options. It uses the given folder for storing the data files.
func OpenDB(dbFolder string, dbOpts ...DBOption) (DB, error) {
	opts := newOptions(dbFolder)
	for _, dbOpt := range dbOpts {
		dbOpt(opts)
	}
	db, err := gorocksdb.OpenDb(opts.rocksDBOpts, dbFolder)
	if err != nil {
		return nil, err
	}
	return &rocksDB{db, opts, 0}, nil
}

func newOptions(dbFolder string) *rocksDBOpts {
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	opts := gorocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetBlockBasedTableFactory(bbto)
	rstOpts := gorocksdb.NewRestoreOptions()
	return &rocksDBOpts{folderName: dbFolder, blockTableOpts: bbto, rocksDBOpts: opts, restoreOpts: rstOpts, lgr: zap.NewNop(), statsCli: stats.NewNoOpClient()}
}

func (rdbOpts *rocksDBOpts) destroy() {
	rdbOpts.blockTableOpts.Destroy()
	rdbOpts.rocksDBOpts.Destroy()
	rdbOpts.restoreOpts.Destroy()
}

func openStore(opts *rocksDBOpts) (*rocksDB, error) {
	db, err := gorocksdb.OpenDb(opts.rocksDBOpts, opts.folderName)
	if err != nil {
		return nil, err
	}
	return &rocksDB{db, opts, 0}, nil
}

func (rdb *rocksDB) Close() error {
	//rdb.opts.destroy()
	rdb.db.Close()
	return nil
}

func (rdb *rocksDB) Put(key []byte, value []byte) error {
	defer rdb.opts.statsCli.Timing("rocksdb.put.latency.ms", time.Now())
	wo := gorocksdb.NewDefaultWriteOptions()
	wo.SetSync(true)
	defer wo.Destroy()
	err := rdb.db.Put(wo, key, value)
	if err != nil {
		rdb.opts.statsCli.Incr("rocksdb.put.errors", 1)
	}
	return err
}

func (rdb *rocksDB) Get(keys ...[]byte) ([]*serverpb.KVPair, error) {
	ro := gorocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	switch numKeys := len(keys); {
	case numKeys == 1:
		return rdb.getSingleKey(ro, keys[0])
	default:
		return rdb.getMultipleKeys(ro, keys)
	}
}

const tempFilePrefix = "rocksdb-sstfile-"

func (rdb *rocksDB) GetSnapshot() ([]byte, error) {
	snap := rdb.db.NewSnapshot()
	defer rdb.db.ReleaseSnapshot(snap)

	envOpts := gorocksdb.NewDefaultEnvOptions()
	opts := gorocksdb.NewDefaultOptions()
	sstWrtr := gorocksdb.NewSSTFileWriter(envOpts, opts)
	defer sstWrtr.Destroy()

	sstFile, err := storage.CreateTempFile(tempFilePrefix)
	if err != nil {
		return nil, err
	}

	defer os.Remove(sstFile)
	if err = sstWrtr.Open(sstFile); err != nil {
		return nil, err
	}

	// TODO: Any options need to be set
	readOpts := gorocksdb.NewDefaultReadOptions()
	defer readOpts.Destroy()
	readOpts.SetSnapshot(snap)

	it := rdb.db.NewIterator(readOpts)
	defer it.Close()
	it.SeekToFirst()

	if it.Valid() {
		for it.Valid() {
			sstWrtr.Add(it.Key().Data(), it.Value().Data())
			it.Next()
		}
	} else {
		return nil, nil
	}

	if err = sstWrtr.Finish(); err != nil {
		return nil, err
	}

	return ioutil.ReadFile(sstFile)
}

func (rdb *rocksDB) PutSnapshot(snap []byte) error {
	if snap == nil || len(snap) == 0 {
		return nil
	}

	sstFile, err := storage.CreateTempFile(tempFilePrefix)
	if err != nil {
		return err
	}

	defer os.Remove(sstFile)
	err = ioutil.WriteFile(sstFile, snap, 0644)
	if err != nil {
		return err
	}

	// TODO: Check any option needs to be set
	ingestOpts := gorocksdb.NewDefaultIngestExternalFileOptions()
	defer ingestOpts.Destroy()

	err = rdb.db.IngestExternalFile([]string{sstFile}, ingestOpts)
	return err
}

func (rdb *rocksDB) BackupTo(folder string) error {
	if err := checksForBackup(folder); err != nil {
		return err
	}
	// Prevent any other backups or restores
	if err := rdb.beginGlobalMutation(); err != nil {
		return err
	}
	defer rdb.endGlobalMutation()

	be, err := rdb.openBackupEngine(path.Clean(folder))
	if err != nil {
		return err
	}
	defer be.Close()

	// Retain only the latest backup in the given folder
	defer be.PurgeOldBackups(1)
	return be.CreateNewBackupFlush(rdb.db, true)
}

const tempDirPrefix = "rocksdb-restore-"

func (rdb *rocksDB) RestoreFrom(folder string) (st storage.KVStore, ba storage.Backupable, cp storage.ChangePropagator, ca storage.ChangeApplier, err error) {

	// Prevent any other backups or restores
	err = rdb.beginGlobalMutation()
	if err != nil {
		return
	}
	defer rdb.endGlobalMutation()

	// In any case, reopen a new DB
	defer func() {
		if finalDB, openErr := openStore(rdb.opts); openErr != nil {
			err = openErr
		} else {
			st, ba, cp, ca = finalDB, finalDB, finalDB, finalDB
		}
	}()

	// Check for the given restore folder validity
	err = checksForRestore(folder)
	if err != nil {
		return
	}

	// Open the backup engine with the given restore folder
	be, err := rdb.openBackupEngine(folder)
	if err != nil {
		return
	}
	defer be.Close()

	// Create temp folder for the restored data
	restoreFolder, err := storage.CreateTempFolder(tempDirPrefix)
	if err != nil {
		return
	}

	// Restore DB onto the temp folder
	err = be.RestoreDBFromLatestBackup(restoreFolder, restoreFolder, rdb.opts.restoreOpts)
	if err != nil {
		return
	}

	// Move the temp folder to the original DB location
	err = storage.RenameFolder(restoreFolder, rdb.opts.folderName)

	// Plain return due to defer function above
	return
}

func (rdb *rocksDB) GetLatestCommittedChangeNumber() (uint64, error) {
	return rdb.db.GetLatestSequenceNumber(), nil
}

func (rdb *rocksDB) LoadChanges(fromChangeNumber uint64, maxChanges int) ([]*serverpb.ChangeRecord, error) {
	defer rdb.opts.statsCli.Timing("rocksdb.load.changes.latency.ms", time.Now())
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
	defer rdb.opts.statsCli.Timing("rocksdb.save.changes.latency.ms", time.Now())
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

type iter struct {
	iterOpts storage.IterationOptions
	readOpts *gorocksdb.ReadOptions
	rdbIter  *gorocksdb.Iterator
}

func (rdb *rocksDB) newIter(iterOpts storage.IterationOptions) *iter {
	readOpts := gorocksdb.NewDefaultReadOptions()
	it := rdb.db.NewIterator(readOpts)

	if sk, prsnt := iterOpts.StartKey(); prsnt {
		it.Seek(sk)
	} else {
		it.SeekToFirst()
	}
	return &iter{iterOpts, readOpts, it}
}

func (rdbIter *iter) HasNext() bool {
	if kp, prsnt := rdbIter.iterOpts.KeyPrefix(); prsnt {
		if rdbIter.rdbIter.ValidForPrefix(kp) {
			return true
		}
		if rdbIter.rdbIter.Valid() {
			rdbIter.rdbIter.Next()
			return rdbIter.HasNext()
		}
		return false
	}
	return rdbIter.rdbIter.Valid()
}

func (rdbIter *iter) Next() ([]byte, []byte) {
	defer rdbIter.rdbIter.Next()
	key := toByteArray(rdbIter.rdbIter.Key())
	val := toByteArray(rdbIter.rdbIter.Value())
	return key, val
}

func (rdbIter *iter) Err() error {
	return rdbIter.rdbIter.Err()
}

func (rdbIter *iter) Close() error {
	rdbIter.readOpts.Destroy()
	rdbIter.rdbIter.Close()
	return nil
}

func (rdb *rocksDB) Iterate(iterOpts storage.IterationOptions) storage.Iterator {
	return rdb.newIter(iterOpts)
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

func (rdb *rocksDB) getSingleKey(ro *gorocksdb.ReadOptions, key []byte) ([]*serverpb.KVPair, error) {
	defer rdb.opts.statsCli.Timing("rocksdb.single.get.latency.ms", time.Now())
	value, err := rdb.db.Get(ro, key)
	if err != nil {
		rdb.opts.statsCli.Incr("rocksdb.single.get.errors", 1)
		return nil, err
	}
	val := toByteArray(value)
	if val != nil && len(val) > 0 {
		return []*serverpb.KVPair{&serverpb.KVPair{Key: key, Value: val}}, nil
	}
	return nil, nil
}

func (rdb *rocksDB) getMultipleKeys(ro *gorocksdb.ReadOptions, keys [][]byte) ([]*serverpb.KVPair, error) {
	defer rdb.opts.statsCli.Timing("rocksdb.multi.get.latency.ms", time.Now())
	values, err := rdb.db.MultiGet(ro, keys...)
	if err != nil {
		rdb.opts.statsCli.Incr("rocksdb.multi.get.errors", 1)
		return nil, err
	}
	var results []*serverpb.KVPair
	for i, value := range values {
		if value != nil {
			key, val := keys[i], toByteArray(value)
			results = append(results, &serverpb.KVPair{Key: key, Value: val})
			value.Free()
		}
	}
	return results, nil
}

var errGlobalMutation = errors.New("Another global keyspace mutation is in progress")

func (rdb *rocksDB) hasGlobalMutation() bool {
	return atomic.LoadUint32(&rdb.globalMutation) == 1
}

func (rdb *rocksDB) beginGlobalMutation() error {
	if atomic.CompareAndSwapUint32(&rdb.globalMutation, 0, 1) {
		return nil
	}
	return errGlobalMutation
}

func (rdb *rocksDB) endGlobalMutation() error {
	if atomic.CompareAndSwapUint32(&rdb.globalMutation, 1, 0) {
		return nil
	}
	return errGlobalMutation
}

func checksForBackup(bckpPath string) error {
	if len(strings.TrimSpace(bckpPath)) == 0 {
		return errors.New("valid path must be provided")
	}

	switch fi, err := os.Stat(bckpPath); {
	case err != nil:
		_, err := os.Stat(path.Dir(bckpPath))
		return err
	case !fi.IsDir():
		return errors.New("require a folder for rocksdb backup")
	default:
		return nil
	}
}

func checksForRestore(rstrPath string) error {
	switch fi, err := os.Stat(rstrPath); {
	case err != nil:
		return err
	case !fi.IsDir():
		return errors.New("require a folder for rocksdb restore")
	default:
		return nil
	}
}
