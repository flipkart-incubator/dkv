package rocksdb

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/flipkart-incubator/dkv/internal/hlc"
	"github.com/flipkart-incubator/dkv/internal/storage/iterators"
	"github.com/shamaton/msgpack"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync/atomic"
	"time"

	"github.com/flipkart-incubator/dkv/internal/stats"
	"github.com/flipkart-incubator/dkv/internal/storage"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
	"github.com/flipkart-incubator/gorocksdb"
	"go.uber.org/zap"
	"gopkg.in/ini.v1"
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
	db          *gorocksdb.DB
	normalCF    *gorocksdb.ColumnFamilyHandle
	ttlCF       *gorocksdb.ColumnFamilyHandle
	optimTrxnDB *gorocksdb.OptimisticTransactionDB
	opts        *rocksDBOpts

	// Indicates a global mutation like backup and restore that
	// require exclusivity. Shall be manipulated using atomics.
	globalMutation uint32
}

type rocksDBOpts struct {
	readOpts       *gorocksdb.ReadOptions
	writeOpts      *gorocksdb.WriteOptions
	blockTableOpts *gorocksdb.BlockBasedTableOptions
	rocksDBOpts    *gorocksdb.Options
	restoreOpts    *gorocksdb.RestoreOptions
	folderName     string
	sstDirectory   string
	lgr            *zap.Logger
	statsCli       stats.Client
	cfNames        []string
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

// WithSyncWrites ensures all writes to RocksDB are
// immediatey flushed to disk from OS buffers.
func WithSyncWrites() DBOption {
	return func(opts *rocksDBOpts) {
		opts.writeOpts.SetSync(true)
	}
}

// WithSSTDir configures the directory to be used
// for SST Operation on RocksDB.
func WithSSTDir(sstDir string) DBOption {
	return func(opts *rocksDBOpts) {
		opts.sstDirectory = sstDir
	}
}

// WithCacheSize is used to set the block cache size.
func WithCacheSize(size uint64) DBOption {
	return func(opts *rocksDBOpts) {
		if size > 0 {
			opts.blockTableOpts.SetBlockCache(gorocksdb.NewLRUCache(size))
		} else {
			opts.blockTableOpts.SetNoBlockCache(true)
		}
	}
}

// WithRocksDBConfig can be used to override internal RocksDB
// storage settings through the given .ini file.
func WithRocksDBConfig(iniFile string) DBOption {
	return func(opts *rocksDBOpts) {
		if iniFile = strings.TrimSpace(iniFile); iniFile != "" {
			if cfg, err := ini.Load(iniFile); err != nil {
				panic(fmt.Errorf("unable to load RocksDB configuration from given file: %s, error: %v", iniFile, err))
			} else {
				var buff strings.Builder
				sect := cfg.Section("")
				sectConf := sect.KeysHash()
				for key, val := range sectConf {
					fmt.Fprintf(&buff, "%s=%s;", key, val)
				}
				if rdbOpts, err := gorocksdb.GetOptionsFromString(opts.rocksDBOpts, buff.String()); err != nil {
					panic(fmt.Errorf("unable to parge RocksDB configuration from given file: %s, error: %v", iniFile, err))
				} else {
					opts.rocksDBOpts = rdbOpts
				}
			}
		}
	}
}

// OpenDB initializes a new instance of RocksDB with specified
// options. It uses the given folder for storing the data files.
func OpenDB(dbFolder string, dbOpts ...DBOption) (DB, error) {
	opts := newOptions(dbFolder)
	for _, dbOpt := range dbOpts {
		dbOpt(opts)
	}
	return openStore(opts)
}

type ttlCompactionFilter struct {
	lgr *zap.Logger
}

// Name returns the CompactionFilter name
func (m *ttlCompactionFilter) Name() string {
	return "dkv.ttlFilter"
}

// Filter applies the logic for the Compaction process.
// this returns remove as true if the TTL of the key has expired.
func (m *ttlCompactionFilter) Filter(level int, key, val []byte) (remove bool, newVal []byte) {
	ttlRow, err := parseTTLMsgPackData(val)
	if err != nil {
		m.lgr.Warn("ttlCompactionFilter::Filter Failed to parse msgpack data", zap.String("Key", string(key)))
		return false, nil
	}
	if hlc.InThePast(ttlRow.ExpiryTS) {
		return true, val
	}
	return false, nil
}

func newOptions(dbFolder string) *rocksDBOpts {
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	opts := gorocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetCreateIfMissingColumnFamilies(true)
	opts.SetBlockBasedTableFactory(bbto)
	rstOpts := gorocksdb.NewRestoreOptions()
	wrOpts := gorocksdb.NewDefaultWriteOptions()
	rdOpts := gorocksdb.NewDefaultReadOptions()
	cfNames := []string{"default", "ttl"}
	return &rocksDBOpts{
		folderName:     dbFolder,
		blockTableOpts: bbto,
		rocksDBOpts:    opts,
		restoreOpts:    rstOpts,
		lgr:            zap.NewNop(),
		readOpts:       rdOpts,
		writeOpts:      wrOpts,
		statsCli:       stats.NewNoOpClient(),
		cfNames:        cfNames,
	}
}

func (rdbOpts *rocksDBOpts) destroy() {
	rdbOpts.blockTableOpts.Destroy()
	rdbOpts.rocksDBOpts.Destroy()
	rdbOpts.restoreOpts.Destroy()
	rdbOpts.readOpts.Destroy()
	rdbOpts.writeOpts.Destroy()
}

func openStore(opts *rocksDBOpts) (*rocksDB, error) {
	normalOpts := opts.rocksDBOpts
	ttlOpts, err := gorocksdb.GetOptionsFromString(normalOpts, "")
	if err != nil {
		return nil, err
	}
	ttlOpts.SetCompactionFilter(&ttlCompactionFilter{opts.lgr})
	optimTrxnDB, cfh, err := gorocksdb.OpenOptimisticTransactionDbColumnFamilies(opts.rocksDBOpts,
		opts.folderName, opts.cfNames, []*gorocksdb.Options{normalOpts, ttlOpts})
	if err != nil {
		return nil, err
	}

	rocksdb := rocksDB{
		db:             optimTrxnDB.GetBaseDb(),
		normalCF:       cfh[0],
		ttlCF:          cfh[1],
		optimTrxnDB:    optimTrxnDB,
		opts:           opts,
		globalMutation: 0,
	}
	//TODO: revisit this later after understanding what is the impact of manually triggered compaction
	//go rocksdb.Compaction()
	return &rocksdb, nil
}

type ttlDataFormat struct {
	ExpiryTS uint64 `msgpack:"t"`
	Data     []byte `msgpack:"d"`
}

// Compaction runs the compaction routine
func (rdb *rocksDB) Compaction() error {
	tick := time.Tick(15 * time.Minute)
	for {
		select {
		case <-tick:
			// trigger a compaction
			rdb.opts.lgr.Info("Triggering RocksDB Compaction")
			rdb.db.CompactRangeCF(rdb.ttlCF, gorocksdb.Range{nil, nil})
		}
	}
	return nil
}

func (rdb *rocksDB) Close() error {
	rdb.optimTrxnDB.Close()
	//rdb.opts.destroy()
	return nil
}

func (rdb *rocksDB) PutTTL(key []byte, value []byte, expireTS uint64) error {
	if expireTS > 0 {
		defer rdb.opts.statsCli.Timing("rocksdb.putTTL.latency.ms", time.Now())
		dF := ttlDataFormat{
			ExpiryTS: expireTS,
			Data:     value,
		}
		msgPack, err := msgpack.Marshal(dF)
		if err != nil {
			rdb.opts.statsCli.Incr("rocksdb.putTTL.errors", 1)
			return err
		}
		wb := gorocksdb.NewWriteBatch()
		wb.Delete(key)
		wb.PutCF(rdb.ttlCF, key, msgPack)
		err = rdb.db.Write(rdb.opts.writeOpts, wb)
		if err != nil {
			rdb.opts.statsCli.Incr("rocksdb.putTTL.errors", 1)
		}
		return err
	}
	return rdb.Put(key, value)
}

func (rdb *rocksDB) Put(key []byte, value []byte) error {
	defer rdb.opts.statsCli.Timing("rocksdb.put.latency.ms", time.Now())
	wb := gorocksdb.NewWriteBatch()
	wb.DeleteCF(rdb.ttlCF, key)
	wb.Put(key, value)
	err := rdb.db.Write(rdb.opts.writeOpts, wb)
	if err != nil {
		rdb.opts.statsCli.Incr("rocksdb.put.errors", 1)
	}
	return err
}

func (rdb *rocksDB) Delete(key []byte) error {
	defer rdb.opts.statsCli.Timing("rocksdb.delete.latency.ms", time.Now())
	wb := gorocksdb.NewWriteBatch()
	wb.DeleteCF(rdb.ttlCF, key)
	wb.Delete(key)
	err := rdb.db.Write(rdb.opts.writeOpts, wb)
	if err != nil {
		rdb.opts.statsCli.Incr("rocksdb.delete.errors", 1)
	}
	return err
}

func (rdb *rocksDB) Get(keys ...[]byte) ([]*serverpb.KVPair, error) {
	ro := rdb.opts.readOpts
	switch numKeys := len(keys); {
	case numKeys == 1:
		return rdb.getSingleKey(ro, keys[0])
	default:
		return rdb.getMultipleKeys(ro, keys)
	}
}

func (rdb *rocksDB) CompareAndSet(key, expect, update []byte) (bool, error) {
	defer rdb.opts.statsCli.Timing("rocksdb.cas.latency.ms", time.Now())
	ro := rdb.opts.readOpts
	wo := rdb.opts.writeOpts
	to := gorocksdb.NewDefaultOptimisticTransactionOptions()
	txn := rdb.optimTrxnDB.TransactionBegin(wo, to, nil)
	defer txn.Destroy()

	exist, err := txn.GetForUpdate(ro, key)
	if err != nil {
		return false, err
	}
	defer exist.Free()

	existVal := exist.Data()
	if expect == nil || len(expect) == 0 {
		if len(existVal) > 0 {
			return false, nil
		}
	} else {
		if !bytes.Equal(existVal, expect) {
			return false, nil
		}
	}
	err = txn.Put(key, update)
	if err != nil {
		rdb.opts.statsCli.Incr("rocksdb.cas.set.errors", 1)
		return false, err
	}
	err = txn.Commit()
	if err != nil && strings.HasSuffix(err.Error(), "Resource busy: ") {
		return false, nil
	}
	return err == nil, err
}

const tempFilePrefix = "rocksdb-sstfile-"

func (rdb *rocksDB) GetSnapshot() ([]byte, error) {
	snap := rdb.db.NewSnapshot()
	defer rdb.db.ReleaseSnapshot(snap)

	envOpts := gorocksdb.NewDefaultEnvOptions()
	opts := gorocksdb.NewDefaultOptions()
	sstWrtr := gorocksdb.NewSSTFileWriter(envOpts, opts)
	defer sstWrtr.Destroy()

	sstFile, err := storage.CreateTempFile(rdb.opts.sstDirectory, tempFilePrefix)
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

	sstFile, err := storage.CreateTempFile(rdb.opts.sstDirectory, tempFilePrefix)
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
	restoreFolder, err := storage.CreateTempFolder(rdb.opts.sstDirectory, tempDirPrefix)
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
		chngs[i] = rdb.toChangeRecord(wb, chngNum)
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
	appldChngNum := uint64(0)
	for _, chng := range changes {
		wb := gorocksdb.WriteBatchFrom(chng.SerialisedForm)
		defer wb.Destroy()
		err := rdb.db.Write(rdb.opts.writeOpts, wb)
		if err != nil {
			return appldChngNum, err
		}
		appldChngNum = chng.ChangeNumber
	}
	return appldChngNum, nil
}

type iter struct {
	iterOpts storage.IterationOptions
	rdbIter  *gorocksdb.Iterator
	ttlCF    bool
}

func (rdb *rocksDB) newIterCF(iterOpts storage.IterationOptions, cf *gorocksdb.ColumnFamilyHandle) *iter {
	readOpts := rdb.opts.readOpts
	it := rdb.db.NewIteratorCF(readOpts, cf)
	if sk, present := iterOpts.StartKey(); present {
		it.Seek(sk)
	} else {
		it.SeekToFirst()
	}
	return &iter{iterOpts, it, cf == rdb.ttlCF}
}

func (rdbIter *iter) verifyTTLValidity() bool {
	if rdbIter.rdbIter.Valid() {
		val := toByteArray(rdbIter.rdbIter.Value())
		if rdbIter.ttlCF {
			ttlRow, _ := parseTTLMsgPackData(val)
			if hlc.InThePast(ttlRow.ExpiryTS) {
				return false
			}
		}
	}
	return true
}

func (rdbIter *iter) HasNext() bool {
	if kp, prsnt := rdbIter.iterOpts.KeyPrefix(); prsnt {
		if rdbIter.rdbIter.ValidForPrefix(kp) && rdbIter.verifyTTLValidity() {
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
	var ttlRow *ttlDataFormat
	if rdbIter.ttlCF { //base iterator doesn't have ttl
		ttlRow, _ = parseTTLMsgPackData(val)
	}
	if ttlRow != nil && ttlRow.ExpiryTS > 0 {
		val = ttlRow.Data
	}
	return key, val
}

func (rdbIter *iter) Err() error {
	return rdbIter.rdbIter.Err()
}

func (rdbIter *iter) Close() error {
	rdbIter.rdbIter.Close()
	return nil
}

func (rdb *rocksDB) Iterate(iterOpts storage.IterationOptions) storage.Iterator {
	baseIter := rdb.newIterCF(iterOpts, rdb.normalCF)
	ttlIter := rdb.newIterCF(iterOpts, rdb.ttlCF)
	return iterators.Concat(baseIter, ttlIter)
}

func (rdb *rocksDB) toChangeRecord(writeBatch *gorocksdb.WriteBatch, changeNum uint64) *serverpb.ChangeRecord {
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
		trxns = append(trxns, rdb.toTrxnRecord(wbr))
	}
	chngRec.Trxns = trxns
	return chngRec
}

func (rdb *rocksDB) openBackupEngine(folder string) (*gorocksdb.BackupEngine, error) {
	opts := rdb.opts.rocksDBOpts
	return gorocksdb.OpenBackupEngine(opts, folder)
}

func (rdb *rocksDB) toTrxnRecord(wbr *gorocksdb.WriteBatchRecord) *serverpb.TrxnRecord {
	trxnRec := &serverpb.TrxnRecord{}
	switch wbr.Type {
	case gorocksdb.WriteBatchCFDeletionRecord:
		trxnRec.Type = serverpb.TrxnRecord_Delete
	case gorocksdb.WriteBatchDeletionRecord:
		trxnRec.Type = serverpb.TrxnRecord_Delete
	case gorocksdb.WriteBatchValueRecord:
		trxnRec.Type = serverpb.TrxnRecord_Put
	case gorocksdb.WriteBatchCFValueRecord:
		trxnRec.Type = serverpb.TrxnRecord_Put
	default:
		trxnRec.Type = serverpb.TrxnRecord_Unknown
	}
	trxnRec.Key, trxnRec.Value = wbr.Key, wbr.Value
	if wbr.CF == 1 { //second CF
		if ttlDf, err := parseTTLMsgPackData(wbr.Value); err != nil {
			rdb.opts.lgr.Warn("ToTrxnRecord parseTTLMsgPackData Failed",
				zap.String("Key", string(wbr.Key)), zap.Int("CF", wbr.CF))
		} else if ttlDf != nil {
			trxnRec.Value = ttlDf.Data
			trxnRec.ExpireTS = ttlDf.ExpiryTS
		}
	}
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

func parseTTLMsgPackData(valueWithTTL []byte) (*ttlDataFormat, error) {
	var row ttlDataFormat
	var err error
	if len(valueWithTTL) > 0 {
		err = msgpack.Unmarshal(valueWithTTL, &row)
	}
	return &row, err
}

func (rdb *rocksDB) getSingleKey(ro *gorocksdb.ReadOptions, key []byte) ([]*serverpb.KVPair, error) {
	defer rdb.opts.statsCli.Timing("rocksdb.single.get.latency.ms", time.Now())
	values, err := rdb.db.MultiGetCFMultiCF(ro, []*gorocksdb.ColumnFamilyHandle{rdb.normalCF, rdb.ttlCF}, [][]byte{key, key})
	if err != nil {
		rdb.opts.statsCli.Incr("rocksdb.single.get.errors", 1)
		return nil, err
	}
	value1, value2 := values[0], values[1]
	kv := rdb.extractResult(value1, value2, key)
	value1.Free()
	value2.Free()
	if kv != nil {
		return []*serverpb.KVPair{kv}, nil
	}
	return nil, nil
}

func (rdb *rocksDB) extractResult(value1 *gorocksdb.Slice, value2 *gorocksdb.Slice, key []byte) *serverpb.KVPair {
	if value1.Size() > 0 {
		//non ttl use-case
		val := toByteArray(value1)
		return &serverpb.KVPair{Key: key, Value: val}
	}

	if value2.Size() > 0 {
		//ttl use-case, check ttl
		val := toByteArray(value2)
		ttlRow, err := parseTTLMsgPackData(val)
		if err != nil {
			rdb.opts.lgr.Warn("RocksDB::extractResult Failed to parse msgpack data",
				zap.String("Key", string(key)), zap.Error(err))
			rdb.opts.statsCli.Incr("rocksdb.get.parse.errors", 1)
			return nil
		}
		if hlc.InThePast(ttlRow.ExpiryTS) {
			return nil
		} else if ttlRow.ExpiryTS > 0 {
			val = ttlRow.Data
		}
		return &serverpb.KVPair{Key: key, Value: val}
	}

	return nil
}

func (rdb *rocksDB) getMultipleKeys(ro *gorocksdb.ReadOptions, keys [][]byte) ([]*serverpb.KVPair, error) {
	defer rdb.opts.statsCli.Timing("rocksdb.multi.get.latency.ms", time.Now())

	kl := len(keys)
	reqCFs := make([]*gorocksdb.ColumnFamilyHandle, kl<<1)
	for i := 0; i < kl; i++ {
		reqCFs[i] = rdb.normalCF
		reqCFs[i+kl] = rdb.ttlCF
	}

	values, err := rdb.db.MultiGetCFMultiCF(ro, reqCFs, append(keys, keys...))
	if err != nil {
		rdb.opts.statsCli.Incr("rocksdb.multi.get.errors", 1)
		return nil, err
	}

	var results []*serverpb.KVPair
	for i := 0; i < kl; i++ {
		value1, value2 := values[i], values[i+kl]
		kv := rdb.extractResult(value1, value2, keys[i])
		value1.Free()
		value2.Free()
		if kv != nil {
			results = append(results, kv)
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
