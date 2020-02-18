package rocksdb

import (
	"github.com/flipkart-incubator/dkv/internal/server/storage"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
	"github.com/tecbot/gorocksdb"
)

type rocksDB struct {
	storage.KVStore
	storage.ChangePropagator
	storage.ChangeApplier
	db *gorocksdb.DB
}

type rocksDBOpts struct {
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

func NewDefaultOptions() *rocksDBOpts {
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	return &rocksDBOpts{blockTableOpts: bbto, rocksDBOpts: opts}
}

func (this *rocksDBOpts) CacheSize(size uint64) *rocksDBOpts {
	this.blockTableOpts.SetBlockCache(gorocksdb.NewLRUCache(size))
	return this
}

func (this *rocksDBOpts) CreateDBFolderIfMissing(flag bool) *rocksDBOpts {
	this.rocksDBOpts.SetCreateIfMissing(flag)
	return this
}

func (this *rocksDBOpts) DBFolder(name string) *rocksDBOpts {
	this.folderName = name
	return this
}

func OpenStore(opts *rocksDBOpts) (*rocksDB, error) {
	if db, err := gorocksdb.OpenDb(opts.rocksDBOpts, opts.folderName); err != nil {
		return nil, err
	} else {
		return &rocksDB{db: db}, nil
	}
}

func (rdb *rocksDB) Close() error {
	rdb.db.Close()
	return nil
}

func (rdb *rocksDB) Put(key []byte, value []byte) *storage.Result {
	wo := gorocksdb.NewDefaultWriteOptions()
	wo.SetSync(true)
	defer wo.Destroy()
	err := rdb.db.Put(wo, key, value)
	return &storage.Result{err}
}

func (rdb *rocksDB) Get(keys ...[]byte) []*storage.ReadResult {
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

func (rdb *rocksDB) GetLatestChangeNumber() uint64 {
	return rdb.db.GetLatestSequenceNumber()
}

func (rdb *rocksDB) LoadChanges(fromChangeNumber uint64, maxChanges int) ([]*serverpb.ChangeRecord, error) {
	if chng_iter, err := rdb.db.GetUpdatesSince(fromChangeNumber); err != nil {
		return nil, err
	} else {
		defer chng_iter.Destroy()
		i, chngs := 0, make([]*serverpb.ChangeRecord, maxChanges)
		for i < maxChanges && chng_iter.Valid() {
			wb, chng_num := chng_iter.GetBatch()
			defer wb.Destroy()
			chngs[i] = toChangeRecord(wb, chng_num)
			i++
			chng_iter.Next()
		}
		return chngs[0:i:i], nil
	}
}

func (rdb *rocksDB) SaveChanges(changes []*serverpb.ChangeRecord) (uint64, error) {
	wo := gorocksdb.NewDefaultWriteOptions()
	wo.SetSync(true)
	defer wo.Destroy()
	applied_chng_num := uint64(0)
	for _, chng := range changes {
		chng_data := chng.SerialisedForm
		wb := gorocksdb.WriteBatchFrom(chng_data)
		defer wb.Destroy()
		err := rdb.db.Write(wo, wb)
		if err != nil {
			return applied_chng_num, err
		}
		applied_chng_num = chng.ChangeNumber
	}
	return applied_chng_num, nil
}

func toChangeRecord(writeBatch *gorocksdb.WriteBatch, changeNum uint64) *serverpb.ChangeRecord {
	chng_rec := &serverpb.ChangeRecord{}
	chng_rec.ChangeNumber = changeNum
	data_bts := writeBatch.Data()
	data_bts_copy := byteArrayCopy(data_bts, len(data_bts))
	chng_rec.SerialisedForm = data_bts_copy
	chng_rec.NumberOfTrxns = uint32(writeBatch.Count())
	wb_iter := NewWriteBatchIterator(data_bts_copy)
	var trxns []*serverpb.TrxnRecord
	for wb_iter.Next() {
		wbr := wb_iter.Record()
		trxns = append(trxns, toTrxnRecord(wbr))
	}
	chng_rec.Trxns = trxns
	return chng_rec
}

func toTrxnRecord(wbr *gorocksdb.WriteBatchRecord) *serverpb.TrxnRecord {
	trxn_rec := &serverpb.TrxnRecord{}
	switch wbr.Type {
	case gorocksdb.WriteBatchDeletionRecord:
		trxn_rec.Type = serverpb.TrxnRecord_Delete
	case gorocksdb.WriteBatchValueRecord:
		trxn_rec.Type = serverpb.TrxnRecord_Put
	default:
		trxn_rec.Type = serverpb.TrxnRecord_Unknown
	}
	trxn_rec.Key, trxn_rec.Value = wbr.Key, wbr.Value
	return trxn_rec
}

func byteArrayCopy(src []byte, dstLen int) []byte {
	dst := make([]byte, dstLen)
	copy(dst, src)
	return dst
}

func toReadResult(value *gorocksdb.Slice) *storage.ReadResult {
	src := value.Data()
	res := byteArrayCopy(src, value.Size())
	return storage.NewReadResultWithValue(res)
}

func (rdb *rocksDB) getSingleKey(ro *gorocksdb.ReadOptions, key []byte) *storage.ReadResult {
	if value, err := rdb.db.Get(ro, key); err != nil {
		return storage.NewReadResultWithError(err)
	} else {
		return toReadResult(value)
	}
}

func (rdb *rocksDB) getMultipleKeys(ro *gorocksdb.ReadOptions, keys [][]byte) []*storage.ReadResult {
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
