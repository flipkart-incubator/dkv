package rocksdb

import (
	"github.com/flipkart-incubator/dkv/internal/server/storage"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
	"github.com/tecbot/gorocksdb"
)

// DB interface represents the capabilities exposed
// by the underlying implmentation based on RocksDB engine.
type DB interface {
	storage.KVStore
	storage.ChangePropagator
	storage.ChangeApplier
}

type rocksDB struct {
	db *gorocksdb.DB
}

type rocksDBOpts struct {
	blockTableOpts *gorocksdb.BlockBasedTableOptions
	rocksDBOpts    *gorocksdb.Options
	folderName     string
}

// OpenDB initializes a new instance of RocksDB with default
// options. It uses the given folder for storing the data files.
func OpenDB(dbFolder string, cacheSize uint64) DB {
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

func (rdb *rocksDB) GetLatestCommittedChangeNumber() (uint64, error) {
	return rdb.db.GetLatestSequenceNumber(), nil
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

func (rdb *rocksDB) GetLatestAppliedChangeNumber() (uint64, error) {
	return rdb.db.GetLatestSequenceNumber(), nil
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

func toByteArray(value *gorocksdb.Slice) []byte {
	src := value.Data()
	res := byteArrayCopy(src, value.Size())
	return res
}

func (rdb *rocksDB) getSingleKey(ro *gorocksdb.ReadOptions, key []byte) ([]byte, error) {
	if value, err := rdb.db.Get(ro, key); err != nil {
		return nil, err
	} else {
		return toByteArray(value), nil
	}
}

func (rdb *rocksDB) getMultipleKeys(ro *gorocksdb.ReadOptions, keys [][]byte) ([][]byte, error) {
	if values, err := rdb.db.MultiGet(ro, keys...); err != nil {
		return nil, err
	} else {
		var results [][]byte
		for _, value := range values {
			results = append(results, toByteArray(value))
			value.Free()
		}
		return results, nil
	}
}
