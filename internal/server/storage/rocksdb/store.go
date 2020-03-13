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
	opts := newDefaultOptions()
	opts.CreateDBFolderIfMissing(true).DBFolder(dbFolder).CacheSize(cacheSize)
	if kvs, err := openStore(opts); err != nil {
		panic(err)
	} else {
		return kvs
	}
}

func newDefaultOptions() *rocksDBOpts {
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	return &rocksDBOpts{blockTableOpts: bbto, rocksDBOpts: opts}
}

func (rdbOpts *rocksDBOpts) CacheSize(size uint64) *rocksDBOpts {
	rdbOpts.blockTableOpts.SetBlockCache(gorocksdb.NewLRUCache(size))
	return rdbOpts
}

func (rdbOpts *rocksDBOpts) CreateDBFolderIfMissing(flag bool) *rocksDBOpts {
	rdbOpts.rocksDBOpts.SetCreateIfMissing(flag)
	return rdbOpts
}

func (rdbOpts *rocksDBOpts) DBFolder(name string) *rocksDBOpts {
	rdbOpts.folderName = name
	return rdbOpts
}

func openStore(opts *rocksDBOpts) (*rocksDB, error) {
	db, err := gorocksdb.OpenDb(opts.rocksDBOpts, opts.folderName)
	if err != nil {
		return nil, err
	}
	return &rocksDB{db: db}, nil
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
