package rocksdb

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/flipkart-incubator/dkv/internal/server/storage"
	"github.com/tecbot/gorocksdb"
)

var store storage.KVStore

const (
	createDBFolderIfMissing = true
	dbFolder                = "/tmp/rocksdb_storage_test"
	cacheSize               = 3 << 30
)

func TestMain(m *testing.M) {
	if kvs, err := openRocksDB(); err != nil {
		panic(err)
	} else {
		store = kvs
		res := m.Run()
		store.Close()
		os.Exit(res)
	}
}

func TestPutAndGet(t *testing.T) {
	numKeys := 10
	for i := 1; i <= numKeys; i++ {
		key, value := fmt.Sprintf("K%d", i), fmt.Sprintf("V%d", i)
		if err := store.Put([]byte(key), []byte(value)).Error; err != nil {
			t.Fatalf("Unable to PUT. Key: %s, Value: %s, Error: %v", key, value, err)
		}
	}

	for i := 1; i <= numKeys; i++ {
		key, expectedValue := fmt.Sprintf("K%d", i), fmt.Sprintf("V%d", i)
		readResult := store.Get([]byte(key))[0]
		if actualValue, err := readResult.Value, readResult.Error; err != nil {
			t.Fatalf("Unable to GET. Key: %s, Error: %v", key, err)
		} else if string(actualValue) != expectedValue {
			t.Errorf("GET mismatch. Key: %s, Expected Value: %s, Actual Value: %s", key, expectedValue, actualValue)
		}
	}
}

func TestGetUpdatesFromSeqNum(t *testing.T) {
	rdb := store.(*RocksDBStore)
	before_seq := rdb.db.GetLatestSequenceNumber()

	exp_num_trxns := 3
	for i := 1; i <= exp_num_trxns; i++ {
		k, v := fmt.Sprintf("aKey_%d", i), fmt.Sprintf("aVal_%d", i)
		if err := store.Put([]byte(k), []byte(v)).Error; err != nil {
			t.Fatal(err)
		} else {
			read_result := store.Get([]byte(k))[0]
			if act_val, err := read_result.Value, read_result.Error; err != nil {
				t.Fatal(err)
			} else if string(act_val) != string(v) {
				t.Errorf("GET mismatch. Key: %s, Expected Value: %s, Actual Value: %s", k, v, act_val)
			}
		}
	}

	after_seq := rdb.db.GetLatestSequenceNumber()
	num_trxns := int(after_seq - before_seq)
	if num_trxns != exp_num_trxns {
		t.Errorf("Incorrect number of transactions reported. Expected: %d, Actual: %d", exp_num_trxns, num_trxns)
	}

	start_seq := 1 + before_seq // This is done to remove previous transaction if any
	if trxn_iter, err := rdb.db.GetUpdatesSince(start_seq); err != nil {
		t.Fatal(err)
	} else {
		defer trxn_iter.Destroy()
		for trxn_iter.Valid() {
			wb, _ := trxn_iter.GetBatch()
			exp_bts := wb.Data()
			wb.Destroy()
			wb = gorocksdb.WriteBatchFrom(exp_bts)
			act_bts := wb.Data()
			if string(exp_bts) != string(act_bts) {
				t.Errorf("WriteBatch mismatch. Expected: %s, Actual: %s", exp_bts, act_bts)
			} else {
				t.Log(string(act_bts))
			}
			wb.Destroy()
			trxn_iter.Next()
		}
	}
}

func TestGetUpdatesFromSeqNumForBatches(t *testing.T) {
	rdb := store.(*RocksDBStore)
	before_seq := rdb.db.GetLatestSequenceNumber()

	exp_num_batch_trxns := 3
	num_trxns_per_batch := 2
	exp_num_trxns := exp_num_batch_trxns * num_trxns_per_batch
	for i := 1; i <= exp_num_batch_trxns; i++ {
		k, v := fmt.Sprintf("bKey_%d", i), fmt.Sprintf("bVal_%d", i)
		wb := gorocksdb.NewWriteBatch()
		wb.Put([]byte(k), []byte(v))
		wb.Delete([]byte(k))
		wo := gorocksdb.NewDefaultWriteOptions()
		wo.SetSync(true)
		if err := rdb.db.Write(wo, wb); err != nil {
			t.Fatal(err)
		}
		wb.Destroy()
		wo.Destroy()
	}

	after_seq := rdb.db.GetLatestSequenceNumber()
	num_trxns := int(after_seq - before_seq)
	if num_trxns != exp_num_trxns {
		t.Errorf("Incorrect number of transactions reported. Expected: %d, Actual: %d", exp_num_trxns, num_trxns)
	}

	start_seq := 1 + before_seq // This is done to remove previous transaction if any
	if trxn_iter, err := rdb.db.GetUpdatesSince(start_seq); err != nil {
		t.Fatal(err)
	} else {
		defer trxn_iter.Destroy()
		for trxn_iter.Valid() {
			wb, _ := trxn_iter.GetBatch()
			num_trxns_per_wb := wb.Count()
			if num_trxns_per_wb != num_trxns_per_batch {
				t.Errorf("Incorrect number of transactions per batch. Expected: %d, Actual: %d", num_trxns_per_batch, num_trxns_per_wb)
			}
			wb_iter := wb.NewIterator()
			for wb_iter.Next() {
				wbr := wb_iter.Record()
				t.Logf("Type: %v, Key: %s, Val: %s", wbr.Type, wbr.Key, wbr.Value)
				switch wbr.Type {
				case 1: // Put
					if !strings.HasPrefix(string(wbr.Key), "bKey_") {
						t.Errorf("Invalid key for PUT record. Value: %s", wbr.Key)
					}
					if !strings.HasPrefix(string(wbr.Value), "bVal_") {
						t.Errorf("Invalid value inside write batch record for key: %s. Value: %s", wbr.Key, wbr.Value)
					}
				case 0: // Delete
					if !strings.HasPrefix(string(wbr.Key), "bKey_") {
						t.Errorf("Invalid key for DELETE record. Value: %s", wbr.Key)
					}
				default:
					t.Errorf("Invalid type: %v", wbr.Type)
				}
			}
			wb.Destroy()
			trxn_iter.Next()
		}
	}
}

func TestMultiGet(t *testing.T) {
	numKeys := 10
	keys, vals := make([][]byte, numKeys), make([]string, numKeys)
	for i := 1; i <= numKeys; i++ {
		key, value := fmt.Sprintf("MK%d", i), fmt.Sprintf("MV%d", i)
		if err := store.Put([]byte(key), []byte(value)).Error; err != nil {
			t.Fatalf("Unable to PUT. Key: %s, Value: %s, Error: %v", key, value, err)
		} else {
			keys[i-1] = []byte(key)
			vals[i-1] = value
		}
	}

	results := store.Get(keys...)
	for i, result := range results {
		if val, err := result.Value, result.Error; err != nil {
			t.Fatalf("Unable to Multi Get. Key: %s, Error: %v", keys[i], err)
		} else if string(val) != vals[i] {
			t.Errorf("Multi Get value mismatch. Key: %s, Expected Value: %s, Actual Value: %s", keys[i], vals[i], val)
		}
	}
}

func TestMissingGet(t *testing.T) {
	key, expectedValue := "MissingKey", ""
	readResult := store.Get([]byte(key))[0]
	if val, err := readResult.Value, readResult.Error; err != nil {
		t.Fatalf("Unable to GET. Key: %s, Error: %v", key, err)
	} else if string(val) != "" {
		t.Errorf("GET mismatch. Key: %s, Expected Value: %s, Actual Value: %s", key, expectedValue, val)
	}
}

func BenchmarkPutNewKeys(b *testing.B) {
	for i := 0; i < b.N; i++ {
		key, value := fmt.Sprintf("BK%d", i), fmt.Sprintf("BV%d", i)
		if err := store.Put([]byte(key), []byte(value)).Error; err != nil {
			b.Fatalf("Unable to PUT. Key: %s, Value: %s, Error: %v", key, value, err)
		}
	}
}

func BenchmarkPutExistingKey(b *testing.B) {
	key := "BKey"
	if err := store.Put([]byte(key), []byte("BVal")).Error; err != nil {
		b.Fatalf("Unable to PUT. Key: %s. Error: %v", key, err)
	}
	for i := 0; i < b.N; i++ {
		value := fmt.Sprintf("BVal%d", i)
		if err := store.Put([]byte(key), []byte(value)).Error; err != nil {
			b.Fatalf("Unable to PUT. Key: %s, Value: %s, Error: %v", key, value, err)
		}
	}
}

func BenchmarkGetKey(b *testing.B) {
	key, val := "BGetKey", "BGetVal"
	if err := store.Put([]byte(key), []byte(val)).Error; err != nil {
		b.Fatalf("Unable to PUT. Key: %s. Error: %v", key, err)
	}
	for i := 0; i < b.N; i++ {
		readResult := store.Get([]byte(key))[0]
		if value, err := readResult.Value, readResult.Error; err != nil {
			b.Fatalf("Unable to GET. Key: %s, Error: %v", key, err)
		} else if string(value) != val {
			b.Errorf("GET mismatch. Key: %s, Expected Value: %s, Actual Value: %s", key, val, value)
		}
	}
}

func BenchmarkGetMissingKey(b *testing.B) {
	key := "BMissingKey"
	for i := 0; i < b.N; i++ {
		if err := store.Get([]byte(key))[0].Error; err != nil {
			b.Fatalf("Unable to GET. Key: %s, Error: %v", key, err)
		}
	}
}

func openRocksDB() (storage.KVStore, error) {
	if err := exec.Command("rm", "-rf", dbFolder).Run(); err != nil {
		return nil, err
	}
	opts := NewDefaultOptions().DBFolder(dbFolder).CreateDBFolderIfMissing(createDBFolderIfMissing).CacheSize(cacheSize)
	return OpenStore(opts)
}
