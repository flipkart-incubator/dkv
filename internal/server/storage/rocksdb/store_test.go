package rocksdb

import (
	"fmt"
	"os"
	"os/exec"
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
		}
	}

	after_seq := rdb.db.GetLatestSequenceNumber()
	num_trxns := int(after_seq - before_seq)
	if num_trxns != exp_num_trxns {
		t.Errorf("Incorrect number of transactions reported. Expected: %d, Actual: %d", exp_num_trxns, num_trxns)
	}

	if trxn_iter, err := rdb.db.GetUpdatesSince(before_seq); err != nil {
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
