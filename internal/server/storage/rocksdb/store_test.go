package rocksdb

import (
	"fmt"
	"os"
	"os/exec"
	"testing"

	"github.com/flipkart-incubator/dkv/internal/server/storage"
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
