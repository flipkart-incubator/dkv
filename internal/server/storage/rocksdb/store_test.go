package rocksdb

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/flipkart-incubator/dkv/internal/server/storage"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
	"github.com/tecbot/gorocksdb"
)

var (
	store            storage.KVStore
	changePropagator storage.ChangePropagator
	changeApplier    storage.ChangeApplier
)

const (
	createDBFolderIfMissing = true
	dbFolder                = "/tmp/rocksdb_storage_test"
	cacheSize               = 3 << 30
)

func TestMain(m *testing.M) {
	if kvs, err := openRocksDB(); err != nil {
		panic(err)
	} else {
		store, changePropagator, changeApplier = kvs, kvs, kvs
		res := m.Run()
		store.Close()
		os.Exit(res)
	}
}

func TestPutAndGet(t *testing.T) {
	numKeys := 10
	for i := 1; i <= numKeys; i++ {
		key, value := fmt.Sprintf("K%d", i), fmt.Sprintf("V%d", i)
		if err := store.Put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Unable to PUT. Key: %s, Value: %s, Error: %v", key, value, err)
		}
	}

	for i := 1; i <= numKeys; i++ {
		key, expectedValue := fmt.Sprintf("K%d", i), fmt.Sprintf("V%d", i)
		if readResults, err := store.Get([]byte(key)); err != nil {
			t.Fatalf("Unable to GET. Key: %s, Error: %v", key, err)
		} else {
			if string(readResults[0]) != expectedValue {
				t.Errorf("GET mismatch. Key: %s, Expected Value: %s, Actual Value: %s", key, expectedValue, readResults[0])
			}
		}
	}
}

func TestGetLatestChangeNumber(t *testing.T) {
	exp_num_trxns := uint64(5)
	before_chng_num := changePropagator.GetLatestChangeNumber()
	putKeys(t, int(exp_num_trxns), "aaKey", "aaVal")
	after_chng_num := changePropagator.GetLatestChangeNumber()
	act_num_trxns := after_chng_num - before_chng_num
	if exp_num_trxns != act_num_trxns {
		t.Errorf("Mismatch in number of transactions. Expected: %d, Actual: %d", exp_num_trxns, act_num_trxns)
	}
	before_chng_num = after_chng_num
	getKeys(t, int(exp_num_trxns), "aaKey", "aaVal")
	after_chng_num = changePropagator.GetLatestChangeNumber()
	act_num_trxns = after_chng_num - before_chng_num
	if act_num_trxns != 0 {
		t.Errorf("Expected no transactions to have occurred but found %d transactions", act_num_trxns)
	}
}

func TestLoadChanges(t *testing.T) {
	exp_num_trxns, max_chngs := 3, 8
	key_prefix, val_prefix := "bbKey", "bbVal"
	chng_num := 1 + changePropagator.GetLatestChangeNumber() // due to possible previous transaction
	putKeys(t, exp_num_trxns, key_prefix, val_prefix)
	if chngs, err := changePropagator.LoadChanges(chng_num, max_chngs); err != nil {
		t.Fatal(err)
	} else {
		exp_num_chngs, act_num_chngs := 3, len(chngs)
		if exp_num_chngs != act_num_chngs {
			t.Errorf("Incorrect number of changes retrieved. Expected: %d, Actual: %d", exp_num_chngs, act_num_chngs)
		}
		first_chng_num := chngs[0].ChangeNumber
		if first_chng_num != chng_num {
			t.Errorf("Expected first change number to be %d but it is %d", chng_num, first_chng_num)
		}
		for i := 0; i < act_num_chngs; i++ {
			chng := chngs[i]
			// t.Log(string(chng.SerialisedForm))
			if chng.NumberOfTrxns != 1 {
				t.Errorf("Expected only one transaction in this change but found %d transactions", chng.NumberOfTrxns)
			}
			trxn_rec := chng.Trxns[0]
			if trxn_rec.Type != serverpb.TrxnRecord_Put {
				t.Errorf("Expected transaction type to be Put but found %s", trxn_rec.Type.String())
			}
			exp_key, exp_val := fmt.Sprintf("%s_%d", key_prefix, i+1), fmt.Sprintf("%s_%d", val_prefix, i+1)
			act_key, act_val := string(trxn_rec.Key), string(trxn_rec.Value)
			if exp_key != act_key {
				t.Errorf("Key mismatch. Expected: %s, Actual: %s", exp_key, act_key)
			}
			if exp_val != act_val {
				t.Errorf("Value mismatch. Expected: %s, Actual: %s", exp_val, act_val)
			}
		}
	}
}

func TestSaveChanges(t *testing.T) {
	num_trxns := 3
	put_key_prefix, put_val_prefix := "ccKey", "ccVal"
	putKeys(t, num_trxns, put_key_prefix, put_val_prefix)
	chng_num := 1 + changePropagator.GetLatestChangeNumber() // due to possible previous transaction
	wb_put_key_prefix, wb_put_val_prefix := "ddKey", "ddVal"
	chngs := make([]*serverpb.ChangeRecord, num_trxns)
	for i := 0; i < num_trxns; i++ {
		wb := gorocksdb.NewWriteBatch()
		defer wb.Destroy()
		ks, vs := fmt.Sprintf("%s_%d", wb_put_key_prefix, i+1), fmt.Sprintf("%s_%d", wb_put_val_prefix, i+1)
		wb.Put([]byte(ks), []byte(vs))
		del_ks := fmt.Sprintf("%s_%d", put_key_prefix, i+1)
		wb.Delete([]byte(del_ks))
		chngs[i] = toChangeRecord(wb, chng_num)
		chng_num++
	}
	exp_chng_num := chng_num - 1

	if act_chng_num, err := changeApplier.SaveChanges(chngs); err != nil {
		t.Fatal(err)
	} else {
		if exp_chng_num != act_chng_num {
			t.Errorf("Change numbers mismatch. Expected: %d, Actual: %d", exp_chng_num, act_chng_num)
		}
		getKeys(t, num_trxns, wb_put_key_prefix, wb_put_val_prefix)
		noKeys(t, num_trxns, put_key_prefix)
	}
}

// Following test can be removed once DKV supports bulk writes
func TestGetUpdatesFromSeqNumForBatches(t *testing.T) {
	rdb := store.(*rocksDB)
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
				// t.Logf("Type: %v, Key: %s, Val: %s", wbr.Type, wbr.Key, wbr.Value)
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
		if err := store.Put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Unable to PUT. Key: %s, Value: %s, Error: %v", key, value, err)
		} else {
			keys[i-1] = []byte(key)
			vals[i-1] = value
		}
	}

	if results, err := store.Get(keys...); err != nil {
		t.Fatal(err)
	} else {
		for i, result := range results {
			if string(result) != vals[i] {
				t.Errorf("Multi Get value mismatch. Key: %s, Expected Value: %s, Actual Value: %s", keys[i], vals[i], result)
			}
		}
	}
}

func TestMissingGet(t *testing.T) {
	key, expectedValue := "MissingKey", ""
	if readResults, err := store.Get([]byte(key)); err != nil {
		t.Fatal(err)
	} else if string(readResults[0]) != "" {
		t.Errorf("GET mismatch. Key: %s, Expected Value: %s, Actual Value: %s", key, expectedValue, readResults[0])
	}
}

func BenchmarkPutNewKeys(b *testing.B) {
	for i := 0; i < b.N; i++ {
		key, value := fmt.Sprintf("BK%d", i), fmt.Sprintf("BV%d", i)
		if err := store.Put([]byte(key), []byte(value)); err != nil {
			b.Fatalf("Unable to PUT. Key: %s, Value: %s, Error: %v", key, value, err)
		}
	}
}

func BenchmarkPutExistingKey(b *testing.B) {
	key := "BKey"
	if err := store.Put([]byte(key), []byte("BVal")); err != nil {
		b.Fatalf("Unable to PUT. Key: %s. Error: %v", key, err)
	}
	for i := 0; i < b.N; i++ {
		value := fmt.Sprintf("BVal%d", i)
		if err := store.Put([]byte(key), []byte(value)); err != nil {
			b.Fatalf("Unable to PUT. Key: %s, Value: %s, Error: %v", key, value, err)
		}
	}
}

func BenchmarkGetKey(b *testing.B) {
	key, val := "BGetKey", "BGetVal"
	if err := store.Put([]byte(key), []byte(val)); err != nil {
		b.Fatalf("Unable to PUT. Key: %s. Error: %v", key, err)
	}
	for i := 0; i < b.N; i++ {
		if readResults, err := store.Get([]byte(key)); err != nil {
			b.Fatalf("Unable to GET. Key: %s, Error: %v", key, err)
		} else if string(readResults[0]) != val {
			b.Errorf("GET mismatch. Key: %s, Expected Value: %s, Actual Value: %s", key, val, readResults[0])
		}
	}
}

func BenchmarkGetMissingKey(b *testing.B) {
	key := "BMissingKey"
	for i := 0; i < b.N; i++ {
		if _, err := store.Get([]byte(key)); err != nil {
			b.Fatalf("Unable to GET. Key: %s, Error: %v", key, err)
		}
	}
}

func noKeys(t *testing.T, numKeys int, keyPrefix string) {
	for i := 1; i <= numKeys; i++ {
		key := fmt.Sprintf("%s_%d", keyPrefix, i)
		if readResults, err := store.Get([]byte(key)); err != nil {
			t.Fatalf("Unable to GET. Key: %s, Error: %v", key, err)
		} else if string(readResults[0]) != "" {
			t.Errorf("Expected missing for key: %s. But found it with value: %s", key, readResults[0])
		}
	}
}

func getKeys(t *testing.T, numKeys int, keyPrefix, valPrefix string) {
	for i := 1; i <= numKeys; i++ {
		key, expectedValue := fmt.Sprintf("%s_%d", keyPrefix, i), fmt.Sprintf("%s_%d", valPrefix, i)
		if readResults, err := store.Get([]byte(key)); err != nil {
			t.Fatalf("Unable to GET. Key: %s, Error: %v", key, err)
		} else if string(readResults[0]) != expectedValue {
			t.Errorf("GET mismatch. Key: %s, Expected Value: %s, Actual Value: %s", key, expectedValue, readResults[0])
		}
	}
}

func putKeys(t *testing.T, numKeys int, keyPrefix, valPrefix string) {
	for i := 1; i <= numKeys; i++ {
		k, v := fmt.Sprintf("%s_%d", keyPrefix, i), fmt.Sprintf("%s_%d", valPrefix, i)
		if err := store.Put([]byte(k), []byte(v)); err != nil {
			t.Fatal(err)
		} else {
			if read_results, err := store.Get([]byte(k)); err != nil {
				t.Fatal(err)
			} else if string(read_results[0]) != string(v) {
				t.Errorf("GET mismatch. Key: %s, Expected Value: %s, Actual Value: %s", k, v, read_results[0])
			}
		}
	}
}

func openRocksDB() (*rocksDB, error) {
	if err := exec.Command("rm", "-rf", dbFolder).Run(); err != nil {
		return nil, err
	}
	opts := NewDefaultOptions().DBFolder(dbFolder).CreateDBFolderIfMissing(createDBFolderIfMissing).CacheSize(cacheSize)
	return OpenStore(opts)
}
