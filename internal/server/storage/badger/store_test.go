package badger

import (
	"fmt"
	"os"
	"os/exec"
	"testing"

	"github.com/flipkart-incubator/dkv/internal/server/storage"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
)

var (
	store         storage.KVStore
	changeApplier storage.ChangeApplier
)

const (
	dbFolder = "/tmp/badger_storage_test"
)

func TestMain(m *testing.M) {
	if kvs, err := openBadgerDB(); err != nil {
		panic(err)
	} else {
		store, changeApplier = kvs, kvs
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
		if results, err := store.Get([]byte(key)); err != nil {
			t.Fatalf("Unable to GET. Key: %s, Error: %v", key, err)
		} else if string(results[0]) != expectedValue {
			t.Errorf("GET mismatch. Key: %s, Expected Value: %s, Actual Value: %s", key, expectedValue, results[0])
		}
	}
}

func TestMultiGet(t *testing.T) {
	numKeys := 10
	keys, vals := make([][]byte, numKeys), make([][]byte, numKeys)
	for i := 1; i <= numKeys; i++ {
		key, value := fmt.Sprintf("MK%d", i), fmt.Sprintf("MV%d", i)
		if err := store.Put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Unable to PUT. Key: %s, Value: %s, Error: %v", key, value, err)
		} else {
			keys[i-1] = []byte(key)
			vals[i-1] = []byte(value)
		}
	}

	checkGetResults(t, keys, vals)
}

func TestMissingGet(t *testing.T) {
	key := "MissingKey"
	if vals, err := store.Get([]byte(key)); err == nil {
		t.Errorf("Expected an error since given key must be missing. But got its value: %s", vals[0])
	}
}

func TestSaveChanges(t *testing.T) {
	if chng_num, err := changeApplier.GetLatestAppliedChangeNumber(); err != nil {
		t.Error(err)
	} else {
		num_chngs, key_pref, val_pref := 10, "KSC_", "VSC_"
		var ks, vs [][]byte
		chng_recs := make([]*serverpb.ChangeRecord, num_chngs)
		for i := 0; i < num_chngs; i++ {
			k := []byte(fmt.Sprintf("%s%d", key_pref, i))
			v := []byte(fmt.Sprintf("%s%d", val_pref, i))
			chng_recs[i] = &serverpb.ChangeRecord{
				ChangeNumber:  chng_num + uint64(i+1),
				NumberOfTrxns: 1,
				Trxns: []*serverpb.TrxnRecord{
					&serverpb.TrxnRecord{
						Type:  serverpb.TrxnRecord_Put,
						Key:   k,
						Value: v,
					},
				},
			}
			ks = append(ks, k)
			vs = append(vs, v)
		}
		if appld_chng, err := changeApplier.SaveChanges(chng_recs); err != nil {
			t.Error(err)
		} else {
			act_num_chngs := int(appld_chng - chng_num)
			if act_num_chngs != num_chngs {
				t.Errorf("Mismatch in the expected number of changes. Expected: %d, Actual: %d", num_chngs, act_num_chngs)
			} else {
				checkGetResults(t, ks, vs)
			}
		}
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
		if results, err := store.Get([]byte(key)); err != nil {
			b.Fatalf("Unable to GET. Key: %s, Error: %v", key, err)
		} else if string(results[0]) != val {
			b.Errorf("GET mismatch. Key: %s, Expected Value: %s, Actual Value: %s", key, val, results[0])
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

func checkGetResults(t *testing.T, ks, exp_vs [][]byte) {
	if results, err := store.Get(ks...); err != nil {
		t.Error(err)
	} else {
		for i, result := range results {
			if string(result) != string(exp_vs[i]) {
				t.Errorf("Get value mismatch. Key: %s, Expected Value: %s, Actual Value: %s", ks[i], exp_vs[i], result)
			}
		}
	}
}

func openBadgerDB() (*badgerDBStore, error) {
	if err := exec.Command("rm", "-rf", dbFolder).Run(); err != nil {
		return nil, err
	}
	opts := NewDefaultOptions(dbFolder)
	return OpenStore(opts)
}
