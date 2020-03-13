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

func TestSaveChangesForPutAndDelete(t *testing.T) {
	if chng_num, err := changeApplier.GetLatestAppliedChangeNumber(); err != nil {
		t.Error(err)
	} else {
		num_chngs, key_pref, val_pref := 10, "KSC_", "VSC_"
		ks, vs := make([][]byte, num_chngs), make([][]byte, num_chngs)
		chng_recs := make([]*serverpb.ChangeRecord, num_chngs)
		for i := 0; i < num_chngs; i++ {
			ks[i] = []byte(fmt.Sprintf("%s%d", key_pref, i))
			vs[i] = []byte(fmt.Sprintf("%s%d", val_pref, i))
			chng_recs[i] = newPutChange(chng_num+uint64(i+1), ks[i], vs[i])
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
		chng_num = chng_num + uint64(num_chngs)
		del_num_chngs := 5
		del_chng_recs := make([]*serverpb.ChangeRecord, del_num_chngs)
		for i := 0; i < del_num_chngs; i++ {
			del_chng_recs[i] = newDelChange(chng_num+uint64(i+1), ks[i])
		}
		if appld_chng, err := changeApplier.SaveChanges(del_chng_recs); err != nil {
			t.Error(err)
		} else {
			act_num_chngs := int(appld_chng - chng_num)
			if act_num_chngs != del_num_chngs {
				t.Errorf("Mismatch in the expected number of changes. Expected: %d, Actual: %d", del_num_chngs, act_num_chngs)
			} else {
				checkMissingGetResults(t, ks[:act_num_chngs])
				checkGetResults(t, ks[act_num_chngs:], vs[act_num_chngs:])
			}
		}
	}
}

func TestSaveChangesForInterleavedPutAndDelete(t *testing.T) {
	if chng_num, err := changeApplier.GetLatestAppliedChangeNumber(); err != nil {
		t.Error(err)
	} else {
		num_chngs, key_pref, val_pref := 4, "KISC_", "VISC_"
		ks, vs := make([][]byte, num_chngs>>1), make([][]byte, num_chngs>>1)
		chng_recs := make([]*serverpb.ChangeRecord, num_chngs)
		for i, j := 0, 0; i < num_chngs; i++ {
			if i&1 == 0 { // PUT change for even indices
				ks[j] = []byte(fmt.Sprintf("%s%d", key_pref, j))
				vs[j] = []byte(fmt.Sprintf("%s%d", val_pref, j))
				chng_recs[i] = newPutChange(chng_num+uint64(i+1), ks[j], vs[j])
				j++
			} else { // DEL change for odd indices
				chng_recs[i] = newDelChange(chng_num+uint64(i+1), ks[j-1])
			}
		}
		if appld_chng, err := changeApplier.SaveChanges(chng_recs); err != nil {
			t.Error(err)
		} else {
			act_num_chngs := int(appld_chng - chng_num)
			if act_num_chngs != num_chngs {
				t.Errorf("Mismatch in the expected number of changes. Expected: %d, Actual: %d", num_chngs, act_num_chngs)
			} else {
				// As 'even' change records add keys and 'odd' ones remove the previous key, we expect no keys to be present
				checkMissingGetResults(t, ks)
			}
		}
	}
}

func BenchmarkSaveChangesOneByOne(b *testing.B) {
	for i := 0; i < b.N; i++ {
		key, value := fmt.Sprintf("BSK%d", i), fmt.Sprintf("BSV%d", i)
		chng_recs := []*serverpb.ChangeRecord{newPutChange(uint64(i+1), []byte(key), []byte(value))}
		if _, err := changeApplier.SaveChanges(chng_recs); err != nil {
			b.Fatalf("Unable to SaveChange for PUT. Key: %s, Value: %s, Error: %v", key, value, err)
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
		if res, err := store.Get([]byte(key)); err == nil {
			b.Fatalf("Expected an error on missing key, but got no error. Key: %s, Value: %s", key, res)
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

func checkMissingGetResults(t *testing.T, ks [][]byte) {
	for _, k := range ks {
		if result, err := store.Get(k); err == nil {
			t.Errorf("Expected missing entry for key: %s. But instead found value: %s", k, result)
		}
	}
}

func newDelChange(chngNum uint64, key []byte) *serverpb.ChangeRecord {
	return &serverpb.ChangeRecord{
		ChangeNumber:  chngNum,
		NumberOfTrxns: 1,
		Trxns: []*serverpb.TrxnRecord{
			{
				Type: serverpb.TrxnRecord_Delete,
				Key:  key,
			},
		},
	}
}

func newPutChange(chngNum uint64, key, val []byte) *serverpb.ChangeRecord {
	return &serverpb.ChangeRecord{
		ChangeNumber:  chngNum,
		NumberOfTrxns: 1,
		Trxns: []*serverpb.TrxnRecord{
			{
				Type:  serverpb.TrxnRecord_Put,
				Key:   key,
				Value: val,
			},
		},
	}
}

func openBadgerDB() (*badgerDB, error) {
	if err := exec.Command("rm", "-rf", dbFolder).Run(); err != nil {
		return nil, err
	}
	return openStore(newDefaultOptions(dbFolder))
}
