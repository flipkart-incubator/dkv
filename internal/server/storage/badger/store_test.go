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
	if chngNum, err := changeApplier.GetLatestAppliedChangeNumber(); err != nil {
		t.Error(err)
	} else {
		numChngs, keyPref, valPref := 10, "KSC_", "VSC_"
		ks, vs := make([][]byte, numChngs), make([][]byte, numChngs)
		chngRecs := make([]*serverpb.ChangeRecord, numChngs)
		for i := 0; i < numChngs; i++ {
			ks[i] = []byte(fmt.Sprintf("%s%d", keyPref, i))
			vs[i] = []byte(fmt.Sprintf("%s%d", valPref, i))
			chngRecs[i] = newPutChange(chngNum+uint64(i+1), ks[i], vs[i])
		}
		if appldChng, err := changeApplier.SaveChanges(chngRecs); err != nil {
			t.Error(err)
		} else {
			actNumChngs := int(appldChng - chngNum)
			if actNumChngs != numChngs {
				t.Errorf("Mismatch in the expected number of changes. Expected: %d, Actual: %d", numChngs, actNumChngs)
			} else {
				checkGetResults(t, ks, vs)
			}
		}
		chngNum = chngNum + uint64(numChngs)
		delNumChngs := 5
		delChngRecs := make([]*serverpb.ChangeRecord, delNumChngs)
		for i := 0; i < delNumChngs; i++ {
			delChngRecs[i] = newDelChange(chngNum+uint64(i+1), ks[i])
		}
		if appldChng, err := changeApplier.SaveChanges(delChngRecs); err != nil {
			t.Error(err)
		} else {
			actNumChngs := int(appldChng - chngNum)
			if actNumChngs != delNumChngs {
				t.Errorf("Mismatch in the expected number of changes. Expected: %d, Actual: %d", delNumChngs, actNumChngs)
			} else {
				checkMissingGetResults(t, ks[:actNumChngs])
				checkGetResults(t, ks[actNumChngs:], vs[actNumChngs:])
			}
		}
	}
}

func TestSaveChangesForInterleavedPutAndDelete(t *testing.T) {
	if chngNum, err := changeApplier.GetLatestAppliedChangeNumber(); err != nil {
		t.Error(err)
	} else {
		numChngs, keyPref, valPref := 4, "KISC_", "VISC_"
		ks, vs := make([][]byte, numChngs>>1), make([][]byte, numChngs>>1)
		chngRecs := make([]*serverpb.ChangeRecord, numChngs)
		for i, j := 0, 0; i < numChngs; i++ {
			if i&1 == 0 { // PUT change for even indices
				ks[j] = []byte(fmt.Sprintf("%s%d", keyPref, j))
				vs[j] = []byte(fmt.Sprintf("%s%d", valPref, j))
				chngRecs[i] = newPutChange(chngNum+uint64(i+1), ks[j], vs[j])
				j++
			} else { // DEL change for odd indices
				chngRecs[i] = newDelChange(chngNum+uint64(i+1), ks[j-1])
			}
		}
		if appldChng, err := changeApplier.SaveChanges(chngRecs); err != nil {
			t.Error(err)
		} else {
			actNumChngs := int(appldChng - chngNum)
			if actNumChngs != numChngs {
				t.Errorf("Mismatch in the expected number of changes. Expected: %d, Actual: %d", numChngs, actNumChngs)
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
		chngRecs := []*serverpb.ChangeRecord{newPutChange(uint64(i+1), []byte(key), []byte(value))}
		if _, err := changeApplier.SaveChanges(chngRecs); err != nil {
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

func checkGetResults(t *testing.T, ks, expVs [][]byte) {
	if results, err := store.Get(ks...); err != nil {
		t.Error(err)
	} else {
		for i, result := range results {
			if string(result) != string(expVs[i]) {
				t.Errorf("Get value mismatch. Key: %s, Expected Value: %s, Actual Value: %s", ks[i], expVs[i], result)
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
