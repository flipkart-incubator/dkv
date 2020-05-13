package badger

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/dgraph-io/badger"
	badger_pb "github.com/dgraph-io/badger/pb"
	"github.com/flipkart-incubator/dkv/internal/server/storage"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
)

const dbFolder = "/tmp/badger_storage_test"

var store *badgerDB

func TestMain(m *testing.M) {
	if kvs, err := openBadgerDB(); err != nil {
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
	if chngNum, err := store.GetLatestAppliedChangeNumber(); err != nil {
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
		if appldChng, err := store.SaveChanges(chngRecs); err != nil {
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
		if appldChng, err := store.SaveChanges(delChngRecs); err != nil {
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
	if chngNum, err := store.GetLatestAppliedChangeNumber(); err != nil {
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
		if appldChng, err := store.SaveChanges(chngRecs); err != nil {
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

func TestBackupFileValidity(t *testing.T) {
	expectError(t, checksForBackup(""))
	expectError(t, checksForBackup(dbFolder))
	expectNoError(t, checksForBackup("/tmp/backup.bak"))
}

func TestRestoreFileValidity(t *testing.T) {
	expectError(t, checksForRestore(""))
	expectError(t, checksForRestore(dbFolder))
	expectError(t, checksForRestore("/missing/backup.bak"))
}

func TestBackupAndRestore(t *testing.T) {
	numTrxns := 50
	keyPrefix, valPrefix := "brKey", "brVal"
	putKeys(t, numTrxns, keyPrefix, valPrefix)

	backupPath := fmt.Sprintf("%s/%s", dbFolder, "badger.bak")
	if err := store.BackupTo(backupPath); err != nil {
		t.Fatal(err)
	} else {
		missKeyPrefix, missValPrefix := "mbrKey", "mbrVal"
		putKeys(t, numTrxns, missKeyPrefix, missValPrefix)
		if err := store.RestoreFrom(backupPath); err != nil {
			t.Fatal(err)
		} else {
			getKeys(t, numTrxns, keyPrefix, valPrefix)
			noKeys(t, numTrxns, missKeyPrefix)
		}
	}
}

func TestGetPutSnapshot(t *testing.T) {
	numTrxns := 100
	keyPrefix1, valPrefix1, newValPrefix1 := "firSnapKey", "firSnapVal", "newFirSnapVal"
	putKeys(t, numTrxns, keyPrefix1, valPrefix1)

	if snap, err := store.GetSnapshot(); err != nil {
		t.Fatal(err)
	} else {
		putKeys(t, numTrxns, keyPrefix1, newValPrefix1)
		keyPrefix2, valPrefix2 := "secSnapKey", "secSnapVal"
		putKeys(t, numTrxns, keyPrefix2, valPrefix2)

		if err := store.PutSnapshot(snap); err != nil {
			t.Fatal(err)
		} else {
			getKeys(t, numTrxns, keyPrefix1, valPrefix1)
			getKeys(t, numTrxns, keyPrefix2, valPrefix2)
		}
	}
}

func TestIterationUsingStream(t *testing.T) {
	numTrxns := 100
	keyPrefix, valPrefix := "firKey", "firVal"
	putKeys(t, numTrxns, keyPrefix, valPrefix)

	actCnt := 0
	strm := store.db.NewStream()
	strm.Send = func(list *badger_pb.KVList) error {
		for _, kv := range list.Kv {
			k, v := string(kv.Key), string(kv.Value)
			if strings.HasPrefix(k, keyPrefix) && strings.HasPrefix(v, valPrefix) {
				actCnt++
			}
		}
		return nil
	}

	if err := strm.Orchestrate(context.Background()); err != nil {
		t.Fatal(err)
	}

	if actCnt != numTrxns {
		t.Errorf("Expected snapshot iterator to give %d keys, but only got %d keys", numTrxns, actCnt)
	}
}

func TestIteratorPrefixScan(t *testing.T) {
	numTrxns := 3
	keyPrefix1, valPrefix1 := "aaPrefixKey", "aaPrefixVal"
	putKeys(t, numTrxns, keyPrefix1, valPrefix1)
	keyPrefix2, valPrefix2 := "bbPrefixKey", "bbPrefixVal"
	putKeys(t, numTrxns, keyPrefix2, valPrefix2)
	keyPrefix3, valPrefix3 := "ccPrefixKey", "ccPrefixVal"
	putKeys(t, numTrxns, keyPrefix3, valPrefix3)

	prefix := []byte("bbPrefix")
	itOpts, err := storage.NewIteratorOptions(
		storage.IterationPrefixKey(prefix),
		storage.IterationStartKey(prefix),
	)
	if err != nil {
		t.Fatal(err)
	}
	it := store.Iterate(itOpts)
	defer it.Close()

	actCount := 0
	for it.HasNext() {
		key, val := it.Next()
		actCount++
		if strings.HasPrefix(string(key), string(prefix)) {
			t.Logf("Key: %s Value: %s\n", key, val)
		} else {
			t.Errorf("Expected key %s to have prefix %s", key, prefix)
		}
	}

	if numTrxns != actCount {
		t.Errorf("Expected %d records with prefix: %s. But got %d records.", numTrxns, prefix, actCount)
	}

}

func TestIterationUsingIterator(t *testing.T) {
	numTrxns := 100
	keyPrefix1, valPrefix1 := "firKey", "firVal"
	putKeys(t, numTrxns, keyPrefix1, valPrefix1)

	txn := store.db.NewTransaction(false)
	defer txn.Discard()

	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()

	keyPrefix2, valPrefix2 := "secKey", "secVal"
	putKeys(t, numTrxns, keyPrefix2, valPrefix2)

	actCnt := 0
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		k := string(item.Key())
		if val, err := item.ValueCopy(nil); err != nil {
			t.Fatal(err)
		} else {
			v := string(val)
			if strings.HasPrefix(k, keyPrefix1) && strings.HasPrefix(v, valPrefix1) {
				actCnt++
			}

			if strings.HasPrefix(k, keyPrefix2) || strings.HasPrefix(v, valPrefix2) {
				t.Errorf("Did not expect snapshot iterator to give key: %s with value: %s", k, v)
			}
		}
	}

	if actCnt != numTrxns {
		t.Errorf("Expected snapshot iterator to give %d keys, but only got %d keys", numTrxns, actCnt)
	}
}

func TestPreventParallelBackups(t *testing.T) {
	numTrxns := 500
	keyPrefix, valPrefix := "brKey", "brVal"
	putKeys(t, numTrxns, keyPrefix, valPrefix)

	parallelism := 5
	var succ, fail uint32
	var wg sync.WaitGroup
	wg.Add(parallelism)

	for i := 1; i <= parallelism; i++ {
		go func(n int) {
			defer wg.Done()
			backupPath := fmt.Sprintf("%s/%s_%d.bak", dbFolder, "backup", n)
			if err := store.BackupTo(backupPath); err != nil {
				atomic.AddUint32(&fail, 1)
				t.Log(err)
			} else {
				atomic.AddUint32(&succ, 1)
			}
		}(i)
	}
	wg.Wait()
	t.Logf("Successful backups: %d", succ)
	t.Logf("Failed backups: %d", fail)

	if succ > 1 || fail < uint32(parallelism-1) {
		t.Errorf("Only one backup must have succeeded.")
	}
}

func TestPreventParallelRestores(t *testing.T) {
	numTrxns := 500
	keyPrefix, valPrefix := "brKey", "brVal"
	putKeys(t, numTrxns, keyPrefix, valPrefix)
	backupPath := fmt.Sprintf("%s/%s.bak", dbFolder, "test_backup")
	if err := store.BackupTo(backupPath); err != nil {
		t.Fatal(err)
	}

	parallelism := 5
	var succ, fail uint32
	var wg sync.WaitGroup
	wg.Add(parallelism)

	for i := 1; i <= parallelism; i++ {
		go func(n int) {
			defer wg.Done()
			if err := store.RestoreFrom(backupPath); err != nil {
				atomic.AddUint32(&fail, 1)
				t.Log(err)
			} else {
				atomic.AddUint32(&succ, 1)
			}
		}(i)
	}
	wg.Wait()
	t.Logf("Successful restores: %d", succ)
	t.Logf("Failed restores: %d", fail)

	if succ > 1 || fail < uint32(parallelism-1) {
		t.Errorf("Only one restore must have succeeded.")
	}
}

func BenchmarkSaveChangesOneByOne(b *testing.B) {
	for i := 0; i < b.N; i++ {
		key, value := fmt.Sprintf("BSK%d", i), fmt.Sprintf("BSV%d", i)
		chngRecs := []*serverpb.ChangeRecord{newPutChange(uint64(i+1), []byte(key), []byte(value))}
		if _, err := store.SaveChanges(chngRecs); err != nil {
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

func BenchmarkStreamBasedIteration(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()
	numTrxns := b.N
	keyPrefix, valPrefix := "strmBenchKey", "strmBenchVal"
	data := putKeys(b, numTrxns, keyPrefix, valPrefix)
	b.StartTimer()

	actCnt := 0
	strm := store.db.NewStream()
	strm.Send = func(list *badger_pb.KVList) error {
		for _, kv := range list.Kv {
			k, v := string(kv.Key), string(kv.Value)
			if expVal, present := data[k]; present && expVal == v {
				actCnt++
			}
		}
		return nil
	}

	if err := strm.Orchestrate(context.Background()); err != nil {
		b.Fatal(err)
	}

	if actCnt != numTrxns {
		b.Errorf("Expected snapshot iterator to give %d keys, but only got %d keys", numTrxns, actCnt)
	}
}

func BenchmarkIteratorBasedIteration(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()
	numTrxns := b.N
	keyPrefix, valPrefix := "benchKey", "benchVal"
	data := putKeys(b, numTrxns, keyPrefix, valPrefix)
	b.StartTimer()

	txn := store.db.NewTransaction(false)
	defer txn.Discard()

	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()

	actCnt := 0
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		k := string(item.Key())
		if val, err := item.ValueCopy(nil); err != nil {
			b.Fatal(err)
		} else {
			if expVal, present := data[k]; present && expVal == string(val) {
				actCnt++
			}
		}
	}

	if actCnt != numTrxns {
		b.Errorf("Expected snapshot iterator to give %d keys, but only got %d keys", numTrxns, actCnt)
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

func noKeys(t *testing.T, numKeys int, keyPrefix string) {
	for i := 1; i <= numKeys; i++ {
		key := fmt.Sprintf("%s_%d", keyPrefix, i)
		if _, err := store.Get([]byte(key)); err == nil {
			t.Fatalf("Expected missing key. Key: %s", key)
		}
	}
}

func getKeys(t *testing.T, numKeys int, keyPrefix, valPrefix string) {
	for i := 1; i <= numKeys; i++ {
		key, expectedValue := fmt.Sprintf("%s_%d", keyPrefix, i), fmt.Sprintf("%s_%d", valPrefix, i)
		if readResults, err := store.Get([]byte(key)); err != nil {
			t.Errorf("Unable to GET. Key: %s, Error: %v", key, err)
		} else if string(readResults[0]) != expectedValue {
			t.Errorf("GET mismatch. Key: %s, Expected Value: %s, Actual Value: %s", key, expectedValue, readResults[0])
		}
	}
}

func putKeys(t testing.TB, numKeys int, keyPrefix, valPrefix string) map[string]string {
	data := make(map[string]string, numKeys)
	for i := 1; i <= numKeys; i++ {
		k, v := fmt.Sprintf("%s_%d", keyPrefix, i), fmt.Sprintf("%s_%d", valPrefix, i)
		if err := store.Put([]byte(k), []byte(v)); err != nil {
			t.Fatal(err)
		} else {
			if readResults, err := store.Get([]byte(k)); err != nil {
				t.Fatal(err)
			} else if string(readResults[0]) != string(v) {
				t.Errorf("GET mismatch. Key: %s, Expected Value: %s, Actual Value: %s", k, v, readResults[0])
			} else {
				data[k] = v
			}
		}
	}
	return data
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

func expectError(t *testing.T, err error) {
	if err == nil {
		t.Error("Expected an error but received none")
	}
}

func expectNoError(t *testing.T, err error) {
	if err != nil {
		t.Error("Expected no error but got error")
		t.Log(err)
	}
}

func openBadgerDB() (*badgerDB, error) {
	if err := exec.Command("rm", "-rf", dbFolder).Run(); err != nil {
		return nil, err
	}
	return openStore(NewOptions(dbFolder))
}
