package rocksdb

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/flipkart-incubator/dkv/internal/storage"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
	"github.com/flipkart-incubator/gorocksdb"
)

const (
	dbFolder  = "/tmp/rocksdb_storage_test"
	cacheSize = 3 << 30
)

var store *rocksDB

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
		if err := store.Put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Unable to PUT. Key: %s, Value: %s, Error: %v", key, value, err)
		}
	}

	for i := 1; i <= numKeys; i++ {
		key, expectedValue := fmt.Sprintf("K%d", i), fmt.Sprintf("V%d", i)
		if readResults, err := store.Get([]byte(key)); err != nil {
			t.Fatalf("Unable to GET. Key: %s, Error: %v", key, err)
		} else {
			if string(readResults[0].Value) != expectedValue {
				t.Errorf("GET mismatch. Key: %s, Expected Value: %s, Actual Value: %s", key, expectedValue, readResults[0].Value)
			}
		}
	}
}

func TestPutEmptyValue(t *testing.T) {
	key, val := "EmptyKey", ""
	if err := store.Put([]byte(key), []byte(val)); err != nil {
		t.Fatalf("Unable to PUT empty value. Key: %s", key)
	}

	if res, err := store.Get([]byte(key)); err != nil {
		t.Fatalf("Unable to GET empty value. Key: %s", key)
	} else {
		t.Logf("Got value: '%v'", res)
	}

	// update nil value for same key
	if err := store.Put([]byte(key), nil); err != nil {
		t.Fatalf("Unable to PUT empty value. Key: %s", key)
	}

	if res, err := store.Get([]byte(key)); err != nil {
		t.Fatalf("Unable to GET empty value. Key: %s", key)
	} else {
		t.Logf("Got value: '%v'", res)
	}
}

func TestDelete(t *testing.T) {
	key, val := "SomeKey", "SomeValue"
	if err := store.Put([]byte(key), []byte(val)); err != nil {
		t.Fatalf("Unable to PUT. Key: %s", key)
	}

	if res, err := store.Get([]byte(key)); err != nil {
		t.Fatalf("Unable to GET. Key: %s", key)
	} else {
		t.Logf("Got value: '%s'", string(res[0].Value))
	}

	// delete key
	if err := store.Delete([]byte(key)); err != nil {
		t.Fatalf("Unable to delete Key: %s", key)
	}

	if res, err := store.Get([]byte(key)); err != nil {
		t.Fatalf("Got Exception while trying to GET deleted key value. Key: %s", key)
	} else if len(res) != 0 {
		t.Fatalf("Got Result while trying to GET deleted key value. Key: %s", key)
	}
}

func TestGetLatestChangeNumber(t *testing.T) {
	expNumTrxns := uint64(5)
	beforeChngNum, _ := store.GetLatestCommittedChangeNumber()
	putKeys(t, int(expNumTrxns), "aaKey", "aaVal")
	afterChngNum, _ := store.GetLatestCommittedChangeNumber()
	actNumTrxns := afterChngNum - beforeChngNum
	if expNumTrxns != actNumTrxns {
		t.Errorf("Mismatch in number of transactions. Expected: %d, Actual: %d", expNumTrxns, actNumTrxns)
	}
	beforeChngNum = afterChngNum
	getKeys(t, int(expNumTrxns), "aaKey", "aaVal")
	afterChngNum, _ = store.GetLatestCommittedChangeNumber()
	actNumTrxns = afterChngNum - beforeChngNum
	if actNumTrxns != 0 {
		t.Errorf("Expected no transactions to have occurred but found %d transactions", actNumTrxns)
	}
}

func TestLoadChanges(t *testing.T) {
	expNumTrxns, maxChngs := 3, 8
	keyPrefix, valPrefix := "bbKey", "bbVal"
	chngNum, _ := store.GetLatestCommittedChangeNumber()
	chngNum++ // due to possible previous transaction
	putKeys(t, expNumTrxns, keyPrefix, valPrefix)
	if chngs, err := store.LoadChanges(chngNum, maxChngs); err != nil {
		t.Fatal(err)
	} else {
		expNumChngs, actNumChngs := 3, len(chngs)
		if expNumChngs != actNumChngs {
			t.Errorf("Incorrect number of changes retrieved. Expected: %d, Actual: %d", expNumChngs, actNumChngs)
		}
		firstChngNum := chngs[0].ChangeNumber
		if firstChngNum != chngNum {
			t.Errorf("Expected first change number to be %d but it is %d", chngNum, firstChngNum)
		}
		for i := 0; i < actNumChngs; i++ {
			chng := chngs[i]
			// t.Log(string(chng.SerialisedForm))
			if chng.NumberOfTrxns != 1 {
				t.Errorf("Expected only one transaction in this change but found %d transactions", chng.NumberOfTrxns)
			}
			trxnRec := chng.Trxns[0]
			if trxnRec.Type != serverpb.TrxnRecord_Put {
				t.Errorf("Expected transaction type to be Put but found %s", trxnRec.Type.String())
			}
			expKey, expVal := fmt.Sprintf("%s_%d", keyPrefix, i+1), fmt.Sprintf("%s_%d", valPrefix, i+1)
			actKey, actVal := string(trxnRec.Key), string(trxnRec.Value)
			if expKey != actKey {
				t.Errorf("Key mismatch. Expected: %s, Actual: %s", expKey, actKey)
			}
			if expVal != actVal {
				t.Errorf("Value mismatch. Expected: %s, Actual: %s", expVal, actVal)
			}
		}
	}
}

func TestSaveChanges(t *testing.T) {
	numTrxns := 3
	putKeyPrefix, putValPrefix := "ccKey", "ccVal"
	putKeys(t, numTrxns, putKeyPrefix, putValPrefix)
	chngNum, _ := store.GetLatestCommittedChangeNumber()
	chngNum++ // due to possible previous transaction
	wbPutKeyPrefix, wbPutValPrefix := "ddKey", "ddVal"
	chngs := make([]*serverpb.ChangeRecord, numTrxns)
	for i := 0; i < numTrxns; i++ {
		wb := gorocksdb.NewWriteBatch()
		defer wb.Destroy()
		ks, vs := fmt.Sprintf("%s_%d", wbPutKeyPrefix, i+1), fmt.Sprintf("%s_%d", wbPutValPrefix, i+1)
		wb.Put([]byte(ks), []byte(vs))
		delKs := fmt.Sprintf("%s_%d", putKeyPrefix, i+1)
		wb.Delete([]byte(delKs))
		chngs[i] = toChangeRecord(wb, chngNum)
		chngNum++
	}
	expChngNum := chngNum - 1

	if actChngNum, err := store.SaveChanges(chngs); err != nil {
		t.Fatal(err)
	} else {
		if expChngNum != actChngNum {
			t.Errorf("Change numbers mismatch. Expected: %d, Actual: %d", expChngNum, actChngNum)
		}
		getKeys(t, numTrxns, wbPutKeyPrefix, wbPutValPrefix)
		noKeys(t, numTrxns, putKeyPrefix)
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

	if err := it.Err(); err != nil {
		t.Fatal(err)
	}

	if numTrxns != actCount {
		t.Errorf("Expected %d records with prefix: %s. But got %d records.", numTrxns, prefix, actCount)
	}
}

func TestIteratorFromStartKey(t *testing.T) {
	numTrxns := 3
	keyPrefix1, valPrefix1 := "StartKeyAA", "aaStartVal"
	putKeys(t, numTrxns, keyPrefix1, valPrefix1)
	keyPrefix2, valPrefix2 := "StartKeyBB", "bbStartVal"
	putKeys(t, numTrxns, keyPrefix2, valPrefix2)
	keyPrefix3, valPrefix3 := "StartKeyCC", "ccStartVal"
	putKeys(t, numTrxns, keyPrefix3, valPrefix3)

	prefix, startKey := []byte("StartKey"), []byte("StartKeyBB_2")
	itOpts, err := storage.NewIteratorOptions(
		storage.IterationPrefixKey(prefix),
		storage.IterationStartKey(startKey),
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

	expCount := 5
	if expCount != actCount {
		t.Errorf("Expected %d records with prefix: %s, start key: %s. But got %d records.", expCount, prefix, startKey, actCount)
	}
}

// Following test can be removed once DKV supports bulk writes
func TestGetUpdatesFromSeqNumForBatches(t *testing.T) {
	beforeSeq := store.db.GetLatestSequenceNumber()

	expNumBatchTrxns := 3
	numTrxnsPerBatch := 2
	expNumTrxns := expNumBatchTrxns * numTrxnsPerBatch
	for i := 1; i <= expNumBatchTrxns; i++ {
		k, v := fmt.Sprintf("bKey_%d", i), fmt.Sprintf("bVal_%d", i)
		wb := gorocksdb.NewWriteBatch()
		wb.Put([]byte(k), []byte(v))
		wb.Delete([]byte(k))
		wo := gorocksdb.NewDefaultWriteOptions()
		wo.SetSync(true)
		if err := store.db.Write(wo, wb); err != nil {
			t.Fatal(err)
		}
		wb.Destroy()
		wo.Destroy()
	}

	afterSeq := store.db.GetLatestSequenceNumber()
	numTrxns := int(afterSeq - beforeSeq)
	if numTrxns != expNumTrxns {
		t.Errorf("Incorrect number of transactions reported. Expected: %d, Actual: %d", expNumTrxns, numTrxns)
	}

	startSeq := 1 + beforeSeq // This is done to remove previous transaction if any
	if trxnIter, err := store.db.GetUpdatesSince(startSeq); err != nil {
		t.Fatal(err)
	} else {
		defer trxnIter.Destroy()
		for trxnIter.Valid() {
			wb, _ := trxnIter.GetBatch()
			numTrxnsPerWb := wb.Count()
			if numTrxnsPerWb != numTrxnsPerBatch {
				t.Errorf("Incorrect number of transactions per batch. Expected: %d, Actual: %d", numTrxnsPerBatch, numTrxnsPerWb)
			}
			wbIter := wb.NewIterator()
			for wbIter.Next() {
				wbr := wbIter.Record()
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
			trxnIter.Next()
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
			if string(result.Value) != vals[i] {
				t.Errorf("Multi Get value mismatch. Key: %s, Expected Value: %s, Actual Value: %s", keys[i], vals[i], result)
			}
		}
	}
}

func TestMissingGet(t *testing.T) {
	key := "MissingKey"
	if readResults, err := store.Get([]byte(key)); err != nil {
		t.Errorf("Expected no error since given key is only missing. But got error: %v", err)
	} else if len(readResults) > 0 {
		t.Errorf("Expected no values for missing key. Key: %s, Actual Value: %v", key, readResults)
	}
}

func TestBackupFolderValidity(t *testing.T) {
	expectError(t, checksForBackup(""))
	expectError(t, checksForBackup("/missing/backup"))
	expectNoError(t, checksForBackup("/tmp/backup.bak"))
	expectNoError(t, checksForBackup("/missing"))
	expectNoError(t, checksForBackup(dbFolder))
}

func TestRestoreFolderValidity(t *testing.T) {
	expectError(t, checksForRestore(""))
	expectError(t, checksForRestore("/missing/backup"))
	expectError(t, checksForRestore("/missing"))
	expectNoError(t, checksForRestore(dbFolder))
}

func TestBackupAndRestore(t *testing.T) {
	numTrxns, keyPrefix, valPrefix := 5, "brKey", "brVal"
	putKeys(t, numTrxns, keyPrefix, valPrefix)

	backupPath := fmt.Sprintf("%s/%s", dbFolder, "backup")
	if err := store.BackupTo(backupPath); err != nil {
		t.Fatal(err)
	} else {
		missKeyPrefix, missValPrefix := "mbrKey", "mbrVal"
		putKeys(t, numTrxns, missKeyPrefix, missValPrefix)
		store.Close()
		if st, _, _, _, err := store.RestoreFrom(backupPath); err != nil {
			t.Fatal(err)
		} else {
			store = st.(*rocksDB)
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

func TestIterationOnExplicitSnapshot(t *testing.T) {
	numTrxns := 100
	keyPrefix1, valPrefix1 := "firKey", "firVal"
	putKeys(t, numTrxns, keyPrefix1, valPrefix1)

	snap := store.db.NewSnapshot()
	defer store.db.ReleaseSnapshot(snap)

	keyPrefix2, valPrefix2 := "secKey", "secVal"
	putKeys(t, numTrxns, keyPrefix2, valPrefix2)

	readOpts := gorocksdb.NewDefaultReadOptions()
	defer readOpts.Destroy()

	readOpts.SetSnapshot(snap)
	it := store.db.NewIterator(readOpts)
	defer it.Close()

	actCnt := 0
	for it.SeekToFirst(); it.Valid(); it.Next() {
		k, v := string(it.Key().Data()), string(it.Value().Data())
		if strings.HasPrefix(k, keyPrefix1) && strings.HasPrefix(v, valPrefix1) {
			actCnt++
		}

		if strings.HasPrefix(k, keyPrefix2) || strings.HasPrefix(v, valPrefix2) {
			t.Errorf("Did not expect snapshot iterator to give key: %s with value: %s", k, v)
		}
	}

	if err := it.Err(); err != nil {
		t.Fatal(err)
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
			backupPath := fmt.Sprintf("%s/%s_%d", dbFolder, "backup", n)
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
	backupPath := fmt.Sprintf("%s/%s", dbFolder, "test_backup")
	if err := store.BackupTo(backupPath); err != nil {
		t.Fatal(err)
	}

	parallelism := 5
	var succ, fail uint32
	var wg sync.WaitGroup
	wg.Add(parallelism)

	store.Close()
	for i := 1; i <= parallelism; i++ {
		go func(n int) {
			defer wg.Done()
			if st, _, _, _, err := store.RestoreFrom(backupPath); err != nil {
				atomic.AddUint32(&fail, 1)
				t.Log(err)
			} else {
				store = st.(*rocksDB)
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

func TestOptimisticTransactions(t *testing.T) {
	name := fmt.Sprintf("%s-TestOptimTrans", store.opts.folderName)
	opts := store.opts.rocksDBOpts
	ro := gorocksdb.NewDefaultReadOptions()
	wo := gorocksdb.NewDefaultWriteOptions()
	to := gorocksdb.NewDefaultOptimisticTransactionOptions()

	tdb, err := gorocksdb.OpenOptimisticTransactionDb(opts, name)
	if err != nil {
		t.Errorf("Unable to open optimistic transaction DB. Error: %v", err)
	}
	defer tdb.Close()

	ctrKey := []byte("num")
	bdb := tdb.GetBaseDb()
	err = bdb.Put(wo, ctrKey, []byte{0})
	if err != nil {
		t.Errorf("Unable to PUT using base DB of optimistic transaction. Error: %v", err)
	}

	targetCnt := 10
	var wg sync.WaitGroup
	for i := 1; i <= targetCnt; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				txn := tdb.TransactionBegin(wo, to, nil)
				cnt, err := txn.GetForUpdate(ro, ctrKey)
				if err != nil {
					t.Errorf("Unable to GetForUpdate. Error: %v", err)
				}
				val := cnt.Data()[0]
				newVal := val + 1
				err = txn.Put(ctrKey, []byte{newVal})
				if err != nil {
					t.Errorf("Unable to PUT. Error: %v", err)
				}
				err = txn.Commit()
				cnt.Free()
				txn.Destroy()
				if err == nil {
					break
				}
			}
		}()
	}
	wg.Wait()
	cnt, err := bdb.Get(ro, ctrKey)
	defer cnt.Free()
	if err != nil {
		t.Errorf("Unable to GET using base DB of optimistic transaction. Error: %v", err)
	}
	val := cnt.Data()[0]
	if val != byte(targetCnt) {
		t.Errorf("Value mismatch for key: %s. Expected: %d, Actual: %d", ctrKey, targetCnt, val)
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
		} else if string(readResults[0].Value) != val {
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

func BenchmarkIteration(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()
	numTrxns := b.N
	keyPrefix, valPrefix := "snapBenchKey", "snapBenchVal"
	data := putKeys(b, numTrxns, keyPrefix, valPrefix)
	b.StartTimer()

	snap := store.db.NewSnapshot()
	defer store.db.ReleaseSnapshot(snap)

	readOpts := gorocksdb.NewDefaultReadOptions()
	defer readOpts.Destroy()

	readOpts.SetSnapshot(snap)
	it := store.db.NewIterator(readOpts)
	defer it.Close()

	actCnt := 0
	for it.SeekToFirst(); it.Valid(); it.Next() {
		k, v := string(it.Key().Data()), string(it.Value().Data())
		if expVal, present := data[k]; present && expVal == v {
			actCnt++
		}
	}

	if err := it.Err(); err != nil {
		b.Fatal(err)
	}

	if actCnt != numTrxns {
		b.Errorf("Expected snapshot iterator to give %d keys, but only got %d keys", numTrxns, actCnt)
	}
}

func noKeys(t *testing.T, numKeys int, keyPrefix string) {
	for i := 1; i <= numKeys; i++ {
		key := fmt.Sprintf("%s_%d", keyPrefix, i)
		if readResults, err := store.Get([]byte(key)); err != nil {
			t.Fatalf("Unable to GET. Key: %s, Error: %v", key, err)
		} else if len(readResults) > 0 {
			t.Errorf("Expected missing for key: %s. But found it with value: %v", key, readResults)
		}
	}
}

func getKeys(t *testing.T, numKeys int, keyPrefix, valPrefix string) {
	for i := 1; i <= numKeys; i++ {
		key, expectedValue := fmt.Sprintf("%s_%d", keyPrefix, i), fmt.Sprintf("%s_%d", valPrefix, i)
		if readResults, err := store.Get([]byte(key)); err != nil {
			t.Fatalf("Unable to GET. Key: %s, Error: %v", key, err)
		} else if string(readResults[0].Value) != expectedValue {
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
			} else if string(readResults[0].Value) != string(v) {
				t.Errorf("GET mismatch. Key: %s, Expected Value: %s, Actual Value: %s", k, v, readResults[0])
			} else {
				data[k] = v
			}
		}
	}
	return data
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

func openRocksDB() (*rocksDB, error) {
	if err := exec.Command("rm", "-rf", dbFolder).Run(); err != nil {
		return nil, err
	}
	db, err := OpenDB(dbFolder, WithCacheSize(cacheSize))
	return db.(*rocksDB), err
}
