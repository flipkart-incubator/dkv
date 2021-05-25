package master

import (
	"bytes"
	"fmt"
	"net"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/flipkart-incubator/dkv/internal/stats"
	"github.com/flipkart-incubator/dkv/internal/storage"
	"github.com/flipkart-incubator/dkv/internal/storage/badger"
	"github.com/flipkart-incubator/dkv/internal/storage/rocksdb"
	"github.com/flipkart-incubator/dkv/pkg/ctl"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	dbFolder   = "/tmp/dkv_test_db"
	cacheSize  = 3 << 30
	dkvSvcPort = 8080
	dkvSvcHost = "localhost"
	engine     = "rocksdb"
	// engine = "badger"
)

var (
	dkvCli   *ctl.DKVClient
	dkvSvc   DKVService
	grpcSrvr *grpc.Server
)

func TestStandaloneService(t *testing.T) {
	go serveStandaloneDKV()
	sleepInSecs(3)
	dkvSvcAddr := fmt.Sprintf("%s:%d", dkvSvcHost, dkvSvcPort)
	if client, err := ctl.NewDKVClient(dkvSvcAddr, "", grpc.WithInsecure()); err != nil {
		t.Fatalf("Unable to connect to DKV service at %s. Error: %v", dkvSvcAddr, err)
	} else {
		dkvCli = client
		defer dkvCli.Close()
		defer dkvSvc.Close()
		defer grpcSrvr.Stop()
		t.Run("testPutAndGet", testPutAndGet)
		t.Run("testPutTTLAndGet", testPutTTLAndGet)
		t.Run("testAtomicKeyCreation", testAtomicKeyCreation)
		t.Run("testAtomicIncrDecr", testAtomicIncrDecr)
		t.Run("testDelete", testDelete)
		t.Run("testMultiGet", testMultiGet)
		t.Run("testIteration", testIteration)
		t.Run("testMissingGet", testMissingGet)
		t.Run("testGetChanges", testGetChanges)
		t.Run("testBackupRestore", testBackupRestore)
	}
}

func testPutAndGet(t *testing.T) {
	numKeys, keyPrefix, valPrefix := 10, "K", "V"
	putKeys(t, numKeys, keyPrefix, valPrefix)

	for i := 1; i <= numKeys; i++ {
		key, expectedValue := fmt.Sprintf("%s_%d", keyPrefix, i), fmt.Sprintf("%s_%d", valPrefix, i)
		if actualValue, err := dkvCli.Get(rc, []byte(key)); err != nil {
			t.Fatalf("Unable to GET. Key: %s, Error: %v", key, err)
		} else if string(actualValue.Value) != expectedValue {
			t.Errorf("GET mismatch. Key: %s, Expected Value: %s, Actual Value: %s", key, expectedValue, actualValue)
		}
	}
}

func testPutTTLAndGet(t *testing.T) {
	key1, key2, value := "ValidKey", "ExpiredKey", "SomeValue"

	if err := dkvCli.PutTTL([]byte(key1), []byte(value), uint64(time.Now().Add(2*time.Second).Unix())); err != nil {
		t.Fatalf("Unable to PUT. Key: %s, Value: %s, Error: %v", key1, value, err)
	}

	if err := dkvCli.PutTTL([]byte(key2), []byte(value), uint64(time.Now().Add(-2*time.Second).Unix())); err != nil {
		t.Fatalf("Unable to PUT. Key: %s, Value: %s, Error: %v", key2, value, err)
	}

	if actualValue, err := dkvCli.Get(rc, []byte(key1)); err != nil {
		t.Fatalf("Unable to GET. Key: %s, Error: %v", key1, err)
	} else if string(actualValue.Value) != value {
		t.Errorf("GET mismatch. Key: %s, Expected Value: %s, Actual Value: %s", key1, value, actualValue)
	}

	if val, _ := dkvCli.Get(rc, []byte(key2)); val != nil && string(val.Value) != "" {
		t.Errorf("Expected no value for key %s. But got %s", key2, val)
	}
}

func testAtomicKeyCreation(t *testing.T) {
	var (
		wg             sync.WaitGroup
		freqs          sync.Map
		numThrs        = 10
		casKey, casVal = []byte("casKey"), []byte{0}
	)

	// verify key creation under contention
	for i := 0; i < numThrs; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			res, err := dkvCli.CompareAndSet(casKey, nil, casVal)
			freqs.Store(id, res && err == nil)
		}(i)
	}
	wg.Wait()

	expNumSucc := 1
	expNumFail := numThrs - expNumSucc
	actNumSucc, actNumFail := 0, 0
	freqs.Range(func(_, val interface{}) bool {
		if val.(bool) {
			actNumSucc++
		} else {
			actNumFail++
		}
		return true
	})

	if expNumSucc != actNumSucc {
		t.Errorf("Mismatch in number of successes. Expected: %d, Actual: %d", expNumSucc, actNumSucc)
	}

	if expNumFail != actNumFail {
		t.Errorf("Mismatch in number of failures. Expected: %d, Actual: %d", expNumFail, actNumFail)
	}
}

func testAtomicIncrDecr(t *testing.T) {
	var (
		wg             sync.WaitGroup
		numThrs        = 10
		casKey, casVal = []byte("ctrKey"), []byte{0}
	)
	if err := dkvCli.Put(casKey, casVal); err != nil {
		t.Fatalf("Unable to Put key, %s. Error: %v", casKey, err)
	}

	// even threads increment, odd threads decrement
	// a given key
	for i := 0; i < numThrs; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			delta := byte(0)
			if (id & 1) == 1 { // odd
				delta--
			} else {
				delta++
			}
			for {
				exist, _ := dkvCli.Get(rc, casKey)
				expect := exist.Value
				update := []byte{expect[0] + delta}
				res, err := dkvCli.CompareAndSet(casKey, expect, update)
				if res && err == nil {
					break
				}
			}
		}(i)
	}
	wg.Wait()

	actual, _ := dkvCli.Get(rc, casKey)
	actVal := actual.Value
	// since even and odd increments cancel out completely
	// we should expect `actVal` to be 0 (i.e., `casVal`)
	if !bytes.Equal(casVal, actVal) {
		t.Errorf("Mismatch in values for key: %s. Expected: %d, Actual: %d", string(casKey), casVal[0], actVal[0])
	}
}

func testMultiGet(t *testing.T) {
	numKeys := 10
	keys, vals := make([][]byte, numKeys), make([]string, numKeys)
	for i := 1; i <= numKeys; i++ {
		key, value := fmt.Sprintf("MK%d", i), fmt.Sprintf("MV%d", i)
		if err := dkvCli.Put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Unable to PUT. Key: %s, Value: %s, Error: %v", key, value, err)
		} else {
			keys[i-1] = []byte(key)
			vals[i-1] = value
		}
	}

	if results, err := dkvCli.MultiGet(rc, keys...); err != nil {
		t.Fatalf("Unable to MultiGet. Error: %v", err)
	} else {
		for i, result := range results {
			if string(result.Value) != vals[i] {
				t.Errorf("Multi Get value mismatch. Key: %s, Expected Value: %s, Actual Value: %s", keys[i], vals[i], result)
			}
		}
	}
}

func testIteration(t *testing.T) {
	numKeys, keyPrefix, valPrefix := 10, "IterK", "IterV"
	putKeys(t, numKeys, keyPrefix, valPrefix)
	numNewKeys, newKeyPrefix, newValPrefix := 10, "NewIterK", "NewIterV"
	count := 0

	if ch, err := dkvCli.Iterate(nil, nil); err != nil {
		t.Fatal(err)
	} else {
		// insert after iterator creation
		putKeys(t, numNewKeys, newKeyPrefix, newValPrefix)
		for kvp := range ch {
			k, v := string(kvp.Key), string(kvp.Val)
			count++
			switch {
			case strings.HasPrefix(k, keyPrefix):
				suffix := k[len(keyPrefix):]
				expVal := valPrefix + suffix
				if v != expVal {
					t.Errorf("Expected value to be %s. Actual %s.", expVal, v)
				}
			case strings.HasPrefix(k, newKeyPrefix):
				t.Errorf("Did not expect the key %s in this iteration.", k)
			}
		}
	}

	if count == 0 {
		t.Error("Iterate didn't return any rows")
	}
}

func testMissingGet(t *testing.T) {
	key := "MissingKey"
	if val, _ := dkvCli.Get(rc, []byte(key)); val != nil && string(val.Value) != "" {
		t.Errorf("Expected no value for key %s. But got %s", key, val)
	}
}

func testDelete(t *testing.T) {
	key, value := "DeletedKey", "SomeValue"

	if err := dkvCli.Put([]byte(key), []byte(value)); err != nil {
		t.Fatalf("Unable to PUT. Key: %s, Value: %s, Error: %v", key, value, err)
	}

	if err := dkvCli.Delete([]byte(key)); err != nil {
		t.Fatalf("Unable to DELETE. Key: %s, Error: %v", key, err)
	}

	if val, _ := dkvCli.Get(rc, []byte(key)); val != nil && string(val.Value) != "" {
		t.Errorf("Expected no value for key %s. But got %s", key, val)
	}
}

func testGetChanges(t *testing.T) {
	numKeys, keyPrefix, valPrefix := 10, "GCK", "GCV"
	putKeys(t, numKeys, keyPrefix, valPrefix)

	if chngsRes, err := dkvCli.GetChanges(0, 1000); err != nil {
		t.Fatalf("Unable to get changes. Error: %v", err)
	} else {
		if chngsRes.MasterChangeNumber == 0 {
			t.Errorf("Expected master change number to be greater than 0")
		}
		chngs := chngsRes.Changes
		numChngs := len(chngs)
		if numChngs < numKeys {
			t.Errorf("Expected at least %d changes. But got only %d changes.", numKeys, numChngs)
		} else {
			// Loop from the back since changes sent in chronological order.
			for i := 1; i <= numKeys; i++ {
				chng := chngs[numChngs-i]
				id := numKeys - i + 1
				expKey, expVal := fmt.Sprintf("%s_%d", keyPrefix, id), fmt.Sprintf("%s_%d", valPrefix, id)
				if chng.NumberOfTrxns != 2 {
					t.Errorf("Expected two transaction, but found %d transactions.", chng.NumberOfTrxns)
				} else {
					trxn := chng.Trxns[1]
					if trxn.Type != serverpb.TrxnRecord_Put {
						t.Errorf("Expected PUT transaction but found %s transaction", trxn.Type.String())
					} else if string(trxn.Key) != expKey {
						t.Errorf("Key mismatch. Expected %s, Actual %s", trxn.Key, expKey)
					} else if string(trxn.Value) != expVal {
						t.Errorf("Value mismatch. Expected %s, Actual %s", trxn.Value, expVal)
					}
				}
			}
		}
	}
}

func testBackupRestore(t *testing.T) {
	numKeys, keyPrefix, valPrefix := 5, "brKey", "brVal"
	putKeys(t, numKeys, keyPrefix, valPrefix)

	backupPath := fmt.Sprintf("%s/%s", dbFolder, "backup")
	if err := dkvCli.Backup(backupPath); err != nil {
		t.Fatal(err)
	} else {
		missKeyPrefix, missValPrefix := "mbrKey", "mbrVal"
		putKeys(t, numKeys, missKeyPrefix, missValPrefix)
		if err := dkvCli.Restore(backupPath); err != nil {
			t.Fatal(err)
		} else {
			getKeys(t, numKeys, keyPrefix, valPrefix)
			noKeys(t, numKeys, missKeyPrefix)
		}
	}
}

func newKVStore() (storage.KVStore, storage.ChangePropagator, storage.Backupable) {
	if err := exec.Command("rm", "-rf", dbFolder).Run(); err != nil {
		panic(err)
	}
	switch engine {
	case "rocksdb":
		rocksDb, err := rocksdb.OpenDB(dbFolder,
			rocksdb.WithSyncWrites(), rocksdb.WithCacheSize(cacheSize))
		if err != nil {
			panic(err)
		}
		return rocksDb, rocksDb, rocksDb
	case "badger":
		bdgrDb, err := badger.OpenDB(badger.WithSyncWrites(), badger.WithDBDir(dbFolder))
		if err != nil {
			panic(err)
		}
		return bdgrDb, nil, bdgrDb
	default:
		panic(fmt.Sprintf("Unknown storage engine: %s", engine))
	}
}

func serveStandaloneDKV() {
	kvs, cp, ba := newKVStore()
	dkvSvc = NewStandaloneService(kvs, cp, ba, zap.NewNop(), stats.NewNoOpClient())
	grpcSrvr = grpc.NewServer()
	serverpb.RegisterDKVServer(grpcSrvr, dkvSvc)
	serverpb.RegisterDKVReplicationServer(grpcSrvr, dkvSvc)
	serverpb.RegisterDKVBackupRestoreServer(grpcSrvr, dkvSvc)
	listenAndServe(grpcSrvr, dkvSvcPort)
}

func listenAndServe(grpcSrvr *grpc.Server, port int) {
	if lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port)); err != nil {
		panic(fmt.Sprintf("failed to listen: %v", err))
	} else {
		grpcSrvr.Serve(lis)
	}
}

func putKeys(t *testing.T, numKeys int, keyPrefix, valPrefix string) {
	for i := 1; i <= numKeys; i++ {
		key, value := fmt.Sprintf("%s_%d", keyPrefix, i), fmt.Sprintf("%s_%d", valPrefix, i)
		if err := dkvCli.Put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Unable to PUT. Key: %s, Value: %s, Error: %v", key, value, err)
		}
	}
}

func noKeys(t *testing.T, numKeys int, keyPrefix string) {
	for i := 1; i <= numKeys; i++ {
		key := fmt.Sprintf("%s_%d", keyPrefix, i)
		if res, err := dkvCli.Get(rc, []byte(key)); err != nil {
			t.Fatalf("Unable to GET. Key: %s, Error: %v", key, err)
		} else if string(res.Value) != "" {
			t.Errorf("Expected missing for key: %s. But found it with value: %s", key, res.Value)
		}
	}
}

func getKeys(t *testing.T, numKeys int, keyPrefix, valPrefix string) {
	for i := 1; i <= numKeys; i++ {
		key, expectedValue := fmt.Sprintf("%s_%d", keyPrefix, i), fmt.Sprintf("%s_%d", valPrefix, i)
		if res, err := dkvCli.Get(rc, []byte(key)); err != nil {
			t.Fatalf("Unable to GET. Key: %s, Error: %v", key, err)
		} else if string(res.Value) != expectedValue {
			t.Errorf("GET mismatch. Key: %s, Expected Value: %s, Actual Value: %s", key, expectedValue, res.Value)
		}
	}
}

func sleepInSecs(duration int) {
	<-time.After(time.Duration(duration) * time.Second)
}
