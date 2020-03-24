package master

import (
	"fmt"
	"net"
	"os/exec"
	"testing"
	"time"

	"github.com/flipkart-incubator/dkv/internal/ctl"
	"github.com/flipkart-incubator/dkv/internal/server/storage"
	"github.com/flipkart-incubator/dkv/internal/server/storage/badger"
	"github.com/flipkart-incubator/dkv/internal/server/storage/rocksdb"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
	"google.golang.org/grpc"
)

const (
	dbFolder   = "/tmp/dkv_test_db"
	cacheSize  = 3 << 30
	dkvSvcPort = 8080
	dkvSvcHost = "localhost"
	engine     = "rocksdb" // or "badger"
)

var (
	dkvCli *ctl.DKVClient
	dkvSvc DKVService
)

func TestStandaloneService(t *testing.T) {
	go serveStandaloneDKV()
	sleepInSecs(3)
	dkvSvcAddr := fmt.Sprintf("%s:%d", dkvSvcHost, dkvSvcPort)
	if client, err := ctl.NewInSecureDKVClient(dkvSvcAddr); err != nil {
		panic(err)
	} else {
		dkvCli = client
		defer dkvCli.Close()
		defer dkvSvc.Close()
		t.Run("testPutAndGet", testPutAndGet)
		t.Run("testMultiGet", testMultiGet)
		t.Run("testMissingGet", testMissingGet)
		t.Run("testGetChanges", testGetChanges)
		t.Run("testBackupRestore", testBackupRestore)
	}
}

func testPutAndGet(t *testing.T) {
	numKeys, keyPrefix, valPrefix := 10, "K", "V"
	putKeys(t, numKeys, keyPrefix, valPrefix)

	for i := 1; i <= numKeys; i++ {
		key, expectedValue := fmt.Sprintf("%s%d", keyPrefix, i), fmt.Sprintf("%s%d", valPrefix, i)
		if actualValue, err := dkvCli.Get([]byte(key)); err != nil {
			t.Fatalf("Unable to GET. Key: %s, Error: %v", key, err)
		} else if string(actualValue.Value) != expectedValue {
			t.Errorf("GET mismatch. Key: %s, Expected Value: %s, Actual Value: %s", key, expectedValue, actualValue)
		}
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

	if results, err := dkvCli.MultiGet(keys...); err != nil {
		t.Fatalf("Unable to MultiGet. Error: %v", err)
	} else {
		for i, result := range results {
			if string(result) != vals[i] {
				t.Errorf("Multi Get value mismatch. Key: %s, Expected Value: %s, Actual Value: %s", keys[i], vals[i], result)
			}
		}
	}
}

func testMissingGet(t *testing.T) {
	key, expectedValue := "MissingKey", ""
	if val, err := dkvCli.Get([]byte(key)); err != nil {
		t.Fatalf("Unable to GET. Key: %s, Error: %v", key, err)
	} else if string(val.Value) != "" {
		t.Errorf("GET mismatch. Key: %s, Expected Value: %s, Actual Value: %s", key, expectedValue, val)
	}
}

func testGetChanges(t *testing.T) {
	numKeys, keyPrefix, valPrefix := 10, "GCK", "GCV"
	putKeys(t, numKeys, keyPrefix, valPrefix)

	if chngsRes, err := dkvCli.GetChanges(0, 100); err != nil {
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
			for i, j := numKeys, numChngs; i >= 1; i, j = i-1, j-1 {
				chng := chngs[j-1]
				expKey, expVal := fmt.Sprintf("%s%d", keyPrefix, i), fmt.Sprintf("%s%d", valPrefix, i)
				if chng.NumberOfTrxns != 1 {
					t.Errorf("Expected one transaction, but found %d transactions.", chng.NumberOfTrxns)
				} else {
					trxn := chng.Trxns[0]
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
	numKeys, keyPrefix, valPrefix := 500, "BRK", "BRV"
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
		}
	}
}

func newKVStore() (storage.KVStore, storage.ChangePropagator, storage.Backupable) {
	if err := exec.Command("rm", "-rf", dbFolder).Run(); err != nil {
		panic(err)
	}
	switch engine {
	case "rocksdb":
		rocksDb := rocksdb.OpenDB(dbFolder, cacheSize)
		return rocksDb, rocksDb, rocksDb
	case "badger":
		bdgrDb := badger.OpenDB(dbFolder)
		return bdgrDb, nil, bdgrDb
	default:
		panic(fmt.Sprintf("Unknown storage engine: %s", engine))
	}
}

func serveStandaloneDKV() {
	dkvSvc = NewStandaloneService(newKVStore())
	grpcSrvr := grpc.NewServer()
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
		key, value := fmt.Sprintf("%s%d", keyPrefix, i), fmt.Sprintf("%s%d", valPrefix, i)
		if err := dkvCli.Put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Unable to PUT. Key: %s, Value: %s, Error: %v", key, value, err)
		}
	}
}

func noKeys(t *testing.T, numKeys int, keyPrefix string) {
	for i := 1; i <= numKeys; i++ {
		key := fmt.Sprintf("%s_%d", keyPrefix, i)
		if res, err := dkvCli.Get([]byte(key)); err != nil {
			t.Fatalf("Unable to GET. Key: %s, Error: %v", key, err)
		} else if string(res.Value) != "" {
			t.Errorf("Expected missing for key: %s. But found it with value: %s", key, res.Value)
		}
	}
}

func getKeys(t *testing.T, numKeys int, keyPrefix, valPrefix string) {
	for i := 1; i <= numKeys; i++ {
		key, expectedValue := fmt.Sprintf("%s_%d", keyPrefix, i), fmt.Sprintf("%s_%d", valPrefix, i)
		if res, err := dkvCli.Get([]byte(key)); err != nil {
			t.Fatalf("Unable to GET. Key: %s, Error: %v", key, err)
		} else if string(res.Value) != expectedValue {
			t.Errorf("GET mismatch. Key: %s, Expected Value: %s, Actual Value: %s", key, expectedValue, res.Value)
		}
	}
}

func sleepInSecs(duration int) {
	<-time.After(time.Duration(duration) * time.Second)
}
