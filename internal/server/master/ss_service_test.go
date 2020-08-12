package master

import (
	"fmt"
	"net"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/flipkart-incubator/dkv/internal/server/storage"
	"github.com/flipkart-incubator/dkv/internal/server/storage/badger"
	"github.com/flipkart-incubator/dkv/internal/server/storage/rocksdb"
	"github.com/flipkart-incubator/dkv/pkg/ctl"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
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
	if client, err := ctl.NewInSecureDKVClient(dkvSvcAddr); err != nil {
		t.Fatalf("Unable to connect to DKV service at %s. Error: %v", dkvSvcAddr, err)
	} else {
		dkvCli = client
		defer dkvCli.Close()
		defer dkvSvc.Close()
		defer grpcSrvr.Stop()
		t.Run("testPutAndGet", testPutAndGet)
		t.Run("testMultiGet", testMultiGet)
		t.Run("testIteration", testIteration)
		t.Run("testMissingGet", testMissingGet)
		t.Run("testGetChanges", testGetChanges)
		t.Run("testAddRemoveReplicas", testAddRemoveReplicas)
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
			if string(result) != vals[i] {
				t.Errorf("Multi Get value mismatch. Key: %s, Expected Value: %s, Actual Value: %s", keys[i], vals[i], result)
			}
		}
	}
}

func testIteration(t *testing.T) {
	numKeys, keyPrefix, valPrefix := 10, "IterK", "IterV"
	putKeys(t, numKeys, keyPrefix, valPrefix)
	numNewKeys, newKeyPrefix, newValPrefix := 10, "NewIterK", "NewIterV"

	if ch, err := dkvCli.Iterate(nil, nil); err != nil {
		t.Fatal(err)
	} else {
		// insert after iterator creation
		putKeys(t, numNewKeys, newKeyPrefix, newValPrefix)
		for kvp := range ch {
			k, v := string(kvp.Key), string(kvp.Val)
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
}

func testMissingGet(t *testing.T) {
	key := "MissingKey"
	if val, _ := dkvCli.Get(rc, []byte(key)); val != nil && string(val.Value) != "" {
		t.Errorf("Expected no value for key %s. But got %s", key, val)
	}
}

func testAddRemoveReplicas(t *testing.T) {
	verifyAddRemoveReplicas(t, "in-chennai-1", []string{"ch1:1111", "ch2:2222", "ch3:3333"})
	verifyAddRemoveReplicas(t, "", []string{"host1:1111", "host2:2222", "host3:3333"})
	verifyAddRemoveReplicas(t, "in-hyderabad-1", []string{"hy1:1111", "hy2:2222", "hy3:3333"})
}

func verifyAddRemoveReplicas(t *testing.T, zone string, replicas []string) {
	for _, replica := range replicas {
		if err := dkvCli.AddReplica(replica, zone); err != nil {
			t.Fatalf("Unable to add replica %s. Error: %v", replica, err)
		}
	}
	repls := dkvCli.GetReplicas(zone)
	if !reflect.DeepEqual(repls, replicas) {
		t.Errorf("Expected %q replicas but got %q", replicas, repls)
	}

	if err := dkvCli.RemoveReplica(replicas[0], zone); err != nil {
		t.Fatalf("Unable to remove replica %s. Error: %v", replicas[0], err)
	}

	repls = dkvCli.GetReplicas(zone)
	replicas = replicas[1:]
	if !reflect.DeepEqual(repls, replicas) {
		t.Errorf("Expected %q replicas but got %q", replicas, repls)
	}

	for _, replica := range replicas {
		if err := dkvCli.RemoveReplica(replica, zone); err != nil {
			t.Fatalf("Unable to remove replica %s. Error: %v", replica, err)
		}
	}

	repls = dkvCli.GetReplicas(zone)
	if len(repls) > 0 {
		t.Errorf("Expected no replicas but got %q", repls)
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
				expKey, expVal := fmt.Sprintf("%s_%d", keyPrefix, i), fmt.Sprintf("%s_%d", valPrefix, i)
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
		rocksDb, err := rocksdb.OpenDB(dbFolder, cacheSize)
		if err != nil {
			panic(err)
		}
		return rocksDb, rocksDb, rocksDb
	case "badger":
		bdgrDb, err := badger.OpenDB(dbFolder)
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
	dkvSvc = NewStandaloneService(kvs, cp, ba, nil)
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
