package api

import (
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/flipkart-incubator/dkv/internal/ctl"
	"github.com/flipkart-incubator/dkv/internal/server/storage"
	"github.com/flipkart-incubator/dkv/internal/server/storage/badger"
	"github.com/flipkart-incubator/dkv/internal/server/storage/rocksdb"
)

const (
	dbFolder   = "/tmp/dkv_test"
	cacheSize  = 3 << 30
	dkvSvcPort = 8080
	dkvSvcHost = "localhost"
	engine     = "rocksdb" // or "badger"
)

var (
	dkvCli *ctl.DKVClient
	dkvSvc *DKVService
)

func TestMain(m *testing.M) {
	go serveDKV()
	sleepInSecs(3)
	dkvSvcAddr := fmt.Sprintf("%s:%d", dkvSvcHost, dkvSvcPort)
	if client, err := ctl.NewInSecureDKVClient(dkvSvcAddr); err != nil {
		panic(err)
	} else {
		dkvCli = client
		res := m.Run()
		dkvCli.Close()
		dkvSvc.Close()
		os.Exit(res)
	}
}

func TestPutAndGet(t *testing.T) {
	numKeys := 10
	for i := 1; i <= numKeys; i++ {
		key, value := fmt.Sprintf("K%d", i), fmt.Sprintf("V%d", i)
		if err := dkvCli.Put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Unable to PUT. Key: %s, Value: %s, Error: %v", key, value, err)
		}
	}

	for i := 1; i <= numKeys; i++ {
		key, expectedValue := fmt.Sprintf("K%d", i), fmt.Sprintf("V%d", i)
		if actualValue, err := dkvCli.Get([]byte(key)); err != nil {
			t.Fatalf("Unable to GET. Key: %s, Error: %v", key, err)
		} else if string(actualValue.Value) != expectedValue {
			t.Errorf("GET mismatch. Key: %s, Expected Value: %s, Actual Value: %s", key, expectedValue, actualValue)
		}
	}
}

func TestMultiGet(t *testing.T) {
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
			if string(result.Value) != vals[i] {
				t.Errorf("Multi Get value mismatch. Key: %s, Expected Value: %s, Actual Value: %s", keys[i], vals[i], result.Value)
			}
		}
	}
}

func TestMissingGet(t *testing.T) {
	key, expectedValue := "MissingKey", ""
	if val, err := dkvCli.Get([]byte(key)); err != nil {
		t.Fatalf("Unable to GET. Key: %s, Error: %v", key, err)
	} else if string(val.Value) != "" {
		t.Errorf("GET mismatch. Key: %s, Expected Value: %s, Actual Value: %s", key, expectedValue, val)
	}
}

func serveDKV() {
	if err := exec.Command("rm", "-rf", dbFolder).Run(); err != nil {
		panic(err)
	}
	var kvs storage.KVStore
	switch engine {
	case "rocksdb":
		kvs = rocksdb.OpenDB(dbFolder, cacheSize)
	case "badger":
		kvs = badger.OpenDB(dbFolder)
	default:
		panic(fmt.Sprintf("Unknown storage engine: %s", engine))
	}
	dkvSvc = NewDKVService(dkvSvcPort, kvs)
	dkvSvc.ListenAndServe()
}

func sleepInSecs(duration int) {
	<-time.After(time.Duration(duration) * time.Second)
}
