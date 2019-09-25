package api

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/flipkart-incubator/dkv/internal/ctl"
	"github.com/flipkart-incubator/dkv/internal/server/api"
	"github.com/flipkart-incubator/dkv/internal/server/storage"
)

const (
	createDBFolderIfMissing = true
	dbFolder                = "/tmp/dkv_test"
	cacheSize               = 3 << 30
	dkvSvcPort              = 8080
	dkvSvcHost              = "localhost"
)

var (
	dkvCli      *ctl.DKVClient
	parallelism int
)

func init() {
	flag.IntVar(&parallelism, "parallelism", 2, "Number of parallel entities per core")
}

func TestMain(m *testing.M) {
	flag.Parse()
	go serveDKV()
	sleepInSecs(5)
	dkvSvcAddr := fmt.Sprintf("%s:%d", dkvSvcHost, dkvSvcPort)
	if client, err := ctl.NewInSecureDKVClient(dkvSvcAddr); err != nil {
		panic(err)
	} else {
		dkvCli = client
		os.Exit(m.Run())
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
		} else if string(actualValue) != expectedValue {
			t.Errorf("GET mismatch. Key: %s, Expected Value: %s, Actual Value: %s", key, expectedValue, actualValue)
		}
	}
}

func TestMissingGet(t *testing.T) {
	key, expectedValue := "MissingKey", ""
	if val, err := dkvCli.Get([]byte(key)); err != nil {
		t.Fatalf("Unable to GET. Key: %s, Error: %v", key, err)
	} else if string(val) != "" {
		t.Errorf("GET mismatch. Key: %s, Expected Value: %s, Actual Value: %s", key, expectedValue, val)
	}
}

func BenchmarkPutNewKeys(b *testing.B) {
	b.SetParallelism(parallelism)
	b.RunParallel(func(pb *testing.PB) {
		for i := 0; pb.Next(); i++ {
			key, value := fmt.Sprintf("BK%d", i), fmt.Sprintf("BV%d", i)
			if err := dkvCli.Put([]byte(key), []byte(value)); err != nil {
				b.Fatalf("Unable to PUT. Key: %s, Value: %s, Error: %v", key, value, err)
			}
		}
	})
}

func BenchmarkPutExistingKey(b *testing.B) {
	key := "BKey"
	if err := dkvCli.Put([]byte(key), []byte("BVal")); err != nil {
		b.Fatalf("Unable to PUT. Key: %s. Error: %v", key, err)
	}
	b.SetParallelism(parallelism)
	b.RunParallel(func(pb *testing.PB) {
		for i := 0; pb.Next(); i++ {
			value := fmt.Sprintf("BVal%d", i)
			if err := dkvCli.Put([]byte(key), []byte(value)); err != nil {
				b.Fatalf("Unable to PUT. Key: %s, Value: %s, Error: %v", key, value, err)
			}
		}
	})
}

func BenchmarkGetKey(b *testing.B) {
	key, val := "BGetKey", "BGetVal"
	if err := dkvCli.Put([]byte(key), []byte(val)); err != nil {
		b.Fatalf("Unable to PUT. Key: %s. Error: %v", key, err)
	}
	b.SetParallelism(parallelism)
	b.RunParallel(func(pb *testing.PB) {
		for i := 0; pb.Next(); i++ {
			if value, err := dkvCli.Get([]byte(key)); err != nil {
				b.Fatalf("Unable to GET. Key: %s, Error: %v", key, err)
			} else if string(value) != val {
				b.Errorf("GET mismatch. Key: %s, Expected Value: %s, Actual Value: %s", key, val, value)
			}
		}
	})
}

func BenchmarkGetMissingKey(b *testing.B) {
	key := "BMissingKey"
	b.SetParallelism(parallelism)
	b.RunParallel(func(pb *testing.PB) {
		for i := 0; pb.Next(); i++ {
			if _, err := dkvCli.Get([]byte(key)); err != nil {
				b.Fatalf("Unable to GET. Key: %s, Error: %v", key, err)
			}
		}
	})
}

func serveDKV() {
	if err := exec.Command("rm", "-rf", dbFolder).Run(); err != nil {
		panic(err)
	}
	opts := storage.NewDefaultRocksDBOptions()
	opts.CreateDBFolderIfMissing(createDBFolderIfMissing).DBFolder(dbFolder).CacheSize(cacheSize)
	if kvs, err := storage.OpenRocksDBStore(opts); err != nil {
		panic(err)
	} else {
		svc := api.NewDKVService(dkvSvcPort, kvs)
		svc.Serve()
	}
}

func sleepInSecs(duration int) {
	<-time.After(time.Duration(duration) * time.Second)
}
