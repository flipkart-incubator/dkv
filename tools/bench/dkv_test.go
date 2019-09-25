package bench

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/flipkart-incubator/dkv/internal/ctl"
	"github.com/flipkart-incubator/dkv/internal/server/api"
	"github.com/flipkart-incubator/dkv/internal/server/storage"
)

// go test ./tools/bench -bench . -parallelism 20 -valueSizeInBytes 16384 -totalNumKeys 10000 -numHotKeys 5000 -count 3

const (
	createDBFolderIfMissing = true
	dbFolder                = "/tmp/dkv_test"
	cacheSize               = 3 << 30
	dkvSvcPort              = 8080
	dkvSvcHost              = "localhost"
)

var (
	dkvCli           *ctl.DKVClient
	parallelism      int
	valueSizeInBytes int
	totalNumKeys     int
	numHotKeys       int
)

func init() {
	flag.IntVar(&parallelism, "parallelism", 2, "Number of parallel entities per core")
	flag.IntVar(&valueSizeInBytes, "valueSizeInBytes", 10, "Size of every value in bytes")
	flag.IntVar(&totalNumKeys, "totalNumKeys", 1000, "Total number of keys")
	flag.IntVar(&numHotKeys, "numHotKeys", 100, "Number of keys that are repeatedly read")
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

func randomBytes(size int) []byte {
	res := make([]byte, size)
	for i := 0; i < size; i++ {
		res[i] = byte(rand.Intn(129))
	}
	return res
}

func loadAndGetHotKeys(b *testing.B) (hotKeys [][]byte) {
	defer b.ResetTimer()
	hotKeys = make([][]byte, numHotKeys)
	for i, j := 0, 0; i < totalNumKeys; i++ {
		key, val := []byte(fmt.Sprintf("BGetKey%d", i)), randomBytes(valueSizeInBytes)
		if err := dkvCli.Put(key, val); err != nil {
			b.Fatalf("Unable to PUT. Key: %s. Error: %v", key, err)
		}
		if j < numHotKeys && 1 == rand.Intn(2) {
			hotKeys[j] = key
			j++
		}
	}
	return
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
