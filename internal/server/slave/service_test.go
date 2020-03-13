package slave

import (
	"fmt"
	"net"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/flipkart-incubator/dkv/internal/ctl"
	"github.com/flipkart-incubator/dkv/internal/server/master"
	"github.com/flipkart-incubator/dkv/internal/server/storage"
	"github.com/flipkart-incubator/dkv/internal/server/storage/badger"
	"github.com/flipkart-incubator/dkv/internal/server/storage/rocksdb"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
	"google.golang.org/grpc"
)

const (
	masterDBFolder       = "/tmp/dkv_test_db_master"
	slaveDBFolder        = "/tmp/dkv_test_db_slave"
	masterSvcPort        = 8181
	slaveSvcPort         = 8282
	dkvSvcHost           = "localhost"
	cacheSize            = 3 << 30
	replPollIntervalSecs = 1
)

var (
	masterCli      *ctl.DKVClient
	masterSvc      master.DKVService
	masterGrpcSrvr *grpc.Server

	slaveCli      *ctl.DKVClient
	slaveSvc      DKVService
	slaveGrpcSrvr *grpc.Server
)

func TestMasterRocksDBSlaveRocksDB(t *testing.T) {
	masterStore, cp, _ := newRocksDBStore(masterDBFolder)
	slaveStore, _, ca := newRocksDBStore(slaveDBFolder)
	testMasterSlaveRepl(t, masterStore, slaveStore, cp, ca)
}

func TestMasterRocksDBSlaveBadger(t *testing.T) {
	masterStore, cp, _ := newRocksDBStore(masterDBFolder)
	slaveStore, _, ca := newBadgerDBStore(slaveDBFolder)
	testMasterSlaveRepl(t, masterStore, slaveStore, cp, ca)
}

func testMasterSlaveRepl(t *testing.T, masterStore, slaveStore storage.KVStore, cp storage.ChangePropagator, ca storage.ChangeApplier) {
	var wg sync.WaitGroup
	wg.Add(1)
	go serveStandaloneDKVMaster(&wg, masterStore, cp)
	wg.Wait()

	masterCli = newDKVClient(masterSvcPort)
	defer masterCli.Close()
	defer masterSvc.Close()
	defer masterGrpcSrvr.GracefulStop()

	wg.Add(1)
	go serveStandaloneDKVSlave(&wg, slaveStore, ca, masterCli)
	wg.Wait()

	slaveCli = newDKVClient(slaveSvcPort)
	defer slaveCli.Close()
	defer slaveSvc.Close()
	defer slaveGrpcSrvr.GracefulStop()

	numKeys, keyPrefix, valPrefix := 10, "K", "V"
	putKeys(t, masterCli, numKeys, keyPrefix, valPrefix)
	// wait for atleast one replPollInterval to ensure slave replication
	sleepInSecs(2)
	getKeys(t, masterCli, numKeys, keyPrefix, valPrefix)
	getKeys(t, slaveCli, numKeys, keyPrefix, valPrefix)
}

func putKeys(t *testing.T, dkvCli *ctl.DKVClient, numKeys int, keyPrefix, valPrefix string) {
	for i := 1; i <= numKeys; i++ {
		key, value := fmt.Sprintf("%s%d", keyPrefix, i), fmt.Sprintf("%s%d", valPrefix, i)
		if err := dkvCli.Put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Unable to PUT. Key: %s, Value: %s, Error: %v", key, value, err)
		}
	}
}

func getKeys(t *testing.T, dkvCli *ctl.DKVClient, numKeys int, keyPrefix, valPrefix string) {
	for i := 1; i <= numKeys; i++ {
		key, value := fmt.Sprintf("%s%d", keyPrefix, i), fmt.Sprintf("%s%d", valPrefix, i)
		if res, err := dkvCli.Get([]byte(key)); err != nil {
			t.Fatalf("Unable to GET. Key: %s, Error: %v", key, err)
		} else if string(res.Value) != value {
			t.Errorf("GET value mismatch for Key: %s, Expected: %s, Actual: %s", key, value, res.Value)
		}
	}
}

func newDKVClient(port int) *ctl.DKVClient {
	dkvSvcAddr := fmt.Sprintf("%s:%d", dkvSvcHost, port)
	if client, err := ctl.NewInSecureDKVClient(dkvSvcAddr); err != nil {
		panic(err)
	} else {
		return client
	}
}

func newRocksDBStore(dbFolder string) (storage.KVStore, storage.ChangePropagator, storage.ChangeApplier) {
	if err := exec.Command("rm", "-rf", dbFolder).Run(); err != nil {
		panic(err)
	}
	rocksDb := rocksdb.OpenDB(dbFolder, cacheSize)
	return rocksDb, rocksDb, rocksDb
}

func newBadgerDBStore(dbFolder string) (storage.KVStore, storage.ChangePropagator, storage.ChangeApplier) {
	if err := exec.Command("rm", "-rf", dbFolder).Run(); err != nil {
		panic(err)
	}
	bdgrDb := badger.OpenDB(dbFolder)
	return bdgrDb, nil, bdgrDb
}

func serveStandaloneDKVMaster(wg *sync.WaitGroup, store storage.KVStore, cp storage.ChangePropagator) {
	masterSvc = master.NewStandaloneService(store, cp)
	masterGrpcSrvr = grpc.NewServer()
	serverpb.RegisterDKVServer(masterGrpcSrvr, masterSvc)
	serverpb.RegisterDKVReplicationServer(masterGrpcSrvr, masterSvc)
	lis := listen(masterSvcPort)
	wg.Done()
	masterGrpcSrvr.Serve(lis)
}

func serveStandaloneDKVSlave(wg *sync.WaitGroup, store storage.KVStore, ca storage.ChangeApplier, masterCli *ctl.DKVClient) {
	if ss, err := NewService(store, ca, masterCli, replPollIntervalSecs); err != nil {
		panic(err)
	} else {
		slaveSvc = ss
		slaveGrpcSrvr = grpc.NewServer()
		serverpb.RegisterDKVServer(slaveGrpcSrvr, slaveSvc)
		lis := listen(slaveSvcPort)
		wg.Done()
		slaveGrpcSrvr.Serve(lis)
	}
}

func listen(port int) net.Listener {
	if lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port)); err != nil {
		panic(fmt.Sprintf("failed to listen: %v", err))
	} else {
		return lis
	}
}

func sleepInSecs(duration int) {
	<-time.After(time.Duration(duration) * time.Second)
}
