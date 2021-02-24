package slave

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"net"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/flipkart-incubator/dkv/internal/master"
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
	masterDBFolder   = "/tmp/dkv_test_db_master"
	slaveDBFolder    = "/tmp/dkv_test_db_slave"
	masterSvcPort    = 8181
	slaveSvcPort     = 8282
	dkvSvcHost       = "localhost"
	cacheSize        = 3 << 30
	replPollInterval = 1 * time.Second
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
	masterRDB := newRocksDBStore(masterDBFolder)
	slaveRDB := newRocksDBStore(slaveDBFolder)
	testMasterSlaveRepl(t, masterRDB, slaveRDB, masterRDB, slaveRDB, masterRDB)
}

func TestMasterRocksDBSlaveBadger(t *testing.T) {
	masterRDB := newRocksDBStore(masterDBFolder)
	slaveRDB := newBadgerDBStore(slaveDBFolder)
	testMasterSlaveRepl(t, masterRDB, slaveRDB, masterRDB, slaveRDB, masterRDB)
}

func TestLargePayloadsDuringRepl(t *testing.T) {
	masterRDB := newRocksDBStore(masterDBFolder)
	slaveRDB := newBadgerDBStore(slaveDBFolder)

	var wg sync.WaitGroup
	wg.Add(1)
	go serveStandaloneDKVMaster(&wg, masterRDB, masterRDB, masterRDB)
	wg.Wait()

	masterCli = newDKVClient(masterSvcPort)
	defer masterCli.Close()
	defer masterSvc.Close()
	defer masterGrpcSrvr.GracefulStop()

	wg.Add(1)
	go serveStandaloneDKVSlave(&wg, slaveRDB, slaveRDB, masterCli)
	wg.Wait()

	// Reduce the max number of changes for testing
	slaveSvc.(*dkvSlaveService).maxNumChngs = 100
	slaveCli = newDKVClient(slaveSvcPort)
	defer slaveCli.Close()
	defer slaveSvc.Close()
	defer slaveGrpcSrvr.GracefulStop()

	// We insert data more than 50 MB on master that
	// results in the ResourceExhausted error on slave,
	// which when not handled properly causes assertion
	// failures on these key look ups.
	keySize, valSize := 1<<10, 1<<20
	numKeys := 50
	keys, vals := make([][]byte, numKeys), make([][]byte, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = make([]byte, keySize)
		rand.Read(keys[i])
		vals[i] = make([]byte, valSize)
		rand.Read(vals[i])
	}

	for i := 0; i < numKeys; i++ {
		if err := masterCli.Put(keys[i], vals[i]); err != nil {
			t.Fatalf("Unable to PUT key value pair at index: %d. Error: %v", i, err)
		}
	}

	// wait for atleast couple of replPollInterval to ensure slave replication
	sleepInSecs(10)

	for i := 0; i < numKeys; i++ {
		getRes, _ := slaveCli.Get(0, keys[i])
		if !bytes.Equal(vals[i], getRes.Value) {
			t.Errorf("Value mismatch for key value pair at index: %d", i)
		}
	}
}

func testMasterSlaveRepl(t *testing.T, masterStore, slaveStore storage.KVStore, cp storage.ChangePropagator, ca storage.ChangeApplier, masterBU storage.Backupable) {
	var wg sync.WaitGroup
	wg.Add(1)
	go serveStandaloneDKVMaster(&wg, masterStore, cp, masterBU)
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
	// wait for atleast couple of replPollInterval to ensure slave replication
	sleepInSecs(5)
	getKeys(t, masterCli, numKeys, keyPrefix, valPrefix)
	getKeys(t, slaveCli, numKeys, keyPrefix, valPrefix)

	backupFolder := fmt.Sprintf("%s/backup", masterDBFolder)
	if err := masterCli.Backup(backupFolder); err != nil {
		t.Fatalf("An error occurred while backing up. Error: %v", err)
	}

	numKeys, keyPrefix, valPrefix = 10, "BK", "BV"
	putKeys(t, masterCli, numKeys, keyPrefix, valPrefix)
	// wait for atleast couple of replPollInterval to ensure slave replication
	sleepInSecs(5)
	getKeys(t, masterCli, numKeys, keyPrefix, valPrefix)
	getKeys(t, slaveCli, numKeys, keyPrefix, valPrefix)

	if err := masterCli.Restore(backupFolder); err != nil {
		t.Fatalf("An error occurred while restoring. Error: %v", err)
	}

	if err := slaveSvc.(*dkvSlaveService).applyChangesFromMaster(maxNumChangesRepl); err == nil {
		t.Error("Expected an error from slave instance")
	} else {
		t.Log(err)
	}
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
	rc := serverpb.ReadConsistency_SEQUENTIAL
	for i := 1; i <= numKeys; i++ {
		key, value := fmt.Sprintf("%s%d", keyPrefix, i), fmt.Sprintf("%s%d", valPrefix, i)
		if res, err := dkvCli.Get(rc, []byte(key)); err != nil {
			t.Fatalf("Unable to GET. Key: %s, Error: %v", key, err)
		} else if string(res.Value) != value {
			t.Errorf("GET value mismatch for Key: %s, Expected: %s, Actual: %s", key, value, res.Value)
		}
	}
}

func newDKVClient(port int) *ctl.DKVClient {
	dkvSvcAddr := fmt.Sprintf("%s:%d", dkvSvcHost, port)
	if client, err := ctl.NewDKVClient(dkvSvcAddr, "", grpc.WithInsecure()); err != nil {
		panic(err)
	} else {
		return client
	}
}

func newRocksDBStore(dbFolder string) rocksdb.DB {
	if err := exec.Command("rm", "-rf", dbFolder).Run(); err != nil {
		panic(err)
	}
	store, err := rocksdb.OpenDB(dbFolder, rocksdb.WithCacheSize(cacheSize))
	if err != nil {
		panic(err)
	}
	return store
}

func newBadgerDBStore(dbFolder string) badger.DB {
	if err := exec.Command("rm", "-rf", dbFolder).Run(); err != nil {
		panic(err)
	}
	store, err := badger.OpenDB(dbFolder)
	if err != nil {
		panic(err)
	}
	return store
}

func serveStandaloneDKVMaster(wg *sync.WaitGroup, store storage.KVStore, cp storage.ChangePropagator, bu storage.Backupable) {
	// No need to set the storage.Backupable instance since its not needed here
	lgr, _ := zap.NewDevelopment()
	masterSvc = master.NewStandaloneService(store, cp, bu, lgr, stats.NewNoOpClient())
	masterGrpcSrvr = grpc.NewServer()
	serverpb.RegisterDKVServer(masterGrpcSrvr, masterSvc)
	serverpb.RegisterDKVReplicationServer(masterGrpcSrvr, masterSvc)
	serverpb.RegisterDKVBackupRestoreServer(masterGrpcSrvr, masterSvc)
	lis := listen(masterSvcPort)
	wg.Done()
	masterGrpcSrvr.Serve(lis)
}

func serveStandaloneDKVSlave(wg *sync.WaitGroup, store storage.KVStore, ca storage.ChangeApplier, masterCli *ctl.DKVClient) {
	lgr, _ := zap.NewDevelopment()
	if ss, err := NewService(store, ca, masterCli, replPollInterval, lgr, stats.NewNoOpClient()); err != nil {
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
