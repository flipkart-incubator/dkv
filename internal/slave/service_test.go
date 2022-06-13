package slave

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"net"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/flipkart-incubator/dkv/internal/discovery"
	dkv_sync "github.com/flipkart-incubator/dkv/internal/sync"
	nexus_api "github.com/flipkart-incubator/nexus/pkg/api"
	nexus "github.com/flipkart-incubator/nexus/pkg/raft"

	"github.com/flipkart-incubator/dkv/pkg/health"

	"github.com/flipkart-incubator/dkv/internal/master"
	"github.com/flipkart-incubator/dkv/internal/opts"
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
	masterDBFolder = "/tmp/dkv_test_db_master"
	slaveDBFolder  = "/tmp/dkv_test_db_slave"
	masterSvcPort  = 8181
	slaveSvcPort   = 8282
	dkvSvcHost     = "localhost"
	cacheSize      = 3 << 30

	// for creating a distribute server cluster
	dbFolderMaster   = "/tmp/dkv_test_db_master"
	dbFolderSlave    = "/tmp/dkv_test_db_slave"
	clusterSize      = 5
	discoveryPort    = 8686
	logDir           = "/tmp/dkv_test/logs"
	snapDir          = "/tmp/dkv_test/snap"
	clusterURL       = "http://127.0.0.1:9331,http://127.0.0.1:9332,http://127.0.0.1:9333,http://127.0.0.1:9334,http://127.0.0.1:9335"
	replTimeout      = 3 * time.Second
	engine           = "rocksdb"
	dbName           = "default"
	vbucket          = "default"
	heartBeatTimeOut = 2
	statusTtl        = 5
)

var (
	masterCli      *ctl.DKVClient
	masterSvc      master.DKVService
	masterGrpcSrvr *grpc.Server
	slaveCli       *ctl.DKVClient
	slaveSvc       DKVService
	slaveGrpcSrvr  *grpc.Server
	healthCheckCli *HealthCheckClient

	lgr, _     = zap.NewDevelopment()
	serverOpts = &opts.ServerOpts{
		Logger:                    lgr,
		StatsCli:                  stats.NewNoOpClient(),
		PrometheusRegistry:        stats.NewPromethousNoopRegistry(),
		HealthCheckTickerInterval: opts.DefaultHealthCheckTickterInterval,
	}

	// for creating a distribute server cluster
	dkvPorts        = map[int]int{1: 9981, 2: 9982, 3: 9983, 4: 9984, 5: 9985}
	grpcSrvs        = make(map[int]*grpc.Server)
	dkvClis         = make(map[int]*ctl.DKVClient)
	dkvSvcs         = make(map[int]DKVService)
	mutex           = sync.Mutex{}
	discoveryCli    discovery.Client
	discoverydkvSvc master.DKVService
	closedMasters   = make(map[int]bool)
)

type HealthCheckClient struct {
	cliConn        *grpc.ClientConn
	healthCheckCli health.HealthClient
}

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

func TestSlaveDiscoveryFunctionality(t *testing.T) {
	masterRDB := newRocksDBStore(masterDBFolder)
	slaveRDB := newRocksDBStore(slaveDBFolder)
	testGetStatus(t, masterRDB, slaveRDB, masterRDB, slaveRDB, masterRDB)
}

func TestSlaveHealthCheck(t *testing.T) {
	masterRDB := newRocksDBStore(masterDBFolder)
	slaveRDB := newRocksDBStore(slaveDBFolder)
	testHealthCheck(t, masterRDB, slaveRDB, masterRDB, slaveRDB, masterRDB)
}

func TestSlaveHealthCheckStream(t *testing.T) {
	masterRDB := newRocksDBStore(masterDBFolder)
	slaveRDB := newRocksDBStore(slaveDBFolder)
	testHealthCheckStream(t, masterRDB, slaveRDB, masterRDB, slaveRDB, masterRDB)
}

func TestSlaveAutoConnect(t *testing.T) {
	//reset raft directories
	resetRaftStateDirs(t)

	//start discovery server
	startDiscoveryServer()
	sleepInSecs(3)
	startDiscoveryCli()
	defer discoverydkvSvc.Close()
	defer discoveryCli.Close()

	//start dkvservers
	initDKVServers()
	sleepInSecs(3)
	registerDkvServerWithDiscovery()
	initDKVClients()
	defer stopClients()
	defer stopServers()

	//start slave server
	startSlaveAndAttachToMaster(nil)
	registerSlaveWithDiscovery()
	sleepInSecs(10)
	defer closeSlave()
	if err := slaveSvc.(*slaveService).findAndConnectToMaster(); err != nil {
		t.Fatalf("Cannot connect to new master. Error: %v", err)
	}
	lastClosedMasterId := -1
	for cnt := 0; cnt < clusterSize+1; cnt++ {
		masterId := getCurrentMasterIdFromSlave(t)
		t.Log("Current master id:", dkvPorts[masterId])
		currentMasterSvc := dkvSvcs[masterId]
		currentMasterCli := dkvClis[masterId]
		//add some kv using CLI
		fg := false
		for i := 0; i < 10; i++ {
			if lastClosedMasterId != -1 && fg == false {
				// add current node to the cluster config
				currentMasterCli.AddNode(getNodeUrl(lastClosedMasterId))
				fg = true
			}
			key, value := fmt.Sprintf("K_CLI_%d_%d", i, cnt), fmt.Sprintf("V_CLI_%d_%d", i, cnt)
			if err := currentMasterCli.PutTTL([]byte(key), []byte(value), uint64(time.Now().Add(2*time.Hour).Unix())); err != nil {
				t.Fatalf("Error while putting key to kv. Error: %v", err)
			}
		}

		if err := slaveSvc.(*slaveService).applyChangesFromMaster(100); err != nil {
			t.Errorf("Error while applying changes from master. Error: %v", err)
		}

		//abruptly stop the master
		grpcSrvs[masterId].Stop()
		currentMasterCli.Close()
		currentMasterSvc.Close()

		closedMasters[masterId] = true
		lastClosedMasterId = masterId

		//wait for new leader election
		sleepInSecs(10)

		//let slave connect to a new master
		slaveSvc.(*slaveService).reconnectMaster()
		masterId = getCurrentMasterIdFromSlave(t)
		startDkvSvcAndCli(lastClosedMasterId)
		sleepInSecs(10)

		t.Log("New master port:", dkvPorts[masterId])
	}
}

func getNodeUrl(id int) string {
	clusURLs := strings.Split(clusterURL, ",")
	return clusURLs[id-1]
}

func startDkvSvcAndCli(id int) {
	clusURLs := strings.Split(clusterURL, ",")
	go serveDistributedDKV(id, clusURLs[id-1])
	sleepInSecs(3)
	initSingleDkvClient(id)
	discoveryCli.RegisterRegion(dkvSvcs[id])
	discoveryCli.PropagateStatus()
}

func getCurrentMasterId(t *testing.T) int {
	var currentMaster string
	if regions, err := discoveryCli.GetClusterStatus(dbName, vbucket); err != nil {
		t.Fatalf("Error while fetching the cluster info. Cannot proceed further. Error: %v", err)
	} else {
		for _, region := range regions {
			if region.MasterHost != nil {
				currentMaster = *region.MasterHost
			}
		}
	}
	if currentMaster == "" {
		t.Fatalf("No master found")
	}
	detail := strings.Split(currentMaster, ":")
	for i := 1; i <= clusterSize; i++ {
		if fmt.Sprintf("%d", dkvPorts[i]) == detail[1] {
			return i
		}
	}
	t.Fatalf("Error: Master port not found")
	return -1
}

func getCurrentMasterIdFromSlave(t *testing.T) int {
	var currentMaster string
	if slaveSvc.(*slaveService).regionInfo.MasterHost != nil {
		currentMaster = *slaveSvc.(*slaveService).regionInfo.MasterHost
	}
	if currentMaster == "" {
		t.Fatalf("No master found")
	}
	detail := strings.Split(currentMaster, ":")
	for i := 1; i <= clusterSize; i++ {
		if fmt.Sprintf("%d", dkvPorts[i]) == detail[1] {
			return i
		}
	}
	t.Fatalf("Error: Master port not found")
	return -1
}

func registerSlaveWithDiscovery() {
	discoveryCli.RegisterRegion(slaveSvc)
	discoveryCli.PropagateStatus()
}

func registerDkvServerWithDiscovery() {
	for id := 1; id <= clusterSize; id++ {
		discoveryCli.RegisterRegion(dkvSvcs[id])
	}
	discoveryCli.PropagateStatus()
	sleepInSecs(3)
}

func startDiscoveryServer() {
	//todo check why does this need a kv store?
	discoverykvs, discoverycp, discoveryba := newKVStore(masterDBFolder + "_DC")
	discoverydkvSvc = master.NewStandaloneService(discoverykvs, discoverycp, discoveryba, &serverpb.RegionInfo{Database: dbName, VBucket: vbucket}, serverOpts)
	grpcSrvr := grpc.NewServer()
	serverpb.RegisterDKVServer(grpcSrvr, discoverydkvSvc)
	serverpb.RegisterDKVReplicationServer(grpcSrvr, discoverydkvSvc)
	serverpb.RegisterDKVBackupRestoreServer(grpcSrvr, discoverydkvSvc)

	discoverServiceConf := &opts.DiscoveryServerConfig{statusTtl, heartBeatTimeOut}
	discoveryService, _ := discovery.NewDiscoveryService(discoverydkvSvc, zap.NewNop(), discoverServiceConf)
	serverpb.RegisterDKVDiscoveryServer(grpcSrvr, discoveryService)
	go grpcSrvr.Serve(newListener(discoveryPort))
}

func startDiscoveryCli() {
	clientConfig := &opts.DiscoveryClientConfig{DiscoveryServiceAddr: fmt.Sprintf("%s:%d", dkvSvcHost, discoveryPort),
		PushStatusInterval: time.Duration(5), PollClusterInfoInterval: time.Duration(5)}
	discoveryCli, _ = discovery.NewDiscoveryClient(clientConfig, zap.NewNop())
}

func startSlaveAndAttachToMaster(client *ctl.DKVClient) {
	wg := sync.WaitGroup{}
	wg.Add(1)
	rdbStore := newRocksDBStore(dbFolderSlave)
	go serveStandaloneDKVSlave(&wg, rdbStore, rdbStore, client, discoveryCli)
	wg.Wait()

	// stop the slave poller so as to avoid race with this poller
	// and the explicit call to applyChangesFromMaster later
	//TODO Check why is this even required?
	slaveSvc.(*slaveService).replInfo.replTckr.Stop()
	slaveSvc.(*slaveService).replInfo.replStop <- struct{}{}
	//sleepInSecs(2)

	slaveCli = newDKVClient(slaveSvcPort)
}

func stopClients() {
	for id := 1; id <= clusterSize; id++ {
		dkvClis[id].Close()
	}
}

func stopServers() {
	for id := 1; id <= clusterSize; id++ {
		if closedMasters[id] == true {
			continue
		}
		dkvSvcs[id].Close()
		grpcSrvs[id].GracefulStop()
	}
}

func initDKVClients(ids ...int) {
	for id := 1; id <= clusterSize; id++ {
		svcAddr := fmt.Sprintf("%s:%d", dkvSvcHost, dkvPorts[id])
		if client, err := ctl.NewInSecureDKVClient(svcAddr, "", ctl.DefaultConnectOpts); err != nil {
			panic(err)
		} else {
			dkvClis[id] = client
		}
	}
}

func initSingleDkvClient(id int) {
	svcAddr := fmt.Sprintf("%s:%d", dkvSvcHost, dkvPorts[id])
	if client, err := ctl.NewInSecureDKVClient(svcAddr, "", ctl.DefaultConnectOpts); err != nil {
		panic(err)
	} else {
		dkvClis[id] = client
	}
}

func initDKVServers() {
	clusURLs := strings.Split(clusterURL, ",")
	for id := 1; id <= clusterSize; id++ {
		go serveDistributedDKV(id, clusURLs[id-1])
	}
}

func serveDistributedDKV(id int, nodeURL string) {
	dkvSvc, grpcSvc := newDistributedDKVNode(id, nodeURL, clusterURL)
	mutex.Lock()
	dkvSvcs[id] = dkvSvc
	grpcSrvs[id] = grpcSvc
	mutex.Unlock()
	grpcSrvs[id].Serve(newListener(dkvPorts[id]))
}

func newReplicator(kvs storage.KVStore, nodeURL, clusterURL string) nexus_api.RaftReplicator {
	replStore := dkv_sync.NewDKVReplStore(kvs)
	opts := []nexus.Option{
		nexus.NodeUrl(nodeURL),
		nexus.LogDir(logDir),
		nexus.SnapDir(snapDir),
		nexus.ClusterUrl(clusterURL),
		nexus.ReplicationTimeout(replTimeout),
		//nexus.LeaseBasedReads(),
	}
	if nexusRepl, err := nexus_api.NewRaftReplicator(replStore, opts...); err != nil {
		panic(err)
	} else {
		return nexusRepl
	}
}

func newListener(port int) net.Listener {
	if lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port)); err != nil {
		panic(fmt.Sprintf("failed to listen: %v", err))
	} else {
		return lis
	}
}

func newKVStore(dir string) (storage.KVStore, storage.ChangePropagator, storage.Backupable) {
	if err := exec.Command("rm", "-rf", dir).Run(); err != nil {
		panic(err)
	}
	switch engine {
	case "rocksdb":
		rocksDb, err := rocksdb.OpenDB(dir,
			rocksdb.WithSyncWrites(), rocksdb.WithCacheSize(cacheSize))
		if err != nil {
			panic(err)
		}
		return rocksDb, rocksDb, rocksDb
	case "badger":
		bdgrDb, err := badger.OpenDB(badger.WithSyncWrites(), badger.WithDBDir(dir))
		if err != nil {
			panic(err)
		}
		return bdgrDb, nil, bdgrDb
	default:
		panic(fmt.Sprintf("Unknown storage engine: %s", engine))
	}
}

func newDistributedDKVNode(id int, nodeURL, clusURL string) (DKVService, *grpc.Server) {
	dir := fmt.Sprintf("%s_%d", dbFolderMaster, id)
	kvs, cp, br := newKVStore(dir)
	dkvRepl := newReplicator(kvs, nodeURL, clusURL)
	dkvRepl.Start()
	regionInfo := &serverpb.RegionInfo{Database: dbName, VBucket: vbucket}
	regionInfo.NodeAddress = "127.0.0.1" + ":" + fmt.Sprint(dkvPorts[id])
	distSrv := master.NewDistributedService(kvs, cp, br, dkvRepl, regionInfo, serverOpts)
	grpcSrv := grpc.NewServer()
	serverpb.RegisterDKVServer(grpcSrv, distSrv)
	serverpb.RegisterDKVClusterServer(grpcSrv, distSrv)
	serverpb.RegisterDKVReplicationServer(grpcSrv, distSrv)
	return distSrv, grpcSrv
}

func resetRaftStateDirs(t *testing.T) {
	if err := exec.Command("rm", "-rf", logDir).Run(); err != nil {
		t.Fatal(err)
	}
	if err := exec.Command("mkdir", "-p", logDir).Run(); err != nil {
		t.Fatal(err)
	}
	if err := exec.Command("rm", "-rf", snapDir).Run(); err != nil {
		t.Fatal(err)
	}
	if err := exec.Command("mkdir", "-p", snapDir).Run(); err != nil {
		t.Fatal(err)
	}
	for tmp := 1; tmp <= 5; tmp++ {
		folderName := fmt.Sprintf("%s_%d", dbFolderMaster, tmp)
		if err := exec.Command("rm", "-rf", folderName).Run(); err != nil {
			t.Fatal(err)
		}
		if err := exec.Command("mkdir", "-p", folderName).Run(); err != nil {
			t.Fatal(err)
		}
	}

	for tmp := 1; tmp <= 5; tmp++ {
		folderName := fmt.Sprintf("%s_%d", dbFolderSlave, tmp)
		if err := exec.Command("rm", "-rf", folderName).Run(); err != nil {
			t.Fatal(err)
		}
		if err := exec.Command("mkdir", "-p", folderName).Run(); err != nil {
			t.Fatal(err)
		}
	}
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
	go serveStandaloneDKVSlave(&wg, slaveRDB, slaveRDB, masterCli, testingClusterInfo{})
	wg.Wait()

	// stop the slave poller so as to avoid race with this poller
	// and the explicit call to applyChangesFromMaster later
	slaveSvc.(*slaveService).replInfo.replTckr.Stop()
	slaveSvc.(*slaveService).replInfo.replStop <- struct{}{}
	sleepInSecs(2)
	// Reduce the max number of changes for testing
	//slaveSvc.(*slaveService).maxNumChngs = 100

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

	// we try to sync with master 10 times before giving up
	for i := 1; i <= 10; i++ {
		slaveSvc.(*slaveService).applyChangesFromMaster(100)
	}

	for i := 0; i < numKeys; i++ {
		getRes, _ := slaveCli.Get(0, keys[i])
		if !bytes.Equal(vals[i], getRes.Value) {
			t.Errorf("Value mismatch for key value pair at index: %d", i)
		}
	}
}

func initMasterAndSlaves(masterStore, slaveStore storage.KVStore, cp storage.ChangePropagator, ca storage.ChangeApplier, masterBU storage.Backupable) {
	var wg sync.WaitGroup
	wg.Add(1)
	go serveStandaloneDKVMaster(&wg, masterStore, cp, masterBU)
	wg.Wait()

	masterCli = newDKVClient(masterSvcPort)

	wg.Add(1)
	go serveStandaloneDKVSlave(&wg, slaveStore, ca, masterCli, testingClusterInfo{})
	wg.Wait()

	// stop the slave poller so as to avoid race with this poller
	// and the explicit call to applyChangesFromMaster later
	slaveSvc.(*slaveService).replInfo.replTckr.Stop()
	slaveSvc.(*slaveService).replInfo.replStop <- struct{}{}
	sleepInSecs(2)

	slaveCli = newDKVClient(slaveSvcPort)
}

func testMasterSlaveRepl(t *testing.T, masterStore, slaveStore storage.KVStore, cp storage.ChangePropagator, ca storage.ChangeApplier, masterBU storage.Backupable) {

	initMasterAndSlaves(masterStore, slaveStore, cp, ca, masterBU)
	defer closeMasterAndSlave()

	numKeys, keyPrefix, valPrefix := 10, "K", "V"
	putKeys(t, masterCli, numKeys, keyPrefix, valPrefix, 0)

	numKeys, ttlKeyPrefix, ttlValPrefix, ttlExpiredKeyPrefix := 10, "TTL-K", "TTL-V", "EXPIRED-TTL-K"
	putKeys(t, masterCli, numKeys, ttlKeyPrefix, ttlValPrefix, uint64(time.Now().Add(2*time.Hour).Unix()))
	putKeys(t, masterCli, numKeys, ttlExpiredKeyPrefix, ttlValPrefix, uint64(time.Now().Add(-2*time.Second).Unix()))

	testDelete(t, masterCli, keyPrefix)

	maxNumChangesRepl := uint32(100)

	if err := slaveSvc.(*slaveService).applyChangesFromMaster(maxNumChangesRepl); err != nil {
		t.Error(err)
	}

	getKeys(t, masterCli, numKeys, keyPrefix, valPrefix)
	getKeys(t, slaveCli, numKeys, keyPrefix, valPrefix)

	//TTL
	getKeys(t, masterCli, numKeys, ttlKeyPrefix, ttlValPrefix)
	getKeys(t, slaveCli, numKeys, ttlKeyPrefix, ttlValPrefix)

	//TTL Expired
	getInvalidKeys(t, masterCli, numKeys, ttlExpiredKeyPrefix)
	getInvalidKeys(t, slaveCli, numKeys, ttlExpiredKeyPrefix)

	getNonExistentKey(t, slaveCli, keyPrefix)

	backupFolder := fmt.Sprintf("%s/backup", masterDBFolder)
	if err := masterCli.Backup(backupFolder); err != nil {
		t.Fatalf("An error occurred while backing up. Error: %v", err)
	}

	numKeys, keyPrefix, valPrefix = 10, "BK", "BV"
	putKeys(t, masterCli, numKeys, keyPrefix, valPrefix, 0)
	testDelete(t, masterCli, keyPrefix)

	if err := slaveSvc.(*slaveService).applyChangesFromMaster(maxNumChangesRepl); err != nil {
		t.Error(err)
	}

	getKeys(t, masterCli, numKeys, keyPrefix, valPrefix)
	getKeys(t, slaveCli, numKeys, keyPrefix, valPrefix)
	getNonExistentKey(t, slaveCli, keyPrefix)

	if err := masterCli.Restore(backupFolder); err != nil {
		t.Fatalf("An error occurred while restoring. Error: %v", err)
	}

	if err := slaveSvc.(*slaveService).applyChangesFromMaster(maxNumChangesRepl); err == nil {
		t.Error("Expected an error from slave instance")
	} else {
		t.Log(err)
	}
}

func testGetStatus(t *testing.T, masterStore, slaveStore storage.KVStore, cp storage.ChangePropagator, ca storage.ChangeApplier, masterBU storage.Backupable) {
	initMasterAndSlaves(masterStore, slaveStore, cp, ca, masterBU)
	defer closeMaster()

	numKeys, keyPrefix, valPrefix := 10, "KGetStatus", "VGetStatus"
	putKeys(t, masterCli, numKeys, keyPrefix, valPrefix, 0)

	slaveServer := slaveSvc.(*slaveService)
	validateStatus(t, "replNotStarted", serverpb.RegionStatus_INACTIVE)

	// Validate status when too high lag
	if err := slaveServer.applyChangesFromMaster(2); err != nil {
		t.Error(err)
	}
	if slaveServer.replInfo.replLag != 16 {
		t.Errorf("Replication lag unexpected, Expected: %d, Actual: %d", 16, slaveServer.replInfo.replLag)
	}
	validateStatus(t, "tooHighReplLag", serverpb.RegionStatus_INACTIVE)

	// Validate status when replication catching up
	if err := slaveServer.applyChangesFromMaster(4); err != nil {
		t.Error(err)
	}
	if slaveServer.replInfo.replLag != 8 {
		t.Errorf("Replication lag unexpected, Expected: %d, Actual: %d", 8, slaveServer.replInfo.replLag)
	}
	validateStatus(t, "replCaughtUp", serverpb.RegionStatus_ACTIVE_SLAVE)

	// Validate status when replication not successful for long time
	time.Sleep(7 * time.Second)
	validateStatus(t, "replDelayed", serverpb.RegionStatus_INACTIVE)

	// Validate status when replication caught up
	if err := slaveServer.applyChangesFromMaster(10); err != nil {
		t.Error(err)
	}
	if slaveServer.replInfo.replLag != 0 {
		t.Errorf("Replication lag unexpected, Expected: %d, Actual: %d", 0, slaveServer.replInfo.replLag)
	}
	validateStatus(t, "replCaughtUp", serverpb.RegionStatus_ACTIVE_SLAVE)

	// Validate status after closing
	closeSlave()
	validateStatus(t, "replCaughtUp", serverpb.RegionStatus_INACTIVE)
}

func validateStatus(t *testing.T, useCase string, expectedStatus serverpb.RegionStatus) {
	regionInfo, _ := slaveSvc.GetStatus(nil, nil)
	if regionInfo.Status != expectedStatus {
		t.Errorf("Unexpected status. Use case: %s, Expected: %s, Actual: %s", useCase,
			expectedStatus.String(), regionInfo.Status.String())
	}
}

func testHealthCheck(t *testing.T, masterStore, slaveStore storage.KVStore, cp storage.ChangePropagator, ca storage.ChangeApplier, masterBU storage.Backupable) {
	initMasterAndSlaves(masterStore, slaveStore, cp, ca, masterBU)
	defer closeMaster()

	numKeys, keyPrefix, valPrefix := 10, "KHealthCheck", "VHealthCheck"
	putKeys(t, masterCli, numKeys, keyPrefix, valPrefix, 0)

	slaveServer := slaveSvc.(*slaveService)
	validateHealthCheckResponse(t, "replNotStarted", health.HealthCheckResponse_NOT_SERVING)

	// Validate status when too high lag
	if err := slaveServer.applyChangesFromMaster(2); err != nil {
		t.Error(err)
	}
	if slaveServer.replInfo.replLag != 16 {
		t.Errorf("Replication lag unexpected, Expected: %d, Actual: %d", 16, slaveServer.replInfo.replLag)
	}
	validateHealthCheckResponse(t, "tooHighReplLag", health.HealthCheckResponse_NOT_SERVING)

	// Validate status when replication catching up
	if err := slaveServer.applyChangesFromMaster(4); err != nil {
		t.Error(err)
	}
	if slaveServer.replInfo.replLag != 8 {
		t.Errorf("Replication lag unexpected, Expected: %d, Actual: %d", 8, slaveServer.replInfo.replLag)
	}
	validateHealthCheckResponse(t, "replCaughtUp", health.HealthCheckResponse_SERVING)

	// Validate status when replication not successful for long time
	time.Sleep(7 * time.Second)
	validateHealthCheckResponse(t, "replDelayed", health.HealthCheckResponse_NOT_SERVING)

	// Validate status when replication caught up
	if err := slaveServer.applyChangesFromMaster(10); err != nil {
		t.Error(err)
	}
	if slaveServer.replInfo.replLag != 0 {
		t.Errorf("Replication lag unexpected, Expected: %d, Actual: %d", 0, slaveServer.replInfo.replLag)
	}
	validateHealthCheckResponse(t, "replCaughtUp", health.HealthCheckResponse_SERVING)

	// Validate status after closing
	closeSlave()
	validateHealthCheckResponse(t, "slaveClosed", health.HealthCheckResponse_NOT_SERVING)
}

func testHealthCheckStream(t *testing.T, masterStore, slaveStore storage.KVStore, cp storage.ChangePropagator, ca storage.ChangeApplier, masterBU storage.Backupable) {
	initMasterAndSlaves(masterStore, slaveStore, cp, ca, masterBU)
	defer closeMaster()

	numKeys, keyPrefix, valPrefix := 10, "KHealthCheck", "VHealthCheck"
	putKeys(t, masterCli, numKeys, keyPrefix, valPrefix, 0)

	slaveServer := slaveSvc.(*slaveService)
	healthCheckCli, err := newHealthCheckClient(slaveSvcPort)
	if err != nil {
		t.Errorf("Error while creating health check client. Error: %v", err)
	}
	defer healthCheckCli.cliConn.Close()
	validateHealthCheckResponseStream(t, "replNotStarted", health.HealthCheckResponse_NOT_SERVING, healthCheckCli)

	// Validate status when too high lag
	if err := slaveServer.applyChangesFromMaster(2); err != nil {
		t.Error(err)
	}
	if slaveServer.replInfo.replLag != 16 {
		t.Errorf("Replication lag unexpected, Expected: %d, Actual: %d", 16, slaveServer.replInfo.replLag)
	}
	validateHealthCheckResponseStream(t, "tooHighReplLag", health.HealthCheckResponse_NOT_SERVING, healthCheckCli)

	// Validate status when replication catching up
	if err := slaveServer.applyChangesFromMaster(4); err != nil {
		t.Error(err)
	}
	if slaveServer.replInfo.replLag != 8 {
		t.Errorf("Replication lag unexpected, Expected: %d, Actual: %d", 8, slaveServer.replInfo.replLag)
	}

	// this health check has been done in the same thread as the replication thread. That's why we cannot
	// have a health check time interval greater than the max replication lag. In a production use case
	// health check will done parallel to replication so the health check will not fail in case the time period
	// of the health check is greater than max replication lag
	validateHealthCheckResponseStream(t, "replCaughtUp", health.HealthCheckResponse_SERVING, healthCheckCli)

	// Validate status when replication not successful for long time
	time.Sleep(7 * time.Second)
	validateHealthCheckResponseStream(t, "replDelayed", health.HealthCheckResponse_NOT_SERVING, healthCheckCli)

	// Validate status when replication caught up
	if err := slaveServer.applyChangesFromMaster(10); err != nil {
		t.Error(err)
	}
	if slaveServer.replInfo.replLag != 0 {
		t.Errorf("Replication lag unexpected, Expected: %d, Actual: %d", 0, slaveServer.replInfo.replLag)
	}
	validateHealthCheckResponseStream(t, "replCaughtUp", health.HealthCheckResponse_SERVING, healthCheckCli)

	// Validate status after closing
	closeSlaveOnly()
	validateHealthCheckResponseStream(t, "slaveClosed", health.HealthCheckResponse_NOT_SERVING, healthCheckCli)
	slaveGrpcSrvr.Stop()
}

func validateHealthCheckResponseStream(t *testing.T, useCase string, expectedResponse health.HealthCheckResponse_ServingStatus, healthCheckCli *HealthCheckClient) {
	actualResponse, _ := healthCheckCli.healthCheckCli.Watch(context.Background(), &health.HealthCheckRequest{})
	if actualResponse == nil {
		t.Fatalf("Actual response is nil in use case %s. Expected Response: %s", useCase, expectedResponse.String())
	}
	recv, err := actualResponse.Recv()
	if err != nil {
		t.Fatalf("Error while receiving response from serve. Error: %v", err)
	}
	if recv == nil {
		t.Errorf("Recevied null message from the server during health check")
	}
	if recv.GetStatus() != expectedResponse {
		t.Errorf("Unexpected status. Use case: %s, Expected: %s, Actual: %s", useCase, expectedResponse.String(), recv.GetStatus().String())
	}
}

func validateHealthCheckResponse(t *testing.T, useCase string, expectedResponse health.HealthCheckResponse_ServingStatus) {
	actualResponse, _ := slaveSvc.Check(nil, nil)
	if actualResponse.GetStatus() != expectedResponse {
		t.Errorf("Unexpected status. Use case: %s, Expected: %s, Actual: %s", useCase,
			expectedResponse.String(), actualResponse.GetStatus().String())
	}
}

func putKeys(t *testing.T, dkvCli *ctl.DKVClient, numKeys int, keyPrefix, valPrefix string, ttl uint64) {
	for i := 1; i <= numKeys; i++ {
		key, value := fmt.Sprintf("%s%d", keyPrefix, i), fmt.Sprintf("%s%d", valPrefix, i)
		if err := dkvCli.PutTTL([]byte(key), []byte(value), ttl); err != nil {
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

func getInvalidKeys(t *testing.T, dkvCli *ctl.DKVClient, numKeys int, keyPrefix string) {
	rc := serverpb.ReadConsistency_SEQUENTIAL
	for i := 1; i <= numKeys; i++ {
		key := fmt.Sprintf("%s%d", keyPrefix, i)
		if res, err := dkvCli.Get(rc, []byte(key)); err != nil {
			t.Fatalf("Unable to GET. Key: %s, Error: %v", key, err)
		} else if res != nil && string(res.Value) != "" {
			t.Errorf("Expected no value for key %s. But got %s", key, string(res.Value))
		}
	}
}

func getNonExistentKey(t *testing.T, dkvCli *ctl.DKVClient, keyPrefix string) {
	rc := serverpb.ReadConsistency_SEQUENTIAL
	key := keyPrefix + "DeletedKey"
	if val, _ := dkvCli.Get(rc, []byte(key)); val != nil && string(val.Value) != "" {
		t.Errorf("Expected no value for key %s. But got %s", key, val)
	}
}

func testDelete(t *testing.T, dkvCli *ctl.DKVClient, keyPrefix string) {
	key, value := keyPrefix+"DeletedKey", "SomeValue"
	rc := serverpb.ReadConsistency_SEQUENTIAL
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

func newDKVClient(port int) *ctl.DKVClient {
	dkvSvcAddr := fmt.Sprintf("%s:%d", dkvSvcHost, port)
	if client, err := ctl.NewInSecureDKVClient(dkvSvcAddr, "", ctl.DefaultConnectOpts); err != nil {
		panic(err)
	} else {
		return client
	}

}

func newHealthCheckClient(port int) (*HealthCheckClient, error) {
	var options []grpc.DialOption
	options = append(options, grpc.WithInsecure())
	options = append(options, grpc.WithBlock())
	dkvSvcAddr := fmt.Sprintf("%s:%d", dkvSvcHost, port)
	conn, err := grpc.DialContext(context.Background(), dkvSvcAddr, options...)
	if err != nil {
		return nil, err
	}
	client := health.NewHealthClient(conn)
	return &HealthCheckClient{cliConn: conn, healthCheckCli: client}, nil
}

func newRocksDBStore(dbFolder string) rocksdb.DB {
	if err := exec.Command("rm", "-rf", dbFolder).Run(); err != nil {
		panic(err)
	}
	store, err := rocksdb.OpenDB(dbFolder,
		rocksdb.WithSyncWrites(), rocksdb.WithCacheSize(cacheSize))
	if err != nil {
		panic(err)
	}
	return store
}

func newBadgerDBStore(dbFolder string) badger.DB {
	if err := exec.Command("rm", "-rf", dbFolder).Run(); err != nil {
		panic(err)
	}
	store, err := badger.OpenDB(badger.WithSyncWrites(), badger.WithDBDir(dbFolder))
	if err != nil {
		panic(err)
	}
	return store
}

func serveStandaloneDKVMaster(wg *sync.WaitGroup, store storage.KVStore, cp storage.ChangePropagator, bu storage.Backupable) {
	// No need to set the storage.Backupable instance since its not needed here
	masterSvc = master.NewStandaloneService(store, cp, bu, &serverpb.RegionInfo{}, serverOpts)
	masterGrpcSrvr = grpc.NewServer()
	serverpb.RegisterDKVServer(masterGrpcSrvr, masterSvc)
	serverpb.RegisterDKVReplicationServer(masterGrpcSrvr, masterSvc)
	serverpb.RegisterDKVBackupRestoreServer(masterGrpcSrvr, masterSvc)
	lis := listen(masterSvcPort)
	wg.Done()
	masterGrpcSrvr.Serve(lis)
}

func serveStandaloneDKVSlave(wg *sync.WaitGroup, store storage.KVStore, ca storage.ChangeApplier, masterCli *ctl.DKVClient, discoveryClient discovery.Client) {
	lgr, _ := zap.NewDevelopment()
	replConf := ReplicationConfig{
		MaxNumChngs:          2,
		ReplPollInterval:     5 * time.Second,
		MaxActiveReplLag:     10,
		MaxActiveReplElapsed: 5,
	}

	specialOpts := &opts.ServerOpts{
		Logger:                    lgr,
		StatsCli:                  stats.NewNoOpClient(),
		PrometheusRegistry:        stats.NewPromethousNoopRegistry(),
		HealthCheckTickerInterval: uint(1),
	}

	if ss, err := NewService(store, ca, &serverpb.RegionInfo{Database: dbName, VBucket: vbucket}, &replConf, discoveryClient, specialOpts); err != nil {
		panic(err)
	} else {
		slaveSvc = ss
		if masterCli != nil {
			slaveSvc.(*slaveService).replInfo.replCli = masterCli
		}
		slaveGrpcSrvr = grpc.NewServer()
		serverpb.RegisterDKVServer(slaveGrpcSrvr, slaveSvc)
		health.RegisterHealthServer(slaveGrpcSrvr, slaveSvc)
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

func closeMasterAndSlave() {
	closeMaster()
	closeSlave()
}

func closeMaster() {
	masterCli.Close()
	masterSvc.Close()
	masterGrpcSrvr.GracefulStop()
}

func closeSlave() {
	slaveCli.Close()
	slaveSvc.Close()
	slaveGrpcSrvr.GracefulStop()
}

func closeSlaveOnly() {
	slaveCli.Close()
	slaveSvc.Close()
}

type testingClusterInfo struct {
	region *serverpb.RegionInfo
	discovery.Client
}

func (m testingClusterInfo) GetClusterStatus(database string, vBucket string) ([]*serverpb.RegionInfo, error) {
	regions := make([]*serverpb.RegionInfo, 1)
	regions[0] = &serverpb.RegionInfo{
		NodeAddress: fmt.Sprintf("127.0.0.1:%d", masterSvcPort),
		Database:    database,
		DcID:        "default",
		VBucket:     vBucket,
		Status:      serverpb.RegionStatus_LEADER,
	}

	return regions, nil
}
