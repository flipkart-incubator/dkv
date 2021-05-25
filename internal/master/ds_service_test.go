package master

import (
	"bytes"
	"context"
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
	dkv_sync "github.com/flipkart-incubator/dkv/internal/sync"
	"github.com/flipkart-incubator/dkv/pkg/ctl"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
	nexus_api "github.com/flipkart-incubator/nexus/pkg/api"
	nexus "github.com/flipkart-incubator/nexus/pkg/raft"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	clusterSize = 3
	logDir      = "/tmp/dkv_test/logs"
	snapDir     = "/tmp/dkv_test/snap"
	clusterURL  = "http://127.0.0.1:9321,http://127.0.0.1:9322,http://127.0.0.1:9323"
	replTimeout = 3 * time.Second
	newNodeID   = 4
	newNodeURL  = "http://127.0.0.1:9324"
)

var (
	grpcSrvs = make(map[int]*grpc.Server)
	dkvPorts = map[int]int{1: 9081, 2: 9082, 3: 9083, 4: 9084}
	dkvClis  = make(map[int]*ctl.DKVClient)
	dkvSvcs  = make(map[int]DKVService)
	mutex    = sync.Mutex{}
	rc       = serverpb.ReadConsistency_SEQUENTIAL
)

func TestDistributedService(t *testing.T) {
	resetRaftStateDirs(t)
	initDKVServers()
	sleepInSecs(3)
	initDKVClients()
	defer stopClients()
	defer stopServers()

	t.Run("testDistributedPut", testDistributedPut)
	t.Run("testDistAtomicKeyCreation", testDistAtomicKeyCreation)
	t.Run("testDistAtomicIncrDecr", testDistAtomicIncrDecr)
	t.Run("testLinearizableGet", testLinearizableGet)
	t.Run("testRestore", testRestore)
	t.Run("testNewDKVNodeJoiningAndLeaving", testNewDKVNodeJoiningAndLeaving)
}

func testDistributedPut(t *testing.T) {
	for i := 1; i <= clusterSize; i++ {
		key, value := fmt.Sprintf("K_CLI_%d", i), fmt.Sprintf("V_CLI_%d", i)
		if err := dkvClis[i].Put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Unable to PUT. Key: %s, Value: %s, Error: %v", key, value, err)
		}
	}
	sleepInSecs(3)
	for i := 1; i <= clusterSize; i++ {
		key, expectedValue := fmt.Sprintf("K_CLI_%d", i), fmt.Sprintf("V_CLI_%d", i)
		for j, dkvCli := range dkvClis {
			if actualValue, err := dkvCli.Get(rc, []byte(key)); err != nil {
				t.Errorf("Unable to GET for CLI ID: %d. Key: %s, Error: %v", j, key, err)
			} else if string(actualValue.Value) != expectedValue {
				t.Errorf("GET mismatch for CLI ID: %d. Key: %s, Expected Value: %s, Actual Value: %s", j, key, expectedValue, actualValue)
			}
		}
	}
}

func testDistAtomicKeyCreation(t *testing.T) {
	var (
		wg             sync.WaitGroup
		freqs          sync.Map
		numThrs        = 10
		casKey, casVal = []byte("casKey"), []byte{0}
	)

	// verify atomic key creation under contention
	// against all cluster nodes
	cliID := 0
	for i := 0; i < numThrs; i++ {
		wg.Add(1)
		// cycle through the next cluster node client
		cliID = 1 + (cliID+1)%clusterSize
		go func(id, clId int) {
			defer wg.Done()
			res, err := dkvClis[clId].CompareAndSet(casKey, nil, casVal)
			freqs.Store(id, res && err == nil)
		}(i, cliID)
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

func testDistAtomicIncrDecr(t *testing.T) {
	var (
		wg             sync.WaitGroup
		numThrs        = 10
		casKey, casVal = []byte("ctrKey"), []byte{0}
	)
	dkvClis[1].Put(casKey, casVal)

	// even threads increment, odd threads decrement
	// a given key, all this done across cluster nodes
	for i := 0; i < numThrs; i++ {
		wg.Add(1)
		// cycle through the next cluster node client
		cliID := 1 + i%clusterSize
		go func(id, clId int) {
			defer wg.Done()
			delta := byte(0)
			if (id & 1) == 1 { // odd
				delta--
			} else {
				delta++
			}
			dkvCli := dkvClis[clId]
			for {
				exist, err := dkvCli.Get(rc, casKey)
				if err != nil {
					t.Errorf("Unable to perform GET against client ID: %d. Error: %v", clId, err)
					break
				}
				expect := exist.Value
				if len(expect) > 0 {
					update := []byte{expect[0] + delta}
					res, err := dkvCli.CompareAndSet(casKey, expect, update)
					if res && err == nil {
						break
					}
				} else {
					t.Logf("Empty GET against client ID: %d. Error: %v", clId, err)
				}
			}
		}(i, cliID)
	}
	wg.Wait()

	for i := 1; i <= clusterSize; i++ {
		actual, _ := dkvClis[i].Get(serverpb.ReadConsistency_LINEARIZABLE, casKey)
		actVal := actual.Value
		// since even and odd increments cancel out completely
		// we should expect `actVal` to be 0 (i.e., `casVal`)
		if !bytes.Equal(casVal, actVal) {
			t.Errorf("Mismatch in values for key: %s. Expected: %d, Actual: %d", string(casKey), casVal[0], actVal[0])
		}
	}
}

func testLinearizableGet(t *testing.T) {
	getRC := serverpb.ReadConsistency_LINEARIZABLE
	kp, vp := "LGK", "LGV"
	for i := 1; i <= clusterSize; i++ {
		key, value := fmt.Sprintf("%s%d", kp, i), fmt.Sprintf("%s%d", vp, i)
		if err := dkvClis[i].Put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Unable to PUT. Key: %s, Value: %s, Error: %v", key, value, err)
		}
		wg := sync.WaitGroup{}
		for j, dkvCli := range dkvClis {
			wg.Add(1)
			go func(idx int, dkvClnt *ctl.DKVClient) {
				defer wg.Done()
				if actualValue, err := dkvClnt.Get(getRC, []byte(key)); err != nil {
					t.Errorf("Unable to GET for CLI ID: %d. Key: %s, Error: %v", idx, key, err)
				} else if string(actualValue.Value) != value {
					t.Errorf("GET mismatch for CLI ID: %d. Key: %s, Expected Value: %s, Actual Value: %v", idx, key, value, actualValue)
				}
			}(j, dkvCli)
		}
		wg.Wait()
	}
}

func testRestore(t *testing.T) {
	for _, dkvSvc := range dkvSvcs {
		rstrReq := new(serverpb.RestoreRequest)
		if _, err := dkvSvc.Restore(context.Background(), rstrReq); err == nil {
			t.Error("Expected error during restore from service, but got none")
		} else {
			t.Log(err)
		}
	}
}

func testNewDKVNodeJoiningAndLeaving(t *testing.T) {
	// Create and start the new DKV node
	dkvSvc, grpcSrv := newDistributedDKVNode(newNodeID, newNodeURL, clusterURL)
	defer dkvSvc.Close()
	defer grpcSrv.GracefulStop()
	go grpcSrv.Serve(newListener(dkvPorts[newNodeID]))
	<-time.After(3 * time.Second)

	// Add this new DKV node through the client of any other DKV node (1)
	if err := dkvClis[1].AddNode(newNodeURL); err != nil {
		t.Fatal(err)
	}
	<-time.After(3 * time.Second)

	// Create the client for the new DKV node
	svcAddr := fmt.Sprintf("%s:%d", dkvSvcHost, dkvPorts[newNodeID])
	if dkvCli, err := ctl.NewDKVClient(svcAddr, "", grpc.WithInsecure()); err != nil {
		t.Fatal(err)
	} else {
		defer dkvCli.Close()
		// Expect to find all data in the new DKV node
		for i := 1; i <= clusterSize; i++ {
			key, expectedValue := fmt.Sprintf("K_CLI_%d", i), fmt.Sprintf("V_CLI_%d", i)
			if actualValue, err := dkvCli.Get(rc, []byte(key)); err != nil {
				t.Fatalf("Unable to GET for CLI ID: %d. Key: %s, Error: %v", i, key, err)
			} else if string(actualValue.Value) != expectedValue {
				t.Errorf("GET mismatch for CLI ID: %d. Key: %s, Expected Value: %s, Actual Value: %s", i, key, expectedValue, actualValue)
			}
		}
		// Remove new DKV node through the client of any other DKV Node (2)
		if err := dkvClis[2].RemoveNode(newNodeURL); err != nil {
			t.Fatal(err)
		}
		<-time.After(3 * time.Second)
	}
}

func initDKVClients(ids ...int) {
	for id := 1; id <= clusterSize; id++ {
		svcAddr := fmt.Sprintf("%s:%d", dkvSvcHost, dkvPorts[id])
		if client, err := ctl.NewDKVClient(svcAddr, "", grpc.WithInsecure()); err != nil {
			panic(err)
		} else {
			dkvClis[id] = client
		}
	}
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
}

func initDKVServers(ids ...int) {
	clusURLs := strings.Split(clusterURL, ",")
	for id := 1; id <= clusterSize; id++ {
		go serveDistributedDKV(id, clusURLs[id-1])
	}
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

func newKVStoreWithID(id int) (storage.KVStore, storage.ChangePropagator, storage.Backupable) {
	dbFolder := fmt.Sprintf("%s_%d", dbFolder, id)
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

func newDistributedDKVNode(id int, nodeURL, clusURL string) (DKVService, *grpc.Server) {
	kvs, cp, br := newKVStoreWithID(id)
	dkvRepl := newReplicator(kvs, nodeURL, clusURL)
	dkvRepl.Start()
	distSrv := NewDistributedService(kvs, cp, br, dkvRepl, zap.NewNop(), stats.NewNoOpClient())
	grpcSrv := grpc.NewServer()
	serverpb.RegisterDKVServer(grpcSrv, distSrv)
	serverpb.RegisterDKVClusterServer(grpcSrv, distSrv)
	return distSrv, grpcSrv
}

func serveDistributedDKV(id int, nodeURL string) {
	dkvSvc, grpcSvc := newDistributedDKVNode(id, nodeURL, clusterURL)
	mutex.Lock()
	dkvSvcs[id] = dkvSvc
	grpcSrvs[id] = grpcSvc
	mutex.Unlock()
	grpcSrvs[id].Serve(newListener(dkvPorts[id]))
}

func stopClients() {
	for id := 1; id <= clusterSize; id++ {
		dkvClis[id].Close()
	}
}

func stopServers() {
	for id := 1; id <= clusterSize; id++ {
		dkvSvcs[id].Close()
		grpcSrvs[id].GracefulStop()
	}
}
