package master

import (
	"context"
	"fmt"
	"net"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/flipkart-incubator/dkv/internal/ctl"
	"github.com/flipkart-incubator/dkv/internal/server/storage"
	"github.com/flipkart-incubator/dkv/internal/server/storage/badger"
	"github.com/flipkart-incubator/dkv/internal/server/storage/rocksdb"
	dkv_sync "github.com/flipkart-incubator/dkv/internal/server/sync"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
	nexus_api "github.com/flipkart-incubator/nexus/pkg/api"
	nexus "github.com/flipkart-incubator/nexus/pkg/raft"
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
				t.Fatalf("Unable to GET for CLI ID: %d. Key: %s, Error: %v", j, key, err)
			} else if string(actualValue.Value) != expectedValue {
				t.Errorf("GET mismatch for CLI ID: %d. Key: %s, Expected Value: %s, Actual Value: %s", j, key, expectedValue, actualValue)
			}
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
					t.Fatalf("Unable to GET for CLI ID: %d. Key: %s, Error: %v", idx, key, err)
				} else if string(actualValue.Value) != value {
					t.Errorf("GET mismatch for CLI ID: %d. Key: %s, Expected Value: %s, Actual Value: %s", idx, key, value, actualValue)
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
	// Add new DKV node through the client of any other DKV node (1)
	if err := dkvClis[1].AddNode(newNodeID, newNodeURL); err != nil {
		t.Fatal(err)
	}
	<-time.After(3 * time.Second)
	clusURL := fmt.Sprintf("%s,%s", clusterURL, newNodeURL)
	// Create the new DKV node
	dkvSvc, grpcSrv := newDistributedDKVNode(newNodeID, true, clusURL)
	defer dkvSvc.Close()
	defer grpcSrv.GracefulStop()
	go grpcSrv.Serve(newListener(dkvPorts[newNodeID]))
	<-time.After(3 * time.Second)
	svcAddr := fmt.Sprintf("%s:%d", dkvSvcHost, dkvPorts[newNodeID])
	// Create the client for the new DKV node
	if dkvCli, err := ctl.NewInSecureDKVClient(svcAddr); err != nil {
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
		if err := dkvClis[2].RemoveNode(newNodeID); err != nil {
			t.Fatal(err)
		}
		<-time.After(3 * time.Second)
	}
}

func initDKVClients(ids ...int) {
	for id := 1; id <= clusterSize; id++ {
		svcAddr := fmt.Sprintf("%s:%d", dkvSvcHost, dkvPorts[id])
		if client, err := ctl.NewInSecureDKVClient(svcAddr); err != nil {
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
	for id := 1; id <= clusterSize; id++ {
		go serveDistributedDKV(id)
	}
}

func newReplicator(id int, kvs storage.KVStore, joining bool, clusterURL string) nexus_api.RaftReplicator {
	replStore := dkv_sync.NewDKVReplStore(kvs)
	opts := []nexus.Option{
		nexus.NodeId(id),
		nexus.LogDir(logDir),
		nexus.SnapDir(snapDir),
		nexus.ClusterUrl(clusterURL),
		nexus.ReplicationTimeout(replTimeout),
		nexus.Join(joining),
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

func newDistributedDKVNode(id int, join bool, clusURL string) (DKVService, *grpc.Server) {
	kvs, cp, br := newKVStoreWithID(id)
	dkvRepl := newReplicator(id, kvs, join, clusURL)
	dkvRepl.Start()
	distSrv := NewDistributedService(kvs, cp, br, dkvRepl)
	grpcSrv := grpc.NewServer()
	serverpb.RegisterDKVServer(grpcSrv, distSrv)
	serverpb.RegisterDKVClusterServer(grpcSrv, distSrv)
	return distSrv, grpcSrv
}

func serveDistributedDKV(id int) {
	dkvSvc, grpcSvc := newDistributedDKVNode(id, false, clusterURL)
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
