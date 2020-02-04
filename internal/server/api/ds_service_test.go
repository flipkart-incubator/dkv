package api

import (
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
	clusterUrl  = "http://127.0.0.1:9321,http://127.0.0.1:9322,http://127.0.0.1:9323"
	peer4Id     = 4
	peer4Url    = "http://127.0.0.1:9324"
	replTimeout = 3 * time.Second
)

var (
	dkvRepls = make(map[int]nexus_api.RaftReplicator)
	kvStores = make(map[int]storage.KVStore)
	grpcSrvs = make(map[int]*grpc.Server)
	dkvClnts = make(map[int]*ctl.DKVClient)
	dkvPorts = map[int]int{1: 9081, 2: 9082, 3: 9083}
	mutex    = sync.Mutex{}
)

func TestDistributedService(t *testing.T) {
	initDKVServers(1, 2, 3)
	sleepInSecs(3)
	initDKVClients(1, 2, 3)
	defer stopClients()
	defer stopServers()

	t.Run("testDistributedPut", testDistributedPut)
}

func testDistributedPut(t *testing.T) {
	for i, dkv_cli := range dkvClnts {
		key, value := fmt.Sprintf("K_CLI_%d", i), fmt.Sprintf("V_CLI_%d", i)
		if err := dkv_cli.Put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Unable to PUT. Key: %s, Value: %s, Error: %v", key, value, err)
		}
	}
	sleepInSecs(3)
	for i := range dkvClnts {
		key, expectedValue := fmt.Sprintf("K_CLI_%d", i), fmt.Sprintf("V_CLI_%d", i)
		for _, dkv_cli := range dkvClnts {
			if actualValue, err := dkv_cli.Get([]byte(key)); err != nil {
				t.Fatalf("Unable to GET for CLI ID: %d. Key: %s, Error: %v", i, key, err)
			} else if string(actualValue.Value) != expectedValue {
				t.Errorf("GET mismatch for CLI ID: %d. Key: %s, Expected Value: %s, Actual Value: %s", i, key, expectedValue, actualValue)
			}
		}
	}
}

func initDKVClients(ids ...int) {
	for _, id := range ids {
		svc_addr := fmt.Sprintf("%s:%d", dkvSvcHost, dkvPorts[id])
		if client, err := ctl.NewInSecureDKVClient(svc_addr); err != nil {
			panic(err)
		} else {
			dkvClnts[id] = client
		}
	}
}

func initDKVServers(ids ...int) {
	for _, id := range ids {
		go serveDistributedDKV(id)
	}
}

func newReplicator(id int, kvs storage.KVStore) nexus_api.RaftReplicator {
	repl_store := dkv_sync.NewDKVReplStore(kvs)
	opts := []nexus.Option{
		nexus.NodeId(id),
		nexus.LogDir(logDir),
		nexus.SnapDir(snapDir),
		nexus.ClusterUrl(clusterUrl),
		nexus.ReplicationTimeout(replTimeout),
	}
	if nexus_repl, err := nexus_api.NewRaftReplicator(repl_store, opts...); err != nil {
		panic(err)
	} else {
		return nexus_repl
	}
}

func newListener(port int) net.Listener {
	if lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port)); err != nil {
		panic(fmt.Sprintf("failed to listen: %v", err))
	} else {
		return lis
	}
}

func newKVStoreWithId(id int) storage.KVStore {
	db_folder := fmt.Sprintf("%s_%d", dbFolder, id)
	if err := exec.Command("rm", "-rf", db_folder).Run(); err != nil {
		panic(err)
	}
	switch engine {
	case "rocksdb":
		return rocksdb.OpenDB(db_folder, cacheSize)
	case "badger":
		return badger.OpenDB(db_folder)
	default:
		panic(fmt.Sprintf("Unknown storage engine: %s", engine))
	}
}

func serveDistributedDKV(id int) {
	mutex.Lock()
	kvStores[id] = newKVStoreWithId(id)
	dkvRepls[id] = newReplicator(id, kvStores[id])
	grpcSrvs[id] = grpc.NewServer()
	mutex.Unlock()
	dkv_svc := NewDistributedService(kvStores[id], dkvRepls[id])
	serverpb.RegisterDKVServer(grpcSrvs[id], dkv_svc)
	lstnr := newListener(dkvPorts[id])
	dkvRepls[id].Start()
	grpcSrvs[id].Serve(lstnr)
}

func stopClients() {
	for id := range dkvClnts {
		dkvClnts[id].Close()
	}
}

func stopServers() {
	for id := range kvStores {
		dkvRepls[id].Stop()
		grpcSrvs[id].GracefulStop()
	}
}
