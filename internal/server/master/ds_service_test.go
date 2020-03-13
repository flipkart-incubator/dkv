package master

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
	clusterURL  = "http://127.0.0.1:9321,http://127.0.0.1:9322,http://127.0.0.1:9323"
	replTimeout = 3 * time.Second
)

var (
	grpcSrvs = make(map[int]*grpc.Server)
	dkvPorts = map[int]int{1: 9081, 2: 9082, 3: 9083}
	dkvClis  = make(map[int]*ctl.DKVClient)
	dkvSvcs  = make(map[int]DKVService)
	mutex    = sync.Mutex{}
)

func TestDistributedService(t *testing.T) {
	initDKVServers()
	sleepInSecs(3)
	initDKVClients()
	defer stopClients()
	defer stopServers()

	t.Run("testDistributedPut", testDistributedPut)
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
		for _, dkvCli := range dkvClis {
			if actualValue, err := dkvCli.Get([]byte(key)); err != nil {
				t.Fatalf("Unable to GET for CLI ID: %d. Key: %s, Error: %v", i, key, err)
			} else if string(actualValue.Value) != expectedValue {
				t.Errorf("GET mismatch for CLI ID: %d. Key: %s, Expected Value: %s, Actual Value: %s", i, key, expectedValue, actualValue)
			}
		}
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

func initDKVServers(ids ...int) {
	for id := 1; id <= clusterSize; id++ {
		go serveDistributedDKV(id)
	}
}

func newReplicator(id int, kvs storage.KVStore) nexus_api.RaftReplicator {
	replStore := dkv_sync.NewDKVReplStore(kvs)
	opts := []nexus.Option{
		nexus.NodeId(id),
		nexus.LogDir(logDir),
		nexus.SnapDir(snapDir),
		nexus.ClusterUrl(clusterURL),
		nexus.ReplicationTimeout(replTimeout),
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

func newKVStoreWithID(id int) (storage.KVStore, storage.ChangePropagator) {
	dbFolder := fmt.Sprintf("%s_%d", dbFolder, id)
	if err := exec.Command("rm", "-rf", dbFolder).Run(); err != nil {
		panic(err)
	}
	switch engine {
	case "rocksdb":
		rocksDb := rocksdb.OpenDB(dbFolder, cacheSize)
		return rocksDb, rocksDb
	case "badger":
		return badger.OpenDB(dbFolder), nil
	default:
		panic(fmt.Sprintf("Unknown storage engine: %s", engine))
	}
}

func serveDistributedDKV(id int) {
	kvs, cp := newKVStoreWithID(id)
	dkvRepl := newReplicator(id, kvs)
	dkvRepl.Start()
	mutex.Lock()
	dkvSvcs[id] = NewDistributedService(kvs, cp, dkvRepl)
	grpcSrvs[id] = grpc.NewServer()
	mutex.Unlock()
	serverpb.RegisterDKVServer(grpcSrvs[id], dkvSvcs[id])
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
