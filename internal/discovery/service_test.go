package discovery

import (
	"fmt"
	"github.com/flipkart-incubator/dkv/internal/opts"
	"net"
	"os/exec"
	"testing"
	"time"

	"github.com/flipkart-incubator/dkv/internal/master"
	"github.com/flipkart-incubator/dkv/internal/storage"
	"github.com/flipkart-incubator/dkv/internal/storage/badger"
	"github.com/flipkart-incubator/dkv/internal/storage/rocksdb"
	"github.com/flipkart-incubator/dkv/pkg/ctl"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	dkvSvcPort       = 8082
	dkvSvcHost       = "localhost"
	dbFolder         = "/tmp/dkv_discovery_test_db"
	cacheSize        = 3 << 30
	engine           = "rocksdb"
	heartBeatTimeOut = 2
	statusTtl        = 5
)

func TestDKVDiscoveryService(t *testing.T) {
	var dkvCli *ctl.DKVClient
	var err error

	dkvSvc, grpcSvc := serveStandaloneDKVWithDiscovery(dkvSvcPort, &serverpb.RegionInfo{}, dbFolder+"_DS")
	defer dkvSvc.Close()
	defer grpcSvc.GracefulStop()

	svcAddr := fmt.Sprintf("%s:%d", dkvSvcHost, dkvSvcPort)
	if dkvCli, err = ctl.NewInSecureDKVClient(svcAddr, "", ctl.DefaultConnectOpts); err != nil {
		panic(err)
	}
	defer dkvCli.Close()

	regionInfo := serverpb.RegionInfo{
		DcID:            "dc1",
		NodeAddress:     "host1:port",
		Database:        "db1",
		VBucket:         "vbucket1",
		Status:          serverpb.RegionStatus_LEADER,
		MasterHost:      nil,
		NexusClusterUrl: nil,
	}
	dkvCli.UpdateStatus(regionInfo)

	masterHost := "host1:port"
	regionInfo.MasterHost = &masterHost
	regionInfo.Status = serverpb.RegionStatus_ACTIVE_SLAVE
	regionInfo.NodeAddress = "host2:port"
	dkvCli.UpdateStatus(regionInfo)

	regionInfo.Status = serverpb.RegionStatus_INACTIVE
	regionInfo.NodeAddress = "host3:port"
	dkvCli.UpdateStatus(regionInfo)

	regionInfo.Status = serverpb.RegionStatus_ACTIVE_SLAVE
	regionInfo.Database = "db2"
	regionInfo.NodeAddress = "host4:port"
	dkvCli.UpdateStatus(regionInfo)

	regionInfo.Status = serverpb.RegionStatus_ACTIVE_SLAVE
	regionInfo.Database = "db1"
	regionInfo.VBucket = "vbucket2"
	regionInfo.NodeAddress = "host5:port"
	dkvCli.UpdateStatus(regionInfo)

	regionInfo.Status = serverpb.RegionStatus_ACTIVE_SLAVE
	regionInfo.DcID = "dc2"
	regionInfo.NodeAddress = "host6:port"
	dkvCli.UpdateStatus(regionInfo)

	// Test status entry
	regionInfos, _ := dkvCli.GetClusterInfo("dc2", "", "")
	response := regionInfos[0]
	if response.Database != "db1" || response.DcID != "dc2" || response.VBucket != "vbucket2" ||
		response.Status != serverpb.RegionStatus_ACTIVE_SLAVE || response.NodeAddress != "host6:port" ||
		*response.MasterHost != masterHost || response.NexusClusterUrl != nil {
		t.Errorf("GET Cluster Info mismatch. Criteria: Exact response match")
	}

	// Test various filter conditions
	regionInfos, _ = dkvCli.GetClusterInfo("", "", "")
	if len(regionInfos) != 5 {
		t.Errorf("GET Cluster Info mismatch. Criteria: %s, Expected Value: %d, Actual Value: %d", "All", 5, len(regionInfos))
	}

	regionInfos, _ = dkvCli.GetClusterInfo("dc1", "", "")
	if len(regionInfos) != 4 {
		t.Errorf("GET Cluster Info mismatch. Criteria: %s, Expected Value: %d, Actual Value: %d", "DC 1", 4, len(regionInfos))
	}

	regionInfos, _ = dkvCli.GetClusterInfo("dc2", "", "")
	if len(regionInfos) != 1 {
		t.Errorf("GET Cluster Info mismatch. Criteria: %s, Expected Value: %d, Actual Value: %d", "DC 2", 1, len(regionInfos))
	}

	regionInfos, _ = dkvCli.GetClusterInfo("dc1", "db1", "")
	if len(regionInfos) != 3 {
		t.Errorf("GET Cluster Info mismatch. Criteria: %s, Expected Value: %d, Actual Value: %d", "DC and DB", 3, len(regionInfos))
	}

	regionInfos, _ = dkvCli.GetClusterInfo("", "db2", "")
	if len(regionInfos) != 1 {
		t.Errorf("GET Cluster Info mismatch. Criteria: %s, Expected Value: %d, Actual Value: %d", "DB 2", 1, len(regionInfos))
	}

	regionInfos, _ = dkvCli.GetClusterInfo("dc1", "db1", "vbucket1")
	if len(regionInfos) != 2 {
		t.Errorf("GET Cluster Info mismatch. Criteria: %s, Expected Value: %d, Actual Value: %d", "DC and DB and VBucket", 2, len(regionInfos))
	}

	regionInfos, _ = dkvCli.GetClusterInfo("", "db1", "vbucket2")
	if len(regionInfos) != 2 {
		t.Errorf("GET Cluster Info mismatch. Criteria: %s, Expected Value: %d, Actual Value: %d", "DB and VBucket 2", 2, len(regionInfos))
	}

	// Test status update of a node to inactive
	regionInfo.Status = serverpb.RegionStatus_INACTIVE
	dkvCli.UpdateStatus(regionInfo)
	regionInfos, _ = dkvCli.GetClusterInfo("", "db1", "vbucket2")
	if len(regionInfos) != 1 {
		t.Errorf("GET Cluster Info mismatch. Criteria: %s, Expected Value: %d, Actual Value: %d", "DB and VBucket 2", 2, len(regionInfos))
	}

	// Test incorrect arguments
	regionInfos, _ = dkvCli.GetClusterInfo("dc1", "db1", "vbucket3")
	if len(regionInfos) != 0 {
		t.Errorf("GET Cluster Info mismatch. Criteria: %s, Expected Value: %d, Actual Value: %d", "Incorrect args", 0, len(regionInfos))
	}

	// Test regions are marked inactive after heartbeat interval expired
	time.Sleep((heartBeatTimeOut + 1) * time.Second)
	regionInfos, _ = dkvCli.GetClusterInfo("", "", "")
	if len(regionInfos) != 0 {
		t.Errorf("GET Cluster Info mismatch. Criteria: %s, Expected Value: %d, Actual Value: %d", "All expired", 0, len(regionInfos))
	}

	// Test keys are purged after status TTL expired
	getResponse, _ := dkvCli.Get(serverpb.ReadConsistency_SEQUENTIAL, []byte("db1:vbucket1:host1:port"))
	if getResponse.Value != nil {
		time.Sleep((heartBeatTimeOut + 1) * time.Second)
		getResponse, _ = dkvCli.Get(serverpb.ReadConsistency_SEQUENTIAL, []byte("db1:vbucket1:host1:port"))
		if getResponse.Value != nil {
			t.Errorf("Key not expired")
		}
	} else {
		t.Errorf("Key not found")
	}
}

func serveStandaloneDKVWithDiscovery(port int, info *serverpb.RegionInfo, dbFolder string) (master.DKVService, *grpc.Server) {
	kvs, cp, ba := newKVStore(dbFolder)
	dkvSvc := master.NewStandaloneService(kvs, cp, ba, info, serveropts)
	grpcSrvr := grpc.NewServer()
	serverpb.RegisterDKVServer(grpcSrvr, dkvSvc)
	serverpb.RegisterDKVReplicationServer(grpcSrvr, dkvSvc)
	serverpb.RegisterDKVBackupRestoreServer(grpcSrvr, dkvSvc)

	discoverServiceConf := &opts.DiscoveryServerConfig{StatusTTl: statusTtl, HeartbeatTimeout: heartBeatTimeOut}
	discoveryService, _ := NewDiscoveryService(dkvSvc, zap.NewNop(), discoverServiceConf)
	serverpb.RegisterDKVDiscoveryServer(grpcSrvr, discoveryService)

	go listenAndServe(grpcSrvr, port)
	return dkvSvc, grpcSrvr
}

func listenAndServe(grpcSrvr *grpc.Server, port int) {
	if lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port)); err != nil {
		panic(fmt.Sprintf("failed to listen: %v", err))
	} else {
		grpcSrvr.Serve(lis)
	}
}

func newKVStore(dbDir string) (storage.KVStore, storage.ChangePropagator, storage.Backupable) {
	if err := exec.Command("rm", "-rf", dbDir).Run(); err != nil {
		panic(err)
	}
	switch engine {
	case "rocksdb":
		rocksDb, err := rocksdb.OpenDB(dbDir,
			rocksdb.WithSyncWrites(), rocksdb.WithCacheSize(cacheSize))
		if err != nil {
			panic(err)
		}
		return rocksDb, rocksDb, rocksDb
	case "badger":
		bdgrDb, err := badger.OpenDB(badger.WithSyncWrites(), badger.WithDBDir(dbDir))
		if err != nil {
			panic(err)
		}
		return bdgrDb, nil, bdgrDb
	default:
		panic(fmt.Sprintf("Unknown storage engine: %s", engine))
	}
}
