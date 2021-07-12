package discovery

import (
	"fmt"
	"github.com/flipkart-incubator/dkv/internal/master"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
	"go.uber.org/zap"
	"testing"
	"time"
)

const (
	discoverySvcPort = 8070
)


func TestDiscoveryClient(t *testing.T) {
	_, _ = setupDiscoveryServer(dbFolder + "_DC", discoverySvcPort)

	clientConfig := &DiscoveryClientConfig{DiscoveryServiceAddr: fmt.Sprintf("%s:%d", dkvSvcHost, discoverySvcPort),
		PushStatusInterval: time.Duration(5), PollClusterInfoInterval: time.Duration(5)}

	dClient, _ := NewDiscoveryClient(clientConfig, zap.NewNop())
	defer dClient.Close()

	// stop the poller so as to avoid race with these poller
	// and explicit call to required methods later
	dClient.(*discoveryClient).statusUpdateTicker.Stop()
	dClient.(*discoveryClient).pollClusterInfoTicker.Stop()
	dClient.(*discoveryClient).stopChannel <- struct{}{}

	regionInfo1 := serverpb.RegionInfo{
		DcID:            "dc1",
		NodeAddress:     "host1:port",
		Database:        "db1",
		VBucket:         "vbucket1",
		Status:          0,
		MasterHost:      nil,
		NexusClusterUrl: nil,
	}
	dkvSvc1, grpcSvc1 := master.ServeStandaloneDKVWithID(&regionInfo1, dbFolder, 1)
	defer dkvSvc1.Close()
	defer grpcSvc1.GracefulStop()

	dClient.RegisterRegion(dkvSvc1)

	regionInfo2 := serverpb.RegionInfo{
		DcID:            "dc1",
		NodeAddress:     "host2:port",
		Database:        "db1",
		VBucket:         "vbucket1",
		Status:          0,
		MasterHost:      nil,
		NexusClusterUrl: nil,
	}
	dkvSvc2, grpcSvc2 := master.ServeStandaloneDKVWithID(&regionInfo2, dbFolder, 2)
	defer dkvSvc2.Close()
	defer grpcSvc2.GracefulStop()
	dClient.RegisterRegion(dkvSvc2)

	regionInfo3 := serverpb.RegionInfo{
		DcID:            "dc1",
		NodeAddress:     "host3:port",
		Database:        "db1",
		VBucket:         "vbucket2",
		Status:          0,
		MasterHost:      nil,
		NexusClusterUrl: nil,
	}
	dkvSvc3, grpcSvc3 := master.ServeStandaloneDKVWithID(&regionInfo3, dbFolder, 3)
	defer dkvSvc3.Close()
	defer grpcSvc3.GracefulStop()
	dClient.RegisterRegion(dkvSvc3)

	dClient.propagateStatus()

	regionInfos, _ := dClient.GetClusterStatus("db1", "vbucket1")
	if len(regionInfos) != 2 {
		t.Errorf("GET Cluster Status Mismatch. Criteria: %s, Expected Value: %d, Actual Value: %d", "DB1 vBucket1", 2, len(regionInfos))
	}

	regionInfos, _ = dClient.GetClusterStatus("db1", "vbucket2")
	if len(regionInfos) != 1 {
		t.Errorf("GET Cluster Status Mismatch. Criteria: %s, Expected Value: %d, Actual Value: %d", "DB1 vBucket2", 1, len(regionInfos))
	}

	regionInfos, _ = dClient.GetClusterStatus("", "vbucket2")
	if len(regionInfos) != 0 {
		t.Errorf("GET Cluster Status Mismatch. Criteria: %s, Expected Value: %d, Actual Value: %d", "No database", 0, len(regionInfos))
	}

	regionInfos, _ = dClient.GetClusterStatus("db1", "")
	if len(regionInfos) != 0 {
		t.Errorf("GET Cluster Status Mismatch. Criteria: %s, Expected Value: %d, Actual Value: %d", "No vBucket", 0, len(regionInfos))
	}

	regionInfos, _ = dClient.GetClusterStatus("db1", "vbucket3")
	if len(regionInfos) != 0 {
		t.Errorf("GET Cluster Status Mismatch. Criteria: %s, Expected Value: %d, Actual Value: %d", "DB1 vBucket3", 0, len(regionInfos))
	}
}
