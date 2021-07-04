package discovery

import (
    "fmt"
    "github.com/flipkart-incubator/dkv/internal/master"
    "github.com/flipkart-incubator/dkv/internal/utils"
    "github.com/flipkart-incubator/dkv/pkg/serverpb"
    "go.uber.org/zap"
    "testing"
)

func TestDiscoveryClient(t *testing.T) {
    _, _ = setupDiscoveryServer()

    clientConfig := NewDiscoveryClientConfig(dkvSvcHost + ":" + fmt.Sprint(dkvSvcPort), 5, 5)
    statusPropagator, clusterInfoGetter, _ := NewDiscoveryClient(clientConfig, zap.NewNop())
    defer statusPropagator.Close()

    // stop the poller so as to avoid race with these poller
    // and explicit call to required methods later
    statusPropagator.(*discoveryClient).statusUpdateTicker.Stop()
    statusPropagator.(*discoveryClient).pollClusterInfoTicker.Stop()
    statusPropagator.(*discoveryClient).stopChannel <- struct{}{}
    utils.SleepInSecs(2)

    regionInfo1 := serverpb.RegionInfo{
        DcID:            "dc1",
        NodeAddress:     "host1:port",
        Database:        "db1",
        VBucket:         "vbucket1",
        Status:          0,
        MasterHost:      nil,
        NexusClusterUrl: nil,
    }
    dkvSvc1, grpcSvc1 := master.ServeStandaloneDKV(&regionInfo1, dbFolder + "_1")
    defer dkvSvc1.Close()
    defer grpcSvc1.GracefulStop()

    statusPropagator.RegisterRegion(dkvSvc1)

    regionInfo2 := serverpb.RegionInfo{
        DcID:            "dc1",
        NodeAddress:     "host2:port",
        Database:        "db1",
        VBucket:         "vbucket1",
        Status:          0,
        MasterHost:      nil,
        NexusClusterUrl: nil,
    }
    dkvSvc2, grpcSvc2 := master.ServeStandaloneDKV(&regionInfo2, dbFolder + "_2")
    defer dkvSvc2.Close()
    defer grpcSvc2.GracefulStop()
    statusPropagator.RegisterRegion(dkvSvc2)

    regionInfo3 := serverpb.RegionInfo{
        DcID:            "dc1",
        NodeAddress:     "host3:port",
        Database:        "db1",
        VBucket:         "vbucket2",
        Status:          0,
        MasterHost:      nil,
        NexusClusterUrl: nil,
    }
    dkvSvc3, grpcSvc3 := master.ServeStandaloneDKV(&regionInfo3, dbFolder + "_3")
    defer dkvSvc3.Close()
    defer grpcSvc3.GracefulStop()
    statusPropagator.RegisterRegion(dkvSvc3)

    statusPropagator.propagateStatus()

    regionInfos, _ := clusterInfoGetter.GetClusterStatus("db1", "vbucket1")
    if (len(regionInfos) != 2) {
        t.Errorf("GET Cluster Status Mismatch. Criteria: %s, Expected Value: %d, Actual Value: %d", "DB1 vBucket1", 2, len(regionInfos))
    }

    regionInfos, _ = clusterInfoGetter.GetClusterStatus("db1", "vbucket2")
    if (len(regionInfos) != 1) {
        t.Errorf("GET Cluster Status Mismatch. Criteria: %s, Expected Value: %d, Actual Value: %d", "DB1 vBucket2", 1, len(regionInfos))
    }

    regionInfos, _ = clusterInfoGetter.GetClusterStatus("", "vbucket2")
    if (len(regionInfos) != 0) {
        t.Errorf("GET Cluster Status Mismatch. Criteria: %s, Expected Value: %d, Actual Value: %d", "No database", 0, len(regionInfos))
    }

    regionInfos, _ = clusterInfoGetter.GetClusterStatus("db1", "")
    if (len(regionInfos) != 0) {
        t.Errorf("GET Cluster Status Mismatch. Criteria: %s, Expected Value: %d, Actual Value: %d", "No vBucket", 0, len(regionInfos))
    }

    regionInfos, _ = clusterInfoGetter.GetClusterStatus("db1", "vbucket3")
    if (len(regionInfos) != 0) {
        t.Errorf("GET Cluster Status Mismatch. Criteria: %s, Expected Value: %d, Actual Value: %d", "DB1 vBucket3", 0, len(regionInfos))
    }
}
