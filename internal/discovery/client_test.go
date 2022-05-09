package discovery

import (
	"fmt"
	"testing"
	"time"

	"github.com/flipkart-incubator/dkv/internal/master"
	"github.com/flipkart-incubator/dkv/internal/opts"
	"github.com/flipkart-incubator/dkv/internal/stats"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
	"go.uber.org/zap"
)

const (
	discoverySvcPort        = 8070
	pollClusterInfoInterval = time.Duration(5)
	pushStatusInterval      = time.Duration(5)
)

var (
	lgr, _     = zap.NewDevelopment()
	serveropts = &opts.ServerOpts{
		Logger:                    lgr,
		HealthCheckTickerInterval: opts.DefaultHealthCheckTickterInterval,
		StatsCli:                  stats.NewNoOpClient(),
		PrometheusRegistry:        stats.NewPromethousNoopRegistry(),
	}
)

func TestDiscoveryClient(t *testing.T) {
	dkvSvc, grpcSrvr := serveStandaloneDKVWithDiscovery(discoverySvcPort, &serverpb.RegionInfo{}, dbFolder+"_DC")
	defer dkvSvc.Close()
	defer grpcSrvr.GracefulStop()
	<-time.After(time.Duration(10) * time.Second)

	discoveryClientConfig := opts.DiscoveryClientConfig{
		DiscoveryServiceAddr:    fmt.Sprintf("%s:%d", dkvSvcHost, discoverySvcPort),
		PushStatusInterval:      pushStatusInterval,
		PollClusterInfoInterval: pollClusterInfoInterval,
	}

	dClient, _ := NewDiscoveryClient(&discoveryClientConfig, zap.NewNop())
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

	dkvSvc1 := newStandaloneDKVWithID(&regionInfo1, dbFolder, 1)
	defer dkvSvc1.Close()

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
	dkvSvc2 := newStandaloneDKVWithID(&regionInfo2, dbFolder, 2)
	defer dkvSvc2.Close()
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
	dkvSvc3 := newStandaloneDKVWithID(&regionInfo3, dbFolder, 3)
	defer dkvSvc3.Close()
	dClient.RegisterRegion(dkvSvc3)

	dClient.PropagateStatus()

	regionInfos, err := dClient.GetClusterStatus("db1", "vbucket1")
	if err != nil {
		t.Errorf(err.Error())
	}
	if len(regionInfos) != 2 {
		t.Errorf("GET Cluster Status Mismatch. Criteria: %s, Expected Value: %d, Actual Value: %d", "DB1 vBucket1", 2, len(regionInfos))
	}

	regionInfos, _ = dClient.GetClusterStatus("db1", "vbucket2")
	if len(regionInfos) != 1 {
		t.Errorf("GET Cluster Status Mismatch. Criteria: %s, Expected Value: %d, Actual Value: %d", "DB1 vBucket2", 1, len(regionInfos))
	}

	regionInfos, _ = dClient.GetClusterStatus("", "vbucket2")
	if len(regionInfos) != 1 {
		t.Errorf("GET Cluster Status Mismatch. Criteria: %s, Expected Value: %d, Actual Value: %d", "No database", 1, len(regionInfos))
	}

	regionInfos, _ = dClient.GetClusterStatus("db1", "")
	if len(regionInfos) != 3 {
		t.Errorf("GET Cluster Status Mismatch. Criteria: %s, Expected Value: %d, Actual Value: %d", "No vBucket", 3, len(regionInfos))
	}

	regionInfos, _ = dClient.GetClusterStatus("db1", "vbucket3")
	if len(regionInfos) != 0 {
		t.Errorf("GET Cluster Status Mismatch. Criteria: %s, Expected Value: %d, Actual Value: %d", "DB1 vBucket3", 0, len(regionInfos))
	}
}

func newStandaloneDKVWithID(info *serverpb.RegionInfo, dbFolder string, id int) master.DKVService {
	dbDir := fmt.Sprintf("%s_%d", dbFolder, id)
	kvs, cp, ba := newKVStore(dbDir)
	dkvSvc := master.NewStandaloneService(kvs, cp, ba, info, serveropts)
	return dkvSvc
}
