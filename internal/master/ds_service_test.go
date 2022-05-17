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

	"github.com/flipkart-incubator/dkv/pkg/health"

	"github.com/flipkart-incubator/dkv/internal/storage"
	dkv_sync "github.com/flipkart-incubator/dkv/internal/sync"
	"github.com/flipkart-incubator/dkv/pkg/ctl"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
	nexus_api "github.com/flipkart-incubator/nexus/pkg/api"
	nexus "github.com/flipkart-incubator/nexus/pkg/raft"
	"google.golang.org/grpc"
)

const (
	clusterSize           = 3
	logDir                = "/tmp/dkv_test/logs"
	snapDir               = "/tmp/dkv_test/snap"
	clusterURL            = "http://127.0.0.1:9321,http://127.0.0.1:9322,http://127.0.0.1:9323"
	clusterStreamingURL   = "http://127.0.0.1:9321,http://127.0.0.1:9322,http://127.0.0.1:9323"
	replTimeout           = 3 * time.Second
	newNodeID             = 4
	newNodeURL            = "http://127.0.0.1:9324"
	extraNodeURL          = "http://127.0.0.1:9325"
	extraStreamingNodeURL = "http://127.0.0.1:9325"
)

var (
	grpcSrvs           = make(map[int]*grpc.Server)
	dkvPorts           = map[int]int{1: 9081, 2: 9082, 3: 9083, 4: 9084}
	dkvClis            = make(map[int]*ctl.DKVClient)
	dkvSvcs            = make(map[int]DKVService)
	dkvUrlToServiceMap = make(map[string]DKVService)

	grpcStreamingSrvs           = make(map[int]*grpc.Server)
	dkvStreamingPorts           = map[int]int{1: 9085, 2: 9086, 3: 9087, 4: 9088}
	dkvStreamingClis            = make(map[int]*ctl.DKVClient)
	dkvStreamingSvcs            = make(map[int]DKVService)
	dkvStreamingUrlToServiceMap = make(map[string]DKVService)
	mutex                       = sync.Mutex{}
	rc                          = serverpb.ReadConsistency_SEQUENTIAL
	leaderDkvSvc                DKVService
	leaderStreamingDkvSvc       DKVService
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
	t.Run("testGetStatus", testGetStatus)
	t.Run("testNewDKVNodeJoiningAndLeaving", testNewDKVNodeJoiningAndLeaving)
}

func TestHealthCheckUnary(t *testing.T) {
	resetRaftStateDirs(t)
	initDKVServers()
	sleepInSecs(3)
	initDKVClients()
	defer stopClients()

	t.Run("testHealthCheckUnary", testHealthCheckUnary)

	for id := 1; id <= clusterSize; id++ {
		//don't close the leader again
		if dkvSvcs[id] == leaderDkvSvc {
			continue
		}
		dkvSvcs[id].Close()
		grpcSrvs[id].GracefulStop()
	}
}

func TestHealthCheckStreaming(t *testing.T) {
	resetRaftStateDirs(t)
	initStreamingDKVServers()
	sleepInSecs(3)
	initStreamingDKVClients()
	defer stopStreamingClients()

	t.Run("testHealthCheckStreaming", testHealthCheckStreaming)

	for id := 1; id <= clusterSize; id++ {
		//don't close the leader again
		if dkvStreamingSvcs[id] == leaderStreamingDkvSvc {
			continue
		}
		dkvStreamingSvcs[id].Close()
		grpcStreamingSrvs[id].GracefulStop()
	}
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

func testGetStatus(t *testing.T) {
	leaderCount := 0
	followerCount := 0
	for _, dkv := range dkvSvcs {
		info, _ := dkv.GetStatus(nil, nil)
		if info.Status == serverpb.RegionStatus_LEADER {
			leaderCount++
		} else if info.Status == serverpb.RegionStatus_PRIMARY_FOLLOWER {
			followerCount++
		} else {
			t.Errorf("Incorrect node status. Actual %s", info.Status.String())
		}
	}
	if leaderCount != 1 {
		t.Errorf("Incorrect leader counts . Expected %d, Actual %d", 1, leaderCount)
	}
	expectedFollowerCount := len(dkvSvcs) - 1
	if followerCount != expectedFollowerCount {
		t.Errorf("Incorrect follower counts . Expected %d, Actual %d", expectedFollowerCount, followerCount)
	}
}

func testHealthCheckUnary(t *testing.T) {
	dir := fmt.Sprintf("%s_%d", dbFolder, newNodeID)
	kvs, cp, br := newKVStore(dir)
	// Create and start the new DKV node
	dkvRepl := newReplicator(kvs, extraNodeURL, clusterURL)
	dkvSvc, grpcSrv := newDistributedDKVNodeWithRepl(newNodeID, extraNodeURL, clusterURL, dkvRepl, kvs, cp, br)
	go grpcSrv.Serve(newListener(dkvPorts[newNodeID]))
	if err := dkvClis[1].AddNode(extraNodeURL); err != nil {
		t.Fatal(err)
	}
	<-time.After(3 * time.Second)

	healthCheckResponseNewNode, _ := dkvSvc.Check(nil, nil)
	if healthCheckResponseNewNode.Status != health.HealthCheckResponse_SERVING {
		t.Errorf("Incorrect health check status. Expected %s, Actual %s", health.HealthCheckResponse_SERVING.String(), healthCheckResponseNewNode.Status.String())
	}

	// Now remove the leader from the cluster. Since the this node was attached later, it won't be leader at first
	// But after re-election it can be become a leader or follower. But the health check should still return serving
	// if the re-election was completed successfully
	leader, members := dkvRepl.ListMembers()
	leaderDkvSvc = dkvUrlToServiceMap[members[leader].NodeUrl]
	err := leaderDkvSvc.Close()
	if err != nil {
		t.Fatalf("Error while closing leader service. Error: %v", err)
	}
	//Let the re-relection get completed
	<-time.After(10 * time.Second)

	healthCheckResponseAfterReelection, _ := dkvSvc.Check(nil, nil)
	if healthCheckResponseAfterReelection.Status != health.HealthCheckResponse_SERVING {
		t.Errorf("Incorrect node status. Expected %s, Actual %s", health.HealthCheckResponse_SERVING.String(), healthCheckResponseAfterReelection.Status.String())
	}

	grpcSrv.GracefulStop()
	dkvSvc.Close()
	healthCheckResponseAfterShutdown, _ := dkvSvc.Check(nil, nil)
	if healthCheckResponseAfterShutdown.Status != health.HealthCheckResponse_NOT_SERVING {
		t.Errorf("Incorrect node status. Expected %s, Actual %s", health.HealthCheckResponse_NOT_SERVING.String(), healthCheckResponseAfterShutdown.Status.String())
	}
}

func testHealthCheckStreaming(t *testing.T) {
	// Create and start the new DKV node
	dir := fmt.Sprintf("%s_%d", dbFolder, newNodeID)
	kvs, cp, br := newKVStore(dir)
	// Create and start the new DKV node
	dkvRepl := newReplicator(kvs, extraStreamingNodeURL, clusterStreamingURL)
	dkvSvc, grpcSrv := newDistributedDKVNodeWithRepl(newNodeID, extraStreamingNodeURL, clusterStreamingURL, dkvRepl, kvs, cp, br)
	go grpcSrv.Serve(newListener(dkvStreamingPorts[newNodeID]))

	<-time.After(3 * time.Second)

	if err := dkvStreamingClis[1].AddNode(extraStreamingNodeURL); err != nil {
		t.Fatal(err)
	}
	<-time.After(3 * time.Second)

	healthCheckCli, err := newHealthCheckClient(dkvStreamingPorts[newNodeID])
	if err != nil {
		t.Fatalf("Error while creating health check client: %v", err)
	} else {
		healthCheckResponseWatcher, error := healthCheckCli.healthCheckCli.Watch(context.Background(), &health.HealthCheckRequest{})
		if error != nil {
			t.Errorf("Erroneous response from health check client: %v", error)
		}
		response, err := healthCheckResponseWatcher.Recv()
		if err != nil {
			t.Fatalf("Error while receiving response from Watcher: %v", err)
		}
		if response.Status != health.HealthCheckResponse_SERVING {
			t.Errorf("Incorrect node status. Expected %s, Actual %s", health.HealthCheckResponse_SERVING.String(), response.Status.String())
		}

		// Now remove the leader from the cluster. Since the this node was attached later, it won't be leader at first
		// But after re-election it can be become a leader or follower. But the health check should still return serving
		// if the re-election was completed successfully
		leader, members := dkvRepl.ListMembers()
		leaderStreamingDkvSvc = dkvStreamingUrlToServiceMap[members[leader].NodeUrl]
		er := leaderStreamingDkvSvc.Close()

		if er != nil {
			t.Fatalf("Error while closing leader service. Error: %v", err)
		}
		//Let the re-relection get completed
		<-time.After(10 * time.Second)

		healthCheckResponseAfterReelection, _ := healthCheckResponseWatcher.Recv()
		if healthCheckResponseAfterReelection.Status != health.HealthCheckResponse_SERVING {
			t.Errorf("Incorrect node status. Expected %s, Actual %s", health.HealthCheckResponse_SERVING.String(), healthCheckResponseAfterReelection.Status.String())
		}

		if err := dkvSvc.Close(); err != nil {
			t.Fatalf("Error while closing the dkv service. Error: %v", err)
		}

		healthCheckResponseAfterShutdown, _ := healthCheckResponseWatcher.Recv()
		if healthCheckResponseAfterShutdown.Status != health.HealthCheckResponse_NOT_SERVING {
			t.Errorf("Incorrect node status. Expected %s, Actual %s", health.HealthCheckResponse_NOT_SERVING.String(), healthCheckResponseAfterShutdown.Status.String())
		}
		grpcSrv.GracefulStop()
	}
	healthCheckCli.cliConn.Close()
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

func testNewDKVNodeJoiningAndLeaving(t *testing.T) {
	// Create and start the new DKV node
	dkvSvc, grpcSrv := newDistributedDKVNode(newNodeID, newNodeURL, clusterURL)
	go grpcSrv.Serve(newListener(dkvPorts[newNodeID]))
	<-time.After(3 * time.Second)

	// Add this new DKV node through the client of any other DKV node (1)
	if err := dkvClis[1].AddNode(newNodeURL); err != nil {
		t.Fatal(err)
	}
	<-time.After(3 * time.Second)

	// Create the client for the new DKV node
	svcAddr := fmt.Sprintf("%s:%d", dkvSvcHost, dkvPorts[newNodeID])
	if dkvCli, err := ctl.NewInSecureDKVClient(svcAddr, "", ctl.DefaultConnectOpts); err != nil {
		t.Fatal(err)
	} else {
		defer dkvCli.Close()
		// Expect to find all data in the new DKV node
		for i := 1; i <= clusterSize; i++ {
			key, expectedValue := fmt.Sprintf("K_CLI_%d", i), fmt.Sprintf("V_CLI_%d", i)
			if actualValue, err := dkvCli.Get(rc, []byte(key)); err != nil {
				t.Fatalf("Unable to GET for CLI ID: %d. Key: %s, Error: %v", i, key, err)
			} else if string(actualValue.Value) != expectedValue {
				t.Errorf("GET mismatch for CLI ID: %d. Key: %s, Expected Value: %s, Actual Value: %s", i, key, expectedValue, actualValue.Value)
			}
		}
		regionInfo, _ := dkvSvc.GetStatus(nil, nil)
		// Currently new region will be marked as follower
		if regionInfo.Status != serverpb.RegionStatus_PRIMARY_FOLLOWER {
			t.Errorf("Incorrect node status. Expected %s, Actual %s", serverpb.RegionStatus_PRIMARY_FOLLOWER.String(), regionInfo.Status.String())
		}
		// Remove new DKV node through the client of any other DKV Node (2)
		if err := dkvClis[2].RemoveNode(newNodeURL); err != nil {
			t.Fatal(err)
		}
		// TODO - node should now ideally be marked inactive but since status check is using ip address, can't test here
		<-time.After(3 * time.Second)

		grpcSrv.GracefulStop()
		dkvSvc.Close()
		regionInfo, _ = dkvSvc.GetStatus(nil, nil)
		if regionInfo.Status != serverpb.RegionStatus_INACTIVE {
			t.Errorf("Incorrect node status. Expected %s, Actual %s", serverpb.RegionStatus_INACTIVE.String(), regionInfo.Status.String())
		}
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

func initStreamingDKVClients(ids ...int) {
	for id := 1; id <= clusterSize; id++ {
		svcAddr := fmt.Sprintf("%s:%d", dkvSvcHost, dkvStreamingPorts[id])
		if client, err := ctl.NewInSecureDKVClient(svcAddr, "", ctl.DefaultConnectOpts); err != nil {
			panic(err)
		} else {
			dkvStreamingClis[id] = client
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

func initStreamingDKVServers(ids ...int) {
	clusURLs := strings.Split(clusterStreamingURL, ",")
	for id := 1; id <= clusterSize; id++ {
		go serveStreamingDistributedDKV(id, clusURLs[id-1])
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

func newDistributedDKVNode(id int, nodeURL, clusURL string) (DKVService, *grpc.Server) {
	dir := fmt.Sprintf("%s_%d", dbFolder, id)
	kvs, cp, br := newKVStore(dir)
	dkvRepl := newReplicator(kvs, nodeURL, clusURL)
	dkvRepl.Start()
	regionInfo := &serverpb.RegionInfo{}
	regionInfo.NodeAddress = "127.0.0.1" + ":" + fmt.Sprint(dkvPorts[id])
	distSrv := NewDistributedService(kvs, cp, br, dkvRepl, regionInfo, serverOpts)
	grpcSrv := grpc.NewServer()
	serverpb.RegisterDKVServer(grpcSrv, distSrv)
	serverpb.RegisterDKVClusterServer(grpcSrv, distSrv)
	health.RegisterHealthServer(grpcSrv, distSrv)
	return distSrv, grpcSrv
}

func newDistributedDKVNodeWithRepl(id int, nodeURL, clusURL string, dkvRepl nexus_api.RaftReplicator, kvs storage.KVStore, cp storage.ChangePropagator, br storage.Backupable) (DKVService, *grpc.Server) {
	dkvRepl.Start()
	regionInfo := &serverpb.RegionInfo{}
	regionInfo.NodeAddress = "127.0.0.1" + ":" + fmt.Sprint(dkvPorts[id])
	distSrv := NewDistributedService(kvs, cp, br, dkvRepl, regionInfo, serverOpts)
	grpcSrv := grpc.NewServer()
	serverpb.RegisterDKVServer(grpcSrv, distSrv)
	serverpb.RegisterDKVClusterServer(grpcSrv, distSrv)
	health.RegisterHealthServer(grpcSrv, distSrv)
	return distSrv, grpcSrv
}

func serveDistributedDKV(id int, nodeURL string) {
	dkvSvc, grpcSvc := newDistributedDKVNode(id, nodeURL, clusterURL)
	mutex.Lock()
	dkvSvcs[id] = dkvSvc
	dkvUrlToServiceMap[nodeURL] = dkvSvc
	grpcSrvs[id] = grpcSvc
	mutex.Unlock()
	grpcSrvs[id].Serve(newListener(dkvPorts[id]))
}

func serveStreamingDistributedDKV(id int, nodeURL string) {
	dkvSvc, grpcSvc := newDistributedDKVNode(id, nodeURL, clusterStreamingURL)
	mutex.Lock()
	dkvStreamingSvcs[id] = dkvSvc
	dkvStreamingUrlToServiceMap[nodeURL] = dkvSvc
	grpcStreamingSrvs[id] = grpcSvc
	mutex.Unlock()
	grpcStreamingSrvs[id].Serve(newListener(dkvStreamingPorts[id]))
}

func stopClients() {
	for id := 1; id <= clusterSize; id++ {
		dkvClis[id].Close()
	}
}

func stopStreamingClients() {
	for id := 1; id <= clusterSize; id++ {
		dkvStreamingClis[id].Close()
	}
}

func stopServers() {
	for id := 1; id <= clusterSize; id++ {
		dkvSvcs[id].Close()
		grpcSrvs[id].GracefulStop()
	}
}
