package discovery

/*
This class contains the behaviour of propagating a nodes status updates to discovery server
*/

import (
	"context"
	"github.com/flipkart-incubator/dkv/internal/opts"
	"time"

	_ "github.com/Jille/grpc-multi-resolver"
	"github.com/flipkart-incubator/dkv/internal/hlc"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type discoveryClient struct {
	// All the regions hosted in the current node
	regions               []serverpb.DKVDiscoveryNodeServer
	dkvClient             serverpb.DKVDiscoveryClient
	conn                  *grpc.ClientConn
	logger                *zap.Logger
	statusUpdateTicker    *time.Ticker
	pollClusterInfoTicker *time.Ticker
	config                *opts.DiscoveryClientConfig
	stopChannel           chan struct{}
	// latest info of all regions in the cluster
	clusterInfo []*serverpb.RegionInfo
}

// Config describing parameters to connect to discovery server
// Hardcoded because no use case to customise this. Can be made configurable if required later
const (
	readBufSize    = 10 << 20
	writeBufSize   = 10 << 20
	maxMsgSize     = 50 << 20
	timeout        = 1 * time.Second
	connectTimeout = 10 * time.Second
)

func NewDiscoveryClient(config *opts.DiscoveryClientConfig, logger *zap.Logger) (Client, error) {
	conn, err := getDiscoveryClient(config.DiscoveryServiceAddr)
	if err != nil {
		logger.Error("Unable to create DKV client to connect to discovery server", zap.Error(err))
		return nil, err
	}

	dkvCli := serverpb.NewDKVDiscoveryClient(conn)
	storePropagator := &discoveryClient{regions: []serverpb.DKVDiscoveryNodeServer{},
		dkvClient: dkvCli, logger: logger, config: config, conn: conn}
	storePropagator.init()
	return storePropagator, nil
}

func getDiscoveryClient(discoveryServiceAddr string) (*grpc.ClientConn, error) {
	// TODO - check if authority is required
	ctx, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()
	return grpc.DialContext(ctx, discoveryServiceAddr,
		grpc.WithInsecure(),
		grpc.WithBlock(),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSize)),
		grpc.WithReadBufferSize(readBufSize),
		grpc.WithWriteBufferSize(writeBufSize),
		grpc.WithAuthority(""),
		grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"round_robin"}`))
}

func (m *discoveryClient) RegisterRegion(server serverpb.DKVDiscoveryNodeServer) {
	m.regions = append(m.regions, server)
}

func (m *discoveryClient) init() {
	m.statusUpdateTicker = time.NewTicker(time.Second * m.config.PushStatusInterval)
	m.pollClusterInfoTicker = time.NewTicker(time.Second * m.config.PollClusterInfoInterval)
	m.stopChannel = make(chan struct{})
	go m.tick()
}

func (m *discoveryClient) tick() {
	for {
		select {
		case <-m.statusUpdateTicker.C:
			m.PropagateStatus()
		case <-m.pollClusterInfoTicker.C:
			m.pollClusterInfo()
		case <-m.stopChannel:
			break
		}
	}
}

func (m *discoveryClient) PropagateStatus() {
	for _, server := range m.regions {
		regionInfo, _ := server.GetStatus(nil, nil)
		request := serverpb.UpdateStatusRequest{
			RegionInfo: regionInfo,
			Timestamp:  hlc.UnixNow(),
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		_, err := m.dkvClient.UpdateStatus(ctx, &request)
		if err != nil {
			m.logger.Error("Unable to propagate status update request", zap.Error(err))
		}
		cancel()
	}
}

func (m *discoveryClient) Close() error {
	m.logger.Info("Closing the discovery client")
	m.stopChannel <- struct{}{}
	m.statusUpdateTicker.Stop()
	m.PropagateStatus()
	return m.conn.Close()
}

func (m *discoveryClient) pollClusterInfo() error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	response, err := m.dkvClient.GetClusterInfo(ctx, &serverpb.GetClusterInfoRequest{})
	if err != nil {
		m.logger.Error("Unable to poll cluster info", zap.Error(err))
		return err
	} else {
		// This can be set to nil if its empty array as protobuf optimises it this way
		m.clusterInfo = response.GetRegionInfos()
		return nil
	}
}

// gets cluster info for the provided database and vBucket
// database and vBucket can be empty strings, in which case, the entire cluster set is returned
func (m *discoveryClient) GetClusterStatus(database string, vBucket string) ([]*serverpb.RegionInfo, error) {
	if m.clusterInfo == nil {
		// When called before cluster info is initialised
		err := m.pollClusterInfo()
		if err != nil {
			return nil, err
		}
	}
	var regions []*serverpb.RegionInfo
	for _, region := range m.clusterInfo {
		if region.Database == database || database == "" {
			if region.VBucket == vBucket || vBucket == "" {
				regions = append(regions, region)
			}
		}
	}
	return regions, nil
}
