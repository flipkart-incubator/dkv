package discovery

/*
This class contains the behaviour of propagating a nodes status updates to discovery server
 */

import (
	"context"
	"fmt"
	"github.com/flipkart-incubator/dkv/internal/hlc"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"gopkg.in/ini.v1"
	"strconv"
	"time"
)

type DiscoveryClientConfig struct {
	discoveryServiceAddr 	string
	// time in seconds to push status updates to discovery server
	pushStatusInterval	 	time.Duration
	// time in seconds to poll cluster info from discovery server
	pollClusterInfoInterval	time.Duration
}

func NewDiscoveryClientConfigFromIni(sect *ini.Section) *DiscoveryClientConfig {
	sectConf := sect.KeysHash()
	if discoveryServiceAddr, ok := sectConf["discoveryServiceAddr"]; ok {
		if pushStatusInterval, err := strconv.Atoi(sectConf["pushStatusInterval"]); err == nil {
			if pollClusterInfoInterval, err := strconv.Atoi(sectConf["pollClusterInfoInterval"]); err == nil {
				return NewDiscoveryClientConfig(discoveryServiceAddr, pushStatusInterval, pollClusterInfoInterval)
			}
		}
	}
	panic(fmt.Errorf("Invalid discovery client configuration. Check section %s", sect.Name()))
}

func NewDiscoveryClientConfig(discoveryServiceAddr string, pushStatusInterval int, pollClusterInfoInterval int) *DiscoveryClientConfig {
	return &DiscoveryClientConfig{discoveryServiceAddr,
		time.Duration(pushStatusInterval), time.Duration(pollClusterInfoInterval)}
}

type discoveryClient struct {
	// All the regions hosted in the current node
	regions               []serverpb.DKVServer
	dkvClient             serverpb.DKVDiscoveryClient
	conn                  *grpc.ClientConn
	logger                *zap.Logger
	statusUpdateTicker    *time.Ticker
	pollClusterInfoTicker *time.Ticker
	config                *DiscoveryClientConfig
	stopChannel           chan struct{}
	// latest info of all regions in the cluster
	clusterInfo           []*serverpb.RegionInfo
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

func NewDiscoveryClient(config *DiscoveryClientConfig, logger *zap.Logger) (StatusPropagator, ClusterInfoGetter, error) {
	conn, err := getDiscoveryClient(config.discoveryServiceAddr)
	if (err != nil) {
		logger.Error("Unable to create DKV client to connect to discovery server", zap.Error(err))
		return nil, nil, err
	}

	dkvCli := serverpb.NewDKVDiscoveryClient(conn)
	storePropagator := &discoveryClient{regions: []serverpb.DKVServer{},
		dkvClient: dkvCli, logger: logger, config: config, conn: conn}
	storePropagator.init()
	return storePropagator, storePropagator, nil
}

func getDiscoveryClient(discoveryServiceAddr string) (*grpc.ClientConn, error) {
	// TODO - check if authority is required
	// TODO - create dkvctl with pool of server addresses so that if one fails, it can connect to the other
	ctx, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()
	return grpc.DialContext(ctx, discoveryServiceAddr,
		grpc.WithInsecure(),
		grpc.WithBlock(),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSize)),
		grpc.WithReadBufferSize(readBufSize),
		grpc.WithWriteBufferSize(writeBufSize),
		grpc.WithAuthority(""))
}

func (m *discoveryClient) RegisterRegion(server serverpb.DKVServer) {
	m.regions = append(m.regions, server)
}

func (m *discoveryClient) init() {
	m.statusUpdateTicker = time.NewTicker(time.Second * m.config.pushStatusInterval)
	m.pollClusterInfoTicker = time.NewTicker(time.Second * m.config.pollClusterInfoInterval)
	m.stopChannel = make(chan struct{})
	go m.tick()
}

func (m *discoveryClient) tick() {
	for {
		select {
		case <-m.statusUpdateTicker.C:
			m.propagateStatus()
		case <-m.pollClusterInfoTicker.C:
			m.pollClusterInfo()
		case <-m.stopChannel:
			break
		}
	}
}

func (m *discoveryClient) propagateStatus() {
	for _, server := range m.regions {
		regionInfo, _ := server.GetStatus(nil, nil)
		request := serverpb.UpdateStatusRequest{
			RegionInfo: regionInfo,
			Timestamp:   hlc.UnixNow(),
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		_, err := m.dkvClient.UpdateStatus(ctx, &request)
		if (err != nil) {
			m.logger.Error("Unable to propagate status update request", zap.Error(err));
		}
	}
}

func (m *discoveryClient) Close() error {
	m.logger.Info("Closing the discovery client")
	m.stopChannel <- struct{}{}
	m.statusUpdateTicker.Stop()
	m.propagateStatus()
	return m.conn.Close()
}

func (m *discoveryClient) pollClusterInfo() {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	response, err := m.dkvClient.GetClusterInfo(ctx, &serverpb.GetClusterInfoRequest{})
	if (err != nil) {
		m.logger.Error("Unable to poll cluster info", zap.Error(err))
	} else {
		m.clusterInfo = response.GetRegionInfos()
	}
}

func (m *discoveryClient) GetClusterStatus(database string, vBucket string) ([]*serverpb.RegionInfo, error) {
	if (m.clusterInfo == nil) {
		// When called before cluster info is initialised
		m.pollClusterInfo()
		if (m.clusterInfo == nil) {
			// in case cluster info initialise fails
			return nil, fmt.Errorf("Unable to get cluster info")
		}
	}
	regions := []*serverpb.RegionInfo{}
	for _, region := range m.clusterInfo {
		if (region.Database == database && region.VBucket == vBucket) {
			regions = append(regions, region)
		}
	}
	return regions, nil
}