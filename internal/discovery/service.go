package discovery

import (
	"context"
	"fmt"
	"github.com/flipkart-incubator/dkv/internal/hlc"
	"github.com/flipkart-incubator/dkv/internal/master"
	"github.com/flipkart-incubator/dkv/internal/storage"
	"github.com/flipkart-incubator/dkv/pkg/ctl"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
	"gopkg.in/ini.v1"
	"io"
	"strconv"

	"encoding/json"
	"go.uber.org/zap"
)

/*
This class contains the behaviour of receiving status updates from nodes in the cluster
and providing the latest cluster info of active master / followers / slave of a region when requested
*/

type DiscoveryConfig struct {
	// time in seconds after which the status entry of the region & node combination can be purged
	StatusTTl uint64
	// maximum time in seconds for the last status update to be considered valid
	// after exceeding this time the region & node combination can be marked invalid
	HeartbeatTimeout uint64
}

func NewDiscoverConfigFromIni(sect *ini.Section) (*DiscoveryConfig, error) {
	sectConf := sect.KeysHash()
	if statusTTL, err := strconv.ParseUint(sectConf["statusTTL"], 10, 64); err == nil {
		if heartbeatTimeout, err := strconv.ParseUint(sectConf["heartbeatTimeout"], 10, 64); err == nil {
			return &DiscoveryConfig{statusTTL, heartbeatTimeout}, nil
		}
	}
	return nil, fmt.Errorf("Invalid discovery server configuration. Check section %s", sect.Name())
}

type discoverService struct {
	// TODO - dkvServer should point to interface of master (currently distributedService)
	dkvCli serverpb.DKVClient
	logger *zap.Logger
	config *DiscoveryConfig
}

// Create the dkv lookup key from region info
// Since granularity of an update is at a region per node, the same is used for creating a key
// As lookups will be mostly on database and possibly on vBucket (within a database) the same order is used for key
// This enables faster scans for database/vBucket
func createKeyToInsert(info *serverpb.RegionInfo) []byte {
	return []byte(info.GetDatabase() + ":" + info.GetVBucket() + ":" + info.GetNodeAddress())
}

// Create lookup key based on input request
// Using the sorted nature of keys, lookup by either database or by database and vBucket or all keys
func createKeyToGet(request *serverpb.GetClusterInfoRequest) []byte {
	if request.GetDatabase() != "" {
		if request.GetVBucket() != "" {
			return []byte(request.GetDatabase() + ":" + request.GetVBucket() + ":")
		} else {
			return []byte(request.GetDatabase() + ":")
		}
	} else {
		return []byte{}
	}
}

func (d *discoverService) UpdateStatus(ctx context.Context, request *serverpb.UpdateStatusRequest) (*serverpb.Status, error) {
	// Store the serialised form of the request
	val, err := json.Marshal(request)

	if err != nil {
		d.logger.Error("Unable to marshal status request", zap.Error(err))
		return newErrorStatus(err), err
	}

	// TODO - check if expiry TTL is working
	putRequest := serverpb.PutRequest{Key: createKeyToInsert(request.GetRegionInfo()), Value: val, ExpireTS: hlc.GetUnixTimeFromNow(d.config.StatusTTl)}
	if _, err = d.dkvCli.Put(ctx, &putRequest); err != nil {
		return newErrorStatus(err), err
	} else {
		return &serverpb.Status{Code: 0, Message: ""}, nil
	}
}

func (d *discoverService) GetClusterInfo(ctx context.Context, request *serverpb.GetClusterInfoRequest) (*serverpb.GetClusterInfoResponse, error) {
	// Create a prefix multi get request to loop through all required status updates
	// Iterator sends the responses on grpc stream which is unnecessary here
	iterReq := &serverpb.IterateRequest{KeyPrefix: createKeyToGet(request)}
	kvStrm, err := d.dkvCli.Iterate(context.Background(), iterReq)
	if err != nil {
		return nil, err
	}
	var clusterInfo []storage.KVEntry
	for {
		itRes, err := kvStrm.Recv()
		if err == io.EOF || itRes == nil {
			break
		} else {
			clusterInfo = append(clusterInfo, storage.KVEntry{
				Key:   itRes.Key,
				Value: itRes.Value,
			})
		}
	}

	var regionsInfo []*serverpb.RegionInfo
	for _, serializedStatusUpdate := range clusterInfo {
		statusUpdate := serverpb.UpdateStatusRequest{}
		if err = json.Unmarshal(serializedStatusUpdate.Value, &statusUpdate); err != nil {
			d.logger.Error("Unable to unmarshal status request", zap.Error(err))
			continue
		}
		// Filter regions outside the requested DC (if provided)
		if request.GetDcID() != "" && request.GetDcID() != statusUpdate.GetRegionInfo().GetDcID() {
			continue
		}
		// Filter inactive regions and regions whose status was updated long time back and hence considered inactive
		if hlc.GetTimeAgo(statusUpdate.GetTimestamp()) < d.config.HeartbeatTimeout && statusUpdate.GetRegionInfo().GetStatus() != serverpb.RegionStatus_INACTIVE {
			regionsInfo = append(regionsInfo, statusUpdate.GetRegionInfo())
		}
		// TODO : Filter such that only 1 master is returned per region
	}
	return &serverpb.GetClusterInfoResponse{RegionInfos: regionsInfo}, nil
}

func NewDiscoveryService(dkvService master.DKVService, logger *zap.Logger, config *DiscoveryConfig) (serverpb.DKVDiscoveryServer, error) {
	dkvClient, err := ctl.CreateInProcessDKVClient(dkvService).GRPCClient()
	if err != nil {
		return nil, err
	}
	return &discoverService{dkvCli: dkvClient, logger: logger, config: config}, nil
}

func newErrorStatus(err error) *serverpb.Status {
	return &serverpb.Status{Code: -1, Message: err.Error()}
}