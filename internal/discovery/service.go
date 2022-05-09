package discovery

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/flipkart-incubator/dkv/internal/hlc"
	"github.com/flipkart-incubator/dkv/internal/opts"
	"github.com/flipkart-incubator/dkv/pkg/ctl"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
	"go.uber.org/zap"
	"io"
)

type discoverService struct {
	dkvCli serverpb.DKVClient
	logger *zap.Logger
	config *opts.DiscoveryServerConfig
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

	putRequest := serverpb.PutRequest{Key: createKeyToInsert(request.GetRegionInfo()), Value: val, ExpireTS: hlc.GetUnixTimeFromNow(uint64(d.config.StatusTTl))}
	if _, err = d.dkvCli.Put(ctx, &putRequest); err != nil {
		return newErrorStatus(err), err
	} else {
		return &serverpb.Status{Code: 0, Message: ""}, nil
	}
}

func (d *discoverService) GetClusterInfo(ctx context.Context, request *serverpb.GetClusterInfoRequest) (*serverpb.GetClusterInfoResponse, error) {
	iterReq := &serverpb.IterateRequest{KeyPrefix: createKeyToGet(request)}
	kvStrm, err := d.dkvCli.Iterate(ctx, iterReq)
	if err != nil {
		d.logger.Error("Unable to get cluster info", zap.Error(err))
		return nil, err
	}
	var clusterInfo []serverpb.KVPair
	for {
		itRes, err := kvStrm.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			// Better to return error rather than partial results as incomplete results could cause client misbehaviour
			d.logger.Error("Partial failure in getting cluster info", zap.Error(err))
			return nil, err
		} else if bytes.Contains(itRes.Key, []byte("dkv_meta")) {
			continue
		} else {
			clusterInfo = append(clusterInfo, serverpb.KVPair{
				Key:   itRes.Key,
				Value: itRes.Value,
			})
		}
	}

	var regionsInfo []*serverpb.RegionInfo
	for _, serializedStatusUpdate := range clusterInfo {
		statusUpdate := serverpb.UpdateStatusRequest{}
		if err = json.Unmarshal(serializedStatusUpdate.Value, &statusUpdate); err != nil {
			d.logger.Error("Unable to unmarshal status request", zap.Error(err), zap.String("json", string(serializedStatusUpdate.Value)))
			continue
		}
		// Filter regions outside the requested DC (if provided)
		if request.GetDcID() != "" && request.GetDcID() != statusUpdate.GetRegionInfo().GetDcID() {
			continue
		}
		// Filter inactive regions and regions whose status was updated long time back and hence considered inactive
		// This simplifies logic on consumers of this API (envoy, slaves) which don't need to filter by status
		if hlc.GetTimeAgo(statusUpdate.GetTimestamp()) < uint64(d.config.HeartbeatTimeout) && statusUpdate.GetRegionInfo().GetStatus() != serverpb.RegionStatus_INACTIVE {
			regionsInfo = append(regionsInfo, statusUpdate.GetRegionInfo())
		}
		// TODO : Filter such that only 1 master is returned per region
	}
	return &serverpb.GetClusterInfoResponse{RegionInfos: regionsInfo}, nil
}

func NewDiscoveryService(dkvService serverpb.DKVServer, logger *zap.Logger, config *opts.DiscoveryServerConfig) (serverpb.DKVDiscoveryServer, error) {
	dkvClient, err := ctl.CreateInProcessDKVClient(dkvService).GRPCClient()
	if err != nil {
		return nil, err
	}
	return &discoverService{dkvCli: dkvClient, logger: logger, config: config}, nil
}

func newErrorStatus(err error) *serverpb.Status {
	return &serverpb.Status{Code: -1, Message: err.Error()}
}
