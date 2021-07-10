package discovery

/*
This class contains the behaviour of receiving status updates from nodes in the cluster
and providing the latest cluster info of active master / followers / slave of a region when requested
 */

import (
	. "context"
	"fmt"
	"github.com/flipkart-incubator/dkv/internal/hlc"
	. "github.com/flipkart-incubator/dkv/pkg/serverpb"
	"gopkg.in/ini.v1"
	"strconv"

	"encoding/json"
	"go.uber.org/zap"
)

type DiscoveryConfig struct {
	// time in seconds after which the status entry of the region & node combination can be purged
	statusTTl			uint64
	// maximum time in seconds for the last status update to be considered valid
	// after exceeding this time the region & node combination can be marked invalid
	heartbeatTimeout	uint64
}

func NewDiscoverConfigFromIni(sect *ini.Section) *DiscoveryConfig {
	sectConf := sect.KeysHash()
	if statusTTL, err := strconv.ParseUint(sectConf["statusTTL"], 10, 64); err == nil {
		if heartbeatTimeout, err := strconv.ParseUint(sectConf["heartbeatTimeout"], 10, 64); err == nil {
			return NewDiscoverConfig(statusTTL, heartbeatTimeout)
		}
	}
	panic(fmt.Errorf("Invalid discovery server configuration. Check section %s", sect.Name()))
}

func NewDiscoverConfig(statusTTL uint64, heartbeatTimeout uint64) *DiscoveryConfig {
	return &DiscoveryConfig{statusTTL, heartbeatTimeout}
}

type discoverService struct {
	// TODO - dkvServer should point to interface of master (currently distributedService)
	dkvServer DKVServer
	logger    *zap.Logger
	config    *DiscoveryConfig
}

// Create the dkv lookup key from region info
// Since granularity of an update is at a region per node, the same is used for creating a key
// As lookups will be mostly on database and possibly on vBucket (within a database) the same order is used for key
// This enables faster scans for database/vBucket
func createKeyToInsert(info *RegionInfo) []byte {
	return []byte(info.GetDatabase() + ":" + info.GetVBucket() + ":" + info.GetNodeAddress())
}

// Create lookup key based on input request
// Using the sorted nature of keys, lookup by either database or by database and vBucket or all keys
func createKeyToGet(request *GetClusterInfoRequest) []byte {
	if request.GetDatabase() != "" {
		if request.GetVBucket() != "" {
			return []byte(request.GetDatabase() + ":" + request.GetVBucket() + ":");
		} else {
			return []byte(request.GetDatabase() + ":");
		}
	} else {
		return []byte{};
	}
}

func (d *discoverService) UpdateStatus(ctx Context, request *UpdateStatusRequest) (*Status, error) {
	// Store the serialised form of the request
	val, err := json.Marshal(request);

	if err != nil {
		d.logger.Error("Unable to marshal status request", zap.Error(err));
		return newErrorStatus(err), err;
	}

	// TODO - check if expiry TTL is working
	putRequest := PutRequest{Key: createKeyToInsert(request.GetRegionInfo()), Value: val, ExpireTS: hlc.GetUnixTimeFromNow(d.config.statusTTl)};

	if _, err := d.dkvServer.Put(ctx, &putRequest); err != nil {
		return newErrorStatus(err), err;
	} else {
		return newEmptyStatus(), nil;
	}
}

func (d *discoverService) GetClusterInfo(ctx Context, request *GetClusterInfoRequest) (*GetClusterInfoResponse, error) {
	// Create a prefix multi get request to loop through all required status updates
	// Iterator sends the responses on grpc stream which is unnecessary here
	getRequest := PrefixMultiGetRequest{KeyPrefix: createKeyToGet(request)}

	if clusterInfo, err := d.dkvServer.PrefixMultiGet(ctx, &getRequest); err == nil {
		regionsInfo := []*RegionInfo{};
		for _, serializedStatusUpdate := range clusterInfo.KeyValues {
			statusUpdate := UpdateStatusRequest{};
			if err := json.Unmarshal(serializedStatusUpdate.Value, &statusUpdate); err != nil {
				d.logger.Error("Unable to unmarshal status request", zap.Error(err));
				continue
			}
			// Filter regions outside the requested DC (if provided)
			if (request.GetDcID() != "" && request.GetDcID() != statusUpdate.GetRegionInfo().GetDcID()) {
				continue
			}
			// Filter inactive regions and regions whose status was updated long time back and hence considered inactive
			if (hlc.GetTimeAgo(statusUpdate.GetTimestamp()) < d.config.HeartbeatTimeout && statusUpdate.GetRegionInfo().GetStatus() != RegionStatus_INACTIVE) {
				regionsInfo = append(regionsInfo, statusUpdate.GetRegionInfo());
			}
			// TODO : Filter such that only 1 master is returned per region
		}
		return &GetClusterInfoResponse{RegionInfos: regionsInfo}, nil;
	} else {
		return nil, err
	}
}

func NewDiscoveryService(server DKVServer, logger *zap.Logger, config *DiscoveryConfig) DKVDiscoveryServer {
	return &discoverService{dkvServer: server, logger: logger, config: config}
}

func newErrorStatus(err error) *Status {
	return &Status{Code: -1, Message: err.Error()}
}

func newEmptyStatus() *Status {
	return &Status{Code: 0, Message: ""}
}
