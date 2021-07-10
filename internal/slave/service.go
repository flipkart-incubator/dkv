package slave

import (
	"context"
	"errors"
	"fmt"
	"github.com/flipkart-incubator/dkv/internal/discovery"
	"github.com/flipkart-incubator/dkv/internal/hlc"
	"google.golang.org/protobuf/types/known/emptypb"
	"io"
	"math/rand"
	"strings"
	"time"

	"github.com/flipkart-incubator/dkv/internal/stats"
	"github.com/flipkart-incubator/dkv/internal/storage"
	"github.com/flipkart-incubator/dkv/pkg/ctl"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
	"go.uber.org/zap"
)

// A DKVService represents a service for serving key value data.
type DKVService interface {
	io.Closer
	serverpb.DKVServer
}

type ReplicationConfig struct {
	// Max num changes to poll from master in a single replication call
	maxNumChngs uint32
	// Interval to periodically poll changes from master
	replPollInterval time.Duration
	// Maximum allowed replication lag from master for the slave to be considered valid
	maxActiveReplLag uint64
	// Maximum allowed replication time elapsed from master for the slave to be considered valid
	// Applicable when replication requests are erroring out due to an issue with master / slave
	maxActiveReplElapsed uint64
	// Listener address of the master node
	replMasterAddr string
}

func NewReplicationConfig(maxNumChanges uint32, replPollInterval time.Duration,
	maxActiveReplLag uint64, maxActiveReplElapsed uint64) *ReplicationConfig {
	return &ReplicationConfig{maxNumChngs: maxNumChanges, replPollInterval: replPollInterval,
		maxActiveReplLag: maxActiveReplLag, maxActiveReplElapsed: maxActiveReplElapsed}
}

type slaveService struct {
	store        storage.KVStore
	ca           storage.ChangeApplier
	lg           *zap.Logger
	statsCli     stats.Client
	replCli      *ctl.DKVClient // can be nil when trying to find a master to replicate from
	replTckr     *time.Ticker
	regionInfo   *serverpb.RegionInfo
	replStop     chan struct{}
	replLag      uint64
	fromChngNum  uint64
	lastReplTime uint64
	replConfig   *ReplicationConfig
	clusterInfo  discovery.ClusterInfoGetter
	isClosed     bool
}

// NewService creates a slave DKVService that periodically polls
// for changes from master node and replicates them onto its local
// storage. As a result, it forbids changes to this local storage
// through any of the other key value mutators.
func NewService(store storage.KVStore, ca storage.ChangeApplier, lgr *zap.Logger,
	statsCli stats.Client, regionInfo *serverpb.RegionInfo, replConf *ReplicationConfig, clusterInfo discovery.ClusterInfoGetter) (DKVService, error) {
	if store == nil || ca == nil {
		return nil, errors.New("invalid args - params `store`, `ca` and `replPollInterval` are all mandatory")
	}
	return newSlaveService(store, ca, lgr, statsCli, regionInfo, replConf, clusterInfo), nil
}

func newSlaveService(store storage.KVStore, ca storage.ChangeApplier, lgr *zap.Logger,
	statsCli stats.Client, info *serverpb.RegionInfo, replConf *ReplicationConfig, clusterInfo discovery.ClusterInfoGetter) *slaveService {
	ss := &slaveService{store: store, ca: ca, lg: lgr, statsCli: statsCli,
		regionInfo: info, replConfig: replConf, clusterInfo: clusterInfo}
	ss.findAndConnectToMaster()
	ss.startReplication()
	return ss
}

func (ss *slaveService) Put(_ context.Context, _ *serverpb.PutRequest) (*serverpb.PutResponse, error) {
	return nil, errors.New("DKV slave service does not support keyspace mutations")
}

func (ss *slaveService) Delete(_ context.Context, _ *serverpb.DeleteRequest) (*serverpb.DeleteResponse, error) {
	return nil, errors.New("DKV slave service does not support keyspace mutations")
}

func (ss *slaveService) CompareAndSet(_ context.Context, _ *serverpb.CompareAndSetRequest) (*serverpb.CompareAndSetResponse, error) {
	return nil, errors.New("DKV slave service does not support keyspace mutations")
}

func (ss *slaveService) Get(ctx context.Context, getReq *serverpb.GetRequest) (*serverpb.GetResponse, error) {
	readResults, err := ss.store.Get(getReq.Key)
	res := &serverpb.GetResponse{Status: newEmptyStatus()}
	if err != nil {
		res.Status = newErrorStatus(err)
	} else {
		if len(readResults) == 1 {
			res.Value = readResults[0].Value
		}
	}
	return res, err
}

func (ss *slaveService) MultiGet(ctx context.Context, multiGetReq *serverpb.MultiGetRequest) (*serverpb.MultiGetResponse, error) {
	readResults, err := ss.store.Get(multiGetReq.Keys...)
	res := &serverpb.MultiGetResponse{Status: newEmptyStatus()}
	if err != nil {
		res.Status = newErrorStatus(err)
	} else {
		res.KeyValues = readResults
	}
	return res, err
}

func (ss *slaveService) Iterate(iterReq *serverpb.IterateRequest, dkvIterSrvr serverpb.DKV_IterateServer) error {
	iteration := storage.NewIteration(ss.store, iterReq)
	err := iteration.ForEach(func(k, v []byte) error {
		itRes := &serverpb.IterateResponse{Status: newEmptyStatus(), Key: k, Value: v}
		return dkvIterSrvr.Send(itRes)
	})
	if err != nil {
		itRes := &serverpb.IterateResponse{Status: newErrorStatus(err)}
		return dkvIterSrvr.Send(itRes)
	}
	return nil
}

func (ss *slaveService) PrefixMultiGet(ctx context.Context, request *serverpb.PrefixMultiGetRequest) (*serverpb.MultiGetResponse, error) {
	// TODO - share code between standalone server and slave server where applicable (like here)
	panic("implement me")
}

func (ss *slaveService) Close() error {
	ss.lg.Info("Closing the slave service")
	ss.replStop <- struct{}{}
	ss.replTckr.Stop()
	if ss.replCli != nil {
		ss.replCli.Close()
	}
	ss.store.Close()
	ss.isClosed = true
	return nil
}

func (ss *slaveService) startReplication() {
	ss.replTckr = time.NewTicker(ss.replConfig.replPollInterval)
	latestChngNum, _ := ss.ca.GetLatestAppliedChangeNumber()
	ss.fromChngNum = 1 + latestChngNum
	ss.replStop = make(chan struct{})
	slg := ss.lg.Sugar()
	slg.Infof("Replicating changes from change number: %d and polling interval: %s",
		ss.fromChngNum, ss.replConfig.replPollInterval.String())
	slg.Sync()
	go ss.pollAndApplyChanges()
}

func (ss *slaveService) pollAndApplyChanges() {
	for {
		select {
		case <-ss.replTckr.C:
			ss.lg.Info("Current replication lag", zap.Uint64("ReplicationLag", ss.replLag))
			ss.statsCli.Gauge("replication.lag", int64(ss.replLag))
			if err := ss.applyChangesFromMaster(ss.replConfig.maxNumChngs); err != nil {
				ss.lg.Error("Unable to retrieve changes from master", zap.Error(err))
				if err := ss.replaceMasterIfInactive(); err != nil {
					ss.lg.Error("Unable to replace master", zap.Error(err))
				}
			}
		case <-ss.replStop:
			ss.lg.Info("Stopping the change poller")
			break
		}
	}
}

func (ss *slaveService) applyChangesFromMaster(chngsPerBatch uint32) error {
	ss.lg.Info("Retrieving changes from master", zap.Uint64("FromChangeNumber", ss.fromChngNum), zap.Uint32("ChangesPerBatch", chngsPerBatch))

	if ss.replCli == nil {
		return errors.New("Can not replicate as replication client not yet established")
	}

	res, err := ss.replCli.GetChanges(ss.fromChngNum, chngsPerBatch)
	if err == nil {
		if res.Status.Code != 0 {
			// this is an error from DKV master's end
			err = errors.New(res.Status.Message)
		} else {
			if res.MasterChangeNumber < (ss.fromChngNum - 1) {
				ss.lg.Error("change number of the master node can not be lesser than the change number of the slave node", zap.Uint64("MasterChangeNum", res.MasterChangeNumber), zap.Uint64("FromChangeNum", ss.fromChngNum))
				err = errors.New("change number of the master node can not be lesser than the change number of the slave node")
			} else {
				err = ss.applyChanges(res)
			}
		}
	} else {
		if strings.Contains(err.Error(), "ResourceExhausted") {
			// This is an error from DKV slave's end where the GRPC
			// receive buffer is exhausted. We now attempt to retrieve
			// the changes by halving the batch size. We try this until
			// the batch size can no longer be halved (= 0) and then
			// give up with an error. In such cases, this method is
			// invoked recursively utmost log2[ss.maxNumChngs] times.
			ss.lg.Warn("GetChanges call exceeded resource limits", zap.Error(err))
			if newMaxNumChngs := chngsPerBatch >> 1; newMaxNumChngs > 0 {
				ss.lg.Warn("Retrieving smaller batches of changes", zap.Uint32("before", chngsPerBatch), zap.Uint32("after", newMaxNumChngs))
				err = ss.applyChangesFromMaster(newMaxNumChngs)
			} else {
				err = errors.New("unable to retrieve changes from master due to GRPC resource exhaustion on slave")
			}
		}
	}
	if err == nil {
		ss.lastReplTime = hlc.UnixNow()
	}
	return err
}

func (ss *slaveService) applyChanges(chngsRes *serverpb.GetChangesResponse) error {
	if chngsRes.NumberOfChanges > 0 {
		ss.lg.Info("Applying the changes received from master", zap.Uint32("NumberOfChanges", chngsRes.NumberOfChanges))
		actChngNum, err := ss.ca.SaveChanges(chngsRes.Changes)
		if err != nil {
			return err
		}
		ss.fromChngNum = actChngNum + 1
		ss.lg.Info("Changes applied to local storage", zap.Uint64("FromChangeNumber", ss.fromChngNum))
		ss.replLag = chngsRes.MasterChangeNumber - actChngNum
	} else {
		ss.lg.Info("Not received any changes from master")
	}
	return nil
}

func newErrorStatus(err error) *serverpb.Status {
	return &serverpb.Status{Code: -1, Message: err.Error()}
}

func newEmptyStatus() *serverpb.Status {
	return &serverpb.Status{Code: 0, Message: ""}
}

func (ss *slaveService) GetStatus(context context.Context, request *emptypb.Empty) (*serverpb.RegionInfo, error) {
	if ss.replLag > ss.replConfig.maxActiveReplLag {
		ss.regionInfo.Status = serverpb.RegionStatus_INACTIVE
	} else if ss.lastReplTime == 0 || hlc.GetTimeAgo(ss.lastReplTime) > ss.replConfig.maxActiveReplElapsed {
		ss.regionInfo.Status = serverpb.RegionStatus_INACTIVE
	} else if ss.isClosed {
		ss.regionInfo.Status = serverpb.RegionStatus_INACTIVE
	} else {
		ss.regionInfo.Status = serverpb.RegionStatus_ACTIVE_SLAVE
	}
	ss.regionInfo.MasterHost = &ss.replConfig.replMasterAddr
	ss.lg.Debug("Current Info", zap.String("Status", ss.regionInfo.Status.String()),
		zap.Uint64("Repl Lag", ss.replLag), zap.Uint64("Last Repl time", ss.lastReplTime))
	return ss.regionInfo, nil
}

func (ss *slaveService) replaceMasterIfInactive() error {
	if regions, err := ss.clusterInfo.GetClusterStatus(ss.regionInfo.GetDatabase(), ss.regionInfo.GetVBucket()); err == nil {
		currentMasterIdx := len(regions)
		for i, region := range regions {
			if region.NodeAddress == ss.replConfig.replMasterAddr {
				currentMasterIdx = i
				break
			}
		}

		if currentMasterIdx == len(regions) {
			// Current Master not found in cluster. implies current master is inactive
			return ss.reconnectMaster()
		} else if !isVBucketApplicableToBeMaster(regions[currentMasterIdx]) {
			return ss.reconnectMaster()
		} else {
			// current master is active. No action required as replication error could be temporary
			// need to validate this assumption though
			return nil
		}
	} else {
		return err
	}
}

func (ss *slaveService) reconnectMaster() error {
	// Close any existing client connection
	// This is so that replication doesn't happen from inactive master
	// which could otherwise result in slave marking itself active if no errors in replication
	if ss.replCli != nil {
		ss.replCli.Close()
		ss.replCli = nil
		ss.replConfig.replMasterAddr = ""
	}
	return ss.findAndConnectToMaster()
}

func (ss *slaveService) findAndConnectToMaster() error {
	if master, err := ss.findNewMaster(); err == nil {
		// TODO: Check if authority override option is needed for slaves while they connect with masters
		if replCli, err := ctl.NewInSecureDKVClient(master, ""); err == nil {
			// concurrency issues ?
			ss.replCli = replCli
			ss.replConfig.replMasterAddr = master
		} else {
			ss.lg.Warn("Unable to create a replication client", zap.Error(err))
			return err
		}
	} else {
		ss.lg.Warn("Unable to find a master for this slave to replicate from", zap.Error(err))
		return err
	}
	return nil
}

// Finds a new active master for the region
// Prefers followers within the local DC first, followed by master within local DC
// followed by followers outside DC, followed by master outside DC
// TODO - rather than randomly selecting a master from applicable followers, load balance to distribute better
func (ss *slaveService) findNewMaster() (string, error) {
	// Get all active regions
	if vBuckets, err := ss.clusterInfo.GetClusterStatus(ss.regionInfo.GetDatabase(), ss.regionInfo.GetVBucket()); err == nil {
		// Filter regions applicable to become master for this slave
		filteredVBuckets := []*serverpb.RegionInfo{}
		for _, vBucket := range vBuckets {
			if isVBucketApplicableToBeMaster(vBucket) {
				filteredVBuckets = append(filteredVBuckets, vBucket)
			}
		}
		if len(filteredVBuckets) == 0 {
			return "", fmt.Errorf("No active master found for database %s and vBucket %s",
				ss.regionInfo.Database, ss.regionInfo.VBucket)
		} else {
			vBuckets = filteredVBuckets
		}

		// If any region exists within the DC, prefer those.
		localDCVBuckets := []*serverpb.RegionInfo{}
		for _, vBucket := range vBuckets {
			if vBucket.DcID == ss.regionInfo.DcID {
				localDCVBuckets = append(localDCVBuckets, vBucket)
			}
		}
		if len(localDCVBuckets) > 0 {
			vBuckets = localDCVBuckets
		}

		// If any non master region exists, prefer those.
		followers := []*serverpb.RegionInfo{}
		for _, vBucket := range vBuckets {
			if vBucket.Status != serverpb.RegionStatus_LEADER {
				followers = append(followers, vBucket)
			}
		}
		if len(followers) > 0 {
			vBuckets = followers
		}

		// Randomly select 1 region
		idx := rand.Intn(len(vBuckets))
		return vBuckets[idx].NodeAddress, nil
	} else {
		return "", err
	}
}

// Assumption is slave can not replicate from any other slave
// if above assumption is incorrect, we should modify the filter conditions appropriately
func isVBucketApplicableToBeMaster(info *serverpb.RegionInfo) bool {
	return info.Status == serverpb.RegionStatus_LEADER || info.Status == serverpb.RegionStatus_PRIMARY_FOLLOWER ||
		info.Status == serverpb.RegionStatus_SECONDARY_FOLLOWER
}
