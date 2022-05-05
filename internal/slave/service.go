package slave

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/flipkart-incubator/dkv/pkg/health"

	"github.com/flipkart-incubator/dkv/internal/discovery"
	"github.com/flipkart-incubator/dkv/internal/hlc"
	opts "github.com/flipkart-incubator/dkv/internal/opts"
	"github.com/flipkart-incubator/dkv/internal/stats"
	"github.com/flipkart-incubator/dkv/internal/storage"
	"github.com/flipkart-incubator/dkv/pkg/ctl"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/emptypb"
)

// A DKVService represents a service for serving key value data.
type DKVService interface {
	io.Closer
	serverpb.DKVServer
	serverpb.DKVDiscoveryNodeServer
	health.HealthServer
}

type ReplicationConfig struct {
	// Max num changes to poll from master in a single replication call
	MaxNumChngs uint32
	// Interval to periodically poll changes from master
	ReplPollInterval time.Duration
	// Maximum allowed replication lag from master for the slave to be considered valid
	MaxActiveReplLag uint64
	// Maximum allowed replication time elapsed from master for the slave to be considered valid
	// Applicable when replication requests are erroring out due to an issue with master / slave
	MaxActiveReplElapsed uint64
	// Listener address of the master node
	ReplMasterAddr string
}

type replInfo struct {
	// can be nil only initially when trying to find a master to replicate from
	replCli *ctl.DKVClient
	// replActive can be used to avoid setting replCli to nil during master reelection
	// which would otherwise require additional locks to prevent crashes due to intermediate null switches
	replActive   bool
	replTckr     *time.Ticker
	replStop     chan struct{}
	replLag      uint64
	lastReplTime uint64
	replConfig   *ReplicationConfig
	fromChngNum  uint64
	//replDelay is an approximation of the delay in time units of the changes seen
	// in the master and the same changes seen in the slave
	replDelay float64
}

type slaveService struct {
	store       storage.KVStore
	ca          storage.ChangeApplier
	regionInfo  *serverpb.RegionInfo
	clusterInfo discovery.ClusterInfoGetter
	isClosed    bool
	replInfo    *replInfo
	serveropts  *opts.ServerOpts
	stat        *stat
}
type stat struct {
	ReplicationLag    prometheus.Gauge
	ReplicationDelay  prometheus.Gauge
	ReplicationStatus *prometheus.SummaryVec
	ReplicationSpeed  prometheus.Histogram
}

func newStat(registry prometheus.Registerer) *stat {
	replicationLag := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: stats.Namespace,
		Name:      "slave_replication_lag",
		Help:      "replication lag of the slave",
	})
	replicationDelay := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: stats.Namespace,
		Name:      "slave_replication_delay",
		Help:      "replication delay of the slave",
	})
	replicationStatus := prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace: stats.Namespace,
		Name:      "slave_replication_status",
		Help:      "replication status of the slave",
		MaxAge:    5 * time.Second,
	}, []string{"masterAddr"})
	replicationSpeed := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: stats.Namespace,
		Name:      "slave_replication_speed",
		Help:      "replication speed of the slave",
	})
	registry.MustRegister(replicationLag, replicationDelay, replicationSpeed, replicationStatus)
	return &stat{
		ReplicationLag:    replicationLag,
		ReplicationDelay:  replicationDelay,
		ReplicationStatus: replicationStatus,
		ReplicationSpeed:  replicationSpeed,
	}
}

// NewService creates a slave DKVService that periodically polls
// for changes from master node and replicates them onto its local
// storage. As a result, it forbids changes to this local storage
// through any of the other key value mutators.
func NewService(store storage.KVStore, ca storage.ChangeApplier, regionInfo *serverpb.RegionInfo,
	replConf *ReplicationConfig, clusterInfo discovery.ClusterInfoGetter,
	serveropts *opts.ServerOpts) (DKVService, error) {
	if store == nil || ca == nil {
		return nil, errors.New("invalid args - params `store`, `ca` and `replPollInterval` are all mandatory")
	}
	return newSlaveService(store, ca, regionInfo, replConf, clusterInfo, serveropts), nil
}

func newSlaveService(store storage.KVStore, ca storage.ChangeApplier, info *serverpb.RegionInfo,
	replConf *ReplicationConfig, clusterInfo discovery.ClusterInfoGetter, serveropts *opts.ServerOpts) *slaveService {
	ri := &replInfo{replConfig: replConf}
	ss := &slaveService{store: store, ca: ca, regionInfo: info, replInfo: ri, clusterInfo: clusterInfo, serveropts: serveropts, stat: newStat(serveropts.PrometheusRegistry)}
	ss.findAndConnectToMaster()
	ss.startReplication()
	return ss
}

func (ss *slaveService) Put(_ context.Context, _ *serverpb.PutRequest) (*serverpb.PutResponse, error) {
	return nil, errors.New("DKV slave service does not support keyspace mutations")
}

func (ss *slaveService) MultiPut(_ context.Context, _ *serverpb.MultiPutRequest) (*serverpb.PutResponse, error) {
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

func (ss *slaveService) Check(ctx context.Context, healthCheckReq *health.HealthCheckRequest) (*health.HealthCheckResponse, error) {
	if ss.isClosed {
		return &health.HealthCheckResponse{Status: health.HealthCheckResponse_NOT_SERVING}, nil
	}
	if ss.replInfo.replLag > ss.replInfo.replConfig.MaxActiveReplLag {
		return &health.HealthCheckResponse{Status: health.HealthCheckResponse_NOT_SERVING}, nil
	}

	// server has not started replicating yet or the server last replicated more than MaxActiveReplElapsed ago
	if ss.replInfo.lastReplTime == 0 || hlc.GetTimeAgo(ss.replInfo.lastReplTime) > ss.replInfo.replConfig.MaxActiveReplElapsed {
		return &health.HealthCheckResponse{Status: health.HealthCheckResponse_NOT_SERVING}, nil
	}
	return &health.HealthCheckResponse{Status: health.HealthCheckResponse_SERVING}, nil
}

func (ss *slaveService) Watch(req *health.HealthCheckRequest, watcher health.Health_WatchServer) error {
	if ss.isClosed {
		return watcher.Send(&health.HealthCheckResponse{Status: health.HealthCheckResponse_NOT_SERVING})
	}
	ticker := time.NewTicker(time.Duration(ss.serveropts.HealthCheckTickerInterval) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			checkResponse, err := ss.Check(context.Background(), req)
			if err != nil {
				return err
			}
			if err := watcher.Send(checkResponse); err != nil {
				return err
			}
		case <-ss.replInfo.replStop:
			return watcher.Send(&health.HealthCheckResponse{Status: health.HealthCheckResponse_NOT_SERVING})
		}
	}
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
	err := iteration.ForEach(func(e *serverpb.KVPair) error {
		itRes := &serverpb.IterateResponse{Status: newEmptyStatus(), Key: e.Key, Value: e.Value}
		return dkvIterSrvr.Send(itRes)
	})
	if err != nil {
		itRes := &serverpb.IterateResponse{Status: newErrorStatus(err)}
		return dkvIterSrvr.Send(itRes)
	}
	return nil
}

func (ss *slaveService) Close() error {
	ss.serveropts.Logger.Info("Closing the slave service")
	ss.replInfo.replStop <- struct{}{}
	ss.replInfo.replTckr.Stop()
	if ss.replInfo.replCli != nil {
		ss.replInfo.replCli.Close()
	}
	ss.store.Close()
	ss.isClosed = true
	return nil
}

func (ss *slaveService) startReplication() {
	ss.replInfo.replTckr = time.NewTicker(ss.replInfo.replConfig.ReplPollInterval)
	latestChngNum, _ := ss.ca.GetLatestAppliedChangeNumber()
	ss.replInfo.fromChngNum = 1 + latestChngNum
	ss.replInfo.replStop = make(chan struct{})
	slg := ss.serveropts.Logger.Sugar()
	slg.Infof("Replicating changes from change number: %d and polling interval: %s", ss.replInfo.fromChngNum, ss.replInfo.replConfig.ReplPollInterval.String())
	slg.Sync()
	go ss.pollAndApplyChanges()
}

func (ss *slaveService) pollAndApplyChanges() {
	for {
		select {
		case <-ss.replInfo.replTckr.C:
			ss.serveropts.Logger.Info("Current replication lag", zap.Uint64("ReplicationLag", ss.replInfo.replLag))
			ss.serveropts.StatsCli.Gauge("replication.lag", int64(ss.replInfo.replLag))
			ss.stat.ReplicationLag.Set(float64(ss.replInfo.replLag))
			ss.stat.ReplicationDelay.Set(ss.replInfo.replDelay)
			if err := ss.applyChangesFromMaster(ss.replInfo.replConfig.MaxNumChngs); err != nil {
				ss.stat.ReplicationStatus.WithLabelValues("no-master").Observe(1)
				ss.serveropts.Logger.Error("Unable to retrieve changes from master", zap.Error(err))
				if err := ss.replaceMasterIfInactive(); err != nil {
					ss.serveropts.Logger.Error("Unable to replace master", zap.Error(err))
				}
			}
			ss.stat.ReplicationStatus.WithLabelValues(ss.replInfo.replConfig.ReplMasterAddr).Observe(1)
		case <-ss.replInfo.replStop:
			ss.stat.ReplicationStatus.WithLabelValues("no-master").Observe(0)
			ss.serveropts.Logger.Info("Stopping the change poller")
			break
		}
	}
}

func (ss *slaveService) applyChangesFromMaster(chngsPerBatch uint32) error {
	defer ss.serveropts.StatsCli.Timing("slave.applyChangesFromMaster.latency.ms", time.Now())

	if ss.replInfo.replCli == nil || !ss.replInfo.replActive {
		return errors.New("can not replicate as unable to connect to an active master")
	}
	ss.serveropts.Logger.Info("Retrieving changes from master", zap.Uint64("FromChangeNumber", ss.replInfo.fromChngNum), zap.Uint32("ChangesPerBatch", chngsPerBatch))

	res, err := ss.replInfo.replCli.GetChanges(ss.replInfo.fromChngNum, chngsPerBatch)
	if err == nil {
		if res.Status.Code != 0 {
			// this is an error from DKV master's end
			err = errors.New(res.Status.Message)
		} else {
			if res.MasterChangeNumber < (ss.replInfo.fromChngNum - 1) {
				ss.serveropts.Logger.Error("change number of the master node can not be lesser than the change number of the slave node", zap.Uint64("MasterChangeNum", res.MasterChangeNumber), zap.Uint64("FromChangeNum", ss.replInfo.fromChngNum))
				err = errors.New("change number of the master node can not be lesser than the change number of the slave node")
			} else {
				if err = ss.applyChanges(res); err == nil {
					ss.replInfo.lastReplTime = hlc.UnixNow()
				}
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
			ss.serveropts.Logger.Warn("GetChanges call exceeded resource limits", zap.Error(err))
			if newMaxNumChngs := chngsPerBatch >> 1; newMaxNumChngs > 0 {
				ss.serveropts.Logger.Warn("Retrieving smaller batches of changes", zap.Uint32("before", chngsPerBatch), zap.Uint32("after", newMaxNumChngs))
				err = ss.applyChangesFromMaster(newMaxNumChngs)
			} else {
				err = errors.New("unable to retrieve changes from master due to GRPC resource exhaustion on slave")
			}
		}
	}
	return err
}

func (ss *slaveService) applyChanges(chngsRes *serverpb.GetChangesResponse) error {
	if chngsRes.NumberOfChanges > 0 {
		ss.serveropts.Logger.Info("Applying the changes received from master", zap.Uint32("NumberOfChanges", chngsRes.NumberOfChanges))
		actChngNum, err := ss.ca.SaveChanges(chngsRes.Changes)
		if err != nil {
			return err
		}
		if timeBwRepl := (hlc.UnixNow() - ss.replInfo.lastReplTime); ss.replInfo.lastReplTime != 0 && timeBwRepl != 0 && actChngNum > ss.replInfo.fromChngNum {
			currentReplSpeed := (float64(actChngNum-ss.replInfo.fromChngNum) / float64(timeBwRepl))
			ss.stat.ReplicationSpeed.Observe(currentReplSpeed)
		}
		ss.replInfo.fromChngNum = actChngNum + 1
		ss.serveropts.Logger.Info("Changes applied to local storage", zap.Uint64("FromChangeNumber", ss.replInfo.fromChngNum))
		if chngsRes.MasterChangeNumber >= actChngNum {
			ss.replInfo.replLag = chngsRes.MasterChangeNumber - actChngNum
		} else {
			ss.replInfo.replLag = 0 //replication lag can be negative when master has returned every change that was available to it
		}
		replicationSpeedMetric := getReplicationSpeed(ss)
		if cnt := replicationSpeedMetric.Histogram.GetSampleCount(); cnt != 0 {
			replSpeedAvg := replicationSpeedMetric.Histogram.GetSampleSum() / float64(cnt)
			if replSpeedAvg > float64(1e-9) {
				ss.replInfo.replDelay = float64(ss.replInfo.replLag) / replSpeedAvg
			}
		}
	} else {
		ss.serveropts.Logger.Info("Not received any changes from master")
	}
	return nil
}

func getReplicationSpeed(ss *slaveService) *dto.Metric {
	metric := &dto.Metric{}
	if err := ss.stat.ReplicationSpeed.Write(metric); err != nil {
		ss.serveropts.Logger.Warn("Error while writing out %s metric", zap.String("Metric", "ReplicationSpeed"))
	}
	return metric
}

func newErrorStatus(err error) *serverpb.Status {
	return &serverpb.Status{Code: -1, Message: err.Error()}
}

func newEmptyStatus() *serverpb.Status {
	return &serverpb.Status{Code: 0, Message: ""}
}

func (ss *slaveService) GetStatus(context context.Context, request *emptypb.Empty) (*serverpb.RegionInfo, error) {
	if ss.replInfo.replLag > ss.replInfo.replConfig.MaxActiveReplLag {
		ss.regionInfo.Status = serverpb.RegionStatus_INACTIVE
	} else if ss.replInfo.lastReplTime == 0 || hlc.GetTimeAgo(ss.replInfo.lastReplTime) > ss.replInfo.replConfig.MaxActiveReplElapsed {
		ss.regionInfo.Status = serverpb.RegionStatus_INACTIVE
	} else if ss.isClosed {
		ss.regionInfo.Status = serverpb.RegionStatus_INACTIVE
	} else {
		ss.regionInfo.Status = serverpb.RegionStatus_ACTIVE_SLAVE
	}
	ss.regionInfo.MasterHost = &ss.replInfo.replConfig.ReplMasterAddr
	ss.serveropts.Logger.Debug("Current Info", zap.String("Status", ss.regionInfo.Status.String()),
		zap.Uint64("Repl Lag", ss.replInfo.replLag), zap.Uint64("Last Repl time", ss.replInfo.lastReplTime))
	return ss.regionInfo, nil
}

func (ss *slaveService) replaceMasterIfInactive() error {
	if ss.replInfo.replConfig.ReplMasterAddr != "" {
		return ss.reconnectMaster() // reconnect to the existing master
	}
	if regions, err := ss.clusterInfo.GetClusterStatus(ss.regionInfo.GetDatabase(), ss.regionInfo.GetVBucket()); err == nil {
		var currentMaster *serverpb.RegionInfo = nil
		for _, region := range regions {
			if region.NodeAddress == ss.replInfo.replConfig.ReplMasterAddr {
				currentMaster = region
				break
			}
		}

		if currentMaster == nil {
			// Current Master not found in cluster. implies current master is inactive
			return ss.reconnectMaster()
		} else if !isVBucketApplicableToBeMaster(currentMaster) {
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
	// This is so that replication doesn't happen from inactive master
	// which could otherwise result in slave marking itself active if no errors in replication
	ss.replInfo.replConfig.ReplMasterAddr = ""
	ss.replInfo.replActive = false
	return ss.findAndConnectToMaster()
}

func (ss *slaveService) findAndConnectToMaster() error {
	if master, err := ss.findNewMaster(); err == nil {
		// TODO: Check if authority override option is needed for slaves while they connect with masters
		if replCli, err := ctl.NewInSecureDKVClient(*master, "", ctl.DefaultConnectOpts); err == nil {
			if ss.replInfo.replCli != nil {
				ss.replInfo.replCli.Close()
			}
			ss.replInfo.replCli = replCli
			ss.replInfo.replConfig.ReplMasterAddr = *master
			ss.replInfo.replActive = true
			ss.serveropts.Logger.Warn("Started replication client with master", zap.String("MasterIP", *master))
		} else {
			ss.serveropts.Logger.Warn("Unable to create a replication client", zap.Error(err))
			return err
		}
	} else {
		ss.serveropts.Logger.Warn("Unable to find a master for this slave to replicate from", zap.Error(err))
		return err
	}
	return nil
}

// Finds a new active master for the region
// Prefers followers within the local DC first, followed by master within local DC
// followed by followers outside DC, followed by master outside DC
// TODO - rather than randomly selecting a master from applicable followers, load balance to distribute better
func (ss *slaveService) findNewMaster() (*string, error) {
	if ss.replInfo.replConfig.ReplMasterAddr != "" {
		return &ss.replInfo.replConfig.ReplMasterAddr, nil
	}
	// Get all active regions
	if vBuckets, err := ss.clusterInfo.GetClusterStatus(ss.regionInfo.GetDatabase(), ss.regionInfo.GetVBucket()); err == nil {
		// Filter regions applicable to become master for this slave
		var filteredVBuckets []*serverpb.RegionInfo
		for _, vBucket := range vBuckets {
			if isVBucketApplicableToBeMaster(vBucket) {
				filteredVBuckets = append(filteredVBuckets, vBucket)
			}
		}
		if len(filteredVBuckets) == 0 {
			return nil, fmt.Errorf("no active master found for database %s and vBucket %s",
				ss.regionInfo.Database, ss.regionInfo.VBucket)
		} else {
			vBuckets = filteredVBuckets
		}

		// If any region exists within the DC, prefer those.
		var localDCVBuckets []*serverpb.RegionInfo
		for _, vBucket := range vBuckets {
			if vBucket.DcID == ss.regionInfo.DcID {
				localDCVBuckets = append(localDCVBuckets, vBucket)
			}
		}
		if len(localDCVBuckets) > 0 {
			vBuckets = localDCVBuckets
		}

		// If any non master region exists, prefer those.
		var followers []*serverpb.RegionInfo
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
		return &vBuckets[idx].NodeAddress, nil
	} else {
		return nil, err
	}
}

// Assumption is slave can not replicate from any other slave
// if above assumption is incorrect, we should modify the filter conditions appropriately
func isVBucketApplicableToBeMaster(info *serverpb.RegionInfo) bool {
	return info.Status == serverpb.RegionStatus_LEADER || info.Status == serverpb.RegionStatus_PRIMARY_FOLLOWER ||
		info.Status == serverpb.RegionStatus_SECONDARY_FOLLOWER
}
