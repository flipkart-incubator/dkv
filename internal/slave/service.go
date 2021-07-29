package slave

import (
	"context"
	"errors"
	"github.com/prometheus/client_golang/prometheus"
	"io"
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

type slaveService struct {
	store       storage.KVStore
	ca          storage.ChangeApplier
	lg          *zap.Logger
	statsCli    stats.Client
	stat        *stat
	replCli     *ctl.DKVClient
	replTckr    *time.Ticker
	replStop    chan struct{}
	replLag     uint64
	fromChngNum uint64
	maxNumChngs uint32
}

type stat struct {
	replicationLag prometheus.Gauge
}

func newStat() *stat {
	prometheus.NewGoCollector()
	return &stat{
		replicationLag: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "slave",
			Name:      "replication_lag",
			Help:      "replication lag of the slave",
		}),
	}
}

// TODO: check if this needs to be exposed as a flag
const maxNumChangesRepl = 10000

// NewService creates a slave DKVService that periodically polls
// for changes from master node and replicates them onto its local
// storage. As a result, it forbids changes to this local storage
// through any of the other key value mutators.
func NewService(store storage.KVStore, ca storage.ChangeApplier, replCli *ctl.DKVClient, replPollInterval time.Duration,
	lgr *zap.Logger, statsCli stats.Client) (DKVService, error) {
	if replPollInterval == 0 || replCli == nil || store == nil || ca == nil {
		return nil, errors.New("invalid args - params `store`, `ca`, `replCli` and `replPollInterval` are all mandatory")
	}
	return newSlaveService(store, ca, replCli, replPollInterval, lgr, statsCli), nil
}

func newSlaveService(store storage.KVStore, ca storage.ChangeApplier, replCli *ctl.DKVClient, pollInterval time.Duration,
	lgr *zap.Logger, statsCli stats.Client) *slaveService {
	ss := &slaveService{store: store, ca: ca, replCli: replCli, lg: lgr, statsCli: statsCli}
	ss.startReplication(pollInterval)
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
	err := iteration.ForEach(func(e *storage.KVEntry) error {
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
	ss.replStop <- struct{}{}
	ss.replTckr.Stop()
	ss.replCli.Close()
	ss.store.Close()
	return nil
}

func (ss *slaveService) startReplication(replPollInterval time.Duration) {
	ss.replTckr = time.NewTicker(replPollInterval)
	latestChngNum, _ := ss.ca.GetLatestAppliedChangeNumber()
	ss.fromChngNum = 1 + latestChngNum
	ss.maxNumChngs = maxNumChangesRepl
	ss.replStop = make(chan struct{})
	slg := ss.lg.Sugar()
	slg.Infof("Replicating changes from change number: %d and polling interval: %s", ss.fromChngNum, replPollInterval.String())
	slg.Sync()
	go ss.pollAndApplyChanges()
}

func (ss *slaveService) pollAndApplyChanges() {
	for {
		select {
		case <-ss.replTckr.C:
			ss.lg.Info("Current replication lag", zap.Uint64("ReplicationLag", ss.replLag))
			ss.statsCli.Gauge("replication.lag", int64(ss.replLag))
			ss.stat.replicationLag.Set(float64(ss.replLag))
			if err := ss.applyChangesFromMaster(ss.maxNumChngs); err != nil {
				ss.lg.Fatal("Unable to retrieve changes from master", zap.Error(err))
			}
		case <-ss.replStop:
			ss.lg.Info("Stopping the change poller")
			break
		}
	}
}

func (ss *slaveService) applyChangesFromMaster(chngsPerBatch uint32) error {
	ss.lg.Info("Retrieving changes from master", zap.Uint64("FromChangeNumber", ss.fromChngNum), zap.Uint32("ChangesPerBatch", chngsPerBatch))
	res, err := ss.replCli.GetChanges(ss.fromChngNum, chngsPerBatch)
	if err == nil {
		if res.Status.Code != 0 {
			// this is an error from DKV master's end
			err = errors.New(res.Status.Message)
		} else {
			if res.MasterChangeNumber < (ss.fromChngNum - 1) {
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
