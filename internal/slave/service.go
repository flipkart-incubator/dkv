package slave

import (
	"context"
	"errors"
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

type dkvSlaveService struct {
	store       storage.KVStore
	ca          storage.ChangeApplier
	lg          *zap.Logger
	statsCli    stats.Client
	replCli     *ctl.DKVClient
	replTckr    *time.Ticker
	replStop    chan struct{}
	replLag     uint64
	fromChngNum uint64
	maxNumChngs uint32
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
	lgr *zap.Logger, statsCli stats.Client) *dkvSlaveService {
	dss := &dkvSlaveService{store: store, ca: ca, replCli: replCli, lg: lgr, statsCli: statsCli}
	dss.startReplication(pollInterval)
	return dss
}

func (dss *dkvSlaveService) Put(ctx context.Context, putReq *serverpb.PutRequest) (*serverpb.PutResponse, error) {
	return nil, errors.New("DKV slave service does not support keyspace mutations")
}

func (dss *dkvSlaveService) Delete(ctx context.Context, delReq *serverpb.DeleteRequest) (*serverpb.DeleteResponse, error) {
	return nil, errors.New("DKV slave service does not support keyspace mutations")
}

func (dss *dkvSlaveService) Get(ctx context.Context, getReq *serverpb.GetRequest) (*serverpb.GetResponse, error) {
	readResults, err := dss.store.Get(getReq.Key)
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

func (dss *dkvSlaveService) MultiGet(ctx context.Context, multiGetReq *serverpb.MultiGetRequest) (*serverpb.MultiGetResponse, error) {
	readResults, err := dss.store.Get(multiGetReq.Keys...)
	res := &serverpb.MultiGetResponse{Status: newEmptyStatus()}
	if err != nil {
		res.Status = newErrorStatus(err)
	} else {
		res.KeyValues = readResults
	}
	return res, err
}

func (dss *dkvSlaveService) Iterate(iterReq *serverpb.IterateRequest, dkvIterSrvr serverpb.DKV_IterateServer) error {
	iteration := storage.NewIteration(dss.store, iterReq)
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

func (dss *dkvSlaveService) Close() error {
	dss.replStop <- struct{}{}
	dss.replTckr.Stop()
	dss.replCli.Close()
	dss.store.Close()
	return nil
}

func (dss *dkvSlaveService) startReplication(replPollInterval time.Duration) {
	dss.replTckr = time.NewTicker(replPollInterval)
	latestChngNum, _ := dss.ca.GetLatestAppliedChangeNumber()
	dss.fromChngNum = 1 + latestChngNum
	dss.maxNumChngs = maxNumChangesRepl
	dss.replStop = make(chan struct{})
	slg := dss.lg.Sugar()
	slg.Infof("Replicating changes from change number: %d and polling interval: %s", dss.fromChngNum, replPollInterval.String())
	slg.Sync()
	go dss.pollAndApplyChanges()
}

func (dss *dkvSlaveService) pollAndApplyChanges() {
	for {
		select {
		case <-dss.replTckr.C:
			dss.lg.Info("Current replication lag", zap.Uint64("ReplicationLag", dss.replLag))
			dss.statsCli.Gauge("replication.lag", int64(dss.replLag))
			if err := dss.applyChangesFromMaster(dss.maxNumChngs); err != nil {
				dss.lg.Fatal("Unable to retrieve changes from master", zap.Error(err))
			}
		case <-dss.replStop:
			dss.lg.Info("Stopping the change poller")
			break
		}
	}
}

func (dss *dkvSlaveService) applyChangesFromMaster(chngsPerBatch uint32) error {
	dss.lg.Info("Retrieving changes from master", zap.Uint64("FromChangeNumber", dss.fromChngNum), zap.Uint32("ChangesPerBatch", chngsPerBatch))
	res, err := dss.replCli.GetChanges(dss.fromChngNum, chngsPerBatch)
	if err == nil {
		if res.Status.Code != 0 {
			// this is an error from DKV master's end
			err = errors.New(res.Status.Message)
		} else {
			if res.MasterChangeNumber < (dss.fromChngNum - 1) {
				err = errors.New("change number of the master node can not be lesser than the change number of the slave node")
			} else {
				err = dss.applyChanges(res)
			}
		}
	} else {
		if strings.Contains(err.Error(), "ResourceExhausted") {
			// This is an error from DKV slave's end where the GRPC
			// receive buffer is exhausted. We now attempt to retrieve
			// the changes by halving the batch size. We try this until
			// the batch size can no longer be halved (= 0) and then
			// give up with an error. In such cases, this method is
			// invoked recursively utmost log2[dss.maxNumChngs] times.
			dss.lg.Warn("GetChanges call exceeded resource limits", zap.Error(err))
			if newMaxNumChngs := chngsPerBatch >> 1; newMaxNumChngs > 0 {
				dss.lg.Warn("Retrieving smaller batches of changes", zap.Uint32("before", chngsPerBatch), zap.Uint32("after", newMaxNumChngs))
				err = dss.applyChangesFromMaster(newMaxNumChngs)
			} else {
				err = errors.New("unable to retrieve changes from master due to GRPC resource exhaustion on slave")
			}
		}
	}
	return err
}

func (dss *dkvSlaveService) applyChanges(chngsRes *serverpb.GetChangesResponse) error {
	if chngsRes.NumberOfChanges > 0 {
		dss.lg.Info("Applying the changes received from master", zap.Uint32("NumberOfChanges", chngsRes.NumberOfChanges))
		actChngNum, err := dss.ca.SaveChanges(chngsRes.Changes)
		if err != nil {
			return err
		}
		dss.fromChngNum = actChngNum + 1
		dss.lg.Info("Changes applied to local storage", zap.Uint64("FromChangeNumber", dss.fromChngNum))
		dss.replLag = chngsRes.MasterChangeNumber - actChngNum
	} else {
		dss.lg.Info("Not received any changes from master")
	}
	return nil
}

func newErrorStatus(err error) *serverpb.Status {
	return &serverpb.Status{Code: -1, Message: err.Error()}
}

func newEmptyStatus() *serverpb.Status {
	return &serverpb.Status{Code: 0, Message: ""}
}
