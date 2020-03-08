package slave

import (
	"context"
	"errors"
	"io"
	"log"
	"time"

	"github.com/flipkart-incubator/dkv/internal/ctl"
	"github.com/flipkart-incubator/dkv/internal/server/storage"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
)

type DKVService interface {
	io.Closer
	serverpb.DKVServer
}

type dkvSlaveService struct {
	store       storage.KVStore
	ca          storage.ChangeApplier
	replCli     *ctl.DKVClient
	replTckr    *time.Ticker
	replStop    chan struct{}
	replLag     uint64
	fromChngNum uint64
	maxNumChngs uint32
}

const (
	MaxNumChangesRepl = 100 // TODO: check if this needs to be exposed as a flag
)

func NewService(store storage.KVStore, ca storage.ChangeApplier, replCli *ctl.DKVClient, replPollIntervalSecs uint) (*dkvSlaveService, error) {
	if replPollIntervalSecs == 0 || replCli == nil || store == nil || ca == nil {
		return nil, errors.New("Invalid args. Params `store`, `ca`, `replCli` and `replPollIntervalSecs` are all mandatory.")
	} else {
		replPollInterval := time.Duration(replPollIntervalSecs) * time.Second
		return newSlaveService(store, ca, replCli, replPollInterval), nil
	}
}

func newSlaveService(store storage.KVStore, ca storage.ChangeApplier, replCli *ctl.DKVClient, pollInterval time.Duration) *dkvSlaveService {
	dss := &dkvSlaveService{store: store, ca: ca, replCli: replCli}
	dss.startReplication(pollInterval)
	return dss
}

func (dss *dkvSlaveService) Put(ctx context.Context, putReq *serverpb.PutRequest) (*serverpb.PutResponse, error) {
	return nil, errors.New("DKV slave service does not support keyspace mutations")
}

func (dss *dkvSlaveService) Get(ctx context.Context, getReq *serverpb.GetRequest) (*serverpb.GetResponse, error) {
	if readResults, err := dss.store.Get(getReq.Key); err != nil {
		return &serverpb.GetResponse{Status: newErrorStatus(err), Value: nil}, err
	} else {
		return &serverpb.GetResponse{Status: newEmptyStatus(), Value: readResults[0]}, nil
	}
}

func (dss *dkvSlaveService) MultiGet(ctx context.Context, multiGetReq *serverpb.MultiGetRequest) (*serverpb.MultiGetResponse, error) {
	if readResults, err := dss.store.Get(multiGetReq.Keys...); err != nil {
		return &serverpb.MultiGetResponse{Status: newErrorStatus(err), Values: nil}, err
	} else {
		return &serverpb.MultiGetResponse{Status: newEmptyStatus(), Values: readResults}, nil
	}
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
	latest_chng_num, _ := dss.ca.GetLatestAppliedChangeNumber()
	dss.fromChngNum = 1 + latest_chng_num
	dss.maxNumChngs = MaxNumChangesRepl
	dss.replStop = make(chan struct{})
	go dss.pollAndApplyChanges()
}

func (dss *dkvSlaveService) pollAndApplyChanges() {
	for {
		select {
		case <-dss.replTckr.C:
			if err := dss.applyChangesFromMaster(); err != nil {
				log.Fatal(err)
			}
		case <-dss.replStop:
			break
		}
	}
}

func (dss *dkvSlaveService) applyChangesFromMaster() error {
	if res, err := dss.replCli.GetChanges(dss.fromChngNum, dss.maxNumChngs); err != nil {
		return err
	} else {
		if res.Status.Code != 0 {
			return errors.New(res.Status.Message)
		}
		return dss.applyChanges(res)
	}
}

func (dss *dkvSlaveService) applyChanges(chngsRes *serverpb.GetChangesResponse) error {
	if chngsRes.NumberOfChanges > 0 {
		act_chng_num, err := dss.ca.SaveChanges(chngsRes.Changes)
		dss.fromChngNum = act_chng_num + 1
		dss.replLag = chngsRes.MasterChangeNumber - act_chng_num
		return err
	}
	return nil
}

func newErrorStatus(err error) *serverpb.Status {
	return &serverpb.Status{Code: -1, Message: err.Error()}
}

func newEmptyStatus() *serverpb.Status {
	return &serverpb.Status{Code: 0, Message: ""}
}
