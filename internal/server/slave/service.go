package slave

import (
	"context"
	"errors"
	"flag"
	"io"
	"log"
	"time"

	"github.com/flipkart-incubator/dkv/internal/server/storage"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
	"google.golang.org/grpc"
)

type DKVService interface {
	io.Closer
	serverpb.DKVServer
}

type dkvSlaveService struct {
	store       storage.KVStore
	ca          storage.ChangeApplier
	replCli     serverpb.DKVReplicationClient
	replTckr    *time.Ticker
	replStop    chan struct{}
	replLag     uint64
	fromChngNum uint64
	maxNumChngs uint32
}

var (
	masterAddr           string
	replTimeout          time.Duration
	replPollInterval     time.Duration
	replTimeoutSecs      uint
	replPollIntervalSecs uint
)

const (
	grpcReadBufSize   = 10 << 30
	grpcWriteBufSize  = 10 << 30
	MaxNumChangesRepl = 100 // TODO: check if this needs to be exposed as a flag
)

func init() {
	flag.StringVar(&masterAddr, "replMasterAddr", "", "GRPC service addr of DKV Master for replication [host:port]")
	flag.UintVar(&replTimeoutSecs, "replTimeout", 10, "Replication timeout in seconds")
	flag.UintVar(&replPollIntervalSecs, "replPollInterval", 1, "Interval between successive polls in seconds")
}

func NewService(store storage.KVStore, ca storage.ChangeApplier) (*dkvSlaveService, error) {
	if err := validateFlags(); err != nil {
		return nil, err
	}
	if conn, err := grpc.Dial(masterAddr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithReadBufferSize(grpcReadBufSize), grpc.WithWriteBufferSize(grpcWriteBufSize)); err != nil {
		return nil, err
	} else {
		dkvReplCli := serverpb.NewDKVReplicationClient(conn)
		return newSlaveService(store, ca, dkvReplCli, replPollInterval, replTimeout), nil
	}
}

func newSlaveService(store storage.KVStore, ca storage.ChangeApplier, dkvReplCli serverpb.DKVReplicationClient, pollInterval, timeout time.Duration) *dkvSlaveService {
	dss := &dkvSlaveService{store: store, ca: ca, replCli: dkvReplCli}
	dss.startReplPoller(pollInterval, timeout)
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
	dss.replTckr.Stop()
	dss.replStop <- struct{}{}
	return dss.store.Close()
}

func (dss *dkvSlaveService) startReplPoller(replPollInterval, replTimeout time.Duration) {
	dss.replTckr = time.NewTicker(replPollInterval)
	dss.fromChngNum = 1 + dss.ca.GetLatestChangeNumber()
	dss.maxNumChngs = MaxNumChangesRepl
	dss.replStop = make(chan struct{})
	go dss.pollAndApplyChanges(replTimeout)
}

func (dss *dkvSlaveService) pollAndApplyChanges(replTimeout time.Duration) {
	for {
		select {
		case <-dss.replTckr.C:
			if err := dss.applyChangesFromMaster(replTimeout); err != nil {
				log.Fatal(err)
			}
		case <-dss.replStop:
			break
		}
	}
}

func (dss *dkvSlaveService) applyChangesFromMaster(replTimeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), replTimeout)
	defer cancel()
	getChngsReq := &serverpb.GetChangesRequest{FromChangeNumber: dss.fromChngNum, MaxNumberOfChanges: dss.maxNumChngs}
	if res, err := dss.replCli.GetChanges(ctx, getChngsReq); err != nil {
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

func validateFlags() error {
	if masterAddr == "" {
		return errors.New("GRPC service address of DKV Master is missing")
	}

	if replTimeoutSecs == 0 {
		return errors.New("Replication timeout in seconds must be a positive integer")
	} else {
		replTimeout = time.Duration(replTimeoutSecs) * time.Second
	}

	if replPollIntervalSecs == 0 {
		return errors.New("Replication polling interval in seconds must be a positive integer")
	} else {
		replPollInterval = time.Duration(replPollIntervalSecs) * time.Second
	}

	return nil
}

func newErrorStatus(err error) *serverpb.Status {
	return &serverpb.Status{Code: -1, Message: err.Error()}
}

func newEmptyStatus() *serverpb.Status {
	return &serverpb.Status{Code: 0, Message: ""}
}
