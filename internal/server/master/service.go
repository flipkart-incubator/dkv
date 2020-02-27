package master

import (
	"context"
	"io"
	"sync"

	"github.com/flipkart-incubator/dkv/internal/server/storage"
	"github.com/flipkart-incubator/dkv/internal/server/sync/raftpb"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
	nexus_api "github.com/flipkart-incubator/nexus/pkg/api"
	"github.com/gogo/protobuf/proto"
)

// A DKVService captures the publicly visible behaviors
// of the key value store.
type DKVService interface {
	io.Closer
	serverpb.DKVServer
	serverpb.DKVReplicationServer
}

type standaloneService struct {
	store              storage.KVStore
	cp                 storage.ChangePropagator
	streamSignals      []chan struct{}
	streamSignalsMutex *sync.Mutex
}

// NewStandaloneService creates a standalone variant of the DKVService
// that works only with the local storage.
func NewStandaloneService(store storage.KVStore, cp storage.ChangePropagator) *standaloneService {
	return &standaloneService{store: store, cp: cp, streamSignalsMutex: &sync.Mutex{}}
}

func (ss *standaloneService) Put(ctx context.Context, putReq *serverpb.PutRequest) (*serverpb.PutResponse, error) {
	if err := ss.store.Put(putReq.Key, putReq.Value); err != nil {
		return &serverpb.PutResponse{Status: newErrorStatus(err)}, err
	}
	return &serverpb.PutResponse{Status: newEmptyStatus()}, nil
}

func (ss *standaloneService) Get(ctx context.Context, getReq *serverpb.GetRequest) (*serverpb.GetResponse, error) {
	if readResults, err := ss.store.Get(getReq.Key); err != nil {
		return &serverpb.GetResponse{Status: newErrorStatus(err), Value: nil}, err
	} else {
		return &serverpb.GetResponse{Status: newEmptyStatus(), Value: readResults[0]}, nil
	}
}

func (ss *standaloneService) MultiGet(ctx context.Context, multiGetReq *serverpb.MultiGetRequest) (*serverpb.MultiGetResponse, error) {
	if readResults, err := ss.store.Get(multiGetReq.Keys...); err != nil {
		return &serverpb.MultiGetResponse{Status: newErrorStatus(err), Values: nil}, err
	} else {
		return &serverpb.MultiGetResponse{Status: newEmptyStatus(), Values: readResults}, nil
	}
}

func (ss *standaloneService) GetChanges(ctx context.Context, getChngsReq *serverpb.GetChangesRequest) (*serverpb.GetChangesResponse, error) {
	if chngs, err := ss.cp.LoadChanges(getChngsReq.FromChangeNumber, int(getChngsReq.MaxNumberOfChanges)); err != nil {
		return &serverpb.GetChangesResponse{Status: newErrorStatus(err)}, err
	} else {
		return &serverpb.GetChangesResponse{Status: newEmptyStatus(), NumberOfChanges: uint32(len(chngs)), Changes: chngs}, nil
	}
}

const NumChangesToLoadPerBatch = 100

func (ss *standaloneService) StreamChanges(strmChngsReq *serverpb.StreamChangesRequest, chngStream serverpb.DKVReplication_StreamChangesServer) error {
	strm_sig := ss.newStreamSignal()
	for chng_num := strmChngsReq.FromChangeNumber; ; {
		select {
		case <-strm_sig:
			break
		default:
			if chngs, err := ss.cp.LoadChanges(chng_num, NumChangesToLoadPerBatch); err != nil {
				return err
			} else {
				for _, chng := range chngs {
					if err := chngStream.Send(chng); err != nil {
						return err
					} else {
						chng_num = chng.ChangeNumber
					}
				}
			}
		}
	}
}

func (ss *standaloneService) newStreamSignal() chan struct{} {
	ss.streamSignalsMutex.Lock()
	defer ss.streamSignalsMutex.Unlock()
	stream_sig := make(chan struct{})
	ss.streamSignals = append(ss.streamSignals, stream_sig)
	return stream_sig
}

func (ss *standaloneService) Close() error {
	for _, stream_sig := range ss.streamSignals {
		stream_sig <- struct{}{}
	}
	return ss.store.Close()
}

type distributedService struct {
	*standaloneService
	raftRepl nexus_api.RaftReplicator
}

// NewDistributedService creates a distributed variant of the DKV service
// that attempts to replicate data across multiple replicas over Nexus.
func NewDistributedService(kvs storage.KVStore, cp storage.ChangePropagator, raftRepl nexus_api.RaftReplicator) *distributedService {
	return &distributedService{NewStandaloneService(kvs, cp), raftRepl}
}

func (ds *distributedService) Put(ctx context.Context, putReq *serverpb.PutRequest) (*serverpb.PutResponse, error) {
	intReq := new(raftpb.InternalRaftRequest)
	intReq.Put = putReq
	if reqBts, err := proto.Marshal(intReq); err != nil {
		return &serverpb.PutResponse{Status: newErrorStatus(err)}, err
	} else {
		if _, err := ds.raftRepl.Replicate(ctx, reqBts); err != nil {
			return &serverpb.PutResponse{Status: newErrorStatus(err)}, err
		}
		return &serverpb.PutResponse{Status: newEmptyStatus()}, nil
	}
}

func (ds *distributedService) Get(ctx context.Context, getReq *serverpb.GetRequest) (*serverpb.GetResponse, error) {
	// TODO: Check for consistency level of GetRequest and process this either via local state or RAFT
	return ds.standaloneService.Get(ctx, getReq)
}

func (ds *distributedService) MultiGet(ctx context.Context, multiGetReq *serverpb.MultiGetRequest) (*serverpb.MultiGetResponse, error) {
	// TODO: Check for consistency level of MultiGetRequest and process this either via local state or RAFT
	return ds.standaloneService.MultiGet(ctx, multiGetReq)
}

func (ds *distributedService) Close() error {
	ds.raftRepl.Stop()
	return nil
}

func newErrorStatus(err error) *serverpb.Status {
	return &serverpb.Status{Code: -1, Message: err.Error()}
}

func newEmptyStatus() *serverpb.Status {
	return &serverpb.Status{Code: 0, Message: ""}
}
