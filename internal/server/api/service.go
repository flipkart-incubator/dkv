package api

import (
	"context"

	"github.com/flipkart-incubator/dkv/internal/server/storage"
	"github.com/flipkart-incubator/dkv/internal/server/sync/raftpb"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
	nexus_api "github.com/flipkart-incubator/nexus/pkg/api"
	"github.com/gogo/protobuf/proto"
)

type standaloneService struct {
	store storage.KVStore
}

func NewStandaloneService(store storage.KVStore) *standaloneService {
	return &standaloneService{store}
}

func (ss *standaloneService) Put(ctx context.Context, putReq *serverpb.PutRequest) (*serverpb.PutResponse, error) {
	if res := ss.store.Put(putReq.Key, putReq.Value); res.Error != nil {
		return &serverpb.PutResponse{Status: newErrorStatus(res.Error)}, res.Error
	} else {
		return &serverpb.PutResponse{Status: newEmptyStatus()}, nil
	}
}

func toGetResponse(readResult *storage.ReadResult) (*serverpb.GetResponse, error) {
	if value, err := readResult.Value, readResult.Error; err != nil {
		return &serverpb.GetResponse{Status: newErrorStatus(err), Value: nil}, err
	} else {
		return &serverpb.GetResponse{Status: newEmptyStatus(), Value: value}, nil
	}
}

func (ss *standaloneService) Get(ctx context.Context, getReq *serverpb.GetRequest) (*serverpb.GetResponse, error) {
	readResult := ss.store.Get(getReq.Key)[0]
	return toGetResponse(readResult)
}

func (ss *standaloneService) MultiGet(ctx context.Context, multiGetReq *serverpb.MultiGetRequest) (*serverpb.MultiGetResponse, error) {
	numReqs := len(multiGetReq.GetRequests)
	keys := make([][]byte, numReqs)
	for i, getReq := range multiGetReq.GetRequests {
		keys[i] = getReq.Key
	}
	readResults := ss.store.Get(keys...)
	responses := make([]*serverpb.GetResponse, len(readResults))
	for i, readResult := range readResults {
		responses[i], _ = toGetResponse(readResult)
	}
	return &serverpb.MultiGetResponse{GetResponses: responses}, nil
}

type distributedService struct {
	*standaloneService
	raftRepl nexus_api.RaftReplicator
}

func NewDistributedService(kvs storage.KVStore, raftRepl nexus_api.RaftReplicator) *distributedService {
	return &distributedService{NewStandaloneService(kvs), raftRepl}
}

func (ds *distributedService) Put(ctx context.Context, putReq *serverpb.PutRequest) (*serverpb.PutResponse, error) {
	int_req := new(raftpb.InternalRaftRequest)
	int_req.Put = putReq
	if req_bts, err := proto.Marshal(int_req); err != nil {
		return &serverpb.PutResponse{Status: newErrorStatus(err)}, err
	} else {
		if _, err := ds.raftRepl.Replicate(ctx, req_bts); err != nil {
			return &serverpb.PutResponse{Status: newErrorStatus(err)}, err
		} else {
			return &serverpb.PutResponse{Status: newEmptyStatus()}, nil
		}
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

func newErrorStatus(err error) *serverpb.Status {
	return &serverpb.Status{Code: -1, Message: err.Error()}
}

func newEmptyStatus() *serverpb.Status {
	return &serverpb.Status{Code: 0, Message: ""}
}
