package api

import (
	"context"

	"github.com/flipkart-incubator/dkv/internal/server/storage"
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
		return &serverpb.PutResponse{&serverpb.Status{-1, res.Error.Error()}}, res.Error
	} else {
		return &serverpb.PutResponse{&serverpb.Status{0, ""}}, nil
	}
}

func toGetResponse(readResult *storage.ReadResult) (*serverpb.GetResponse, error) {
	if value, err := readResult.Value, readResult.Error; err != nil {
		return &serverpb.GetResponse{&serverpb.Status{-1, err.Error()}, nil}, err
	} else {
		return &serverpb.GetResponse{&serverpb.Status{0, ""}, value}, nil
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
	return &serverpb.MultiGetResponse{responses}, nil
}

type distributedService struct {
	*standaloneService
	raftRepl nexus_api.RaftReplicator
}

func NewDistributedService(kvs storage.KVStore, raftRepl nexus_api.RaftReplicator) *distributedService {
	return &distributedService{NewStandaloneService(kvs), raftRepl}
}

func (ds *distributedService) Put(ctx context.Context, putReq *serverpb.PutRequest) (*serverpb.PutResponse, error) {
	// TODO: Needs to be marshalled as a union message type
	if req_bts, err := proto.Marshal(putReq); err != nil {
		return &serverpb.PutResponse{&serverpb.Status{-1, err.Error()}}, err
	} else {
		if _, err := ds.raftRepl.Replicate(ctx, req_bts); err != nil {
			return &serverpb.PutResponse{&serverpb.Status{-1, err.Error()}}, err
		} else {
			return &serverpb.PutResponse{&serverpb.Status{0, ""}}, nil
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
