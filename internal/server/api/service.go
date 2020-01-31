package api

import (
	"context"

	"github.com/flipkart-incubator/dkv/internal/server/storage"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
	nexus_api "github.com/flipkart-incubator/nexus/pkg/api"
)

type standaloneService struct {
	store storage.KVStore
}

func NewStandaloneService(store storage.KVStore) *standaloneService {
	return &standaloneService{store}
}

func (this *standaloneService) Put(ctx context.Context, putReq *serverpb.PutRequest) (*serverpb.PutResponse, error) {
	if res := this.store.Put(putReq.Key, putReq.Value); res.Error != nil {
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

func (this *standaloneService) Get(ctx context.Context, getReq *serverpb.GetRequest) (*serverpb.GetResponse, error) {
	readResult := this.store.Get(getReq.Key)[0]
	return toGetResponse(readResult)
}

func (this *standaloneService) MultiGet(ctx context.Context, multiGetReq *serverpb.MultiGetRequest) (*serverpb.MultiGetResponse, error) {
	numReqs := len(multiGetReq.GetRequests)
	keys := make([][]byte, numReqs)
	for i, getReq := range multiGetReq.GetRequests {
		keys[i] = getReq.Key
	}
	readResults := this.store.Get(keys...)
	responses := make([]*serverpb.GetResponse, len(readResults))
	for i, readResult := range readResults {
		responses[i], _ = toGetResponse(readResult)
	}
	return &serverpb.MultiGetResponse{responses}, nil
}

type distributedService struct {
	raftRepl nexus_api.RaftReplicator
}

func NewDistributedService(raftRepl nexus_api.RaftReplicator) *distributedService {
	return &distributedService{raftRepl}
}

func (this *distributedService) Put(ctx context.Context, putReq *serverpb.PutRequest) (*serverpb.PutResponse, error) {
	return nil, nil
}

func (this *distributedService) Get(ctx context.Context, getReq *serverpb.GetRequest) (*serverpb.GetResponse, error) {
	return nil, nil
}

func (this *distributedService) MultiGet(ctx context.Context, multiGetReq *serverpb.MultiGetRequest) (*serverpb.MultiGetResponse, error) {
	return nil, nil
}
