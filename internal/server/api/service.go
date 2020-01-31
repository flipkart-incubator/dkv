package api

import (
	"context"
	"io"

	"github.com/flipkart-incubator/dkv/internal/server/storage"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
	nexus_api "github.com/flipkart-incubator/nexus/pkg/api"
)

// TODO: Check if this needs to be moved to 'pkg' root instead of 'internal'
type DKVService interface {
	io.Closer
	serverpb.DKVServer
}

type standaloneDKVService struct {
	store storage.KVStore
}

func NewStandaloneDKVService(store storage.KVStore) *standaloneDKVService {
	return &standaloneDKVService{store}
}

func (this *standaloneDKVService) Close() error {
	return this.store.Close()
}

func (this *standaloneDKVService) Put(ctx context.Context, putReq *serverpb.PutRequest) (*serverpb.PutResponse, error) {
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

func (this *standaloneDKVService) Get(ctx context.Context, getReq *serverpb.GetRequest) (*serverpb.GetResponse, error) {
	readResult := this.store.Get(getReq.Key)[0]
	return toGetResponse(readResult)
}

func (this *standaloneDKVService) MultiGet(ctx context.Context, multiGetReq *serverpb.MultiGetRequest) (*serverpb.MultiGetResponse, error) {
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

type distributedDKVService struct {
	raftRepl nexus_api.RaftReplicator
}

func NewDistributedDKVService(raftRepl nexus_api.RaftReplicator) *distributedDKVService {
	return &distributedDKVService{raftRepl}
}

func (this *distributedDKVService) Put(ctx context.Context, putReq *serverpb.PutRequest) (*serverpb.PutResponse, error) {
	return nil, nil
}

func (this *distributedDKVService) Get(ctx context.Context, getReq *serverpb.GetRequest) (*serverpb.GetResponse, error) {
	return nil, nil
}

func (this *distributedDKVService) MultiGet(ctx context.Context, multiGetReq *serverpb.MultiGetRequest) (*serverpb.MultiGetResponse, error) {
	return nil, nil
}

func (this *distributedDKVService) Close() error {
	return nil
}
