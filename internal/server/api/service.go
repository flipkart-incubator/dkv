package api

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/flipkart-incubator/dkv/internal/server/storage"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
	"google.golang.org/grpc"
)

type DKVService struct {
	port  uint
	store storage.KVStore
}

func NewDKVService(port uint, store storage.KVStore) *DKVService {
	return &DKVService{port, store}
}

func (this *DKVService) ListenAndServe() {
	this.NewGRPCServer().Serve(this.NewListener())
}

func (this *DKVService) NewGRPCServer() *grpc.Server {
	grpcServer := grpc.NewServer()
	serverpb.RegisterDKVServer(grpcServer, this)
	return grpcServer
}

func (this *DKVService) NewListener() net.Listener {
	if lis, err := net.Listen("tcp", fmt.Sprintf(":%d", this.port)); err != nil {
		log.Fatalf("failed to listen: %v", err)
		return nil
	} else {
		return lis
	}
}

func (this *DKVService) Close() error {
	return this.store.Close()
}

func (this *DKVService) Put(ctx context.Context, putReq *serverpb.PutRequest) (*serverpb.PutResponse, error) {
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

func (this *DKVService) Get(ctx context.Context, getReq *serverpb.GetRequest) (*serverpb.GetResponse, error) {
	readResult := this.store.Get(getReq.Key)[0]
	return toGetResponse(readResult)
}

func (this *DKVService) MultiGet(ctx context.Context, multiGetReq *serverpb.MultiGetRequest) (*serverpb.MultiGetResponse, error) {
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
