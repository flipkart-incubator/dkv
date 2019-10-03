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

func (this *DKVService) Serve() {
	if lis, err := net.Listen("tcp", fmt.Sprintf(":%d", this.port)); err != nil {
		log.Fatalf("failed to listen: %v", err)
	} else {
		grpcServer := grpc.NewServer()
		serverpb.RegisterDKVServer(grpcServer, this)
		grpcServer.Serve(lis)
	}
}

func (this *DKVService) Put(ctx context.Context, putReq *serverpb.PutRequest) (*serverpb.PutResponse, error) {
	if err := this.store.Put(putReq.Key, putReq.Value); err != nil {
		return &serverpb.PutResponse{&serverpb.Status{-1, err.Error()}}, err
	} else {
		return &serverpb.PutResponse{&serverpb.Status{0, ""}}, nil
	}
}

func (this *DKVService) Get(ctx context.Context, getReq *serverpb.GetRequest) (*serverpb.GetResponse, error) {
	if value, err := this.store.Get(getReq.Key); err != nil {
		return &serverpb.GetResponse{&serverpb.Status{-1, err.Error()}, nil}, err
	} else {
		return &serverpb.GetResponse{&serverpb.Status{0, ""}, value}, nil
	}
}
