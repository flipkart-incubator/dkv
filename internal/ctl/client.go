package ctl

import (
	"context"
	"errors"
	"time"

	"github.com/flipkart-incubator/dkv/pkg/serverpb"
	"google.golang.org/grpc"
)

type DKVClient struct {
	cliConn *grpc.ClientConn
	dkvCli  serverpb.DKVClient
}

// TODO: Should these be paramterised ?
const (
	ReadBufSize  = 10 << 30
	WriteBufSize = 10 << 30
	Timeout      = 1 * time.Second
)

func NewInSecureDKVClient(svcAddr string) (*DKVClient, error) {
	if conn, err := grpc.Dial(svcAddr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithReadBufferSize(ReadBufSize), grpc.WithWriteBufferSize(WriteBufSize)); err != nil {
		return nil, err
	} else {
		dkvCli := serverpb.NewDKVClient(conn)
		return &DKVClient{conn, dkvCli}, nil
	}
}

func (this *DKVClient) Put(key []byte, value []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), Timeout)
	defer cancel()
	putReq := &serverpb.PutRequest{key, value}
	res, err := this.dkvCli.Put(ctx, putReq)
	if err != nil {
		return err
	}
	if res.Status.Code != 0 {
		return errors.New(res.Status.Message)
	}
	return nil
}

func (this *DKVClient) Get(key []byte) (*serverpb.GetResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), Timeout)
	defer cancel()
	getReq := &serverpb.GetRequest{key}
	return this.dkvCli.Get(ctx, getReq)
}

func (this *DKVClient) MultiGet(keys ...[]byte) ([]*serverpb.GetResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), Timeout)
	defer cancel()
	getReqs := make([]*serverpb.GetRequest, len(keys))
	for i, key := range keys {
		getReqs[i] = &serverpb.GetRequest{key}
	}
	multiGetReq := &serverpb.MultiGetRequest{getReqs}
	res, err := this.dkvCli.MultiGet(ctx, multiGetReq)
	return res.GetResponses, err
}

func (this *DKVClient) Close() {
	this.cliConn.Close()
}
