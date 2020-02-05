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

func (dkvClnt *DKVClient) Put(key []byte, value []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), Timeout)
	defer cancel()
	putReq := &serverpb.PutRequest{Key: key, Value: value}
	res, err := dkvClnt.dkvCli.Put(ctx, putReq)
	if err != nil {
		return err
	}
	if res.Status.Code != 0 {
		return errors.New(res.Status.Message)
	}
	return nil
}

func (dkvClnt *DKVClient) Get(key []byte) (*serverpb.GetResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), Timeout)
	defer cancel()
	getReq := &serverpb.GetRequest{Key: key}
	return dkvClnt.dkvCli.Get(ctx, getReq)
}

func (dkvClnt *DKVClient) MultiGet(keys ...[]byte) ([]*serverpb.GetResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), Timeout)
	defer cancel()
	getReqs := make([]*serverpb.GetRequest, len(keys))
	for i, key := range keys {
		getReqs[i] = &serverpb.GetRequest{Key: key}
	}
	multiGetReq := &serverpb.MultiGetRequest{GetRequests: getReqs}
	res, err := dkvClnt.dkvCli.MultiGet(ctx, multiGetReq)
	return res.GetResponses, err
}

func (dkvClnt *DKVClient) Close() error {
	return dkvClnt.cliConn.Close()
}
