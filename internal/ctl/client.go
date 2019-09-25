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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
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

func (this *DKVClient) Get(key []byte) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	getReq := &serverpb.GetRequest{key}
	res, err := this.dkvCli.Get(ctx, getReq)
	if err != nil {
		return nil, err
	}
	if res.Status.Code != 0 {
		return nil, errors.New(res.Status.Message)
	}
	return res.Value, nil
}

func (this *DKVClient) Close() {
	this.cliConn.Close()
}
