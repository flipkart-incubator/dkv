package ctl

import (
	"context"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	"log"
	"net"
)

// InProcessDKVClient offers an internal buffer based dial capability to the DKVServer
// Based on the discussion at https://github.com/grpc/grpc-go/issues/906
// This is the best possible current solution for inprocess grpc-client
type InProcessDKVClient struct {
	dKVService serverpb.DKVServer
	lis        *bufconn.Listener
}

func (bc *InProcessDKVClient) Listen() {
	const bufConnSize = 1024 * 1024
	bc.lis = bufconn.Listen(bufConnSize)
	s := grpc.NewServer()
	serverpb.RegisterDKVServer(s, bc.dKVService)
	go func() {
		if err := s.Serve(bc.lis); err != nil {
			log.Fatal(err)
		}
	}()
}

func CreateInProcessDKVClient(dKVService serverpb.DKVServer) *InProcessDKVClient {
	client := &InProcessDKVClient{dKVService: dKVService}
	client.Listen()
	return client
}

func (bc *InProcessDKVClient) GRPCClient() (serverpb.DKVClient, error) {
	conn, err := grpc.DialContext(context.Background(), "bufnet", grpc.WithContextDialer(bc.dialer), grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	return serverpb.NewDKVClient(conn), nil
}

func (bc *InProcessDKVClient) dialer(ctx context.Context, address string) (net.Conn, error) {
	return bc.lis.Dial()
}
