package main

import (
	"context"
	"log"
	"time"

	"github.com/flipkart-incubator/dkv/pkg/serverpb"
	"google.golang.org/grpc"
)

const dkvAddr = "localhost:8080"

func main() {
	conn, err := grpc.Dial(dkvAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Unable to connect. Error: %v", err)
	}
	defer conn.Close()
	dkvCli := serverpb.NewDKVClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	putReq := &serverpb.PutRequest{[]byte("go"), []byte("rocks")}
	if res, err := dkvCli.Put(ctx, putReq); err != nil || res.Status.Code != 0 {
		log.Fatalf("Unable to perform PUT. Message: %s, Error: %v", res.Status.Message, err)
	}
	getReq := &serverpb.GetRequest{[]byte("go")}
	if res, err := dkvCli.Get(ctx, getReq); err != nil || res.Status.Code != 0 {
		log.Fatalf("Unable to perform GET. Message: %s, Error: %v", res.Status.Message, err)
	} else {
		log.Println(string(res.Value))
	}
}
