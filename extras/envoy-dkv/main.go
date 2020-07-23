package main

import (
	"context"
	"fmt"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	api "github.com/envoyproxy/go-control-plane/envoy/api/v2"
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v2"
	"github.com/envoyproxy/go-control-plane/pkg/cache/types"
	"github.com/envoyproxy/go-control-plane/pkg/cache/v2"
	xds "github.com/envoyproxy/go-control-plane/pkg/server/v2"
	"github.com/envoyproxy/go-control-plane/pkg/test/resource/v2"
)

func main() {
	clusterName := "dkv"
	dkvCluster := resource.MakeCluster(resource.Ads, clusterName)
	dkvEndpoints := []types.Resource{
		resource.MakeEndpoint(clusterName, 7071),
		resource.MakeEndpoint(clusterName, 7072),
		resource.MakeEndpoint(clusterName, 7073),
	}
	snapshot := cache.NewSnapshot("1.0", dkvEndpoints, []types.Resource{dkvCluster}, nil, nil, nil)

	snapshotCache := cache.NewSnapshotCache(false, cache.IDHash{}, nil)
	if err := snapshotCache.SetSnapshot("node", snapshot); err != nil {
		fmt.Println(err)
	}

	server := xds.NewServer(context.Background(), snapshotCache, nil)
	grpcServer := grpc.NewServer()
	reflection.Register(grpcServer)
	if lis, err := net.Listen("tcp", "127.0.0.1:9090"); err != nil {
		fmt.Println(err)
	} else {
		discovery.RegisterAggregatedDiscoveryServiceServer(grpcServer, server)
		api.RegisterEndpointDiscoveryServiceServer(grpcServer, server)
		api.RegisterClusterDiscoveryServiceServer(grpcServer, server)
		if err := grpcServer.Serve(lis); err != nil {
			fmt.Println(err)
			// error handling
		}
	}
}
