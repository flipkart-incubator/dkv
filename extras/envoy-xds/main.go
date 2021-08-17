package main

// go build -o envoy-xds main.go

import (
	"context"
	"flag"
	"github.com/flipkart-incubator/dkv/extras/envoy-xds/pkg"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	api "github.com/envoyproxy/go-control-plane/envoy/api/v2"
	cluster "github.com/envoyproxy/go-control-plane/envoy/api/v2"
	listener "github.com/envoyproxy/go-control-plane/envoy/api/v2"
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v2"
	"github.com/envoyproxy/go-control-plane/pkg/cache/v2"
	xds "github.com/envoyproxy/go-control-plane/pkg/server/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var (
	configPath      string
	listenAddr      string
	pollInterval    time.Duration
	discoveryClient *pkg.DiscoveryClient
)

func init() {
	flag.StringVar(&configPath, "config", "", "Path to the config JSON file with Envoy xDS configuration")
	flag.StringVar(&listenAddr, "listenAddr", "", "Address (host:port) to bind for Envoy xDS")
	flag.DurationVar(&pollInterval, "pollInterval", 5*time.Second, "Polling interval for checking config updates")
}

func main() {
	flag.Parse()
	validateFlags()

	snapshotCache := cache.NewSnapshotCache(false, cache.IDHash{}, nil)
	grpcServer, lis := setupXDSService(snapshotCache)
	//defer grpcServer.GracefulStop()
	defer grpcServer.Stop()
	go grpcServer.Serve(lis)

	discoveryClient = pkg.InitServiceDiscoveryClient(configPath)
	tckr := time.NewTicker(pollInterval)
	defer tckr.Stop()
	go pollForConfigUpdates(tckr, snapshotCache)

	sig := <-setupSignalHandler()
	log.Printf("[WARN] Caught signal: %v. Shutting down...\n", sig)
}

func pollForConfigUpdates(tckr *time.Ticker, snapshotCache cache.SnapshotCache) {
	snapVersion := uint(1)
	for range tckr.C {
		envoyConfig, err := discoveryClient.GetEnvoyConfig()
		if err != nil {
			log.Printf("Unable to get cluster info: Error: %v \n", err)
		} else {
			if err = envoyConfig.ComputeAndSetSnapshot(snapVersion, snapshotCache); err != nil {
				log.Printf("Unable to compute and set snapshot. Error: %v \n", err)
			} else {
				snapVersion++
			}
		}
	}
}

func validateFlags() {
	if configPath == "" || strings.TrimSpace(configPath) == "" {
		flag.Usage()
		os.Exit(1)
	}

	if listenAddr == "" || strings.TrimSpace(listenAddr) == "" {
		flag.Usage()
		os.Exit(1)
	}
}

func setupXDSService(snapshotCache cache.SnapshotCache) (*grpc.Server, net.Listener) {
	server := xds.NewServer(context.Background(), snapshotCache, nil)
	grpcServer := grpc.NewServer()
	reflection.Register(grpcServer)
	discovery.RegisterAggregatedDiscoveryServiceServer(grpcServer, server)
	api.RegisterEndpointDiscoveryServiceServer(grpcServer, server)
	listener.RegisterListenerDiscoveryServiceServer(grpcServer, server)
	cluster.RegisterClusterDiscoveryServiceServer(grpcServer, server)
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Panicf("Unable to create listener for xDS GRPC service. Error: %v", err)
		return nil, nil
	}
	log.Printf("Successfully setup the xDS GRPC service at %s... \n", listenAddr)
	return grpcServer, lis
}

func setupSignalHandler() <-chan os.Signal {
	signals := []os.Signal{syscall.SIGINT, syscall.SIGQUIT, syscall.SIGSTOP, syscall.SIGTERM}
	stopChan := make(chan os.Signal, len(signals))
	signal.Notify(stopChan, signals...)
	return stopChan
}
