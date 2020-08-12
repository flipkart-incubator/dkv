package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	api "github.com/envoyproxy/go-control-plane/envoy/api/v2"
	endpoint "github.com/envoyproxy/go-control-plane/envoy/api/v2"
	core "github.com/envoyproxy/go-control-plane/envoy/api/v2/core"
	endpointv2 "github.com/envoyproxy/go-control-plane/envoy/api/v2/endpoint"
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v2"
	"github.com/envoyproxy/go-control-plane/pkg/cache/types"
	"github.com/envoyproxy/go-control-plane/pkg/cache/v2"
	xds "github.com/envoyproxy/go-control-plane/pkg/server/v2"
	"github.com/envoyproxy/go-control-plane/pkg/test/resource/v2"
	"github.com/flipkart-incubator/dkv/pkg/ctl"
)

// go build -o envoy-dkv ../envoy-dkv

const clusterName = "dkv"

var (
	dkvMasterAddr string
	zone          string
	lgr           *zap.SugaredLogger
	grpcPort      int
	pollInterval  time.Duration
)

func init() {
	flag.StringVar(&dkvMasterAddr, "dkvMaster", "", "GRPC address of DKV master in host:port format")
	flag.StringVar(&zone, "zone", "", "Zone identifier for the given DKV master node")
	flag.IntVar(&grpcPort, "grpcPort", 9090, "Port for serving GRPC xDS requests")
	flag.DurationVar(&pollInterval, "pollInterval", 5*time.Second, "Polling interval for fetching replicas")
	setupLogger()
}

func main() {
	flag.Parse()
	defer lgr.Sync()

	cli := connectToDKVMaster()
	defer cli.Close()

	snapshotCache := cache.NewSnapshotCache(false, cache.IDHash{}, nil)
	tckr := time.NewTicker(pollInterval)
	defer tckr.Stop()
	go pollForDKVReplicas(tckr, cli, snapshotCache)

	grpcServer, lis := setupXDSService(snapshotCache)
	defer grpcServer.GracefulStop()
	go grpcServer.Serve(lis)

	sig := <-setupSignalHandler()
	lgr.Warnf("[WARN] Caught signal: %v. Shutting down...\n", sig)
}

func setupLogger() {
	loggerConfig := zap.Config{
		Development:   false,
		Encoding:      "console",
		DisableCaller: true,
		Level:         zap.NewAtomicLevelAt(zap.DebugLevel),

		EncoderConfig: zapcore.EncoderConfig{
			TimeKey:        "ts",
			LevelKey:       "level",
			NameKey:        "logger",
			CallerKey:      "caller",
			MessageKey:     "msg",
			LineEnding:     zapcore.DefaultLineEnding,
			EncodeLevel:    zapcore.LowercaseLevelEncoder,
			EncodeTime:     zapcore.ISO8601TimeEncoder,
			EncodeDuration: zapcore.StringDurationEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder,
		},
		OutputPaths:      []string{"stderr"},
		ErrorOutputPaths: []string{"stderr"},
	}

	if lg, err := loggerConfig.Build(); err != nil {
		panic(err)
	} else {
		lgr = lg.Sugar()
	}
}

func setupXDSService(snapshotCache cache.SnapshotCache) (*grpc.Server, net.Listener) {
	server := xds.NewServer(context.Background(), snapshotCache, nil)
	grpcServer := grpc.NewServer()
	reflection.Register(grpcServer)
	discovery.RegisterAggregatedDiscoveryServiceServer(grpcServer, server)
	api.RegisterEndpointDiscoveryServiceServer(grpcServer, server)
	api.RegisterClusterDiscoveryServiceServer(grpcServer, server)
	xdsAddr := fmt.Sprintf("127.0.0.1:%d", grpcPort)
	lis, err := net.Listen("tcp", xdsAddr)
	if err != nil {
		lgr.Panicf("Unable to create listener for xDS GRPC service. Error: %v", err)
		return nil, nil
	}
	lgr.Infof("Successfully setup the xDS GRPC service at %s...", xdsAddr)
	return grpcServer, lis
}

func connectToDKVMaster() *ctl.DKVClient {
	client, err := ctl.NewInSecureDKVClient(dkvMasterAddr)
	if err != nil {
		lgr.Panicf("Unable to connect to DKV master at %s. Error: %v", dkvMasterAddr, err)
	}
	lgr.Infof("Successfully connected to DKV master at %s.", dkvMasterAddr)
	return client
}

func pollForDKVReplicas(tckr *time.Ticker, cli *ctl.DKVClient, snapshotCache cache.SnapshotCache) {
	for range tckr.C {
		repls := cli.GetReplicas(zone)
		if len(repls) > 0 {
			dkvCluster := resource.MakeCluster(resource.Ads, clusterName)
			var dkvReplicas []types.Resource
			for _, repl := range repls {
				replEndPoint := makeEndpoint(clusterName, repl)
				dkvReplicas = append(dkvReplicas, replEndPoint)
			}
			snapshot := cache.NewSnapshot("1.0", dkvReplicas, []types.Resource{dkvCluster}, nil, nil, nil)
			if err := snapshotCache.SetSnapshot("node", snapshot); err != nil {
				lgr.Panicf("Unable to set snapshot. Error: %v", err)
			} else {
				lgr.Info("Successfully updated endpoints with %q", repls)
			}
		} else {
			lgr.Warn("Not received any DKV replicas. Shall try later.")
		}
	}
}

func setupSignalHandler() <-chan os.Signal {
	signals := []os.Signal{syscall.SIGINT, syscall.SIGQUIT, syscall.SIGSTOP, syscall.SIGTERM}
	stopChan := make(chan os.Signal, len(signals))
	signal.Notify(stopChan, signals...)
	return stopChan
}

func makeEndpoint(clusterName, replicaAddr string) *endpoint.ClusterLoadAssignment {
	comps := strings.Split(replicaAddr, ":")
	replicaHost := comps[0]
	replicaPort, _ := strconv.ParseUint(comps[1], 10, 32)

	return &endpoint.ClusterLoadAssignment{
		ClusterName: clusterName,
		Endpoints: []*endpointv2.LocalityLbEndpoints{{
			LbEndpoints: []*endpointv2.LbEndpoint{{
				HostIdentifier: &endpointv2.LbEndpoint_Endpoint{
					Endpoint: &endpointv2.Endpoint{
						Address: &core.Address{
							Address: &core.Address_SocketAddress{
								SocketAddress: &core.SocketAddress{
									Protocol: core.SocketAddress_TCP,
									Address:  replicaHost,
									PortSpecifier: &core.SocketAddress_PortValue{
										PortValue: uint32(replicaPort),
									},
								},
							},
						},
					},
				},
			}},
		}},
	}
}
