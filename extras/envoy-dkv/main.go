package main

import (
	"context"
	"flag"
	"net"
	"os"
	"os/signal"
	"reflect"
	"sort"
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

var (
	dkvMasterAddr string
	zone          string
	lgr           *zap.SugaredLogger
	listenAddr    string
	pollInterval  time.Duration
	clusterName   string
	nodeName      string
)

func init() {
	flag.StringVar(&dkvMasterAddr, "dkvMaster", "", "Comma separated values of DKV master addresses in host:port format")
	flag.StringVar(&zone, "zone", "", "Zone identifier for the given DKV master node")
	flag.DurationVar(&pollInterval, "pollInterval", 5*time.Second, "Polling interval for fetching replicas")
	flag.StringVar(&clusterName, "clusterName", "dkv-demo", "Local service cluster name where Envoy is running")
	flag.StringVar(&nodeName, "nodeName", "demo", "Local service node name where Envoy is running")
	flag.StringVar(&listenAddr, "listenAddr", "127.0.0.1:9090", "Address to bind the xDS GRPC service")
	setupLogger()
}

func main() {
	flag.Parse()
	defer lgr.Sync()

	clis := connectToDKVMasters()
	defer closeClients(clis)

	snapshotCache := cache.NewSnapshotCache(false, cache.IDHash{}, nil)
	grpcServer, lis := setupXDSService(snapshotCache)
	//defer grpcServer.GracefulStop()
	defer grpcServer.Stop()
	go grpcServer.Serve(lis)

	tckr := time.NewTicker(pollInterval)
	defer tckr.Stop()
	go pollForDKVReplicas(tckr, snapshotCache, clis...)

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
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		lgr.Panicf("Unable to create listener for xDS GRPC service. Error: %v", err)
		return nil, nil
	}
	lgr.Infof("Successfully setup the xDS GRPC service at %s...", listenAddr)
	return grpcServer, lis
}

func connectToDKVMasters() []*ctl.DKVClient {
	var clis []*ctl.DKVClient
	dkvMasters := strings.Split(dkvMasterAddr, ",")
	for _, dkvMaster := range dkvMasters {
		dkvMaster = strings.TrimSpace(dkvMaster)
		if client, err := ctl.NewInSecureDKVClient(dkvMaster); err != nil {
			lgr.Panicf("Unable to connect to DKV master at %s. Error: %v", dkvMaster, err)
		} else {
			lgr.Infof("Successfully connected to DKV master at %s.", dkvMaster)
			clis = append(clis, client)
		}
	}
	return clis
}

func closeClients(clis []*ctl.DKVClient) {
	for _, cli := range clis {
		cli.Close()
	}
}

func pollForDKVReplicas(tckr *time.Ticker, snapshotCache cache.SnapshotCache, clis ...*ctl.DKVClient) {
	snapVersion := 0
	dkvClusters := []types.Resource{resource.MakeCluster(resource.Ads, clusterName)}
	snapshot := cache.NewSnapshot("", nil, dkvClusters, nil, nil, nil)
	var oldRepls []string
	for range tckr.C {
		var newRepls []string
		for _, cli := range clis {
			newRepls = append(newRepls, cli.GetReplicas(zone)...)
		}
		sort.Sort(sort.StringSlice(newRepls)) // Sorting for deterministic comparison
		if !reflect.DeepEqual(oldRepls, newRepls) {
			replEndPoints := []types.Resource{makeEndpoint(clusterName, newRepls...)}
			snapVersion++
			snapshot.Resources[types.Endpoint] = cache.NewResources(strconv.Itoa(snapVersion), replEndPoints)
			if err := snapshotCache.SetSnapshot(nodeName, snapshot); err != nil {
				lgr.Panicf("Unable to set snapshot. Error: %v", err)
			} else {
				lgr.Infof("Successfully updated endpoints with %q", newRepls)
				oldRepls = newRepls
			}
		}
	}
}

func setupSignalHandler() <-chan os.Signal {
	signals := []os.Signal{syscall.SIGINT, syscall.SIGQUIT, syscall.SIGSTOP, syscall.SIGTERM}
	stopChan := make(chan os.Signal, len(signals))
	signal.Notify(stopChan, signals...)
	return stopChan
}

func makeEndpoint(clusterName string, replicaAddrs ...string) *endpoint.ClusterLoadAssignment {
	var endpoints []*endpointv2.LbEndpoint
	for _, replicaAddr := range replicaAddrs {
		comps := strings.Split(replicaAddr, ":")
		replicaHost := comps[0]
		replicaPort, _ := strconv.ParseUint(comps[1], 10, 32)
		endpoints = append(endpoints, &endpointv2.LbEndpoint{
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
		})
	}

	return &endpoint.ClusterLoadAssignment{
		ClusterName: clusterName,
		Endpoints: []*endpointv2.LocalityLbEndpoints{{
			//Locality:
			LbEndpoints: endpoints,
		}},
	}
}
