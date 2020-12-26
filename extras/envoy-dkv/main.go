package main

// go build -o envoy-dkv main.go

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	api "github.com/envoyproxy/go-control-plane/envoy/api/v2"
	cluster "github.com/envoyproxy/go-control-plane/envoy/api/v2"
	endpoint "github.com/envoyproxy/go-control-plane/envoy/api/v2"
	listener "github.com/envoyproxy/go-control-plane/envoy/api/v2"
	core "github.com/envoyproxy/go-control-plane/envoy/api/v2/core"
	endpointv2 "github.com/envoyproxy/go-control-plane/envoy/api/v2/endpoint"
	listenerv2 "github.com/envoyproxy/go-control-plane/envoy/api/v2/listener"
	route "github.com/envoyproxy/go-control-plane/envoy/api/v2/route"
	hcm "github.com/envoyproxy/go-control-plane/envoy/config/filter/network/http_connection_manager/v2"
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v2"
	"github.com/envoyproxy/go-control-plane/pkg/cache/types"
	"github.com/envoyproxy/go-control-plane/pkg/cache/v2"
	xds "github.com/envoyproxy/go-control-plane/pkg/server/v2"
	"github.com/golang/protobuf/ptypes"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var (
	configPath   string
	listenAddr   string
	pollInterval time.Duration
)

const (
	cacheSize   = 50
	envoyRouter = "envoy.router"
	envoyHCM    = "envoy.http_connection_manager"
	appIdKey    = "appId"
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

	tckr := time.NewTicker(pollInterval)
	defer tckr.Stop()
	go pollForConfigUpdates(tckr, snapshotCache)

	sig := <-setupSignalHandler()
	log.Printf("[WARN] Caught signal: %v. Shutting down...\n", sig)
}

func pollForConfigUpdates(tckr *time.Ticker, snapshotCache cache.SnapshotCache) {
	lastModTime := int64(0)
	snapVersion := uint(1)
	for range tckr.C {

		fi, err := os.Stat(configPath)
		if err != nil {
			log.Panicf("Unable to stat given config file: %s. Error: %v", configPath, err)
		}

		if fi.IsDir() {
			log.Panicf("Given config path: %s points to a directory. This must be a file.", configPath)
		}

		currModTime := fi.ModTime().Unix()
		if currModTime <= lastModTime {
			continue
		}

		config, err := ioutil.ReadFile(configPath)
		if err != nil {
			log.Panicf("Unable to read given config file: %s. Error: %v", configPath, err)
		}

		kvs := make(map[string]interface{})
		err = json.Unmarshal(config, &kvs)
		if err != nil {
			log.Panicf("Unable to convert the contents of config file: %s into JSON. Error: %v", configPath, err)
		}

		appId, igs, err := readAppIdInstanceGroups(kvs)
		if err != nil {
			log.Panicf("Unable to fetch instance groups. Error: %v", err)
		}

		if snap, err := computeSnapshot(appId, kvs, snapVersion); err != nil {
			log.Panicf("Unable to compute snapshot. Error: %v", err)
		} else {
			for _, nodeName := range igs {
				if err := snapshotCache.SetSnapshot(nodeName, snap); err != nil {
					log.Panicf("Unable to set snapshot. Error: %v", err)
				}
			}
			snapVersion++
		}
	}
}

func makeGRPCListener(shrd string, hp hostPort, domains ...string) *listener.Listener {
	vhosts := make([]*route.VirtualHost, len(domains))
	for i, domain := range domains {
		vhosts[i] = &route.VirtualHost{
			Name:    domain,
			Domains: []string{domain},
			Routes: []*route.Route{{
				Match: &route.RouteMatch{
					PathSpecifier: &route.RouteMatch_Prefix{
						Prefix: "/",
					},
				},
				Action: &route.Route_Route{
					Route: &route.RouteAction{
						ClusterSpecifier: &route.RouteAction_Cluster{
							Cluster: domain,
						},
					},
				},
			}},
		}
	}
	manager := &hcm.HttpConnectionManager{
		CodecType:  hcm.HttpConnectionManager_AUTO,
		StatPrefix: "ingress_http",
		RouteSpecifier: &hcm.HttpConnectionManager_RouteConfig{
			RouteConfig: &api.RouteConfiguration{
				VirtualHosts: vhosts,
			},
		},
		HttpFilters: []*hcm.HttpFilter{{
			Name: envoyRouter,
		}},
	}
	pbst, err := ptypes.MarshalAny(manager)
	if err != nil {
		panic(err)
	}

	return &listener.Listener{
		Name: fmt.Sprintf("%s-listener", shrd),
		Address: &core.Address{
			Address: &core.Address_SocketAddress{
				SocketAddress: &core.SocketAddress{
					Protocol: core.SocketAddress_TCP,
					Address:  hp.host,
					PortSpecifier: &core.SocketAddress_PortValue{
						PortValue: hp.port,
					},
				},
			},
		},
		FilterChains: []*listenerv2.FilterChain{{
			Filters: []*listenerv2.Filter{{
				Name: envoyHCM,
				ConfigType: &listenerv2.Filter_TypedConfig{
					TypedConfig: pbst,
				},
			}},
		}},
	}
}

func computeSnapshot(appId string, kvs map[string]interface{}, version uint) (snap cache.Snapshot, err error) {
	snap = cache.Snapshot{}
	shardsKey := fmt.Sprintf("%s.shards", appId)
	if data, present := kvs[shardsKey]; !present {
		err = fmt.Errorf("'%s' key is missing from configuration", shardsKey)
	} else {
		var lstnrs []*listener.Listener
		var clusts []*cluster.Cluster
		if shrds, ok := readAsStringSlice(data); ok {
			for _, shrd := range shrds {
				if hp, confErr := readListenerHostPort(shrd, kvs); confErr != nil {
					err = confErr
					break
				} else {
					if clusters, endpoints, connectTimeouts, confErr := readClusters(shrd, kvs); confErr != nil {
						err = confErr
						break
					} else {
						lstnrs = append(lstnrs, makeGRPCListener(shrd, hp, clusters...))
						clusts = append(clusts, makeClusters(endpoints, connectTimeouts, clusters...)...)
					}
				}
			}
			snapVersion := strconv.Itoa(int(version))
			clustsRes := getClusterResources(clusts)
			lstnrsRes := getListenerResources(lstnrs)
			snap = cache.NewSnapshot(snapVersion, nil, clustsRes, nil, lstnrsRes, nil)
		} else {
			err = fmt.Errorf("'%s' key must have an array of strings as value", shardsKey)
		}
	}
	return
}

func getClusterResources(clusts []*cluster.Cluster) []types.Resource {
	res := make([]types.Resource, len(clusts))
	for i, clus := range clusts {
		res[i] = clus
	}
	return res
}

func getListenerResources(lstnrs []*listener.Listener) []types.Resource {
	res := make([]types.Resource, len(lstnrs))
	for i, list := range lstnrs {
		res[i] = list
	}
	return res
}

func makeClusters(endpoints map[string][]hostPort, connectTimeouts map[string]time.Duration, clusterNames ...string) (clusts []*cluster.Cluster) {
	for _, clusName := range clusterNames {
		dkvClus := new(cluster.Cluster)
		dkvClus.Name = clusName
		dkvClus.ConnectTimeout = ptypes.DurationProto(connectTimeouts[clusName])
		dkvClus.ClusterDiscoveryType = &cluster.Cluster_Type{Type: cluster.Cluster_STATIC}
		dkvClus.Http2ProtocolOptions = new(core.Http2ProtocolOptions)
		dkvClus.LbPolicy = cluster.Cluster_ROUND_ROBIN
		var lbEndpoints []*endpointv2.LbEndpoint
		for _, endpoint := range endpoints[clusName] {
			lbEndpoints = append(lbEndpoints, &endpointv2.LbEndpoint{
				HostIdentifier: &endpointv2.LbEndpoint_Endpoint{
					Endpoint: &endpointv2.Endpoint{
						Address: &core.Address{
							Address: &core.Address_SocketAddress{
								SocketAddress: &core.SocketAddress{
									Protocol: core.SocketAddress_TCP,
									Address:  endpoint.host,
									PortSpecifier: &core.SocketAddress_PortValue{
										PortValue: endpoint.port,
									},
								},
							},
						},
					},
				},
			})
		}

		dkvClus.LoadAssignment = &endpoint.ClusterLoadAssignment{
			ClusterName: clusName,
			Endpoints: []*endpointv2.LocalityLbEndpoints{{
				//Locality:
				LbEndpoints: lbEndpoints,
			}},
		}
		clusts = append(clusts, dkvClus)
	}
	return
}

func readAsStringSlice(val interface{}) (res []string, ok bool) {
	var vals []interface{}
	if vals, ok = val.([]interface{}); ok {
		res = make([]string, len(vals))
		for i, v := range vals {
			if res[i], ok = v.(string); !ok {
				break
			}
		}
	}
	return
}

func readAppIdInstanceGroups(kvs map[string]interface{}) (appId string, igs []string, err error) {
	if appIdVal, ok := kvs[appIdKey]; ok {
		if appId, ok = appIdVal.(string); ok {
			igKey := fmt.Sprintf("%s.instanceGroups", appId)
			if igsVal, ok := kvs[igKey]; ok {
				if igs, ok = readAsStringSlice(igsVal); !ok {
					err = fmt.Errorf("'%s' key must have an array of strings as value", igKey)
				}
			} else {
				err = fmt.Errorf("'%s' key is missing from configuration", igKey)
			}
		} else {
			err = fmt.Errorf("'%s' key must have a string value", appIdKey)
		}
	} else {
		err = fmt.Errorf("'%s' key is missing from configuration", appIdKey)
	}
	return
}

type hostPort struct {
	host string
	port uint32
}

func newHostPort(hostPortStrs ...string) (hps []hostPort, err error) {
	for _, hostPortStr := range hostPortStrs {
		hostPorts := strings.Split(hostPortStr, ":")
		if len(hostPorts) == 2 {
			hostStr, portStr := hostPorts[0], hostPorts[1]
			if portVal, fmtErr := strconv.ParseUint(portStr, 10, 32); fmtErr == nil {
				hps = append(hps, hostPort{hostStr, uint32(portVal)})
			} else {
				err = fmt.Errorf("unable to read port number from '%s', error: %v", hostPortStr, fmtErr)
				break
			}
		} else {
			err = fmt.Errorf("given value '%s' is not in host:port format", hostPortStr)
			break
		}
	}
	return
}

func readListenerHostPort(shrd string, kvs map[string]interface{}) (hp hostPort, err error) {
	listenAddrKey := fmt.Sprintf("%s.listener_addr", shrd)
	if listenAddrVal, present := kvs[listenAddrKey]; !present {
		err = fmt.Errorf("'%s' key is missing from configuration", listenAddrKey)
	} else {
		if listenAddr, ok := listenAddrVal.(string); ok {
			if hps, fmtErr := newHostPort(listenAddr); fmtErr != nil {
				err = fmt.Errorf("unable to convert the value for key '%s' as a listener address, error: %v", listenAddrKey, fmtErr)
			} else {
				hp = hps[0]
			}
		} else {
			err = fmt.Errorf("'%s' key must have a string (host:port) as value", listenAddrKey)
		}
	}
	return
}

func readClusters(shrd string, kvs map[string]interface{}) (clusters []string, endpoints map[string][]hostPort, connectTimeouts map[string]time.Duration, err error) {
	clustersKey := fmt.Sprintf("%s.clusters", shrd)
	if clustersVal, ok := kvs[clustersKey]; ok {
		if clusters, ok = readAsStringSlice(clustersVal); !ok {
			err = fmt.Errorf("'%s' key must have an array of strings as value", clustersKey)
		} else {
			endpoints = make(map[string][]hostPort, len(clusters))
			connectTimeouts = make(map[string]time.Duration, len(clusters))
			for _, clus := range clusters {
				endpointsKey := fmt.Sprintf("%s.endpoints", clus)
				if endpointsVal, present := kvs[endpointsKey]; present {
					if endpointsSlc, ok := readAsStringSlice(endpointsVal); ok {
						if hps, fmtErr := newHostPort(endpointsSlc...); fmtErr == nil {
							endpoints[clus] = hps
						} else {
							fmt.Errorf("unable to convert the value for key '%s' as a set of endpoints, error: %v", endpointsKey, fmtErr)
							break
						}
					} else {
						err = fmt.Errorf("'%s' key must have an array of strings as value", endpointsKey)
						break
					}
				} else {
					err = fmt.Errorf("'%s' key is missing from configuration", endpointsKey)
					break
				}

				connectTimeoutKey := fmt.Sprintf("%s.connect_timeout", clus)
				if connectTimeoutVal, present := kvs[connectTimeoutKey]; present {
					if connectTimeoutStr, ok := connectTimeoutVal.(string); ok {
						if connectTimeout, parseErr := time.ParseDuration(connectTimeoutStr); parseErr == nil {
							connectTimeouts[clus] = connectTimeout
						} else {
							err = fmt.Errorf("Unable to convert the value '%s' into a duration, for key '%s'", connectTimeoutStr, connectTimeoutKey)
							break
						}
					} else {
						err = fmt.Errorf("'%s' key must have a string value", connectTimeoutKey)
						break
					}
				} else {
					err = fmt.Errorf("'%s' key is missing from configuration", connectTimeoutKey)
					break
				}
			}
		}
	} else {
		err = fmt.Errorf("'%s' key is missing from configuration", clustersKey)
	}
	return
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
	log.Printf("Successfully setup the xDS GRPC service at %s...", listenAddr)
	return grpcServer, lis
}

func setupSignalHandler() <-chan os.Signal {
	signals := []os.Signal{syscall.SIGINT, syscall.SIGQUIT, syscall.SIGSTOP, syscall.SIGTERM}
	stopChan := make(chan os.Signal, len(signals))
	signal.Notify(stopChan, signals...)
	return stopChan
}
