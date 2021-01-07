package pkg

import (
	"fmt"
	api "github.com/envoyproxy/go-control-plane/envoy/api/v2"
	cluster "github.com/envoyproxy/go-control-plane/envoy/api/v2"
	endpoint "github.com/envoyproxy/go-control-plane/envoy/api/v2"
	listener "github.com/envoyproxy/go-control-plane/envoy/api/v2"
	core "github.com/envoyproxy/go-control-plane/envoy/api/v2/core"
	endpointV2 "github.com/envoyproxy/go-control-plane/envoy/api/v2/endpoint"
	listenerV2 "github.com/envoyproxy/go-control-plane/envoy/api/v2/listener"
	route "github.com/envoyproxy/go-control-plane/envoy/api/v2/route"
	hcm "github.com/envoyproxy/go-control-plane/envoy/config/filter/network/http_connection_manager/v2"
	"github.com/envoyproxy/go-control-plane/pkg/cache/types"
	"github.com/envoyproxy/go-control-plane/pkg/cache/v2"
	"github.com/golang/protobuf/ptypes"
	"strconv"
	"strings"
	"time"
)

const (
	envoyRouter = "envoy.router"
	envoyHCM    = "envoy.http_connection_manager"
	appIdKey    = "appId"
)

type EnvoyDKVConfig map[string]interface{}

func (conf EnvoyDKVConfig) ComputeAndSetSnapshot(snapVer uint, snapCache cache.SnapshotCache) error {
	appId, igs, err := conf.readAppIdInstanceGroups()
	if err != nil {
		return fmt.Errorf("unable to fetch instance groups, error: %v", err)
	}

	if snap, err := conf.computeSnapshot(appId, snapVer); err != nil {
		return fmt.Errorf("unable to compute snapshot, error: %v", err)
	} else {
		for _, nodeName := range igs {
			if err := snapCache.SetSnapshot(nodeName, snap); err != nil {
				return fmt.Errorf("unable to set snapshot, error: %v", err)
			}
		}
	}
	return nil
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
		FilterChains: []*listenerV2.FilterChain{{
			Filters: []*listenerV2.Filter{{
				Name: envoyHCM,
				ConfigType: &listenerV2.Filter_TypedConfig{
					TypedConfig: pbst,
				},
			}},
		}},
	}
}

func makeClusters(endpoints map[string][]hostPort, connectTimeouts map[string]time.Duration, clusterNames ...string) (clusts []*cluster.Cluster) {
	for _, clusName := range clusterNames {
		dkvClus := new(cluster.Cluster)
		dkvClus.Name = clusName
		dkvClus.ConnectTimeout = ptypes.DurationProto(connectTimeouts[clusName])
		dkvClus.ClusterDiscoveryType = &cluster.Cluster_Type{Type: cluster.Cluster_STATIC}
		dkvClus.Http2ProtocolOptions = new(core.Http2ProtocolOptions)
		dkvClus.LbPolicy = cluster.Cluster_ROUND_ROBIN
		var lbEndpoints []*endpointV2.LbEndpoint
		for _, ep := range endpoints[clusName] {
			lbEndpoints = append(lbEndpoints, &endpointV2.LbEndpoint{
				HostIdentifier: &endpointV2.LbEndpoint_Endpoint{
					Endpoint: &endpointV2.Endpoint{
						Address: &core.Address{
							Address: &core.Address_SocketAddress{
								SocketAddress: &core.SocketAddress{
									Protocol: core.SocketAddress_TCP,
									Address:  ep.host,
									PortSpecifier: &core.SocketAddress_PortValue{
										PortValue: ep.port,
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
			Endpoints: []*endpointV2.LocalityLbEndpoints{{
				//Locality:
				LbEndpoints: lbEndpoints,
			}},
		}
		clusts = append(clusts, dkvClus)
	}
	return
}

func (conf EnvoyDKVConfig) computeSnapshot(appId string, version uint) (snap cache.Snapshot, err error) {
	snap = cache.Snapshot{}
	shardsKey := fmt.Sprintf("%s.shards", appId)
	if data, present := conf[shardsKey]; !present {
		err = fmt.Errorf("'%s' key is missing from configuration", shardsKey)
	} else {
		var lstnrs []*listener.Listener
		var clusts []*cluster.Cluster
		if shrds, ok := readAsStringSlice(data); ok {
			for _, shrd := range shrds {
				if hp, confErr := conf.readListenerHostPort(shrd); confErr != nil {
					err = confErr
					break
				} else {
					if clusters, endpoints, connectTimeouts, confErr := conf.readClusters(shrd); confErr != nil {
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

func (conf EnvoyDKVConfig) readClusters(shrd string) (clusters []string, endpoints map[string][]hostPort, connectTimeouts map[string]time.Duration, err error) {
	clustersKey := fmt.Sprintf("%s.clusters", shrd)
	if clustersVal, ok := conf[clustersKey]; ok {
		if clusters, ok = readAsStringSlice(clustersVal); !ok {
			err = fmt.Errorf("'%s' key must have an array of strings as value", clustersKey)
		} else {
			endpoints = make(map[string][]hostPort, len(clusters))
			connectTimeouts = make(map[string]time.Duration, len(clusters))
			for _, clus := range clusters {
				endpointsKey := fmt.Sprintf("%s.endpoints", clus)
				if endpointsVal, present := conf[endpointsKey]; present {
					if endpointsSlc, ok := readAsStringSlice(endpointsVal); ok {
						if hps, fmtErr := newHostPort(endpointsSlc...); fmtErr == nil {
							endpoints[clus] = hps
						} else {
							err = fmt.Errorf("unable to convert the value for key '%s' as a set of endpoints, error: %v", endpointsKey, fmtErr)
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
				if connectTimeoutVal, present := conf[connectTimeoutKey]; present {
					if connectTimeoutStr, ok := connectTimeoutVal.(string); ok {
						if connectTimeout, parseErr := time.ParseDuration(connectTimeoutStr); parseErr == nil {
							connectTimeouts[clus] = connectTimeout
						} else {
							err = fmt.Errorf("unable to convert the value '%s' into a duration, for key '%s'", connectTimeoutStr, connectTimeoutKey)
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

func (conf EnvoyDKVConfig) readAppIdInstanceGroups() (appId string, igs []string, err error) {
	if appIdVal, ok := conf[appIdKey]; ok {
		if appId, ok = appIdVal.(string); ok {
			igKey := fmt.Sprintf("%s.instanceGroups", appId)
			if igsVal, ok := conf[igKey]; ok {
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

func (conf EnvoyDKVConfig) readListenerHostPort(shrd string, ) (hp hostPort, err error) {
	listenAddrKey := fmt.Sprintf("%s.listener_addr", shrd)
	if listenAddrVal, present := conf[listenAddrKey]; !present {
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
