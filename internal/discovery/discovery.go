package discovery

import (
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
)

// StatusPropagator is the interface to propagate status updates of all regions in the node to the discovery system
type StatusPropagator interface {
	// propagate status updates of all regions in the node to the discovery system
	PropagateStatus()
	// register regions part of current node
	RegisterRegion(server serverpb.DKVDiscoveryNodeServer)
	// Sends one last status update before closing
	// This should ideally be called after closing all regions so that status update is correct
	Close() error
}

// ClusterInfoGetter is the interface to get status updates of cluster based on request criteria
type ClusterInfoGetter interface {
	// GetClusterStatus get the status of nodes matching given database and vBucket
	GetClusterStatus(database string, vBucket string) ([]*serverpb.RegionInfo, error)
}

type Client interface {
	StatusPropagator
	ClusterInfoGetter
}

type DiscoveryServiceConfig struct {

	ServiceConfig DiscoveryConfig

	ClientConfig DiscoveryClientConfig
}

type DiscoveryServiceConfigDto struct {

	ServiceConfig DiscoveryConfigDto
	ClientConfig DiscoveryClientConfigDto
}