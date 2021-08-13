package pkg

import (
	"encoding/json"
	"fmt"
	_ "github.com/Jille/grpc-multi-resolver"
	"github.com/flipkart-incubator/dkv/pkg/ctl"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
	"io/ioutil"
	"log"
)

type DiscoveryClient struct {
	client     *ctl.DKVClient
	configPath string
}

// Create a connection to DKV Service discovery client which provides a view of entire cluster
// Cluster comprises of all databases and shards (vBuckets) which are registering to this service discovery group
func InitServiceDiscoveryClient(discoveryServiceAddr string, configPath string) *DiscoveryClient {
	client, err := ctl.NewInSecureDKVClient(discoveryServiceAddr, "")
	if (err != nil) {
	   log.Panicf("Failed to start Discovery Client %v.", err)
	}
	return &DiscoveryClient{client: client, configPath: configPath}
}

func (c *DiscoveryClient) GetEnvoyConfig() (EnvoyDKVConfig, error) {

	config, err := ioutil.ReadFile(c.configPath)
	if err != nil {
		return nil, err
	}

	kvs := make(map[string]interface{})
	err = json.Unmarshal(config, &kvs)
	if err != nil {
		return nil, err
	}

	regions, err := c.client.GetClusterInfo("", "", "")

	if err != nil {
		return nil, err
	}

	all_endpoints := make(map[string][]string)

	for _, region := range regions {
		var nodeType string
		if region.Status == serverpb.RegionStatus_LEADER || region.Status == serverpb.RegionStatus_PRIMARY_FOLLOWER ||
			region.Status == serverpb.RegionStatus_SECONDARY_FOLLOWER {
			nodeType = "masters"
		} else {
			nodeType = "slaves"
		}
		key := fmt.Sprintf("%s-%s.endpoints", region.VBucket, nodeType)
		endpoints, ok := all_endpoints[key]
		if !ok {
			endpoints = make([]string, 0)
		}
		all_endpoints[key] = append(endpoints, region.NodeAddress)
	}

	for key, value := range all_endpoints {
		kvs[key] = value
	}

	fmt.Printf("Envoy Config is %s", kvs)
	return kvs, nil
}
