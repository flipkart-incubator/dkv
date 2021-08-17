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

// InitServiceDiscoveryClient Create a connection to DKV Service discovery client which provides a view of entire cluster
// Cluster comprises of all databases and shards (vBuckets) which are registering to this service discovery group
func InitServiceDiscoveryClient(configPath string) *DiscoveryClient {
	kvs, err := readConfig(configPath)
	if err != nil {
		log.Panicf("Failed to read Config File %v.", err)
	}
	client, err := ctl.NewInSecureDKVClient(kvs["discoveryServerAddr"].(string), "")
	if err != nil {
		log.Panicf("Failed to start Discovery Client %v.", err)
	}
	return &DiscoveryClient{client: client, configPath: configPath}
}

func readConfig(configPath string) (map[string]interface{}, error) {
	config, err := ioutil.ReadFile(configPath)
	if err != nil {
		return nil, err
	}
	kvs := make(map[string]interface{})
	err = json.Unmarshal(config, &kvs)
	if err != nil {
		return nil, err
	}
	return kvs, nil
}

func (c *DiscoveryClient) GetEnvoyConfig() (EnvoyDKVConfig, error) {
	kvs, err := readConfig(c.configPath)
	if err != nil {
		return nil, err
	}

	regions, err := c.client.GetClusterInfo(kvs["dc-id"].(string), kvs["database"].(string), "")
	if err != nil {
		return nil, err
	}

	allEndpoints := make(map[string][]string)
	for _, region := range regions {
		var nodeType string
		if region.Status == serverpb.RegionStatus_LEADER || region.Status == serverpb.RegionStatus_PRIMARY_FOLLOWER ||
			region.Status == serverpb.RegionStatus_SECONDARY_FOLLOWER {
			nodeType = "masters"
		} else if region.Status == serverpb.RegionStatus_ACTIVE_SLAVE {
			nodeType = "slaves"
		} else {
			log.Printf("Unexpected node status %s \n", region)
			continue
		}
		key := fmt.Sprintf("%s-%s.endpoints", region.VBucket, nodeType)
		endpoints, ok := allEndpoints[key]
		if !ok {
			endpoints = make([]string, 0)
		}
		allEndpoints[key] = append(endpoints, region.NodeAddress)
	}

	for key, value := range allEndpoints {
		kvs[key] = value
	}

	fmt.Printf("Envoy Config is %s \n", kvs)
	return kvs, nil
}
