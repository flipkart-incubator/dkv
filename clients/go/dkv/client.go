package dkv

import (
	"fmt"
	"github.com/OneOfOne/xxhash"
	"github.com/dgraph-io/ristretto"
	"github.com/flipkart-incubator/dkv/pkg/ctl"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"

	"log"
)

//DKVServerRole defines the role of the DKV node
type DKVServerRole string

const (
	noRole     DKVServerRole = "UNKNOWN"
	masterRole               = "MASTER"
	slaveRole                = "SLAVE"
)

func getNodeTypeByReadConsistency(rc serverpb.ReadConsistency) DKVServerRole {
	if rc == serverpb.ReadConsistency_LINEARIZABLE {
		return masterRole
	}
	return slaveRole
}

//DKVNode defines a DKV Node by a given Host & Port
type DKVNode struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

//DKVNodeSet defines a group of DKV Node(s)
type DKVNodeSet struct {
	Name  string    `json:"name"`
	Nodes []DKVNode `json:"nodes"`
}

//DKVShard defines a group of DKVNodeSet(s) along with their roles.
type DKVShard struct {
	Name     string                        `json:"name"`
	Topology map[DKVServerRole]*DKVNodeSet `json:"topology"`
}

func (s DKVShard) getNodesByType(nodeType ...DKVServerRole) (*DKVNodeSet, error) {
	for _, v := range nodeType {
		if val, ok := s.Topology[v]; ok {
			return val, nil
		}
	}
	return nil, fmt.Errorf("valid DKV node type must be given")
}

type ListOfKeys [][]byte

//ShardProvider Provides the ShardInformation for the given key(s)
type ShardProvider interface {
	ProvideShard(key []byte) (*DKVShard, error)
	ProvideShards(keys ...[]byte) (map[*DKVShard]ListOfKeys, error)
}

//KeyHashBasedShardProvider A xxhash based shared provider.
type KeyHashBasedShardProvider struct {
	shardConfiguration []DKVShard
}

func (p *KeyHashBasedShardProvider) getShardID(key []byte) int {
	h := xxhash.New64()
	h.Write(key)
	hash := h.Sum64()
	var id = (hash & 0xFFFF) % uint64(len(p.shardConfiguration))
	//LongHashFunction xx = LongHashFunction.xx();
	//long hsh = xx.hashBytes(key);
	//return (int) ((hsh & 0xFFFF) % shardConfiguration.getNumShards());
	return int(id)
}

//ProvideShard provides shard based on input key
func (p *KeyHashBasedShardProvider) ProvideShard(key []byte) (*DKVShard, error) {
	shardId := p.getShardID(key)
	return &p.shardConfiguration[shardId], nil
}

//ProvideShards provides list of pairs<shard, keys> for given list of keys.
func (p *KeyHashBasedShardProvider) ProvideShards(keys ...[]byte) (map[*DKVShard]ListOfKeys, error) {
	m := make(map[*DKVShard]ListOfKeys)
	for _, key := range keys {
		shardId := p.getShardID(key)
		shard := &p.shardConfiguration[shardId]
		m[shard] = append(m[shard], key)
	}
	return m, nil
}

type simpleDKVClient struct {
	*ctl.DKVClient
	addr string
}

// A ShardedDKVClient instance is used to communicate with a shared DKV cluster
// over GRPC. It is an adapter to the underlying GRPC clients that
// exposes a simpler API to its users without having to deal with timeouts,
// contexts, sharding and other GRPC semantics.
type ShardedDKVClient struct {
	pool          *ristretto.Cache
	shardProvider ShardProvider
}

// NewShardedDKVClient creates and returns a instance of ShardedDKVClient.
func NewShardedDKVClient(shardProvider ShardProvider) (*ShardedDKVClient, error) {
	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 1000,
		MaxCost:     1000,
		BufferItems: 64,
		OnExit: func(val interface{}) {
			client := val.(*simpleDKVClient)
			log.Printf("[INFO] Closing Client of %s \n", client.addr)
			client.Close()
		},
	})

	if err != nil {
		return nil, err
	}
	return &ShardedDKVClient{
		shardProvider: shardProvider,
		pool:          cache,
	}, nil
}

func (dkvClnt *ShardedDKVClient) getShardNode(shard *DKVShard, role ...DKVServerRole) (*DKVNodeSet, error) {
	var nodeSet *DKVNodeSet
	var err error
	for _, serverRole := range role {
		nodeSet, err = shard.getNodesByType(serverRole)
		if err == nil {
			return nodeSet, nil
		}
	}
	return nodeSet, err
}

func (dkvClnt *ShardedDKVClient) getShardedClient(shard *DKVShard, role ...DKVServerRole) (*simpleDKVClient, error) {
	var client *simpleDKVClient
	nodeSet, err := dkvClnt.getShardNode(shard, role...)
	if err != nil {
		return nil, err
	}
	if nodeSet == nil {
		return nil, fmt.Errorf("unable to get proper dkv shard endpoint")
	}

	//got a node
	dkvNode := nodeSet.Nodes[0]
	svcAddr := fmt.Sprintf("%s:%d", dkvNode.Host, dkvNode.Port)
	if value, found := dkvClnt.pool.Get(svcAddr); !found {
		log.Printf("[INFO] Creating new Client to : %s\n", svcAddr)
		_client, err := ctl.NewInSecureDKVClient(svcAddr, nodeSet.Name)
		if err != nil {
			return nil, err
		}
		client = &simpleDKVClient{_client, svcAddr}
		dkvClnt.pool.Set(svcAddr, client, 1)
	} else {
		client = value.(*simpleDKVClient)
	}
	return client, nil
}

// Put takes the key and value as byte arrays, find the corresponding shard
// and invokes the GRPC Put method.
func (dkvClnt *ShardedDKVClient) Put(key []byte, value []byte) error {
	dkvShard, err := dkvClnt.shardProvider.ProvideShard(key)
	if err != nil {
		return err
	}
	clnt, err := dkvClnt.getShardedClient(dkvShard, masterRole, noRole)
	if err != nil {
		return err
	}
	return clnt.Put(key, value)
}

// Delete takes the key and value as byte arrays, find the corresponding shard
// and invokes the GRPC Delete method.
func (dkvClnt *ShardedDKVClient) Delete(key []byte) error {
	dkvShard, err := dkvClnt.shardProvider.ProvideShard(key)
	if err != nil {
		return err
	}
	clnt, err := dkvClnt.getShardedClient(dkvShard, masterRole, noRole)
	if err != nil {
		return err
	}
	return clnt.Delete(key)
}

// Get takes the key as byte array along with the consistency,
// finds the corresponding shard and invokes the GRPC Get method.
func (dkvClnt *ShardedDKVClient) Get(rc serverpb.ReadConsistency, key []byte) (*serverpb.GetResponse, error) {
	//DKVShard dkvShard = shardProvider.provideShard(key);
	//DKVNodeType nodeType = getNodeTypeByReadConsistency(consistency);
	//DKVClient dkvClient = pool.getDKVClient(dkvShard, nodeType, UNKNOWN);
	//return dkvClient.get(consistency, key);
	dkvShard, err := dkvClnt.shardProvider.ProvideShard(key)
	if err != nil {
		return nil, err
	}
	nodeRole := getNodeTypeByReadConsistency(rc)
	clnt, err := dkvClnt.getShardedClient(dkvShard, nodeRole, noRole)

	if err != nil {
		return nil, err
	}
	return clnt.Get(rc, key)
}

// MultiGet takes the keys as byte arrays along with the consistency,
// finds the corresponding shard and invokes the GRPC MultiGet method.
func (dkvClnt *ShardedDKVClient) MultiGet(rc serverpb.ReadConsistency, keys ...[]byte) ([]*serverpb.KVPair, error) {
	var kv []*serverpb.KVPair
	dkvShards, err := dkvClnt.shardProvider.ProvideShards(keys...)
	if err != nil {
		return nil, err
	}
	nodeType := getNodeTypeByReadConsistency(rc)
	for dkvShard, keys := range dkvShards {
		_dkvClient, err := dkvClnt.getShardedClient(dkvShard, nodeType, noRole)
		if err != nil {
			return nil, err
		}
		kvs, err := _dkvClient.MultiGet(rc, keys...)
		if err != nil {
			return nil, err
		}
		kv = append(kv, kvs...)
	}
	return kv, nil
}

// Iterate invokes the underlying GRPC method for iterating through the
// entire keyspace in no particular order. `keyPrefix` can be used to
// select only the keys matching the given prefix and `startKey` can
// be used to set the lower bound for the iteration.
func (dkvClnt *ShardedDKVClient) Iterate(keyPrefix, startKey []byte) (<-chan *ctl.KVPair, error) {
	dkvShard, err := dkvClnt.shardProvider.ProvideShard(startKey)
	if err != nil {
		return nil, err
	}
	clnt, err := dkvClnt.getShardedClient(dkvShard, slaveRole, noRole)
	if err != nil {
		return nil, err
	}
	return clnt.Iterate(keyPrefix, startKey)
}

// Close closes the underlying GRPC client(s) connection to DKV service
func (dkvClnt *ShardedDKVClient) Close() error {
	dkvClnt.pool.Clear()
	return nil
}
