package dkv

import (
	"fmt"
	"github.com/OneOfOne/xxhash"
	"github.com/dgraph-io/ristretto"
	"github.com/flipkart-incubator/dkv/pkg/ctl"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"

	"log"
)

type DKVServerRole string

const (
	noRole     DKVServerRole = "UNKNOWN"
	masterRole               = "MASTER"
	slaveRole                = "SLAVE"
)

func GetNodeTypeByReadConsistency(rc serverpb.ReadConsistency) DKVServerRole {
	if rc == serverpb.ReadConsistency_LINEARIZABLE {
		return masterRole
	}
	return slaveRole
}

type DKVNode struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

type DKVNodeSet struct {
	Name  string    `json:"name"`
	Nodes []DKVNode `json:"nodes"`
}

type DKVShard struct {
	Name     string                        `json:"name"`
	Topology map[DKVServerRole]*DKVNodeSet `json:"topology"`
}

func (s DKVShard) GetNodesByType(nodeType ...DKVServerRole) (*DKVNodeSet, error) {
	for _, v := range nodeType {
		if val, ok := s.Topology[v]; ok {
			return val, nil
		}
	}
	return nil, fmt.Errorf("valid DKV node type must be given")
}

type ShardProvider interface {
	ProvideShard(key []byte) (*DKVShard, error)
	ProvideShards(keys ...[]byte) (map[*DKVShard][][]byte, error)
}

type KeyHashBasedShardProvider struct {
	ShardConfiguration []DKVShard
}

func (p KeyHashBasedShardProvider) getShardID(key []byte) int {
	h := xxhash.New64()
	h.Write(key)
	hash := h.Sum64()
	var id = (hash & 0xFFFF) % uint64(len(p.ShardConfiguration))
	//LongHashFunction xx = LongHashFunction.xx();
	//long hsh = xx.hashBytes(key);
	//return (int) ((hsh & 0xFFFF) % shardConfiguration.getNumShards());
	return int(id)
}
func (p KeyHashBasedShardProvider) ProvideShard(key []byte) (*DKVShard, error) {
	shardId := p.getShardID(key)
	return &p.ShardConfiguration[shardId], nil
}

func (p KeyHashBasedShardProvider) ProvideShards(keys ...[]byte) (map[*DKVShard][][]byte, error) {
	m := make(map[*DKVShard][][]byte)
	for _, key := range keys {
		shardId := p.getShardID(key)
		shard := &p.ShardConfiguration[shardId]
		m[shard] = append(m[shard], key)
	}
	return m, nil
}

type SimpleDKVClient struct {
	*ctl.DKVClient
	Addr string
}

type ShardedDKVClient struct {
	pool          *ristretto.Cache
	shardProvider ShardProvider
	shards        []DKVShard
}

func NewShardedDKVClient(shardProvider ShardProvider) (*ShardedDKVClient, error) {
	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 1000,
		MaxCost:     1000,
		BufferItems: 64,
		OnExit: func(val interface{}) {
			client := val.(*SimpleDKVClient)
			log.Printf("Closing Client of %s \n", client.Addr)
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
		nodeSet, err = shard.GetNodesByType(serverRole)
		if err == nil {
			return nodeSet, err
		}
	}
	return nodeSet, err
}

func (dkvClnt *ShardedDKVClient) getShardedClient(shard *DKVShard, role ...DKVServerRole) (*SimpleDKVClient, error) {
	var client *SimpleDKVClient
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
		client = &SimpleDKVClient{_client, svcAddr}
		dkvClnt.pool.Set(svcAddr, client, 1)
	} else {
		client = value.(*SimpleDKVClient)
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
	nodeRole := GetNodeTypeByReadConsistency(rc)
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
	nodeType := GetNodeTypeByReadConsistency(rc)
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
