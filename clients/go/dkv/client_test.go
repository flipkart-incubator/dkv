package dkv

import (
	"encoding/json"
	"fmt"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
	"os"
	"time"

	"sync"
	"testing"
)

var (
	dkvPorts  = map[int]int{1: 9081, 2: 9082, 3: 9083}
	dkvCli    *ShardedDKVClient
	mutex     = sync.Mutex{}
	rc        = serverpb.ReadConsistency_SEQUENTIAL
	Dkvshards []DKVShard
	err       error
)

func TestDistributedClient(t *testing.T) {
	if os.Getenv("CI") == "" {
		t.Skip("Skipping testing in CI environment, test requires DKV cluster")
	}
	initClusterConfig(t)
	testDistributedPut(t)
	testLinearizableGet(t)
	testMultiGet(t)

	err = dkvCli.Close()
	if err != nil {
		t.Fatal("ShardedDKVClient Close Failure")
	}
}

func initClusterConfig(t *testing.T) {
	for i, v := range dkvPorts {
		shard := DKVShard{
			Name: DKVShardKey(fmt.Sprintf("shard%d", i)),
			Topology: map[DKVServerRole]*DKVNodeSet{
				noRole: &DKVNodeSet{
					Name: "default",
					Nodes: []DKVNode{
						{
							Host: "127.0.0.1",
							Port: v,
						},
					},
				},
			},
		}
		Dkvshards = append(Dkvshards, shard)
	}

	b, _ := json.Marshal(Dkvshards)
	t.Log("Shard Info ", string(b))

	shardProvider := &KeyHashBasedShardProvider{Dkvshards}
	dkvCli, err = NewShardedDKVClient(shardProvider)
	if err != nil {
		t.Fatal("ShardedDKVClient Create Failure")
	}
}

func testDistributedPut(t *testing.T) {
	for i := 1; i <= 50; i++ {
		key, value := fmt.Sprintf("K_CLI_%d", i), fmt.Sprintf("V_CLI_%d", i)
		if err := dkvCli.Put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Unable to PUT. Key: %s, Value: %s, Error: %v", key, value, err)
		}
	}
	sleepInSecs(1)
	for i := 1; i <= 50; i++ {
		key, expectedValue := fmt.Sprintf("K_CLI_%d", i), fmt.Sprintf("V_CLI_%d", i)
		if actualValue, err := dkvCli.Get(rc, []byte(key)); err != nil {
			t.Errorf("Unable to GET for Key: %s, Error: %v", key, err)
		} else if string(actualValue.Value) != expectedValue {
			t.Errorf("GET mismatch for Key: %s, Expected Value: %s, Actual Value: %s", key, expectedValue, actualValue)
		}
	}
}

func testLinearizableGet(t *testing.T) {
	getRC := serverpb.ReadConsistency_LINEARIZABLE
	kp, vp := "LGK", "LGV"
	for i := 1; i <= 25; i++ {
		key, value := fmt.Sprintf("%s%d", kp, i), fmt.Sprintf("%s%d", vp, i)
		if err := dkvCli.Put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Unable to PUT. Key: %s, Value: %s, Error: %v", key, value, err)
		}

		if actualValue, err := dkvCli.Get(getRC, []byte(key)); err != nil {
			t.Errorf("Unable to GET for Key: %s, Error: %v", key, err)
		} else if string(actualValue.Value) != value {
			t.Errorf("GET mismatch for Key: %s, Expected Value: %s, Actual Value: %v", key, value, actualValue)
		}
	}
}

func testMultiGet(t *testing.T) {
	kp := "MGK"
	var keyMap = make(map[string][]byte)
	var keys [][]byte
	var iter = 50
	for i := 1; i <= iter; i++ {
		key, value := fmt.Sprintf("%s%d", kp, i), fmt.Sprintf("%s%d", kp, i)
		keyMap[key] = []byte(key)
		keys = append(keys, []byte(key))
		if err = dkvCli.Put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Unable to PUT. Key: %s, Value: %s, Error: %v", key, value, err)
		}
	}

	res2, err := dkvCli.MultiGet(serverpb.ReadConsistency_LINEARIZABLE, keys...)
	if err != nil {
		t.Fatal("ShardedDKVClient MultiGet Failure")
	}
	for _, pair := range res2 {
		if v, found := keyMap[string(pair.Key)]; found {
			if string(pair.Value) != string(v) {
				t.Errorf("GET mismatch for Key: %s, Expected Value: %s, Actual Value: %v", string(pair.Key), string(v), string(pair.Value))
			}

		} else {
			t.Errorf("GET mismatch for unknown Key: %s, Expected Value: %s, Actual Value: %v", string(pair.Key), "Unknown", string(pair.Value))
		}
	}

	if len(res2) != iter {
		t.Fatalf("NewShardedDKVClient MultiGet Failure Size Mismatch Expected: %d, Actual: %d", iter, len(res2))
	}

}

func sleepInSecs(duration int) {
	<-time.After(time.Duration(duration) * time.Second)
}
