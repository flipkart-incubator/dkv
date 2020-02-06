package sync

import (
	"bytes"
	"encoding/gob"
	"errors"
	"testing"

	"github.com/flipkart-incubator/dkv/internal/server/storage"
	"github.com/flipkart-incubator/dkv/internal/server/sync/raftpb"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
	"github.com/gogo/protobuf/proto"
)

func TestDKVReplStoreSave(t *testing.T) {
	kvs := newMemStore()
	dkvRepl := NewDKVReplStore(kvs)

	testPut(t, kvs, dkvRepl, []byte("foo"), []byte("bar"))
	testPut(t, kvs, dkvRepl, []byte("hello"), []byte("world"))
	testPut(t, kvs, dkvRepl, []byte("kit"), []byte("kat"))

	testGet(t, kvs, dkvRepl, []byte("foo"))
	testGet(t, kvs, dkvRepl, []byte("hello"))
	testGet(t, kvs, dkvRepl, []byte("kit"))

	testMultiGet(t, kvs, dkvRepl, []byte("foo"), []byte("hello"), []byte("kit"))
}

func TestDKVReplStoreClose(t *testing.T) {
	kvs := newMemStore()
	dkvRepl := NewDKVReplStore(kvs)
	if err := dkvRepl.Close(); err != nil {
		t.Error(err)
	} else if kvs.store != nil {
		t.Errorf("Underlying store is expected to be closed but is open")
	}
}

func testPut(t *testing.T, kvs *memStore, dkvRepl *dkvReplStore, key, val []byte) {
	intReq := new(raftpb.InternalRaftRequest)
	intReq.Put = &serverpb.PutRequest{Key: key, Value: val}
	if reqBts, err := proto.Marshal(intReq); err != nil {
		t.Error(err)
	} else {
		if _, err := dkvRepl.Save(reqBts); err != nil {
			t.Error(err)
		} else {
			if res := kvs.Get(key)[0]; res.Error != nil {
				t.Error(res.Error)
			} else if string(res.Value) != string(val) {
				t.Errorf("Value mismatch for key: %s. Expected: %s, Actual: %s", key, val, res.Value)
			}
		}
	}
}

func testGet(t *testing.T, kvs *memStore, dkvRepl *dkvReplStore, key []byte) {
	intReq := new(raftpb.InternalRaftRequest)
	intReq.Get = &serverpb.GetRequest{Key: key}
	if reqBts, err := proto.Marshal(intReq); err != nil {
		t.Error(err)
	} else {
		if val, err := dkvRepl.Save(reqBts); err != nil {
			t.Error(err)
		} else if kvsVal := kvs.Get(key)[0].Value; string(val) != string(kvsVal) {
			t.Errorf("Value mismatch for key: %s. Expected: %s, Actual: %s", key, kvsVal, val)
		}
	}
}

func testMultiGet(t *testing.T, kvs *memStore, dkvRepl *dkvReplStore, keys ...[]byte) {
	getReqs := make([]*serverpb.GetRequest, len(keys))
	for i, key := range keys {
		getReqs[i] = &serverpb.GetRequest{Key: key}
	}
	intReq := new(raftpb.InternalRaftRequest)
	intReq.MultiGet = &serverpb.MultiGetRequest{GetRequests: getReqs}
	if reqBts, err := proto.Marshal(intReq); err != nil {
		t.Error(err)
	} else {
		if vals, err := dkvRepl.Save(reqBts); err != nil {
			t.Error(err)
		} else {
			readResults := make([]*storage.ReadResult, len(keys))
			buf := bytes.NewBuffer(vals)
			if err := gob.NewDecoder(buf).Decode(&readResults); err != nil {
				t.Error(err)
			} else {
				kvsVals := kvs.Get(keys...)
				for i, readResult := range readResults {
					readVal := readResult.Value
					kvsVal := kvsVals[i].Value
					if string(readVal) != string(kvsVal) {
						t.Errorf("Value mismatch for key: %s. Expected: %s, Actual: %s", keys[i], kvsVal, readVal)
					}
				}
			}
		}
	}
}

type memStore struct {
	store map[string][]byte
}

func newMemStore() *memStore {
	return &memStore{store: make(map[string][]byte)}
}

func (ms *memStore) Put(key []byte, value []byte) *storage.Result {
	storeKey := string(key)
	res := storage.Result{}
	if _, present := ms.store[storeKey]; present {
		res.Error = errors.New("Given key already exists")
	} else {
		ms.store[storeKey] = value
	}
	return &res
}

func (ms *memStore) Get(keys ...[]byte) []*storage.ReadResult {
	rss := make([]*storage.ReadResult, len(keys))
	for i, key := range keys {
		storeKey := string(key)
		rss[i] = &storage.ReadResult{&storage.Result{Error: nil}, nil}
		if val, present := ms.store[storeKey]; present {
			rss[i].Value = val
		} else {
			rss[i].Error = errors.New("Given key not found")
		}
	}
	return rss
}

func (ms *memStore) Close() error {
	ms.store = nil
	return nil
}
