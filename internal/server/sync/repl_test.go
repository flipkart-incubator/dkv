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
	dkv_repl := NewDKVReplStore(kvs)

	testPut(t, kvs, dkv_repl, []byte("foo"), []byte("bar"))
	testPut(t, kvs, dkv_repl, []byte("hello"), []byte("world"))
	testPut(t, kvs, dkv_repl, []byte("kit"), []byte("kat"))

	testGet(t, kvs, dkv_repl, []byte("foo"))
	testGet(t, kvs, dkv_repl, []byte("hello"))
	testGet(t, kvs, dkv_repl, []byte("kit"))

	testMultiGet(t, kvs, dkv_repl, []byte("foo"), []byte("hello"), []byte("kit"))
}

func TestDKVReplStoreClose(t *testing.T) {
	kvs := newMemStore()
	dkv_repl := NewDKVReplStore(kvs)
	if err := dkv_repl.Close(); err != nil {
		t.Error(err)
	} else if kvs.store != nil {
		t.Errorf("Underlying store is expected to be closed but is open")
	}
}

func testPut(t *testing.T, kvs *memStore, dkvRepl *dkvReplStore, key, val []byte) {
	int_req := new(raftpb.InternalRaftRequest)
	int_req.Put = &serverpb.PutRequest{Key: key, Value: val}
	if req_bts, err := proto.Marshal(int_req); err != nil {
		t.Error(err)
	} else {
		if _, err := dkvRepl.Save(req_bts); err != nil {
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
	int_req := new(raftpb.InternalRaftRequest)
	int_req.Get = &serverpb.GetRequest{Key: key}
	if req_bts, err := proto.Marshal(int_req); err != nil {
		t.Error(err)
	} else {
		if val, err := dkvRepl.Save(req_bts); err != nil {
			t.Error(err)
		} else if kvs_val := kvs.Get(key)[0].Value; string(val) != string(kvs_val) {
			t.Errorf("Value mismatch for key: %s. Expected: %s, Actual: %s", key, kvs_val, val)
		}
	}
}

func testMultiGet(t *testing.T, kvs *memStore, dkvRepl *dkvReplStore, keys ...[]byte) {
	get_reqs := make([]*serverpb.GetRequest, len(keys))
	for i, key := range keys {
		get_reqs[i] = &serverpb.GetRequest{Key: key}
	}
	int_req := new(raftpb.InternalRaftRequest)
	int_req.MultiGet = &serverpb.MultiGetRequest{GetRequests: get_reqs}
	if req_bts, err := proto.Marshal(int_req); err != nil {
		t.Error(err)
	} else {
		if vals, err := dkvRepl.Save(req_bts); err != nil {
			t.Error(err)
		} else {
			read_results := make([]*storage.ReadResult, len(keys))
			buf := bytes.NewBuffer(vals)
			if err := gob.NewDecoder(buf).Decode(&read_results); err != nil {
				t.Error(err)
			} else {
				kvs_vals := kvs.Get(keys...)
				for i, read_result := range read_results {
					read_val := read_result.Value
					kvs_val := kvs_vals[i].Value
					if string(read_val) != string(kvs_val) {
						t.Errorf("Value mismatch for key: %s. Expected: %s, Actual: %s", keys[i], kvs_val, read_val)
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
	store_key := string(key)
	res := storage.Result{}
	if _, present := ms.store[store_key]; present {
		res.Error = errors.New("Given key already exists")
	} else {
		ms.store[store_key] = value
	}
	return &res
}

func (ms *memStore) Get(keys ...[]byte) []*storage.ReadResult {
	rss := make([]*storage.ReadResult, len(keys))
	for i, key := range keys {
		store_key := string(key)
		rss[i] = &storage.ReadResult{&storage.Result{Error: nil}, nil}
		if val, present := ms.store[store_key]; present {
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
