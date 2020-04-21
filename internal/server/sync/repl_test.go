package sync

import (
	"bytes"
	"encoding/gob"
	"errors"
	"testing"

	"github.com/flipkart-incubator/dkv/internal/server/sync/raftpb"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
	"github.com/flipkart-incubator/nexus/pkg/db"
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

func testPut(t *testing.T, kvs *memStore, dkvRepl db.Store, key, val []byte) {
	intReq := new(raftpb.InternalRaftRequest)
	intReq.Put = &serverpb.PutRequest{Key: key, Value: val}
	if reqBts, err := proto.Marshal(intReq); err != nil {
		t.Error(err)
	} else {
		if _, err := dkvRepl.Save(reqBts); err != nil {
			t.Error(err)
		} else {
			if res, err := kvs.Get(key); err != nil {
				t.Error(err)
			} else if string(res[0]) != string(val) {
				t.Errorf("Value mismatch for key: %s. Expected: %s, Actual: %s", key, val, res[0])
			}
		}
	}
}

func testGet(t *testing.T, kvs *memStore, dkvRepl db.Store, key []byte) {
	intReq := new(raftpb.InternalRaftRequest)
	intReq.Get = &serverpb.GetRequest{Key: key}
	if reqBts, err := proto.Marshal(intReq); err != nil {
		t.Error(err)
	} else {
		if val, err := dkvRepl.Save(reqBts); err != nil {
			t.Error(err)
		} else {
			if kvsVals, err := kvs.Get(key); err != nil {
				t.Error(err)
			} else if string(val) != string(kvsVals[0]) {
				t.Errorf("Value mismatch for key: %s. Expected: %s, Actual: %s", key, kvsVals[0], val)
			}
		}
	}
}

func testMultiGet(t *testing.T, kvs *memStore, dkvRepl db.Store, keys ...[]byte) {
	intReq := new(raftpb.InternalRaftRequest)
	intReq.MultiGet = &serverpb.MultiGetRequest{Keys: keys}
	if reqBts, err := proto.Marshal(intReq); err != nil {
		t.Error(err)
	} else {
		if vals, err := dkvRepl.Save(reqBts); err != nil {
			t.Error(err)
		} else {
			readResults := make([][]byte, len(keys))
			buf := bytes.NewBuffer(vals)
			if err := gob.NewDecoder(buf).Decode(&readResults); err != nil {
				t.Error(err)
			} else {
				if kvsVals, err := kvs.Get(keys...); err != nil {
					t.Error(err)
				} else {
					for i, readResult := range readResults {
						if string(readResult) != string(kvsVals[i]) {
							t.Errorf("Value mismatch for key: %s. Expected: %s, Actual: %s", keys[i], kvsVals[i], readResult)
						}
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

func (ms *memStore) Put(key []byte, value []byte) error {
	storeKey := string(key)
	if _, present := ms.store[storeKey]; present {
		return errors.New("Given key already exists")
	}
	ms.store[storeKey] = value
	return nil
}

func (ms *memStore) Get(keys ...[]byte) ([][]byte, error) {
	rss := make([][]byte, len(keys))
	for i, key := range keys {
		storeKey := string(key)
		if val, present := ms.store[storeKey]; present {
			rss[i] = val
		} else {
			return nil, errors.New("Given key not found")
		}
	}
	return rss, nil
}

func (ms *memStore) Close() error {
	ms.store = nil
	return nil
}

func (ms *memStore) GetSnapshot() ([]byte, error) {
	return gobEncode(ms.store)
}

func (ms *memStore) PutSnapshot(snap []byte) error {
	data := make(map[string][]byte)
	err := gob.NewDecoder(bytes.NewBuffer(snap)).Decode(data)
	ms.store = data
	return err
}
