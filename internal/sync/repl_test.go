package sync

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"sync"
	"testing"

	"github.com/flipkart-incubator/dkv/internal/hlc"
	"github.com/flipkart-incubator/dkv/internal/storage"
	"github.com/flipkart-incubator/dkv/internal/sync/raftpb"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
	"github.com/flipkart-incubator/nexus/pkg/db"
	"github.com/gogo/protobuf/proto"
)

func TestDKVReplStoreSave(t *testing.T) {
	kvs := newMemStore()
	dkvRepl := NewDKVReplStore(kvs)

	term, index := uint64(1), uint64(1)
	testPut(t, kvs, dkvRepl, []byte("foo"), []byte("bar"), term, index)
	testPut(t, kvs, dkvRepl, []byte("hello"), []byte("world"), term, index + 1)
	testPut(t, kvs, dkvRepl, []byte("kit"), []byte("kat"), term, index + 2)

	testGet(t, kvs, dkvRepl, []byte("foo"))
	testGet(t, kvs, dkvRepl, []byte("hello"))
	testGet(t, kvs, dkvRepl, []byte("kit"))

	testMultiGet(t, kvs, dkvRepl, []byte("foo"), []byte("hello"), []byte("kit"))

	testDelete(t, kvs, dkvRepl, []byte("foo"), term, index + 3)
	testDelete(t, kvs, dkvRepl, []byte("hello"), term, index + 4)
	testDelete(t, kvs, dkvRepl, []byte("kit"), term, index + 5)
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

func testPut(t *testing.T, kvs *memStore, dkvRepl db.Store, key, val []byte, term, index uint64) {
	intReq := new(raftpb.InternalRaftRequest)
	intReq.Put = &serverpb.PutRequest{Key: key, Value: val}
	if reqBts, err := proto.Marshal(intReq); err != nil {
		t.Error(err)
	} else {
		if _, err := dkvRepl.Save(db.RaftEntry{term, index}, reqBts); err != nil {
			t.Error(err)
		} else {
			if res, err := kvs.Get(key); err != nil {
				t.Error(err)
			} else if string(res[0].Value) != string(val) {
				t.Errorf("Value mismatch for key: %s. Expected: %s, Actual: %s", key, val, res[0].Value)
			}
			checkRAFTEntry(t, kvs, term, index)
		}
	}
}

func testDelete(t *testing.T, kvs *memStore, dkvRepl db.Store, key []byte, term, index uint64) {
	intReq := new(raftpb.InternalRaftRequest)
	intReq.Delete = &serverpb.DeleteRequest{Key: key}
	if reqBts, err := proto.Marshal(intReq); err != nil {
		t.Error(err)
	} else {
		if _, err := dkvRepl.Save(db.RaftEntry{term, index}, reqBts); err != nil {
			t.Error(err)
		} else {
			if _, err := kvs.Get(key); err.Error() != "Given key not found" {
				t.Error(err)
			}
		}
		checkRAFTEntry(t, kvs, term, index)
	}
}

func checkRAFTEntry(t *testing.T, kvs *memStore, term, index uint64) {
	if res, err := kvs.Get([]byte(raftMeta)); err != nil {
		t.Error(err)
	} else {
		exp := fmt.Sprintf("%d%c%d", term, raftMetaDelim, index)
		if string(res[0].Value) != exp {
			t.Errorf("Mismatch in RAFT entry. Expected: %s, Actual: %s", exp, res[0].Value)
		}
	}
}

func testGet(t *testing.T, kvs *memStore, dkvRepl db.Store, key []byte) {
	intReq := new(raftpb.InternalRaftRequest)
	intReq.Get = &serverpb.GetRequest{Key: key}
	if reqBts, err := proto.Marshal(intReq); err != nil {
		t.Error(err)
	} else {
		if val, err := dkvRepl.Load(reqBts); err != nil {
			t.Error(err)
		} else {
			if kvsVals, err := kvs.Get(key); err != nil {
				t.Error(err)
			} else {
				readResults := make([]*serverpb.KVPair, 1)
				buf := bytes.NewBuffer(val)
				if err := gob.NewDecoder(buf).Decode(&readResults); err != nil {
					t.Error(err)
				} else {
					if string(readResults[0].Value) != string(kvsVals[0].Value) {
						t.Errorf("Value mismatch for key: %s. Expected: %s, Actual: %s", key, kvsVals[0].Value, readResults[0].Value)
					}
				}
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
		if vals, err := dkvRepl.Load(reqBts); err != nil {
			t.Error(err)
		} else {
			readResults := make([]*serverpb.KVPair, len(keys))
			buf := bytes.NewBuffer(vals)
			if err := gob.NewDecoder(buf).Decode(&readResults); err != nil {
				t.Error(err)
			} else {
				if kvsVals, err := kvs.Get(keys...); err != nil {
					t.Error(err)
				} else {
					for i, readResult := range readResults {
						if string(readResult.Value) != string(kvsVals[i].Value) {
							t.Errorf("Value mismatch for key: %s. Expected: %s, Actual: %s", keys[i], kvsVals[i].Value, readResult.Value)
						}
					}
				}
			}
		}
	}
}

type memStore struct {
	store map[string]memStoreObject
	mu    sync.Mutex
}

type memStoreObject struct {
	data     []byte
	expiryTS uint64
}

func newMemStore() *memStore {
	return &memStore{store: make(map[string]memStoreObject), mu: sync.Mutex{}}
}

func (ms *memStore) Put(pairs ...*serverpb.KVPair) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	for _, kv := range pairs {
		storeKey := string(kv.Key)
		if storeKey == raftMeta {
			ms.store[storeKey] = memStoreObject{kv.Value, 0}
			continue
		}
		if _, present := ms.store[storeKey]; present {
			return errors.New("given key already exists")
		}
		ms.store[storeKey] = memStoreObject{kv.Value, 0}
	}

	return nil
}

func (ms *memStore) Delete(key []byte) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	storeKey := string(key)
	delete(ms.store, storeKey)
	return nil
}

func (ms *memStore) Get(keys ...[]byte) ([]*serverpb.KVPair, error) {
	rss := make([]*serverpb.KVPair, len(keys))
	for i, key := range keys {
		storeKey := string(key)
		if val, present := ms.store[storeKey]; present {
			var v []byte
			if val.expiryTS == 0 || val.expiryTS > hlc.UnixNow() {
				v = val.data
			}
			rss[i] = &serverpb.KVPair{Key: key, Value: v}
		} else {
			return nil, errors.New("Given key not found")
		}
	}
	return rss, nil
}

func (ms *memStore) CompareAndSet(key, expect, update []byte) (bool, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	storeKey := string(key)
	exist, present := ms.store[storeKey]
	if (!present && expect != nil) || (present && expect == nil) {
		return false, nil
	}

	if !present && expect == nil || bytes.Equal(expect, exist.data) {
		ms.store[storeKey] = memStoreObject{update, 0}
	}
	return true, nil
}

func (ms *memStore) Close() error {
	ms.store = nil
	return nil
}

func (ms *memStore) GetSnapshot() (io.ReadCloser, error) {
	snap, err := gobEncode(ms.store)
	if err != nil {
		return nil, err
	}
	return ioutil.NopCloser(bytes.NewBuffer(snap)), nil
}

func (ms *memStore) PutSnapshot(snap io.ReadCloser) error {
	data := make(map[string]memStoreObject)
	err := gob.NewDecoder(snap).Decode(data)
	ms.store = data
	return err
}

func (ms *memStore) Iterate(iterOpts storage.IterationOptions) storage.Iterator {
	return nil
}
