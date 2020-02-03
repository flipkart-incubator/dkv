package sync

import (
	"errors"
	"testing"

	"github.com/flipkart-incubator/dkv/internal/server/storage"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
	"github.com/gogo/protobuf/proto"
)

func TestDKVReplStoreSave(t *testing.T) {
	kvs := newMemStore()
	dkv_repl := NewDKVReplStore(kvs)
	key, val := []byte("foo"), []byte("bar")
	put_req := &serverpb.PutRequest{Key: key, Value: val}
	if req_bts, err := proto.Marshal(put_req); err != nil {
		t.Error(err)
	} else {
		if _, err := dkv_repl.Save(req_bts); err != nil {
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

func TestDKVReplStoreClose(t *testing.T) {
	kvs := newMemStore()
	dkv_repl := NewDKVReplStore(kvs)
	if err := dkv_repl.Close(); err != nil {
		t.Error(err)
	} else if kvs.store != nil {
		t.Errorf("Underlying store is expected to be closed but is open")
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
