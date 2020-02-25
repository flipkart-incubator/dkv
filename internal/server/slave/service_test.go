package slave

import (
	"context"
	"errors"
	"testing"

	"github.com/flipkart-incubator/dkv/pkg/serverpb"
)

func TestSlaveServiceCreationFailures(t *testing.T) {
	masterAddr, replTimeoutSecs, replPollIntervalSecs = "", 10, 1
	if err := validateFlags(); err == nil {
		t.Error("Expected error for not setting masterAddr, instead got no error")
	}

	masterAddr, replTimeoutSecs, replPollIntervalSecs = "host:1000", 0, 1
	if err := validateFlags(); err == nil {
		t.Error("Expected error for not setting replTimeoutSecs, instead got no error")
	}

	masterAddr, replTimeoutSecs, replPollIntervalSecs = "host:1000", 10, 0
	if err := validateFlags(); err == nil {
		t.Error("Expected error for not setting replPollIntervalSecs, instead got no error")
	}

	masterAddr, replTimeoutSecs, replPollIntervalSecs = "host:1000", 10, 1
	if err := validateFlags(); err != nil {
		t.Errorf("Expected no error, instead got an error: %v", err)
	}
}

func TestSlaveServiceStoreOps(t *testing.T) {
	ss := new(dkvSlaveService)
	if _, err := ss.Put(context.Background(), new(serverpb.PutRequest)); err == nil {
		t.Error("Expected an error to indicate slave service does not handle PUT, but got no error")
	} else {
		t.Log(err)
	}

	ss.store = newMemStore()
	k1, k2, k3 := []byte("k1"), []byte("k2"), []byte("k3")
	v1, v2, v3 := []byte("v1"), []byte("v2"), []byte("v3")
	ss.store.Put(k1, v1)
	ss.store.Put(k2, v2)
	ss.store.Put(k3, v3)

	result, err := ss.Get(context.TODO(), &serverpb.GetRequest{Key: k1})
	if err != nil || result.Status.Code != 0 {
		t.Fatalf("Slave service GET resulted in error. Message: %s, Error: %v", result.Status.Message, err)
	} else if string(result.Value) != string(v1) {
		t.Errorf("Mismatch in slave service GET values. Key: %s, Expected: %s, Actual: %s", k1, v1, result.Value)
	}

	bulk_result, err := ss.MultiGet(context.TODO(), &serverpb.MultiGetRequest{Keys: [][]byte{k2, k3}})
	if err != nil || bulk_result.Status.Code != 0 {
		t.Fatalf("Slave service GET resulted in error. Message: %s, Error: %v", result.Status.Message, err)
	} else {
		if string(bulk_result.Values[0]) != string(v2) {
			t.Errorf("Mismatch in slave service multi GET values. Key: %s, Expected: %s, Actual: %s", k2, v2, bulk_result.Values[0])
		}
		if string(bulk_result.Values[1]) != string(v3) {
			t.Errorf("Mismatch in slave service multi GET values. Key: %s, Expected: %s, Actual: %s", k3, v3, bulk_result.Values[1])
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
	} else {
		ms.store[storeKey] = value
		return nil
	}
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
