package slave

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/flipkart-incubator/dkv/pkg/serverpb"
	"google.golang.org/grpc"
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

func TestSlaveReplication(t *testing.T) {
	ms := newMemStore()
	repl_cli := newMockReplClient()

	num_chngs, k_prfx, v_prfx := 1000, "SRK", "SRV"
	for i := 1; i <= num_chngs; i++ {
		k, v := fmt.Sprintf("%s%d", k_prfx, i), fmt.Sprintf("%s%d", v_prfx, i)
		repl_cli.appendPutChange([]byte(k), []byte(v))
	}

	ss := newSlaveService(ms, ms, repl_cli, 1*time.Millisecond, 10*time.Second)
	<-time.After(1 * time.Second)
	for i := 1; i <= num_chngs; i++ {
		k, v := fmt.Sprintf("%s%d", k_prfx, i), fmt.Sprintf("%s%d", v_prfx, i)
		result, err := ss.Get(context.TODO(), &serverpb.GetRequest{Key: []byte(k)})
		if err != nil || result.Status.Code != 0 {
			t.Fatalf("Slave service GET resulted in error. Key: %s, Message: %s, Error: %v", k, result.Status.Message, err)
		} else if string(result.Value) != v {
			t.Errorf("Mismatch in slave service GET values. Key: %s, Expected: %s, Actual: %s", k, v, result.Value)
		}
	}
}

type memStore struct {
	store   map[string][]byte
	chngNum uint64
	mu      *sync.Mutex
}

func newMemStore() *memStore {
	return &memStore{store: make(map[string][]byte), mu: &sync.Mutex{}}
}

func (ms *memStore) Put(key []byte, value []byte) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	storeKey := string(key)
	if _, present := ms.store[storeKey]; present {
		return errors.New("Given key already exists")
	} else {
		ms.store[storeKey] = value
		ms.chngNum++
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

func (ms *memStore) SaveChanges(changes []*serverpb.ChangeRecord) (uint64, error) {
	var chng_num uint64
	for _, chng := range changes {
		for _, trxn := range chng.Trxns {
			if trxn.Type == serverpb.TrxnRecord_Put {
				ms.Put(trxn.Key, trxn.Value)
			}
		}
		chng_num = chng.ChangeNumber
	}
	return chng_num, nil
}

func (ms *memStore) GetLatestAppliedChangeNumber() (uint64, error) {
	return ms.chngNum, nil
}

func (ms *memStore) Close() error {
	ms.store = nil
	ms.mu = nil
	ms.chngNum = 0
	return nil
}

// mockReplClient allows for appending change record into a slice
// and provides access to these change records through the change
// number equal to 1 + slice_index
type mockReplClient struct {
	chngs []*serverpb.ChangeRecord
	mu    *sync.Mutex
}

func newMockReplClient() *mockReplClient {
	return &mockReplClient{mu: &sync.Mutex{}}
}

func (rc *mockReplClient) appendPutChange(key, val []byte) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	chng := new(serverpb.ChangeRecord)
	chng.ChangeNumber = uint64(len(rc.chngs) + 1)
	chng.NumberOfTrxns = 1
	chng.Trxns = []*serverpb.TrxnRecord{new(serverpb.TrxnRecord)}
	chng.Trxns[0].Type, chng.Trxns[0].Key, chng.Trxns[0].Value = serverpb.TrxnRecord_Put, key, val
	rc.chngs = append(rc.chngs, chng)
}

func (rc *mockReplClient) GetChanges(ctx context.Context, req *serverpb.GetChangesRequest, opts ...grpc.CallOption) (*serverpb.GetChangesResponse, error) {
	num_all_chngs := uint32(len(rc.chngs))
	master_chng_num := rc.chngs[num_all_chngs-1].ChangeNumber
	res := new(serverpb.GetChangesResponse)
	res.Status = new(serverpb.Status)
	res.MasterChangeNumber = master_chng_num
	if req.FromChangeNumber <= master_chng_num {
		res.NumberOfChanges = min(req.MaxNumberOfChanges, num_all_chngs)
		si, ei := int(req.FromChangeNumber-1), int(req.FromChangeNumber-1)+int(res.NumberOfChanges)
		res.Changes = rc.chngs[si:ei]
	}
	return res, nil
}

func min(a, b uint32) uint32 {
	if a < b {
		return a
	}
	return b
}
