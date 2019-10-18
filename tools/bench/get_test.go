package bench

import (
	"fmt"
	"testing"

	"github.com/flipkart-incubator/dkv/pkg/serverpb"
)

const (
	hotKeyCnt       = 9
	reqCnt          = 101
	numReqsPerBatch = 4
)

func checkKeys(t *testing.T, getReqs []*serverpb.GetRequest) {
	j := 0
	for i, getReq := range getReqs {
		if i%hotKeyCnt == 0 {
			j = 0
		} else {
			j++
		}
		exp_key := fmt.Sprintf("%s%d", ExistingKeyPrefix, j)
		act_key := string(getReq.Key)
		if exp_key != act_key {
			t.Errorf("Key mismatch. Expected key: %s, Actual key: %s", exp_key, act_key)
		}
	}
}

func TestGetHotKeysBenchmark(t *testing.T) {
	bm := CreateGetHotKeysBenchmark(hotKeyCnt)
	getReqs := bm.CreateRequests(reqCnt).([]*serverpb.GetRequest)
	numGetReqs := len(getReqs)
	if numGetReqs != reqCnt {
		t.Errorf("Expected number of get requests: %d. Actual: %d", reqCnt, numGetReqs)
	}
	checkKeys(t, getReqs)
}

func TestMultiGetHotKeysBenchmark(t *testing.T) {
	bm := CreateMultiGetHotKeysBenchmark(hotKeyCnt, numReqsPerBatch)
	multiGetReqs := bm.CreateRequests(reqCnt).([]*serverpb.MultiGetRequest)
	numMGetReqs := len(multiGetReqs)
	if numMGetReqs != reqCnt {
		t.Errorf("Expected number of multi get requests: %d. Actual: %d", reqCnt, numMGetReqs)
	}
	numGetReqCnt := numReqsPerBatch * numMGetReqs
	getReqs := make([]*serverpb.GetRequest, numGetReqCnt)
	for i, j := 0, -1; i < numGetReqCnt; i++ {
		k := i % numReqsPerBatch
		if k == 0 {
			j++
		}
		getReqs[i] = multiGetReqs[j].GetRequests[k]
	}
	checkKeys(t, getReqs)
}
