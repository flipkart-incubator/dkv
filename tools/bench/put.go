package bench

import (
	"fmt"

	"github.com/flipkart-incubator/dkv/pkg/serverpb"
)

type PutNewKeysBenchmark struct{}

func (this *PutNewKeysBenchmark) ApiName() string {
	return "dkv.serverpb.DKV.Put"
}

func (this *PutNewKeysBenchmark) CreateRequests(numRequests uint) interface{} {
	var putReqs []*serverpb.PutRequest
	for i := 0; i < int(numRequests); i++ {
		key, value := []byte(fmt.Sprintf("NewKey%d", i)), randomBytes(valueSizeInBytes)
		putReqs = append(putReqs, &serverpb.PutRequest{key, value})
	}
	return putReqs
}

type PutModifyKeysBenchmark struct{}

func (this *PutModifyKeysBenchmark) ApiName() string {
	return "dkv.serverpb.DKV.Put"
}

func (this *PutModifyKeysBenchmark) CreateRequests(numRequests uint) interface{} {
	var putReqs []*serverpb.PutRequest
	for i, j := 0, 0; i < int(numRequests); i, j = i+1, (j+1)%int(numHotKeys) {
		key, value := []byte(fmt.Sprintf("ExistingKey%d", j)), randomBytes(valueSizeInBytes)
		putReqs = append(putReqs, &serverpb.PutRequest{key, value})
	}
	return putReqs
}
