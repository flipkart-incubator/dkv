package bench

import (
	"fmt"

	"github.com/flipkart-incubator/dkv/pkg/serverpb"
)

type PutNewKeysBenchmark struct {
	numBytesInValue uint
}

func DefaultPutNewKeysBenchmark() *PutNewKeysBenchmark {
	return CreatePutNewKeysBenchmark(valueSizeInBytes)
}

func CreatePutNewKeysBenchmark(numBytesInValue uint) *PutNewKeysBenchmark {
	return &PutNewKeysBenchmark{numBytesInValue}
}

func (this *PutNewKeysBenchmark) ApiName() string {
	return "dkv.serverpb.DKV.Put"
}

func (this *PutNewKeysBenchmark) CreateRequests(numRequests uint) interface{} {
	var putReqs []*serverpb.PutRequest
	for i := 0; i < int(numRequests); i++ {
		key, value := []byte(fmt.Sprintf("NewKey%d", i)), randomBytes(this.numBytesInValue)
		putReqs = append(putReqs, &serverpb.PutRequest{key, value})
	}
	return putReqs
}

type PutModifyKeysBenchmark struct {
	numBytesInValue, numHotKeys uint
}

func DefaultPutModifyKeysBenchmark() *PutModifyKeysBenchmark {
	return CreatePutModifyKeysBenchmark(valueSizeInBytes, numHotKeys)
}

func CreatePutModifyKeysBenchmark(numBytesInValue, numHotKeys uint) *PutModifyKeysBenchmark {
	return &PutModifyKeysBenchmark{numBytesInValue, numHotKeys}
}

func (this *PutModifyKeysBenchmark) ApiName() string {
	return "dkv.serverpb.DKV.Put"
}

func (this *PutModifyKeysBenchmark) CreateRequests(numRequests uint) interface{} {
	var putReqs []*serverpb.PutRequest
	for i, j := 0, 0; i < int(numRequests); i, j = i+1, (j+1)%int(this.numHotKeys) {
		key, value := []byte(fmt.Sprintf("ExistingKey%d", j)), randomBytes(this.numBytesInValue)
		putReqs = append(putReqs, &serverpb.PutRequest{key, value})
	}
	return putReqs
}
