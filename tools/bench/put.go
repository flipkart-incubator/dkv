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
		key, value := []byte(fmt.Sprintf("%s%d", NewKeyPrefix, i)), randomBytes(this.numBytesInValue)
		putReqs = append(putReqs, &serverpb.PutRequest{Key: key, Value: value})
	}
	return putReqs
}

func (this *PutNewKeysBenchmark) String() string {
	return fmt.Sprintf("API: %s, Value Size: %d bytes", this.ApiName(), this.numBytesInValue)
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
		key, value := []byte(fmt.Sprintf("%s%d", ExistingKeyPrefix, j)), randomBytes(this.numBytesInValue)
		putReqs = append(putReqs, &serverpb.PutRequest{Key: key, Value: value})
	}
	return putReqs
}

func (this *PutModifyKeysBenchmark) String() string {
	return fmt.Sprintf("API: %s, Value Size: %d bytes, Hot Keys: %d", this.ApiName(), this.numBytesInValue, this.numHotKeys)
}
