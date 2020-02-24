package bench

import (
	"fmt"

	"github.com/flipkart-incubator/dkv/pkg/serverpb"
)

type GetHotKeysBenchmark struct {
	numHotKeys uint
}

func DefaultGetHotKeysBenchmark() *GetHotKeysBenchmark {
	return CreateGetHotKeysBenchmark(numHotKeys)
}

func CreateGetHotKeysBenchmark(numHotKeys uint) *GetHotKeysBenchmark {
	return &GetHotKeysBenchmark{numHotKeys}
}

func (this *GetHotKeysBenchmark) ApiName() string {
	return "dkv.serverpb.DKV.Get"
}

func (this *GetHotKeysBenchmark) CreateRequests(numRequests uint) interface{} {
	var getReqs [][]byte
	for i, j := 0, 0; i < int(numRequests); i, j = i+1, (j+1)%int(this.numHotKeys) {
		key := []byte(fmt.Sprintf("%s%d", ExistingKeyPrefix, j))
		getReqs = append(getReqs, key)
	}
	return getReqs
}

func (this *GetHotKeysBenchmark) String() string {
	return fmt.Sprintf("API: %s, Hot Keys: %d", this.ApiName(), this.numHotKeys)
}

type MultiGetHotKeysBenchmark struct {
	*GetHotKeysBenchmark
	batchSize uint
}

func DefaultMultiGetHotKeysBenchmark() *MultiGetHotKeysBenchmark {
	return CreateMultiGetHotKeysBenchmark(numHotKeys, batchSize)
}

func CreateMultiGetHotKeysBenchmark(numHotKeys, batchSize uint) *MultiGetHotKeysBenchmark {
	if batchSize > numHotKeys {
		panic(fmt.Sprintf("Batch size must be less than or equal to the number of hot keys. Given batchSize: %d, numHotKeys: %d", batchSize, numHotKeys))
	}
	return &MultiGetHotKeysBenchmark{CreateGetHotKeysBenchmark(numHotKeys), batchSize}
}

func (this *MultiGetHotKeysBenchmark) ApiName() string {
	return "dkv.serverpb.DKV.MultiGet"
}

func (this *MultiGetHotKeysBenchmark) CreateRequests(numRequests uint) interface{} {
	var multiGetReqs []*serverpb.MultiGetRequest
	for i, j := 0, 0; i < int(numRequests); i++ {
		var keys [][]byte
		for k := 0; k < int(this.batchSize); k, j = k+1, (j+1)%int(this.numHotKeys) {
			key := []byte(fmt.Sprintf("%s%d", ExistingKeyPrefix, j))
			keys = append(keys, key)
		}
		multiGetReqs = append(multiGetReqs, &serverpb.MultiGetRequest{Keys: keys})
	}
	return multiGetReqs
}

func (this *MultiGetHotKeysBenchmark) String() string {
	return fmt.Sprintf("API: %s, Hot Keys: %d, Batch Size: %d", this.ApiName(), this.numHotKeys, this.batchSize)
}
