package bench

import (
	"fmt"

	"github.com/flipkart-incubator/dkv/pkg/serverpb"
)

type getHotKeysBenchmark struct {
	numHotKeys uint
}

// DefaultGetHotKeysBenchmark returns an instance of a benchmark
// that performs repeated GETs on a subset of keys.
func DefaultGetHotKeysBenchmark() Benchmark {
	return CreateGetHotKeysBenchmark(numHotKeys)
}

// CreateGetHotKeysBenchmark returns an instance of a benchmark
// that performs repeated GETs on a given number of keys.
func CreateGetHotKeysBenchmark(numHotKeys uint) Benchmark {
	return &getHotKeysBenchmark{numHotKeys}
}

func (getBm *getHotKeysBenchmark) APIName() string {
	return "dkv.serverpb.DKV.Get"
}

func (getBm *getHotKeysBenchmark) CreateRequests(numRequests uint) interface{} {
	var getReqs [][]byte
	for i, j := 0, 0; i < int(numRequests); i, j = i+1, (j+1)%int(getBm.numHotKeys) {
		key := []byte(fmt.Sprintf("%s%d", ExistingKeyPrefix, j))
		getReqs = append(getReqs, key)
	}
	return getReqs
}

func (getBm *getHotKeysBenchmark) String() string {
	return fmt.Sprintf("API: %s, Hot Keys: %d", getBm.APIName(), getBm.numHotKeys)
}

type multiGetHotKeysBenchmark struct {
	numHotKeys, batchSize uint
}

// DefaultMultiGetHotKeysBenchmark returns an instance of a benchmark
// that repeatedly calls MultiGet API on a subset of keys.
func DefaultMultiGetHotKeysBenchmark() Benchmark {
	return CreateMultiGetHotKeysBenchmark(numHotKeys, batchSize)
}

// CreateMultiGetHotKeysBenchmark returns an instance of a benchmark
// that repeatedly calls MultiGet API with the given batch size and
// the number of keys.
func CreateMultiGetHotKeysBenchmark(numHotKeys, batchSize uint) Benchmark {
	if batchSize > numHotKeys {
		panic(fmt.Sprintf("Batch size must be less than or equal to the number of hot keys. Given batchSize: %d, numHotKeys: %d", batchSize, numHotKeys))
	}
	return &multiGetHotKeysBenchmark{numHotKeys, batchSize}
}

func (getBm *multiGetHotKeysBenchmark) APIName() string {
	return "dkv.serverpb.DKV.MultiGet"
}

func (getBm *multiGetHotKeysBenchmark) CreateRequests(numRequests uint) interface{} {
	var multiGetReqs []*serverpb.MultiGetRequest
	for i, j := 0, 0; i < int(numRequests); i++ {
		var keys [][]byte
		for k := 0; k < int(getBm.batchSize); k, j = k+1, (j+1)%int(getBm.numHotKeys) {
			key := []byte(fmt.Sprintf("%s%d", ExistingKeyPrefix, j))
			keys = append(keys, key)
		}
		multiGetReqs = append(multiGetReqs, &serverpb.MultiGetRequest{Keys: keys})
	}
	return multiGetReqs
}

func (getBm *multiGetHotKeysBenchmark) String() string {
	return fmt.Sprintf("API: %s, Hot Keys: %d, Batch Size: %d", getBm.APIName(), getBm.numHotKeys, getBm.batchSize)
}
