package bench

import (
	"fmt"

	"github.com/flipkart-incubator/dkv/pkg/serverpb"
)

type putNewKeysBenchmark struct {
	numBytesInValue uint
}

// DefaultPutNewKeysBenchmark returns an instance of a benchmark
// that repeatedly calls the PUT API with the default value size.
func DefaultPutNewKeysBenchmark() Benchmark {
	return CreatePutNewKeysBenchmark(valueSizeInBytes)
}

// CreatePutNewKeysBenchmark returns an instance of a benchmark
// that repeatedly calls the PUT API with the given value size.
func CreatePutNewKeysBenchmark(numBytesInValue uint) Benchmark {
	return &putNewKeysBenchmark{numBytesInValue}
}

func (putBm *putNewKeysBenchmark) APIName() string {
	return "dkv.serverpb.DKV.Put"
}

func (putBm *putNewKeysBenchmark) CreateRequests(numRequests uint) interface{} {
	var putReqs []*serverpb.PutRequest
	for i := 0; i < int(numRequests); i++ {
		key, value := []byte(fmt.Sprintf("%s%d", NewKeyPrefix, i)), randomBytes(putBm.numBytesInValue)
		putReqs = append(putReqs, &serverpb.PutRequest{Key: key, Value: value})
	}
	return putReqs
}

func (putBm *putNewKeysBenchmark) String() string {
	return fmt.Sprintf("API: %s, Value Size: %d bytes", putBm.APIName(), putBm.numBytesInValue)
}

type putModifyKeysBenchmark struct {
	numBytesInValue, numHotKeys uint
}

// DefaultPutModifyKeysBenchmark returns an instance of a benchmark
// that repeatedly calls the PUT API with the default value size and
// number of hot keys.
func DefaultPutModifyKeysBenchmark() Benchmark {
	return CreatePutModifyKeysBenchmark(valueSizeInBytes, numHotKeys)
}

// CreatePutModifyKeysBenchmark returns an instance of a benchmark
// that repeatedly calls the PUT API with the given value size and
// number of hot keys.
func CreatePutModifyKeysBenchmark(numBytesInValue, numHotKeys uint) Benchmark {
	return &putModifyKeysBenchmark{numBytesInValue, numHotKeys}
}

func (putBm *putModifyKeysBenchmark) APIName() string {
	return "dkv.serverpb.DKV.Put"
}

func (putBm *putModifyKeysBenchmark) CreateRequests(numRequests uint) interface{} {
	var putReqs []*serverpb.PutRequest
	for i, j := 0, 0; i < int(numRequests); i, j = i+1, (j+1)%int(putBm.numHotKeys) {
		key, value := []byte(fmt.Sprintf("%s%d", ExistingKeyPrefix, j)), randomBytes(putBm.numBytesInValue)
		putReqs = append(putReqs, &serverpb.PutRequest{Key: key, Value: value})
	}
	return putReqs
}

func (putBm *putModifyKeysBenchmark) String() string {
	return fmt.Sprintf("API: %s, Value Size: %d bytes, Hot Keys: %d", putBm.APIName(), putBm.numBytesInValue, putBm.numHotKeys)
}
