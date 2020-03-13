package bench

import (
	"flag"
	"math/rand"
)

// Benchmark represents the behavior required for
// benchmarking DKV services. Each instance represents
// the benchmark for a given DKV API.
type Benchmark interface {
	CreateRequests(numRequests uint) interface{}
	APIName() string
	String() string
}

const (
	// NewKeyPrefix is the prefix applied on keys for insert benchmarks
	NewKeyPrefix = "NewKey"
	// ExistingKeyPrefix is the prefix applied on keys for update benchmarks
	ExistingKeyPrefix = "ExistingKey"
)

var (
	valueSizeInBytes uint
	numHotKeys       uint
	batchSize        uint
)

func init() {
	flag.UintVar(&valueSizeInBytes, "valueSizeInBytes", 10, "Size of every value in bytes")
	flag.UintVar(&numHotKeys, "numHotKeys", 100, "Number of keys that are repeatedly read or updated")
	flag.UintVar(&batchSize, "batchSize", 10, "Batch size for GetAll requests")
}

func randomBytes(size uint) []byte {
	res := make([]byte, size)
	for i := 0; i < int(size); i++ {
		res[i] = byte(rand.Intn(129))
	}
	return res
}
