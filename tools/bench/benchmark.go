package bench

import (
	"flag"
	"math/rand"
)

type Benchmark interface {
	CreateRequests(numRequests uint) interface{}
	ApiName() string
	String() string
}

const (
	NewKeyPrefix      = "NewKey"
	ExistingKeyPrefix = "ExistingKey"
)

var (
	valueSizeInBytes uint
	numHotKeys       uint
	batchSize        uint
)

func init() {
	flag.UintVar(&valueSizeInBytes, "valueSizeInBytes", 10, "Size of every value in bytes")
	flag.UintVar(&numHotKeys, "numHotKeys", 100, "Number of keys that are repeatedly read")
	flag.UintVar(&batchSize, "batchSize", 10, "Batch size for multi get requests")
}

func randomBytes(size uint) []byte {
	res := make([]byte, size)
	for i := 0; i < int(size); i++ {
		res[i] = byte(rand.Intn(129))
	}
	return res
}
