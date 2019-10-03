package bench

import (
	"flag"
	"math/rand"
)

type Benchmark interface {
	CreateRequests(numRequests uint) interface{}
	ApiName() string
}

var (
	valueSizeInBytes uint
	numHotKeys       uint
)

func init() {
	flag.UintVar(&valueSizeInBytes, "valueSizeInBytes", 10, "Size of every value in bytes")
	flag.UintVar(&numHotKeys, "numHotKeys", 100, "Number of keys that are repeatedly read")
}

func randomBytes(size uint) []byte {
	res := make([]byte, size)
	for i := 0; i < int(size); i++ {
		res[i] = byte(rand.Intn(129))
	}
	return res
}
