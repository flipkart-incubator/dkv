package bench

import (
	"fmt"
	"testing"
)

func BenchmarkPutNewKeys(b *testing.B) {
	b.SetParallelism(parallelism)
	b.RunParallel(func(pb *testing.PB) {
		for i := 0; pb.Next(); i++ {
			key, value := []byte(fmt.Sprintf("BK%d", i)), randomBytes(valueSizeInBytes)
			if err := dkvCli.Put(key, value); err != nil {
				b.Fatalf("Unable to PUT. Key: %s, Value: %v, Error: %v", key, value, err)
			}
		}
	})
}

func BenchmarkPutExistingKey(b *testing.B) {
	hotKeys := loadAndGetHotKeys(b)
	b.SetParallelism(parallelism)
	b.RunParallel(func(pb *testing.PB) {
		for i, j := 0, 0; pb.Next(); i, j = i+1, (j+1)%numHotKeys {
			key := hotKeys[j]
			value := randomBytes(valueSizeInBytes)
			if err := dkvCli.Put(key, value); err != nil {
				b.Fatalf("Unable to PUT. Key: %s, Value: %v, Error: %v", key, value, err)
			}
		}
	})
}
