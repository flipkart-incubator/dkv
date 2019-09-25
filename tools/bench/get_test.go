package bench

import (
	"testing"
)

func BenchmarkGetKey(b *testing.B) {
	hotKeys := loadAndGetHotKeys(b)
	b.SetParallelism(parallelism)
	b.RunParallel(func(pb *testing.PB) {
		for i, j := 0, 0; pb.Next(); i, j = i+1, (j+1)%numHotKeys {
			key := hotKeys[j]
			if _, err := dkvCli.Get(key); err != nil {
				b.Fatalf("Unable to GET. Key: %s, Error: %v", key, err)
			}
		}
	})
}

func BenchmarkGetMissingKey(b *testing.B) {
	key := "BMissingKey"
	b.SetParallelism(parallelism)
	b.RunParallel(func(pb *testing.PB) {
		for i := 0; pb.Next(); i++ {
			if _, err := dkvCli.Get([]byte(key)); err != nil {
				b.Fatalf("Unable to GET. Key: %s, Error: %v", key, err)
			}
		}
	})
}
