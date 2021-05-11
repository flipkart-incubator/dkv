package utils

import (
	"github.com/flipkart-incubator/dkv/internal/storage"
)

type ConcatenatedIterator struct {
	iterators   []storage.Iterator
	currentIter int
}

func (ci *ConcatenatedIterator) HasNext() bool {
	//non prefix use-case
	valid := ci.iterators[ci.currentIter].HasNext()
	if !valid && ci.currentIter < len(ci.iterators)-1 {
		ci.currentIter++
		return ci.HasNext()
	}
	return valid
}

func (ci *ConcatenatedIterator) Next() ([]byte, []byte) {
	return ci.iterators[ci.currentIter].Next()
}

func (ci *ConcatenatedIterator) Err() error {
	return ci.iterators[ci.currentIter].Err()
}

func (ci *ConcatenatedIterator) Close() error {
	for _, iterator := range ci.iterators {
		iterator.Close()
	}
	return nil
}

// Concat Concatenates multiple iterators together in one.
// d := iter.Concat(a, b, c)
func Concat(iterators ...storage.Iterator) storage.Iterator {
	return &ConcatenatedIterator{
		currentIter: 0,
		iterators:   iterators,
	}
}
