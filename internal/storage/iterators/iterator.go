package iterators

import (
	"github.com/flipkart-incubator/dkv/internal/storage"
)

type concatenatedIterator struct {
	iterators   []storage.Iterator
	currentIter int
}

func (ci *concatenatedIterator) HasNext() bool {
	valid := ci.iterators[ci.currentIter].HasNext()
	if !valid && ci.currentIter < len(ci.iterators)-1 {
		ci.currentIter++
		return ci.HasNext()
	}
	return valid
}

func (ci *concatenatedIterator) Next() ([]byte, []byte) {
	return ci.iterators[ci.currentIter].Next()
}

func (ci *concatenatedIterator) Err() error {
	return ci.iterators[ci.currentIter].Err()
}

func (ci *concatenatedIterator) Close() error {
	for _, iterator := range ci.iterators {
		iterator.Close()
	}
	return nil
}

// Concat Concatenates multiple iterators together in one.
// d := iter.Concat(a, b, c)
func Concat(iterators ...storage.Iterator) storage.Iterator {
	if iterators == nil || len(iterators) == 0 {
		return nil
	}

	return &concatenatedIterator{
		currentIter: 0,
		iterators:   iterators,
	}
}
