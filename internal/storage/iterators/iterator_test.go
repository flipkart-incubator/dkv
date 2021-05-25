package iterators

import (
	"testing"
)

type simpleIterator struct {
	data       []string
	currentPos int
}

func (si *simpleIterator) HasNext() bool {
	//non prefix use-case
	return si.currentPos < len(si.data)
}

func (si *simpleIterator) Next() ([]byte, []byte) {
	d := si.data[si.currentPos]
	si.currentPos++
	return []byte(d), []byte(d)
}

func (si *simpleIterator) Err() error {
	return nil
}

func (si *simpleIterator) Close() error {
	return nil
}

func TestIterationConcat(t *testing.T) {
	iter1 := &simpleIterator{
		data:       []string{"one", "two", "three"},
		currentPos: 0,
	}

	iter2 := &simpleIterator{
		data:       []string{"alpha", "beta", "gamma"},
		currentPos: 0,
	}

	all := []string{"one", "two", "three", "alpha", "beta", "gamma"}

	iter3 := Concat(iter1, iter2)
	count := 0

	for iter3.HasNext() {
		k, _ := iter3.Next()
		kS := string(k)
		aI := all[count]

		if aI != kS {
			t.Errorf("Expected %s  But got : %s", aI, kS)
		}
		count++
	}

	if count != 6 {
		t.Errorf("Expected count of 6  But got : %d", count)
	}

}
