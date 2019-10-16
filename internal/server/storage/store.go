package storage

type Result struct {
	Error error
}

type ReadResult struct {
	*Result
	Value []byte
}

func NewReadResultWithValue(val []byte) *ReadResult {
	return &ReadResult{&Result{Error: nil}, val}
}

func NewReadResultWithError(err error) *ReadResult {
	return &ReadResult{&Result{Error: err}, nil}
}

type KVStore interface {
	Put(key []byte, value []byte) *Result
	Get(keys ...[]byte) []*ReadResult
}
