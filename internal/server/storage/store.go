package storage

import "io"

// A Result instance holds the error if available from the
// storage implementation.
type Result struct {
	Error error
}

// A ReadResult instance of a read from the storage implementation.
type ReadResult struct {
	*Result
	Value []byte
}

// NewReadResultWithValue creates a ReadResult with the given value.
func NewReadResultWithValue(val []byte) *ReadResult {
	return &ReadResult{&Result{Error: nil}, val}
}

// NewReadResultWithError creates a ReadResult with the given error.
func NewReadResultWithError(err error) *ReadResult {
	return &ReadResult{&Result{Error: err}, nil}
}

// A KVStore represents the key value store that provides
// the underlying storage implementation for the various
// DKV operations.
type KVStore interface {
	io.Closer
	Put(key []byte, value []byte) *Result
	Get(keys ...[]byte) []*ReadResult
}
