package storage

import "io"

// Generic result that holds the error if available
// from the storage implementation.
type Result struct {
	Error error
}

// Result of a read from the storage implementation.
type ReadResult struct {
	*Result
	Value []byte
}

// Creates a ReadResult with the given value.
func NewReadResultWithValue(val []byte) *ReadResult {
	return &ReadResult{&Result{Error: nil}, val}
}

// Creates a ReadResult with the given error.
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
