package storage

import (
	"bytes"
	"errors"
	"io"

	"github.com/flipkart-incubator/dkv/pkg/serverpb"
)

// IterationOptions captures the various options that can
// be set to control iteration process.
type IterationOptions interface {
	KeyPrefix() ([]byte, bool)
	StartKey() ([]byte, bool)
}

type iterOpts struct {
	keyPrefix []byte
	startKey  []byte
}

func (io *iterOpts) KeyPrefix() ([]byte, bool) {
	return io.keyPrefix, io.keyPrefix != nil && len(io.keyPrefix) > 0
}

func (io *iterOpts) StartKey() ([]byte, bool) {
	return io.startKey, io.startKey != nil && len(io.startKey) > 0
}

func (io *iterOpts) validate() error {
	if kp, kpPrsnt := io.KeyPrefix(); kpPrsnt {
		if sk, skPrsnt := io.StartKey(); skPrsnt {
			if !bytes.HasPrefix(sk, kp) {
				return errors.New("StartKey must have the same prefix as PrefixKey")
			}
		}
	}
	return nil
}

// IterationOption captures an iteration option.
type IterationOption func(*iterOpts)

// NewIteratorOptions allows for the creation of `IterationOptions` instance
// that is used for controlling the behavior of iterating through keyspace.
func NewIteratorOptions(opts ...IterationOption) (IterationOptions, error) {
	itOpts := new(iterOpts)
	for _, opt := range opts {
		opt(itOpts)
	}

	return itOpts, itOpts.validate()
}

// IterationPrefixKey is used to indicate the prefix of the keys
// that are to be iterated. In other words, only those keys that
// begin with this prefix are returned by the iterator.
func IterationPrefixKey(prefix []byte) IterationOption {
	return func(opts *iterOpts) {
		opts.keyPrefix = prefix
	}
}

// IterationStartKey sets the start key for iteration. All keys
// starting with this key are returned by the iterator.
func IterationStartKey(start []byte) IterationOption {
	return func(opts *iterOpts) {
		opts.startKey = start
	}
}

// Iterator represents the behavior of a key space iterator
// that allows for iterating though keys using the `HasNext`
// and `Next` methods.
type Iterator interface {
	io.Closer
	HasNext() bool
	Next() *serverpb.KVPair
	Err() error
}

// Iteration is a convenience wrapper around `Iterator`
// that allows for a given handler to be invoked exactly
// once for every key value pair iterated.
type Iteration interface {
	ForEach(func(*serverpb.KVPair) error) error
}

type iteration struct {
	kvs  KVStore
	opts *iterOpts
}

func (iter *iteration) ForEach(hndlr func(*serverpb.KVPair) error) error {
	if err := iter.opts.validate(); err != nil {
		return err
	}

	itrtr := iter.kvs.Iterate(iter.opts)
	defer itrtr.Close()
	for itrtr.HasNext() {
		if err := hndlr(itrtr.Next()); err != nil {
			return err
		}
	}
	return itrtr.Err()
}

// NewIteration allows for the creation of an `Iteration` instance
// that uses the underlying store's Iterator to callback for every
// key value pair iterated.
func NewIteration(kvs KVStore, iterReq *serverpb.IterateRequest) Iteration {
	itOpts := &iterOpts{iterReq.KeyPrefix, iterReq.StartKey}
	return &iteration{kvs, itOpts}
}
