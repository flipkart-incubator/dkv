package storage

import (
	"bytes"
	"errors"
	"io"

	"github.com/flipkart-incubator/dkv/pkg/serverpb"
)

type IterationOptions interface {
	KeyPrefix() ([]byte, bool)
	StartKey() ([]byte, bool)
}

type IterationOption func(*iterOpts)

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
				return errors.New("IterationStartKey must have the same prefix as IterationPrefixKey")
			}
		} else {
			return errors.New("IterationStartKey must be provided when IterationPrefixKey is used")
		}
	}
	return nil
}

func NewIteratorOptions(opts ...IterationOption) (IterationOptions, error) {
	itOpts := new(iterOpts)
	for _, opt := range opts {
		opt(itOpts)
	}

	return itOpts, itOpts.validate()
}

func IterationPrefixKey(prefix []byte) IterationOption {
	return func(opts *iterOpts) {
		opts.keyPrefix = prefix
	}
}

func IterationStartKey(start []byte) IterationOption {
	return func(opts *iterOpts) {
		opts.startKey = start
	}
}

type Iterator interface {
	io.Closer
	HasNext() bool
	Next() ([]byte, []byte)
	Err() error
}

type Iteration interface {
	ForEach(func([]byte, []byte) error) error
}

type iteration struct {
	kvs  KVStore
	opts *iterOpts
}

func (iter *iteration) ForEach(hndlr func([]byte, []byte) error) error {
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

func NewIteration(kvs KVStore, iterReq *serverpb.IterateRequest) Iteration {
	itOpts := &iterOpts{iterReq.KeyPrefix, iterReq.StartKey}
	return &iteration{kvs, itOpts}
}
