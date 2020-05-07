package storage

import (
	"bytes"
	"errors"
	"io"
	"strings"
)

type IterationOptions interface {
	HasKeyPrefix() bool
	KeyPrefix() []byte
	HasStartKey() bool
	StartKey() []byte
}

type IterationOption func(*iterOpts) error

type iterOpts struct {
	keyPrefix []byte
	startKey  []byte
}

func (io *iterOpts) HasKeyPrefix() bool {
	return io.keyPrefix != nil && len(io.keyPrefix) > 0
}

func (io *iterOpts) KeyPrefix() []byte {
	return io.keyPrefix
}

func (io *iterOpts) HasStartKey() bool {
	return io.startKey != nil && len(io.startKey) > 0
}

func (io *iterOpts) StartKey() []byte {
	return io.startKey
}

func (io *iterOpts) validate() error {
	if io.HasKeyPrefix() {
		if io.HasStartKey() {
			if !bytes.HasPrefix(io.StartKey(), io.KeyPrefix()) {
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
		if err := opt(itOpts); err != nil {
			return nil, err
		}
	}

	return itOpts, itOpts.validate()
}

func IterationPrefixKey(prefix []byte) IterationOption {
	return func(opts *iterOpts) error {
		if prefix == nil || strings.TrimSpace(string(prefix)) == "" {
			return errors.New("invalid key prefix for iteration")
		}
		opts.keyPrefix = prefix
		return nil
	}
}

func IterationStartKey(start []byte) IterationOption {
	return func(opts *iterOpts) error {
		if start == nil || strings.TrimSpace(string(start)) == "" {
			return errors.New("invalid start key for iteration")
		}
		opts.startKey = start
		return nil
	}
}

type Iterator interface {
	io.Closer
	HasNext() bool
	Next() ([]byte, []byte)
	Err() error
}
