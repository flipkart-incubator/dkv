package storage

import (
	"io"

	"github.com/flipkart-incubator/dkv/pkg/serverpb"
)

// A KVStore represents the key value store that provides
// the underlying storage implementation for the various
// DKV operations.
type KVStore interface {
	io.Closer
	Put(key []byte, value []byte) error
	Get(keys ...[]byte) ([][]byte, error)
}

type ChangePropagator interface {
	GetLatestChangeNumber() uint64
	LoadChanges(fromChangeNumber uint64, maxChanges int) ([]*serverpb.ChangeRecord, error)
}

type ChangeApplier interface {
	GetLatestChangeNumber() uint64
	SaveChanges(changes []*serverpb.ChangeRecord) (uint64, error)
}
