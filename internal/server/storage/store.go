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
	// Put stores the association between the given key and value
	Put(key []byte, value []byte) error
	// Get bulk fetches the associated values for the given keys.
	// Note that during partial failures, any successful results
	// are discarded and an error is returned instead.
	Get(keys ...[]byte) ([][]byte, error)
}

// A ChangePropagator represents the capability of the underlying
// store from which committed changes can be retrieved for replication
// purposes. The implementor of this interface assumes the role of a
// master node in a typical master-slave setup.
type ChangePropagator interface {
	// GetLatestCommittedChangeNumber retrieves the change number of
	// the latest committed change. Returns an error if unable to
	// load this number.
	GetLatestCommittedChangeNumber() (uint64, error)
	// LoadChanges retrieves all the changes committed since the given
	// `fromChangeNumber`. Also, `maxChanges` can be used to limit the
	// number of changes returned in the response.
	LoadChanges(fromChangeNumber uint64, maxChanges int) ([]*serverpb.ChangeRecord, error)
}

// A ChangeApplier represents the capability of the underlying store
// to apply changes directly onto its key space. This is typically
// used for replication purposes to indicate that the implementor
// assumes the role of a slave node in a master-slave setup.
type ChangeApplier interface {
	// GetLatestAppliedChangeNumber retrieves the change number of
	// the latest committed change applied. Returns an error if unable
	// to load this number.
	GetLatestAppliedChangeNumber() (uint64, error)
	// SaveChanges commits to local key space the given changes and
	// returns the change number of the last committed change along
	// with an error that might have occurred during the process.
	// Note that implementors must treat every change on its own and
	// return the first error that occurs during the process. Remaining
	// changes if any must NOT be applied in order to ensure sequential
	// consistency.
	SaveChanges(changes []*serverpb.ChangeRecord) (uint64, error)
}
