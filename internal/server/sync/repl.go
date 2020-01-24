package sync

import (
	"github.com/flipkart-incubator/nexus/pkg/db"
)

type dkvReplicator struct {
}

func NewDKVReplicator() (db.Store, error) {
	return nil, nil
}

func (dr *dkvReplicator) Save(req []byte) error {
	return nil
}

func (dr *dkvReplicator) Close() error {
	return nil
}

func Backup() ([]byte, error) {
	return nil, nil
}

func Restore(data []byte) error {
	return nil
}
