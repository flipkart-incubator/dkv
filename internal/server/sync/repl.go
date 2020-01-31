package sync

import (
	"github.com/flipkart-incubator/dkv/internal/server/storage"
	"github.com/flipkart-incubator/nexus/pkg/db"
)

type dkvReplStore struct {
}

func NewDKVReplStore(kvs storage.KVStore) (db.Store, error) {
	return nil, nil
}

func (dr *dkvReplStore) Save(req []byte) error {
	return nil
}

func (dr *dkvReplStore) Close() error {
	return nil
}

func Backup() ([]byte, error) {
	return nil, nil
}

func Restore(data []byte) error {
	return nil
}
