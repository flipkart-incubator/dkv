package sync

import (
	"github.com/flipkart-incubator/dkv/internal/server/storage"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
	"github.com/gogo/protobuf/proto"
)

type dkvReplStore struct {
	kvs storage.KVStore
}

func NewDKVReplStore(kvs storage.KVStore) *dkvReplStore {
	return &dkvReplStore{kvs}
}

func (dr *dkvReplStore) Save(req []byte) ([]byte, error) {
	// TODO: Needs to be extended for Get and MultiGet requests, using a union message type
	put_req := new(serverpb.PutRequest)
	if err := proto.Unmarshal(req, put_req); err != nil {
		return nil, err
	} else {
		return nil, dr.kvs.Put(put_req.Key, put_req.Value).Error
	}
}

func (dr *dkvReplStore) Close() error {
	return dr.kvs.Close()
}

func (dr *dkvReplStore) Backup() ([]byte, error) {
	// TODO: Implement this
	return nil, nil
}

func (dr *dkvReplStore) Restore(data []byte) error {
	// TODO: Implement this
	return nil
}
