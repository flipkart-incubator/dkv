package sync

import (
	"bytes"
	"encoding/gob"
	"errors"

	"github.com/flipkart-incubator/dkv/internal/server/storage"
	"github.com/flipkart-incubator/dkv/internal/server/sync/raftpb"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
	"github.com/flipkart-incubator/nexus/pkg/db"
	"github.com/gogo/protobuf/proto"
)

type dkvReplStore struct {
	kvs storage.KVStore
}

// NewDKVReplStore creates a wrapper out of the given KVStore
// that performs synchronous replication of all operations
// over Nexus onto multiple replicas.
func NewDKVReplStore(kvs storage.KVStore) db.Store {
	return &dkvReplStore{kvs}
}

func (dr *dkvReplStore) Save(req []byte) ([]byte, error) {
	intReq := new(raftpb.InternalRaftRequest)
	if err := proto.Unmarshal(req, intReq); err != nil {
		return nil, err
	}
	switch {
	case intReq.Put != nil:
		return dr.put(intReq.Put)
	default:
		return nil, errors.New("Unknown Save request in dkv")
	}
}

func (dr *dkvReplStore) Load(req []byte) ([]byte, error) {
	intReq := new(raftpb.InternalRaftRequest)
	if err := proto.Unmarshal(req, intReq); err != nil {
		return nil, err
	}
	switch {
	case intReq.Get != nil:
		return dr.get(intReq.Get)
	case intReq.MultiGet != nil:
		return dr.multiGet(intReq.MultiGet)
	default:
		return nil, errors.New("Unknown Load request in dkv")
	}
}

func (dr *dkvReplStore) put(putReq *serverpb.PutRequest) ([]byte, error) {
	err := dr.kvs.Put(putReq.Key, putReq.Value)
	return nil, err
}

func (dr *dkvReplStore) get(getReq *serverpb.GetRequest) ([]byte, error) {
	vals, err := dr.kvs.Get(getReq.Key)
	if err != nil {
		return nil, err
	}
	return vals[0], nil
}

func (dr *dkvReplStore) multiGet(multiGetReq *serverpb.MultiGetRequest) ([]byte, error) {
	vals, err := dr.kvs.Get(multiGetReq.Keys...)
	if err != nil {
		return nil, err
	}
	return gobEncode(vals)
}

func gobEncode(data interface{}) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(data); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (dr *dkvReplStore) Close() error {
	return dr.kvs.Close()
}

func (dr *dkvReplStore) Backup() ([]byte, error) {
	return dr.kvs.GetSnapshot()
}

func (dr *dkvReplStore) Restore(data []byte) error {
	return dr.kvs.PutSnapshot(data)
}
