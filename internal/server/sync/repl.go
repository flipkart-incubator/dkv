package sync

import (
	"bytes"
	"encoding/gob"
	"errors"

	"github.com/flipkart-incubator/dkv/internal/server/storage"
	"github.com/flipkart-incubator/dkv/internal/server/sync/raftpb"
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
	int_req := new(raftpb.InternalRaftRequest)
	if err := proto.Unmarshal(req, int_req); err != nil {
		return nil, err
	} else {
		switch {
		case int_req.Put != nil:
			return dr.put(int_req.Put)
		case int_req.Get != nil:
			return dr.get(int_req.Get)
		case int_req.MultiGet != nil:
			return dr.multiGet(int_req.MultiGet)
		default:
			return nil, errors.New("Unknown request to Save in dkv")
		}
	}
}

func (dr *dkvReplStore) put(putReq *serverpb.PutRequest) ([]byte, error) {
	put_res := dr.kvs.Put(putReq.Key, putReq.Value)
	return nil, put_res.Error
}

func (dr *dkvReplStore) get(getReq *serverpb.GetRequest) ([]byte, error) {
	get_res := dr.kvs.Get(getReq.Key)[0]
	return get_res.Value, get_res.Error
}

func (dr *dkvReplStore) multiGet(multiGetReq *serverpb.MultiGetRequest) ([]byte, error) {
	numReqs := len(multiGetReq.GetRequests)
	keys := make([][]byte, numReqs)
	for i, getReq := range multiGetReq.GetRequests {
		keys[i] = getReq.Key
	}
	read_results := dr.kvs.Get(keys...)
	return gobEncode(read_results)
}

func gobEncode(readResults []*storage.ReadResult) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(readResults); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
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
