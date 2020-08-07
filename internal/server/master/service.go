package master

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"

	"github.com/flipkart-incubator/dkv/internal/server/storage"
	"github.com/flipkart-incubator/dkv/internal/server/sync/raftpb"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
	nexus_api "github.com/flipkart-incubator/nexus/pkg/api"
	"github.com/gogo/protobuf/proto"
	"go.uber.org/zap"
)

// A DKVService represents a service for serving key value data
// along with exposing all mutations as a replication stream.
type DKVService interface {
	io.Closer
	serverpb.DKVServer
	serverpb.DKVReplicationServer
	serverpb.DKVBackupRestoreServer
}

type standaloneService struct {
	store storage.KVStore
	cp    storage.ChangePropagator
	br    storage.Backupable
	lg    *zap.Logger
}

// NewStandaloneService creates a standalone variant of the DKVService
// that works only with the local storage and the instance of Zap logger.
func NewStandaloneService(store storage.KVStore, cp storage.ChangePropagator, br storage.Backupable, lgr *zap.Logger) DKVService {
	if lgr == nil {
		lgr = zap.NewNop()
	}
	return &standaloneService{store, cp, br, lgr}
}

func (ss *standaloneService) Put(ctx context.Context, putReq *serverpb.PutRequest) (*serverpb.PutResponse, error) {
	if err := ss.store.Put(putReq.Key, putReq.Value); err != nil {
		ss.lg.Error("Unable to PUT", zap.Error(err))
		return &serverpb.PutResponse{Status: newErrorStatus(err)}, err
	}
	return &serverpb.PutResponse{Status: newEmptyStatus()}, nil
}

func (ss *standaloneService) Get(ctx context.Context, getReq *serverpb.GetRequest) (*serverpb.GetResponse, error) {
	readResults, err := ss.store.Get(getReq.Key)
	res := &serverpb.GetResponse{Status: newEmptyStatus()}
	if err != nil {
		ss.lg.Error("Unable to GET", zap.Error(err))
		res.Status = newErrorStatus(err)
	} else {
		res.Value = readResults[0]
	}
	return res, err
}

func (ss *standaloneService) MultiGet(ctx context.Context, multiGetReq *serverpb.MultiGetRequest) (*serverpb.MultiGetResponse, error) {
	readResults, err := ss.store.Get(multiGetReq.Keys...)
	res := &serverpb.MultiGetResponse{Status: newEmptyStatus()}
	if err != nil {
		ss.lg.Error("Unable to MultiGET", zap.Error(err))
		res.Status = newErrorStatus(err)
	} else {
		res.Values = readResults
	}
	return res, err
}

func (ss *standaloneService) GetChanges(ctx context.Context, getChngsReq *serverpb.GetChangesRequest) (*serverpb.GetChangesResponse, error) {
	latestChngNum, _ := ss.cp.GetLatestCommittedChangeNumber()
	res := &serverpb.GetChangesResponse{Status: newEmptyStatus(), MasterChangeNumber: latestChngNum}
	if getChngsReq.FromChangeNumber > latestChngNum {
		if getChngsReq.FromChangeNumber > (latestChngNum + 1) {
			ss.lg.Warn("GetChanges: From change number more than the latest change number",
				zap.Uint64("FromChangeNumber", getChngsReq.FromChangeNumber), zap.Uint64("LatestChangeNumber", latestChngNum))
		}
		return res, nil
	}

	chngs, err := ss.cp.LoadChanges(getChngsReq.FromChangeNumber, int(getChngsReq.MaxNumberOfChanges))
	if err != nil {
		ss.lg.Error("Unable to load changes", zap.Error(err))
		res.Status = newErrorStatus(err)
	} else {
		res.NumberOfChanges = uint32(len(chngs))
		res.Changes = chngs
	}
	return res, err
}

func (ss *standaloneService) Backup(ctx context.Context, backupReq *serverpb.BackupRequest) (*serverpb.Status, error) {
	bckpPath := backupReq.BackupPath
	if err := ss.br.BackupTo(bckpPath); err != nil {
		ss.lg.Error("Unable to perform backup", zap.Error(err))
		return newErrorStatus(err), err
	}
	return newEmptyStatus(), nil
}

func (ss *standaloneService) Restore(ctx context.Context, restoreReq *serverpb.RestoreRequest) (*serverpb.Status, error) {
	rstrPath := restoreReq.RestorePath
	st, ba, cp, _, err := ss.br.RestoreFrom(rstrPath)
	if err != nil {
		ss.lg.Error("Unable to perform restore", zap.Error(err))
		return newErrorStatus(err), err
	}
	// TODO: Check if this needs locking before mutating references.
	ss.store, ss.br, ss.cp = st, ba, cp
	return newEmptyStatus(), nil
}

func (ss *standaloneService) Iterate(iterReq *serverpb.IterateRequest, dkvIterSrvr serverpb.DKV_IterateServer) error {
	iteration := storage.NewIteration(ss.store, iterReq)
	err := iteration.ForEach(func(k, v []byte) error {
		itRes := &serverpb.IterateResponse{Status: newEmptyStatus(), Key: k, Value: v}
		return dkvIterSrvr.Send(itRes)
	})
	if err != nil {
		ss.lg.Error("Unable to iterate", zap.Error(err))
		itRes := &serverpb.IterateResponse{Status: newErrorStatus(err)}
		return dkvIterSrvr.Send(itRes)
	}
	return nil
}

func (ss *standaloneService) Close() error {
	defer ss.lg.Sync()
	ss.lg.Info("Closing DKV service")
	ss.store.Close()
	return nil
}

// A DKVClusterService represents a service for serving key value data
// along with exposing all mutations as a replication stream. Moreover
// it also provides means to add and remove DKV nodes onto the current
// cluster.
type DKVClusterService interface {
	DKVService
	serverpb.DKVClusterServer
}

type distributedService struct {
	DKVService
	raftRepl nexus_api.RaftReplicator
	lg       *zap.Logger
}

// NewDistributedService creates a distributed variant of the DKV service
// that attempts to replicate data across multiple replicas over Nexus.
func NewDistributedService(kvs storage.KVStore, cp storage.ChangePropagator, br storage.Backupable, raftRepl nexus_api.RaftReplicator, lgr *zap.Logger) DKVClusterService {
	if lgr == nil {
		lgr = zap.NewNop()
	}
	return &distributedService{NewStandaloneService(kvs, cp, br, lgr), raftRepl, lgr}
}

func (ds *distributedService) Put(ctx context.Context, putReq *serverpb.PutRequest) (*serverpb.PutResponse, error) {
	reqBts, err := proto.Marshal(&raftpb.InternalRaftRequest{Put: putReq})
	res := &serverpb.PutResponse{Status: newEmptyStatus()}
	if err != nil {
		ds.lg.Error("Unable to PUT over Nexus", zap.Error(err))
		res.Status = newErrorStatus(err)
	} else {
		if _, err = ds.raftRepl.Save(ctx, reqBts); err != nil {
			ds.lg.Error("Unable to save in replicated storage", zap.Error(err))
			res.Status = newErrorStatus(err)
		}
	}
	return res, err
}

func (ds *distributedService) Get(ctx context.Context, getReq *serverpb.GetRequest) (*serverpb.GetResponse, error) {
	switch getReq.ReadConsistency {
	case serverpb.ReadConsistency_SEQUENTIAL:
		return ds.DKVService.Get(ctx, getReq)
	case serverpb.ReadConsistency_LINEARIZABLE:
		reqBts, _ := proto.Marshal(&raftpb.InternalRaftRequest{Get: getReq})
		res := &serverpb.GetResponse{Status: newEmptyStatus()}
		var loadError error
		if val, err := ds.raftRepl.Load(ctx, reqBts); err != nil {
			ds.lg.Error("Unable to load from replicated storage", zap.Error(err))
			res.Status = newErrorStatus(err)
			loadError = err
		} else {
			res.Value = val
		}
		return res, loadError
	default:
		return nil, fmt.Errorf("Unknown read consistency level: %d", getReq.ReadConsistency)
	}
}

func (ds *distributedService) MultiGet(ctx context.Context, multiGetReq *serverpb.MultiGetRequest) (*serverpb.MultiGetResponse, error) {
	switch multiGetReq.ReadConsistency {
	case serverpb.ReadConsistency_SEQUENTIAL:
		return ds.DKVService.MultiGet(ctx, multiGetReq)
	case serverpb.ReadConsistency_LINEARIZABLE:
		reqBts, _ := proto.Marshal(&raftpb.InternalRaftRequest{MultiGet: multiGetReq})
		res := &serverpb.MultiGetResponse{Status: newEmptyStatus()}
		var readError error
		if val, err := ds.raftRepl.Load(ctx, reqBts); err != nil {
			ds.lg.Error("Unable to load (MultiGet) from replicated storage", zap.Error(err))
			res.Status = newErrorStatus(err)
			readError = err
		} else {
			res.Values, readError = gobDecodeAs2DByteArray(val)
		}
		return res, readError
	default:
		return nil, fmt.Errorf("Unknown read consistency level: %d", multiGetReq.ReadConsistency)
	}
}

func gobDecodeAs2DByteArray(val []byte) ([][]byte, error) {
	buf := bytes.NewBuffer(val)
	res := new([][]byte)
	if err := gob.NewDecoder(buf).Decode(res); err != nil {
		return nil, err
	}
	return *res, nil
}

func (ds *distributedService) Iterate(iterReq *serverpb.IterateRequest, dkvIterSrvr serverpb.DKV_IterateServer) error {
	return ds.DKVService.Iterate(iterReq, dkvIterSrvr)
}

func (ds *distributedService) Restore(ctx context.Context, restoreReq *serverpb.RestoreRequest) (*serverpb.Status, error) {
	err := errors.New("Current DKV instance does not support restores")
	return newErrorStatus(err), err
}

func (ds *distributedService) AddNode(ctx context.Context, req *serverpb.AddNodeRequest) (*serverpb.Status, error) {
	// TODO: We can include any relevant checks on the joining node - like reachability, storage engine compatibility, etc.
	if err := ds.raftRepl.AddMember(ctx, int(req.NodeId), req.NodeUrl); err != nil {
		ds.lg.Error("Unable to add node", zap.Error(err))
		return newErrorStatus(err), err
	}
	return newEmptyStatus(), nil
}

func (ds *distributedService) RemoveNode(ctx context.Context, req *serverpb.RemoveNodeRequest) (*serverpb.Status, error) {
	if err := ds.raftRepl.RemoveMember(ctx, int(req.NodeId)); err != nil {
		ds.lg.Error("Unable to remove node", zap.Error(err))
		return newErrorStatus(err), err
	}
	return newEmptyStatus(), nil
}

func (ds *distributedService) Close() error {
	ds.raftRepl.Stop()
	return nil
}

func newErrorStatus(err error) *serverpb.Status {
	return &serverpb.Status{Code: -1, Message: err.Error()}
}

func newEmptyStatus() *serverpb.Status {
	return &serverpb.Status{Code: 0, Message: ""}
}
