package master

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"

	"github.com/flipkart-incubator/dkv/internal/stats"
	"github.com/flipkart-incubator/dkv/internal/storage"
	"github.com/flipkart-incubator/dkv/internal/sync/raftpb"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
	nexus_api "github.com/flipkart-incubator/nexus/pkg/api"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
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

	rwl      *sync.RWMutex
	lg       *zap.Logger
	statsCli stats.Client
}

// NewStandaloneService creates a standalone variant of the DKVService
// that works only with the local storage.
func NewStandaloneService(store storage.KVStore, cp storage.ChangePropagator, br storage.Backupable, lgr *zap.Logger, statsCli stats.Client) DKVService {
	rwl := &sync.RWMutex{}
	return &standaloneService{store, cp, br, rwl, lgr, statsCli}
}

func (ss *standaloneService) Put(ctx context.Context, putReq *serverpb.PutRequest) (*serverpb.PutResponse, error) {
	ss.rwl.RLock()
	defer ss.rwl.RUnlock()
	if err := ss.store.PutTTL(putReq.Key, putReq.Value, putReq.ExpireTS); err != nil {
		ss.lg.Error("Unable to PUT", zap.Error(err))
		return &serverpb.PutResponse{Status: newErrorStatus(err)}, err
	}
	return &serverpb.PutResponse{Status: newEmptyStatus()}, nil
}

func (ss *standaloneService) Delete(ctx context.Context, delReq *serverpb.DeleteRequest) (*serverpb.DeleteResponse, error) {
	ss.rwl.RLock()
	defer ss.rwl.RUnlock()

	if err := ss.store.Delete(delReq.Key); err != nil {
		ss.lg.Error("Unable to DELETE", zap.Error(err))
		return &serverpb.DeleteResponse{Status: newErrorStatus(err)}, err
	}
	return &serverpb.DeleteResponse{Status: newEmptyStatus()}, nil
}

func (ss *standaloneService) Get(ctx context.Context, getReq *serverpb.GetRequest) (*serverpb.GetResponse, error) {
	ss.rwl.RLock()
	defer ss.rwl.RUnlock()

	readResults, err := ss.store.Get(getReq.Key)
	res := &serverpb.GetResponse{Status: newEmptyStatus()}
	if err != nil {
		ss.lg.Error("Unable to GET", zap.Error(err))
		res.Status = newErrorStatus(err)
	} else {
		// Needed to take care of the (valid) case when the
		// given key is not found with DKV
		if len(readResults) == 1 {
			res.Value = readResults[0].Value
		}
	}
	return res, err
}

func (ss *standaloneService) MultiGet(ctx context.Context, multiGetReq *serverpb.MultiGetRequest) (*serverpb.MultiGetResponse, error) {
	ss.rwl.RLock()
	defer ss.rwl.RUnlock()

	readResults, err := ss.store.Get(multiGetReq.Keys...)
	res := &serverpb.MultiGetResponse{Status: newEmptyStatus()}
	if err != nil {
		ss.lg.Error("Unable to MultiGET", zap.Error(err))
		res.Status = newErrorStatus(err)
	} else {
		res.KeyValues = readResults
	}
	return res, err
}

func (ss *standaloneService) CompareAndSet(ctx context.Context, casReq *serverpb.CompareAndSetRequest) (*serverpb.CompareAndSetResponse, error) {
	ss.rwl.RLock()
	defer ss.rwl.RUnlock()

	res := &serverpb.CompareAndSetResponse{Status: newEmptyStatus()}
	casRes, err := ss.store.CompareAndSet(casReq.Key, casReq.OldValue, casReq.NewValue)
	if err != nil {
		ss.lg.Error("Unable to perform CAS", zap.Error(err))
		res.Status = newErrorStatus(err)
	}
	res.Updated = casRes
	return res, err
}

func (ss *standaloneService) GetChanges(ctx context.Context, getChngsReq *serverpb.GetChangesRequest) (*serverpb.GetChangesResponse, error) {
	ss.rwl.RLock()
	defer ss.rwl.RUnlock()

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

const (
	dkvMetaReplicaPrefix   = "_dkv_meta::Replica_"
	zoneReplicaValueFormat = "%s@%s:%d"
	replicaValueFormat     = "%s:%d"
)

func asReplicaValue(replica *serverpb.Replica) string {
	replicaValue := fmt.Sprintf(replicaValueFormat, replica.Hostname, replica.Port)
	if zone := strings.TrimSpace(replica.Zone); len(zone) > 0 {
		replicaValue = fmt.Sprintf(zoneReplicaValueFormat, zone, replica.Hostname, replica.Port)
	}
	return replicaValue
}

func (ss *standaloneService) AddReplica(ctx context.Context, replica *serverpb.Replica) (*serverpb.Status, error) {
	ss.rwl.RLock()
	defer ss.rwl.RUnlock()

	replicaValue := asReplicaValue(replica)
	replicaKey := fmt.Sprintf("%s%s", dkvMetaReplicaPrefix, replicaValue)
	if err := ss.store.Put([]byte(replicaKey), []byte(replicaValue)); err != nil {
		ss.lg.Error("Unable to add replica", zap.Error(err), zap.String("replica", replicaValue))
		return newErrorStatus(err), err
	}
	ss.lg.Info("Successfully added replica", zap.String("replica", replicaValue))
	return newEmptyStatus(), nil
}

func (ss *standaloneService) RemoveReplica(ctx context.Context, replica *serverpb.Replica) (*serverpb.Status, error) {
	ss.rwl.RLock()
	defer ss.rwl.RUnlock()

	replicaValue := asReplicaValue(replica)
	replicaKey := fmt.Sprintf("%s%s", dkvMetaReplicaPrefix, replicaValue)
	// We set the current replica key's value to empty - indicating a remove.
	// Once storage layer exposes DEL primitives, this impl. needs to perhaps change.
	if err := ss.store.Put([]byte(replicaKey), nil); err != nil {
		ss.lg.Error("Unable to remove replica", zap.Error(err), zap.String("replica", replicaValue))
		return newErrorStatus(err), err
	}
	ss.lg.Info("Successfully removed replica", zap.String("replica", replicaValue))
	return newEmptyStatus(), nil
}

func (ss *standaloneService) GetReplicas(ctx context.Context, req *serverpb.GetReplicasRequest) (*serverpb.GetReplicasResponse, error) {
	ss.rwl.RLock()
	defer ss.rwl.RUnlock()

	replicaPrefix, zone := dkvMetaReplicaPrefix, ""
	if zone = strings.TrimSpace(req.Zone); len(zone) > 0 {
		replicaPrefix = fmt.Sprintf("%s%s@", dkvMetaReplicaPrefix, zone)
	}
	iterOpts, _ := storage.NewIteratorOptions(storage.IterationPrefixKey([]byte(replicaPrefix)))
	iter := ss.store.Iterate(iterOpts)
	defer iter.Close()

	var replicas []*serverpb.Replica
	for iter.HasNext() {
		key, val := iter.Next()
		replicaKey, replicaVal := string(key), string(val)
		replicaAddr := strings.TrimPrefix(replicaKey, dkvMetaReplicaPrefix)

		// checking for valid replicas and not the removed ones whose values are empty
		if replicaAddr == replicaVal {
			comps := strings.Split(replicaVal, ":")
			port, _ := strconv.ParseUint(comps[1], 10, 32)
			replZone, replHost := "", comps[0]
			if comps = strings.Split(replHost, "@"); len(comps) == 2 {
				replZone, replHost = comps[0], comps[1]
			}
			// Check needed to ensure when zone is not given, only those
			// replicas NOT belonging to any zones are picked up. Note
			// that prefix matching from storage returns both kinds of
			// replicas and hence the need for this if condition.
			if replZone == zone {
				replicas = append(replicas, &serverpb.Replica{
					Hostname: replHost,
					Port:     uint32(port),
					Zone:     replZone,
				})
			}
		}
	}

	return &serverpb.GetReplicasResponse{Replicas: replicas}, nil
}

func (ss *standaloneService) Backup(ctx context.Context, backupReq *serverpb.BackupRequest) (*serverpb.Status, error) {
	ss.rwl.RLock()
	defer ss.rwl.RUnlock()

	bckpPath := backupReq.BackupPath
	if err := ss.br.BackupTo(bckpPath); err != nil {
		ss.lg.Error("Unable to perform backup", zap.Error(err))
		return newErrorStatus(err), err
	}
	return newEmptyStatus(), nil
}

func (ss *standaloneService) Restore(ctx context.Context, restoreReq *serverpb.RestoreRequest) (*serverpb.Status, error) {
	ss.lg.Info("Waiting for all other requests to complete")
	ss.rwl.Lock()
	defer ss.rwl.Unlock()

	ss.lg.Info("Closing the current DB connection")
	ss.store.Close()

	rstrPath := restoreReq.RestorePath
	ss.lg.Info("Beginning the restoration.", zap.String("RestorePath", rstrPath))
	st, ba, cp, _, err := ss.br.RestoreFrom(rstrPath)
	if err != nil {
		ss.lg.Error("Unable to perform restore, DKV must be restarted.", zap.Error(err))
		return newErrorStatus(err), err
	}
	ss.store, ss.br, ss.cp = st, ba, cp
	ss.lg.Info("Restoration completed")
	return newEmptyStatus(), nil
}

func (ss *standaloneService) Iterate(iterReq *serverpb.IterateRequest, dkvIterSrvr serverpb.DKV_IterateServer) error {
	ss.rwl.RLock()
	defer ss.rwl.RUnlock()

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
	statsCli stats.Client
}

// NewDistributedService creates a distributed variant of the DKV service
// that attempts to replicate data across multiple replicas over Nexus.
func NewDistributedService(kvs storage.KVStore, cp storage.ChangePropagator, br storage.Backupable,
	raftRepl nexus_api.RaftReplicator, lgr *zap.Logger, statsCli stats.Client) DKVClusterService {
	return &distributedService{NewStandaloneService(kvs, cp, br, lgr, statsCli), raftRepl, lgr, statsCli}
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

func (ds *distributedService) CompareAndSet(ctx context.Context, casReq *serverpb.CompareAndSetRequest) (*serverpb.CompareAndSetResponse, error) {
	reqBts, _ := proto.Marshal(&raftpb.InternalRaftRequest{Cas: casReq})
	res := &serverpb.CompareAndSetResponse{Status: newEmptyStatus()}
	casRes, err := ds.raftRepl.Save(ctx, reqBts)
	if err != nil {
		ds.lg.Error("Unable to CAS in replicated storage", zap.Error(err))
		res.Status = newErrorStatus(err)
		return res, err
	}
	// '0' indicates CAS update was successful
	res.Updated = casRes[0] == 0
	return res, err
}

func (ds *distributedService) Delete(ctx context.Context, delReq *serverpb.DeleteRequest) (*serverpb.DeleteResponse, error) {
	reqBts, err := proto.Marshal(&raftpb.InternalRaftRequest{Delete: delReq})
	res := &serverpb.DeleteResponse{Status: newEmptyStatus()}
	if err != nil {
		ds.lg.Error("Unable to DEL over Nexus", zap.Error(err))
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
			if kvs, err := gobDecodeAsKVPairs(val); err != nil {
				loadError = err
			} else {
				if kvs != nil && len(kvs) == 1 {
					res.Value = kvs[0].Value
				} else {
					loadError = fmt.Errorf("unable to compute value for given key from %v", kvs)
				}
			}
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
			res.KeyValues, readError = gobDecodeAsKVPairs(val)
		}
		return res, readError
	default:
		return nil, fmt.Errorf("Unknown read consistency level: %d", multiGetReq.ReadConsistency)
	}
}

func gobDecodeAsKVPairs(val []byte) ([]*serverpb.KVPair, error) {
	buf := bytes.NewBuffer(val)
	res := new([]*serverpb.KVPair)
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
	if err := ds.raftRepl.AddMember(ctx, req.NodeUrl); err != nil {
		ds.lg.Error("Unable to add node", zap.Error(err))
		return newErrorStatus(err), err
	}
	return newEmptyStatus(), nil
}

func (ds *distributedService) RemoveNode(ctx context.Context, req *serverpb.RemoveNodeRequest) (*serverpb.Status, error) {
	if err := ds.raftRepl.RemoveMember(ctx, req.NodeUrl); err != nil {
		ds.lg.Error("Unable to remove node", zap.Error(err))
		return newErrorStatus(err), err
	}
	return newEmptyStatus(), nil
}

func (ds *distributedService) ListNodes(ctx context.Context, _ *empty.Empty) (*serverpb.ListNodesResponse, error) {
	leader, members := ds.raftRepl.ListMembers()
	return &serverpb.ListNodesResponse{Status: newEmptyStatus(), Leader: leader, Nodes: members}, nil
}

func (ds *distributedService) Close() error {
	// Do not invoke DKVService::Close here since `raftRepl` already
	// closes the underlying storage connection.
	ds.raftRepl.Stop()
	return nil
}

func newErrorStatus(err error) *serverpb.Status {
	return &serverpb.Status{Code: -1, Message: err.Error()}
}

func newEmptyStatus() *serverpb.Status {
	return &serverpb.Status{Code: 0, Message: ""}
}
