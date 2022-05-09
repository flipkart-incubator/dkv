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
	"time"

	"github.com/flipkart-incubator/dkv/internal/stats"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/flipkart-incubator/dkv/pkg/health"

	"github.com/flipkart-incubator/nexus/models"

	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/flipkart-incubator/dkv/internal/opts"
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
	serverpb.DKVDiscoveryNodeServer
	serverpb.DKVReplicationServer
	serverpb.DKVBackupRestoreServer
	health.HealthServer
}

type dkvServiceStat struct {
	Latency       *prometheus.SummaryVec
	ResponseError *prometheus.CounterVec
}

func newDKVServiceStat(registry prometheus.Registerer) *dkvServiceStat {
	RequestLatency := prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace:  stats.Namespace,
		Name:       "latency",
		Help:       "Latency statistics for dkv service",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		MaxAge:     10 * time.Second,
	}, []string{"Ops"})
	ResponseError := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: stats.Namespace,
		Name:      "error",
		Help:      "Error count for storage operations",
	}, []string{"Ops"})
	registry.MustRegister(RequestLatency, ResponseError)
	return &dkvServiceStat{RequestLatency, ResponseError}
}

type standaloneService struct {
	store      storage.KVStore
	cp         storage.ChangePropagator
	br         storage.Backupable
	rwl        *sync.RWMutex
	regionInfo *serverpb.RegionInfo
	isClosed   bool
	shutdown   chan struct{}
	opts       *opts.ServerOpts
	stat       *dkvServiceStat
}

func (ss *standaloneService) GetStatus(ctx context.Context, request *emptypb.Empty) (*serverpb.RegionInfo, error) {
	return ss.regionInfo, nil
}

func (ss *standaloneService) Check(ctx context.Context, healthCheckReq *health.HealthCheckRequest) (*health.HealthCheckResponse, error) {
	if !ss.isClosed && ss.regionInfo != nil && ss.regionInfo.Status == serverpb.RegionStatus_LEADER {
		return &health.HealthCheckResponse{Status: health.HealthCheckResponse_SERVING}, nil
	}
	return &health.HealthCheckResponse{Status: health.HealthCheckResponse_NOT_SERVING}, nil
}

func (ss *standaloneService) Watch(req *health.HealthCheckRequest, watcher health.Health_WatchServer) error {
	// if the call is made after service shutdown, it should always return not serving health check
	// response
	if ss.isClosed {
		return watcher.Send(&health.HealthCheckResponse{Status: health.HealthCheckResponse_NOT_SERVING})
	}
	ticker := time.NewTicker(time.Duration(ss.opts.HealthCheckTickerInterval) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			res, _ := ss.Check(context.Background(), req)
			if err := watcher.Send(res); err != nil {
				return err
			}
		case <-ss.shutdown:
			return watcher.Send(&health.HealthCheckResponse{Status: health.HealthCheckResponse_NOT_SERVING})
		}
	}
}

// NewStandaloneService creates a standalone variant of the DKVService
// that works only with the local storage.
func NewStandaloneService(store storage.KVStore, cp storage.ChangePropagator, br storage.Backupable, regionInfo *serverpb.RegionInfo, opts *opts.ServerOpts) DKVService {
	rwl := &sync.RWMutex{}
	regionInfo.Status = serverpb.RegionStatus_LEADER
	return &standaloneService{store, cp, br, rwl, regionInfo, false, make(chan struct{}, 1), opts, newDKVServiceStat(opts.PrometheusRegistry)}
}

func (ss *standaloneService) Put(ctx context.Context, putReq *serverpb.PutRequest) (*serverpb.PutResponse, error) {
	defer stats.MeasureLatency(ss.stat.Latency.WithLabelValues(stats.Put), time.Now())
	ss.rwl.RLock()
	defer ss.rwl.RUnlock()
	if err := ss.store.Put(&serverpb.KVPair{Key: putReq.Key, Value: putReq.Value, ExpireTS: putReq.ExpireTS}); err != nil {
		ss.opts.Logger.Error("Unable to PUT", zap.Error(err))
		return &serverpb.PutResponse{Status: newErrorStatus(err)}, err
	}
	return &serverpb.PutResponse{Status: newEmptyStatus()}, nil
}

func (ss *standaloneService) MultiPut(ctx context.Context, putReq *serverpb.MultiPutRequest) (*serverpb.PutResponse, error) {
	defer stats.MeasureLatency(ss.stat.Latency.WithLabelValues(stats.MultiPut), time.Now())
	ss.rwl.RLock()
	defer ss.rwl.RUnlock()
	puts := make([]*serverpb.KVPair, len(putReq.PutRequest))
	for i, request := range putReq.PutRequest {
		puts[i] = &serverpb.KVPair{Key: request.Key, Value: request.Value, ExpireTS: request.ExpireTS}
	}

	if err := ss.store.Put(puts...); err != nil {
		ss.opts.Logger.Error("Unable to PUT", zap.Error(err))
		return &serverpb.PutResponse{Status: newErrorStatus(err)}, err
	}
	return &serverpb.PutResponse{Status: newEmptyStatus()}, nil
}

func (ss *standaloneService) Delete(ctx context.Context, delReq *serverpb.DeleteRequest) (*serverpb.DeleteResponse, error) {
	defer stats.MeasureLatency(ss.stat.Latency.WithLabelValues(stats.Delete), time.Now())
	ss.rwl.RLock()
	defer ss.rwl.RUnlock()

	if err := ss.store.Delete(delReq.Key); err != nil {
		ss.opts.Logger.Error("Unable to DELETE", zap.Error(err))
		return &serverpb.DeleteResponse{Status: newErrorStatus(err)}, err
	}
	return &serverpb.DeleteResponse{Status: newEmptyStatus()}, nil
}

func (ss *standaloneService) Get(ctx context.Context, getReq *serverpb.GetRequest) (*serverpb.GetResponse, error) {
	defer stats.MeasureLatency(ss.stat.Latency.WithLabelValues(stats.Get+getReadConsistencySuffix(getReq.ReadConsistency)), time.Now())
	ss.rwl.RLock()
	defer ss.rwl.RUnlock()
	readResults, err := ss.store.Get(getReq.Key)
	res := &serverpb.GetResponse{Status: newEmptyStatus()}
	if err != nil {
		ss.opts.Logger.Error("Unable to GET", zap.Error(err))
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
	defer stats.MeasureLatency(ss.stat.Latency.WithLabelValues(stats.MultiGet+getReadConsistencySuffix(multiGetReq.ReadConsistency)), time.Now())
	ss.rwl.RLock()
	defer ss.rwl.RUnlock()

	readResults, err := ss.store.Get(multiGetReq.Keys...)
	res := &serverpb.MultiGetResponse{Status: newEmptyStatus()}
	if err != nil {
		ss.opts.Logger.Error("Unable to MultiGET", zap.Error(err))
		res.Status = newErrorStatus(err)
	} else {
		res.KeyValues = readResults
	}
	return res, err
}

func (ss *standaloneService) CompareAndSet(ctx context.Context, casReq *serverpb.CompareAndSetRequest) (*serverpb.CompareAndSetResponse, error) {
	defer stats.MeasureLatency(ss.stat.Latency.WithLabelValues(stats.CompareAndSet), time.Now())
	ss.rwl.RLock()
	defer ss.rwl.RUnlock()

	res := &serverpb.CompareAndSetResponse{Status: newEmptyStatus()}
	casRes, err := ss.store.CompareAndSet(casReq.Key, casReq.OldValue, casReq.NewValue)
	if err != nil {
		ss.opts.Logger.Error("Unable to perform CAS", zap.Error(err))
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
			ss.opts.Logger.Warn("GetChanges: From change number more than the latest change number",
				zap.Uint64("FromChangeNumber", getChngsReq.FromChangeNumber), zap.Uint64("LatestChangeNumber", latestChngNum))
		}
		return res, nil
	}

	chngs, err := ss.cp.LoadChanges(getChngsReq.FromChangeNumber, int(getChngsReq.MaxNumberOfChanges))
	if err != nil {
		ss.opts.Logger.Error("Unable to load changes", zap.Error(err))
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
	if err := ss.store.Put(&serverpb.KVPair{Key: []byte(replicaKey), Value: []byte(replicaValue)}); err != nil {
		ss.opts.Logger.Error("Unable to add replica", zap.Error(err), zap.String("replica", replicaValue))
		return newErrorStatus(err), err
	}
	ss.opts.Logger.Info("Successfully added replica", zap.String("replica", replicaValue))
	return newEmptyStatus(), nil
}

func (ss *standaloneService) RemoveReplica(ctx context.Context, replica *serverpb.Replica) (*serverpb.Status, error) {
	ss.rwl.RLock()
	defer ss.rwl.RUnlock()

	replicaValue := asReplicaValue(replica)
	replicaKey := fmt.Sprintf("%s%s", dkvMetaReplicaPrefix, replicaValue)
	if err := ss.store.Delete([]byte(replicaKey)); err != nil {
		ss.opts.Logger.Error("Unable to remove replica", zap.Error(err), zap.String("replica", replicaValue))
		return newErrorStatus(err), err
	}
	ss.opts.Logger.Info("Successfully removed replica", zap.String("replica", replicaValue))
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
		entry := iter.Next()
		replicaKey, replicaVal := string(entry.Key), string(entry.Value)
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
		ss.opts.Logger.Error("Unable to perform backup", zap.Error(err))
		return newErrorStatus(err), err
	}
	return newEmptyStatus(), nil
}

func (ss *standaloneService) Restore(ctx context.Context, restoreReq *serverpb.RestoreRequest) (*serverpb.Status, error) {
	ss.opts.Logger.Info("Waiting for all other requests to complete")
	ss.rwl.Lock()
	defer ss.rwl.Unlock()

	ss.opts.Logger.Info("Closing the current DB connection")
	ss.store.Close()

	rstrPath := restoreReq.RestorePath
	ss.opts.Logger.Info("Beginning the restoration.", zap.String("RestorePath", rstrPath))
	st, ba, cp, _, err := ss.br.RestoreFrom(rstrPath)
	if err != nil {
		ss.opts.Logger.Error("Unable to perform restore, DKV must be restarted.", zap.Error(err))
		return newErrorStatus(err), err
	}
	ss.store, ss.br, ss.cp = st, ba, cp
	ss.opts.Logger.Info("Restoration completed")
	return newEmptyStatus(), nil
}

func (ss *standaloneService) Iterate(iterReq *serverpb.IterateRequest, dkvIterSrvr serverpb.DKV_IterateServer) error {
	defer stats.MeasureLatency(ss.stat.Latency.WithLabelValues(stats.Iterate), time.Now())
	ss.rwl.RLock()
	defer ss.rwl.RUnlock()

	iteration := storage.NewIteration(ss.store, iterReq)
	err := iteration.ForEach(func(e *serverpb.KVPair) error {
		itRes := &serverpb.IterateResponse{Status: newEmptyStatus(), Key: e.Key, Value: e.Value}
		return dkvIterSrvr.Send(itRes)
	})
	if err != nil {
		ss.opts.Logger.Error("Unable to iterate", zap.Error(err))
		itRes := &serverpb.IterateResponse{Status: newErrorStatus(err)}
		return dkvIterSrvr.Send(itRes)
	}
	return nil
}

func (ss *standaloneService) Close() error {
	defer ss.opts.Logger.Sync()
	ss.opts.Logger.Info("Closing DKV service")
	ss.shutdown <- struct{}{}
	ss.isClosed = true
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
	isClosed bool
	// shutdown should be a buffer channel to avoid blocking close in case the health check client
	// is not running
	shutdown chan struct{}
	opts     *opts.ServerOpts
	stat     *dkvServiceStat
}

// NewDistributedService creates a distributed variant of the DKV service
// that attempts to replicate data across multiple replicas over Nexus.
func NewDistributedService(kvs storage.KVStore, cp storage.ChangePropagator, br storage.Backupable,
	raftRepl nexus_api.RaftReplicator, regionInfo *serverpb.RegionInfo, opts *opts.ServerOpts) DKVClusterService {
	return &distributedService{
		DKVService: NewStandaloneService(kvs, cp, br, regionInfo, opts),
		raftRepl:   raftRepl,
		shutdown:   make(chan struct{}, 1),
		opts:       opts,
		stat:       newDKVServiceStat(stats.NewPromethousNoopRegistry()),
	}
}

func (ds *distributedService) Put(ctx context.Context, putReq *serverpb.PutRequest) (*serverpb.PutResponse, error) {
	defer stats.MeasureLatency(ds.stat.Latency.WithLabelValues(stats.Put), time.Now())
	reqBts, err := proto.Marshal(&raftpb.InternalRaftRequest{Put: putReq})
	res := &serverpb.PutResponse{Status: newEmptyStatus()}
	if err != nil {
		ds.opts.Logger.Error("Unable to PUT over Nexus", zap.Error(err))
		res.Status = newErrorStatus(err)
	} else {
		if _, err = ds.raftRepl.Save(ctx, reqBts); err != nil {
			ds.opts.Logger.Error("Unable to save in replicated storage", zap.Error(err))
			res.Status = newErrorStatus(err)
		}
	}
	return res, err
}

func (ds *distributedService) MultiPut(ctx context.Context, multiPutReq *serverpb.MultiPutRequest) (*serverpb.PutResponse, error) {
	reqBts, err := proto.Marshal(&raftpb.InternalRaftRequest{MultiPut: multiPutReq})
	res := &serverpb.PutResponse{Status: newEmptyStatus()}
	if err != nil {
		ds.opts.Logger.Error("Unable to PUT over Nexus", zap.Error(err))
		res.Status = newErrorStatus(err)
	} else {
		if _, err = ds.raftRepl.Save(ctx, reqBts); err != nil {
			ds.opts.Logger.Error("Unable to save in replicated storage", zap.Error(err))
			res.Status = newErrorStatus(err)
		}
	}
	return res, err
}

func (ds *distributedService) CompareAndSet(ctx context.Context, casReq *serverpb.CompareAndSetRequest) (*serverpb.CompareAndSetResponse, error) {
	defer stats.MeasureLatency(ds.stat.Latency.WithLabelValues(stats.CompareAndSet), time.Now())
	reqBts, _ := proto.Marshal(&raftpb.InternalRaftRequest{Cas: casReq})
	res := &serverpb.CompareAndSetResponse{Status: newEmptyStatus()}
	casRes, err := ds.raftRepl.Save(ctx, reqBts)
	if err != nil {
		ds.opts.Logger.Error("Unable to CAS in replicated storage", zap.Error(err))
		res.Status = newErrorStatus(err)
		return res, err
	}
	// '0' indicates CAS update was successful
	res.Updated = casRes[0] == 0
	return res, err
}

func (ds *distributedService) Delete(ctx context.Context, delReq *serverpb.DeleteRequest) (*serverpb.DeleteResponse, error) {
	defer stats.MeasureLatency(ds.stat.Latency.WithLabelValues(stats.Delete), time.Now())
	reqBts, err := proto.Marshal(&raftpb.InternalRaftRequest{Delete: delReq})
	res := &serverpb.DeleteResponse{Status: newEmptyStatus()}
	if err != nil {
		ds.opts.Logger.Error("Unable to DEL over Nexus", zap.Error(err))
		res.Status = newErrorStatus(err)
	} else {
		if _, err = ds.raftRepl.Save(ctx, reqBts); err != nil {
			ds.opts.Logger.Error("Unable to delete in replicated storage", zap.Error(err))
			res.Status = newErrorStatus(err)
		}
	}
	return res, err
}

func (ds *distributedService) Get(ctx context.Context, getReq *serverpb.GetRequest) (*serverpb.GetResponse, error) {
	defer stats.MeasureLatency(ds.stat.Latency.WithLabelValues(stats.Get+getReadConsistencySuffix(getReq.GetReadConsistency())), time.Now())
	switch getReq.ReadConsistency {
	case serverpb.ReadConsistency_SEQUENTIAL:
		return ds.DKVService.Get(ctx, getReq)
	case serverpb.ReadConsistency_LINEARIZABLE:
		reqBts, _ := proto.Marshal(&raftpb.InternalRaftRequest{Get: getReq})
		res := &serverpb.GetResponse{Status: newEmptyStatus()}
		var loadError error
		if val, err := ds.raftRepl.Load(ctx, reqBts); err != nil {
			ds.opts.Logger.Error("Unable to load from replicated storage", zap.Error(err))
			res.Status = newErrorStatus(err)
			loadError = err
		} else {
			if kvs, err := gobDecodeAsKVPairs(val); err != nil {
				loadError = err
			} else {
				if kvs != nil && len(kvs) == 1 {
					res.Value = kvs[0].Value
				}
			}
		}
		return res, loadError
	default:
		return nil, fmt.Errorf("Unknown read consistency level: %d", getReq.ReadConsistency)
	}
}

func (ds *distributedService) MultiGet(ctx context.Context, multiGetReq *serverpb.MultiGetRequest) (*serverpb.MultiGetResponse, error) {
	defer stats.MeasureLatency(ds.stat.Latency.WithLabelValues(stats.MultiGet+getReadConsistencySuffix(multiGetReq.GetReadConsistency())), time.Now())
	switch multiGetReq.ReadConsistency {
	case serverpb.ReadConsistency_SEQUENTIAL:
		return ds.DKVService.MultiGet(ctx, multiGetReq)
	case serverpb.ReadConsistency_LINEARIZABLE:
		reqBts, _ := proto.Marshal(&raftpb.InternalRaftRequest{MultiGet: multiGetReq})
		res := &serverpb.MultiGetResponse{Status: newEmptyStatus()}
		var readError error
		if val, err := ds.raftRepl.Load(ctx, reqBts); err != nil {
			ds.opts.Logger.Error("Unable to load (MultiGet) from replicated storage", zap.Error(err))
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
	defer stats.MeasureLatency(ds.stat.Latency.WithLabelValues(stats.Iterate), time.Now())
	return ds.DKVService.Iterate(iterReq, dkvIterSrvr)
}

func (ds *distributedService) Restore(ctx context.Context, restoreReq *serverpb.RestoreRequest) (*serverpb.Status, error) {
	err := errors.New("Current DKV instance does not support restores")
	return newErrorStatus(err), err
}

func (ds *distributedService) AddNode(ctx context.Context, req *serverpb.AddNodeRequest) (*serverpb.Status, error) {
	// TODO: We can include any relevant checks on the joining node - like reachability, storage engine compatibility, etc.
	if err := ds.raftRepl.AddMember(ctx, req.NodeUrl); err != nil {
		ds.opts.Logger.Error("Unable to add node", zap.Error(err))
		return newErrorStatus(err), err
	}
	return newEmptyStatus(), nil
}

func (ds *distributedService) RemoveNode(ctx context.Context, req *serverpb.RemoveNodeRequest) (*serverpb.Status, error) {
	if err := ds.raftRepl.RemoveMember(ctx, req.NodeUrl); err != nil {
		ds.opts.Logger.Error("Unable to remove node", zap.Error(err))
		return newErrorStatus(err), err
	}
	return newEmptyStatus(), nil
}

func (ds *distributedService) ListNodes(ctx context.Context, _ *empty.Empty) (*serverpb.ListNodesResponse, error) {
	leader, members := ds.raftRepl.ListMembers()
	return &serverpb.ListNodesResponse{Status: newEmptyStatus(), Leader: leader, Nodes: members}, nil
}

func (ds *distributedService) Close() error {
	ds.opts.Logger.Info("Closing the master service")
	// Do not invoke DKVService::Close here since `raftRepl` already
	// closes the underlying storage connection.
	// TODO - fix the above as ideally it should call DKVService::Close if there are more aspects to DKVService to close other than storage
	ds.raftRepl.Stop()
	ds.shutdown <- struct{}{}
	ds.isClosed = true
	return nil
}

func (ds *distributedService) GetStatus(context context.Context, request *emptypb.Empty) (*serverpb.RegionInfo, error) {
	regionInfo := ds.DKVService.(*standaloneService).regionInfo

	if ds.isClosed {
		regionInfo.Status = serverpb.RegionStatus_INACTIVE
	} else {
		// Currently there is no way for this instance of DKVServer to know if its the local dc follower, or is a follower with a lot of lag
		// Even the member list api ListMembers() is just an in memory lookup rather than actually looking at the current raft member state
		// If the current node is itself disconnected, it will provide stale / incorrect member list
		// For now, we will return status based on listMembers() and identify based on stale data
		// As of now, there is no hard requirement to identify true leader via service discovery thus using listmembers() is fine
		// TODO - Provide correct status wrt master / local dc follower / follower with lot of lag
		leaderId, _ := ds.raftRepl.ListMembers()
		selfId := ds.raftRepl.Id()

		if leaderId == selfId {
			regionInfo.Status = serverpb.RegionStatus_LEADER
		} else {
			// Leader is unknown. Could be when raft quorum is incomplete or leader election is in progress
			// TODO - Provide correct status wrt local dc follower / follower with lot of lag
			regionInfo.Status = serverpb.RegionStatus_PRIMARY_FOLLOWER
		}
	}
	return regionInfo, nil
}

func (ds *distributedService) Check(ctx context.Context, healthCheckReq *health.HealthCheckRequest) (*health.HealthCheckResponse, error) {
	if ds.isClosed {
		return &health.HealthCheckResponse{Status: health.HealthCheckResponse_NOT_SERVING}, nil
	} else {
		leaderId, members := ds.raftRepl.ListMembers()
		selfId := ds.raftRepl.Id()
		isFollower := members[selfId] != nil && (members[selfId].Status == models.NodeInfo_FOLLOWER)
		if leaderId == selfId || isFollower {
			return &health.HealthCheckResponse{Status: health.HealthCheckResponse_SERVING}, nil
		} else if !isFollower {
			return &health.HealthCheckResponse{Status: health.HealthCheckResponse_NOT_SERVING}, nil
		}
	}
	return &health.HealthCheckResponse{Status: health.HealthCheckResponse_NOT_SERVING}, nil
}

func (ds *distributedService) Watch(req *health.HealthCheckRequest, watcher health.Health_WatchServer) error {
	if ds.isClosed {
		return watcher.Send(&health.HealthCheckResponse{Status: health.HealthCheckResponse_NOT_SERVING})
	}
	ticker := time.NewTicker(time.Duration(ds.opts.HealthCheckTickerInterval) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			res, err := ds.Check(context.Background(), req)
			if err != nil {
				return err
			}
			if err := watcher.Send(res); err != nil {
				return err
			}
		case <-ds.shutdown:
			return watcher.Send(&health.HealthCheckResponse{Status: health.HealthCheckResponse_NOT_SERVING})
		}
	}
}

func newErrorStatus(err error) *serverpb.Status {
	return &serverpb.Status{Code: -1, Message: err.Error()}
}

func newEmptyStatus() *serverpb.Status {
	return &serverpb.Status{Code: 0, Message: ""}
}

func getReadConsistencySuffix(rc serverpb.ReadConsistency) string {
	switch rc {
	case serverpb.ReadConsistency_SEQUENTIAL:
		return "Seq"
	case serverpb.ReadConsistency_LINEARIZABLE:
		return "Lin"
	}
	return ""
}
