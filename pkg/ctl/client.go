package ctl

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/flipkart-incubator/dkv/pkg/serverpb"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
)

// A DKVClient instance is used to communicate with various DKV services
// over GRPC. It is an adapter to the underlying GRPC clients that
// exposes a simpler API to its users without having to deal with timeouts,
// contexts and other GRPC semantics.
type DKVClient struct {
	cliConn    *grpc.ClientConn
	dkvCli     serverpb.DKVClient
	dkvReplCli serverpb.DKVReplicationClient
	dkvBRCli   serverpb.DKVBackupRestoreClient
	dkvClusCli serverpb.DKVClusterClient
}

// TODO: Should these be paramterised ?
const (
	ReadBufSize    = 10 << 20
	WriteBufSize   = 10 << 20
	MaxMsgSize     = 50 << 20
	Timeout        = 10 * time.Second
	ConnectTimeout = 10 * time.Second
)

// NewDKVClient creates a GRPC client against the
// given DKV service address and dial options. Optionally the authority param can be
//// used to send a :authority psuedo-header for routing purposes
func NewDKVClient(svcAddr string, authority string, opts ...grpc.DialOption) (*DKVClient, error) {
	var dkvClnt *DKVClient
	ctx, cancel := context.WithTimeout(context.Background(), ConnectTimeout)
	optsCopy := opts
	optsCopy = append(optsCopy, grpc.WithBlock(), grpc.WithReadBufferSize(ReadBufSize),
		grpc.WithWriteBufferSize(WriteBufSize), grpc.WithAuthority(authority),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(MaxMsgSize)))
	defer cancel()
	conn, err := grpc.DialContext(ctx, svcAddr, opts...)
	if err == nil {
		dkvCli := serverpb.NewDKVClient(conn)
		dkvReplCli := serverpb.NewDKVReplicationClient(conn)
		dkvBRCli := serverpb.NewDKVBackupRestoreClient(conn)
		dkvClusCli := serverpb.NewDKVClusterClient(conn)
		dkvClnt = &DKVClient{conn, dkvCli, dkvReplCli, dkvBRCli, dkvClusCli}
	}
	return dkvClnt, err
}

// Put takes the key and value as byte arrays and invokes the
// GRPC Put method. This is a convenience wrapper.
func (dkvClnt *DKVClient) Put(key []byte, value []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), Timeout)
	defer cancel()
	putReq := &serverpb.PutRequest{Key: key, Value: value}
	res, err := dkvClnt.dkvCli.Put(ctx, putReq)
	var status *serverpb.Status
	if res != nil {
		status = res.Status
	}
	return errorFromStatus(status, err)
}

// PutTTL takes the key and value as byte arrays, expireTS as epoch seconds and invokes the
// GRPC Put method. This is a convenience wrapper.
func (dkvClnt *DKVClient) PutTTL(key []byte, value []byte, expireTS uint64) error {
	ctx, cancel := context.WithTimeout(context.Background(), Timeout)
	defer cancel()
	putReq := &serverpb.PutRequest{Key: key, Value: value, ExpireTS: expireTS}
	res, err := dkvClnt.dkvCli.Put(ctx, putReq)
	var status *serverpb.Status
	if res != nil {
		status = res.Status
	}
	return errorFromStatus(status, err)
}

// CompareAndSet provides the wrapper for the standard CAS primitive.
// It invokes the underlying GRPC CompareAndSet method. This is a
// convenience wrapper.
func (dkvClnt *DKVClient) CompareAndSet(key []byte, expect []byte, update []byte) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), Timeout)
	defer cancel()
	casReq := &serverpb.CompareAndSetRequest{Key: key, OldValue: expect, NewValue: update}
	casRes, err := dkvClnt.dkvCli.CompareAndSet(ctx, casReq)
	if err != nil {
		return false, err
	}
	return casRes.Updated, errorFromStatus(casRes.Status, nil)
}

// Delete takes the key as byte arrays and invokes the
// GRPC Delete method. This is a convenience wrapper.
func (dkvClnt *DKVClient) Delete(key []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), Timeout)
	defer cancel()
	delReq := &serverpb.DeleteRequest{Key: key}
	res, err := dkvClnt.dkvCli.Delete(ctx, delReq)
	var status *serverpb.Status
	if res != nil {
		status = res.Status
	}
	return errorFromStatus(status, err)
}

// Get takes the key as byte array along with the consistency
// level and invokes the GRPC Get method. This is a convenience wrapper.
func (dkvClnt *DKVClient) Get(rc serverpb.ReadConsistency, key []byte) (*serverpb.GetResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), Timeout)
	defer cancel()
	getReq := &serverpb.GetRequest{Key: key, ReadConsistency: rc}
	return dkvClnt.dkvCli.Get(ctx, getReq)
}

// MultiGet takes the keys as byte arrays along with the consistency
// level and invokes the GRPC MultiGet method. This is a convenience wrapper.
func (dkvClnt *DKVClient) MultiGet(rc serverpb.ReadConsistency, keys ...[]byte) ([]*serverpb.KVPair, error) {
	ctx, cancel := context.WithTimeout(context.Background(), Timeout)
	defer cancel()
	multiGetReq := &serverpb.MultiGetRequest{Keys: keys, ReadConsistency: rc}
	res, err := dkvClnt.dkvCli.MultiGet(ctx, multiGetReq)
	if err != nil {
		return nil, err
	}
	return res.KeyValues, nil
}

// GetChanges retrieves changes since the given change number
// using the underlying GRPC GetChanges method. One can limit the
// number of changes retrieved using the maxNumChanges parameter.
// This is a convenience wrapper.
func (dkvClnt *DKVClient) GetChanges(fromChangeNum uint64, maxNumChanges uint32) (*serverpb.GetChangesResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), Timeout)
	defer cancel()
	getChngsReq := &serverpb.GetChangesRequest{FromChangeNumber: fromChangeNum, MaxNumberOfChanges: maxNumChanges}
	return dkvClnt.dkvReplCli.GetChanges(ctx, getChngsReq)
}

// Backup backs up the entire keyspace into the given filesystem
// location using the underlying GRPC Backup method. This is a
// convenience wrapper.
func (dkvClnt *DKVClient) Backup(path string) error {
	ctx, cancel := context.WithTimeout(context.Background(), Timeout)
	defer cancel()
	backupReq := &serverpb.BackupRequest{BackupPath: path}
	res, err := dkvClnt.dkvBRCli.Backup(ctx, backupReq)
	return errorFromStatus(res, err)
}

// Restore restores the entire keyspace from the given filesystem
// location using the underlying GRPC Restore method. This is a
// convenience wrapper.
func (dkvClnt *DKVClient) Restore(path string) error {
	ctx, cancel := context.WithTimeout(context.Background(), Timeout)
	defer cancel()
	restoreReq := &serverpb.RestoreRequest{RestorePath: path}
	res, err := dkvClnt.dkvBRCli.Restore(ctx, restoreReq)
	return errorFromStatus(res, err)
}

// AddNode adds the node with the given Nexus URL to
// the Nexus cluster of which the current node is a member of.
func (dkvClnt *DKVClient) AddNode(nodeURL string) error {
	ctx, cancel := context.WithTimeout(context.Background(), Timeout)
	defer cancel()
	addNodeReq := &serverpb.AddNodeRequest{NodeUrl: nodeURL}
	res, err := dkvClnt.dkvClusCli.AddNode(ctx, addNodeReq)
	return errorFromStatus(res, err)
}

// RemoveNode removes the node with the given URL from the
// Nexus cluster of which the current node is a member of.
func (dkvClnt *DKVClient) RemoveNode(nodeURL string) error {
	ctx, cancel := context.WithTimeout(context.Background(), Timeout)
	defer cancel()
	remNodeReq := &serverpb.RemoveNodeRequest{NodeUrl: nodeURL}
	res, err := dkvClnt.dkvClusCli.RemoveNode(ctx, remNodeReq)
	return errorFromStatus(res, err)
}

// ListNodes retrieves the current members of the Nexus cluster
// along with identifying the leader.
func (dkvClnt *DKVClient) ListNodes() (uint64, map[uint64]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), Timeout)
	defer cancel()
	res, err := dkvClnt.dkvClusCli.ListNodes(ctx, &empty.Empty{})
	if err := errorFromStatus(res.Status, err); err != nil {
		return 0, nil, err
	}
	return res.Leader, res.Nodes, nil
}

// KVPair is convenience wrapper that captures a key and its value.
type KVPair struct {
	Key, Val []byte
	ErrMsg   string
}

// Iterate invokes the underlying GRPC method for iterating through the
// entire keyspace in no particular order. `keyPrefix` can be used to
// select only the keys matching the given prefix and `startKey` can
// be used to set the lower bound for the iteration.
func (dkvClnt *DKVClient) Iterate(keyPrefix, startKey []byte) (<-chan *KVPair, error) {
	iterReq := &serverpb.IterateRequest{KeyPrefix: keyPrefix, StartKey: startKey}
	kvStrm, err := dkvClnt.dkvCli.Iterate(context.Background(), iterReq)
	if err != nil {
		return nil, err
	}
	ch := make(chan *KVPair)
	go func() {
		defer close(ch)
		for {
			itRes, err := kvStrm.Recv()
			if err == io.EOF || itRes == nil {
				break
			} else {
				ch <- &KVPair{itRes.Key, itRes.Value, itRes.Status.Message}
			}
		}
	}()
	return ch, nil
}

// Close closes the underlying GRPC client connection to DKV service
func (dkvClnt *DKVClient) Close() error {
	if dkvClnt.cliConn != nil {
		return dkvClnt.cliConn.Close()
	}
	return nil
}

func errorFromStatus(res *serverpb.Status, err error) error {
	switch {
	case err != nil:
		return err
	case res != nil && res.Code != 0:
		return errors.New(res.Message)
	default:
		return nil
	}
}
