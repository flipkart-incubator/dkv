package slave


import (
"context"
"errors"

"time"

"github.com/flipkart-incubator/dkv/pkg/serverpb"
"github.com/golang/protobuf/ptypes/empty"
"google.golang.org/grpc"
)

// A DKVClient instance is used to communicate with various DKV services
// over GRPC. It is an adapter to the underlying GRPC clients that
// exposes a simpler API to its users without having to deal with timeouts,
// contexts and other GRPC semantics.
type DKVInternalClient struct {
	conn    *grpc.ClientConn
	dkvReplCli serverpb.DKVReplicationClient
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

// TODO: This must be a secure client in future.
// NewInSecureDKVInternalClient creates an insecure GRPC client against the
// given DKV service address. Optionally the authority param can be
// used to send a :authority psuedo-header for routing purposes.
func NewInSecureDKVInternalClient(svcAddr, authority string) (*DKVInternalClient, error) {
	var dkvClnt *DKVInternalClient
	ctx, cancel := context.WithTimeout(context.Background(), ConnectTimeout)
	defer cancel()
	conn, err := grpc.DialContext(ctx, svcAddr,
		grpc.WithInsecure(),
		grpc.WithBlock(),
		grpc.WithMaxMsgSize(MaxMsgSize),
		grpc.WithReadBufferSize(ReadBufSize),
		grpc.WithWriteBufferSize(WriteBufSize),
		grpc.WithAuthority(authority))
	if err == nil {
		dkvReplCli := serverpb.NewDKVReplicationClient(conn)
		dkvClusCli := serverpb.NewDKVClusterClient(conn)
		dkvClnt = &DKVInternalClient{conn,  dkvReplCli,  dkvClusCli}
	}
	return dkvClnt, err
}

// GetChanges retrieves changes since the given change number
// using the underlying GRPC GetChanges method. One can limit the
// number of changes retrieved using the maxNumChanges parameter.
// This is a convenience wrapper.
func (dkvClnt *DKVInternalClient) GetChanges(fromChangeNum uint64, maxNumChanges uint32) (*serverpb.GetChangesResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), Timeout)
	defer cancel()
	getChngsReq := &serverpb.GetChangesRequest{FromChangeNumber: fromChangeNum, MaxNumberOfChanges: maxNumChanges}
	return dkvClnt.dkvReplCli.GetChanges(ctx, getChngsReq)
}

// AddNode adds the node with the given Nexus URL to
// the Nexus cluster of which the current node is a member of.
func (dkvClnt *DKVInternalClient) AddNode(nodeURL string) error {
	ctx, cancel := context.WithTimeout(context.Background(), Timeout)
	defer cancel()
	addNodeReq := &serverpb.AddNodeRequest{NodeUrl: nodeURL}
	res, err := dkvClnt.dkvClusCli.AddNode(ctx, addNodeReq)
	return errorFromStatus(res, err)
}

// RemoveNode removes the node with the given URL from the
// Nexus cluster of which the current node is a member of.
func (dkvClnt *DKVInternalClient) RemoveNode(nodeURL string) error {
	ctx, cancel := context.WithTimeout(context.Background(), Timeout)
	defer cancel()
	remNodeReq := &serverpb.RemoveNodeRequest{NodeUrl: nodeURL}
	res, err := dkvClnt.dkvClusCli.RemoveNode(ctx, remNodeReq)
	return errorFromStatus(res, err)
}

func (dkvClnt *DKVInternalClient) ListNodes() (uint64, map[uint64]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), Timeout)
	defer cancel()
	res, err := dkvClnt.dkvClusCli.ListNodes(ctx, &empty.Empty{})
	if err := errorFromStatus(res.Status, err); err != nil {
		return 0, nil, err
	}
	return res.Leader, res.Nodes, nil
}

// Close closes the underlying GRPC client connection to DKV service
func (dkvClnt *DKVInternalClient) Close() error {
	if dkvClnt.conn != nil {
		return dkvClnt.conn.Close()
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
