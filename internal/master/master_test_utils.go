package master

import (
	"fmt"
	"github.com/flipkart-incubator/dkv/internal/stats"
	"github.com/flipkart-incubator/dkv/internal/storage"
	"github.com/flipkart-incubator/dkv/internal/storage/badger"
	"github.com/flipkart-incubator/dkv/internal/storage/rocksdb"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"net"
	"os/exec"
)

const (
	dbFolder   = "/tmp/dkv_test_db"
	cacheSize  = 3 << 30
	dkvSvcPort = 8080
	dkvSvcHost = "localhost"
	engine     = "rocksdb"
	// engine = "badger"
)

func ServeStandaloneDKV(info *serverpb.RegionInfo, dbFolder string) (DKVService, *grpc.Server) {
	kvs, cp, ba := newKVStore(dbFolder)
	dkvSvc := NewStandaloneService(kvs, cp, ba, zap.NewNop(), stats.NewNoOpClient(), info)
	grpcSrvr := grpc.NewServer()
	serverpb.RegisterDKVServer(grpcSrvr, dkvSvc)
	serverpb.RegisterDKVReplicationServer(grpcSrvr, dkvSvc)
	serverpb.RegisterDKVBackupRestoreServer(grpcSrvr, dkvSvc)
	return dkvSvc, grpcSrvr
}

func ListenAndServe(grpcSrvr *grpc.Server, port int) {
	if lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port)); err != nil {
		panic(fmt.Sprintf("failed to listen: %v", err))
	} else {
		grpcSrvr.Serve(lis)
	}
}

func newKVStore(dbFolder string) (storage.KVStore, storage.ChangePropagator, storage.Backupable) {
	if err := exec.Command("rm", "-rf", dbFolder).Run(); err != nil {
		panic(err)
	}
	switch engine {
	case "rocksdb":
		rocksDb, err := rocksdb.OpenDB(dbFolder,
			rocksdb.WithSyncWrites(), rocksdb.WithCacheSize(cacheSize))
		if err != nil {
			panic(err)
		}
		return rocksDb, rocksDb, rocksDb
	case "badger":
		bdgrDb, err := badger.OpenDB(badger.WithSyncWrites(), badger.WithDBDir(dbFolder))
		if err != nil {
			panic(err)
		}
		return bdgrDb, nil, bdgrDb
	default:
		panic(fmt.Sprintf("Unknown storage engine: %s", engine))
	}
}
