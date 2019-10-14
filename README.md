# dkv
DKV is a distributed key value store server written in [Go](https://golang.org). It exposes all its functionality over
[gRPC](http://www.grpc.io) & [Protocol Buffers](https://developers.google.com/protocol-buffers/).

## Features
- Tunable consistency
- Database replication

## Supported APIs
* createVBucket(replicationFactor)
* put(K,V,vBucket)
* get(K,consistency)

## Design
<img src="https://github.com/flipkart-incubator/dkv/raw/master/docs/design.png">

## Dependencies
- [RocksDB](https://github.com/facebook/rocksdb) as the storage engine
- [GoRocksDB](https://github.com/tecbot/gorocksdb) provides the CGo bindings with RocksDB
- [Raft consensus algorithm](https://raft.github.io/) by [etcd/raft](https://github.com/etcd-io/etcd/tree/master/raft)

## Building DKV
When you satisfied dependencies, let's build DKV for Linux as following:

```bash
$ mkdir -p ${GOPATH}/src/github.com/flipkart-incubator
$ cd ${GOPATH}/src/github.com/flipkart-incubator
$ git clone https://github.com/flipkart-incubator/dkv
$ cd dkv
$ make build
```

If you want to build for other platform, set `GOOS`, `GOARCH` environment variables. For example, build for macOS like following:

```bash
$ make GOOS=darwin build
```

## Testing DKV

If you want to test your changes, run command like following:

```bash
$ make test
```

## Packaging DKV

###  Linux

```bash
$ make GOOS=linux dist
```

### macOS

```bash
$ make GOOS=darwin dist
```

