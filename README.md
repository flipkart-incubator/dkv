
[![DKV CI](https://github.com/flipkart-incubator/dkv/actions/workflows/main.yml/badge.svg?branch=master)](https://github.com/flipkart-incubator/dkv/actions/workflows/main.yml)

# dkv
DKV is a distributed key value store server written in [Go](https://golang.org). It exposes all its functionality over
[gRPC](http://www.grpc.io) & [Protocol Buffers](https://developers.google.com/protocol-buffers/).

## Features
- Data Sharding
- Tunable consistency
- Data replication over WANs

## Supported APIs
- createVBucket(replicationFactor)
- put(K,V,vBucket)
- del(K,vBucket)
- get(K,consistency)

## Design
<img src="https://github.com/flipkart-incubator/dkv/raw/master/docs/design.png">

## Dependencies
- Go version 1.16+
- [RocksDB](https://github.com/facebook/rocksdb) v6.22.1 as a storage engine
- [GoRocksDB](https://github.com/flipkart-incubator/gorocksdb) provides the CGo bindings with RocksDB
- [Badger](https://github.com/dgraph-io/badger) v1.6 as a storage engine
- [Nexus](https://github.com/flipkart-incubator/nexus) for sync replication over [Raft](https://raft.github.io/) consensus

## DKV on Docker
Follow these instructions to launch a DKV container using the Dockerfile included.

```bash
$ curl -fsSL https://raw.githubusercontent.com/flipkart-incubator/dkv/master/Dockerfile | docker build -t dkv/dkv-deb9-amd64 -f - .
$ docker run -it dkv/dkv-deb9-amd64:latest dkvsrv --help
```

## Building DKV on Mac OSX

### Installing Dependencies 

DKV depends on RocksDB, and its CGo bindings, so we need to install rocksdb along with its dependecies.

- Ensure [HomeBrew](https://brew.sh/) is installed
- `brew install rocksdb zstd`


### Building DKV

```bash
$ mkdir -p ${GOPATH}/src/github.com/flipkart-incubator
$ cd ${GOPATH}/src/github.com/flipkart-incubator
$ git clone https://github.com/flipkart-incubator/dkv
$ cd dkv
$ make build
```

If you want to build for other platform, set `GOOS`, `GOARCH` environment variables. For example, build on macOS for linux like following:

```bash
$ make GOOS=linux build
```

## Running

Once DKV is built, the `<PROJECT_ROOT>/bin` folder should contain the following binaries:
- `dkvsrv` - DKV server program
- `dkvctl` - DKV client program

### Launching the DKV server in standalone mode

A single DKV instance can be launched using the following command:

```bash
$ ./bin/dkvsrv --config dkvsrv.yaml  --db-folder <folder_name>  --listen-addr <host:port>
```

```bash
$ ./bin/dkvctl -a <host:port> --set <key> <value>
$ ./bin/dkvctl -a <host:port> --get <key>
```

Example session:
```bash
$ ./bin/dkvsrv --config dkvsrv.yaml --db-folder /tmp/db --listen-addr 127.0.0.1:8080
$ ./bin/dkvctl -a 127.0.0.1:8080 --set foo bar
$ ./bin/dkvctl -a 127.0.0.1:8080 --get foo
bar
$ ./bin/dkvctl -a 127.0.0.1:8080 --set hello world
$ ./bin/dkvctl -a 127.0.0.1:8080 --get hello
world
$ ./bin/dkvctl -a 127.0.0.1:8080 --del foo
$ ./bin/dkvctl -a 127.0.0.1:8080 --iter "*"
hello => world
```

### Launching the DKV server for synchronous/asynchronous replication

Please refer to the [wiki instructions](https://github.com/flipkart-incubator/dkv/wiki/Running-dkv#launching-the-dkv-server-for-synchronous-replication) on how to run DKV in cluster mode.

## Documentation
Detailed documentation on specific features, design principles, data guarantees etc. can be found in the [dkv Wiki](https://github.com/flipkart-incubator/dkv/wiki)

## Testing

If you want to execute tests inside DKV, run this command:

```bash
$ make test
```

## Packaging

###  Linux

```bash
$ make GOOS=linux dist
```

### macOS

```bash
$ make GOOS=darwin dist
```

## Support
dkv is undergoing active development. Consider joining the [dkv-interest](https://groups.google.com/forum/#!forum/dkv-interest) Google group for updates, design discussions, roadmap etc. in the initial stages of this project.
