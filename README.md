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
- get(K,consistency)

## Design
<img src="https://github.com/flipkart-incubator/dkv/raw/master/docs/design.png">

## Dependencies
- Go version 1.13+
- [RocksDB](https://github.com/facebook/rocksdb) v5.16+ as the storage engine
- [GoRocksDB](https://github.com/tecbot/gorocksdb) provides the CGo bindings with RocksDB
- [Raft consensus algorithm](https://raft.github.io/) by [etcd/raft](https://github.com/etcd-io/etcd/tree/master/raft)

## DKV on Docker
Follow these instructions to launch a DKV container using the Dockerfile included.

```bash
$ curl -fsSL https://raw.githubusercontent.com/flipkart-incubator/dkv/master/Dockerfile | docker build -t dkv/dkv-deb9-amd64 -f - .
$ docker run -it dkv/dkv-deb9-amd64:latest dkvsrv --help
```

## Building DKV on Mac OSX

### Building RocksDB
- Ensure [HomeBrew](https://brew.sh/) is installed
- brew install gcc49
- brew install rocksdb
- Install [ZStd](https://github.com/facebook/zstd)
- Execute this command:

```bash
CGO_CFLAGS="-I/usr/local/Cellar/rocksdb/6.1.2/include" \
CGO_LDFLAGS="-L/usr/local/Cellar/rocksdb/6.1.2 -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd" \
go get github.com/tecbot/gorocksdb
```

### Building DKV

```bash
$ mkdir -p ${GOPATH}/src/github.com/flipkart-incubator
$ cd ${GOPATH}/src/github.com/flipkart-incubator
$ git clone https://github.com/flipkart-incubator/dkv
$ cd dkv
$ make build
```

If you want to build for other platform, set `GOOS`, `GOARCH` environment variables. For example, build for macOS like following:

```bash
$ make GOOS=linux build
```

## Running

Once DKV is built, the `<PROJECT_ROOT>/bin` folder should contain the following binaries:
- `dkvsrv` - DKV server program
- `dkvctl` - DKV client program
- `dkvbench` - DKV benchmarking program

### Launching the DKV server in standalone mode

A single DKV instance can be launched using the following command:

```bash
$ ./bin/dkvsrv \
    -dbFolder <folder_name> \
    -dbListenAddr <host:port> \
    -dbEngine <rocksdb|badger>
```

```bash
$ ./bin/dkvctl -dkvAddr <host:port> -set <key:value>
$ ./bin/dkvctl -dkvAddr <host:port> -get <key>
```

Example session:
```bash
$ ./bin/dkvsrv -dbFolder /tmp/db -dkvSvcPort 8080 -storage rocksdb
$ ./bin/dkvctl -dkvAddr 127.0.0.1:8080 -set foo:bar
$ ./bin/dkvctl -dkvAddr 127.0.0.1:8080 -get foo
bar
$ ./bin/dkvctl -dkvAddr 127.0.0.1:8080 -set hello:world
$ ./bin/dkvctl -dkvAddr 127.0.0.1:8080 -get hello
world
```

### Launching the DKV server for asynchronous replication

This configuration allows for DKV instances to be launched either as a master
node or a slave node. All mutations are permitted only on the master node while
one or more slave nodes asynchronously replicate the changes received from master
and make them available for reads. In other words, no keyspace mutations are
permitted on the slave nodes, except by the replication stream received from
master node.

The built-in replication mechanism guarantees _sequential consistency_ for reads
executed from the slave nodes. Moreover, all slave nodes will eventually converge
to an identical state which is often referred to as _string eventual consistency_.

Such a configuration is typically deployed on applications where the typical number
of reads far exceed the number of writes.

First launch the DKV master node using the RocksDB engine with this command:
```bash
$ ./bin/dkvsrv \
    -dbFolder <folder_name> \
    -dbListenAddr <host:port> \
    -dbEngine rocksdb \
    -dbRole master
```

Then launch the DKV slave node using either RocksDB or Badger engine with this command:
```bash
$ ./bin/dkvsrv \
    -dbFolder <folder_name> \
    -dbListenAddr <host:port> \
    -dbEngine <rocksdb|badger> \
    -dbRole slave \
    -replMasterAddr <dkv_master_listen_addr> \
```

Subsequently, any mutations performed on the master node's keyspace using `dkvctl`
will be applied automatically onto the slave node's keyspace. By default, a given
slave node polls for changes from its master node once every _5 seconds_. This can
be changes through the `replPollInterval` flag while launching the slave node.

Note that only **rocksdb** engine is supported on the DKV master node while the slave
node can be launched with either *rocksdb* or *badger* engines.

## Testing

If you want to execute tests inside DKV, run command like following:

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
