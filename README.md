
![dkv logo](https://github.com/flipkart-incubator/dkv/raw/master/docs/dkv.png)

[![DKV CI](https://github.com/flipkart-incubator/dkv/actions/workflows/main.yml/badge.svg?branch=master)](https://github.com/flipkart-incubator/dkv/actions/workflows/main.yml) 

DKV is a distributed key value store server written in [Go](https://golang.org). It exposes all its functionality over
[gRPC](http://www.grpc.io) & [Protocol Buffers](https://developers.google.com/protocol-buffers/).

## Features
- Data Sharding
- Tunable consistency
- Data replication over WANs

## Supported APIs

- Put(Key, Value)
- MultiPut([]{Key,Value})
- Get(Key, Consistency)
- MultiGet([]Keys, Consistency)
- Delete([]Keys)
- CompareAndSet(Key, Value, OldValue)
- Scan(KeyPrefix, StartKey)

## Design
<img src="https://github.com/flipkart-incubator/dkv/raw/master/docs/design.png">

## Dependencies
- Go version 1.16+
- [RocksDB](https://github.com/facebook/rocksdb) v6.22.1 as a storage engine
- [GoRocksDB](https://github.com/flipkart-incubator/gorocksdb) provides the CGo bindings with RocksDB
- [Badger](https://github.com/dgraph-io/badger) v1.6 as a storage engine
- [Nexus](https://github.com/flipkart-incubator/nexus) for sync replication over [Raft](https://raft.github.io/) consensus


## Running 

### Launching the DKV server in standalone mode


A single DKV instance can be launched using the following docker command:


```
docker run -it -p 8080:8080 ghcr.io/flipkart-incubator/dkv:latest dkvsrv
```

or while using [native binaries](https://github.com/flipkart-incubator/dkv/wiki/Running-dkv) using :


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



## Support
dkv is undergoing active development. Consider joining the [dkv-interest](https://groups.google.com/forum/#!forum/dkv-interest) Google group for updates, design discussions, roadmap etc. in the initial stages of this project.
