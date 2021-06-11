
[![build](https://github.com/flipkart-incubator/dkv/actions/workflows/main.yml/badge.svg)](https://github.com/flipkart-incubator/dkv/actions/workflows/main.yml)

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
- Go version 1.13+
- [RocksDB](https://github.com/facebook/rocksdb) v6.5.3 as a storage engine
- [GoRocksDB](https://github.com/tecbot/gorocksdb) provides the CGo bindings with RocksDB
- [Badger](https://github.com/dgraph-io/badger) v1.6 as a storage engine
- [Nexus](https://github.com/flipkart-incubator/nexus) for sync replication over [Raft](https://raft.github.io/) consensus

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

### Launching the DKV server in standalone mode

A single DKV instance can be launched using the following command:

```bash
$ ./bin/dkvsrv \
    -dbFolder <folder_name> \
    -dbListenAddr <host:port> \
    -dbEngine <rocksdb|badger>
```

```bash
$ ./bin/dkvctl -dkvAddr <host:port> -set <key> <value>
$ ./bin/dkvctl -dkvAddr <host:port> -get <key>
```

Example session:
```bash
$ ./bin/dkvsrv -dbFolder /tmp/db -dbListenAddr 127.0.0.1:8080 -dbEngine rocksdb
$ ./bin/dkvctl -dkvAddr 127.0.0.1:8080 -set foo bar
$ ./bin/dkvctl -dkvAddr 127.0.0.1:8080 -get foo
bar
$ ./bin/dkvctl -dkvAddr 127.0.0.1:8080 -set hello world
$ ./bin/dkvctl -dkvAddr 127.0.0.1:8080 -get hello
world
$ ./bin/dkvctl -dkvAddr 127.0.0.1:8080 -del foo
$ ./bin/dkvctl -dkvAddr 127.0.0.1:8080 -iter "*"
hello => world
```

### Launching the DKV server for synchronous replication

This launch configuration allows for synchronously replicating changes to DKV keyspace
on multiple instances spread across independently failing regions or availability
zones. Typically such configurations are deployed over WANs so as to ensure better
read & write availability in the face of individual cluster failures and disasters.

Under the hood, we use [Nexus](https://github.com/flipkart-incubator/nexus) to replicate
keyspace mutations across multiple DKV instances using the RAFT consensus protocol.
Currently, the `put` API automatically replicates changes when the request is handled
by given DKV instance started in a special distributed mode (see below). However, `get`
and `multiget` APIs targetting such an instance serve the data from its own local store.
Hence such calls may or may not reflect the latest changes to the keyspace and hence are
not *linearizable*. In the future, these APIs will be enhanced to support linearizability.

Assuming you have 3 availability zones, run the following 3 commands one in every zone
in order to setup these instances for synchronous replication.
```bash
$ ./bin/dkvsrv \
    -dbFolder <folder_path> \
    -dbListenAddr <host:port> \
    -dbRole master \
    -nexusNodeUrl http://<host:port> \ #optional when running on separete nodes.
    -nexusClusterUrl <cluster_url>
```

All these 3 DKV instances form a database cluster each listening on separate ports for
Nexus & client communications. One can now construct the value for `nexusClusterUrl` param
in the above command using this example setup below:

|NexusNodeId|Hostname|NexusPort|
|-|-|-|
|1|dkv.az1|9020|
|2|dkv.az2|9020|
|3|dkv.az3|9020|

Then the value for `nexusClusterUrl` must be:
```bash
"http://dkv.az1:9020,http://dkv.az2:9020,http://dkv.az3:9020"
```

Note that same value must be used in each of the 3 commands used to launch the DKV cluster.
Subsequently, `dkvctl` utility can be used to perform keyspace mutations against any one
of the DKV instances which are then automatically replicated to the other 2 instances.

Example session on local machine:

Launch Node 1:
```bash
$ ./bin/dkvsrv \
    -dbFolder /tmp/dkvsrv/n1 \
    -dbListenAddr 127.0.0.1:9081 \
    -dbRole master \
    -nexusNodeUrl http://127.0.0.1:9021 \
    -nexusClusterUrl "http://127.0.0.1:9021,http://127.0.0.1:9022,http://127.0.0.1:9023"
```

Launch Node 2:
```bash
$ ./bin/dkvsrv \
    -dbFolder /tmp/dkvsrv/n2 \
    -dbListenAddr 127.0.0.1:9082 \
    -dbRole master \
    -nexusNodeUrl http://127.0.0.1:9022 \
    -nexusClusterUrl "http://127.0.0.1:9021,http://127.0.0.1:9022,http://127.0.0.1:9023"
```

Launch Node 3:
```bash
$ ./bin/dkvsrv \
    -dbFolder /tmp/dkvsrv/n3 \
    -dbListenAddr 127.0.0.1:9083 \
    -dbRole master \
    -nexusNodeUrl http://127.0.0.1:9023 \
    -nexusClusterUrl "http://127.0.0.1:9021,http://127.0.0.1:9022,http://127.0.0.1:9023"
```

Launch Node 4, not yet part of the cluster:
```bash
$ ./bin/dkvsrv \
    -dbFolder /tmp/dkvsrv/n4 \
    -dbListenAddr 127.0.0.1:9084 \
    -dbRole master \
    -nexusNodeUrl http://127.0.0.1:9024 \
    -nexusClusterUrl "http://127.0.0.1:9021,http://127.0.0.1:9022,http://127.0.0.1:9023" \
    -nexusJoin
```

Add this node to the existing 3 node cluster:
```bash
$ ./bin/dkvctl -dkvAddr 127.0.0.1:9081 -addNode "http://127.0.0.1:9024"
```

List members of the cluster from any member:
```bash
$ ./bin/dkvctl -dkvAddr 127.0.0.1:9082 -listNodes
Connecting to DKV service at 127.0.0.1:9082...DONE
Current DKV cluster members:
6ab6bfdf9f29bd1a => http://127.0.0.1:9094 (leader)
619f1e32973e3e7a => http://127.0.0.1:9092
785bb0a54cd7f8d5 => http://127.0.0.1:9093
c63380dd0a493345 => http://127.0.0.1:9091
```

Remove a node from the cluster:
```bash
$ ./bin/dkvctl -dkvAddr 127.0.0.1:9082 -removeNode http://127.0.0.1:9091
Connecting to DKV service at 127.0.0.1:9082...DONE
```

Confirm node removal from the cluster:
```bash
$ ./bin/dkvctl -dkvAddr 127.0.0.1:9082 -listNodes
Connecting to DKV service at 127.0.0.1:9082...DONE
Current DKV cluster members:
6ab6bfdf9f29bd1a => http://127.0.0.1:9094 (leader)
619f1e32973e3e7a => http://127.0.0.1:9092
785bb0a54cd7f8d5 => http://127.0.0.1:9093
```

### Launching the DKV server for asynchronous replication

This launch configuration allows for DKV instances to be started either as a master
node or a slave node. All mutations are permitted only on the master node while
one or more slave nodes asynchronously replicate the changes received from master
and make them available for reads. In other words, no keyspace mutations are
permitted on the slave nodes, except by the replication stream received from
master node.

The built-in replication mechanism guarantees _sequential consistency_ for reads
executed from the slave nodes. Moreover, all slave nodes will eventually converge
to an identical state which is often referred to as _strong eventual consistency_.

Such a configuration is typically deployed on applications where the typical number
of reads far exceed the number of writes.

First launch the DKV master node using the RocksDB engine with this command:
```bash
$ ./bin/dkvsrv \
    -dbFolder <folder_name> \
    -dbListenAddr <host:port> \
    -dbRole master
```

Then launch the DKV slave node using either RocksDB or Badger engine with this command:
```bash
$ ./bin/dkvsrv \
    -dbFolder <folder_name> \
    -dbListenAddr <host:port> \
    -dbEngine <rocksdb|badger> \
    -dbRole slave \
    -replMasterAddr <dkv_master_listen_addr>
```

Subsequently, any mutations performed on the master node's keyspace using `dkvctl`
will be applied automatically onto the slave node's keyspace. By default, a given
slave node polls for changes from its master node once every _5 seconds_. This can
be changed through the `replPollInterval` flag while launching the slave node.

Note that only **rocksdb** engine is supported on the DKV master node while the slave
node can be launched with either *rocksdb* or *badger* storage engines.

#### In memory mode for Badger based slaves

For slave nodes using the Badger storage engine, we also support an in-memory mode
where the entire dataset is stored in RAM without any writes to disk whatsoever.
This can be achieved by using the `-dbDiskless` option during launch as shown here.

```bash
$ ./bin/dkvsrv \
    -dbDiskless \
    -dbListenAddr <host:port> \
    -dbEngine badger \
    -dbRole slave \
    -replMasterAddr <dkv_master_listen_addr>
```

This mode may provide better performance for reads and is also useful for deployments
that are cache-like having optional durability requirements.

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
