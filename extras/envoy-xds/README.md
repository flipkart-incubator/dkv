# envoy-xds
A reference implementation of the Envoy control plane for DKV clusters.
The cluster information is sourced from config file. Please look at the
[sample](sample-config.json) configuration file for an example.

## Steps for running DKV behind Envoy

### 1. Launch several DKV instances configured as masters or slaves
```bash
$ ./bin/dkvsrv -access-log stdout -role master -dbFolder /tmp/dkvsrv -dbListenAddr 127.0.0.1:8080
$ ./bin/dkvsrv -access-log stdout -db-engine badger -listen-addr 127.0.0.1:9090 -dbDiskless -role slave -repl-master-addr 127.0.0.1:8080 -repl-poll-interval 5s
$ ./bin/dkvsrv -access-log stdout -db-engine badger -listen-addr 127.0.0.1:9191 -dbDiskless -role slave -repl-master-addr 127.0.0.1:8080 -repl-poll-interval 5s
```

### 2. Build and launch envoy-xds
```bash
$ cd $PROJECT_ROOT/extras/envoy-xds
$ go build -o envoy-xds main.go
$ ./envoy-dkv -config ./sample-config.json -listenAddr 127.0.0.1:7979
```

### 3. Launch Envoy
```bash
$ $ENVOY_INSTALL_DIR/bin/envoy \
      -l warning \
      -c $PROJECT_ROOT/extras/envoy-xds/sample-envoy-config.yaml \
      --service-node local-group --service-cluster local
```

Note that a [sample](sample-envoy-config.yaml) Envoy configuration file is included which has been used
in the above step for configuring Envoy.

After this, one can use `dkvctl` or any GRPC client for working with DKV backends via the Envoy proxy.
