module github.com/flipkart-incubator/dkv

go 1.15

require (
	github.com/Jille/grpc-multi-resolver v1.0.0
	github.com/coreos/etcd v3.3.19+incompatible // indirect
	github.com/coreos/go-systemd v0.0.0-20191104093116-d3cd4ed1dbcf // indirect
	github.com/dgraph-io/badger/v3 v3.2103.1
	github.com/dgraph-io/ristretto v0.1.0
	github.com/dgryski/go-farm v0.0.0-20200201041132-a6ae2369ad13 // indirect
	github.com/flipkart-incubator/gorocksdb v0.0.0-20210920082714-1f7dcbb7b2e4
	github.com/flipkart-incubator/nexus v0.0.0-20220406090439-e24842a8cf59
	github.com/gogo/protobuf v1.3.2
	github.com/golang/protobuf v1.5.2
	github.com/grpc-ecosystem/go-grpc-middleware v1.2.0
	github.com/kpango/fastime v1.0.16
	github.com/matttproud/golang_protobuf_extensions v1.0.1
	github.com/prometheus/client_golang v1.5.1 // indirect
	github.com/prometheus/procfs v0.0.10 // indirect
	github.com/smira/go-statsd v1.3.1
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.10.1
	github.com/vmihailenco/msgpack/v5 v5.3.4
	go.uber.org/zap v1.17.0
	golang.org/x/sys v0.0.0-20220310020820-b874c991c1a5 // indirect
	google.golang.org/grpc v1.43.0
	google.golang.org/protobuf v1.27.1
	gopkg.in/ini.v1 v1.66.2
	honnef.co/go/tools v0.0.1-2020.1.3 // indirect
)

replace (
	cloud.google.com/go => github.com/googleapis/google-cloud-go v0.26.0
	github.com/Jille/grpc-multi-resolver => github.com/mqy/grpc-multi-resolver v1.0.1-0.20211016000115-097a4c652dad
	github.com/spf13/pflag => github.com/TiboStev/pflag v1.0.6-0.20200918204434-33dec6aac494
	go.uber.org/atomic => github.com/uber-go/atomic v1.6.0
	go.uber.org/multierr => github.com/uber-go/multierr v1.5.0
	go.uber.org/tools => github.com/uber-go/tools v0.0.0-20190618225709-2cfd321de3ee
	go.uber.org/zap => github.com/uber-go/zap v1.14.1
	golang.org/x/crypto => github.com/golang/crypto v0.0.0-20191011191535-87dc89f01550
	golang.org/x/exp => github.com/golang/exp v0.0.0-20190121172915-509febef88a4
	golang.org/x/lint => github.com/golang/lint v0.0.0-20200302205851-738671d3881b
	golang.org/x/mod => github.com/golang/mod v0.2.0
	golang.org/x/net => github.com/golang/net v0.0.0-20200301022130-244492dfa37a
	golang.org/x/oauth2 => github.com/golang/oauth2 v0.0.0-20180821212333-d2e6202438be
	golang.org/x/sync => github.com/golang/sync v0.0.0-20190911185100-cd5d95a43a6e
	golang.org/x/sys => github.com/golang/sys v0.0.0-20220310020820-b874c991c1a5
	golang.org/x/text => github.com/golang/text v0.3.2
	golang.org/x/time => github.com/golang/time v0.0.0-20191024005414-555d28b269f0
	golang.org/x/tools => github.com/golang/tools v0.0.0-20200318150045-ba25ddc85566
	golang.org/x/xerrors => github.com/golang/xerrors v0.0.0-20191204190536-9bdfabe68543
	honnef.co/go/tools => github.com/dominikh/go-tools v0.0.1-2020.1.3
)
