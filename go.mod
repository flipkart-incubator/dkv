module github.com/flipkart-incubator/dkv

go 1.13

require (
	github.com/DataDog/zstd v1.4.5 // indirect
	github.com/OneOfOne/xxhash v1.2.2
	github.com/coreos/etcd v3.3.19+incompatible // indirect
	github.com/coreos/go-systemd v0.0.0-20191104093116-d3cd4ed1dbcf // indirect
	github.com/dgraph-io/badger/v2 v2.2007.2
	github.com/dgraph-io/ristretto v0.0.4-0.20210415235014-b837fdf292c8
	github.com/dgryski/go-farm v0.0.0-20200201041132-a6ae2369ad13 // indirect
	github.com/flipkart-incubator/gorocksdb v0.0.0-20210507064827-a2162cb9a3f7
	github.com/flipkart-incubator/nexus v0.0.0-20210612071733-03ce3d2a7406
	github.com/gogo/protobuf v1.3.1
	github.com/golang/protobuf v1.5.2
	github.com/golang/snappy v0.0.2 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.2.0
	github.com/kpango/fastime v1.0.16
	github.com/pkg/errors v0.9.1 // indirect
	github.com/prometheus/client_golang v1.5.1 // indirect
	github.com/prometheus/procfs v0.0.10 // indirect
	github.com/shamaton/msgpack v1.2.1
	github.com/smartystreets/goconvey v1.6.4 // indirect
	github.com/smira/go-statsd v1.3.1
	go.uber.org/zap v1.14.1
	golang.org/x/lint v0.0.0-20200302205851-738671d3881b // indirect
	golang.org/x/net v0.0.0-20201002202402-0a1ea396d57c // indirect
	golang.org/x/sys v0.0.0-20210415045647-66c3f260301c // indirect
	golang.org/x/text v0.3.2 // indirect
	golang.org/x/tools v0.0.0-20200318150045-ba25ddc85566 // indirect
	google.golang.org/grpc v1.28.0
	google.golang.org/protobuf v1.26.0
	gopkg.in/ini.v1 v1.62.0
	honnef.co/go/tools v0.0.1-2020.1.3 // indirect
)

replace (
	cloud.google.com/go => github.com/googleapis/google-cloud-go v0.26.0
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
	golang.org/x/sys => github.com/golang/sys v0.0.0-20200317113312-5766fd39f98d
	golang.org/x/text => github.com/golang/text v0.3.2
	golang.org/x/time => github.com/golang/time v0.0.0-20191024005414-555d28b269f0
	golang.org/x/tools => github.com/golang/tools v0.0.0-20200318150045-ba25ddc85566
	golang.org/x/xerrors => github.com/golang/xerrors v0.0.0-20191204190536-9bdfabe68543
	honnef.co/go/tools => github.com/dominikh/go-tools v0.0.1-2020.1.3
)
