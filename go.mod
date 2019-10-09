module github.com/flipkart-incubator/dkv

go 1.13

require (
	github.com/bojand/ghz v0.41.0 // indirect
	github.com/dgraph-io/badger v1.6.0
	github.com/go-redis/redis v6.15.6+incompatible
	github.com/golang/protobuf v1.3.2
	github.com/tecbot/gorocksdb v0.0.0-20190705090504-162552197222
	golang.org/x/net v0.0.0-20190918130420-a8b05e9114ab
	google.golang.org/grpc v1.23.1
)

replace google.golang.org/genproto v0.0.0-20170818100345-ee236bd376b0 => google.golang.org/genproto v0.0.0-20170818010345-ee236bd376b0
