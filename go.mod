module github.com/flipkart-incubator/dkv

go 1.13

require (
	github.com/bojand/ghz v0.41.0
	github.com/dgraph-io/badger v1.6.0
	github.com/facebookgo/ensure v0.0.0-20160127193407-b4ab57deab51 // indirect
	github.com/facebookgo/stack v0.0.0-20160209184415-751773369052 // indirect
	github.com/facebookgo/subset v0.0.0-20150612182917-8dac2c3c4870 // indirect
	github.com/flipkart-incubator/nexus v0.0.0-20200131091313-d846fd39981f
	github.com/go-redis/redis v6.15.6+incompatible
	github.com/gogo/protobuf v1.3.1
	github.com/golang/protobuf v1.3.2
	github.com/tecbot/gorocksdb v0.0.0-20190705090504-162552197222
	golang.org/x/net v0.0.0-20190918130420-a8b05e9114ab
	google.golang.org/grpc v1.25.1
)

replace google.golang.org/genproto v0.0.0-20170818100345-ee236bd376b0 => google.golang.org/genproto v0.0.0-20170818010345-ee236bd376b0
