package name_resolvers

// To enable grpc clients to connect to multiple servers ("," delimited)
// Refer documentation from https://github.com/grpc/grpc/blob/master/doc/naming.md
// Refer examples from https://github.com/grpc/grpc-go/tree/master/examples/features/name_resolving

import (
	"google.golang.org/grpc/resolver"
	"strings"
)

// The target endpoint needs to start with this scheme to get parsed as this resolver
// Eg: multi:///127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003
const scheme = "multi"

type multiAddress struct {
}

func (m multiAddress) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	addresses := parseTarget(target.Endpoint)
	r := &multiAddressResolver{
		target:    target,
		cc:        cc,
		addresses: addresses,
	}
	r.start()
	return r, nil
}

func (m multiAddress) Scheme() string {
	return scheme
}

func parseTarget(endpoints string) []string {
	return strings.Split(endpoints, ",")
}

func Initialise() {
	resolver.Register(&multiAddress{})
}

type multiAddressResolver struct {
	target    resolver.Target
	cc        resolver.ClientConn
	addresses []string
}

func (r *multiAddressResolver) start() {
	addrs := make([]resolver.Address, len(r.addresses))
	for i, s := range r.addresses {
		addrs[i] = resolver.Address{Addr: s}
	}
	r.cc.UpdateState(resolver.State{Addresses: addrs})
}
func (*multiAddressResolver) ResolveNow(o resolver.ResolveNowOptions) {}
func (*multiAddressResolver) Close()                                  {}
