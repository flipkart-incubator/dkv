package opts

import "time"

type DiscoveryClientConfiguration struct {

	DiscoveryServiceAddr string `mapstructure:"discovery-service-addr"`
	PushStatusInterval time.Duration `mapstructure:"push-status-interval"`
	PollClusterInfoInterval time.Duration `mapstructure:"poll-cluster-info-interval"`
}

type DiscoveryServerConfiguration struct {

	StatusTTl int64 `mapstructure:"status-ttl"`
	HeartbeatTimeout int64 `mapstructure:"heartbeat-timeout"`
}

type DiscoveryServiceConfiguration struct {

	ServerConfig DiscoveryServerConfiguration `mapstructure:"server-config"`
	ClientConfig DiscoveryClientConfiguration `mapstructure:"client-config"`
}
