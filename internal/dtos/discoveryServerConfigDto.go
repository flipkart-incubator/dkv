package dtos

type DiscoveryClientConfigDto struct {

	DiscoveryServiceAddr string
	PushStatusInterval string
	PollClusterInfoInterval string
}

type DiscoveryConfigDto struct {

	StatusTTl string
	HeartbeatTimeout string
}

type DiscoveryServiceConfigDto struct {

	ServerConfig DiscoveryConfigDto
	ClientConfig DiscoveryClientConfigDto
}
