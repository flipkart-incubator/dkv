package opts

import (
	"github.com/flipkart-incubator/dkv/internal/stats"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

//ServerOpts is a wrapper structure for all things related to cross-cutting concerns in DKV. All
//new configurations (e.g. health check interval), tools (e.g. logger, metric handler) should be
//wrapped in this struct.
type ServerOpts struct {
	HealthCheckTickerInterval uint
	StatsCli                  stats.Client
	Logger                    *zap.Logger
	PrometheusRegistry        prometheus.Registerer
}

const (
	DefaultHealthCheckTickerInterval = uint(10)
	DefaultSOSNDBuffer               = 1024 * 1024
	DefaultSORCVBuffer               = 1024 * 32
)
