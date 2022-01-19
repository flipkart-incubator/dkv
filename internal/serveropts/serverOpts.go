package serveropts

import (
	"github.com/flipkart-incubator/dkv/internal/stats"
	"go.uber.org/zap"
)

//ServerOpts is a wrapper structure for all things related to cross-cutting concerns in DKV. All
//new configurations (e.g. health check interval), tools (e.g. logger, metric handler) should be
//wrapped in this struct.
type ServerOpts struct {
	HealthCheckTickerInterval uint8
	StatsCli                  stats.Client
	Logger                    *zap.Logger
}
