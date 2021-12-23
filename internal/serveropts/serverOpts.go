package serveropts

import (
	"github.com/flipkart-incubator/dkv/internal/stats"
	"go.uber.org/zap"
)

type ServerOpts struct {
	HealthCheckTickerInterval uint8
	StatsCli stats.Client
	Logger   *zap.Logger
}