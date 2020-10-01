package stats

import (
	"io"
	"time"

	"github.com/smira/go-statsd"
)

type Tag struct {
	key, val string
}

func NewTag(key, val string) Tag {
	return Tag{key, val}
}

type Client interface {
	io.Closer
	Incr(string, int64)
	Gauge(string, int64)
	GaugeDelta(string, int64)
	Timing(string, time.Time)
}

type noopClient struct{}

func (*noopClient) Incr(_ string, _ int64)       {}
func (*noopClient) Gauge(_ string, _ int64)      {}
func (*noopClient) GaugeDelta(_ string, _ int64) {}
func (*noopClient) Timing(_ string, _ time.Time) {}
func (*noopClient) Close() error                 { return nil }

func NewNoOpClient() *noopClient {
	return &noopClient{}
}

type statsDClient struct {
	cli *statsd.Client
}

func NewStatsDClient(statsdAddr, metricPrfx string, defTags ...Tag) *statsDClient {
	statsTags := make([]statsd.Tag, len(defTags))
	for i, defTag := range defTags {
		statsTags[i] = statsd.StringTag(defTag.key, defTag.val)
	}
	return &statsDClient{
		statsd.NewClient(
			statsdAddr,
			statsd.TagStyle(statsd.TagFormatDatadog),
			statsd.MetricPrefix(metricPrfx),
			statsd.DefaultTags(statsTags...)),
	}
}

func (sdc *statsDClient) Incr(name string, value int64) {
	sdc.cli.Incr(name, value)
}

func (sdc *statsDClient) Gauge(name string, value int64) {
	sdc.cli.Gauge(name, value)
}

func (sdc *statsDClient) GaugeDelta(name string, value int64) {
	sdc.cli.GaugeDelta(name, value)
}

func (sdc *statsDClient) Timing(name string, startTime time.Time) {
	sdc.cli.PrecisionTiming(name, time.Since(startTime))
}

func (sdc *statsDClient) Close() error {
	return sdc.cli.Close()
}
