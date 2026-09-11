package stats

import (
	"log"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type promethousRegistry struct{}

func (*promethousRegistry) Register(c prometheus.Collector) error {
	return prometheus.DefaultRegisterer.Register(c)
}

func (r *promethousRegistry) MustRegister(cs ...prometheus.Collector) {
	for _, c := range cs {
		if err := r.Register(c); err != nil {
			if metric, ok := c.(prometheus.Metric); ok {
				log.Printf("Failed to register collector %s: %s", metric.Desc().String(), err)
			} else {
				log.Printf("Failed to register collector: %s", err)
			}
		}
	}
}

func (*promethousRegistry) Unregister(c prometheus.Collector) bool {
	return prometheus.DefaultRegisterer.Unregister(c)
}

func NewPromethousRegistry() prometheus.Registerer {
	return &promethousRegistry{}
}

func (*noopClient) Register(collector prometheus.Collector) error {
	return nil
}

func (*noopClient) MustRegister(collectors ...prometheus.Collector) {}

func (*noopClient) Unregister(collector prometheus.Collector) bool {
	return true
}

func NewPromethousNoopRegistry() prometheus.Registerer {
	return &noopClient{}
}

func MeasureLatency(observer prometheus.Observer, startTime time.Time) {
	observer.Observe(time.Since(startTime).Seconds())
}

func GetMetrics() (*DKVMetrics, error) {
	dkvMetrics := NewDKVMetric()
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		return dkvMetrics, err
	}
	for _, mf := range mfs {
		switch mf.GetName() {
		case "dkv_storage_latency_rocksdb", "dkv_storage_latency_badger":
			for _, m := range mf.GetMetric() {
				dkvMetrics.StoreLatency[m.Label[0].GetValue()] = NewPercentile(m.GetSummary().GetQuantile())
				dkvMetrics.StorageOpsCount[m.Label[0].GetValue()] = m.GetSummary().GetSampleCount()
			}
		case "nexus_latency":
			for _, m := range mf.GetMetric() {
				dkvMetrics.NexusLatency[m.Label[0].GetValue()] = NewPercentile(m.GetSummary().GetQuantile())
				dkvMetrics.NexusOpsCount[m.Label[0].GetValue()] = m.GetSummary().GetSampleCount()
			}
		case "dkv_latency":
			for _, m := range mf.GetMetric() {
				dkvMetrics.DKVLatency[m.Label[0].GetValue()] = NewPercentile(m.GetSummary().GetQuantile())
				dkvMetrics.DKVReqCount[m.Label[0].GetValue()] = m.GetSummary().GetSampleCount()
			}
		case "dkv_storage_error":
			for _, m := range mf.GetMetric() {
				dkvMetrics.StorageOpsErrorCount[m.Label[0].GetValue()] = m.GetCounter().GetValue()
			}
		}
	}
	return dkvMetrics, nil
}
