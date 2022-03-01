package stats

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

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
		case "storage_latency":
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
		case "storage_error":
			for _, m := range mf.GetMetric() {
				dkvMetrics.StorageOpsErrorCount[m.Label[0].GetValue()] = m.GetCounter().GetValue()
			}
		}
	}
	return dkvMetrics, nil
}
