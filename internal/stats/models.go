package stats

import dto "github.com/prometheus/client_model/go"

type DKVMetrics struct {
	StoreLatency map[string]*Percentile	`json:"storage_latency"`
	NexusLatency map[string]*Percentile	`json:"nexus_latency"`

	GrpcResponseCount map[string]float64		`json:"grpc_request_count"`
	StorageOpsCount map[string]uint64				`json:"storage_ops_count"`
	StorageOpsErrorCount map[string]float64			`json:"storage_ops_error_count"`
	NexusOpsCount	map[string]uint64				`json:"nexus_ops_count"`
}

func (dm *DKVMetrics) Merge(metrics *DKVMetrics){

}

type Percentile struct {
	P50	float64		`json:"p50"`
	P90 float64		`json:"p90"`
	P99 float64		`json:"p99"`
}

func newDKVMetric() *DKVMetrics {
	return 	&DKVMetrics{
		StoreLatency: make(map[string]*Percentile),
		NexusLatency: make(map[string]*Percentile),
		GrpcResponseCount: make(map[string]float64),
		StorageOpsCount: make(map[string]uint64),
		StorageOpsErrorCount : make(map[string]float64),
		NexusOpsCount: make(map[string]uint64),
	}
}
func newPercentile(quantile []*dto.Quantile) *Percentile{
	percentile := &Percentile{}
	for _ , q := range quantile {
		switch *q.Quantile {
		case 0.5 :
			percentile.P50 = *q.Value
		case 0.9 :
			percentile.P90 = *q.Value
		case 0.99 :
			percentile.P99 = *q.Value
		}
	}
	return percentile
}
