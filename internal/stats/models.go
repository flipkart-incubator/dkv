package stats

import (
	dto "github.com/prometheus/client_model/go"
	"time"
)

const (
	Ops           = "ops"
	Put           = "put"
	PutTTL        = "pTtl"
	Get           = "get"
	MultiGet      = "mget"
	Delete        = "del"
	GetSnapShot   = "getSnapShot"
	PutSnapShot   = "putSnapShot"
	Iterate       = "iter"
	CompareAndSet = "cas"
	LoadChange    = "loadChange"
	SaveChange    = "saveChange"
)

type DKVMetrics struct {
	TimeStamp            int64                  `json:"ts"`
	StoreLatency         map[string]*Percentile `json:"storage_latency"`
	NexusLatency         map[string]*Percentile `json:"nexus_latency"`
	DKVLatency           map[string]*Percentile `json:"dkv_latency"`
	StorageOpsCount      map[string]uint64      `json:"storage_ops_count"`
	StorageOpsErrorCount map[string]float64     `json:"storage_ops_error_count"`
	NexusOpsCount        map[string]uint64      `json:"nexus_ops_count"`
	DKVReqCount          map[string]uint64      `json:"dkv_req_count"`
}

type Percentile struct {
	P50 float64 `json:"p50"`
	P90 float64 `json:"p90"`
	P99 float64 `json:"p99"`
}

func newDKVMetric() *DKVMetrics {
	return &DKVMetrics{
		TimeStamp:            time.Now().Unix(),
		StoreLatency:         make(map[string]*Percentile),
		NexusLatency:         make(map[string]*Percentile),
		DKVLatency:           make(map[string]*Percentile),
		StorageOpsCount:      make(map[string]uint64),
		StorageOpsErrorCount: make(map[string]float64),
		NexusOpsCount:        make(map[string]uint64),
		DKVReqCount:          make(map[string]uint64),
	}
}
func newPercentile(quantile []*dto.Quantile) *Percentile {
	percentile := &Percentile{}
	for _, q := range quantile {
		switch *q.Quantile {
		case 0.5:
			percentile.P50 = *q.Value
		case 0.9:
			percentile.P90 = *q.Value
		case 0.99:
			percentile.P99 = *q.Value
		}
	}
	return percentile
}
