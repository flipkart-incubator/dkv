package stats

import (
	"time"

	dto "github.com/prometheus/client_model/go"
)

const (
	Namespace     = "dkv"
	Ops           = "ops"
	Put           = "put"
	MultiPut      = "mput"
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
	Count                float64                `json:"count"`
}

func (dm *DKVMetrics) Merge(dm1 DKVMetrics) {
	dm.StoreLatency = MergeMapPercentile(dm.StoreLatency, dm1.StoreLatency, dm.Count)
	dm.NexusLatency = MergeMapPercentile(dm.NexusLatency, dm1.NexusLatency, dm.Count)
	dm.DKVLatency = MergeMapPercentile(dm.DKVLatency, dm1.DKVLatency, dm.Count)
	dm.StorageOpsCount = MergeMapUint64(dm.StorageOpsCount, dm1.StorageOpsCount)
	dm.StorageOpsErrorCount = MergeMapFloat64(dm.StorageOpsErrorCount, dm.StorageOpsErrorCount)
	dm.NexusOpsCount = MergeMapUint64(dm.NexusOpsCount, dm1.NexusOpsCount)
	dm.DKVReqCount = MergeMapUint64(dm.DKVReqCount, dm1.DKVReqCount)
	dm.Count = dm.Count + 1
}

func MergeMapUint64(m1, m2 map[string]uint64) map[string]uint64 {
	for k, v := range m2 {
		if _, exist := m1[k]; exist {
			m1[k] = m1[k] + v
		} else {
			m1[k] = v
		}
	}
	return m1
}

func MergeMapFloat64(m1, m2 map[string]float64) map[string]float64 {
	for k, v := range m2 {
		if _, exist := m1[k]; exist {
			m1[k] = m1[k] + v
		} else {
			m1[k] = v
		}
	}
	return m1
}

func MergeMapPercentile(m1, m2 map[string]*Percentile, count float64) map[string]*Percentile {
	for k, v := range m2 {
		if _, exist := m1[k]; exist {
			m1[k].P50 = (m1[k].P50*count + m2[k].P50) / (count + 1)
			m1[k].P90 = (m1[k].P90*count + m2[k].P90) / (count + 1)
			m1[k].P99 = (m1[k].P99*count + m2[k].P99) / (count + 1)
		} else {
			m1[k] = v
		}
	}
	return m1
}

type Percentile struct {
	P50 float64 `json:"p50"`
	P90 float64 `json:"p90"`
	P99 float64 `json:"p99"`
}

func NewDKVMetric() *DKVMetrics {
	return &DKVMetrics{
		TimeStamp:            time.Now().Unix(),
		StoreLatency:         make(map[string]*Percentile),
		NexusLatency:         make(map[string]*Percentile),
		DKVLatency:           make(map[string]*Percentile),
		StorageOpsCount:      make(map[string]uint64),
		StorageOpsErrorCount: make(map[string]float64),
		NexusOpsCount:        make(map[string]uint64),
		DKVReqCount:          make(map[string]uint64),
		Count:                1,
	}
}
func NewPercentile(quantile []*dto.Quantile) *Percentile {
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
