package stats

import (
	"strconv"
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

type jFloat64 float64

func MergeMapPercentile(m1, m2 map[string]*Percentile, count float64) map[string]*Percentile {
	var jCount jFloat64 = jFloat64(count)
	for k, v := range m2 {
		if _, exist := m1[k]; exist {
			m1[k].P50 = (m1[k].P50*jCount + m2[k].P50) / (jCount + 1)
			m1[k].P90 = (m1[k].P90*jCount + m2[k].P90) / (jCount + 1)
			m1[k].P99 = (m1[k].P99*jCount + m2[k].P99) / (jCount + 1)
		} else {
			m1[k] = v
		}
	}
	return m1
}

// This is required because json doesnt allow NaN or Inf values
// Based on https://stackoverflow.com/a/32085427
func (fs jFloat64) MarshalJSON() ([]byte, error) {
	vs := strconv.FormatFloat(float64(fs), 'f', 2, 64)
	return []byte(`"` + vs + `"`), nil
}

func (fs *jFloat64) UnmarshalJSON(b []byte) error {
	if b[0] == '"' {
		b = b[1 : len(b)-1]
	}
	f, err := strconv.ParseFloat(string(b), 64)
	*fs = jFloat64(f)
	return err
}

type Percentile struct {
	P50 jFloat64 `json:"p50"`
	P90 jFloat64 `json:"p90"`
	P99 jFloat64 `json:"p99"`
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
			percentile.P50 = jFloat64(*q.Value)
		case 0.9:
			percentile.P90 = jFloat64(*q.Value)
		case 0.99:
			percentile.P99 = jFloat64(*q.Value)
		}
	}
	return percentile
}
