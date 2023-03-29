package rocksdb

import (
	"github.com/flipkart-incubator/dkv/internal/stats"
	"github.com/flipkart-incubator/dkv/internal/storage"
	"github.com/flipkart-incubator/gorocksdb"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type rocksDBCollector struct {
	memTableTotalGauge        *prometheus.Desc
	memTableUnflushedGauge    *prometheus.Desc
	memTableReadersTotalGauge *prometheus.Desc
	cacheTotalGauge           *prometheus.Desc
	db                        *gorocksdb.DB
	lgr                       *zap.Logger
}

func newRocksDBCollector(rdb *rocksDB) *rocksDBCollector {
	return &rocksDBCollector{
		memTableTotalGauge: prometheus.NewDesc(
			prometheus.BuildFQName(stats.Namespace, "rocksdb", "memory_usage_memtable_total"),
			"Rocksdb MemTableTotal estimates memory usage of all mem-tables",
			nil, nil),
		memTableUnflushedGauge: prometheus.NewDesc(
			prometheus.BuildFQName(stats.Namespace, "rocksdb", "memory_usage_memtable_unflushed"),
			"Rocksdb MemTableUnflushed estimates memory usage of unflushed mem-tables",
			nil, nil),
		memTableReadersTotalGauge: prometheus.NewDesc(
			prometheus.BuildFQName(stats.Namespace, "rocksdb", "memory_usage_memtable_readers_total"),
			"Rocksdb MemTableReadersTotal memory usage of table readers (indexes and bloom filters)",
			nil, nil),
		cacheTotalGauge: prometheus.NewDesc(
			prometheus.BuildFQName(stats.Namespace, "rocksdb", "memory_usage_cache_total"),
			"Rocksdb CacheTotal memory usage of cache",
			nil, nil),
		db:  rdb.db,
		lgr: rdb.opts.lgr,
	}

}

// Each and every collector must implement the Describe function.
// It essentially writes all descriptors to the prometheus desc channel.
func (collector *rocksDBCollector) Describe(ch chan<- *prometheus.Desc) {
	//Update this section with the each metric you create for a given collector
	ch <- collector.memTableTotalGauge
	ch <- collector.memTableUnflushedGauge
	ch <- collector.memTableReadersTotalGauge
	ch <- collector.cacheTotalGauge
}

// Collect implements required collect function for all promehteus collectors
func (collector *rocksDBCollector) Collect(ch chan<- prometheus.Metric) {
	memoryUsage, err := gorocksdb.GetApproximateMemoryUsageByType([]*gorocksdb.DB{collector.db}, nil)
	if err != nil {
		collector.lgr.Error("Failed to get rocksgb memory usage", zap.Error(err))
	} else {
		ch <- prometheus.MustNewConstMetric(collector.memTableTotalGauge, prometheus.GaugeValue, float64(memoryUsage.MemTableTotal))
		ch <- prometheus.MustNewConstMetric(collector.memTableUnflushedGauge, prometheus.GaugeValue, float64(memoryUsage.MemTableUnflushed))
		ch <- prometheus.MustNewConstMetric(collector.memTableReadersTotalGauge, prometheus.GaugeValue, float64(memoryUsage.MemTableReadersTotal))
		ch <- prometheus.MustNewConstMetric(collector.cacheTotalGauge, prometheus.GaugeValue, float64(memoryUsage.CacheTotal))
	}
}

var collector prometheus.Collector

// metricsCollector collects rocksdB metrics.
func (rdb *rocksDB) metricsCollector() {
	rdb.stat = storage.NewStat("rocksdb")
	rdb.opts.promRegistry.MustRegister(rdb.stat.RequestLatency, rdb.stat.ResponseError)
	collector = newRocksDBCollector(rdb)
	rdb.opts.promRegistry.MustRegister(collector)
}

func (rdb *rocksDB) unRegisterMetricsCollector() {
	rdb.opts.promRegistry.Unregister(collector)
	rdb.opts.promRegistry.Unregister(rdb.stat.RequestLatency)
	rdb.opts.promRegistry.Unregister(rdb.stat.ResponseError)
}
