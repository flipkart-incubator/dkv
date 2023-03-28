package badger

import (
	"github.com/flipkart-incubator/dkv/internal/stats"
	"github.com/prometheus/client_golang/prometheus"
)

// NewBadgerCollector returns a prometheus Collector for Badger metrics from expvar.
func (bdb *badgerDB) metricsCollector() {
	collector := prometheus.NewExpvarCollector(map[string]*prometheus.Desc{
		"badger_v3_disk_reads_total": prometheus.NewDesc(
			prometheus.BuildFQName(stats.Namespace, "badger", "disk_reads_total"),
			"Number of cumulative reads by Badger",
			nil, stats.ConstLabels,
		),
		"badger_v3_disk_writes_total": prometheus.NewDesc(
			prometheus.BuildFQName(stats.Namespace, "badger", "disk_writes_total"),
			"Number of cumulative writes by Badger",
			nil, stats.ConstLabels,
		),
		"badger_v3_read_bytes": prometheus.NewDesc(
			prometheus.BuildFQName(stats.Namespace, "badger", "read_bytes"),
			"Number of cumulative bytes read by Badger",
			nil, stats.ConstLabels,
		),
		"badger_v3_written_bytes": prometheus.NewDesc(
			prometheus.BuildFQName(stats.Namespace, "badger", "written_bytes"),
			"Number of cumulative bytes written by Badger",
			nil, stats.ConstLabels,
		),
		"badger_v3_lsm_level_gets_total": prometheus.NewDesc(
			prometheus.BuildFQName(stats.Namespace, "badger", "lsm_level_gets_total"),
			"Total number of LSM gets",
			[]string{"level"}, stats.ConstLabels,
		),
		"badger_v3_lsm_bloom_hits_total": prometheus.NewDesc(
			prometheus.BuildFQName(stats.Namespace, "badger", "lsm_bloom_hits_total"),
			"Total number of LSM bloom hits",
			[]string{"level"}, stats.ConstLabels,
		),
		"badger_v3_gets_total": prometheus.NewDesc(
			prometheus.BuildFQName(stats.Namespace, "badger", "gets_total"),
			"Total number of gets",
			nil, stats.ConstLabels,
		),
		"badger_v3_puts_total": prometheus.NewDesc(
			prometheus.BuildFQName(stats.Namespace, "badger", "puts_total"),
			"Total number of puts",
			nil, stats.ConstLabels,
		),
		"badger_v3_blocked_puts_total": prometheus.NewDesc(
			prometheus.BuildFQName(stats.Namespace, "badger", "blocked_puts_total"),
			"Total number of blocked puts",
			nil, stats.ConstLabels,
		),
		"badger_v3_memtable_gets_total": prometheus.NewDesc(
			prometheus.BuildFQName(stats.Namespace, "badger", "memtable_gets_total"),
			"Total number of memtable gets",
			nil, stats.ConstLabels,
		),
		"badger_v3_lsm_size_bytes": prometheus.NewDesc(
			prometheus.BuildFQName(stats.Namespace, "badger", "lsm_size_bytes"),
			"Size of the LSM in bytes",
			[]string{"dir"}, stats.ConstLabels,
		),
		"badger_v3_vlog_size_bytes": prometheus.NewDesc(
			prometheus.BuildFQName(stats.Namespace, "badger", "vlog_size_bytes"),
			"Size of the value log in bytes",
			[]string{"dir"}, stats.ConstLabels,
		),
		"badger_v3_pending_writes_total": prometheus.NewDesc(
			prometheus.BuildFQName(stats.Namespace, "badger", "pending_writes_total"),
			"Total number of pending writes",
			[]string{"dir"}, stats.ConstLabels,
		),
		"badger_v3_compactions_current": prometheus.NewDesc(
			prometheus.BuildFQName(stats.Namespace, "badger", "compactions_current"),
			"Number of tables being actively compacted",
			nil, stats.ConstLabels,
		),
	})

	bdb.opts.promRegistry.MustRegister(collector)
}
