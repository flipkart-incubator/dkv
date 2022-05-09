package opts

import (
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	flag "github.com/spf13/pflag"

	"github.com/spf13/viper"
)

type Config struct {

	// region level configuration.
	DisklessMode           bool   `mapstructure:"diskless"  desc:"Enables badger diskless mode where data is stored entirely in memory. "`
	NodeName               string `mapstructure:"node-name" desc:"Node Name"`
	DbEngine               string `mapstructure:"db-engine" desc:"Underlying DB engine for storing data - badger|rocksdb"`
	DbEngineIni            string `mapstructure:"db-engine-ini" desc:"An .ini file for configuring the underlying storage engine. Refer badger.ini or rocks.ini for more details."`
	DbRole                 string `mapstructure:"role" desc:"Role of the node - master|slave|standalone|discovery"`
	ReplPollIntervalString string `mapstructure:"repl-poll-interval" desc:"Interval used for polling changes from master. Eg., 10s, 5ms, 2h, etc."`
	BlockCacheSize         uint64 `mapstructure:"block-cache-size" desc:"Amount of cache (in bytes) to set aside for data blocks. A value of 0 disables block caching altogether."`
	DcID                   string `mapstructure:"dc-id" desc:"DC / Availability zone identifier"`
	Database               string `mapstructure:"database" desc:"Database identifier"`
	VBucket                string `mapstructure:"vbucket" desc:"vBucket identifier"`

	// Storage Configuration
	RootFolder string `mapstructure:"root-folder" desc:"Root Dir (optional)"` // used to derive other folders if not defined
	DbFolder   string `mapstructure:"db-folder" desc:"DB folder path for storing data files"`

	// Server Configuration
	ListenAddr     string `mapstructure:"listen-addr" desc:"Address on which the DKV service binds"`
	HttpListenAddr string `mapstructure:"http-listen-addr" desc:"Address on which the DKV service binds for http"`
	StatsdAddr     string `mapstructure:"statsd-addr" desc:"StatsD service address in host:port format"`

	//Service discovery related params
	DiscoveryConfig DiscoveryServiceConfiguration `mapstructure:"discovery-service" desc:"config for discovery server"`

	// Temporary variables to be removed once https://github.com/flipkart-incubator/dkv/issues/82 is fixed
	// The above issue causes replication issues during master switch due to inconsistent change numbers
	// Thus enabling hardcoded masters to not degrade current behaviour
	ReplicationMasterAddr string `mapstructure:"repl-master-addr" desc:"Service address of DKV master node for replication"`

	// Logging vars
	AccessLog string `mapstructure:"access-log" desc:"File for logging DKV accesses eg., stdout, stderr, /tmp/access.log"`
	LogLevel  string `mapstructure:"log-level" desc:"Log level for logging info|warn|debug|error"`

	ReplPollInterval time.Duration

	//Nexus vars
	NexusClusterName            string `mapstructure:"nexus-cluster-name" desc:"Nexus Cluster Name"`
	NexusNodeUrl                string `mapstructure:"nexus-node-url" desc:"Nexus Node URL (format: http://<local_node>:<port_num>)"`
	NexusClusterUrl             string `mapstructure:"nexus-cluster-url" desc:"Comma separated list of Nexus URLs of other nodes in the cluster"`
	NexusLeaseBasedReads        bool   `mapstructure:"nexus-lease-based-reads" desc:"Perform reads using RAFT leader leases"`
	NexusReplTimeout            int    `mapstructure:"nexus-repl-timeout" desc:"Replication timeout in seconds"`
	NexusLogDir                 string `mapstructure:"nexus-log-dir" desc:"Dir for storing RAFT logs"`
	NexusSnapDir                string `mapstructure:"nexus-snap-dir" desc:"Dir for storing RAFT snapshots"`
	NexusMaxSnapshots           int    `mapstructure:"nexus-max-snapshots" desc:"Maximum number of snapshot files to retain (0 is unlimited)"`
	NexusMaxWals                int    `mapstructure:"nexus-max-wals" desc:"Maximum number of WAL files to retain (0 is unlimited)"`
	NexusSnapshotCatchupEntries int    `mapstructure:"nexus-snapshot-catchup-entries" desc:"Number of entries for a slow follower to catch-up after compacting the raft storage entries"`
	NexusSnapshotCount          int    `mapstructure:"nexus-snapshot-count" desc:"Number of committed transactions to trigger a snapshot to disk"`
}

type DiscoveryClientConfig struct {
	DiscoveryServiceAddr string `mapstructure:"discovery-service-addr"`
	// time in seconds to push status updates to discovery server
	PushStatusInterval time.Duration `mapstructure:"push-status-interval"`
	// time in seconds to poll cluster info from discovery server
	PollClusterInfoInterval time.Duration `mapstructure:"poll-cluster-info-interval"`
}

/*
This class contains the behaviour of receiving status updates from nodes in the cluster
and providing the latest cluster info of active master / followers / slave of a region when requested
*/

type DiscoveryServerConfig struct {
	// time in seconds after which the status entry of the region & node combination can be purged
	StatusTTl int64 `mapstructure:"status-ttl"`
	// maximum time in seconds for the last status update to be considered valid
	// after exceeding this time the region & node combination can be marked invalid
	HeartbeatTimeout int64 `mapstructure:"heartbeat-timeout"`
}

type DiscoveryServiceConfiguration struct {
	//server side config for discovery service
	ServerConfig DiscoveryServerConfig `mapstructure:"server"`
	//client side config for discovery service
	ClientConfig DiscoveryClientConfig `mapstructure:"client"`
}

func (c *Config) parseConfig() {
	viper.Unmarshal(c)
	//Handling time duration variable unmarshalling
	if c.ReplPollIntervalString != "" {
		replicationPollInterval, err := time.ParseDuration(c.ReplPollIntervalString)
		if err != nil {
			log.Panicf("Failed to read Replication poll iterval value from config %v", err)
		}
		c.ReplPollInterval = replicationPollInterval
	}
	//Append node name to default db folder location
	if c.DbFolder == "" {
		c.DbFolder = path.Join(c.RootFolder, c.NodeName, "data")
	}

	if c.NexusLogDir == "" {
		c.NexusLogDir = path.Join(c.RootFolder, c.NodeName, "nexus")
	}

	if c.NexusSnapDir == "" {
		c.NexusSnapDir = path.Join(c.RootFolder, c.NodeName, "snap")
	}

	c.validateFlags()
}

func (c *Config) validateFlags() {
	if c.ListenAddr != "" && strings.IndexRune(c.ListenAddr, ':') < 0 {
		log.Panicf("given listen address: %s is invalid, must be in host:port format", c.ListenAddr)
	}
	if c.StatsdAddr != "" && strings.IndexRune(c.StatsdAddr, ':') < 0 {
		log.Panicf("given StatsD address: %s is invalid, must be in host:port format", c.StatsdAddr)
	}

	if c.DisklessMode && strings.ToLower(c.DbEngine) == "rocksdb" {
		log.Panicf("diskless is available only on Badger storage")
	}

	if c.DbEngineIni != "" {
		if _, err := os.Stat(c.DbEngineIni); err != nil && os.IsNotExist(err) {
			log.Panicf("given storage configuration file: %s does not exist", c.DbEngineIni)
		}
	}

	if c.DbRole == "slave" {
		if c.ReplicationMasterAddr != "" && strings.IndexRune(c.ReplicationMasterAddr, ':') < 0 {
			log.Panicf("given master address: %s for replication is invalid, must be in host:port format", c.ReplicationMasterAddr)
		}
	}

	//validate discovery configs
	if c.DbRole == "discovery" {
		if c.DiscoveryConfig.ServerConfig.HeartbeatTimeout <= 0 ||
			c.DiscoveryConfig.ServerConfig.StatusTTl <= 0 {
			log.Panicf("Invalid discovery server configuration")
		}
	}

	if c.DbRole != "none" && c.DbRole != "discovery" {
		if c.DiscoveryConfig.ClientConfig.DiscoveryServiceAddr == "" ||
			c.DiscoveryConfig.ClientConfig.PushStatusInterval <= 0 ||
			c.DiscoveryConfig.ClientConfig.PollClusterInfoInterval <= 0 {
			log.Panicf("Invalid discovery server client configuration")
		}
	}
}

func (c *Config) Print() {
	f := reflect.TypeOf(*c)
	v := reflect.ValueOf(*c)
	for i := 0; i < v.NumField(); i++ {
		value := v.Field(i).Interface()
		tag := f.Field(i).Tag
		name := tag.Get("mapstructure")
		if name == "" {
			continue
		}
		log.Printf("%s (%s) : %v\n", name, tag.Get("desc"), value)
	}
}

func (c *Config) Init(cfgFile string) {
	loadConfigFile(cfgFile)
	applyConfigOverrides()
	c.parseConfig()
}

func loadConfigFile(cfgFile string) {
	if cfgFile != "" {
		absPath, err := filepath.Abs(cfgFile)
		if err != nil {
			log.Panicf("Failed to convert cfg file to abs path %v", err)
		}
		viper.SetConfigFile(absPath)
	} else {
		// Search config in /etc/default directory with name "dkvsrv.yaml"
		viper.AddConfigPath("/etc/default")
		viper.SetConfigType("yaml")
		viper.SetConfigName("dkvsrv")
	}
	viper.AutomaticEnv()
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	} else {
		fmt.Println("No config file found")
		flag.Usage()
		os.Exit(2)
	}
}

func applyConfigOverrides() {
	flag.CommandLine.VisitUnknowns(func(f *flag.Flag) {
		// Apply the flag override value to viper config
		val := fmt.Sprintf("%v", f.Value.String())
		viper.Set(f.Name, val)
	})

	flag.VisitAll(func(f *flag.Flag) {
		// Apply the viper config value to the flag when the flag is not set and viper has a value
		if !isFlagPassed(f.Name) {
			if viper.IsSet(f.Name) {
				val := viper.Get(f.Name)
				flag.Set(f.Name, fmt.Sprintf("%v", val))
			}
		} else {
			// If the flag is passed, then set the viper config value to the flag value
			val := fmt.Sprintf("%v", f.Value.String())
			viper.Set(f.Name, val)
		}
	})
}

func isFlagPassed(name string) bool {
	found := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}
