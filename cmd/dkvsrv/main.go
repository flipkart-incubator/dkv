package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"flag"
	"fmt"
	"google.golang.org/grpc/credentials"
	"io/ioutil"
	"log"
	"math/big"
	"net"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	utils "github.com/flipkart-incubator/dkv/internal"
	"github.com/flipkart-incubator/dkv/internal/master"
	"github.com/flipkart-incubator/dkv/internal/slave"
	"github.com/flipkart-incubator/dkv/internal/stats"
	"github.com/flipkart-incubator/dkv/internal/storage"
	"github.com/flipkart-incubator/dkv/internal/storage/badger"
	"github.com/flipkart-incubator/dkv/internal/storage/rocksdb"
	"github.com/flipkart-incubator/dkv/internal/sync"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
	nexus_api "github.com/flipkart-incubator/nexus/pkg/api"
	nexus "github.com/flipkart-incubator/nexus/pkg/raft"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var (
	disklessMode          bool
	dbEngine              string
	dbFolder              string
	dbListenAddr          string
	dbRole                string
	statsdAddr            string
	replMasterAddr        string
	masterMode			  string
	replPollInterval      time.Duration
	mode               string
	certPath              string
	keyPath               string
	caCertPath            string
	generatedCertValidity int
	generatedCertDir      string

	// Logging vars
	dbAccessLog    string
	verboseLogging bool
	accessLogger   *zap.Logger
	dkvLogger      *zap.Logger

	nexusLogDirFlag, nexusSnapDirFlag *flag.Flag

	statsCli stats.Client
)

func init() {
	flag.BoolVar(&disklessMode, "dbDiskless", false, fmt.Sprintf("Enables diskless mode where data is stored entirely in memory.\nAvailable on Badger for standalone and slave roles. (default %v)", disklessMode))
	flag.StringVar(&dbFolder, "dbFolder", "/tmp/dkvsrv", "DB folder path for storing data files")
	flag.StringVar(&dbListenAddr, "dbListenAddr", "127.0.0.1:8080", "Address on which the DKV service binds")
	flag.StringVar(&dbEngine, "dbEngine", "rocksdb", "Underlying DB engine for storing data - badger|rocksdb")
	flag.StringVar(&dbRole, "dbRole", "none", "DB role of this node - none|master|slave")
	flag.StringVar(&statsdAddr, "statsdAddr", "", "StatsD service address in host:port format")
	flag.StringVar(&replMasterAddr, "replMasterAddr", "", "Service address of DKV master node for replication")
	flag.StringVar(&masterMode, "masterMode", "insecure", "Service address of DKV master node for replication")
	flag.DurationVar(&replPollInterval, "replPollInterval", 5*time.Second, "Interval used for polling changes from master. Eg., 10s, 5ms, 2h, etc.")
	flag.StringVar(&dbAccessLog, "dbAccessLog", "", "File for logging DKV accesses eg., stdout, stderr, /tmp/access.log")
	flag.StringVar(&mode, "mode", "insecure", "Security mode for this node - insecure|autoTLS|serverTLS|mutualTLS")
	flag.StringVar(&certPath, "certPath", "", "Path for certificate file of this node")
	flag.StringVar(&keyPath, "keyPath", "", "Path for key file of this node")
	flag.StringVar(&caCertPath, "caCertPath", "", "Path for root certificate of the chain, i.e. CA certificate")
	flag.BoolVar(&verboseLogging, "verbose", false, fmt.Sprintf("Enable verbose logging.\nBy default, only warnings and errors are logged. (default %v)", verboseLogging))
	flag.IntVar(&generatedCertValidity, "generatedCertValidity", 365, "Validity(in days) for the generated certificate")
	flag.StringVar(&generatedCertDir, "generatedCertDir", "/tmp/dkv-certs", "Directory to store the generate key and certificates")
	setDKVDefaultsForNexusDirs()
}

type dkvSrvrRole string

const (
	noRole     dkvSrvrRole = "none"
	masterRole             = "master"
	slaveRole              = "slave"
)

func main() {
	flag.Parse()
	validateFlags()
	setupDKVLogger()
	setupAccessLogger()
	setFlagsForNexusDirs()
	setupStats()

	kvs, cp, ca, br := newKVStore()
	grpcSrvr, lstnr := newGrpcServerListener()
	defer grpcSrvr.GracefulStop()
	srvrRole := toDKVSrvrRole(dbRole)
	srvrRole.printFlags()

	switch srvrRole {
	case noRole:
		dkvSvc := master.NewStandaloneService(kvs, nil, br, dkvLogger, statsCli)
		defer dkvSvc.Close()
		serverpb.RegisterDKVServer(grpcSrvr, dkvSvc)
		serverpb.RegisterDKVBackupRestoreServer(grpcSrvr, dkvSvc)
	case masterRole:
		if cp == nil {
			log.Panicf("Storage engine %s is not supported for DKV master role.", dbEngine)
		}
		var dkvSvc master.DKVService
		if haveFlagsWithPrefix("nexus") {
			dkvSvc = master.NewDistributedService(kvs, cp, br, newDKVReplicator(kvs), dkvLogger, statsCli)
			serverpb.RegisterDKVClusterServer(grpcSrvr, dkvSvc.(master.DKVClusterService))
		} else {
			dkvSvc = master.NewStandaloneService(kvs, cp, br, dkvLogger, statsCli)
			serverpb.RegisterDKVBackupRestoreServer(grpcSrvr, dkvSvc)
		}
		defer dkvSvc.Close()
		serverpb.RegisterDKVServer(grpcSrvr, dkvSvc)
		serverpb.RegisterDKVReplicationServer(grpcSrvr, dkvSvc)
	case slaveRole:
		if replCli, err := utils.NewDKVClient(utils.DKVConfig{ServerMode: masterMode,
			SrvrAddr: replMasterAddr, KeyPath: keyPath, CertPath: certPath,
			CaCertPath: caCertPath});
		
		err != nil {
			panic(err)
		} else {
			defer replCli.Close()
			dkvSvc, _ := slave.NewService(kvs, ca, replCli, replPollInterval, dkvLogger, statsCli)
			defer dkvSvc.Close()
			serverpb.RegisterDKVServer(grpcSrvr, dkvSvc)
		}
	default:
		panic("Invalid 'dbRole'. Allowed values are none|master|slave.")
	}
	go grpcSrvr.Serve(lstnr)
	sig := <-setupSignalHandler()
	log.Printf("[WARN] Caught signal: %v. Shutting down...\n", sig)
}

func validateFlags() {
	if dbListenAddr != "" && strings.IndexRune(dbListenAddr, ':') < 0 {
		log.Panicf("given listen address: %s is invalid, must be in host:port format", dbListenAddr)
	}
	if replMasterAddr != "" && strings.IndexRune(replMasterAddr, ':') < 0 {
		log.Panicf("given master address: %s for replication is invalid, must be in host:port format", replMasterAddr)
	}
	if statsdAddr != "" && strings.IndexRune(statsdAddr, ':') < 0 {
		log.Panicf("given StatsD address: %s is invalid, must be in host:port format", statsdAddr)
	}
	if disklessMode && (strings.ToLower(dbEngine) == "rocksdb" || strings.ToLower(dbRole) == masterRole) {
		log.Panicf("diskless is available only on Badger storage and for standalone and slave roles")
	}
	if strings.ToLower(dbRole) == slaveRole && (replMasterAddr == "" || masterMode == "") {
		log.Panicf("replMasterAddr must be given in slave mode")
	}

	if strings.ToLower(dbRole) == slaveRole {
		utils.ValidateDKVConfig(utils.DKVConfig{ServerMode: masterMode,
			SrvrAddr: replMasterAddr, KeyPath: keyPath, CertPath: certPath,
			CaCertPath: caCertPath})
	}

	validateServerModeCompliantFlags()
}

func validateServerModeCompliantFlags()  {
	utils.ValidateDKVConfig(utils.DKVConfig{ServerMode: mode,
		SrvrAddr: replMasterAddr, KeyPath: keyPath, CertPath: certPath,
		CaCertPath: caCertPath})
	srvrMode := utils.ToServerMode(mode)
	switch srvrMode {
	case utils.AutoTLS:
		if generatedCertValidity <= 0 {
			log.Panicf("Validaty period for the generated certificates(generatedCertValidity) should be > 0")
		}

		if generatedCertDir == "" {
			log.Panicf("Directory for certificate generation(generatedCertDir) must not be empty")
		}
	}
}



func setupAccessLogger() {
	accessLogger = zap.NewNop()
	if dbAccessLog != "" {
		accessLoggerConfig := zap.Config{
			Level:         zap.NewAtomicLevelAt(zap.InfoLevel),
			Development:   false,
			Encoding:      "console",
			DisableCaller: true,

			EncoderConfig: zapcore.EncoderConfig{
				TimeKey:        "ts",
				LevelKey:       "level",
				NameKey:        "logger",
				CallerKey:      "caller",
				MessageKey:     "msg",
				StacktraceKey:  "stacktrace",
				LineEnding:     zapcore.DefaultLineEnding,
				EncodeLevel:    zapcore.LowercaseLevelEncoder,
				EncodeTime:     zapcore.ISO8601TimeEncoder,
				EncodeDuration: zapcore.StringDurationEncoder,
				EncodeCaller:   zapcore.ShortCallerEncoder,
			},

			OutputPaths:      []string{dbAccessLog},
			ErrorOutputPaths: []string{dbAccessLog},
		}
		if lg, err := accessLoggerConfig.Build(); err != nil {
			log.Printf("[WARN] Unable to configure access logger. Error: %v\n", err)
		} else {
			accessLogger = lg
		}
	}
}

func setupDKVLogger() {
	dkvLoggerConfig := zap.Config{
		Development:   false,
		Encoding:      "console",
		DisableCaller: true,

		EncoderConfig: zapcore.EncoderConfig{
			TimeKey:        "ts",
			LevelKey:       "level",
			NameKey:        "logger",
			CallerKey:      "caller",
			MessageKey:     "msg",
			LineEnding:     zapcore.DefaultLineEnding,
			EncodeLevel:    zapcore.LowercaseLevelEncoder,
			EncodeTime:     zapcore.ISO8601TimeEncoder,
			EncodeDuration: zapcore.StringDurationEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder,
		},
		OutputPaths:      []string{"stderr"},
		ErrorOutputPaths: []string{"stderr"},
	}

	if verboseLogging {
		dkvLoggerConfig.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
		dkvLoggerConfig.EncoderConfig.StacktraceKey = "stacktrace"
	} else {
		dkvLoggerConfig.Level = zap.NewAtomicLevelAt(zap.WarnLevel)
	}

	if lg, err := dkvLoggerConfig.Build(); err != nil {
		log.Printf("[WARN] Unable to configure DKV logger. Error: %v\n", err)
		dkvLogger = zap.NewNop()
	} else {
		dkvLogger = lg
	}
}

func newGrpcServerListener() (*grpc.Server, net.Listener) {
	var grpcSrvr *grpc.Server

	if mode == utils.Insecure {
		grpcSrvr = grpc.NewServer(
			grpc.StreamInterceptor(grpc_zap.StreamServerInterceptor(accessLogger)),
			grpc.UnaryInterceptor(grpc_zap.UnaryServerInterceptor(accessLogger)),
		)
	} else {
		srvrCred, err := loadTLSCredentials()
		if err != nil {
			dkvLogger.Sugar().Fatal("Unable to load tls credentials", err)
		}
		grpcSrvr = grpc.NewServer(
			grpc.Creds(srvrCred),
			grpc.StreamInterceptor(grpc_zap.StreamServerInterceptor(accessLogger)),
			grpc.UnaryInterceptor(grpc_zap.UnaryServerInterceptor(accessLogger)),
		)
	}
	reflection.Register(grpcSrvr)
	return grpcSrvr, newListener()
}

func loadTLSCredentials() (credentials.TransportCredentials, error) {
	var serverCert tls.Certificate
	var err error
	if mode == utils.AutoTLS {
		err = generateSelfSignedCert()
		if err != nil {
			return nil, err
		}
	}

	serverCert, err = tls.LoadX509KeyPair(certPath, keyPath)

	if err != nil {
		return nil, err
	}

	var config *tls.Config

	if mode == utils.MutualTLS {
		pemClientCA, err := ioutil.ReadFile(caCertPath)
		if err != nil {
			return nil, err
		}

		certPool := x509.NewCertPool()
		if !certPool.AppendCertsFromPEM(pemClientCA) {
			return nil, fmt.Errorf("failed to add client CA's certificate")
		}

		config = &tls.Config{
			Certificates: []tls.Certificate{serverCert},
			ClientAuth:   tls.RequireAndVerifyClientCert,
			ClientCAs:    certPool,
		}
	} else {
		config = &tls.Config{
			Certificates: []tls.Certificate{serverCert},
			ClientAuth:   tls.NoClientCert,
		}
	}
	return credentials.NewTLS(config), nil
}

func generateSelfSignedCert() error {
	var err error
	if _, errCreate := os.Stat(generatedCertDir); os.IsNotExist(errCreate) {
		err = os.MkdirAll(generatedCertDir, os.ModePerm)
	}

	if err != nil {
		return err
	}
	certPath = filepath.Join(generatedCertDir, "cert.pem")
	keyPath = filepath.Join(generatedCertDir, "key.pem")

	_, errcert := os.Stat(certPath)
	_, errkey := os.Stat(keyPath)
	if errcert == nil && errkey == nil {
		return nil
	}

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return err
	}

	tmpl := x509.Certificate{
		SerialNumber: serialNumber,
		Subject:      pkix.Name{Organization: []string{"DKV"}},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Duration(generatedCertValidity) * (24 * time.Hour)),

		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}


	log.Println("automatically generate certificates",
			zap.Time("certificate-validity-bound-not-after", tmpl.NotAfter))

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &priv.PublicKey, priv)
	if err != nil {
		return err
	}

	certOut, err := os.Create(certPath)
	if err != nil {
		return err
	}
	pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	certOut.Close()
	log.Println("created cert file", zap.String("path", certPath))

	b, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		return err
	}
	keyOut, err := os.OpenFile(keyPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}

	pem.Encode(keyOut, &pem.Block{Type: "EC PRIVATE KEY", Bytes: b})
	keyOut.Close()
	return nil
}

func newListener() (lis net.Listener) {
	var err error
	if lis, err = net.Listen("tcp", dbListenAddr); err != nil {
		log.Panicf("failed to listen: %v", err)
		return
	}
	return
}

func setupSignalHandler() <-chan os.Signal {
	signals := []os.Signal{syscall.SIGINT, syscall.SIGQUIT, syscall.SIGSTOP, syscall.SIGTERM}
	stopChan := make(chan os.Signal, len(signals))
	signal.Notify(stopChan, signals...)
	return stopChan
}

func haveFlagsWithPrefix(prefix string) bool {
	res := false
	flag.Visit(func(f *flag.Flag) {
		if strings.HasPrefix(f.Name, prefix) {
			res = true
		}
	})
	return res
}

func printFlagsWithPrefix(prefixes ...string) {
	log.Println("Launching DKV server with following flags:")
	flag.VisitAll(func(f *flag.Flag) {
		for _, pf := range prefixes {
			if strings.HasPrefix(f.Name, pf) {
				log.Printf("%s (%s): %v\n", f.Name, f.Usage, f.Value)
			}
		}
	})
}

func toDKVSrvrRole(role string) dkvSrvrRole {
	return dkvSrvrRole(strings.TrimSpace(strings.ToLower(role)))
}

func (role dkvSrvrRole) printFlags() {
	switch role {
	case noRole:
		printFlagsWithPrefix("db")
	case masterRole:
		if haveFlagsWithPrefix("nexus") {
			printFlagsWithPrefix("db", "nexus")
		} else {
			printFlagsWithPrefix("db")
		}
	case slaveRole:
		printFlagsWithPrefix("db", "repl")
	}
}

func setDKVDefaultsForNexusDirs() {
	nexusLogDirFlag, nexusSnapDirFlag = flag.Lookup("nexusLogDir"), flag.Lookup("nexusSnapDir")
	dbPath := flag.Lookup("dbFolder").DefValue
	nexusLogDirFlag.DefValue, nexusSnapDirFlag.DefValue = path.Join(dbPath, "logs"), path.Join(dbPath, "snap")
	nexusLogDirFlag.Value.Set("")
	nexusSnapDirFlag.Value.Set("")
}

func setFlagsForNexusDirs() {
	if nexusLogDirFlag.Value.String() == "" {
		nexusLogDirFlag.Value.Set(path.Join(dbFolder, "logs"))
	}
	if nexusSnapDirFlag.Value.String() == "" {
		nexusSnapDirFlag.Value.Set(path.Join(dbFolder, "snap"))
	}
}

func setupStats() {
	if statsdAddr != "" {
		statsCli = stats.NewStatsDClient(statsdAddr, "nexus_redis")
	} else {
		statsCli = stats.NewNoOpClient()
	}
}

const cacheSize = 3 << 30

func newKVStore() (storage.KVStore, storage.ChangePropagator, storage.ChangeApplier, storage.Backupable) {
	slg := dkvLogger.Sugar()
	defer slg.Sync()

	if err := os.MkdirAll(dbFolder, 0777); err != nil {
		slg.Fatalf("Unable to create DB folder at %s. Error: %v.", dbFolder, err)
	}

	dbDir := path.Join(dbFolder, "data")
	slg.Infof("Using %s as data folder", dbDir)
	switch dbEngine {
	case "rocksdb":
		rocksDb, err := rocksdb.OpenDB(dbDir,
			rocksdb.WithCacheSize(cacheSize),
			rocksdb.WithStats(statsCli),
			rocksdb.WithLogger(dkvLogger))
		if err != nil {
			dkvLogger.Panic("RocksDB engine init failed", zap.Error(err))
		}
		return rocksDb, rocksDb, rocksDb, rocksDb
	case "badger":
		var badgerDb badger.DB
		var err error
		if disklessMode {
			badgerDb, err = badger.OpenInMemDB(
				badger.WithLogger(dkvLogger),
				badger.WithStats(statsCli))
		} else {
			badgerDb, err = badger.OpenDB(dbDir,
				badger.WithLogger(dkvLogger),
				badger.WithSyncWrites(),
				badger.WithStats(statsCli))
		}
		if err != nil {
			dkvLogger.Panic("Badger engine init failed", zap.Error(err))
		}
		return badgerDb, nil, badgerDb, badgerDb
	default:
		slg.Panicf("Unknown storage engine: %s", dbEngine)
		return nil, nil, nil, nil
	}
}

func mkdirNexusDirs() {
	if err := os.MkdirAll(nexusLogDirFlag.Value.String(), 0777); err != nil {
		log.Panicf("Unable to create Nexus logDir. Error: %v", err)
	}
	if err := os.MkdirAll(nexusSnapDirFlag.Value.String(), 0777); err != nil {
		log.Panicf("Unable to create Nexus snapDir. Error: %v", err)
	}
}

func newDKVReplicator(kvs storage.KVStore) nexus_api.RaftReplicator {
	mkdirNexusDirs()
	replStore := sync.NewDKVReplStore(kvs)
	nexusOpts := nexus.OptionsFromFlags()
	nexusOpts = append(nexusOpts, nexus.StatsDAddr(statsdAddr))
	if nexusRepl, err := nexus_api.NewRaftReplicator(replStore, nexusOpts...); err != nil {
		panic(err)
	} else {
		nexusRepl.Start()
		return nexusRepl
	}
}
