package internal

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"github.com/flipkart-incubator/dkv/pkg/ctl"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/reflection"
	"io/ioutil"
	"log"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

type ConnectionMode string

const (
	ServerTLS ConnectionMode = "serverTLS"
	MutualTLS                = "mutualTLS"
	Insecure                 = "insecure"
)

type DKVConfig struct {
	ConnectionMode ConnectionMode
	SrvrAddr       string
	KeyPath        string
	CertPath       string
	CaCertPath     string
}

func NewDKVClient(clientConfig DKVConfig, authority string) (*ctl.DKVClient, error) {
	var opt grpc.DialOption
	var err error = nil
	var config *tls.Config
	var clientMode = clientConfig.ConnectionMode
	if clientMode == "" {
		clientMode = clientModeFromFlags(clientConfig)
	}
	switch clientMode {
	case MutualTLS:
		config, err = getTLSConfigWithCertPool(clientConfig.CaCertPath)
		if err == nil {
			var cert tls.Certificate
			cert, err = tls.LoadX509KeyPair(clientConfig.CertPath, clientConfig.KeyPath)
			if err == nil {
				config.Certificates = []tls.Certificate{cert}
				opt = grpc.WithTransportCredentials(credentials.NewTLS(config))
			}
		}
	case ServerTLS:
		config, err = getTLSConfigWithCertPool(clientConfig.CaCertPath)
		if err == nil {
			opt = grpc.WithTransportCredentials(credentials.NewTLS(config))
		}
	case Insecure:
		opt = grpc.WithInsecure()
	}
	if err != nil {
		return nil, err
	}
	return ctl.NewDKVClient(clientConfig.SrvrAddr, authority, opt)
}

func getTLSConfigWithCertPool(caCertPath string) (*tls.Config, error) {
	certPool := x509.NewCertPool()
	bs, err := ioutil.ReadFile(caCertPath)
	if err != nil {
		return nil, err
	}
	ok := certPool.AppendCertsFromPEM(bs)
	if !ok {
		return nil, errors.New("failed to append certs")
	}
	return &tls.Config{InsecureSkipVerify: false, RootCAs: certPool}, err
}

func NewGrpcServerListener(config DKVConfig, loogger *zap.Logger) (*grpc.Server, net.Listener) {
	opts := []grpc.ServerOption{
		grpc.StreamInterceptor(grpc_zap.StreamServerInterceptor(loogger)),
		grpc.UnaryInterceptor(grpc_zap.UnaryServerInterceptor(loogger))}
	if config.ConnectionMode != Insecure {
		srvrCred, err := loadTLSCredentials(config)
		if err != nil {
			log.Fatal("Unable to load tls credentials", err)
		}
		opts = append(opts, grpc.Creds(srvrCred))
	}
	grpcSrvr := grpc.NewServer(opts...)
	reflection.Register(grpcSrvr)
	return grpcSrvr, NewListener(config.SrvrAddr)
}

func loadTLSCredentials(clientConfig DKVConfig) (credentials.TransportCredentials, error) {
	var serverCert tls.Certificate
	var err error

	serverCert, err = tls.LoadX509KeyPair(clientConfig.CertPath, clientConfig.KeyPath)

	if err != nil {
		return nil, err
	}

	var config = &tls.Config{
		Certificates: []tls.Certificate{serverCert},
	}

	if clientConfig.ConnectionMode == MutualTLS {
		pemClientCA, err := ioutil.ReadFile(clientConfig.CaCertPath)
		if err != nil {
			return nil, err
		}

		certPool := x509.NewCertPool()
		if !certPool.AppendCertsFromPEM(pemClientCA) {
			return nil, fmt.Errorf("failed to add client CA's certificate")
		}

		config.ClientAuth = tls.RequireAndVerifyClientCert
		config.ClientCAs = certPool
	} else {
		config.ClientAuth = tls.NoClientCert
	}
	return credentials.NewTLS(config), nil
}

func GenerateSelfSignedCert(generatedCertDir string, generatedCertValidity int) (string, string, error) {
	var err error
	if _, errCreate := os.Stat(generatedCertDir); os.IsNotExist(errCreate) {
		err = os.MkdirAll(generatedCertDir, os.ModePerm)
	}

	if err != nil {
		return "", "", err
	}
	certPath := filepath.Join(generatedCertDir, "cert.pem")
	keyPath := filepath.Join(generatedCertDir, "key.pem")

	_, errcert := os.Stat(certPath)
	_, errkey := os.Stat(keyPath)
	if errcert == nil && errkey == nil {
		return "", "", nil
	}

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return "", "", err
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
		return "", "", err
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &priv.PublicKey, priv)
	if err != nil {
		return "", "", err
	}

	certOut, err := os.Create(certPath)
	if err != nil {
		return "", "", err
	}
	pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	certOut.Close()
	log.Println("created cert file", zap.String("path", certPath))

	b, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		return "", "", err
	}
	keyOut, err := os.OpenFile(keyPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return "", "", err
	}

	pem.Encode(keyOut, &pem.Block{Type: "EC PRIVATE KEY", Bytes: b})
	keyOut.Close()
	return certPath, keyPath, nil
}

func NewListener(listenAddr string) (lis net.Listener) {
	var err error
	if lis, err = net.Listen("tcp", listenAddr); err != nil {
		log.Panicf("failed to listen: %v", err)
		return
	}
	return
}

func clientModeFromFlags(config DKVConfig) ConnectionMode {
	if config.CaCertPath != "" {
		if config.KeyPath != "" && config.CertPath != "" {
			return MutualTLS
		}
		return ServerTLS
	} else {
		return Insecure
	}
}