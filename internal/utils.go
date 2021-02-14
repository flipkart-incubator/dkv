package internal

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"github.com/flipkart-incubator/dkv/pkg/ctl"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"io/ioutil"
	"log"
	"strings"
)

type ServerMode string

const (
	ServerTLS     ServerMode = "serverTLS"
	AutoTLS             = "autoTLS"
	MutualTLS           = "mutualTLS"
	Insecure = "insecure"
)

type DKVConfig struct {
	ServerMode string
	SrvrAddr string
	KeyPath string
	CertPath string
	CaCertPath string
}

func ValidateCertAndKeyPath(keyPath string, certPath string)  {
	if keyPath == "" {
		log.Panicf("Key path(keyPath) must not be empty for non-auto TLS mode")
	}

	if certPath == "" {
		log.Panicf("Certificate path(certPath) must not be empty for non-auto TLS mode")
	}
}

func ValidateDKVConfig(config DKVConfig)  {
	serverMode := ToServerMode(config.ServerMode)
	switch serverMode {
	case AutoTLS:
	case MutualTLS:
		if config.CaCertPath == "" {
			log.Panicf("CA certifacte path(caCertPath) must not be empty for mutualTLS mode")
		}
		ValidateCertAndKeyPath(config.KeyPath, config.CertPath)
	case ServerTLS:
		ValidateCertAndKeyPath(config.KeyPath, config.CertPath)
	case Insecure:
	default:
		log.Panicf("Invalid server mode. Allowed values are insecure|autoTLS|serverTLS|mutualTLS")
	}
}


func ToServerMode(mode string) ServerMode {
	return ServerMode(strings.TrimSpace(mode))
}

func NewDKVClient(clientConfig DKVConfig) (*ctl.DKVClient, error) {
	srvrMode := ToServerMode(clientConfig.ServerMode)
	var opt grpc.DialOption
	var err error = nil
	var config *tls.Config
	switch srvrMode {
	case AutoTLS:
		config = &tls.Config{
			InsecureSkipVerify: true,
		}
		opt = grpc.WithTransportCredentials(credentials.NewTLS(config))
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
	return ctl.NewDKVClient(clientConfig.SrvrAddr, opt)
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