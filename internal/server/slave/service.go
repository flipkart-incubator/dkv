package slave

import (
	"io"

	"github.com/flipkart-incubator/dkv/pkg/serverpb"
)

type DKVService interface {
	io.Closer
	serverpb.DKVServer
}
