package pkg

import (
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/any"
	"google.golang.org/protobuf/types/known/anypb"
)

// MustMarshalAny marshals a protobuf into an any.Any type, panicking
// if that operation fails.
func MustMarshalAny(pb proto.Message) *any.Any {
	msg, err := anypb.New(proto.MessageV2(pb))
	if err != nil {
		panic(err.Error())
	}
	return msg
}
