// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v3.21.5
// source: pkg/serverpb/api.proto

package serverpb

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// DKVClient is the client API for DKV service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type DKVClient interface {
	// Put puts the given key into the key value store.
	Put(ctx context.Context, in *PutRequest, opts ...grpc.CallOption) (*PutResponse, error)
	// Delete deletes the given key from the key value store.
	Delete(ctx context.Context, in *DeleteRequest, opts ...grpc.CallOption) (*DeleteResponse, error)
	// Get gets the value associated with the given key from the key value store.
	Get(ctx context.Context, in *GetRequest, opts ...grpc.CallOption) (*GetResponse, error)
	// MultiGet gets the values associated with given keys from the key value store.
	MultiGet(ctx context.Context, in *MultiGetRequest, opts ...grpc.CallOption) (*MultiGetResponse, error)
	// MultiPut puts the given keys into the key value store.
	MultiPut(ctx context.Context, in *MultiPutRequest, opts ...grpc.CallOption) (*PutResponse, error)
	// Iterate iterates through the entire keyspace in no particular order and
	// returns the results as a stream of key value pairs.
	Iterate(ctx context.Context, in *IterateRequest, opts ...grpc.CallOption) (DKV_IterateClient, error)
	// CompareAndSet offers the standard CAS style transaction over a given
	// key. Intended to be used in concurrent workloads with less contention.
	CompareAndSet(ctx context.Context, in *CompareAndSetRequest, opts ...grpc.CallOption) (*CompareAndSetResponse, error)
}

type dKVClient struct {
	cc grpc.ClientConnInterface
}

func NewDKVClient(cc grpc.ClientConnInterface) DKVClient {
	return &dKVClient{cc}
}

func (c *dKVClient) Put(ctx context.Context, in *PutRequest, opts ...grpc.CallOption) (*PutResponse, error) {
	out := new(PutResponse)
	err := c.cc.Invoke(ctx, "/dkv.serverpb.DKV/Put", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *dKVClient) Delete(ctx context.Context, in *DeleteRequest, opts ...grpc.CallOption) (*DeleteResponse, error) {
	out := new(DeleteResponse)
	err := c.cc.Invoke(ctx, "/dkv.serverpb.DKV/Delete", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *dKVClient) Get(ctx context.Context, in *GetRequest, opts ...grpc.CallOption) (*GetResponse, error) {
	out := new(GetResponse)
	err := c.cc.Invoke(ctx, "/dkv.serverpb.DKV/Get", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *dKVClient) MultiGet(ctx context.Context, in *MultiGetRequest, opts ...grpc.CallOption) (*MultiGetResponse, error) {
	out := new(MultiGetResponse)
	err := c.cc.Invoke(ctx, "/dkv.serverpb.DKV/MultiGet", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *dKVClient) MultiPut(ctx context.Context, in *MultiPutRequest, opts ...grpc.CallOption) (*PutResponse, error) {
	out := new(PutResponse)
	err := c.cc.Invoke(ctx, "/dkv.serverpb.DKV/MultiPut", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *dKVClient) Iterate(ctx context.Context, in *IterateRequest, opts ...grpc.CallOption) (DKV_IterateClient, error) {
	stream, err := c.cc.NewStream(ctx, &DKV_ServiceDesc.Streams[0], "/dkv.serverpb.DKV/Iterate", opts...)
	if err != nil {
		return nil, err
	}
	x := &dKVIterateClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type DKV_IterateClient interface {
	Recv() (*IterateResponse, error)
	grpc.ClientStream
}

type dKVIterateClient struct {
	grpc.ClientStream
}

func (x *dKVIterateClient) Recv() (*IterateResponse, error) {
	m := new(IterateResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *dKVClient) CompareAndSet(ctx context.Context, in *CompareAndSetRequest, opts ...grpc.CallOption) (*CompareAndSetResponse, error) {
	out := new(CompareAndSetResponse)
	err := c.cc.Invoke(ctx, "/dkv.serverpb.DKV/CompareAndSet", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// DKVServer is the server API for DKV service.
// All implementations should embed UnimplementedDKVServer
// for forward compatibility
type DKVServer interface {
	// Put puts the given key into the key value store.
	Put(context.Context, *PutRequest) (*PutResponse, error)
	// Delete deletes the given key from the key value store.
	Delete(context.Context, *DeleteRequest) (*DeleteResponse, error)
	// Get gets the value associated with the given key from the key value store.
	Get(context.Context, *GetRequest) (*GetResponse, error)
	// MultiGet gets the values associated with given keys from the key value store.
	MultiGet(context.Context, *MultiGetRequest) (*MultiGetResponse, error)
	// MultiPut puts the given keys into the key value store.
	MultiPut(context.Context, *MultiPutRequest) (*PutResponse, error)
	// Iterate iterates through the entire keyspace in no particular order and
	// returns the results as a stream of key value pairs.
	Iterate(*IterateRequest, DKV_IterateServer) error
	// CompareAndSet offers the standard CAS style transaction over a given
	// key. Intended to be used in concurrent workloads with less contention.
	CompareAndSet(context.Context, *CompareAndSetRequest) (*CompareAndSetResponse, error)
}

// UnimplementedDKVServer should be embedded to have forward compatible implementations.
type UnimplementedDKVServer struct {
}

func (UnimplementedDKVServer) Put(context.Context, *PutRequest) (*PutResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Put not implemented")
}
func (UnimplementedDKVServer) Delete(context.Context, *DeleteRequest) (*DeleteResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Delete not implemented")
}
func (UnimplementedDKVServer) Get(context.Context, *GetRequest) (*GetResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Get not implemented")
}
func (UnimplementedDKVServer) MultiGet(context.Context, *MultiGetRequest) (*MultiGetResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method MultiGet not implemented")
}
func (UnimplementedDKVServer) MultiPut(context.Context, *MultiPutRequest) (*PutResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method MultiPut not implemented")
}
func (UnimplementedDKVServer) Iterate(*IterateRequest, DKV_IterateServer) error {
	return status.Errorf(codes.Unimplemented, "method Iterate not implemented")
}
func (UnimplementedDKVServer) CompareAndSet(context.Context, *CompareAndSetRequest) (*CompareAndSetResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CompareAndSet not implemented")
}

// UnsafeDKVServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to DKVServer will
// result in compilation errors.
type UnsafeDKVServer interface {
	mustEmbedUnimplementedDKVServer()
}

func RegisterDKVServer(s grpc.ServiceRegistrar, srv DKVServer) {
	s.RegisterService(&DKV_ServiceDesc, srv)
}

func _DKV_Put_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PutRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DKVServer).Put(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/dkv.serverpb.DKV/Put",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DKVServer).Put(ctx, req.(*PutRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _DKV_Delete_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DeleteRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DKVServer).Delete(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/dkv.serverpb.DKV/Delete",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DKVServer).Delete(ctx, req.(*DeleteRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _DKV_Get_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DKVServer).Get(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/dkv.serverpb.DKV/Get",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DKVServer).Get(ctx, req.(*GetRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _DKV_MultiGet_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(MultiGetRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DKVServer).MultiGet(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/dkv.serverpb.DKV/MultiGet",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DKVServer).MultiGet(ctx, req.(*MultiGetRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _DKV_MultiPut_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(MultiPutRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DKVServer).MultiPut(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/dkv.serverpb.DKV/MultiPut",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DKVServer).MultiPut(ctx, req.(*MultiPutRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _DKV_Iterate_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(IterateRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(DKVServer).Iterate(m, &dKVIterateServer{stream})
}

type DKV_IterateServer interface {
	Send(*IterateResponse) error
	grpc.ServerStream
}

type dKVIterateServer struct {
	grpc.ServerStream
}

func (x *dKVIterateServer) Send(m *IterateResponse) error {
	return x.ServerStream.SendMsg(m)
}

func _DKV_CompareAndSet_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CompareAndSetRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DKVServer).CompareAndSet(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/dkv.serverpb.DKV/CompareAndSet",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DKVServer).CompareAndSet(ctx, req.(*CompareAndSetRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// DKV_ServiceDesc is the grpc.ServiceDesc for DKV service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var DKV_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "dkv.serverpb.DKV",
	HandlerType: (*DKVServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Put",
			Handler:    _DKV_Put_Handler,
		},
		{
			MethodName: "Delete",
			Handler:    _DKV_Delete_Handler,
		},
		{
			MethodName: "Get",
			Handler:    _DKV_Get_Handler,
		},
		{
			MethodName: "MultiGet",
			Handler:    _DKV_MultiGet_Handler,
		},
		{
			MethodName: "MultiPut",
			Handler:    _DKV_MultiPut_Handler,
		},
		{
			MethodName: "CompareAndSet",
			Handler:    _DKV_CompareAndSet_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Iterate",
			Handler:       _DKV_Iterate_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "pkg/serverpb/api.proto",
}
