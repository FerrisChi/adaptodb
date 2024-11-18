// router.proto

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.35.2
// 	protoc        v3.20.3
// source: router.proto

package proto

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type GetShardRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key string `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
}

func (x *GetShardRequest) Reset() {
	*x = GetShardRequest{}
	mi := &file_router_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetShardRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetShardRequest) ProtoMessage() {}

func (x *GetShardRequest) ProtoReflect() protoreflect.Message {
	mi := &file_router_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetShardRequest.ProtoReflect.Descriptor instead.
func (*GetShardRequest) Descriptor() ([]byte, []int) {
	return file_router_proto_rawDescGZIP(), []int{0}
}

func (x *GetShardRequest) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

type GetShardResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ShardId uint64 `protobuf:"varint,1,opt,name=shard_id,json=shardId,proto3" json:"shard_id,omitempty"`
}

func (x *GetShardResponse) Reset() {
	*x = GetShardResponse{}
	mi := &file_router_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetShardResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetShardResponse) ProtoMessage() {}

func (x *GetShardResponse) ProtoReflect() protoreflect.Message {
	mi := &file_router_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetShardResponse.ProtoReflect.Descriptor instead.
func (*GetShardResponse) Descriptor() ([]byte, []int) {
	return file_router_proto_rawDescGZIP(), []int{1}
}

func (x *GetShardResponse) GetShardId() uint64 {
	if x != nil {
		return x.ShardId
	}
	return 0
}

var File_router_proto protoreflect.FileDescriptor

var file_router_proto_rawDesc = []byte{
	0x0a, 0x0c, 0x72, 0x6f, 0x75, 0x74, 0x65, 0x72, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x06,
	0x72, 0x6f, 0x75, 0x74, 0x65, 0x72, 0x22, 0x23, 0x0a, 0x0f, 0x47, 0x65, 0x74, 0x53, 0x68, 0x61,
	0x72, 0x64, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x22, 0x2d, 0x0a, 0x10, 0x47,
	0x65, 0x74, 0x53, 0x68, 0x61, 0x72, 0x64, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12,
	0x19, 0x0a, 0x08, 0x73, 0x68, 0x61, 0x72, 0x64, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x04, 0x52, 0x07, 0x73, 0x68, 0x61, 0x72, 0x64, 0x49, 0x64, 0x32, 0x4e, 0x0a, 0x0b, 0x53, 0x68,
	0x61, 0x72, 0x64, 0x52, 0x6f, 0x75, 0x74, 0x65, 0x72, 0x12, 0x3f, 0x0a, 0x08, 0x47, 0x65, 0x74,
	0x53, 0x68, 0x61, 0x72, 0x64, 0x12, 0x17, 0x2e, 0x72, 0x6f, 0x75, 0x74, 0x65, 0x72, 0x2e, 0x47,
	0x65, 0x74, 0x53, 0x68, 0x61, 0x72, 0x64, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x18,
	0x2e, 0x72, 0x6f, 0x75, 0x74, 0x65, 0x72, 0x2e, 0x47, 0x65, 0x74, 0x53, 0x68, 0x61, 0x72, 0x64,
	0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x42, 0x1b, 0x5a, 0x19, 0x61, 0x64,
	0x61, 0x70, 0x74, 0x6f, 0x64, 0x62, 0x2f, 0x70, 0x6b, 0x67, 0x2f, 0x72, 0x6f, 0x75, 0x74, 0x65,
	0x72, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_router_proto_rawDescOnce sync.Once
	file_router_proto_rawDescData = file_router_proto_rawDesc
)

func file_router_proto_rawDescGZIP() []byte {
	file_router_proto_rawDescOnce.Do(func() {
		file_router_proto_rawDescData = protoimpl.X.CompressGZIP(file_router_proto_rawDescData)
	})
	return file_router_proto_rawDescData
}

var file_router_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_router_proto_goTypes = []any{
	(*GetShardRequest)(nil),  // 0: router.GetShardRequest
	(*GetShardResponse)(nil), // 1: router.GetShardResponse
}
var file_router_proto_depIdxs = []int32{
	0, // 0: router.ShardRouter.GetShard:input_type -> router.GetShardRequest
	1, // 1: router.ShardRouter.GetShard:output_type -> router.GetShardResponse
	1, // [1:2] is the sub-list for method output_type
	0, // [0:1] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_router_proto_init() }
func file_router_proto_init() {
	if File_router_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_router_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   2,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_router_proto_goTypes,
		DependencyIndexes: file_router_proto_depIdxs,
		MessageInfos:      file_router_proto_msgTypes,
	}.Build()
	File_router_proto = out.File
	file_router_proto_rawDesc = nil
	file_router_proto_goTypes = nil
	file_router_proto_depIdxs = nil
}