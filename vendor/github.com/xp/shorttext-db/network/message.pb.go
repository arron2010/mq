// Code generated by protoc-gen-go. DO NOT EDIT.
// source: message.proto

package network

import (
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

type Message struct {
	Type                 uint32   `protobuf:"varint,1,opt,name=Type,proto3" json:"Type,omitempty"`
	To                   uint64   `protobuf:"varint,2,opt,name=To,proto3" json:"To,omitempty"`
	From                 uint64   `protobuf:"varint,3,opt,name=From,proto3" json:"From,omitempty"`
	Term                 uint64   `protobuf:"varint,4,opt,name=Term,proto3" json:"Term,omitempty"`
	Index                uint64   `protobuf:"varint,5,opt,name=Index,proto3" json:"Index,omitempty"`
	Count                uint32   `protobuf:"varint,6,opt,name=Count,proto3" json:"Count,omitempty"`
	Data                 []byte   `protobuf:"bytes,7,opt,name=Data,proto3" json:"Data,omitempty"`
	Text                 string   `protobuf:"bytes,8,opt,name=Text,proto3" json:"Text,omitempty"`
	ResultCode           uint32   `protobuf:"varint,9,opt,name=ResultCode,proto3" json:"ResultCode,omitempty"`
	Key                  string   `protobuf:"bytes,10,opt,name=Key,proto3" json:"Key,omitempty"`
	DBName               string   `protobuf:"bytes,11,opt,name=DBName,proto3" json:"DBName,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Message) Reset()         { *m = Message{} }
func (m *Message) String() string { return proto.CompactTextString(m) }
func (*Message) ProtoMessage()    {}
func (*Message) Descriptor() ([]byte, []int) {
	return fileDescriptor_33c57e4bae7b9afd, []int{0}
}

func (m *Message) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Message.Unmarshal(m, b)
}
func (m *Message) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Message.Marshal(b, m, deterministic)
}
func (m *Message) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Message.Merge(m, src)
}
func (m *Message) XXX_Size() int {
	return xxx_messageInfo_Message.Size(m)
}
func (m *Message) XXX_DiscardUnknown() {
	xxx_messageInfo_Message.DiscardUnknown(m)
}

var xxx_messageInfo_Message proto.InternalMessageInfo

func (m *Message) GetType() uint32 {
	if m != nil {
		return m.Type
	}
	return 0
}

func (m *Message) GetTo() uint64 {
	if m != nil {
		return m.To
	}
	return 0
}

func (m *Message) GetFrom() uint64 {
	if m != nil {
		return m.From
	}
	return 0
}

func (m *Message) GetTerm() uint64 {
	if m != nil {
		return m.Term
	}
	return 0
}

func (m *Message) GetIndex() uint64 {
	if m != nil {
		return m.Index
	}
	return 0
}

func (m *Message) GetCount() uint32 {
	if m != nil {
		return m.Count
	}
	return 0
}

func (m *Message) GetData() []byte {
	if m != nil {
		return m.Data
	}
	return nil
}

func (m *Message) GetText() string {
	if m != nil {
		return m.Text
	}
	return ""
}

func (m *Message) GetResultCode() uint32 {
	if m != nil {
		return m.ResultCode
	}
	return 0
}

func (m *Message) GetKey() string {
	if m != nil {
		return m.Key
	}
	return ""
}

func (m *Message) GetDBName() string {
	if m != nil {
		return m.DBName
	}
	return ""
}

type BatchMessage struct {
	Term                 uint64     `protobuf:"varint,1,opt,name=term,proto3" json:"term,omitempty"`
	Messages             []*Message `protobuf:"bytes,2,rep,name=Messages,proto3" json:"Messages,omitempty"`
	XXX_NoUnkeyedLiteral struct{}   `json:"-"`
	XXX_unrecognized     []byte     `json:"-"`
	XXX_sizecache        int32      `json:"-"`
}

func (m *BatchMessage) Reset()         { *m = BatchMessage{} }
func (m *BatchMessage) String() string { return proto.CompactTextString(m) }
func (*BatchMessage) ProtoMessage()    {}
func (*BatchMessage) Descriptor() ([]byte, []int) {
	return fileDescriptor_33c57e4bae7b9afd, []int{1}
}

func (m *BatchMessage) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_BatchMessage.Unmarshal(m, b)
}
func (m *BatchMessage) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_BatchMessage.Marshal(b, m, deterministic)
}
func (m *BatchMessage) XXX_Merge(src proto.Message) {
	xxx_messageInfo_BatchMessage.Merge(m, src)
}
func (m *BatchMessage) XXX_Size() int {
	return xxx_messageInfo_BatchMessage.Size(m)
}
func (m *BatchMessage) XXX_DiscardUnknown() {
	xxx_messageInfo_BatchMessage.DiscardUnknown(m)
}

var xxx_messageInfo_BatchMessage proto.InternalMessageInfo

func (m *BatchMessage) GetTerm() uint64 {
	if m != nil {
		return m.Term
	}
	return 0
}

func (m *BatchMessage) GetMessages() []*Message {
	if m != nil {
		return m.Messages
	}
	return nil
}

func init() {
	proto.RegisterType((*Message)(nil), "network.Message")
	proto.RegisterType((*BatchMessage)(nil), "network.BatchMessage")
}

func init() { proto.RegisterFile("message.proto", fileDescriptor_33c57e4bae7b9afd) }

var fileDescriptor_33c57e4bae7b9afd = []byte{
	// 253 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x4c, 0x90, 0xc1, 0x4a, 0xf3, 0x40,
	0x10, 0xc7, 0xd9, 0x24, 0x4d, 0xda, 0x69, 0xfb, 0x51, 0x86, 0x0f, 0x99, 0x93, 0x2c, 0x3d, 0xed,
	0x41, 0x72, 0xd0, 0x37, 0x68, 0x8b, 0x20, 0xa2, 0xc8, 0x92, 0x17, 0x58, 0xed, 0xa0, 0xa0, 0xc9,
	0x96, 0x64, 0x8b, 0xed, 0x73, 0xfb, 0x02, 0xb2, 0xb3, 0x41, 0x7a, 0xfb, 0xcd, 0x6f, 0x76, 0x66,
	0xf9, 0x0f, 0x2c, 0x5b, 0x1e, 0x06, 0xf7, 0xce, 0xf5, 0xa1, 0xf7, 0xc1, 0x63, 0xd5, 0x71, 0xf8,
	0xf6, 0xfd, 0xe7, 0xfa, 0x47, 0x41, 0xf5, 0x94, 0x5a, 0x88, 0x50, 0x34, 0xe7, 0x03, 0x93, 0xd2,
	0xca, 0x2c, 0xad, 0x30, 0xfe, 0x83, 0xac, 0xf1, 0x94, 0x69, 0x65, 0x0a, 0x9b, 0x35, 0x3e, 0xbe,
	0xb9, 0xef, 0x7d, 0x4b, 0xb9, 0x18, 0x61, 0x99, 0xe3, 0xbe, 0xa5, 0x22, 0xb9, 0xc8, 0xf8, 0x1f,
	0x26, 0x0f, 0xdd, 0x9e, 0x4f, 0x34, 0x11, 0x99, 0x8a, 0x68, 0xb7, 0xfe, 0xd8, 0x05, 0x2a, 0xe5,
	0x8b, 0x54, 0xc4, 0xf9, 0x9d, 0x0b, 0x8e, 0x2a, 0xad, 0xcc, 0xc2, 0x0a, 0xa7, 0x9d, 0xa7, 0x40,
	0x53, 0xad, 0xcc, 0xcc, 0x0a, 0xe3, 0x35, 0x80, 0xe5, 0xe1, 0xf8, 0x15, 0xb6, 0x7e, 0xcf, 0x34,
	0x93, 0x15, 0x17, 0x06, 0x57, 0x90, 0x3f, 0xf2, 0x99, 0x40, 0x46, 0x22, 0xe2, 0x15, 0x94, 0xbb,
	0xcd, 0xb3, 0x6b, 0x99, 0xe6, 0x22, 0xc7, 0x6a, 0xfd, 0x02, 0x8b, 0x8d, 0x0b, 0x6f, 0x1f, 0x17,
	0xc9, 0x43, 0x4c, 0xa0, 0x52, 0x82, 0xc8, 0x78, 0x03, 0xd3, 0xb1, 0x3d, 0x50, 0xa6, 0x73, 0x33,
	0xbf, 0x5d, 0xd5, 0xe3, 0xd5, 0xea, 0xb1, 0x61, 0xff, 0x5e, 0xbc, 0x96, 0x72, 0xd7, 0xbb, 0xdf,
	0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0x25, 0x2b, 0x04, 0x68, 0x01, 0x00, 0x00,
}