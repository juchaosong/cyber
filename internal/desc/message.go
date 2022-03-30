package desc

import (
	"github.com/fullstorydev/grpcurl"
	"github.com/golang/protobuf/proto"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

type MessageFactory struct {
	descSource grpcurl.DescriptorSource
	msgFactory dynamic.MessageFactory
}

func NewMessageFactory(files []*descriptorpb.FileDescriptorProto) (*MessageFactory, error) {
	descSource, err := grpcurl.DescriptorSourceFromFileDescriptorSet(&descriptorpb.FileDescriptorSet{File: files})
	if err != nil {
		return nil, errors.Wrapf(err, "create descriptor source")
	}
	return &MessageFactory{
		descSource: descSource,
		msgFactory: *dynamic.NewMessageFactoryWithDefaults(),
	}, nil
}

func (f *MessageFactory) NewMessage(fqn string) (protoreflect.ProtoMessage, error) {
	descriptor, err := f.descSource.FindSymbol(fqn)
	if err != nil {
		return nil, errors.Wrapf(err, "find symbol %v", fqn)
	}

	msgDesc := descriptor.(*desc.MessageDescriptor)
	msg := f.msgFactory.NewMessage(msgDesc)
	return proto.MessageV2(msg), nil
}
