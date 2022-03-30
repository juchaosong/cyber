package record

import (
	"fmt"
	"io"
	"os"

	"github.com/juchaosong/cyber/internal/desc"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	cyberproto "go.buf.build/protocolbuffers/go/juchaosong/apollo/cyber/proto"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

func NewFile(path string) (*File, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0666)
	if err != nil {
		return nil, errors.Wrapf(err, "open file %v for reading record file", path)
	}

	file := &File{chan2Cache: make(map[string]*cyberproto.ChannelCache), underlying: f}
	if err := file.readHeader(); err != nil {
		return nil, errors.Wrapf(err, "malformed record header")
	}

	return file, nil
}

func (f *File) Name() string {
	return f.underlying.Name()
}

func (f *File) Version() string {
	return fmt.Sprintf("%d.%d", f.Header.GetMajorVersion(), f.Header.GetMinorVersion())
}

func (f *File) ReadIndex() error {
	if !f.Header.GetIsComplete() {
		return errors.New("incomplete record file")
	}

	if _, err := f.underlying.Seek(int64(f.Header.GetIndexPosition()), 0); err != nil {
		return errors.Wrapf(err, "seek to index position")
	}

	section, err := f.readSection()
	if err != nil {
		return errors.Wrapf(err, "read section")
	}

	if section.Type != cyberproto.SectionType_SECTION_INDEX {
		panic(fmt.Sprintf("invalid record file: missing index section, got %v", cyberproto.SectionType_name[int32(section.Type)]))
	}

	if err := f.readProtoMessage(&f.Index, section.Size); err != nil {
		return errors.Wrapf(err, "unmarshar index")
	}

	// Cyber channel to cache
	for _, idx := range f.Index.GetIndexes() {
		if idx.GetType() == *cyberproto.SectionType_SECTION_CHANNEL.Enum() {
			cache := idx.GetChannelCache()
			f.chan2Cache[cache.GetName()] = cache
		}
	}
	f.msgFactory, err = f.parseMessageFactory()
	if err != nil {
		log.Error("Failed to parse descriptor source")
	}

	if err := f.reset(); err != nil {
		log.Errorf("Failed to reset record file: %v", err)
	}
	return nil
}

func (f *File) ReadChunk(channel string) ([]protoreflect.ProtoMessage, error) {
	ret := make([]protoreflect.ProtoMessage, 0)
	for _, idx := range f.Index.GetIndexes() {
		if idx.GetType() == *cyberproto.SectionType_SECTION_CHUNK_BODY.Enum() {
			if _, err := f.underlying.Seek(int64(idx.GetPosition()), 0); err != nil {
				return nil, errors.Wrapf(err, "seek to chunk body position")
			}

			section, err := f.readSection()
			if err != nil {
				return nil, errors.Wrapf(err, "read chunk body")
			}

			if section.Type != cyberproto.SectionType_SECTION_CHUNK_BODY {
				panic(fmt.Sprintf("invalid record file: missing chunk body section, got %v", cyberproto.SectionType_name[int32(section.Type)]))
			}

			var chunk cyberproto.ChunkBody
			if err := f.readProtoMessage(&chunk, section.Size); err != nil {
				return nil, errors.Wrapf(err, "unmarshal chunk body")
			}

			for _, msgBytes := range chunk.GetMessages() {
				if msgBytes.GetChannelName() != channel {
					continue
				}
				
				cache, ok := f.chan2Cache[msgBytes.GetChannelName()]
				if !ok {
					log.Errorf("Failed to find message type for channel %v", msgBytes.GetChannelName())
					continue
				}

				fqn := cache.GetMessageType()
				msg, err := f.msgFactory.NewMessage(fqn)
				if err != nil {
					return nil, errors.Wrapf(err, "create message %v", fqn)
				}

				if err := proto.Unmarshal(msgBytes.GetContent(), msg); err != nil {
					return nil, errors.Wrapf(err, "unmarshal message %v", fqn)
				}
				ret = append(ret, msg)
			}
		}
	}
	return ret, nil
}

func (f *File) Close() error {
	return f.underlying.Close()
}

func (f *File) reset() error {
	if _, err := f.underlying.Seek(int64(SectionLength+HeaderLength), 0); err != nil {
		return errors.Wrapf(err, "seek to (SectionLength+HeaderLength) offset")
	}
	return nil
}

func (f *File) readHeader() error {
	var err error
	section, err := f.readSection()
	if err != nil {
		return errors.Wrapf(err, "read header")
	}

	if section.Type != cyberproto.SectionType_SECTION_HEADER {
		panic("invalid record file: missing header section")
	}

	if err := f.readProtoMessage(&f.Header, section.Size); err != nil {
		return errors.Wrapf(err, "unmarshal header")
	}

	if _, err := f.underlying.Seek(SectionLength+HeaderLength, 0); err != nil {
		return errors.Wrapf(err, "seek to next section")
	}
	return nil
}

func (f *File) readSection() (*Section, error) {
	data, err := f.readFull(SectionLength)
	if err != nil {
		return nil, errors.Wrapf(err, "read section")
	}

	return NewSection(data), nil
}

func (f *File) readProtoMessage(m proto.Message, size int64) error {
	data, err := f.readFull(size)
	if err != nil {
		return errors.Wrapf(err, "unmarshar protobuf message")
	}

	if err := proto.Unmarshal(data, m); err != nil {
		return err
	}
	return nil
}

func (f *File) readFull(size int64) ([]byte, error) {
	buf := make([]byte, size)
	if _, err := io.ReadFull(f.underlying, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func (f *File) parseMessageFactory() (*desc.MessageFactory, error) {
	allFileDescProtos := make([]*descriptorpb.FileDescriptorProto, 0)
	for _, cache := range f.chan2Cache {
		var pd cyberproto.ProtoDesc
		if err := proto.Unmarshal(cache.GetProtoDesc(), &pd); err != nil {
			return nil, errors.Wrapf(err, "umnarshal proto desc")
		}
		fileDescProtos, err := resolveFileDescProto(&pd)
		if err != nil {
			return nil, errors.Wrapf(err, "resolve file descriptor proto")
		}
		allFileDescProtos = append(allFileDescProtos, fileDescProtos...)
	}

	return desc.NewMessageFactory(allFileDescProtos)
}

func resolveFileDescProto(protoDesc *cyberproto.ProtoDesc) ([]*descriptorpb.FileDescriptorProto, error) {
	var (
		ret []*descriptorpb.FileDescriptorProto
		fdp descriptorpb.FileDescriptorProto
	)
	if err := proto.Unmarshal(protoDesc.GetDesc(), &fdp); err != nil {
		return nil, errors.Wrapf(err, "unmarshal file descriptor proto")
	}

	ret = append(ret, &fdp)
	for _, dep := range protoDesc.GetDependencies() {
		fdp, err := resolveFileDescProto(dep)
		if err != nil {
			return nil, err
		}
		ret = append(ret, fdp...)
	}
	return ret, nil
}
