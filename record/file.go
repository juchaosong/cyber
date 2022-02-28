package record

import (
	"fmt"
	"io"
	"os"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"go.buf.build/protocolbuffers/go/juchaosong/apollo/cyber/proto"
	protobuf "google.golang.org/protobuf/proto"
)

func NewFile(path string) (*File, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0666)
	if err != nil {
		return nil, errors.Wrapf(err, "opening file %v for reading record file", path)
	}

	file := &File{underlying: f}
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
		return errors.Wrapf(err, "seeking to index position")
	}

	section, err := f.readSection()
	if err != nil {
		return errors.Wrapf(err, "reading index from record file")
	}

	if section.Type != proto.SectionType_SECTION_INDEX {
		panic(fmt.Sprintf("invalid record file: missing index section, got %v", proto.SectionType_name[int32(section.Type)]))
	}

	if err := f.readProtoMessage(&f.Index, section.Size); err != nil {
		return errors.Wrapf(err, "unmarsharing index from record file")
	}

	if err := f.reset(); err != nil {
		log.Errorf("Failed to reset record file: %v", err)
	}
	return nil
}

func (f *File) Close() error {
	return f.underlying.Close()
}

func (f *File) reset() error {
	if _, err := f.underlying.Seek(int64(SectionLength+HeaderLength), 0); err != nil {
		return errors.Wrapf(err, "seeking to (SectionLength+HeaderLength) offset")
	}
	return nil
}

func (f *File) readHeader() error {
	var err error
	section, err := f.readSection()
	if err != nil {
		return errors.Wrapf(err, "reading header from record file")
	}

	if section.Type != proto.SectionType_SECTION_HEADER {
		panic("invalid record file: missing header section")
	}

	if err := f.readProtoMessage(&f.Header, section.Size); err != nil {
		return errors.Wrapf(err, "unmarshaling header from record file")
	}

	if _, err := f.underlying.Seek(SectionLength+HeaderLength, 0); err != nil {
		return errors.Wrapf(err, "seeking to next section")
	}
	return nil
}

func (f *File) readSection() (*Section, error) {
	data, err := f.readFull(SectionLength)
	if err != nil {
		return nil, errors.Wrapf(err, "reading section from record file")
	}

	return NewSection(data), nil
}

func (f *File) readProtoMessage(m protobuf.Message, size int64) error {
	data, err := f.readFull(size)
	if err != nil {
		return errors.Wrapf(err, "unmarsharing protobuf message")
	}

	if err := protobuf.Unmarshal(data, m); err != nil {
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
