package record

import (
	"encoding/binary"

	"go.buf.build/protocolbuffers/go/juchaosong/apollo/cyber/proto"
)

func NewSection(data []byte) *Section {
	return &Section{
		Type: proto.SectionType(binary.LittleEndian.Uint64(data[:SectionTypeLength])),
		Size: int64(binary.LittleEndian.Uint64((data[SectionTypeLength:SectionLength]))),
	}
}
