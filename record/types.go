package record

import (
	"os"

	"go.buf.build/protocolbuffers/go/juchaosong/apollo/cyber/proto"
)

type File struct {
	Header proto.Header
	Index  proto.Index

	underlying *os.File
}

type Section struct {
	Type proto.SectionType
	Size int64
}

const (
	SectionLength     = 16
	SectionTypeLength = 8

	HeaderLength = 2048
)
