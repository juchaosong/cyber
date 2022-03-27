package record

import (
	"os"

	"go.buf.build/protocolbuffers/go/juchaosong/apollo/cyber/proto"
)

// ┌───────────────────┬───────────────┬─────┬──────────────────┬─────┬───┐
// │section_header(16B)│ header(2048B) │ ... │section_index(16B)│index│...│
// └───────────────────┴───────────────┴─────┴──────────────────┴─────┴───┘
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
