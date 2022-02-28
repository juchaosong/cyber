package record

import (
	"testing"
)

func TestNewFile(t *testing.T) {
	// Download from https://github.com/ApolloAuto/apollo/releases/download/v3.5.0/demo_3.5.record
	// TODO(juchao): download and cache the test record file from apollo releases
	_, err := NewFile("testdata/demo_3.5.record")
	if err != nil {
		t.Fatalf("Cannot open record file: %v", err)
	}
}
