package commands

import (
	"os"

	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/juchaosong/cyber/record"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func NewInspectCommand() *cobra.Command {
	var (
		channel string
	)
	command := &cobra.Command{
		Use:   "inspect",
		Short: "Inspect cyber record message",
		Run: func(cmd *cobra.Command, args []string) {
			marshal := jsonpb.Marshaler{Indent: "  "}
			for _, fileName := range args {
				f, err := record.NewFile(fileName)
				if err != nil {
					log.Fatalf("Failed to craete record file '%s': %v", fileName, err)
				}
				defer f.Close()

				if err := f.ReadIndex(); err != nil {
					log.Fatalf("Failed to read index from record file '%s': %v", fileName, err)
				}

				chunks, err := f.ReadChunk(channel)
				if err != nil {
					log.Fatalf("Failed to read chunk from record file '%s': %v", fileName, err)
				}

				for _, chunk := range chunks {
					marshal.Marshal(os.Stdout, proto.MessageV1(chunk))
				}
			}
		},
	}

	command.Flags().StringVar(&channel, "channel", "", "Message channel")
	return command
}
