package commands

import (
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/juchaosong/cyber/record"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"go.buf.build/protocolbuffers/go/juchaosong/apollo/cyber/proto"
)

func NewInfoCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "info",
		Short: "Show information about cyber",
		Run: func(cmd *cobra.Command, args []string) {
			for _, fileName := range args {
				f, err := record.NewFile(fileName)
				if err != nil {
					log.Fatalf("Failed to craete record file '%s': %v", fileName, err)
				}
				defer f.Close()

				if err := f.ReadIndex(); err != nil {
					log.Fatalf("Failed to read index from record file '%s': %v", fileName, err)
				}
				printRecord(f)
			}
		},
	}

	return command
}

func printRecord(f *record.File) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(w, "File info:")
	fmt.Fprint(w, "FILE\t", f.Name(), "\n")
	fmt.Fprint(w, "VERSION\t", f.Version(), "\n")
	fmt.Fprint(w, "SIZE(B)\t", f.Header.GetSize(), "\n")
	fmt.Fprint(w, "START_TIME\t", toUnix(f.Header.GetBeginTime()), "\n")
	fmt.Fprint(w, "END_TIME\t", toUnix(f.Header.GetEndTime()), "\n")
	fmt.Fprint(w, "IS_COMPLETE\t", f.Header.GetIsComplete(), "\n")
	fmt.Fprint(w, "INDEX_POSITION\t", f.Header.GetIndexPosition(), "\n")
	fmt.Fprint(w, "MESSAGE_NUMBER\t", f.Header.GetMessageNumber(), "\n")
	fmt.Fprint(w, "CHANNEL_NUMBER\t", f.Header.GetChannelNumber(), "\n")

	fmt.Fprintln(w, "\nIndexes info:")
	fmt.Fprintf(w, "NAME\tMESSAGES\tPOSITION\n")
	for _, idx := range f.Index.GetIndexes() {
		if idx.GetType() == *proto.SectionType_SECTION_CHANNEL.Enum() {
			cache := idx.GetChannelCache()
			fmt.Fprintf(w, "%s\t%d\t%d\n", cache.GetName(), cache.GetMessageNumber(), idx.GetPosition())
		}
	}
	_ = w.Flush()
}

func toUnix(ts uint64) time.Time {
	return time.Unix(int64(ts/1e9), int64((ts%1e3)*1e3))
}
