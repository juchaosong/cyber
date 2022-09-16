package commands

import "github.com/spf13/cobra"

const (
	CLIName = "cyber"
)

func NewCommand() *cobra.Command {
	command := &cobra.Command{
		Use: CLIName,
		Short: "cyber is a tool for manipulating cyber records",
		Run: func(cmd *cobra.Command, args []string) {
			cmd.HelpFunc()(cmd, args)
		},
	}

	command.AddCommand(NewInfoCommand())
	command.AddCommand(NewInspectCommand())

	return command
}
