package main

import (
	"fmt"
	"os"

	"github.com/juchaosong/cyber/cmd/commands"
)

func main() {
	if err := commands.NewCommand().Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}