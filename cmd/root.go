package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

const WORKER_LISTEN = ":22345"

var RootCmd = &cobra.Command{
	Use:   "ba-dbench",
	Short: "Distributed benchmark runner",
}

func Execute() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
