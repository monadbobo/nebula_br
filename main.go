package main

import (
	"github.com/monadbobo/br/cmd"
	"github.com/spf13/cobra"
)

func main() {

	rootCmd := &cobra.Command{
		Use:   "br",
		Short: "BR is a Nebula backup and restore tool",
	}
	rootCmd.AddCommand(cmd.NewBackupCmd(), cmd.NewVersionCmd())
	rootCmd.Execute()
}
