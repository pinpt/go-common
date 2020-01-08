package cmd

import (
	"os"

	"github.com/pinpt/go-common/hash"
	"github.com/spf13/cobra"
)

var hashCmd = &cobra.Command{
	Use:   "hash",
	Short: "hash one or more values",
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		values := make([]interface{}, 0)
		for _, v := range args {
			values = append(values, v)
		}
		os.Stdout.WriteString(hash.Values(values...))
	},
}

func init() {
	rootCmd.AddCommand(hashCmd)
}
