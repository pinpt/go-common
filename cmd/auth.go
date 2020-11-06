package cmd

import (
	"os"

	"github.com/pinpt/go-common/v10/auth"
	"github.com/spf13/cobra"
)

var decryptStringCmd = &cobra.Command{
	Use:   "decrypt <string> <password>",
	Short: "decrypt a string",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		slug := args[0]
		password := args[1]
		result, err := auth.DecryptString(slug, password)
		if err != nil {
			os.Stderr.WriteString(err.Error())
			os.Exit(1)
		}
		os.Stdout.WriteString(result)
	},
}

var encryptStringCmd = &cobra.Command{
	Use:   "encrypt <string> <password>",
	Short: "encrypt a string",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		slug := args[0]
		password := args[1]
		result, err := auth.EncryptString(slug, password)
		if err != nil {
			os.Stderr.WriteString(err.Error())
			os.Exit(1)
		}
		os.Stdout.WriteString(result)
	},
}

func init() {
	rootCmd.AddCommand(decryptStringCmd)
	rootCmd.AddCommand(encryptStringCmd)
}
