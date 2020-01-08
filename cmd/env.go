package cmd

import (
	"io/ioutil"
	"os"
	"strings"

	ps "github.com/pinpt/go-common/strings"
	"github.com/spf13/cobra"
)

var envCmd = &cobra.Command{
	Use:   "env",
	Short: "pipe input through environment replacement and output",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		buf, err := ioutil.ReadAll(os.Stdin)
		if err != nil {
			panic(err)
		}
		env := make(map[string]interface{})
		for _, kv := range os.Environ() {
			tok := strings.Split(kv, "=")
			env[tok[0]] = tok[1]
		}
		res, err := ps.InterpolateString(string(buf), env)
		if err != nil {
			panic(err)
		}
		os.Stdout.WriteString(res)
	},
}

func init() {
	rootCmd.AddCommand(envCmd)
}
