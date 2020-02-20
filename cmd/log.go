package cmd

import (
	"bufio"
	"encoding/json"
	"io"
	"os"

	"github.com/pinpt/go-common/log"
	"github.com/spf13/cobra"
)

// logCmd
var logCmd = &cobra.Command{
	Use: "log",
	Run: func(cmd *cobra.Command, args []string) {
		logger := log.NewCommandLogger(cmd)
		defer logger.Close()
		lr := bufio.NewReader(os.Stdin)
		for {
			buf, err := lr.ReadSlice('\n')
			if err == io.EOF {
				break
			}
			var kv map[string]interface{}
			if err := json.Unmarshal(buf, &kv); err != nil {
				log.Info(logger, string(buf))
				continue
			}
			var vals []interface{}
			for k, v := range kv {
				vals = append(vals, k, v)
			}
			switch kv["level"].(string) {
			case "debug":
				log.Debug(logger, kv["message"].(string), vals...)
			case "warn":
				log.Warn(logger, kv["message"].(string), vals...)
			case "info":
				log.Info(logger, kv["message"].(string), vals...)
			case "error":
				log.Error(logger, kv["message"].(string), vals...)
			case "fatal":
				log.Fatal(logger, kv["message"].(string), vals...)
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(logCmd)
	log.RegisterFlags(rootCmd)
}
