package cmd

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"

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
		include, _ := cmd.Flags().GetString("include")
		message, _ := cmd.Flags().GetString("message")
		var matchKey, matchValue string
		if include != "" {
			tok := strings.Split(include, "=")
			matchKey, matchValue = tok[0], tok[1]
		}
		var messageRx *regexp.Regexp
		if message != "" {
			messageRx = regexp.MustCompile(message)
		}
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
			if _, ok := kv["ts"]; !ok {
				if ts, ok := kv["timestamp"]; ok {
					kv["ts"] = ts
					delete(kv, "timestamp")
				} else if ts, ok := kv["@timestamp"]; ok {
					kv["ts"] = ts
				}
			}
			delete(kv, "@timestamp")
			if _, ok := kv["level"]; !ok {
				if lvl, ok := kv["@level"]; ok {
					kv["level"] = lvl
					delete(kv, "@level")
				}
			}
			if _, ok := kv["message"]; !ok {
				if msg, ok := kv["@message"]; ok {
					kv["message"] = msg
					delete(kv, "@message")
				}
			}
			if val, ok := kv["comp"]; ok {
				if _, ok := kv["pkg"]; !ok {
					kv["pkg"] = val
					delete(kv, "comp")
				}
			}
			if matchKey != "" {
				if val, ok := kv[matchKey]; !ok || matchValue != val {
					continue
				}
			}
			msg, ok := kv["message"].(string)
			if !ok {
				log.Error(logger, fmt.Sprintf("message had no message key: %s", string(buf)))
				continue
			}
			if messageRx != nil {
				if !messageRx.MatchString(msg) {
					continue
				}
			}
			var vals []interface{}
			for k, v := range kv {
				vals = append(vals, k, v)
			}
			// fmt.Println(kv)
			switch kv["level"].(string) {
			case "debug":
				log.Debug(logger, msg, vals...)
			case "warn":
				log.Warn(logger, msg, vals...)
			case "info":
				log.Info(logger, msg, vals...)
			case "error":
				log.Error(logger, msg, vals...)
			case "fatal":
				log.Fatal(logger, msg, vals...)
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(logCmd)
	log.RegisterFlags(rootCmd)
	logCmd.Flags().StringP("include", "i", "", "filter by key=value")
	logCmd.Flags().StringP("message", "m", "", "filter by a specific message regular expression")
}
