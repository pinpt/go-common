package log

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"runtime"
	"strings"

	"github.com/fatih/color"
	"github.com/pinpt/go-common/fileutil"
	"github.com/spf13/cobra"
)

const dockerCGroup = "/proc/self/cgroup"
const k8sServiceAcct = "/var/run/secrets/kubernetes.io/serviceaccount"

// RegisterFlags will register the flags for logging
func RegisterFlags(rootCmd *cobra.Command) {
	rootCmd.PersistentFlags().String("log-level", "info", "set the log level")
	rootCmd.PersistentFlags().String("log-color", "dark", "set the log color profile (dark or light). only applies to console logging")
	rootCmd.PersistentFlags().String("log-format", "default", "set the log format (json, logfmt, default)")
	rootCmd.PersistentFlags().String("log-output", "-", "the location of the log file, use - for default or specify a location")
}

// NewCommandLogger returns a new Logger for a given command
func NewCommandLogger(cmd *cobra.Command, opts ...WithLogOptions) Logger {
	pkg := cmd.Name()
	if opts == nil {
		opts = make([]WithLogOptions, 0)
	}
	var isContainer bool
	if runtime.GOOS == "linux" {
		if fileutil.FileExists(dockerCGroup) {
			buf, err := ioutil.ReadFile(dockerCGroup)
			if err != nil && bytes.Contains(buf, []byte("docker")) {
				isContainer = true
			}
		} else if fileutil.FileExists(k8sServiceAcct) {
			isContainer = true
		}
	}

	var writer io.Writer
	var isfile bool
	o, _ := cmd.Flags().GetString("log-output")
	switch o {
	case "-":
		{
			writer = os.Stdout
			if isContainer {
				// for docker, we want to log to /dev/stderr
				writer = os.Stderr
			}
		}
	case "/dev/stdout", "stdout":
		{
			writer = os.Stdout
		}
	case "/dev/stderr", "stderr":
		{
			writer = os.Stderr
		}
	case "/dev/null":
		{
			writer = ioutil.Discard
		}
	default:
		{
			// write to a file
			f, err := os.Create(o)
			if err != nil {
				fmt.Printf("Cannot open %s. %v\n", o, err)
				os.Exit(1)
			}
			w := os.Stdout
			if isContainer {
				w = os.Stderr
			}
			// write to both the normal output as well as the file
			writer = io.MultiWriter(f, w)
			isfile = true
		}
	}
	var logFormat OutputFormat
	lf, _ := cmd.Flags().GetString("log-format")
	switch lf {
	case "json":
		{
			logFormat = JSONLogFormat
		}
	case "logfmt":
		{
			logFormat = LogFmtLogFormat
		}
	default:
		{
			if isfile {
				logFormat = LogFmtLogFormat
			} else {
				logFormat = ConsoleLogFormat
			}
		}
	}

	var logColorTheme ColorTheme
	lc, _ := cmd.Flags().GetString("log-color")
	switch lc {
	case "light":
		{
			logColorTheme = LightLogColorTheme
		}
	case "none":
		{
			logColorTheme = NoColorTheme
			color.NoColor = true
		}
	default:
		{
			if color.NoColor {
				logColorTheme = NoColorTheme
			} else {
				logColorTheme = DarkLogColorTheme
			}
		}
	}

	var minLogLevel Level
	lvl, _ := cmd.Flags().GetString("log-level")
	switch strings.ToLower(lvl) {
	case "debug":
		{
			minLogLevel = DebugLevel
		}
	case "info":
		{
			minLogLevel = InfoLevel
		}
	case "error":
		{
			minLogLevel = ErrorLevel
		}
	case "warn", "warning":
		{
			minLogLevel = WarnLevel
		}
	case "none":
		{
			minLogLevel = NoneLevel
		}
	default:
		{
			minLogLevel = InfoLevel
		}
	}

	// if discard writer, optimize the return
	if writer == ioutil.Discard {
		minLogLevel = NoneLevel
	}

	if isContainer || isfile {
		// if inside docker or in a file, we want timestamp
		opts = append(opts, WithDefaultTimestampLogOption())
	}

	return NewLogger(writer, logFormat, logColorTheme, minLogLevel, pkg, opts...)
}
