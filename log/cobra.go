package log

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/fatih/color"
	isatty "github.com/mattn/go-isatty"
	pos "github.com/pinpt/go-common/os"
	"github.com/spf13/cobra"
)

var isContainer bool

func init() {
	isContainer = pos.IsInsideContainer()
}

// RegisterFlags will register the flags for logging
func RegisterFlags(rootCmd *cobra.Command) {
	timestamps := isContainer || !isatty.IsTerminal(os.Stdout.Fd())
	rootCmd.PersistentFlags().StringP("log-level", "info", pos.Getenv("PP_LOG_LEVEL", "info"), "set the log level")
	rootCmd.PersistentFlags().String("log-color", "dark", "set the log color profile (dark or light). only applies to console logging")
	rootCmd.PersistentFlags().String("log-format", "default", "set the log format (json, logfmt, default)")
	rootCmd.PersistentFlags().String("log-output", "-", "the location of the log file, use - for default or specify a location")
	rootCmd.PersistentFlags().Bool("log-timestamp", timestamps, "turn on timestamps in output")
	rootCmd.PersistentFlags().String("log-timestamp-format", "", "timestamp formatting")
	rootCmd.PersistentFlags().Int("log-maxsize-value", MaxStringValueLength, "the max size of a value when logged")
}

// NewCommandLogger returns a new Logger for a given command
func NewCommandLogger(cmd *cobra.Command, opts ...WithLogOptions) LoggerCloser {
	max, _ := cmd.Flags().GetInt("log-maxsize-value")
	if max > 0 && max != MaxStringValueLength {
		MaxStringValueLength = max // allow it to be overriden at the global level. note that if multiple loggers are used one will overwrite the other
	}
	pkg := cmd.Name()
	if opts == nil {
		opts = make([]WithLogOptions, 0)
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
			if o != "" {
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
			} else {
				writer = os.Stdout
			}
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

	timestamps, _ := cmd.Flags().GetBool("log-timestamp")

	if timestamps || isfile {
		// if inside docker or in a file or not connected to tty, we want timestamp
		layout, _ := cmd.Flags().GetString("log-timestamp-format")
		if layout == "" {
			// if console, it reads better with a shorter and friendlier format
			if logFormat == ConsoleLogFormat {
				layout = time.StampMilli
			} else {
				layout = time.RFC3339Nano
			}
		}
		opts = append(opts, WithDefaultTimestampLogOption(layout))
	}

	return NewLogger(writer, logFormat, logColorTheme, minLogLevel, pkg, opts...)
}
