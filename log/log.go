package log

import (
	"bytes"
	"encoding"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/fatih/color"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pinpt/go-common/fileutil"
	pjson "github.com/pinpt/go-common/json"
	pos "github.com/pinpt/go-common/os"
	pstring "github.com/pinpt/go-common/strings"
	"github.com/pinpt/go-common/term"
)

// Logger is the fundamental interface for all log operations. Log creates a
// log event from keyvals, a variadic sequence of alternating keys and values.
// Implementations must be safe for concurrent use by multiple goroutines. In
// particular, any implementation of Logger that appends to keyvals or
// modifies or retains any of its elements must make a copy first.
type Logger interface {
	Log(keyvals ...interface{}) error
}

// ErrMissingValue is appended to keyvals slices with odd length to substitute
// the missing value.
var ErrMissingValue = errors.New("(MISSING)")

// With returns a new contextual logger with keyvals prepended to those passed
// to calls to Log. If logger is also a contextual logger created by With or
// WithPrefix, keyvals is appended to the existing context.
//
// The returned Logger replaces all value elements (odd indexes) containing a
// Valuer with their generated value for each call to its Log method.
func With(logger Logger, keyvals ...interface{}) Logger {
	return log.With(logger, keyvals...)
}

// Info log helper
func Info(logger Logger, msg string, kv ...interface{}) error {
	a := []interface{}{msgKey, msg}
	if kv != nil {
		a = append(a, kv...)
	}
	return level.Info(logger).Log(a...)
}

// Debug log helper
func Debug(logger Logger, msg string, kv ...interface{}) error {
	a := []interface{}{msgKey, msg}
	if kv != nil {
		a = append(a, kv...)
	}
	return level.Debug(logger).Log(a...)
}

// Warn log helper
func Warn(logger Logger, msg string, kv ...interface{}) error {
	a := []interface{}{msgKey, msg}
	if kv != nil {
		a = append(a, kv...)

	}
	return level.Warn(logger).Log(a...)
}

// Error log helper
func Error(logger Logger, msg string, kv ...interface{}) error {
	a := []interface{}{msgKey, msg}
	if kv != nil {
		a = append(a, kv...)
	}
	return level.Error(logger).Log(a...)
}

// Fatal log helper
func Fatal(logger Logger, msg string, kv ...interface{}) {
	a := []interface{}{msgKey, msg}
	if kv != nil {
		a = append(a, kv...)
	}
	level.Error(logger).Log(a...)
	os.Stderr.Sync()
	os.Stdout.Sync()
	pos.Exit(1)
}

type consoleLogger struct {
	w     io.Writer
	pkg   string
	theme ColorTheme
}

var (
	infoColor     = color.New(color.FgGreen)
	warnColor     = color.New(color.FgRed)
	errColor      = color.New(color.FgRed, color.Bold)
	debugColor    = color.New(color.FgBlue)
	pkgColor      = color.New(color.FgHiMagenta)
	msgColor      = color.New(color.FgWhite, color.Bold)
	msgLightColor = color.New(color.FgBlack, color.Bold)
	kvColor       = color.New(color.FgYellow)

	termMu       sync.RWMutex
	termWidth    = term.GetTerminalWidth()
	ansiStripper = regexp.MustCompile("\\x1b\\[[0-9;]*m")
)

const (
	pkgKey = "pkg"
	msgKey = "msg"
	tsKey  = "ts"
)

var (
	levelKey      = fmt.Sprintf("%v", level.Key())
	debugLevel    = level.DebugValue().String()
	warnLevel     = level.WarnValue().String()
	errLevel      = level.ErrorValue().String()
	infoLevel     = level.InfoValue().String()
	customerIDKey = "customer_id"
	onprem        = os.Getenv("PP_CUSTOMER_ID") != ""
)

func (l *consoleLogger) Log(keyvals ...interface{}) error {
	n := (len(keyvals) + 1) / 2 // +1 to handle case when len is odd
	m := make(map[string]interface{}, n)
	m[pkgKey] = l.pkg
	keys := make([]string, 0)
	for i := 0; i < len(keyvals); i += 2 {
		k := keyvals[i]
		if s, ok := k.(string); ok {
			if s[0:1] == "$" {
				// for the console, we're going to ignore internal keys
				continue
			}
			if onprem && s == customerIDKey {
				// for onpremise env, don't show customer_id to the console
				continue
			}
		}
		var v interface{} = ErrMissingValue
		if i+1 < len(keyvals) {
			v = keyvals[i+1]
		}
		merge(m, k, v)
		keys = append(keys, fmt.Sprintf("%v", k))
	}
	hasColors := !color.NoColor && l.theme != NoColorTheme
	lvl := fmt.Sprintf("%v", m[levelKey])
	var c *color.Color
	if hasColors {
		switch lvl {
		case debugLevel:
			{
				c = debugColor
			}
		case warnLevel:
			{
				c = warnColor
			}
		case errLevel:
			{
				c = errColor
			}
		default:
			{
				lvl = infoLevel
				c = infoColor
			}
		}
	}
	pkg := m[pkgKey].(string)
	if len(pkg) > 7 {
		pkg = pkg[0:7]
	}
	kv := make([]string, 0)
	var msg string
	if ms, ok := m[msgKey].(string); ok {
		msg = ansiStripper.ReplaceAllString(ms, "")
	} else {
		msg = fmt.Sprintf("%v", m[msgKey])
	}
	left := len(msg) + 7 + 10
	slen := 0
	scnt := 0
	sort.Strings(keys)
	for _, k := range keys {
		switch k {
		case levelKey, pkgKey, tsKey, msgKey:
			{
				continue
			}
		}
		v := m[k]
		k = ansiStripper.ReplaceAllString(k, "")
		val := ansiStripper.ReplaceAllString(strings.TrimSpace(fmt.Sprintf("%v", v)), "")
		slen += 1 + len(k) + len(val)
		scnt++
		if hasColors {
			kv = append(kv, fmt.Sprintf("%s=%s", kvColor.Sprint(k), kvColor.Sprint(val)))
		} else {
			kv = append(kv, fmt.Sprintf("%s=%s", k, val))
		}
	}
	var kvs string
	termMu.RLock()
	pad := int(termWidth) - left
	termMu.RUnlock()
	slen += scnt - 1
	if len(kv) > 0 {
		kvs = strings.Join(kv, " ")
		var buf bytes.Buffer
		for i := 0; i < pad-slen; i++ {
			buf.WriteByte(' ')
		}
		buf.WriteString(kvs)
		kvs = buf.String()
	}
	o := l.w
	if l.w == os.Stdout && !color.NoColor {
		// for windows, we must use the color.Output writer to get the escape codes properly output
		o = color.Output
	}
	if color.NoColor {
		fmt.Fprintf(o, "%s %s %s %s\n", fmt.Sprintf("%-6s", strings.ToUpper(lvl)), fmt.Sprintf("%-8s", pkg), msg, kvs)
	} else {
		mc := msgColor
		if l.theme == LightLogColorTheme {
			mc = msgLightColor
		}
		fmt.Fprintf(o, "%s %s %s %s\n", c.Sprintf("%-6s", strings.ToUpper(lvl)), pkgColor.Sprintf("%-8s", pkg), mc.Sprint(msg), kvs)
	}
	return nil
}

func merge(dst map[string]interface{}, k, v interface{}) {
	var key string
	switch x := k.(type) {
	case string:
		key = x
	case fmt.Stringer:
		key = safeString(x)
	default:
		key = fmt.Sprint(x)
	}

	// We want json.Marshaler and encoding.TextMarshaller to take priority over
	// err.Error() and v.String(). But json.Marshall (called later) does that by
	// default so we force a no-op if it's one of those 2 case.
	switch x := v.(type) {
	case json.Marshaler:
	case encoding.TextMarshaler:
	case error:
		v = safeError(x)
	case fmt.Stringer:
		v = safeString(x)
	}

	dst[key] = v
}

func safeString(str fmt.Stringer) (s string) {
	defer func() {
		if panicVal := recover(); panicVal != nil {
			if v := reflect.ValueOf(str); v.Kind() == reflect.Ptr && v.IsNil() {
				s = "NULL"
			} else {
				panic(panicVal)
			}
		}
	}()
	s = str.String()
	return
}

func safeError(err error) (s interface{}) {
	defer func() {
		if panicVal := recover(); panicVal != nil {
			if v := reflect.ValueOf(err); v.Kind() == reflect.Ptr && v.IsNil() {
				s = nil
			} else {
				panic(panicVal)
			}
		}
	}()
	s = err.Error()
	return
}

// OutputFormat is the logging output format
type OutputFormat byte

const (
	// JSONLogFormat will output JSON formatted logs
	JSONLogFormat OutputFormat = 1 << iota
	// LogFmtLogFormat will output logfmt formatted logs
	LogFmtLogFormat
	// ConsoleLogFormat will output logfmt colored logs to console
	ConsoleLogFormat
)

// ColorTheme is the logging color theme
type ColorTheme byte

const (
	// DarkLogColorTheme is the default color theme for console logging (if enabled)
	DarkLogColorTheme ColorTheme = 1 << iota
	// LightLogColorTheme is for consoles that are light (vs dark)
	LightLogColorTheme
	// NoColorTheme will turn off console colors
	NoColorTheme
)

// Level is the minimum logging level
type Level byte

const (
	// InfoLevel will only log level and above (default)
	InfoLevel Level = 1 << iota
	// DebugLevel will log all messages
	DebugLevel
	// WarnLevel will only log warning and above
	WarnLevel
	// ErrorLevel will only log error and above
	ErrorLevel
	// NoneLevel will no log at all
	NoneLevel
)

// LevelFromString will return a LogLevel const from a named string
func LevelFromString(level string) Level {
	switch level {
	case "info", "INFO", "":
		{
			return InfoLevel
		}
	case "debug", "DEBUG":
		{
			return DebugLevel
		}
	case "warn", "WARN", "warning", "WARNING":
		{
			return WarnLevel
		}
	case "error", "ERROR", "fatal", "FATAL":
		{
			return ErrorLevel
		}
	}
	return NoneLevel
}

// LoggerCloser returns a logger which implements a Close interface
type LoggerCloser interface {
	Logger
	Close() error
}

type logcloser struct {
	w io.WriteCloser
	l Logger
	o sync.Once
}

// Log will dispatch the log to the next logger
func (l *logcloser) Log(kv ...interface{}) error {
	return l.l.Log(kv...)
}

// Close will close the underlying writer
func (l *logcloser) Close() error {
	l.o.Do(func() {
		if w, ok := l.l.(LoggerCloser); ok {
			w.Close()
		}
		// don't close the main process stdout/stderr
		if l.w == os.Stdout || l.w == os.Stderr {
			return
		}
		l.w.Close()
	})
	return nil
}

type nocloselog struct {
	l Logger
}

// Log will dispatch the log to the next logger
func (l *nocloselog) Log(kv ...interface{}) error {
	return l.l.Log(kv...)
}

// Close will close the underlying writer
func (l *nocloselog) Close() error {
	if w, ok := l.l.(LoggerCloser); ok {
		return w.Close()
	}
	return nil
}

// WithLogOptions is a callback for customizing the logger event further before returning
type WithLogOptions func(logger Logger) Logger

// WithDefaultTimestampLogOption will add the timestamp in UTC to the ts key
func WithDefaultTimestampLogOption() WithLogOptions {
	return func(logger Logger) Logger {
		return log.With(logger, tsKey, log.DefaultTimestampUTC)
	}
}

// crashlogger will log to a file
type crashlogger struct {
	next Logger
	ch   chan string
}

func (l *crashlogger) Log(keyvals ...interface{}) error {
	l.next.Log(keyvals...)
	pumpLogsIntoChan(l.ch, keyvals...)
	return nil
}

// pumpLogsIntoChan cleans up the keyvals before pumping it into the chan
func pumpLogsIntoChan(pipe chan string, keyvals ...interface{}) {
	n := (len(keyvals) + 1) / 2 // +1 to handle case when len is odd
	m := make(map[string]interface{}, n)
	for i := 0; i < len(keyvals); i += 2 {
		k := keyvals[i]
		var v interface{} = log.ErrMissingValue
		if i+1 < len(keyvals) {
			v = keyvals[i+1]
		}
		merge(m, k, v)
	}
	pipe <- pjson.Stringify(m)
}

// run will spin up a goroutine that pumps logs to a file
func (l *crashlogger) run() {
	go func() {
		lf := byte('\n')
		var failedLastTime bool
		var numFailedInARow int
		f := getLogFd()
		defer f.Close()
		for buf := range l.ch {
			// don't write empty strings
			if len(buf) == 0 || buf[0] == lf {
				continue
			}
			n, err := fmt.Fprintln(f, buf)
			if n < 0 || err != nil {
				if failedLastTime {
					numFailedInARow++
				} else {
					failedLastTime = true
					numFailedInARow = 1
				}
				if numFailedInARow > 5 && err != nil {
					fmt.Println("error writing to log file five times in a row", err)
				}
			} else {
				failedLastTime = false
			}
		}
		f.Sync()
		f.Close()
	}()
}

// WithCrashLogger will tell the logger to create a logfile and log to it, in
// the event of a crash it will send the logfile to our analytics api. At the end
// of execution the log file will be deleted
func WithCrashLogger() WithLogOptions {
	pos.OnExit(func(_ int) {
		DeleteLogFile()
	})
	return func(logger Logger) Logger {
		ch := make(chan string, 1000)
		l := &crashlogger{
			logger,
			ch,
		}
		l.run()
		return l
	}
}

// getLogFd will return an *os.File that is the logfile. If the logfile doesn't
// exist then it will create one
func getLogFd() *os.File {
	path := os.Getenv("PP_LOGFILE")
	if path == "" {
		path = NewRandomLogPath()
	}
	if fileutil.FileExists(path) {
		f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_EXCL, 0600)
		if err != nil {
			panic(fmt.Errorf("Error openning log file %v", err))
		}
		return f
	}
	f, err := os.Create(path)
	if err != nil {
		panic(fmt.Errorf("Error creating crash log file at %s. %v", path, err))
	}
	return f
}

// NewRandomLogPath will define a random log path in the env,
// it does not create a logfile, just a path.
func NewRandomLogPath() string {
	tmp := os.TempDir()
	fn := filepath.Join(tmp, fmt.Sprintf("pinpt-%d%d", time.Now().Unix(), os.Getpid()))
	os.Setenv("PP_LOGFILE", fn)
	return fn
}

// DeleteLogFile will delete the log file
func DeleteLogFile() error {
	path := os.Getenv("PP_LOGFILE")
	return os.Remove(path)
}

// WrappedLogCloser wraps a logger and its delegate
type WrappedLogCloser struct {
	logger   Logger
	delegate Logger
}

// Close will close the underlying channel
func (l *WrappedLogCloser) Close() error {
	if w, ok := l.logger.(LoggerCloser); ok {
		w.Close()
	}
	if w, ok := l.delegate.(LoggerCloser); ok {
		w.Close()
	}
	return nil
}

// Log will send logs to delegate
func (l *WrappedLogCloser) Log(keyvals ...interface{}) error {
	return l.delegate.Log(keyvals...)
}

// NewWrappedLogCloser returns a new instance of WrappedLogCloser
func NewWrappedLogCloser(logger Logger, delegate Logger) *WrappedLogCloser {
	return &WrappedLogCloser{logger, delegate}
}

// NewNoOpTestLogger is a test logger that doesn't log at all
func NewNoOpTestLogger() LoggerCloser {
	return &nocloselog{level.NewFilter(log.NewLogfmtLogger(os.Stderr), level.AllowError())}
}

type maskingLogger struct {
	next Logger
}

var maskedKeys = map[string]bool{
	"password":              true,
	"passwd":                true,
	"email":                 true,
	"access_key":            true,
	"secret":                true,
	"token":                 true,
	"aws_access_key_id":     true,
	"aws_secret_access_key": true,
	"api_key":               true,
	"apikey":                true,
}

var maskedPattern = regexp.MustCompile("(?i)(password|passwd|secret|access_key|token|apikey|api_key)")
var awsKeyPattern = regexp.MustCompile("AKIA[A-Z0-9]{16}")

func mask(v interface{}) string {
	s := pstring.Value(v)
	l := len(s)
	if l == 0 {
		return s
	}
	if l == 1 {
		return "*"
	}
	h := int(l / 2)
	var buf bytes.Buffer
	buf.WriteString(s[0:h])
	buf.WriteString(strings.Repeat("*", l-h))
	return buf.String()
}

// Close will close the underlying channel
func (l *maskingLogger) Close() error {
	if w, ok := l.next.(LoggerCloser); ok {
		return w.Close()
	}
	return nil
}

func keyMatches(k string) bool {
	s := strings.ToLower(k)
	return maskedKeys[s] || maskedPattern.MatchString(s)
}

// MaskKV will mask a map of strings to strings and that and weather
// or not it changed anything
func MaskKV(k, v string) (string, bool) {
	if keyMatches(k) {
		nv := mask(v)
		if nv != v {
			return nv, true
		}
	}
	return v, false
}

func (l *maskingLogger) Log(keyvals ...interface{}) error {
	// we have to make a copy as to not have a race
	newvals := append([]interface{}{}, keyvals...)
	for i := 0; i < len(newvals); i += 2 {
		k := newvals[i]
		var v interface{} = log.ErrMissingValue
		if i+1 < len(newvals) {
			v = newvals[i+1]
		}
		if s, ok := k.(string); ok && v != log.ErrMissingValue {
			if keyMatches(s) {
				nv := mask(v)
				if nv != v {
					newvals[i+1] = nv
				}
			} else {
				switch val := v.(type) {
				case string:
					if awsKeyPattern.MatchString(val) {
						newvals[i+1] = mask(val)
					}
				case map[string]string:
					{
						var found bool
						for k, v := range val {
							v, found = MaskKV(k, v)
							val[k] = v
						}
						if found {
							newvals[i+1] = pjson.Stringify(val)
						}
					}
				case map[string]interface{}:
					{
						var found bool
						for k, v := range val {
							if keyMatches(k) {
								nv := mask(v)
								if nv != v {
									found = true
									val[k] = nv
								}
							}
						}
						if found {
							newvals[i+1] = pjson.Stringify(val)
						}
					}
				case []string:
					{
						var found bool
						for x := 0; x < len(val); x += 2 {
							k := val[x]
							var v string
							if x+1 < len(val) {
								v = val[x+1]
							}
							if keyMatches(k) {
								nv := mask(v)
								if v != nv {
									found = true
									val[x+1] = nv
								}
							} else if awsKeyPattern.MatchString(v) {
								found = true
								val[x+1] = mask(v)
							} else if awsKeyPattern.MatchString(k) {
								val[x] = mask(k)
							}
						}
						if found {
							newvals[i+1] = pjson.Stringify(val)
						}
					}
				}
			}
		}
	}
	return l.next.Log(newvals...)
}

// newMaskingLogger returns a logger that will attempt to mask certain sensitive
// details
func newMaskingLogger(logger Logger) *maskingLogger {
	return &maskingLogger{logger}
}

// dedupelogger will de-dupe the keys (LIFO) excluding msg, level, etc
// such that we only emit one unique key per log message
type dedupelogger struct {
	next Logger
}

func (l *dedupelogger) Log(keyvals ...interface{}) error {
	newvals := make([]interface{}, 0)
	var kvs map[string]interface{}
	for i := 0; i < len(keyvals); i += 2 {
		k := keyvals[i]
		var v interface{} = log.ErrMissingValue
		if i+1 < len(keyvals) {
			v = keyvals[i+1]
		}
		if k == msgKey || k == levelKey {
			newvals = append(newvals, k, v)
		} else {
			if kvs == nil {
				kvs = make(map[string]interface{})
			}
			kvs[fmt.Sprintf("%s", k)] = v
		}
	}
	if kvs != nil && len(kvs) > 0 {
		var i int
		keys := make([]string, len(kvs))
		for k := range kvs {
			keys[i] = k
			i++
		}
		sort.Strings(keys)
		for _, k := range keys {
			newvals = append(newvals, k, kvs[k])
		}
	}
	return l.next.Log(newvals...)
}

// Close will close the underlying channel
func (l *dedupelogger) Close() error {
	if w, ok := l.next.(LoggerCloser); ok {
		return w.Close()
	}
	return nil
}

// track the depth from which the call stack should track the call site
const callStackDepth = 9

// NewLogger will create a new logger
func NewLogger(writer io.Writer, format OutputFormat, theme ColorTheme, minLevel Level, pkg string, opts ...WithLogOptions) LoggerCloser {
	// short circuit it all if log level is none
	if minLevel == NoneLevel {
		return &nocloselog{log.NewNopLogger()}
	}

	var logger Logger

	loggers := make([]Logger, 0)

	switch format {
	case JSONLogFormat:
		{
			logger = log.NewJSONLogger(writer)
		}
	case LogFmtLogFormat:
		{
			logger = log.NewLogfmtLogger(writer)
		}
	case ConsoleLogFormat:
		{
			logger = &consoleLogger{writer, pkg, theme}
		}
	}

	loggers = append(loggers, logger)

	// turn off caller for test package
	allowCaller := pkg != "test"

	switch minLevel {
	case DebugLevel:
		{
			logger = level.NewFilter(logger, level.AllowDebug())
			if allowCaller {
				logger = log.With(logger, "caller", log.Caller(callStackDepth))
			}
		}
	case InfoLevel:
		{
			logger = level.NewFilter(logger, level.AllowInfo())
		}
	case ErrorLevel:
		{
			logger = level.NewFilter(logger, level.AllowError())
			if allowCaller {
				logger = log.With(logger, "caller", log.Caller(callStackDepth))
			}
		}
	case WarnLevel:
		{
			logger = level.NewFilter(logger, level.AllowWarn())
		}
	}

	// allow any functions to transform the logger further before we return
	if opts != nil {
		for _, o := range opts {
			logger = NewWrappedLogCloser(logger, o(logger))
			loggers = append(loggers, logger)
		}
	}

	// make sure we close our loggers on exit
	pos.OnExit(func(_ int) {
		for _, l := range loggers {
			if lc, ok := l.(LoggerCloser); ok {
				lc.Close()
			}
		}
	})

	logger = log.With(logger, pkgKey, pkg)

	// create a masking logger
	logger = newMaskingLogger(logger)

	// make sure that all message have a level
	logger = level.NewInjector(logger, level.InfoValue())

	// make sure we de-dupe log keys
	logger = &dedupelogger{logger}

	// if the writer implements the io.WriteCloser we wrap the
	// return value in a write closer interface
	if w, ok := writer.(io.WriteCloser); ok {
		return &logcloser{w: w, l: logger}
	}

	// wrap in a type that suppresses the call to Close
	return &nocloselog{logger}
}
