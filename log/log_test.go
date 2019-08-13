package log

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/pinpt/go-common/fileutil"
	"github.com/stretchr/testify/assert"
)

func TestLogLevels(t *testing.T) {
	assert := assert.New(t)
	var b bytes.Buffer
	deb := NewLogger(&b, JSONLogFormat, DarkLogColorTheme, DebugLevel, "test")
	inf := NewLogger(&b, JSONLogFormat, DarkLogColorTheme, InfoLevel, "test")
	war := NewLogger(&b, JSONLogFormat, DarkLogColorTheme, WarnLevel, "test")
	err := NewLogger(&b, JSONLogFormat, DarkLogColorTheme, ErrorLevel, "test")
	Debug(deb, "debug level log")
	Info(inf, "info level log")
	Warn(war, "warn level log")
	Error(err, "error level log")
	deb.Log("hello", "world")
	noop := NewNoOpTestLogger()
	Debug(noop, "not in the log")
	assert.Contains(b.String(), `"level":"debug","msg":"debug level log","pkg":"test"}`)
	assert.Contains(b.String(), `"level":"info","msg":"info level log","pkg":"test"}`)
	assert.Contains(b.String(), `"level":"warn","msg":"warn level log","pkg":"test"}`)
	assert.Contains(b.String(), `"level":"error","msg":"error level log","pkg":"test"}`)
	assert.Contains(b.String(), `"hello":"world","level":"debug","pkg":"test"}`)
}

func TestConsoleLogger(t *testing.T) {
	assert := assert.New(t)
	var b bytes.Buffer
	log := NewLogger(&b, ConsoleLogFormat, NoColorTheme, DebugLevel, "test")
	Debug(log, "debug level log")
	Info(log, "info level log")
	Warn(log, "warn level log")
	Error(log, "error level log")
	log.Close()
	assert.Equal(`DEBUG  test     debug level log 
INFO   test     info level log 
WARN   test     warn level log 
ERROR  test     error level log 
`, b.String())
}

func TestWithDefaultTimestampLogOption(t *testing.T) {
	assert := assert.New(t)
	var b bytes.Buffer
	log := NewLogger(&b, JSONLogFormat, DarkLogColorTheme, DebugLevel, "test", WithDefaultTimestampLogOption())
	Info(log, "hello")
	assert.Contains(b.String(), `{"level":"info","msg":"hello","pkg":"test","ts":"`)
}

func TestLogWithNoneLevel(t *testing.T) {
	assert := assert.New(t)
	var b bytes.Buffer
	log := NewLogger(&b, JSONLogFormat, DarkLogColorTheme, NoneLevel, "test", WithDefaultTimestampLogOption())
	Info(log, "hello")
	assert.Equal("", b.String())
}

func TestLogFmtTheme(t *testing.T) {
	assert := assert.New(t)
	var b bytes.Buffer
	log := NewLogger(&b, LogFmtLogFormat, DarkLogColorTheme, DebugLevel, "test", WithDefaultTimestampLogOption())
	Info(log, "hello")
	assert.Contains(b.String(), "pkg=test")
	assert.Contains(b.String(), "level=info msg=hello")
}

func TestLogCloser(t *testing.T) {
	assert := assert.New(t)
	log := NewLogger(ioutil.Discard, LogFmtLogFormat, DarkLogColorTheme, DebugLevel, "test", WithDefaultTimestampLogOption())
	assert.NoError(log.Close())
}

func TestLogNoOp(t *testing.T) {
	assert := assert.New(t)
	var b bytes.Buffer
	log := NewLogger(&b, LogFmtLogFormat, DarkLogColorTheme, NoneLevel, "test")
	log.Log("msg", "hi")
	assert.Empty(b.String())
}

type wc struct {
	b []byte
	c bool
}

func (w *wc) Write(buf []byte) (int, error) {
	w.b = buf
	return len(buf), nil
}

func (w *wc) Close() error {
	w.c = true
	return nil
}

func TestLogWriterCloser(t *testing.T) {
	assert := assert.New(t)
	var w wc
	log := NewLogger(&w, LogFmtLogFormat, DarkLogColorTheme, DebugLevel, "test")
	log.Log("msg", "hi")
	assert.NoError(log.Close())
	assert.Equal("pkg=test level=debug msg=hi\n", string(w.b))
	assert.True(w.c)
}

func TestLogKVNil(t *testing.T) {
	assert := assert.New(t)
	var w wc
	log := NewLogger(&w, LogFmtLogFormat, DarkLogColorTheme, DebugLevel, "test")
	Debug(log, "ni")
	assert.Equal("pkg=test level=debug msg=ni\n", string(w.b))
	Info(log, "ni")
	assert.Equal("pkg=test level=info msg=ni\n", string(w.b))
	Error(log, "ni")
	assert.Equal("pkg=test level=error msg=ni\n", string(w.b))
	Warn(log, "ni")
	assert.Equal("pkg=test level=warn msg=ni\n", string(w.b))
	assert.NoError(log.Close())
	assert.True(w.c)
}

func TestMaskEmail(t *testing.T) {
	assert := assert.New(t)
	assert.Equal("", maskEmailObject(""))
	assert.Equal("hi", maskEmailObject("hi"))
	assert.Equal("jha****@f**.bar", maskEmailObject("jhaynie@foo.bar"))
	assert.Equal("jha****@subd*****.foo.bar", maskEmailObject("jhaynie@subdomain.foo.bar"))
	assert.Equal("your email is jha****@f**.bar", maskEmailObject("your email is jhaynie@foo.bar"))
	assert.Equal("your email is jha****@f**.bar and your other one is jha****@b**.com", maskEmailObject("your email is jhaynie@foo.bar and your other one is jhaynie@bar.com"))
	assert.Equal("your email is jha****@f**.bar and your other one is jha****@b**.com and hi", maskEmailObject("your email is jhaynie@foo.bar and your other one is jhaynie@bar.com and hi"))
}

func TestMasking(t *testing.T) {
	assert := assert.New(t)
	assert.Equal("", mask(""))
	assert.Equal("*", mask("a"))
	assert.Equal("a**", mask("abc"))
	assert.Equal("abcd****", mask("abcdefgh"))
	assert.Equal("1**", mask(123))
	assert.Equal("tr**", mask(true))

	var w wc
	log := NewLogger(&w, LogFmtLogFormat, DarkLogColorTheme, DebugLevel, "test")
	Debug(log, "hi", "password", "secret")
	assert.Equal("pkg=test level=debug msg=hi password=sec***\n", string(w.b))
	Debug(log, "hi", "secret", "password")
	assert.Equal("pkg=test level=debug msg=hi secret=pass****\n", string(w.b))
	Debug(log, "hi", "email", "foo@bar.com")
	assert.Equal("pkg=test level=debug msg=hi email=f**@b**.com\n", string(w.b))
	Debug(log, "hi", "otherkey", "foo@bar.com")
	assert.Equal("pkg=test level=debug msg=hi otherkey=f**@b**.com\n", string(w.b))
	Debug(log, "your email is foo@bar.com")
	assert.Equal("pkg=test level=debug msg=\"your email is f**@b**.com\"\n", string(w.b))
	Debug(log, "hi", "access_key", "secret")
	assert.Equal("pkg=test level=debug msg=hi access_key=sec***\n", string(w.b))
	Debug(log, "hi", "passwd", "1")
	assert.Equal("pkg=test level=debug msg=hi passwd=*\n", string(w.b))
	Debug(log, "hi", "redisPassword", "1")
	assert.Equal("pkg=test level=debug msg=hi redisPassword=*\n", string(w.b))
	Debug(log, "hi", "stuff", map[string]string{"password": "1234"})
	assert.Equal("pkg=test level=debug msg=hi stuff=\"{\\\"password\\\":\\\"12**\\\"}\"\n", string(w.b))
	Debug(log, "hi", "stuff", map[string]interface{}{"password": 1234})
	assert.Equal("pkg=test level=debug msg=hi stuff=\"{\\\"password\\\":\\\"12**\\\"}\"\n", string(w.b))
	Debug(log, "hi", "stuff", []string{"password", "1234"})
	assert.Equal("pkg=test level=debug msg=hi stuff=\"[\\\"password\\\",\\\"12**\\\"]\"\n", string(w.b))
	Debug(log, "hi", "stuff", []string{"--password", "1234"})
	assert.Equal("pkg=test level=debug msg=hi stuff=\"[\\\"--password\\\",\\\"12**\\\"]\"\n", string(w.b))
	Debug(log, "hi", "my_aws_key", "AKIAIOSFODNN7EXAMPLE")
	assert.Equal("pkg=test level=debug msg=hi my_aws_key=AKIAIOSFOD**********\n", string(w.b))
	Debug(log, "hi", "stuff", []string{"my_aws_key", "AKIAIOSFODNN7EXAMPLE"})
	assert.Equal("pkg=test level=debug msg=hi stuff=\"[\\\"my_aws_key\\\",\\\"AKIAIOSFOD**********\\\"]\"\n", string(w.b))
	Debug(log, "hi", "key", "AKIAIOSFODNN7EXAMPLE")
	assert.Equal("pkg=test level=debug msg=hi key=AKIAIOSFOD**********\n", string(w.b))
	Debug(log, "hi", "secret", "AKIAIOSFODNN7EXAMPLE")
	assert.Equal("pkg=test level=debug msg=hi secret=AKIAIOSFOD**********\n", string(w.b))
	Debug(log, "hi", "access_key", "AKIAIOSFODNN7EXAMPLE")
	assert.Equal("pkg=test level=debug msg=hi access_key=AKIAIOSFOD**********\n", string(w.b))
	Debug(log, "hi", "apikey", "1234")
	assert.Equal("pkg=test level=debug msg=hi apikey=12**\n", string(w.b))
	Debug(log, "hi", "api_key", "1234")
	assert.Equal("pkg=test level=debug msg=hi api_key=12**\n", string(w.b))
}

func TestMaskingDoesntMutate(t *testing.T) {
	assert := assert.New(t)

	var w wc
	log := NewLogger(&w, LogFmtLogFormat, DarkLogColorTheme, DebugLevel, "test")
	args := []interface{}{"password", "secret"}
	Debug(log, "hi", args...)
	log.Close()
	assert.Equal("pkg=test level=debug msg=hi password=sec***\n", string(w.b))
	assert.Equal("secret", args[1])
}

func TestLevelFromString(t *testing.T) {
	assert := assert.New(t)
	assert.Equal(InfoLevel, LevelFromString("info"))
	assert.Equal(InfoLevel, LevelFromString("INFO"))
	assert.Equal(DebugLevel, LevelFromString("debug"))
	assert.Equal(DebugLevel, LevelFromString("DEBUG"))
	assert.Equal(WarnLevel, LevelFromString("warn"))
	assert.Equal(WarnLevel, LevelFromString("WARN"))
	assert.Equal(WarnLevel, LevelFromString("warning"))
	assert.Equal(WarnLevel, LevelFromString("WARNING"))
	assert.Equal(ErrorLevel, LevelFromString("error"))
	assert.Equal(ErrorLevel, LevelFromString("ERROR"))
	assert.Equal(ErrorLevel, LevelFromString("fatal"))
	assert.Equal(ErrorLevel, LevelFromString("fatal"))
	assert.Equal(NoneLevel, LevelFromString("none"))
	assert.Equal(NoneLevel, LevelFromString("foo"))
	assert.Equal(InfoLevel, LevelFromString(""))
}

func TestStripAnsiColors(t *testing.T) {
	assert := assert.New(t)
	var buf bytes.Buffer
	l := &consoleLogger{
		w:     &buf,
		pkg:   "p",
		theme: DarkLogColorTheme,
	}
	l.Log(levelKey, infoLevel, "msg", fmt.Sprintf("\x1b[%dm%s\x1b[0m", 32, "m"), fmt.Sprintf("\x1b[%dm%s\x1b[0m", 32, "k"), fmt.Sprintf("\x1b[%dm%s\x1b[0m", 32, "v"))
	assert.Equal("INFO   p        m                                                            k=v\n", buf.String())
}

func TestDeduplicateAndSortKeys(t *testing.T) {
	assert := assert.New(t)
	var w bytes.Buffer
	log := NewLogger(&w, LogFmtLogFormat, DarkLogColorTheme, DebugLevel, "test")
	Debug(log, "hi", "a", "b", "a", "c", "a", "d")
	assert.Equal("pkg=test level=debug msg=hi a=d\n", w.String())
	w.Reset()
	Debug(log, "hi", "a", 0, "b", 0, "a", 1, "b", 2, "c", "3")
	assert.Equal("pkg=test level=debug msg=hi a=1 b=2 c=3\n", w.String())
}

func TestCrashLogger(t *testing.T) {
	assert := assert.New(t)
	var w wc
	log := NewLogger(&w, JSONLogFormat, DarkLogColorTheme, DebugLevel, "test", WithCrashLogger())
	Debug(log, "testlog")
	mylog := "{\"level\":\"debug\",\"msg\":\"testlog\",\"pkg\":\"test\"}\n"
	assert.Equal(mylog, string(w.b))
	log.Close()
	time.Sleep(time.Second)
	s, err := ioutil.ReadFile(os.Getenv("PP_LOGFILE"))
	assert.NoError(err)
	assert.Equal(mylog, string(s))
}

func TestBackgroundLogger(t *testing.T) {
	assert := assert.New(t)
	var w wc
	var called string
	callback := func(fn string) {
		called = fn
	}
	log := NewLogger(&w, JSONLogFormat, DarkLogColorTheme, DebugLevel, "test", WithBackgroundLogger(callback))
	Debug(log, "testlog")
	log.Close()
	assert.NotEmpty(called)
	assert.True(fileutil.FileExists(called))
	os.Remove(called)
}
