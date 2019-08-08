package log

import (
	"os"
	"testing"
)

func TestLogElastic(t *testing.T) {
	if os.Getenv("PP_ELASTIC_USERNAME") == "" {
		t.SkipNow()
		return
	}
	logger := newESLogger(nil)
	Info(logger, "this is a message", "key", "value")
	logger.Close()
}
