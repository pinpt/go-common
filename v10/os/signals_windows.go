// +build windows

package os

import (
	"os"
	"syscall"
)

// getOnExitSignals windows doesn't support some signals so we use this
// file on windows to return supported signals
func getOnExitSignals() []os.Signal {
	return []os.Signal{syscall.SIGINT, syscall.SIGTERM}
}
