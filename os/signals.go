// +build !windows

package os

import (
	"os"
	"syscall"
)

// getOnExitSignals returns the non-windows version of signals
func getOnExitSignals() []os.Signal {
	return []os.Signal{syscall.SIGUSR1, syscall.SIGINT, syscall.SIGTERM}
}
