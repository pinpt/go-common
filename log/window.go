// build !windows

package log

import (
	"os"
	"os/signal"
	"syscall"

	isatty "github.com/mattn/go-isatty"
	"github.com/pinpt/go-common/term"
)

// we run this here (vs windows) since windows doesn't have the syscall.SIGWINCH
// signal ... so we just ignore terminal window resize handling in windows
func init() {
	// run a goroutine to track window console resizes and update the
	// terminal setting when changed
	if os.Getenv("TERM") != "" && isatty.IsTerminal(os.Stdout.Fd()) {
		go func() {
			c := make(chan os.Signal, 1)
			signal.Notify(c, syscall.SIGWINCH)
			for range c {
				w := term.GetTerminalWidth()
				termMu.Lock()
				termWidth = w
				termMu.Unlock()
			}
		}()
	}
}
