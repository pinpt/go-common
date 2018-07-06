package os

import (
	"os"
	"os/signal"
	"sync"
)

// OnExitFunc is a handler for registering for exit events
type OnExitFunc func(code int)

var (
	onexitMu       sync.Mutex
	onexitHandlers []OnExitFunc
	// map to os.Exit normally but can be hooked for testing
	finalHandler OnExitFunc = os.Exit
	onexitinit   bool
)

// Exit will ultimately call os.Exit once any registered shutdown hooks run
func Exit(code int) {
	onexitMu.Lock()
	if onexitHandlers != nil {
		// LIFO
		for i := len(onexitHandlers) - 1; i >= 0; i-- {
			onexitHandlers[i](code)
		}
		// unregister once called so we can never call the
		// registered handlers more than once
		onexitHandlers = nil
	}
	// don't hold the func lock in case it's recursive
	h := finalHandler
	onexitMu.Unlock()
	h(code)
}

// OnExit will register an OnExitFunc to be called in reverse order (LIFO) when the process exits.
// this handler will only get called if the program exits through Exit - however, a
// SIGINT or SIGTERM event should also signal OnExit handlers
func OnExit(handler OnExitFunc) {
	onexitMu.Lock()
	if onexitHandlers == nil {
		onexitHandlers = []OnExitFunc{handler}
	} else {
		onexitHandlers = append(onexitHandlers, handler)
	}
	if !onexitinit {
		onexitinit = true
		onexitMu.Unlock()
		go func() {
			c := make(chan os.Signal, 5)
			signal.Notify(c, getOnExitSignals()...)
			<-c
			Exit(0)
		}()
		return
	}
	onexitMu.Unlock()
}
