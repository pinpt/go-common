// +build !windows

package os

import (
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOnExit(t *testing.T) {
	assert := assert.New(t)
	value := 0
	onexitMu.Lock()
	finalHandler = func(ec int) {
		value += ec
	}
	onexitMu.Unlock()
	// test that our exit handler is called even though we have
	// no onexit handlers
	Exit(1)
	assert.Equal(1, value)

	// now register a customer on exit and make sure that both
	// are called
	value = 0
	onexitMu.Lock()
	finalHandler = func(ec int) {
		value -= ec
	}
	onexitMu.Unlock()
	OnExit(func(ec int) {
		value += ec + 1
	})
	OnExit(func(ec int) {
		value = ec
	})
	// now test the order ... the 2nd should be called before the
	// 1st and thus the math should add correctly
	Exit(1)
	assert.Equal(2, value)

	value = 0
	Exit(1)
	// should have unregistered the previous one when exit calls
	// but final handler will always be called
	assert.Equal(-1, value)

	if os.Getenv("CI") == "" {
		// now we're going to test signal handler to make sure it will
		// trigger the correct shutdown as well
		var called int32
		var wg sync.WaitGroup
		wg.Add(2)
		onexitMu.Lock()
		finalHandler = func(ec int) {
			atomic.StoreInt32(&called, 1)
			wg.Done()
		}
		onexitMu.Unlock()
		go func() {
			syscall.Kill(syscall.Getpid(), syscall.SIGUSR1)
			runtime.Gosched()
			wg.Done()
		}()
		wg.Wait()
		assert.Equal(int32(1), atomic.LoadInt32(&called))
	}
}
