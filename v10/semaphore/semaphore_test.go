package semaphore

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewSemaphore(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	permits := 3
	channel := make(chan struct{}, permits)
	for i := 0; i < permits; i++ {
		channel <- struct{}{}
	}

	tmp := &Semaphore{
		permits: permits,
		avail:   permits,
		channel: channel,
	}

	newS := New(permits)

	assert.Equal(tmp.permits, newS.permits)
	newS.Acquire()
	assert.Equal(newS.permits-1, newS.avail)
	newS.AcquireMany(1)
	assert.Equal(newS.permits-2, newS.avail)
	newS.Release()
	assert.Equal(newS.permits-1, newS.avail)
	newS.ReleaseMany(1)
	assert.Equal(newS.permits, newS.avail)
	assert.Equal(newS.permits, newS.AvailablePermits())
	assert.Equal(newS.permits, newS.DrainPermits())
	newS.AcquireWithin(1, time.Millisecond*100)
	assert.Equal(0, newS.AvailablePermits())
}
