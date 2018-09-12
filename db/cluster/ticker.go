package cluster

import "time"

// ticker is an abstraction over time.Ticker that allows blocking Stop call that waits for ticker goroutine cleanup
type ticker struct {
	ticker  *time.Ticker
	stop    chan bool
	stopped chan bool
}

func newTicker(dur time.Duration) *ticker {
	return &ticker{
		ticker:  time.NewTicker(dur),
		stop:    make(chan bool),
		stopped: make(chan bool),
	}
}

func (s *ticker) Stop() {
	s.ticker.Stop()
	s.stop <- true
	<-s.stopped
}
