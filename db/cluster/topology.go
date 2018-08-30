package cluster

import (
	"time"
)

// topologyOpts are the options needed to create topology state
type topologyOpts struct {
	// MaxTimeLeaving is the max time the server is in leaving state. When this time is exceeded the OnLeave function is called
	MaxTimeLeaving time.Duration
	// OnLeave function is called when a server is in leaving state longer than MaxTimeLeaving
	OnLeave func(id string)
	// FailDuration specifies the duration for which MarkFailed is active for
	FailDuration time.Duration
	// Log outputs the passes data for debugging purposes
	Log func(args ...interface{})
}

// topology manages the states of available servers
type topology struct {
	opts topologyOpts

	// Hosts can move between Available <-> Leaving.
	// We have a separate leaving state to avoid closing sql.DB for a minute to avoid interrupting queries. If the server is flaky we don't want ot recrease sql.DB.

	// available is the list of currently available server ids
	// using map as set
	// map[server_id]bool
	Available map[string]bool

	// map[server_id]startedLeaving
	leaving map[string]time.Time
}

// newTopology creates a new topology state manager
func newTopology(opts topologyOpts) *topology {
	s := &topology{}
	s.opts = opts
	s.Available = map[string]bool{}
	s.leaving = map[string]time.Time{}
	return s
}

// Update updates the topology with current available servers
func (s *topology) Update(ts time.Time, current []string) {
	res := map[string]bool{}
	for _, c := range current {
		res[c] = true
	}
	newHosts := []string{}
	for id := range res {
		ok := s.Available[id]
		if !ok {
			newHosts = append(newHosts, id)
		}
	}
	if len(newHosts) > 0 {
		s.opts.Log("new hosts available:", newHosts)
	}

	// hosts that were leaving but now available again
	for c := range res {
		_, ok := s.leaving[c]
		if !ok {
			continue
		}
		s.opts.Log("host is no longer leaving:", c)
		delete(s.leaving, c)
	}
	// new leaving hosts
	for c := range s.Available {
		ok := res[c]
		if !ok {
			s.leaving[c] = ts
			s.opts.Log("host is leaving:", c)
		}
	}
	s.Available = res
}

// Tick updates the leaving server state. Call this periodically.
func (s *topology) Tick(ts time.Time) {
	for c, startedLeaving := range s.leaving {
		if startedLeaving.Before(ts.Add(-s.opts.MaxTimeLeaving)) {
			s.opts.Log("host left:", c)
			s.opts.OnLeave(c)
			delete(s.leaving, c)
		}
	}
}
