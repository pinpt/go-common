package cluster

import (
	"sort"
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

	Now func() time.Time
}

// topology manages the states of available servers
type topology struct {
	opts topologyOpts

	// values from information_schema.replica_host_statuss
	availableReplicaHostStatus        map[string]bool
	availableReplicaHostStatusUpdated time.Time

	lastAvailable map[string]bool

	// map[server_id]startedLeaving
	leaving map[string]time.Time

	// When a node is marked as failed, it acts as if it is not returned from initial list of replicas for FailDuration time. OnLeave is called after MaxTimeLeaving if it is < FailDuration.
	// map[server_id]lastFailed
	failed map[string]time.Time
}

// newTopology creates a new topology state manager
func newTopology(opts topologyOpts) *topology {
	s := &topology{}
	s.opts = opts
	s.availableReplicaHostStatus = map[string]bool{}
	s.leaving = map[string]time.Time{}
	s.failed = map[string]time.Time{}
	return s
}

func (s *topology) GetAvailable() []string {
	now := s.opts.Now()

	notFailed := map[string]bool{}
	for c := range s.availableReplicaHostStatus {
		if s.failed[c].After(now.Add(-s.opts.FailDuration)) {
			continue
		}
		notFailed[c] = true
	}

	newHosts := []string{}
	for id := range notFailed {
		ok := s.lastAvailable[id]
		if !ok {
			newHosts = append(newHosts, id)
		}
	}
	sort.Strings(newHosts)
	if len(newHosts) > 0 {
		s.opts.Log("new hosts available:", newHosts)
	}

	// hosts that were leaving but now available again
	for c := range notFailed {
		_, ok := s.leaving[c]
		if !ok {
			continue
		}
		s.opts.Log("host is no longer leaving:", c)
		delete(s.leaving, c)
	}

	// new leaving hosts
	for c := range s.lastAvailable {
		ok := notFailed[c]
		if !ok {
			s.leaving[c] = now
			s.opts.Log("host is leaving:", c)
		}
	}

	s.lastAvailable = notFailed

	res := []string{}
	for h := range s.lastAvailable {
		res = append(res, h)
	}
	sort.Strings(res)
	return res
}

// ExecuteOnLeaveIfNeeded calls OnLeave callback if one of the replicas is in leaving state longer than MaxTimeLeaving
func (s *topology) ExecuteOnLeaveIfNeeded() {
	now := s.opts.Now()

	for c, startedLeaving := range s.leaving {
		if startedLeaving.Before(now.Add(-s.opts.MaxTimeLeaving)) {
			s.opts.Log("host left:", c)
			s.opts.OnLeave(c)
			delete(s.leaving, c)
		}
	}
}

func (s *topology) SetAvailableFromReplicaHostStatus(current []string) {
	now := s.opts.Now()

	s.availableReplicaHostStatus = map[string]bool{}
	for _, c := range current {
		s.availableReplicaHostStatus[c] = true
	}
	s.availableReplicaHostStatusUpdated = now
}

func (s *topology) MarkFailed(host string) {
	s.opts.Log("marking host as failed:", host)
	now := s.opts.Now()
	s.failed[host] = now
	s.leaving[host] = now
}
