package cluster

import (
	"fmt"
	"testing"
	"time"
)

func TestTopologyLeft(t *testing.T) {
	var left []string
	var now time.Time
	s := newTopology(topologyOpts{
		MaxTimeLeaving: time.Second,
		OnLeave: func(id string) {
			left = append(left, id)
		},
		Log: func(args ...interface{}) {
			fmt.Println(args...)
		},
		Now: func() time.Time {
			return now
		},
	})
	now = date(1, 1)
	s.SetAvailableFromReplicaHostStatus([]string{"a", "b", "c"})
	assertEq(t,
		[]string{"a", "b", "c"},
		s.GetAvailable())

	now = date(1, 2)
	s.SetAvailableFromReplicaHostStatus([]string{"a", "b"})
	assertEq(t,
		[]string{"a", "b"},
		s.GetAvailable())

	now = date(1, 3)
	s.ExecuteOnLeaveIfNeeded()
	assertEq(t, 0, len(left), "none left")

	now = date(1, 5)
	s.ExecuteOnLeaveIfNeeded()
	assertEq(t, []string{"c"}, left, "left")
}

func TestTopologyLeavingAndBack(t *testing.T) {
	var left []string
	var now time.Time
	s := newTopology(topologyOpts{
		MaxTimeLeaving: time.Second,
		OnLeave: func(id string) {
			left = append(left, id)
		},
		Log: func(args ...interface{}) {
			fmt.Println(args...)
		},
		Now: func() time.Time {
			return now
		},
	})
	now = date(1, 1)
	s.SetAvailableFromReplicaHostStatus([]string{"a", "b", "c"})
	assertEq(t,
		[]string{"a", "b", "c"},
		s.GetAvailable())

	now = date(1, 2)
	s.SetAvailableFromReplicaHostStatus([]string{"a", "b"})
	assertEq(t,
		[]string{"a", "b"},
		s.GetAvailable())

	now = date(1, 3)
	s.ExecuteOnLeaveIfNeeded()
	assertEq(t, 0, len(left), "none left")

	now = date(1, 4)
	s.SetAvailableFromReplicaHostStatus([]string{"a", "b", "c"})
	assertEq(t,
		[]string{"a", "b", "c"},
		s.GetAvailable())

	now = date(1, 10)
	s.ExecuteOnLeaveIfNeeded()
	assertEq(t, 0, len(left), "none left")
}

func TestTopologyMarkFailedAndBack1(t *testing.T) {
	var left []string
	var now time.Time
	s := newTopology(topologyOpts{
		MaxTimeLeaving: time.Second,
		FailDuration:   10 * time.Second,
		OnLeave: func(id string) {
			left = append(left, id)
		},
		Log: func(args ...interface{}) {
			fmt.Println(args...)
		},
		Now: func() time.Time {
			return now
		},
	})

	now = date(1, 1)
	s.SetAvailableFromReplicaHostStatus([]string{"a", "b", "c"})
	assertEq(t,
		[]string{"a", "b", "c"},
		s.GetAvailable())

	now = date(1, 2)
	s.MarkFailed("c")

	now = date(1, 4)
	s.ExecuteOnLeaveIfNeeded()
	assertEq(t, []string{"c"}, left, "left")

	assertEq(t,
		[]string{"a", "b"},
		s.GetAvailable())

	now = date(1, 12)

	assertEq(t,
		[]string{"a", "b", "c"},
		s.GetAvailable())
}

func TestTopologyMarkFailedAndBack2(t *testing.T) {
	var left []string
	var now time.Time
	s := newTopology(topologyOpts{
		MaxTimeLeaving: time.Second,
		FailDuration:   10 * time.Second,
		OnLeave: func(id string) {
			left = append(left, id)
		},
		Log: func(args ...interface{}) {
			fmt.Println(args...)
		},
		Now: func() time.Time {
			return now
		},
	})

	now = date(1, 1)
	s.SetAvailableFromReplicaHostStatus([]string{"a"})
	s.MarkFailed("a")
	now = date(1, 2)
	s.SetAvailableFromReplicaHostStatus([]string{"a", "b"})
	assertEq(t,
		[]string{"b"},
		s.GetAvailable())
}

// When query fails cluster marks the node as failed. Because multiple queries can happen at the same time, this can cause MarkFailed to be called multiple times. Make sure that last timestamp is used for failed and leaving.
func TestTopologyMarkFailedMultiple(t *testing.T) {
	var left []string
	var now time.Time
	s := newTopology(topologyOpts{
		MaxTimeLeaving: 10 * time.Second,
		FailDuration:   20 * time.Second,
		OnLeave: func(id string) {
			left = append(left, id)
		},
		Log: func(args ...interface{}) {
			fmt.Println(args...)
		},
		Now: func() time.Time {
			return now
		},
	})

	now = date(1, 0)
	s.SetAvailableFromReplicaHostStatus([]string{"a", "b"})
	now = date(1, 1)
	s.MarkFailed("b")
	now = date(1, 5)
	s.MarkFailed("b")

	now = date(1, 12)
	s.ExecuteOnLeaveIfNeeded()
	assertEq(t, 0, len(left),
		"OnLeave should not be called for node yet, since we use last mark failed time for this")
	assertEq(t,
		[]string{"a"},
		s.GetAvailable(), "node b should not be marked as available yet")

	now = date(1, 16)
	s.ExecuteOnLeaveIfNeeded()
	assertEq(t, []string{"b"}, left,
		"OnLeave should have been called")
	assertEq(t,
		[]string{"a"},
		s.GetAvailable(), "node b should not be marked as available yet")

	now = date(1, 26)
	assertEq(t,
		[]string{"a", "b"},
		s.GetAvailable(), "node b should not be back")
}
