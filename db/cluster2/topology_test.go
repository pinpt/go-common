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
