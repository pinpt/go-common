package cluster

import (
	"fmt"
	"testing"
	"time"
)

func TestTopologyLeft(t *testing.T) {
	var left []string
	s := newTopology(topologyOpts{
		MaxTimeLeaving: time.Second,
		OnLeave: func(id string) {
			left = append(left, id)
		},
		Log: func(args ...interface{}) {
			fmt.Println(args...)
		},
	})
	s.Update(date(1, 1), []string{"a", "b", "c"})
	assertEq(t,
		map[string]bool{"a": true, "b": true, "c": true},
		s.Available)

	s.Update(date(1, 2), []string{"a", "b"})
	assertEq(t,
		map[string]bool{"a": true, "b": true},
		s.Available)

	s.Tick(date(1, 3))
	assertEq(t, 0, len(left), "none left")

	s.Tick(date(1, 5))
	assertEq(t, []string{"c"}, left, "left")
}

func TestTopologyLeavingAndBack(t *testing.T) {
	var left []string
	s := newTopology(topologyOpts{
		MaxTimeLeaving: time.Second,
		OnLeave: func(id string) {
			left = append(left, id)
		},
		Log: func(args ...interface{}) {
			fmt.Println(args...)
		},
	})
	s.Update(date(1, 1), []string{"a", "b", "c"})
	assertEq(t,
		map[string]bool{"a": true, "b": true, "c": true},
		s.Available)

	s.Update(date(1, 2), []string{"a", "b"})
	assertEq(t,
		map[string]bool{"a": true, "b": true},
		s.Available)

	s.Tick(date(1, 3))
	assertEq(t, 0, len(left), "none left")

	s.Update(date(1, 4), []string{"a", "b", "c"})
	assertEq(t,
		map[string]bool{"a": true, "b": true, "c": true},
		s.Available)

	s.Tick(date(1, 10))
	assertEq(t, 0, len(left), "none left")
}
