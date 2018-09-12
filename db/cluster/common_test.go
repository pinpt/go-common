package cluster

import (
	"reflect"
	"testing"
	"time"
)

func assertEq(t *testing.T, want, got interface{}, labels ...string) {
	t.Helper()
	if !reflect.DeepEqual(want, got) {
		t.Errorf("wanted %v, got %v, labels %v", want, got, labels)
	}
}

func date(m, s int) time.Time {
	return time.Date(2018, 8, 29, 23, m, s, 1, time.UTC)
}
