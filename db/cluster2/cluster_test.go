package cluster

import (
	"reflect"
	"testing"
	"time"
)

/*
import (
	"flag"
	"fmt"
	"reflect"
	"testing"
	"time"
)


var argRunClusterTests = false

var argAction = ""
var argUser = ""
var argPass = ""
var argURLRw = ""
var argURLRo = ""
var argURLSuffix = ""

func init() {
	flag.BoolVar(&argRunClusterTests, "cluster-tests-run", false, "set to true and provide all options to run tests against real db")
	flag.StringVar(&argAction, "cluster-action", "", "insert|select")
	flag.StringVar(&argUser, "cluster-user", "", "")
	flag.StringVar(&argPass, "cluster-pass", "", "")
	flag.StringVar(&argURLRw, "cluster-url-rw", "", "")
	flag.StringVar(&argURLRo, "cluster-url-ro", "", "")
	flag.StringVar(&argURLSuffix, "cluster-url-suffix", "", "")
}

func getOpts() Opts {
	res := Opts{}
	res.User = argUser
	res.Pass = argPass
	res.Database = "testdb"
	res.ReadEndpointURL = argURLRo
	res.ClusterURLSuffix = argURLSuffix
	res.MaxConnectionsPerServer = 1
	res.Log = func(args ...interface{}) {
		fmt.Println(args...)
	}
	return res
}

func TestBasic(t *testing.T) {
	if !argRunClusterTests {
		t.Skip("pass cluster-tests-run to enable")
		return
	}
	opts := getOpts()
	cluster := New(opts)
	defer func() {
		err := cluster.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	_, err := cluster.DB()
	if err != nil {
		t.Fatal(err)
	}
}

func TestBasicRun(t *testing.T) {
	if !argRunClusterTests {
		t.Skip("pass cluster-tests-run to enable")
		return
	}
	opts := getOpts()
	cluster := New(opts)
	defer func() {
		err := cluster.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()
	fmt.Println("waiting for log messages")
	time.Sleep(10 * time.Second)
}
*/

func assertEq(t *testing.T, want, got interface{}, labels ...string) {
	t.Helper()
	if !reflect.DeepEqual(want, got) {
		t.Errorf("wanted %v, got %v, labels %v", want, got, labels)
	}
}

func date(m, s int) time.Time {
	return time.Date(2018, 8, 29, 23, m, s, 1, time.UTC)
}
