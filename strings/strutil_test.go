package strings

import (
	"flag"
	"os"
	"testing"
	"time"
)

func TestResize(t *testing.T) {
	s := "foo"
	got := Resize(s, 5)
	if len(got) != 5 {
		t.Fatal("want", 5, "got", len(got))
	}
	s = "foobar"
	got = Resize(s, 5)

	if got != "fo..." {
		t.Fatal("want", "fo...", "got", got)
	}
}

func TestPadRight(t *testing.T) {
	got := PadRight("foo", 5, '-')
	if got != "foo--" {
		t.Fatal("want", "foo--", "got", got)
	}
}

func TestPadLeft(t *testing.T) {
	got := PadLeft("foo", 5, '-')
	if got != "--foo" {
		t.Fatal("want", "--foo", "got", got)
	}
}

func TestPrettyTime(t *testing.T) {
	d, _ := time.ParseDuration("")
	got := PrettyTime(d)
	if got != "---" {
		t.Fatal("want", "---", "got", got)
	}
}

var (
	database string
	username string
	password string
	hostname string
	port     int
	keyfile  string
	certfile string
)

func init() {
	flag.StringVar(&database, "database", "", "database username")
	flag.StringVar(&username, "username", "root", "database username")
	flag.StringVar(&password, "password", "", "database password")
	flag.StringVar(&hostname, "hostname", "localhost", "database hostname")
	flag.StringVar(&keyfile, "keyFile", "", "TLS key file")
	flag.StringVar(&certfile, "certFile", "", "TLS cert file")
	flag.IntVar(&port, "port", 3306, "database port")
}

func TestMain(m *testing.M) {
	flag.Parse()
	os.Exit(m.Run())
}
