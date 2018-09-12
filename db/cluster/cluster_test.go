package cluster

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/hex"
	"flag"
	"fmt"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
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
	res.InitialConnectionURL = argURLRo
	res.ClusterURLSuffix = argURLSuffix
	res.MaxConnectionsPerServer = 1
	res.Log = func(args ...interface{}) {
		fmt.Println(args...)
	}
	return res
}

func dbByURL(user, pass, url string) *sql.DB {
	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM([]byte(mysqlRDSCACert)) {
		panic("can't add certs")
	}
	config := &tls.Config{
		ServerName: url,
		RootCAs:    caCertPool,
	}
	profile := hex.EncodeToString([]byte(url))
	err := mysql.RegisterTLSConfig(profile, config)
	if err != nil {
		panic(err)
	}

	db, err := sql.Open("mysql", user+":"+pass+"@tcp("+url+":3306)/testdb?tls="+profile)
	if err != nil {
		panic(err)
	}
	return db
}

func dbRW(user, pass, url string) *sql.DB {
	return dbByURL(user, pass, url)
}

func dbRO(user, pass, url string) *sql.DB {
	return dbByURL(user, pass, url)
}

func execm(db *sql.DB, queries string) {
	for _, q := range strings.Split(queries, ";") {
		q := strings.TrimSpace(q)
		if q == "" {
			continue
		}
		exec(db, q)
	}
}

func exec(db *sql.DB, query string, args ...interface{}) {
	_, err := db.Exec(query, args...)
	if err != nil {
		panic(err)
	}
}

type basicRow struct {
	id string
	f1 string
}

// Basic check that retriving data from live database works
func TestBasicRowData(t *testing.T) {
	if !argRunClusterTests {
		t.Skip("pass cluster-tests-run to enable")
		return
	}
	dbRW := dbRW(argUser, argPass, argURLRw)
	defer dbRW.Close()

	execm(dbRW, `
		DROP TABLE IF EXISTS test_basic_row_data;
		CREATE TABLE test_basic_row_data(id varchar(16) primary key, f1 text);
		INSERT INTO test_basic_row_data(id, f1) VALUES ('id1','text1'), ('id2','text2');
	`)

	opts := getOpts()
	cl := New(opts)

	defer func() {
		err := cl.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	rows, err := cl.Query("SELECT id, f1 FROM test_basic_row_data ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var res []basicRow
	for rows.Next() {
		var row basicRow
		err := rows.Scan(&row.id, &row.f1)
		if err != nil {
			t.Fatal(err)
		}
		res = append(res, row)
	}
	err = rows.Err()
	if err != nil {
		t.Fatal(err)
	}

	assertEq(t, []basicRow{{id: "id1", f1: "text1"}, {id: "id2", f1: "text2"}}, res)
}

// Check that querying empty result set works
func TestEmptyResult(t *testing.T) {
	if !argRunClusterTests {
		t.Skip("pass cluster-tests-run to enable")
		return
	}
	dbRW := dbRW(argUser, argPass, argURLRw)
	defer dbRW.Close()

	execm(dbRW, `
		DROP TABLE IF EXISTS test_empty_result;
		CREATE TABLE test_empty_result(id varchar(16) primary key);
	`)

	opts := getOpts()
	cl := New(opts)

	defer func() {
		err := cl.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	rows, err := cl.Query("SELECT id FROM test_empty_result ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		t.Fatal("rows.Next() should not have been called for empty result set")
	}
	err = rows.Err()
	if err != nil {
		t.Fatal(err)
	}
}

// Check that request with invalid query return correct error
func TestInvalidQuery(t *testing.T) {
	if !argRunClusterTests {
		t.Skip("pass cluster-tests-run to enable")
		return
	}

	opts := getOpts()
	cl := New(opts)

	defer func() {
		err := cl.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	rows, err := cl.Query("SELECTXXXX id FROM test_invalid_query")
	if err == nil {
		defer rows.Close()
	}
	if err == nil {
		t.Fatal("should return error")
	}
	if err == ErrNoServersAvailable {
		t.Fatal("should return invalid query err, got ErrNoServersAvailable instead")
	}
	err2, ok := err.(*mysql.MySQLError)
	if !ok {
		t.Fatal("expected MySQLError")
	}
	if err2.Number != 1064 {
		t.Fatal("syntax error")
	}
}
