package main

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/hex"
	"flag"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	"github.com/pinpt/go-common/db/cluster"
)

func main() {
	argAction := ""
	flag.StringVar(&argAction, "action", "", "insert|select")
	argUser := ""
	flag.StringVar(&argUser, "user", "", "")
	argPass := ""
	flag.StringVar(&argPass, "pass", "", "")
	argURLRw := ""
	flag.StringVar(&argURLRw, "url-rw", "", "")
	argURLRo := ""
	flag.StringVar(&argURLRo, "url-ro", "", "")
	argClusterURLSuffix := ""
	flag.StringVar(&argClusterURLSuffix, "cluster-url-suffix", "", "")

	flag.Parse()
	if argAction == "insert" {
		cmdInsert(argUser, argPass, argURLRw)
	} else if argAction == "select" {
		cmdSelect(argUser, argPass, argURLRo, argClusterURLSuffix)
	} else {
		panic("invalid action")
	}

}

func cmdSelect(user, pass, url, clusterURLSuffix string) {
	fmt.Println("selecting data")

	concurrency := 2

	//ro := dbRO(user, pass, url)
	//ro.SetMaxOpenConns(10)

	opts := cluster.Opts{
		User:                    user,
		Pass:                    pass,
		Database:                "testdb",
		ReadEndpointURL:         url,
		ClusterURLSuffix:        clusterURLSuffix,
		MaxConnectionsPerServer: 5,
		Log: func(args ...interface{}) {
			fmt.Println(args...)
		}}
	cl := cluster.New(opts)
	defer cl.Close()

	wg := sync.WaitGroup{}
	count := int64p()
	for c := 0; c < concurrency; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			// start additional worker after delay
			time.Sleep(time.Duration(c*10) * time.Second)
			for {
				query(cl)
				time.Sleep(2 * time.Second)

				v := atomic.AddInt64(count, 1)
				if v%5 == 0 {
					fmt.Println("queries completed", v)
				}

			}
		}(c)
	}

	wg.Wait()
}

func query(cl cluster.RDSReadCluster) {
	q := `
	SELECT t1.f1 FROM table1 AS t1
	INNER JOIN table1 AS t2 ON t2.parent = t1.id
	INNER JOIN table1 AS t3 ON t3.parent = t2.id
	INNER JOIN table1 AS t4 ON t4.parent = t3.id
	INNER JOIN table1 AS t5 ON t5.parent = t4.id
	INNER JOIN table1 AS t6 ON t6.parent = t5.id
	INNER JOIN table1 AS t7 ON t7.parent = t6.id		
	WHERE t1.id > ?
	ORDER BY t1.id DESC
	LIMIT 100
	`
	rows, err := cl.Query(q, random(16, LatinAndNumbers))
	if err != nil {
		panic(err)
	}
	defer func() {
		err := rows.Close()
		if err != nil {
			panic(err)
		}
	}()
	for rows.Next() {
		var f1 string
		err := rows.Scan(&f1)
		if err != nil {
			panic(err)
		}
	}
}

func cmdInsert(user, pass, url string) {
	fmt.Println("inserting data")
	rw := dbRW(user, pass, url)
	setupSchema(rw)
	insertData(rw)

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

func setupSchema(db *sql.DB) {
	execm(db, `
		DROP TABLE IF EXISTS table1;
		CREATE TABLE table1(id varchar(16) primary key, parent varchar(16), f1 text);
	`)
}

func int64p() *int64 {
	v := int64(0)
	return &v
}

func insertData(db *sql.DB) {
	wg := sync.WaitGroup{}
	count := int64p()

	db.SetMaxOpenConns(25)

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				c := insertDataBatch(db)
				v := atomic.AddInt64(count, int64(c))
				if v%100 == 0 {
					fmt.Println("inserted", v)
				}
			}
		}()
	}
	wg.Wait()
}

func insertDataBatch(db *sql.DB) int {
	tx, err := db.Begin()
	if err != nil {
		panic(err)
	}
	var prev *string
	n := 50
	for i := 0; i < n; i++ {
		id := random(16, LatinAndNumbers)
		text := random(1000, LatinAndNumbers)
		_, err := tx.Exec("INSERT INTO table1 (id, parent, f1) VALUES (?,?,?)", id, prev, text)
		if err != nil {
			panic(err)
		}
		prev = &id
	}
	err = tx.Commit()
	if err != nil {
		panic(err)
	}
	return n
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
