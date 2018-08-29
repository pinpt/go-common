// TODO:
// Remove servers when they are turned off due to low load
// (maybe) Prioritize based on CPU usage
// (maybe) Slow down queries if CPU at 100% on all servers

package cluster

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/hex"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
)

type Opts struct {
	User             string
	Pass             string
	ReadEndpoint     string
	ClusterURLSuffix string
}

type Cluster struct {
	user             string
	pass             string
	readEndpoint     string
	clusterURLSuffix string

	readEndpointDB *sql.DB

	topology   []string
	topologyMu sync.Mutex

	// dbs are the currently connected db instances
	dbs   map[string]*sql.DB
	dbsMu sync.Mutex
}

func New(opts Opts) *Cluster {
	s := &Cluster{}
	s.user = opts.User
	s.pass = opts.Pass
	s.readEndpoint = opts.ReadEndpoint
	s.clusterURLSuffix = opts.ClusterURLSuffix

	s.readEndpointDB = s.newDB(s.readEndpoint)
	s.updateTopology()

	s.dbs = map[string]*sql.DB{}

	ticker := time.NewTicker(30 * time.Second)
	go func() {
		for {
			<-ticker.C
			s.updateTopology()
		}
	}()

	return s
}

func (s *Cluster) updateTopology() error {
	// TODO: also check cpu values
	q := `
		SELECT server_id, if(session_id = 'MASTER_SESSION_ID', 'writer', 'reader') as role
		FROM information_schema.replica_host_status
		WHERE last_update_timestamp > NOW() - 60
		HAVING role = 'reader'
	`
	rows, err := s.readEndpointDB.Query(q)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	res := []string{}
	for rows.Next() {
		var serverID string
		var role string
		err := rows.Scan(&serverID, &role)
		if err != nil {
			panic(err)
		}
		res = append(res, serverID)
	}

	s.topologyMu.Lock()
	defer s.topologyMu.Unlock()

	fmt.Println("new topology", res)
	s.topology = res

	return nil

}

const maxInstances = 3

// DB returns a db instance that should be used for next select request. Do no store or reuse.
func (s *Cluster) DB() *sql.DB {
	s.dbsMu.Lock()
	defer s.dbsMu.Unlock()

	if len(s.dbs) < maxInstances {
		notConnected := []string{}
		for _, host := range s.topology {
			if _, ok := s.dbs[host]; !ok {
				notConnected = append(notConnected, host)
			}
		}
		fmt.Println("not connected", notConnected)
		if len(notConnected) == 0 {
			// all connected
			// return random db
		} else {
			host := notConnected[randn(len(notConnected))]
			db := s.newDB(host + "." + s.clusterURLSuffix)
			s.dbs[host] = db
			if db == nil {
				panic("db == nil")
			}
			fmt.Println("not reusing db")
			return db
		}
	}
	fmt.Println("reusing db")
	return randomDB(s.dbs)
}

func randomDB(dbs map[string]*sql.DB) *sql.DB {
	i := 0
	n := randn(len(dbs))
	for _, db := range dbs {
		if i == n {
			return db
		}
		i++
	}
	panic("not possible")
}

func (s *Cluster) newDB(url string) *sql.DB {
	db := dbByURL(s.user, s.pass, url)
	db.SetMaxOpenConns(20)
	return db
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

func randn(maxNotInclusive int) int {
	return rand.Intn(maxNotInclusive)
}
