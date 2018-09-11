package cluster

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
)

// Opts are options needed for creating a new RDSReadCluster.
type Opts struct {
	// User is the database user
	User string
	// Pass is the password
	Pass string
	// Port is the port to use for connections. If not specified 3306 is used.
	Port int
	// Database name to select.
	Database string

	// ExtraDriverOpts are concatenated and added to the string used to initialize the driver connection. Used by underlying driver. Merged to DefaultDriverOpts and tls.
	ExtraDriverOpts url.Values

	// ReadEndpointURL url used to connect to any read replica to retrive the current cluster state from information_schema.replica_host_status table
	ReadEndpointURL string

	// ClusterURLSuffix is added to server_id retrieved from information_schema.replica_host_status. The resulting string is used to connect to specific replica.
	ClusterURLSuffix string

	// UpdateTopologyEvery specifies how often should topology be updated. If zero defaultUpdateTopology (30s) is used.
	UpdateTopologyEvery time.Duration

	// MaxConnectionsPerServer sets the maximum number of open connection per read replica.
	// Make sure to set a reasonable amount.
	MaxConnectionsPerServer int

	// Log outputs log of RDSReadCluster
	Log func(args ...interface{})
}

// defaultPort is used when no Port is specified in Opts.
const defaultPort = 3306

// DefaultDriverOpts are the options passed to mysql driver by default. Adjust ExtraDriverOpts if you want to add or modify.
var DefaultDriverOpts = url.Values{
	"collation": {"utf8_unicode_ci"},
	"charset":   {"utf8mb4"},
	"parseTime": {"true"},
}

// defaultUpdateTopology specifies how often the client should query the sql server for current servers list
const defaultUpdateTopology = 30 * time.Second // 30 * time.Second

const maxTimeLeaving = 20 * time.Second
const failDuration = 60 * time.Second

// RDSReadCluster provides db instances on demand. In comparison to default sql.DB with go-sql-driver/mysql it load balances across all read replicas.
type RDSReadCluster interface {

	// QueryContext executes a query that returns rows, typically a SELECT.
	// The args are for any placeholder parameters in the query.
	// Does automatic load balancing and retries on broken connection.
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)

	// Query executes a query that returns rows, typically a SELECT.
	// The args are for any placeholder parameters in the query.
	Query(query string, args ...interface{}) (*sql.Rows, error)

	// Close frees all resources. Make sure to finish all queries before calling Close.
	Close() error
}

// rdsReadCluster is an implementaion of RDSReadCluster
type rdsReadCluster struct {
	opts Opts

	// topology is the list of currently available server ids
	topology   *topology
	topologyMu sync.Mutex

	topologyUpdates *ticker

	// dbs is a map of db instances connecting to specific server
	// map[server_id]*sql.DB
	dbs   map[string]*sql.DB
	dbsMu sync.Mutex
}

// New creates a new rdsReadCluster
func New(opts Opts) *rdsReadCluster {
	if opts.Port == 0 {
		opts.Port = defaultPort
	}
	if opts.UpdateTopologyEvery == 0 {
		opts.UpdateTopologyEvery = defaultUpdateTopology
	}
	if opts.User == "" || opts.Pass == "" || opts.Port == 0 || opts.Database == "" || opts.ReadEndpointURL == "" || opts.ClusterURLSuffix == "" || opts.UpdateTopologyEvery == 0 || opts.MaxConnectionsPerServer == 0 || opts.Log == nil {
		panic("provide all options")
	}
	s := &rdsReadCluster{}
	s.opts = opts

	s.dbs = map[string]*sql.DB{}

	s.topology = newTopology(topologyOpts{
		MaxTimeLeaving: maxTimeLeaving,
		OnLeave: func(id string) {
			s.dbsMu.Lock()
			defer s.dbsMu.Unlock()
			if _, ok := s.dbs[id]; !ok {
				// already left
				return
			}
			go func(db *sql.DB) {
				// do not block
				db.Close()
			}(s.dbs[id])
			delete(s.dbs, id)
		},
		FailDuration: failDuration,
		Log:          s.opts.Log,
		Now:          time.Now,
	})

	err := s.updateTopologyInitial()
	if err != nil {
		panic(err)
	}

	s.setupTopologyTicker()
	return s
}

var ErrCantConnect = errors.New("cluster: can't connect to any read replica")

func (s *rdsReadCluster) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {

	maxRetries := 3
	for i := 0; i < maxRetries; i++ {
		if i > 0 {
			s.opts.Log("query retry", i)
		}

		db, host, err := s.loadBalancedDB()
		if err != nil {
			return nil, err
		}
		fmt.Println("executing query to host", host)
		rows, err := db.QueryContext(ctx, query, args...)
		if err != nil {
			//if isConnErr(err) { does not cover all possible errors
			s.topologyMu.Lock()
			s.topology.MarkFailed(host)
			s.topologyMu.Unlock()
			continue
		}
		return rows, err
	}

	return nil, ErrCantConnect
}

/*
does not cover all possible errors

func isConnErr(err error) bool {
	if err == nil {
		return false
	}
	if err == mysql.ErrInvalidConn {
		return true
	}
	if _, ok := err.(*net.OpError); ok {
		return true
	}
	return false
}
*/

func (s *rdsReadCluster) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return s.QueryContext(context.Background(), query, args...)
}

func (s *rdsReadCluster) setupTopologyTicker() {

	s.topologyUpdates = newTicker(s.opts.UpdateTopologyEvery)
	go func() {
	LOOP:
		for {
			select {
			case <-s.topologyUpdates.ticker.C:
				s.updateTopologyLogError()
				s.topologyMu.Lock()
				s.topology.ExecuteOnLeaveIfNeeded()
				s.topologyMu.Unlock()
			case <-s.topologyUpdates.stop:
				break LOOP
			}
		}

		s.opts.Log("stopped topology update goroutine")
		s.topologyUpdates.stopped <- true
	}()
}

func (s *rdsReadCluster) getDB(connURL string) (*sql.DB, error) {
	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM([]byte(mysqlRDSCACert)) {
		panic("can't add certs")
	}
	config := &tls.Config{
		ServerName: connURL,
		RootCAs:    caCertPool,
	}
	profile := hex.EncodeToString([]byte(connURL))
	err := mysql.RegisterTLSConfig(profile, config)
	if err != nil {
		panic(err)
	}

	args := url.Values{}
	args.Set("tls", profile)
	for k, v := range DefaultDriverOpts {
		args[k] = v
	}
	for k, v := range s.opts.ExtraDriverOpts {
		args[k] = v
	}
	port := strconv.Itoa(s.opts.Port)
	db, err := sql.Open("mysql", s.opts.User+":"+s.opts.Pass+"@tcp("+connURL+":"+port+")/"+s.opts.Database+"?"+args.Encode())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(s.opts.MaxConnectionsPerServer)
	return db, nil
}

const maxReplicaLastUpdateTimestampSec = 60

type querier interface {
	Query(query string, args ...interface{}) (*sql.Rows, error)
}

func (s *rdsReadCluster) retrieveTopology(db querier) ([]string, error) {
	q := `
		SELECT
			server_id,
			if(session_id = 'MASTER_SESSION_ID', 'writer', 'reader') as role
		FROM information_schema.replica_host_status
		WHERE last_update_timestamp > NOW() - ?
		HAVING role = 'reader'
	`
	rows, err := db.Query(q, maxReplicaLastUpdateTimestampSec)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	res := []string{}
	for rows.Next() {
		var serverID string
		var role string
		err := rows.Scan(&serverID, &role)
		if err != nil {
			return nil, err
		}
		res = append(res, serverID)
	}
	return res, nil
}

func (s *rdsReadCluster) updateTopologyLogError() {
	err := s.updateTopology()
	if err != nil {
		s.opts.Log("ERROR! Could not update RDSReadCluster topology, err: ", err)
	}
}

func (s *rdsReadCluster) updateTopologyInitial() error {
	s.opts.Log("initial topology db connection")
	db, err := s.getDB(s.opts.ReadEndpointURL)
	if err != nil {
		return err
	}
	defer db.Close()
	res, err := s.retrieveTopology(db)
	if err != nil {
		return err
	}
	s.topologyMu.Lock()
	defer s.topologyMu.Unlock()
	s.topology.SetAvailableFromReplicaHostStatus(res)
	return nil
}

func (s *rdsReadCluster) updateTopology() error {
	res, err := s.retrieveTopology(s)
	if err != nil {
		return err
	}
	s.topologyMu.Lock()
	defer s.topologyMu.Unlock()
	s.topology.SetAvailableFromReplicaHostStatus(res)

	return nil
}

var errNoServersAvailable = errors.New("cluster: no servers available")

func (s *rdsReadCluster) loadBalancedDB() (db *sql.DB, host string, _ error) {
	s.topologyMu.Lock()
	available := s.topology.GetAvailable()
	s.topologyMu.Unlock()

	if len(available) == 0 {
		return nil, "", errNoServersAvailable
	}

	host = available[randn(len(available))]

	s.dbsMu.Lock()
	defer s.dbsMu.Unlock()
	db, ok := s.dbs[host]
	if !ok {
		var err error

		// need to create db instance first
		s.opts.Log("creating new db instance", host)
		db, err = s.getDB(host + "." + s.opts.ClusterURLSuffix)
		if err != nil {
			return nil, "", err
		}
		s.opts.Log("created new db instance", host)
		s.dbs[host] = db
	}

	return
}

func randomDB(dbs map[string]*sql.DB) (db *sql.DB, host string) {
	i := 0
	n := randn(len(dbs))
	for host, db := range dbs {
		if i == n {
			return db, host
		}
		i++
	}
	panic("not possible")
}

func randn(maxNotInclusive int) int {
	return rand.Intn(maxNotInclusive)
}

func (s *rdsReadCluster) Close() error {
	s.topologyUpdates.Stop()
	return nil
}
