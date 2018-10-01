package cluster

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net/url"
	"strconv"
	"strings"
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

	// ExtraDriverOpts are concatenated and added to the string used to
	// initialize the driver connection. Used by underlying driver.
	// Merged to DefaultDriverOpts and tls.
	ExtraDriverOpts url.Values

	// InitialConnectionURL is the url used initially to connect to any
	// replica to retrive the current cluster state from
	// information_schema.replica_host_status table.
	//
	// Warning! Do not use Reader endpoint url as it could return a node
	// that is currently offline (at least when using certain dns configration).
	// Either use one fixed node or Cluster endpoint url returning write
	// replica. This connection is only used to retrieve the list of
	// replicas once.
	InitialConnectionURL string

	// ClusterURLSuffix is added to server_id retrieved from
	// information_schema.replica_host_status.
	// The resulting string is used to connect to specific replica.
	ClusterURLSuffix string

	// UpdateTopologyEvery specifies how often should topology be updated.
	// If zero defaultUpdateTopology (30s) is used.
	UpdateTopologyEvery time.Duration

	// MaxConnectionsPerServer sets the maximum number of open connection
	// per read replica.
	// Make sure to set a reasonable amount.
	MaxConnectionsPerServer int

	// Log outputs log of RDSReadCluster
	Log func(args ...interface{})
}

// defaultPort is used when no Port is specified in Opts.
const defaultPort = 3306

// DefaultDriverOpts are the options passed to mysql driver by default. Adjust ExtraDriverOpts if you want to add or modify.
var DefaultDriverOpts = url.Values{
	"collation":  {"utf8_unicode_ci"},
	"charset":    {"utf8mb4"},
	"parseTime":  {"true"},
	"autocommit": {"true"},
}

// defaultUpdateTopology specifies how often the client should query the sql server for current servers list
const defaultUpdateTopology = 30 * time.Second

// maxTimeLeaving is the delay after node was removed from replica_host_status or query failed and db.Close is called. Also required for long running queries to allow interrupting iterating rows.
//
// Shorter time cleans up resources faster.
// Longer time reuses db instance and connection for temporary errors.
//
// Keep shorter than failDuration to be able to test it on node restarts, otherwise it will only be executed on node deletes (it would still work, but better to test more often to avoid bugs in code).
const maxTimeLeaving = 3 * 60 * time.Second

// failDuration is the duration the node is marked as failed after failing query. The node is ignored for this duration.
//
// Shorter time re-connects faster for restarts and temporary errors.
// Longer time avoids unnecessary re-tries for shutdowns.
//
// It takes about ~4 min to a host to be removed  from information_schema.replica_host_status after shutdown and first failed query.
const failDuration = 4 * 60 * time.Second

// RDSReadCluster provides Query methods that are load balanced.
type RDSReadCluster interface {

	// QueryContext executes a query that returns rows, typically a SELECT.
	// Does automatic load balancing and retries on broken connection.
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)

	// Query executes a query that returns rows, typically a SELECT.
	// Does automatic load balancing and retries on broken connection.
	Query(query string, args ...interface{}) (*sql.Rows, error)

	// QueryRowContext executes a query that is expected to return at most one row.
	// Does automatic load balancing and retries on broken connection.
	QueryRowContext(ctx context.Context, query string, args ...interface{}) Row

	// QueryRow executes a query that is expected to return at most one row.
	// Does automatic load balancing and retries on broken connection.
	QueryRow(query string, args ...interface{}) Row

	// Close frees all resources. Make sure to complete all queries before calling Close.
	Close() error

	// GetDB returns the underlying DB instance and should only be used unwillingly ;)
	GetDB() *sql.DB
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

var _ RDSReadCluster = (*rdsReadCluster)(nil)

// New creates a new rdsReadCluster
func New(opts Opts) *rdsReadCluster {
	if opts.Port == 0 {
		opts.Port = defaultPort
	}
	if opts.UpdateTopologyEvery == 0 {
		opts.UpdateTopologyEvery = defaultUpdateTopology
	}
	if opts.User == "" || opts.Pass == "" || opts.Port == 0 || opts.Database == "" || opts.InitialConnectionURL == "" || opts.ClusterURLSuffix == "" || opts.UpdateTopologyEvery == 0 || opts.MaxConnectionsPerServer == 0 || opts.Log == nil {
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

// MaxServersTriedForQuery is the max number of servers tried for a query. The actual number of retries is +1 since it tries to connect to initial instance twice.
const MaxServersTriedForQuery = 3

// ErrConnectMaxRetriesExceeded is returned from Query when query tried more than MaxServersTriedForQuery servers, and all of them failed. There could be more available instances.
var ErrConnectMaxRetriesExceeded = errors.New("cluster: tried MaxServersTriedForQuery servers to connect, all failed")

// ErrNoServersAvailable is returned from Query when no servers are available. All servers returned from information_schema.replica_host_status failed.
var ErrNoServersAvailable = errors.New("cluster: no servers available")

func (s *rdsReadCluster) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {

	var db *sql.DB
	host := ""

	// connects to random db first
	// on getting an error, retries connection to the same db without marking it as failed
	//     not required for load balancing, but increases availability in my tests (probably because of unstable internet connection)
	// for other retries, marks as failed immediately
	// repeats up to MaxServersTriedForQuery times

	for i := 0; i < MaxServersTriedForQuery+1; i++ {

		if i != 1 {
			// on a second retry use the same database connection
			var err error
			db, host, err = s.loadBalancedDB()
			if err != nil {
				return nil, err
			}
		}

		if i > 0 {
			s.opts.Log("query retry", i)
		}

		rows, err := db.QueryContext(ctx, query, args...)

		if err != nil {
			// on syntax errors and similar errors returned by database directly do not retry and return immediately
			if _, ok := err.(*mysql.MySQLError); ok {
				return nil, err
			}

			if i == 0 {
				// do not mark the server as failed on the first try
				// retry once to the same database connection
				continue
			}

			//if isConnErr(err) { does not cover all possible errors
			s.topologyMu.Lock()
			s.topology.MarkFailed(host)
			s.topologyMu.Unlock()

			continue
		}

		if i == 1 {
			s.opts.Log("query retry to the same server fixed the connection problem (check network status and server configuration)")
		}

		return rows, nil
	}

	return nil, ErrConnectMaxRetriesExceeded
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

type querier interface {
	Query(query string, args ...interface{}) (*sql.Rows, error)
}

const maxReplicaLagMs = 100

func (s *rdsReadCluster) retrieveTopology(db querier) ([]string, error) {
	q := `
		SELECT
			server_id,
			if(session_id = 'MASTER_SESSION_ID', 'writer', 'reader') as role
		FROM information_schema.replica_host_status
		WHERE replica_lag_in_milliseconds < ?
		HAVING role = 'reader'
	`
	rows, err := db.Query(q, maxReplicaLagMs)
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
	db, err := s.getDB(s.opts.InitialConnectionURL)
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

func (s *rdsReadCluster) loadBalancedDB() (db *sql.DB, host string, _ error) {
	s.topologyMu.Lock()
	available := s.topology.GetAvailable()
	s.topologyMu.Unlock()

	if len(available) == 0 {

		return nil, "", ErrNoServersAvailable
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

func (s *rdsReadCluster) GetDB() *sql.DB {
	for {
		db, _, err := s.loadBalancedDB()
		if err != nil {
			time.Sleep(time.Microsecond * 10)
		}
		return db
	}
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

// SQLDB is for fetching the underlying DB instance
type SQLDB interface {
	// GetDB returns the underlying DB instance and should only be used unwillingly ;)
	GetDB() *sql.DB
}

// RDSWriteCluster is an interface to a write cluster (master)
type RDSWriteCluster interface {
	// ExecContext executes a query without returning any rows.
	// The args are for any placeholder parameters in the query.
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)

	// QueryContext executes a query that returns rows, typically a SELECT.
	// The args are for any placeholder parameters in the query.
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)

	// QueryRowContext executes a query that is expected to return at most one row.
	// QueryRowContext always returns a non-nil value. Errors are deferred until
	// Row's Scan method is called.
	// If the query selects no rows, the *Row's Scan will return ErrNoRows.
	// Otherwise, the *Row's Scan scans the first selected row and discards
	// the rest.
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row

	// Begin starts a transaction. The default isolation level is dependent on the driver.
	Begin() (*sql.Tx, error)

	// BeginTx starts a transaction.
	//
	// The provided context is used until the transaction is committed or rolled back.
	// If the context is canceled, the sql package will roll back
	// the transaction. Tx.Commit will return an error if the context provided to
	// BeginTx is canceled.
	//
	// The provided TxOptions is optional and may be nil if defaults should be used.
	// If a non-default isolation level is used that the driver doesn't support,
	// an error will be returned.
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)

	// Close frees all resources. Make sure to complete all queries before calling Close.
	Close() error
}

type rdsWriteCluster struct {
	db *sql.DB
}

var _ RDSWriteCluster = (*rdsWriteCluster)(nil)

// NewMaster creates a new rdsWriteCluster
func NewMaster(db *sql.DB) *rdsWriteCluster {
	return &rdsWriteCluster{db}
}

// MaxRetries is the max number of times that a deadlock query will be retried
var MaxRetries = 3

// IsRetryable returns true if the sql error passed in a retryable error
func IsRetryable(err error) bool {
	if err != nil {
		if strings.Contains(err.Error(), "Error 1213: Deadlock found when trying to get lock") {
			// this is a retryable query
			return true
		}
	}
	return false
}

func exponentialBackoff(retryCount int) {
	durf := time.Millisecond * time.Duration(math.Pow(2, float64(retryCount)))
	time.Sleep(durf)
}

func (c *rdsWriteCluster) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	var retryCount int
	for retryCount < MaxRetries {
		r, err := c.db.ExecContext(ctx, query, args...)
		if IsRetryable(err) {
			retryCount++
			exponentialBackoff(retryCount)
			continue
		}
		return r, err
	}
	return nil, fmt.Errorf("timed out trying to retry query")
}

func (c *rdsWriteCluster) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	var retryCount int
	for retryCount < MaxRetries {
		r, err := c.db.QueryContext(ctx, query, args...)
		if IsRetryable(err) {
			retryCount++
			exponentialBackoff(retryCount)
			continue
		}
		return r, err
	}
	return nil, fmt.Errorf("timed out trying to retry query")
}

func (c *rdsWriteCluster) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return c.db.QueryRowContext(ctx, query, args...)
}

func (c *rdsWriteCluster) Begin() (*sql.Tx, error) {
	return c.db.Begin()
}

func (c *rdsWriteCluster) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return c.db.BeginTx(ctx, opts)
}

func (c *rdsWriteCluster) Close() error {
	return c.db.Close()
}

func (c *rdsWriteCluster) GetDB() *sql.DB {
	return c.db
}
