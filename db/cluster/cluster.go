package cluster

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/hex"
	"errors"
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

	// UpdateTopologyEvery specifies how often should topology be updated. If zero 30s is used.
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
const defaultUpdateTopology = 5 * time.Second

// RDSReadCluster provides db instances on demand. In comparison to default sql.DB with go-sql-driver/mysql it load balances across all read replicas.
type RDSReadCluster interface {
	// DB retrieves the DB instance to use for next read request. Do not store or re-use.
	DB() (*sql.DB, error)

	// DBCtx is the same as DB, but provides cancellation capabilities.
	//DBCtx(context.Context) (*sql.DB, error)

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
	dbs map[string]*sql.DB
	// db move to dbsInvalid when they no longer in topology. We wait 1 min before closing to allow any existing queries to finish.
	//dbsInvalid []dbInvalid
	// dbsMu for dbs, dbsInvalid
	dbsMu sync.Mutex

	topologyDB *sql.DB
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
			defer s.dbsMu.Lock()
			s.dbs[id].Close()
			delete(s.dbs, id)
		},
		Log: s.opts.Log,
	})

	s.updateTopologyLogError()
	s.setupTopologyTicker()
	return s
}

const maxTimeLeaving = 60 * time.Second

func (s *rdsReadCluster) setupTopologyTicker() {

	s.topologyUpdates = newTicker(s.opts.UpdateTopologyEvery)
	go func() {
	LOOP:
		for {
			select {
			case <-s.topologyUpdates.ticker.C:
				s.updateTopologyLogError()
				s.topology.Tick(time.Now())
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

const maxReplicaLastUpdateTimestampSec = 30

func (s *rdsReadCluster) getTopologyDB() (*sql.DB, error) {
	if s.topologyDB != nil {
		return s.topologyDB, nil
	}
	s.opts.Log("connecting to new topology db")
	db, err := s.getDB(s.opts.ReadEndpointURL)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	s.topologyDB = db
	return db, nil
}

func (s *rdsReadCluster) retrieveTopology() ([]string, error) {
	db, err := s.getTopologyDB()
	if err != nil {
		return nil, err
	}

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

func (s *rdsReadCluster) updateTopology() error {
	res, err := s.retrieveTopology()
	if err != nil {
		return err
	}

	s.topologyMu.Lock()
	defer s.topologyMu.Unlock()

	s.topology.Update(time.Now(), res)

	return nil
}

func (s *rdsReadCluster) DB() (*sql.DB, error) {
	s.dbsMu.Lock()
	defer s.dbsMu.Unlock()
	s.topologyMu.Lock()
	defer s.topologyMu.Unlock()

	notConnected := []string{}
	for host := range s.topology.Available {
		_, connected := s.dbs[host]
		if !connected {
			notConnected = append(notConnected, host)
		}
	}

	if len(notConnected) == 0 {
		// all connected
		// return random db
	} else {
		// connect a new db
		host := notConnected[randn(len(notConnected))]
		db, err := s.getDB(host + "." + s.opts.ClusterURLSuffix)
		if err != nil {
			return nil, err
		}
		s.opts.Log("connected to a new db", host)
		s.dbs[host] = db
		return db, nil
	}

	if len(s.dbs) == 0 {
		return nil, errors.New("no servers available")
	}

	return randomDB(s.dbs), nil
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

func randn(maxNotInclusive int) int {
	return rand.Intn(maxNotInclusive)
}

func (s *rdsReadCluster) Close() error {
	s.topologyUpdates.Stop()
	return nil
}
