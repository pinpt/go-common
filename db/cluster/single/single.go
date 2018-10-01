// Package single provides cluster interface by using a single instance *sql.DB. Useful for development, tests and similar.
package single

import (
	"context"
	"database/sql"

	"github.com/pinpt/go-common/db/cluster"
)

type single struct {
	driver *sql.DB
}

var _ cluster.RDSReadCluster = (*single)(nil)

func New(driver *sql.DB) *single {
	return &single{
		driver: driver,
	}
}

func (s *single) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return s.driver.QueryContext(ctx, query, args...)
}

func (s *single) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return s.driver.Query(query, args...)
}

func (s *single) QueryRowContext(ctx context.Context, query string, args ...interface{}) cluster.Row {
	rows, err := s.QueryContext(ctx, query, args...)
	return cluster.RowFromQueryRes(rows, err)
}

func (s *single) QueryRow(query string, args ...interface{}) cluster.Row {
	return s.QueryRowContext(context.Background(), query, args...)
}

func (s *single) Close() error {
	return s.driver.Close()
}

func (s *single) GetDB() *sql.DB {
	return s.driver
}
