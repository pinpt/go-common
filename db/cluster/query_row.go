package cluster

import (
	"context"
	"database/sql"
	"errors"
)

// QueryRowContext executes a query that is expected to return at most one row.
func (s *rdsReadCluster) QueryRowContext(ctx context.Context, query string, args ...interface{}) Row {
	rows, err := s.QueryContext(ctx, query, args...)
	return RowFromQueryRes(rows, err)
}

// QueryRow executes a query that is expected to return at most one row.
func (s *rdsReadCluster) QueryRow(query string, args ...interface{}) Row {
	return s.QueryRowContext(context.Background(), query, args...)
}

// Row is the result of calling QueryRow to select a single row.
type Row interface {
	Scan(dest ...interface{}) error
}

// Below is copied from golang/database/sql/sql.go

// row is the result of calling QueryRow to select a single row.
type row struct {
	// One of these two will be non-nil:
	err  error // deferred error for easy chaining
	rows *sql.Rows
}

// RowFromQueryRes creates a Row similar to *sql.Row returned from QueryRow based on query rows and query error.
func RowFromQueryRes(rows *sql.Rows, queryErr error) Row {
	return &row{rows: rows, err: queryErr}
}

// Scan copies the columns from the matched row into the values
// pointed at by dest. See the documentation on Rows.Scan for details.
// If more than one row matches the query,
// Scan uses the first row and discards the rest. If no row matches
// the query, Scan returns ErrNoRows.
func (r *row) Scan(dest ...interface{}) error {
	if r.err != nil {
		return r.err
	}

	defer r.rows.Close()
	for _, dp := range dest {
		if _, ok := dp.(*sql.RawBytes); ok {
			return errors.New("sql: RawBytes isn't allowed on Row.Scan")
		}
	}

	if !r.rows.Next() {
		if err := r.rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}
	err := r.rows.Scan(dest...)
	if err != nil {
		return err
	}
	// Make sure the query can be processed to completion with no errors.
	if err := r.rows.Close(); err != nil {
		return err
	}

	return nil
}
