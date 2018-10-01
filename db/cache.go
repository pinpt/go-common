package db

import (
	"database/sql"
)

// CacheRows is a container to store the values from sql.Rows
type CacheRows struct {
	rows  [][]interface{}
	index int
	err   error
}

// Reset resets the index to 0 - must be called before iteration
func (c *CacheRows) Reset() {
	c.index = 0
}

// Next iterator
func (c *CacheRows) Next() bool {
	c.index++
	return c.index <= len(c.rows)
}

// Err returns the error, if any, encountered in the iteration
func (c *CacheRows) Err() error {
	return c.err
}

// Close does nothing, but it's here for backawards compatibility with sql.Rows
func (c *CacheRows) Close() error {
	return c.err
}

// Scan scanner - similar to sql.Rows.Scan, but you must pass in a Scanner type such as: sql.NullString
func (c *CacheRows) Scan(dest ...sql.Scanner) error {
	row := c.rows[c.index-1]
	for i, scanner := range dest {
		err := scanner.Scan(row[i])
		if err != nil {
			c.err = err
			return err
		}
	}
	return nil
}

// Fill fill with rows
func (c *CacheRows) Fill(rows *sql.Rows) error {

	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	count := len(columns)
	values := make([]interface{}, count)
	scanArgs := make([]interface{}, count)
	for i := range values {
		scanArgs[i] = &values[i]
	}

	c.rows = make([][]interface{}, 0)
	for rows.Next() {
		err := rows.Scan(scanArgs...)
		if err != nil {
			return err
		}
		each := make([]interface{}, 0)
		for _, v := range values {
			each = append(each, v)
		}
		c.rows = append(c.rows, each)
	}
	return nil
}

// RowCount row count
func (c *CacheRows) RowCount() int {
	return len(c.rows)
}

// Index current index
func (c *CacheRows) Index() int {
	return c.index
}
