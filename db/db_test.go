package db

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDSN(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)

	db, err := sql.Open("mysql", "root:@tcp(127.0.0.1:3306)/mysql")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var dbObj = &DB{
		db:  db,
		dsn: "dsn",
	}
	assert.Equal(dbObj.dsn, dbObj.DSN())
	ok, err := dbObj.ParseDSN()
	assert.Nil(ok)
	assert.Error(err)

	assert.Equal(dbObj.db, dbObj.SQLDB())

}
func TestGetTableNames(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)

	db, err := sql.Open("mysql", "root:@tcp(127.0.0.1:3306)/mysql")
	assert.NoError(err)
	defer db.Close()

	tx, err := db.Begin()
	assert.NoError(err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	ok, err := GetTableNames(ctx, tx, "user")
	assert.Nil(err)
	assert.NotNil(ok)
}

func TestDSNMask(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	dsn := GetDSN("username", "password", "hostname", 3306, "name")
	assert.Equal("username:password@tcp(hostname:3306)/name?collation=utf8_unicode_ci&charset=utf8mb4&parseTime=true&tls=false", dsn)
	mask := MaskDSN(dsn, "password")
	assert.Equal("username:*****@tcp(hostname:3306)/name?collation=utf8_unicode_ci&charset=utf8mb4&parseTime=true&tls=false", mask)

	dsn = GetDSN("username", "", "hostname", 3306, "name")
	assert.Equal("username:@tcp(hostname:3306)/name?collation=utf8_unicode_ci&charset=utf8mb4&parseTime=true&tls=false", dsn)
	mask = MaskDSN(dsn, "")
	assert.Equal("username:@tcp(hostname:3306)/name?collation=utf8_unicode_ci&charset=utf8mb4&parseTime=true&tls=false", mask)

	dsn = GetDSN("username", "", "hostname", 0, "name")
	assert.Equal("username:@tcp(hostname:3306)/name?collation=utf8_unicode_ci&charset=utf8mb4&parseTime=true&tls=false", dsn)
}

func TestDSNMaskAdditionalAttributes(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)
	dsn := GetDSN("username", "password", "hostname", 3306, "name", "foo=bar", "bar=yes")
	assert.Equal("username:password@tcp(hostname:3306)/name?collation=utf8_unicode_ci&charset=utf8mb4&parseTime=true&foo=bar&bar=yes&tls=false", dsn)
}

func TestDSNDefaultTLS(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)
	dsn := GetDSN("username", "password", "hostname", 3306, "name")
	assert.Equal("username:password@tcp(hostname:3306)/name?collation=utf8_unicode_ci&charset=utf8mb4&parseTime=true&tls=false", dsn)
}

func TestDSNOverrideTLS(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)
	dsn := GetDSN("username", "password", "hostname", 3306, "name", "tls=true")
	assert.Equal("username:password@tcp(hostname:3306)/name?collation=utf8_unicode_ci&charset=utf8mb4&parseTime=true&tls=true", dsn)
}
