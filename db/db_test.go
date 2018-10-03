package db

import (
	"context"
	"database/sql"
	"net/url"
	"os"
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
	if os.Getenv("CI") == "" {
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
}

func TestDSNMask(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	dsn := GetDSN("username", "password", "hostname", 3306, "name")
	assert.Equal("username:password@tcp(hostname:3306)/name?collation=utf8_unicode_ci&charset=utf8mb4&parseTime=true&tls=false&autocommit=true", dsn)
	mask := MaskDSN(dsn, "password")
	assert.Equal("username:*****@tcp(hostname:3306)/name?collation=utf8_unicode_ci&charset=utf8mb4&parseTime=true&tls=false&autocommit=true", mask)

	dsn = GetDSN("username", "", "hostname", 3306, "name")
	assert.Equal("username:@tcp(hostname:3306)/name?collation=utf8_unicode_ci&charset=utf8mb4&parseTime=true&tls=false&autocommit=true", dsn)
	mask = MaskDSN(dsn, "")
	assert.Equal("username:@tcp(hostname:3306)/name?collation=utf8_unicode_ci&charset=utf8mb4&parseTime=true&tls=false&autocommit=true", mask)

	dsn = GetDSN("username", "", "hostname", 0, "name")
	assert.Equal("username:@tcp(hostname:3306)/name?collation=utf8_unicode_ci&charset=utf8mb4&parseTime=true&tls=false&autocommit=true", dsn)
}

func TestDSNMaskAdditionalAttributes(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)
	dsn := GetDSN("username", "password", "hostname", 3306, "name", "foo=bar", "bar=yes")
	assert.Equal("username:password@tcp(hostname:3306)/name?collation=utf8_unicode_ci&charset=utf8mb4&parseTime=true&foo=bar&bar=yes&tls=false&autocommit=true", dsn)
}

func TestDSNDefaultTLS(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)
	dsn := GetDSN("username", "password", "hostname", 3306, "name")
	assert.Equal("username:password@tcp(hostname:3306)/name?collation=utf8_unicode_ci&charset=utf8mb4&parseTime=true&tls=false&autocommit=true", dsn)
}

func TestDSNOverrideTLS(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)
	dsn := GetDSN("username", "password", "hostname", 3306, "name", "tls=true")
	assert.Equal("username:password@tcp(hostname:3306)/name?collation=utf8_unicode_ci&charset=utf8mb4&parseTime=true&tls=true&autocommit=true", dsn)
	dsn = GetDSN("username", "password", "hostname", 3306, "name", "autocommit=false")
	assert.Equal("username:password@tcp(hostname:3306)/name?collation=utf8_unicode_ci&charset=utf8mb4&parseTime=true&autocommit=false&tls=false", dsn)
}

func TestDSNEscapeUsername(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)
	dsn := GetDSN("hi mom", "xZ{G{V?X-R:y%l", "hostname", 3306, "name", "tls=true")
	assert.Equal("hi%20mom:xZ%7BG%7BV%3FX-R%3Ay%25l@tcp(hostname:3306)/name?collation=utf8_unicode_ci&charset=utf8mb4&parseTime=true&tls=true&autocommit=true", dsn)
}

type testTest string

func TestSQLJoin(t *testing.T) {
	assert := assert.New(t)
	assert.Equal("'1','2'", SQLJoin([]string{"1", "2"}))
	assert.Equal("'1','2'", SQLJoin([]int{1, 2}))
	assert.Equal("'1','2'", SQLJoin([]testTest{testTest("1"), testTest("2")}))
}

func TestFormatSQL(t *testing.T) {
	assert := assert.New(t)
	assert.Equal("SELECT * FROM foo", FormatSQL("SELECT   * FROM foo"))
	assert.Equal("SELECT * FROM foo", FormatSQL("SELECT\n* FROM foo"))
	assert.Equal("SELECT * FROM foo", FormatSQL("SELECT\t* FROM foo"))
	assert.Equal("SELECT * FROM foo", FormatSQL("SELECT * FROM foo\n"))
	assert.Equal("SELECT * FROM foo", FormatSQL(`
		SELECT * FROM foo
`))
	assert.Equal("SELECT * FROM foo where foo=1", FormatSQL("SELECT * FROM foo where foo = 1"))
	assert.Equal("SELECT * FROM foo where foo>=1", FormatSQL("SELECT * FROM foo where foo >= 1"))
	assert.Equal("SELECT * FROM foo where foo=(SELECT id from bar)", FormatSQL("SELECT * FROM foo where foo = ( SELECT id from bar )"))
	assert.Equal("SELECT * FROM foo GROUP BY foo,bar", FormatSQL("SELECT * FROM foo GROUP BY foo, bar"))
	assert.Equal("SELECT * FROM foo where bar!=1", FormatSQL("SELECT * FROM foo where bar != 1"))
	assert.Equal("SELECT * FROM foo where bar<>1", FormatSQL("SELECT * FROM foo where bar <> 1"))
}

func TestGetClusterDSN(t *testing.T) {
	assert := assert.New(t)
	uv := make(url.Values)
	uv.Add("hi", "mom")
	dsn := GetClusterDSN("foo", "xZ{G{V?X-R:y%l", "hostname", 3306, "bar", uv)
	assert.Equal("//foo:xZ%7BG%7BV%3FX-R%3Ay%25l@hostname:3306/bar?hi=mom", dsn)
	u, err := url.Parse(dsn)
	assert.NoError(err)
	pass, ok := u.User.Password()
	assert.True(ok)
	assert.Equal("xZ{G{V?X-R:y%l", pass)
}
