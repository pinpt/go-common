package db

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pinpt/go-common/json"
	"github.com/pinpt/go-common/log"
)

// DB wraps a sql.DB to remember the DSN setting
type DB struct {
	*sql.DB
	db  *sql.DB
	dsn string
}

// DSN will return the DSN string for this database
func (d *DB) DSN() string {
	return d.dsn
}

// ParseDSN returns the parsed DSDN into a mysql.Config
func (d *DB) ParseDSN() (*mysql.Config, error) {
	return mysql.ParseDSN(d.dsn)
}

// SQLDB returns the *sql.DB
func (d *DB) SQLDB() *sql.DB {
	return d.db
}

var dbreg map[string]bool

func init() {
	dbreg = make(map[string]bool)
}

// RegisterDBTLSConfig will register a specific tls config with a given name
// related to this bug: https://github.com/go-sql-driver/mysql/pull/564
func RegisterDBTLSConfig(name string, config *tls.Config) error {
	return mysql.RegisterTLSConfig(name, config)
}

// GetDSN returns a DSN string for use with opening a sql database
func GetDSN(username string, password string, hostname string, port int, name string, attrs ...string) string {
	if port == 0 {
		port = 3306
	}
	cfg := mysql.NewConfig()
	cfg.Net = "tcp"
	cfg.Addr = fmt.Sprintf("%s:%d", hostname, port)
	cfg.User = username
	cfg.Passwd = password
	cfg.DBName = name
	s := "?collation=utf8mb4_unicode_ci&parseTime=true"
	l := len(attrs)
	addac := true
	if l > 0 {
		s += "&"
		for i, attr := range attrs {
			if strings.HasPrefix(attr, "autocommit=") {
				addac = false
			}
			s += attr
			if i+1 < l {
				s += "&"
			}
		}
	}
	if addac {
		s = s + "&autocommit=true"
	}
	return cfg.FormatDSN() + s
}

// MaskDSN will return a masked DSN string to remove the password
func MaskDSN(dsn string, password string) string {
	if password == "" {
		return dsn
	}
	return strings.Replace(dsn, password, "*****", -1)
}

// OpenDB will open a sql database and return the *sql.DB
func OpenDB(username string, password string, hostname string, port int, name string, attrs ...string) (*DB, error) {
	// convenience method to correctly handle AWS RDS SSL profile
	if strings.Contains(hostname, "rds.amazonaws.com") {
		profile := "awsrds_" + name
		reg := dbreg[profile]
		if !reg {
			caCertPool := x509.NewCertPool()
			if !caCertPool.AppendCertsFromPEM([]byte(mysqlRDSCACert)) {
				return nil, errors.New("error adding Amazon CA certificates to pool")
			}
			config := &tls.Config{
				ServerName: hostname,
				RootCAs:    caCertPool,
			}
			RegisterDBTLSConfig(profile, config)
			dbreg[profile] = true
		}
		attrs = append(attrs, "tls="+profile)
	}
	dsn := GetDSN(username, password, hostname, port, name, attrs...)
	maskedDsn := MaskDSN(dsn, password)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("error opening database %s. %v", maskedDsn, err)
	}
	db.SetMaxOpenConns(10) // set a reasonable default which can then be overriden by the caller
	return &DB{db, db, dsn}, nil
}

// GetCurrentDB returns the current database name for the pending transaction
func GetCurrentDB(ctx context.Context, tx *sql.Tx) (string, error) {
	var name string
	err := tx.QueryRowContext(ctx, "select database()").Scan(&name)
	return name, err
}

// GetTableNames returns the table names that have a specified column in their schema definition
func GetTableNames(ctx context.Context, tx *sql.Tx, column string) ([]string, error) {
	dbname, err := GetCurrentDB(ctx, tx)
	if err != nil {
		return nil, fmt.Errorf("error finding the current database name. %v", err)
	}
	rows, err := tx.QueryContext(ctx, `select distinct(table_name) from information_schema.columns where column_name = ? and table_schema = ?`, column, dbname)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	names := make([]string, 0)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, nil
}

// amazon RDS public certs
const mysqlRDSCACert = `-----BEGIN CERTIFICATE-----
MIID9DCCAtygAwIBAgIBQjANBgkqhkiG9w0BAQUFADCBijELMAkGA1UEBhMCVVMx
EzARBgNVBAgMCldhc2hpbmd0b24xEDAOBgNVBAcMB1NlYXR0bGUxIjAgBgNVBAoM
GUFtYXpvbiBXZWIgU2VydmljZXMsIEluYy4xEzARBgNVBAsMCkFtYXpvbiBSRFMx
GzAZBgNVBAMMEkFtYXpvbiBSRFMgUm9vdCBDQTAeFw0xNTAyMDUwOTExMzFaFw0y
MDAzMDUwOTExMzFaMIGKMQswCQYDVQQGEwJVUzETMBEGA1UECAwKV2FzaGluZ3Rv
bjEQMA4GA1UEBwwHU2VhdHRsZTEiMCAGA1UECgwZQW1hem9uIFdlYiBTZXJ2aWNl
cywgSW5jLjETMBEGA1UECwwKQW1hem9uIFJEUzEbMBkGA1UEAwwSQW1hem9uIFJE
UyBSb290IENBMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAuD8nrZ8V
u+VA8yVlUipCZIKPTDcOILYpUe8Tct0YeQQr0uyl018StdBsa3CjBgvwpDRq1HgF
Ji2N3+39+shCNspQeE6aYU+BHXhKhIIStt3r7gl/4NqYiDDMWKHxHq0nsGDFfArf
AOcjZdJagOMqb3fF46flc8k2E7THTm9Sz4L7RY1WdABMuurpICLFE3oHcGdapOb9
T53pQR+xpHW9atkcf3pf7gbO0rlKVSIoUenBlZipUlp1VZl/OD/E+TtRhDDNdI2J
P/DSMM3aEsq6ZQkfbz/Ilml+Lx3tJYXUDmp+ZjzMPLk/+3beT8EhrwtcG3VPpvwp
BIOqsqVVTvw/CwIDAQABo2MwYTAOBgNVHQ8BAf8EBAMCAQYwDwYDVR0TAQH/BAUw
AwEB/zAdBgNVHQ4EFgQUTgLurD72FchM7Sz1BcGPnIQISYMwHwYDVR0jBBgwFoAU
TgLurD72FchM7Sz1BcGPnIQISYMwDQYJKoZIhvcNAQEFBQADggEBAHZcgIio8pAm
MjHD5cl6wKjXxScXKtXygWH2BoDMYBJF9yfyKO2jEFxYKbHePpnXB1R04zJSWAw5
2EUuDI1pSBh9BA82/5PkuNlNeSTB3dXDD2PEPdzVWbSKvUB8ZdooV+2vngL0Zm4r
47QPyd18yPHrRIbtBtHR/6CwKevLZ394zgExqhnekYKIqqEX41xsUV0Gm6x4vpjf
2u6O/+YE2U+qyyxHE5Wd5oqde0oo9UUpFETJPVb6Q2cEeQib8PBAyi0i6KnF+kIV
A9dY7IHSubtCK/i8wxMVqfd5GtbA8mmpeJFwnDvm9rBEsHybl08qlax9syEwsUYr
/40NawZfTUU=
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIIEATCCAumgAwIBAgIBRDANBgkqhkiG9w0BAQUFADCBijELMAkGA1UEBhMCVVMx
EzARBgNVBAgMCldhc2hpbmd0b24xEDAOBgNVBAcMB1NlYXR0bGUxIjAgBgNVBAoM
GUFtYXpvbiBXZWIgU2VydmljZXMsIEluYy4xEzARBgNVBAsMCkFtYXpvbiBSRFMx
GzAZBgNVBAMMEkFtYXpvbiBSRFMgUm9vdCBDQTAeFw0xNTAyMDUyMjAzMDZaFw0y
MDAzMDUyMjAzMDZaMIGUMQswCQYDVQQGEwJVUzETMBEGA1UECAwKV2FzaGluZ3Rv
bjEQMA4GA1UEBwwHU2VhdHRsZTEiMCAGA1UECgwZQW1hem9uIFdlYiBTZXJ2aWNl
cywgSW5jLjETMBEGA1UECwwKQW1hem9uIFJEUzElMCMGA1UEAwwcQW1hem9uIFJE
UyBhcC1ub3J0aGVhc3QtMSBDQTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoC
ggEBAMmM2B4PfTXCZjbZMWiDPyxvk/eeNwIRJAhfzesiGUiLozX6CRy3rwC1ZOPV
AcQf0LB+O8wY88C/cV+d4Q2nBDmnk+Vx7o2MyMh343r5rR3Na+4izd89tkQVt0WW
vO21KRH5i8EuBjinboOwAwu6IJ+HyiQiM0VjgjrmEr/YzFPL8MgHD/YUHehqjACn
C0+B7/gu7W4qJzBL2DOf7ub2qszGtwPE+qQzkCRDwE1A4AJmVE++/FLH2Zx78Egg
fV1sUxPtYgjGH76VyyO6GNKM6rAUMD/q5mnPASQVIXgKbupr618bnH+SWHFjBqZq
HvDGPMtiiWII41EmGUypyt5AbysCAwEAAaNmMGQwDgYDVR0PAQH/BAQDAgEGMBIG
A1UdEwEB/wQIMAYBAf8CAQAwHQYDVR0OBBYEFIiKM0Q6n1K4EmLxs3ZXxINbwEwR
MB8GA1UdIwQYMBaAFE4C7qw+9hXITO0s9QXBj5yECEmDMA0GCSqGSIb3DQEBBQUA
A4IBAQBezGbE9Rw/k2e25iGjj5n8r+M3dlye8ORfCE/dijHtxqAKasXHgKX8I9Tw
JkBiGWiuzqn7gO5MJ0nMMro1+gq29qjZnYX1pDHPgsRjUX8R+juRhgJ3JSHijRbf
4qNJrnwga7pj94MhcLq9u0f6dxH6dXbyMv21T4TZMTmcFduf1KgaiVx1PEyJjC6r
M+Ru+A0eM+jJ7uCjUoZKcpX8xkj4nmSnz9NMPog3wdOSB9cAW7XIc5mHa656wr7I
WJxVcYNHTXIjCcng2zMKd1aCcl2KSFfy56sRfT7J5Wp69QSr+jq8KM55gw8uqAwi
VPrXn2899T1rcTtFYFP16WXjGuc0
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIIEATCCAumgAwIBAgIBRTANBgkqhkiG9w0BAQUFADCBijELMAkGA1UEBhMCVVMx
EzARBgNVBAgMCldhc2hpbmd0b24xEDAOBgNVBAcMB1NlYXR0bGUxIjAgBgNVBAoM
GUFtYXpvbiBXZWIgU2VydmljZXMsIEluYy4xEzARBgNVBAsMCkFtYXpvbiBSRFMx
GzAZBgNVBAMMEkFtYXpvbiBSRFMgUm9vdCBDQTAeFw0xNTAyMDUyMjAzMTlaFw0y
MDAzMDUyMjAzMTlaMIGUMQswCQYDVQQGEwJVUzETMBEGA1UECAwKV2FzaGluZ3Rv
bjEQMA4GA1UEBwwHU2VhdHRsZTEiMCAGA1UECgwZQW1hem9uIFdlYiBTZXJ2aWNl
cywgSW5jLjETMBEGA1UECwwKQW1hem9uIFJEUzElMCMGA1UEAwwcQW1hem9uIFJE
UyBhcC1zb3V0aGVhc3QtMSBDQTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoC
ggEBANaXElmSEYt/UtxHFsARFhSUahTf1KNJzR0Dmay6hqOXQuRVbKRwPd19u5vx
DdF1sLT7D69IK3VDnUiQScaCv2Dpu9foZt+rLx+cpx1qiQd1UHrvqq8xPzQOqCdC
RFStq6yVYZ69yfpfoI67AjclMOjl2Vph3ftVnqP0IgVKZdzeC7fd+umGgR9xY0Qr
Ubhd/lWdsbNvzK3f1TPWcfIKQnpvSt85PIEDJir6/nuJUKMtmJRwTymJf0i+JZ4x
7dJa341p2kHKcHMgOPW7nJQklGBA70ytjUV6/qebS3yIugr/28mwReflg3TJzVDl
EOvi6pqbqNbkMuEwGDCmEQIVqgkCAwEAAaNmMGQwDgYDVR0PAQH/BAQDAgEGMBIG
A1UdEwEB/wQIMAYBAf8CAQAwHQYDVR0OBBYEFAu93/4k5xbWOsgdCdn+/KdiRuit
MB8GA1UdIwQYMBaAFE4C7qw+9hXITO0s9QXBj5yECEmDMA0GCSqGSIb3DQEBBQUA
A4IBAQBlcjSyscpPjf5+MgzMuAsCxByqUt+WFspwcMCpwdaBeHOPSQrXNqX2Sk6P
kth6oCivA64trWo8tFMvPYlUA1FYVD5WpN0kCK+P5pD4KHlaDsXhuhClJzp/OP8t
pOyUr5109RHLxqoKB5J5m1XA7rgcFjnMxwBSWFe3/4uMk/+4T53YfCVXuc6QV3i7
I/2LAJwFf//pTtt6fZenYfCsahnr2nvrNRNyAxcfvGZ/4Opn/mJtR6R/AjvQZHiR
bkRNKF2GW0ueK5W4FkZVZVhhX9xh1Aj2Ollb+lbOqADaVj+AT3PoJPZ3MPQHKCXm
xwG0LOLlRr/TfD6li1AfOVTAJXv9
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIIEATCCAumgAwIBAgIBRjANBgkqhkiG9w0BAQUFADCBijELMAkGA1UEBhMCVVMx
EzARBgNVBAgMCldhc2hpbmd0b24xEDAOBgNVBAcMB1NlYXR0bGUxIjAgBgNVBAoM
GUFtYXpvbiBXZWIgU2VydmljZXMsIEluYy4xEzARBgNVBAsMCkFtYXpvbiBSRFMx
GzAZBgNVBAMMEkFtYXpvbiBSRFMgUm9vdCBDQTAeFw0xNTAyMDUyMjAzMjRaFw0y
MDAzMDUyMjAzMjRaMIGUMQswCQYDVQQGEwJVUzETMBEGA1UECAwKV2FzaGluZ3Rv
bjEQMA4GA1UEBwwHU2VhdHRsZTEiMCAGA1UECgwZQW1hem9uIFdlYiBTZXJ2aWNl
cywgSW5jLjETMBEGA1UECwwKQW1hem9uIFJEUzElMCMGA1UEAwwcQW1hem9uIFJE
UyBhcC1zb3V0aGVhc3QtMiBDQTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoC
ggEBAJqBAJutz69hFOh3BtLHZTbwE8eejGGKayn9hu98YMDPzWzGXWCmW+ZYWELA
cY3cNWNF8K4FqKXFr2ssorBYim1UtYFX8yhydT2hMD5zgQ2sCGUpuidijuPA6zaq
Z3tdhVR94f0q8mpwpv2zqR9PcqaGDx2VR1x773FupRPRo7mEW1vC3IptHCQlP/zE
7jQiLl28bDIH2567xg7e7E9WnZToRnhlYdTaDaJsHTzi5mwILi4cihSok7Shv/ME
hnukvxeSPUpaVtFaBhfBqq055ePq9I+Ns4KGreTKMhU0O9fkkaBaBmPaFgmeX/XO
n2AX7gMouo3mtv34iDTZ0h6YCGkCAwEAAaNmMGQwDgYDVR0PAQH/BAQDAgEGMBIG
A1UdEwEB/wQIMAYBAf8CAQAwHQYDVR0OBBYEFIlQnY0KHYWn1jYumSdJYfwj/Nfw
MB8GA1UdIwQYMBaAFE4C7qw+9hXITO0s9QXBj5yECEmDMA0GCSqGSIb3DQEBBQUA
A4IBAQA0wVU6/l41cTzHc4azc4CDYY2Wd90DFWiH9C/mw0SgToYfCJ/5Cfi0NT/Y
PRnk3GchychCJgoPA/k9d0//IhYEAIiIDjyFVgjbTkKV3sh4RbdldKVOUB9kumz/
ZpShplsGt3z4QQiVnKfrAgqxWDjR0I0pQKkxXa6Sjkicos9LQxVtJ0XA4ieG1E7z
zJr+6t80wmzxvkInSaWP3xNJK9azVRTrgQZQlvkbpDbExl4mNTG66VD3bAp6t3Wa
B49//uDdfZmPkqqbX+hsxp160OH0rxJppwO3Bh869PkDnaPEd/Pxw7PawC+li0gi
NRV8iCEx85aFxcyOhqn0WZOasxee
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIID/zCCAuegAwIBAgIBRzANBgkqhkiG9w0BAQUFADCBijELMAkGA1UEBhMCVVMx
EzARBgNVBAgMCldhc2hpbmd0b24xEDAOBgNVBAcMB1NlYXR0bGUxIjAgBgNVBAoM
GUFtYXpvbiBXZWIgU2VydmljZXMsIEluYy4xEzARBgNVBAsMCkFtYXpvbiBSRFMx
GzAZBgNVBAMMEkFtYXpvbiBSRFMgUm9vdCBDQTAeFw0xNTAyMDUyMjAzMzFaFw0y
MDAzMDUyMjAzMzFaMIGSMQswCQYDVQQGEwJVUzETMBEGA1UECAwKV2FzaGluZ3Rv
bjEQMA4GA1UEBwwHU2VhdHRsZTEiMCAGA1UECgwZQW1hem9uIFdlYiBTZXJ2aWNl
cywgSW5jLjETMBEGA1UECwwKQW1hem9uIFJEUzEjMCEGA1UEAwwaQW1hem9uIFJE
UyBldS1jZW50cmFsLTEgQ0EwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIB
AQDFtP2dhSLuaPOI4ZrrPWsK4OY9ocQBp3yApH1KJYmI9wpQKZG/KCH2E6Oo7JAw
QORU519r033T+FO2Z7pFPlmz1yrxGXyHpJs8ySx3Yo5S8ncDCdZJCLmtPiq/hahg
5/0ffexMFUCQaYicFZsrJ/cStdxUV+tSw2JQLD7UxS9J97LQWUPyyG+ZrjYVTVq+
zudnFmNSe4QoecXMhAFTGJFQXxP7nhSL9Ao5FGgdXy7/JWeWdQIAj8ku6cBDKPa6
Y6kP+ak+In+Lye8z9qsCD/afUozfWjPR2aA4JoIZVF8dNRShIMo8l0XfgfM2q0+n
ApZWZ+BjhIO5XuoUgHS3D2YFAgMBAAGjZjBkMA4GA1UdDwEB/wQEAwIBBjASBgNV
HRMBAf8ECDAGAQH/AgEAMB0GA1UdDgQWBBRm4GsWIA/M6q+tK8WGHWDGh2gcyTAf
BgNVHSMEGDAWgBROAu6sPvYVyEztLPUFwY+chAhJgzANBgkqhkiG9w0BAQUFAAOC
AQEAHpMmeVQNqcxgfQdbDIi5UIy+E7zZykmtAygN1XQrvga9nXTis4kOTN6g5/+g
HCx7jIXeNJzAbvg8XFqBN84Quqgpl/tQkbpco9Jh1HDs558D5NnZQxNqH5qXQ3Mm
uPgCw0pYcPOa7bhs07i+MdVwPBsX27CFDtsgAIru8HvKxY1oTZrWnyIRo93tt/pk
WuItVMVHjaQZVfTCow0aDUbte6Vlw82KjUFq+n2NMSCJDiDKsDDHT6BJc4AJHIq3
/4Z52MSC9KMr0yAaaoWfW/yMEj9LliQauAgwVjArF4q78rxpfKTG9Rfd8U1BZANP
7FrFMN0ThjfA1IvmOYcgskY5bQ==
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIID/DCCAuSgAwIBAgIBSDANBgkqhkiG9w0BAQUFADCBijELMAkGA1UEBhMCVVMx
EzARBgNVBAgMCldhc2hpbmd0b24xEDAOBgNVBAcMB1NlYXR0bGUxIjAgBgNVBAoM
GUFtYXpvbiBXZWIgU2VydmljZXMsIEluYy4xEzARBgNVBAsMCkFtYXpvbiBSRFMx
GzAZBgNVBAMMEkFtYXpvbiBSRFMgUm9vdCBDQTAeFw0xNTAyMDUyMjAzMzVaFw0y
MDAzMDUyMjAzMzVaMIGPMQswCQYDVQQGEwJVUzETMBEGA1UECAwKV2FzaGluZ3Rv
bjEQMA4GA1UEBwwHU2VhdHRsZTEiMCAGA1UECgwZQW1hem9uIFdlYiBTZXJ2aWNl
cywgSW5jLjETMBEGA1UECwwKQW1hem9uIFJEUzEgMB4GA1UEAwwXQW1hem9uIFJE
UyBldS13ZXN0LTEgQ0EwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQCx
PdbqQ0HKRj79Pmocxvjc+P6i4Ux24kgFIl+ckiir1vzkmesc3a58gjrMlCksEObt
Yihs5IhzEq1ePT0gbfS9GYFp34Uj/MtPwlrfCBWG4d2TcrsKRHr1/EXUYhWqmdrb
RhX8XqoRhVkbF/auzFSBhTzcGGvZpQ2KIaxRcQfcXlMVhj/pxxAjh8U4F350Fb0h
nX1jw4/KvEreBL0Xb2lnlGTkwVxaKGSgXEnOgIyOFdOQc61vdome0+eeZsP4jqeR
TGYJA9izJsRbe2YJxHuazD+548hsPlM3vFzKKEVURCha466rAaYAHy3rKur3HYQx
Yt+SoKcEz9PXuSGj96ejAgMBAAGjZjBkMA4GA1UdDwEB/wQEAwIBBjASBgNVHRMB
Af8ECDAGAQH/AgEAMB0GA1UdDgQWBBTebg//h2oeXbZjQ4uuoiuLYzuiPDAfBgNV
HSMEGDAWgBROAu6sPvYVyEztLPUFwY+chAhJgzANBgkqhkiG9w0BAQUFAAOCAQEA
TikPaGeZasTPw+4RBemlsyPAjtFFQLo7ddaFdORLgdEysVf8aBqndvbA6MT/v4lj
GtEtUdF59ZcbWOrVm+fBZ2h/jYJ59dYF/xzb09nyRbdMSzB9+mkSsnOMqluq5y8o
DY/PfP2vGhEg/2ZncRC7nlQU1Dm8F4lFWEiQ2fi7O1cW852Vmbq61RIfcYsH/9Ma
kpgk10VZ75b8m3UhmpZ/2uRY+JEHImH5WpcTJ7wNiPNJsciZMznGtrgOnPzYco8L
cDleOASIZifNMQi9PKOJKvi0ITz0B/imr8KBsW0YjZVJ54HMa7W1lwugSM7aMAs+
E3Sd5lS+SHwWaOCHwhOEVA==
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIID/DCCAuSgAwIBAgIBSTANBgkqhkiG9w0BAQUFADCBijELMAkGA1UEBhMCVVMx
EzARBgNVBAgMCldhc2hpbmd0b24xEDAOBgNVBAcMB1NlYXR0bGUxIjAgBgNVBAoM
GUFtYXpvbiBXZWIgU2VydmljZXMsIEluYy4xEzARBgNVBAsMCkFtYXpvbiBSRFMx
GzAZBgNVBAMMEkFtYXpvbiBSRFMgUm9vdCBDQTAeFw0xNTAyMDUyMjAzNDBaFw0y
MDAzMDUyMjAzNDBaMIGPMQswCQYDVQQGEwJVUzETMBEGA1UECAwKV2FzaGluZ3Rv
bjEQMA4GA1UEBwwHU2VhdHRsZTEiMCAGA1UECgwZQW1hem9uIFdlYiBTZXJ2aWNl
cywgSW5jLjETMBEGA1UECwwKQW1hem9uIFJEUzEgMB4GA1UEAwwXQW1hem9uIFJE
UyBzYS1lYXN0LTEgQ0EwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQCU
X4OBnQ5xA6TLJAiFEI6l7bUWjoVJBa/VbMdCCSs2i2dOKmqUaXu2ix2zcPILj3lZ
GMk3d/2zvTK/cKhcFrewHUBamTeVHdEmynhMQamqNmkM4ptYzFcvEUw1TGxHT4pV
Q6gSN7+/AJewQvyHexHo8D0+LDN0/Wa9mRm4ixCYH2CyYYJNKaZt9+EZfNu+PPS4
8iB0TWH0DgQkbWMBfCRgolLLitAZklZ4dvdlEBS7evN1/7ttBxUK6SvkeeSx3zBl
ww3BlXqc3bvTQL0A+RRysaVyFbvtp9domFaDKZCpMmDFAN/ntx215xmQdrSt+K3F
cXdGQYHx5q410CAclGnbAgMBAAGjZjBkMA4GA1UdDwEB/wQEAwIBBjASBgNVHRMB
Af8ECDAGAQH/AgEAMB0GA1UdDgQWBBT6iVWnm/uakS+tEX2mzIfw+8JL0zAfBgNV
HSMEGDAWgBROAu6sPvYVyEztLPUFwY+chAhJgzANBgkqhkiG9w0BAQUFAAOCAQEA
FmDD+QuDklXn2EgShwQxV13+txPRuVdOSrutHhoCgMwFWCMtPPtBAKs6KPY7Guvw
DpJoZSehDiOfsgMirjOWjvfkeWSNvKfjWTVneX7pZD9W5WPnsDBvTbCGezm+v87z
b+ZM2ZMo98m/wkMcIEAgdSKilR2fuw8rLkAjhYFfs0A7tDgZ9noKwgHvoE4dsrI0
KZYco6DlP/brASfHTPa2puBLN9McK3v+h0JaSqqm5Ro2Bh56tZkQh8AWy/miuDuK
3+hNEVdxosxlkM1TPa1DGj0EzzK0yoeerXuH2HX7LlCrrxf6/wdKnjR12PMrLQ4A
pCqkcWw894z6bV9MAvKe6A==
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIID/DCCAuSgAwIBAgIBQzANBgkqhkiG9w0BAQUFADCBijELMAkGA1UEBhMCVVMx
EzARBgNVBAgMCldhc2hpbmd0b24xEDAOBgNVBAcMB1NlYXR0bGUxIjAgBgNVBAoM
GUFtYXpvbiBXZWIgU2VydmljZXMsIEluYy4xEzARBgNVBAsMCkFtYXpvbiBSRFMx
GzAZBgNVBAMMEkFtYXpvbiBSRFMgUm9vdCBDQTAeFw0xNTAyMDUyMTU0MDRaFw0y
MDAzMDUyMTU0MDRaMIGPMQswCQYDVQQGEwJVUzETMBEGA1UECAwKV2FzaGluZ3Rv
bjEQMA4GA1UEBwwHU2VhdHRsZTEiMCAGA1UECgwZQW1hem9uIFdlYiBTZXJ2aWNl
cywgSW5jLjETMBEGA1UECwwKQW1hem9uIFJEUzEgMB4GA1UEAwwXQW1hem9uIFJE
UyB1cy1lYXN0LTEgQ0EwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDI
UIuwh8NusKHk1SqPXcP7OqxY3S/M2ZyQWD3w7Bfihpyyy/fc1w0/suIpX3kbMhAV
2ESwged2/2zSx4pVnjp/493r4luhSqQYzru78TuPt9bhJIJ51WXunZW2SWkisSaf
USYUzVN9ezR/bjXTumSUQaLIouJt3OHLX49s+3NAbUyOI8EdvgBQWD68H1epsC0n
CI5s+pIktyOZ59c4DCDLQcXErQ+tNbDC++oct1ANd/q8p9URonYwGCGOBy7sbCYq
9eVHh1Iy2M+SNXddVOGw5EuruvHoCIQyOz5Lz4zSuZA9dRbrfztNOpezCNYu6NKM
n+hzcvdiyxv77uNm8EaxAgMBAAGjZjBkMA4GA1UdDwEB/wQEAwIBBjASBgNVHRMB
Af8ECDAGAQH/AgEAMB0GA1UdDgQWBBQSQG3TmMe6Sa3KufaPBa72v4QFDzAfBgNV
HSMEGDAWgBROAu6sPvYVyEztLPUFwY+chAhJgzANBgkqhkiG9w0BAQUFAAOCAQEA
L/mOZfB3187xTmjOHMqN2G2oSKHBKiQLM9uv8+97qT+XR+TVsBT6b3yoPpMAGhHA
Pc7nxAF5gPpuzatx0OTLPcmYucFmfqT/1qA5WlgCnMNtczyNMH97lKFTNV7Njtek
jWEzAEQSyEWrkNpNlC4j6kMYyPzVXQeXUeZTgJ9FNnVZqmvfjip2N22tawMjrCn5
7KN/zN65EwY2oO9XsaTwwWmBu3NrDdMbzJnbxoWcFWj4RBwanR1XjQOVNhDwmCOl
/1Et13b8CPyj69PC8BOVU6cfTSx8WUVy0qvYOKHNY9Bqa5BDnIL3IVmUkeTlM1mt
enRpyBj+Bk9rh/ICdiRKmA==
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIID/DCCAuSgAwIBAgIBSjANBgkqhkiG9w0BAQUFADCBijELMAkGA1UEBhMCVVMx
EzARBgNVBAgMCldhc2hpbmd0b24xEDAOBgNVBAcMB1NlYXR0bGUxIjAgBgNVBAoM
GUFtYXpvbiBXZWIgU2VydmljZXMsIEluYy4xEzARBgNVBAsMCkFtYXpvbiBSRFMx
GzAZBgNVBAMMEkFtYXpvbiBSRFMgUm9vdCBDQTAeFw0xNTAyMDUyMjAzNDVaFw0y
MDAzMDUyMjAzNDVaMIGPMQswCQYDVQQGEwJVUzETMBEGA1UECAwKV2FzaGluZ3Rv
bjEQMA4GA1UEBwwHU2VhdHRsZTEiMCAGA1UECgwZQW1hem9uIFdlYiBTZXJ2aWNl
cywgSW5jLjETMBEGA1UECwwKQW1hem9uIFJEUzEgMB4GA1UEAwwXQW1hem9uIFJE
UyB1cy13ZXN0LTEgQ0EwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDE
Dhw+uw/ycaiIhhyu2pXFRimq0DlB8cNtIe8hdqndH8TV/TFrljNgR8QdzOgZtZ9C
zzQ2GRpInN/qJF6slEd6wO+6TaDBQkPY+07TXNt52POFUhdVkhJXHpE2BS7Xn6J7
7RFAOeG1IZmc2DDt+sR1BgXzUqHslQGfFYNS0/MBO4P+ya6W7IhruB1qfa4HiYQS
dbe4MvGWnv0UzwAqdR7OF8+8/5c58YXZIXCO9riYF2ql6KNSL5cyDPcYK5VK0+Q9
VI6vuJHSMYcF7wLePw8jtBktqAFE/wbdZiIHhZvNyiNWPPNTGUmQbaJ+TzQEHDs5
8en+/W7JKnPyBOkxxENbAgMBAAGjZjBkMA4GA1UdDwEB/wQEAwIBBjASBgNVHRMB
Af8ECDAGAQH/AgEAMB0GA1UdDgQWBBS0nw/tFR9bCjgqWTPJkyy4oOD8bzAfBgNV
HSMEGDAWgBROAu6sPvYVyEztLPUFwY+chAhJgzANBgkqhkiG9w0BAQUFAAOCAQEA
CXGAY3feAak6lHdqj6+YWjy6yyUnLK37bRxZDsyDVXrPRQaXRzPTzx79jvDwEb/H
Q/bdQ7zQRWqJcbivQlwhuPJ4kWPUZgSt3JUUuqkMsDzsvj/bwIjlrEFDOdHGh0mi
eVIngFEjUXjMh+5aHPEF9BlQnB8LfVtKj18e15UDTXFa+xJPFxUR7wDzCfo4WI1m
sUMG4q1FkGAZgsoyFPZfF8IVvgCuGdR8z30VWKklFxttlK0eGLlPAyIO0CQxPQlo
saNJrHf4tLOgZIWk+LpDhNd9Et5EzvJ3aURUsKY4pISPPF5WdvM9OE59bERwUErd
nuOuQWQeeadMceZnauRzJQ==
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIID/DCCAuSgAwIBAgIBSzANBgkqhkiG9w0BAQUFADCBijELMAkGA1UEBhMCVVMx
EzARBgNVBAgMCldhc2hpbmd0b24xEDAOBgNVBAcMB1NlYXR0bGUxIjAgBgNVBAoM
GUFtYXpvbiBXZWIgU2VydmljZXMsIEluYy4xEzARBgNVBAsMCkFtYXpvbiBSRFMx
GzAZBgNVBAMMEkFtYXpvbiBSRFMgUm9vdCBDQTAeFw0xNTAyMDUyMjAzNTBaFw0y
MDAzMDUyMjAzNTBaMIGPMQswCQYDVQQGEwJVUzETMBEGA1UECAwKV2FzaGluZ3Rv
bjEQMA4GA1UEBwwHU2VhdHRsZTEiMCAGA1UECgwZQW1hem9uIFdlYiBTZXJ2aWNl
cywgSW5jLjETMBEGA1UECwwKQW1hem9uIFJEUzEgMB4GA1UEAwwXQW1hem9uIFJE
UyB1cy13ZXN0LTIgQ0EwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDM
H58SR48U6jyERC1vYTnub34smf5EQVXyzaTmspWGWGzT31NLNZGSDFaa7yef9kdO
mzJsgebR5tXq6LdwlIoWkKYQ7ycUaadtVKVYdI40QcI3cHn0qLFlg2iBXmWp/B+i
Z34VuVlCh31Uj5WmhaBoz8t/GRqh1V/aCsf3Wc6jCezH3QfuCjBpzxdOOHN6Ie2v
xX09O5qmZTvMoRBAvPkxdaPg/Mi7fxueWTbEVk78kuFbF1jHYw8U1BLILIAhcqlq
x4u8nl73t3O3l/soNUcIwUDK0/S+Kfqhwn9yQyPlhb4Wy3pfnZLJdkyHldktnQav
9TB9u7KH5Lk0aAYslMLxAgMBAAGjZjBkMA4GA1UdDwEB/wQEAwIBBjASBgNVHRMB
Af8ECDAGAQH/AgEAMB0GA1UdDgQWBBT8roM4lRnlFHWMPWRz0zkwFZog1jAfBgNV
HSMEGDAWgBROAu6sPvYVyEztLPUFwY+chAhJgzANBgkqhkiG9w0BAQUFAAOCAQEA
JwrxwgwmPtcdaU7O7WDdYa4hprpOMamI49NDzmE0s10oGrqmLwZygcWU0jT+fJ+Y
pJe1w0CVfKaeLYNsOBVW3X4ZPmffYfWBheZiaiEflq/P6t7/Eg81gaKYnZ/x1Dfa
sUYkzPvCkXe9wEz5zdUTOCptDt89rBR9CstL9vE7WYUgiVVmBJffWbHQLtfjv6OF
NMb0QME981kGRzc2WhgP71YS2hHd1kXtsoYP1yTu4vThSKsoN4bkiHsaC1cRkLoy
0fFA4wpB3WloMEvCDaUvvH1LZlBXTNlwi9KtcwD4tDxkkBt4tQczKLGpQ/nF/W9n
8YDWk3IIc1sd0bkZqoau2Q==
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIIEATCCAumgAwIBAgIBTDANBgkqhkiG9w0BAQUFADCBijELMAkGA1UEBhMCVVMx
EzARBgNVBAgMCldhc2hpbmd0b24xEDAOBgNVBAcMB1NlYXR0bGUxIjAgBgNVBAoM
GUFtYXpvbiBXZWIgU2VydmljZXMsIEluYy4xEzARBgNVBAsMCkFtYXpvbiBSRFMx
GzAZBgNVBAMMEkFtYXpvbiBSRFMgUm9vdCBDQTAeFw0xNTExMDYwMDA1NDZaFw0y
MDAzMDUwMDA1NDZaMIGUMQswCQYDVQQGEwJVUzETMBEGA1UECAwKV2FzaGluZ3Rv
bjEQMA4GA1UEBwwHU2VhdHRsZTEiMCAGA1UECgwZQW1hem9uIFdlYiBTZXJ2aWNl
cywgSW5jLjETMBEGA1UECwwKQW1hem9uIFJEUzElMCMGA1UEAwwcQW1hem9uIFJE
UyBhcC1ub3J0aGVhc3QtMiBDQTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoC
ggEBAKSwd+RVUzTRH0FgnbwoTK8TMm/zMT4+2BvALpAUe6YXbkisg2goycWuuWLg
jOpFBB3GtyvXZnkqi7MkDWUmj1a2kf8l2oLyoaZ+Hm9x/sV+IJzOqPvj1XVUGjP6
yYYnPJmUYqvZeI7fEkIGdFkP2m4/sgsSGsFvpD9FK1bL1Kx2UDpYX0kHTtr18Zm/
1oN6irqWALSmXMDydb8hE0FB2A1VFyeKE6PnoDj/Y5cPHwPPdEi6/3gkDkSaOG30
rWeQfL3pOcKqzbHaWTxMphd0DSL/quZ64Nr+Ly65Q5PRcTrtr55ekOUziuqXwk+o
9QpACMwcJ7ROqOznZTqTzSFVXFECAwEAAaNmMGQwDgYDVR0PAQH/BAQDAgEGMBIG
A1UdEwEB/wQIMAYBAf8CAQAwHQYDVR0OBBYEFM6Nox/QWbhzWVvzoJ/y0kGpNPK+
MB8GA1UdIwQYMBaAFE4C7qw+9hXITO0s9QXBj5yECEmDMA0GCSqGSIb3DQEBBQUA
A4IBAQCTkWBqNvyRf3Y/W21DwFx3oT/AIWrHt0BdGZO34tavummXemTH9LZ/mqv9
aljt6ZuDtf5DEQjdsAwXMsyo03ffnP7doWm8iaF1+Mui77ot0TmTsP/deyGwukvJ
tkxX8bZjDh+EaNauWKr+CYnniNxCQLfFtXYJsfOdVBzK3xNL+Z3ucOQRhr2helWc
CDQgwfhP1+3pRVKqHvWCPC4R3fT7RZHuRmZ38kndv476GxRntejh+ePffif78bFI
3rIZCPBGobrrUMycafSbyXteoGca/kA+/IqrAPlk0pWQ4aEL0yTWN2h2dnjoD7oX
byIuL/g9AGRh97+ssn7D6bDRPTbW
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIID/TCCAuWgAwIBAgIBTTANBgkqhkiG9w0BAQsFADCBijELMAkGA1UEBhMCVVMx
EzARBgNVBAgMCldhc2hpbmd0b24xEDAOBgNVBAcMB1NlYXR0bGUxIjAgBgNVBAoM
GUFtYXpvbiBXZWIgU2VydmljZXMsIEluYy4xEzARBgNVBAsMCkFtYXpvbiBSRFMx
GzAZBgNVBAMMEkFtYXpvbiBSRFMgUm9vdCBDQTAeFw0xNjA1MDMyMTI5MjJaFw0y
MDAzMDUyMTI5MjJaMIGQMQswCQYDVQQGEwJVUzETMBEGA1UECAwKV2FzaGluZ3Rv
bjEQMA4GA1UEBwwHU2VhdHRsZTEiMCAGA1UECgwZQW1hem9uIFdlYiBTZXJ2aWNl
cywgSW5jLjETMBEGA1UECwwKQW1hem9uIFJEUzEhMB8GA1UEAwwYQW1hem9uIFJE
UyBhcC1zb3V0aC0xIENBMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA
06eWGLE0TeqL9kyWOLkS8q0fXO97z+xyBV3DKSB2lg2GkgBz3B98MkmkeB0SZy3G
Ce4uCpCPbFKiFEdiUclOlhZsrBuCeaimxLM3Ig2wuenElO/7TqgaYHYUbT3d+VQW
GUbLn5GRZJZe1OAClYdOWm7A1CKpuo+cVV1vxbY2nGUQSJPpVn2sT9gnwvjdE60U
JGYU/RLCTm8zmZBvlWaNIeKDnreIc4rKn6gUnJ2cQn1ryCVleEeyc3xjYDSrjgdn
FLYGcp9mphqVT0byeQMOk0c7RHpxrCSA0V5V6/CreFV2LteK50qcDQzDSM18vWP/
p09FoN8O7QrtOeZJzH/lmwIDAQABo2YwZDAOBgNVHQ8BAf8EBAMCAQYwEgYDVR0T
AQH/BAgwBgEB/wIBADAdBgNVHQ4EFgQU2i83QHuEl/d0keXF+69HNJph7cMwHwYD
VR0jBBgwFoAUTgLurD72FchM7Sz1BcGPnIQISYMwDQYJKoZIhvcNAQELBQADggEB
ACqnH2VjApoDqoSQOky52QBwsGaj+xWYHW5Gm7EvCqvQuhWMkeBuD6YJmMvNyA9G
I2lh6/o+sUk/RIsbYbxPRdhNPTOgDR9zsNRw6qxaHztq/CEC+mxDCLa3O1hHBaDV
BmB3nCZb93BvO0EQSEk7aytKq/f+sjyxqOcs385gintdHGU9uM7gTZHnU9vByJsm
/TL07Miq67X0NlhIoo3jAk+xHaeKJdxdKATQp0448P5cY20q4b8aMk1twcNaMvCP
dG4M5doaoUA8OQ/0ukLLae/LBxLeTw04q1/a2SyFaVUX2Twbb1S3xVWwLA8vsyGr
igXx7B5GgP+IHb6DTjPJAi0=
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIID/DCCAuSgAwIBAgIBTjANBgkqhkiG9w0BAQsFADCBijELMAkGA1UEBhMCVVMx
EzARBgNVBAgMCldhc2hpbmd0b24xEDAOBgNVBAcMB1NlYXR0bGUxIjAgBgNVBAoM
GUFtYXpvbiBXZWIgU2VydmljZXMsIEluYy4xEzARBgNVBAsMCkFtYXpvbiBSRFMx
GzAZBgNVBAMMEkFtYXpvbiBSRFMgUm9vdCBDQTAeFw0xNjA4MTExOTU4NDVaFw0y
MDAzMDUxOTU4NDVaMIGPMQswCQYDVQQGEwJVUzETMBEGA1UECAwKV2FzaGluZ3Rv
bjEQMA4GA1UEBwwHU2VhdHRsZTEiMCAGA1UECgwZQW1hem9uIFdlYiBTZXJ2aWNl
cywgSW5jLjETMBEGA1UECwwKQW1hem9uIFJEUzEgMB4GA1UEAwwXQW1hem9uIFJE
UyB1cy1lYXN0LTIgQ0EwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQCp
WnnUX7wM0zzstccX+4iXKJa9GR0a2PpvB1paEX4QRCgfhEdQWDaSqyrWNgdVCKkt
1aQkWu5j6VAC2XIG7kKoonm1ZdBVyBLqW5lXNywlaiU9yhJkwo8BR+/OqgE+PLt/
EO1mlN0PQudja/XkExCXTO29TG2j7F/O7hox6vTyHNHc0H88zS21uPuBE+jivViS
yzj/BkyoQ85hnkues3f9R6gCGdc+J51JbZnmgzUkvXjAEuKhAm9JksVOxcOKUYe5
ERhn0U9zjzpfbAITIkul97VVa5IxskFFTHIPJbvRKHJkiF6wTJww/tc9wm+fSCJ1
+DbQTGZgkQ3bJrqRN29/AgMBAAGjZjBkMA4GA1UdDwEB/wQEAwIBBjASBgNVHRMB
Af8ECDAGAQH/AgEAMB0GA1UdDgQWBBSAHQzUYYZbepwKEMvGdHp8wzHnfDAfBgNV
HSMEGDAWgBROAu6sPvYVyEztLPUFwY+chAhJgzANBgkqhkiG9w0BAQsFAAOCAQEA
MbaEzSYZ+aZeTBxf8yi0ta8K4RdwEJsEmP6IhFFQHYUtva2Cynl4Q9tZg3RMsybT
9mlnSQQlbN/wqIIXbkrcgFcHoXG9Odm/bDtUwwwDaiEhXVfeQom3G77QHOWMTCGK
qadwuh5msrb17JdXZoXr4PYHDKP7j0ONfAyFNER2+uecblHfRSpVq5UeF3L6ZJb8
fSw/GtAV6an+/0r+Qm+PiI2H5XuZ4GmRJYnGMhqWhBYrY7p3jtVnKcsh39wgfUnW
AvZEZG/yhFyAZW0Essa39LiL5VSq14Y1DOj0wgnhSY/9WHxaAo1HB1T9OeZknYbD
fl/EGSZ0TEvZkENrXcPlVA==
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIID/zCCAuegAwIBAgIBTzANBgkqhkiG9w0BAQsFADCBijELMAkGA1UEBhMCVVMx
EzARBgNVBAgMCldhc2hpbmd0b24xEDAOBgNVBAcMB1NlYXR0bGUxIjAgBgNVBAoM
GUFtYXpvbiBXZWIgU2VydmljZXMsIEluYy4xEzARBgNVBAsMCkFtYXpvbiBSRFMx
GzAZBgNVBAMMEkFtYXpvbiBSRFMgUm9vdCBDQTAeFw0xNjA5MTUwMDEwMTFaFw0y
MDAzMDUwMDEwMTFaMIGSMQswCQYDVQQGEwJVUzETMBEGA1UECAwKV2FzaGluZ3Rv
bjEQMA4GA1UEBwwHU2VhdHRsZTEiMCAGA1UECgwZQW1hem9uIFdlYiBTZXJ2aWNl
cywgSW5jLjETMBEGA1UECwwKQW1hem9uIFJEUzEjMCEGA1UEAwwaQW1hem9uIFJE
UyBjYS1jZW50cmFsLTEgQ0EwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIB
AQCZYI/iQ6DrS3ny3t1EwX1wAD+3LMgh7Fd01EW5LIuaK2kYIIQpsVKhxLCit/V5
AGc/1qiJS1Qz9ODLTh0Na6bZW6EakRzuHJLe32KJtoFYPC7Z09UqzXrpA/XL+1hM
P0ZmCWsU7Nn/EmvfBp9zX3dZp6P6ATrvDuYaVFr+SA7aT3FXpBroqBS1fyzUPs+W
c6zTR6+yc4zkHX0XQxC5RH6xjgpeRkoOajA/sNo7AQF7KlWmKHbdVF44cvvAhRKZ
XaoVs/C4GjkaAEPTCbopYdhzg+KLx9eB2BQnYLRrIOQZtRfbQI2Nbj7p3VsRuOW1
tlcks2w1Gb0YC6w6SuIMFkl1AgMBAAGjZjBkMA4GA1UdDwEB/wQEAwIBBjASBgNV
HRMBAf8ECDAGAQH/AgEAMB0GA1UdDgQWBBToYWxE1lawl6Ks6NsvpbHQ3GKEtzAf
BgNVHSMEGDAWgBROAu6sPvYVyEztLPUFwY+chAhJgzANBgkqhkiG9w0BAQsFAAOC
AQEAG/8tQ0ooi3hoQpa5EJz0/E5VYBsAz3YxA2HoIonn0jJyG16bzB4yZt4vNQMA
KsNlQ1uwDWYL1nz63axieUUFIxqxl1KmwfhsmLgZ0Hd2mnTPIl2Hw3uj5+wdgGBg
agnAZ0bajsBYgD2VGQbqjdk2Qn7Fjy3LEWIvGZx4KyZ99OJ2QxB7JOPdauURAtWA
DKYkP4LLJxtj07DSzG8kuRWb9B47uqUD+eKDIyjfjbnzGtd9HqqzYFau7EX3HVD9
9Qhnjl7bTZ6YfAEZ3nH2t3Vc0z76XfGh47rd0pNRhMV+xpok75asKf/lNh5mcUrr
VKwflyMkQpSbDCmcdJ90N2xEXQ==
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIID/DCCAuSgAwIBAgIBUDANBgkqhkiG9w0BAQsFADCBijELMAkGA1UEBhMCVVMx
EzARBgNVBAgMCldhc2hpbmd0b24xEDAOBgNVBAcMB1NlYXR0bGUxIjAgBgNVBAoM
GUFtYXpvbiBXZWIgU2VydmljZXMsIEluYy4xEzARBgNVBAsMCkFtYXpvbiBSRFMx
GzAZBgNVBAMMEkFtYXpvbiBSRFMgUm9vdCBDQTAeFw0xNjEwMTAxNzQ0NDJaFw0y
MDAzMDUxNzQ0NDJaMIGPMQswCQYDVQQGEwJVUzETMBEGA1UECAwKV2FzaGluZ3Rv
bjEQMA4GA1UEBwwHU2VhdHRsZTEiMCAGA1UECgwZQW1hem9uIFdlYiBTZXJ2aWNl
cywgSW5jLjETMBEGA1UECwwKQW1hem9uIFJEUzEgMB4GA1UEAwwXQW1hem9uIFJE
UyBldS13ZXN0LTIgQ0EwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDO
cttLJfubB4XMMIGWNfJISkIdCMGJyOzLiMJaiWB5GYoXKhEl7YGotpy0qklwW3BQ
a0fmVdcCLX+dIuVQ9iFK+ZcK7zwm7HtdDTCHOCKeOh2IcnU4c/VIokFi6Gn8udM6
N/Zi5M5OGpVwLVALQU7Yctsn3c95el6MdVx6mJiIPVu7tCVZn88Z2koBQ2gq9P4O
Sb249SHFqOb03lYDsaqy1NDsznEOhaRBw7DPJFpvmw1lA3/Y6qrExRI06H2VYR2i
7qxwDV50N58fs10n7Ye1IOxTVJsgEA7X6EkRRXqYaM39Z76R894548WHfwXWjUsi
MEX0RS0/t1GmnUQjvevDAgMBAAGjZjBkMA4GA1UdDwEB/wQEAwIBBjASBgNVHRMB
Af8ECDAGAQH/AgEAMB0GA1UdDgQWBBQBxmcuRSxERYCtNnSr5xNfySokHjAfBgNV
HSMEGDAWgBROAu6sPvYVyEztLPUFwY+chAhJgzANBgkqhkiG9w0BAQsFAAOCAQEA
UyCUQjsF3nUAABjfEZmpksTuUo07aT3KGYt+EMMFdejnBQ0+2lJJFGtT+CDAk1SD
RSgfEBon5vvKEtlnTf9a3pv8WXOAkhfxnryr9FH6NiB8obISHNQNPHn0ljT2/T+I
Y6ytfRvKHa0cu3V0NXbJm2B4KEOt4QCDiFxUIX9z6eB4Kditwu05OgQh6KcogOiP
JesWxBMXXGoDC1rIYTFO7szwDyOHlCcVXJDNsTJhc32oDWYdeIbW7o/5I+aQsrXZ
C96HykZcgWzz6sElrQxUaT3IoMw/5nmw4uWKKnZnxgI9bY4fpQwMeBZ96iHfFxvH
mqfEEuC7uUoPofXdBp2ObQ==
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIID/DCCAuSgAwIBAgIBUTANBgkqhkiG9w0BAQsFADCBijELMAkGA1UEBhMCVVMx
EzARBgNVBAgMCldhc2hpbmd0b24xEDAOBgNVBAcMB1NlYXR0bGUxIjAgBgNVBAoM
GUFtYXpvbiBXZWIgU2VydmljZXMsIEluYy4xEzARBgNVBAsMCkFtYXpvbiBSRFMx
GzAZBgNVBAMMEkFtYXpvbiBSRFMgUm9vdCBDQTAeFw0xNzA4MjUyMTM5MjZaFw0y
MDAzMDUyMTM5MjZaMIGPMQswCQYDVQQGEwJVUzETMBEGA1UECAwKV2FzaGluZ3Rv
bjEQMA4GA1UEBwwHU2VhdHRsZTEiMCAGA1UECgwZQW1hem9uIFdlYiBTZXJ2aWNl
cywgSW5jLjETMBEGA1UECwwKQW1hem9uIFJEUzEgMB4GA1UEAwwXQW1hem9uIFJE
UyBldS13ZXN0LTMgQ0EwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQC+
xmlEC/3a4cJH+UPwXCE02lC7Zq5NHd0dn6peMeLN8agb6jW4VfSY0NydjRj2DJZ8
K7wV6sub5NUGT1NuFmvSmdbNR2T59KX0p2dVvxmXHHtIpQ9Y8Aq3ZfhmC5q5Bqgw
tMA1xayDi7HmoPX3R8kk9ktAZQf6lDeksCvok8idjTu9tiSpDiMwds5BjMsWfyjZ
d13PTGGNHYVdP692BSyXzSP1Vj84nJKnciW8tAqwIiadreJt5oXyrCXi8ekUMs80
cUTuGm3aA3Q7PB5ljJMPqz0eVddaiIvmTJ9O3Ez3Du/HpImyMzXjkFaf+oNXf/Hx
/EW5jCRR6vEiXJcDRDS7AgMBAAGjZjBkMA4GA1UdDwEB/wQEAwIBBjASBgNVHRMB
Af8ECDAGAQH/AgEAMB0GA1UdDgQWBBRZ9mRtS5fHk3ZKhG20Oack4cAqMTAfBgNV
HSMEGDAWgBROAu6sPvYVyEztLPUFwY+chAhJgzANBgkqhkiG9w0BAQsFAAOCAQEA
F/u/9L6ExQwD73F/bhCw7PWcwwqsK1mypIdrjdIsu0JSgwWwGCXmrIspA3n3Dqxq
sMhAJD88s9Em7337t+naar2VyLO63MGwjj+vA4mtvQRKq8ScIpiEc7xN6g8HUMsd
gPG9lBGfNjuAZsrGJflrko4HyuSM7zHExMjXLH+CXcv/m3lWOZwnIvlVMa4x0Tz0
A4fklaawryngzeEjuW6zOiYCzjZtPlP8Fw0SpzppJ8VpQfrZ751RDo4yudmPqoPK
5EUe36L8U+oYBXnC5TlYs9bpVv9o5wJQI5qA9oQE2eFWxF1E0AyZ4V5sgGUBStaX
BjDDWul0wSo7rt1Tq7XpnA==
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIIEATCCAumgAwIBAgIBTjANBgkqhkiG9w0BAQUFADCBijELMAkGA1UEBhMCVVMx
EzARBgNVBAgMCldhc2hpbmd0b24xEDAOBgNVBAcMB1NlYXR0bGUxIjAgBgNVBAoM
GUFtYXpvbiBXZWIgU2VydmljZXMsIEluYy4xEzARBgNVBAsMCkFtYXpvbiBSRFMx
GzAZBgNVBAMMEkFtYXpvbiBSRFMgUm9vdCBDQTAeFw0xNzEyMDEwMDU1NDJaFw0y
MDAzMDUwMDU1NDJaMIGUMQswCQYDVQQGEwJVUzETMBEGA1UECAwKV2FzaGluZ3Rv
bjEQMA4GA1UEBwwHU2VhdHRsZTEiMCAGA1UECgwZQW1hem9uIFdlYiBTZXJ2aWNl
cywgSW5jLjETMBEGA1UECwwKQW1hem9uIFJEUzElMCMGA1UEAwwcQW1hem9uIFJE
UyBhcC1ub3J0aGVhc3QtMyBDQTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoC
ggEBAMZtQNnm/XT19mTa10ftHLzg5UhajoI65JHv4TQNdGXdsv+CQdGYU49BJ9Eu
3bYgiEtTzR2lQe9zGMvtuJobLhOWuavzp7IixoIQcHkFHN6wJ1CvqrxgvJfBq6Hy
EuCDCiU+PPDLUNA6XM6Qx3IpHd1wrJkjRB80dhmMSpxmRmx849uFafhN+P1QybsM
TI0o48VON2+vj+mNuQTyLMMP8D4odSQHjaoG+zyJfJGZeAyqQyoOUOFEyQaHC3TT
3IDSNCQlpxb9LerbCoKu79WFBBq3CS5cYpg8/fsnV2CniRBFFUumBt5z4dhw9RJU
qlUXXO1ZyzpGd+c5v6FtrfXtnIUCAwEAAaNmMGQwDgYDVR0PAQH/BAQDAgEGMBIG
A1UdEwEB/wQIMAYBAf8CAQAwHQYDVR0OBBYEFETv7ELNplYy/xTeIOInl6nzeiHg
MB8GA1UdIwQYMBaAFE4C7qw+9hXITO0s9QXBj5yECEmDMA0GCSqGSIb3DQEBBQUA
A4IBAQCpKxOQcd0tEKb3OtsOY8q/MPwTyustGk2Rt7t9G68idADp8IytB7M0SDRo
wWZqynEq7orQVKdVOanhEWksNDzGp0+FPAf/KpVvdYCd7ru3+iI+V4ZEp2JFdjuZ
Zz0PIjS6AgsZqE5Ri1J+NmfmjGZCPhsHnGZiBaenX6K5VRwwwmLN6xtoqrrfR5zL
QfBeeZNJG6KiM3R/DxJ5rAa6Fz+acrhJ60L7HprhB7SFtj1RCijau3+ZwiGmUOMr
yKlMv+VgmzSw7o4Hbxy1WVrA6zQsTHHSGf+vkQn2PHvnFMUEu/ZLbTDYFNmTLK91
K6o4nMsEvhBKgo4z7H1EqqxXhvN2
-----END CERTIFICATE-----
`

// SQLJoin will take an array of ids (of any type) and return them as quoted strings
func SQLJoin(ids interface{}) string {
	s := make([]string, 0)
	array := reflect.ValueOf(ids)
	for index := 0; index < array.Len(); index++ {
		elem := array.Index(index)
		s = append(s, fmt.Sprintf("'%v'", elem.Interface()))
	}
	return strings.Join(s, ",")
}

var sqlreplaceRE = regexp.MustCompile("\\?")
var sqlspaceRE = regexp.MustCompile("\\s{2,}")
var sqlTightRE = regexp.MustCompile("(\\s?)([\\(\\),!<>=])(\\s?)")

// FormatSQL will make a query compact and one-liner
func FormatSQL(q string, args ...interface{}) string {
	var pos int
	return sqlTightRE.ReplaceAllString(strings.TrimSpace(sqlspaceRE.ReplaceAllString(strings.Replace(strings.Replace(sqlreplaceRE.ReplaceAllStringFunc(q, func(needle string) string {
		arg := args[pos]
		pos++
		return json.Stringify(arg)
	}), "\n", " ", -1), "\t", " ", -1), " ")), "$2")
}

var printTimedQuery = os.Getenv("SQL_DEBUG") == "1"

// TimedQuery will execute query and log out useful information if SQL_DEBUG env is configured to 1 otherwise a normal SQL query
func TimedQuery(ctx context.Context, logger log.Logger, db *sql.DB, label string, sql string, args ...interface{}) (*sql.Rows, error) {
	if printTimedQuery {
		started := time.Now()
		rows, err := db.QueryContext(ctx, sql, args...)
		log.Debug(logger, fmt.Sprintf("sql=%s", FormatSQL(sql, args...)), "label", label, "duration", fmt.Sprintf("%v", time.Since(started)))
		return rows, err
	}
	return db.QueryContext(ctx, sql, args...)
}

// TimedQueryRow will execute query and log out useful information if SQL_DEBUG env is configured to 1 otherwise a normal SQL query
func TimedQueryRow(ctx context.Context, logger log.Logger, db *sql.DB, label string, sql string, args ...interface{}) *sql.Row {
	if printTimedQuery {
		started := time.Now()
		row := db.QueryRowContext(ctx, sql, args...)
		log.Debug(logger, fmt.Sprintf("sql=%s", FormatSQL(sql, args...)), "label", label, "duration", fmt.Sprintf("%v", time.Since(started)))
		return row
	}
	return db.QueryRowContext(ctx, sql, args...)
}

// GetTableNamesIfExists returns an array of tables that exist in the database
func GetTableNamesIfExists(ctx context.Context, db *DB, tables []string, dbname string) ([]string, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf("select distinct(table_name) from information_schema.columns where table_schema = ? and table_name IN (%v)", SQLJoin(tables)), dbname)
	if err != nil {
		return nil, err
	}
	tableNames := []string{}
	for rows.Next() {
		var name string
		rows.Scan(&name)
		tableNames = append(tableNames, name)
	}
	return tableNames, nil
}
