package db

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/pinpt/go-common/log"
	"github.com/pinpt/go-common/number"
	pos "github.com/pinpt/go-common/os"
	"github.com/spf13/cobra"
)

// RegisterDBFlags will setup db flags
func RegisterDBFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().String("databaseName", pos.Getenv("PP_DBNAME", pos.Getenv("PP_MYSQL_DATABASE", "pinpoint")), "Database name")
	cmd.PersistentFlags().Int("databasePort", pos.GetenvInt("PP_DB_PORT", pos.GetenvInt("PP_MYSQL_PORT", 3306)), "Database port")
	cmd.PersistentFlags().String("databaseHostname", pos.Getenv("PP_DB_HOSTNAME", pos.Getenv("PP_MYSQL_HOSTNAME", "localhost")), "Database hostname")
	cmd.PersistentFlags().String("databaseUsername", pos.Getenv("PP_DB_USER", pos.Getenv("PP_MYSQL_USERNAME", "root")), "Database username")
	cmd.PersistentFlags().String("databasePassword", pos.Getenv("PP_DB_PASS", pos.Getenv("PP_MYSQL_PASSWORD", "")), "Database password")
	cmd.PersistentFlags().String("databaseTLS", pos.Getenv("PP_DB_TLS", "false"), "Database TLS setting")
}

func setDBEnv(username string, password string, hostname string, database string, port int, tls string) {
	if tls == "" {
		tls = "false"
	}
	if port == 0 {
		port = 3306
	}
	// set them since they can come in different ways and there are a few places where the env are used
	os.Setenv("PP_DBNAME", database)
	os.Setenv("PP_DB_PORT", fmt.Sprintf("%d", port))
	os.Setenv("PP_DB_HOSTNAME", hostname)
	os.Setenv("PP_DB_USER", username)
	os.Setenv("PP_DB_PASS", password)
	os.Setenv("PP_DB_TLS", tls)
}

// GetDB will setup the command for database
func GetDB(ctx context.Context, cmd *cobra.Command, logger log.Logger, createIfNotExist bool, dbAttrs ...string) (db *DB, _ error) {
	username, err := cmd.Flags().GetString("databaseUsername")
	if err != nil {
		return nil, err
	}
	password, err := cmd.Flags().GetString("databasePassword")
	if err != nil {
		return nil, err
	}
	hostname, err := cmd.Flags().GetString("databaseHostname")
	if err != nil {
		return nil, err
	}
	database, err := cmd.Flags().GetString("databaseName")
	if err != nil {
		return nil, err
	}
	port, err := cmd.Flags().GetInt("databasePort")
	if err != nil {
		return nil, err
	}
	tls, err := cmd.Flags().GetString("databaseTLS")
	if err != nil {
		return nil, err
	}
	if strings.Contains(os.Getenv("PP_DEBUG"), "mysql") {
		fmt.Printf("trying to connect to DB using username=%v, password=%v, hostname=%v, port=%d, database=%v, tls=%s\n", username, MaskDSN(password, password), hostname, port, database, tls)
	}
	if len(dbAttrs) == 0 {
		dbAttrs = []string{}
	}
	if tls == "" {
		tls = "false"
	}
	setDBEnv(username, password, hostname, database, port, tls)
	dbAttrs = append(dbAttrs, "tls="+tls)
	var loop int
	for {
		loop++
		db, err = OpenDB(username, password, hostname, port, database, dbAttrs...)
		if err != nil {
			return nil, err
		}
		mo := number.ToInt32(pos.Getenv("PP_DB_MAXOPEN", "10"))
		mi := number.ToInt32(pos.Getenv("PP_DB_MAXIDLE", "5"))
		db.SetMaxOpenConns(int(mo))
		db.SetMaxIdleConns(int(mi))

		tmpctx, cancel := context.WithTimeout(ctx, time.Second*5)
		defer cancel()
		if err := db.PingContext(tmpctx); err != nil {
			db.Close()
			if strings.Contains(err.Error(), "Error 1049:") && loop == 1 {
				if !createIfNotExist {
					return nil, err
				}
				log.Info(logger, "Attempting to create database", "db", database)
				db, err = OpenDB(username, password, hostname, port, "", dbAttrs...)
				if err == nil {
					if _, derr := db.ExecContext(tmpctx, fmt.Sprintf("create database `%s`", database)); derr == nil {
						log.Info(logger, "Successfully created database", "db", database)
						db.Close()
						continue
					} else {
						db.Close()
						return nil, fmt.Errorf("error creating database `%s`", database)
					}
				}
			}
			return nil, fmt.Errorf("error connecting to mysql at %v:%d with %v. %v", hostname, port, username, err)
		}
		break
	}
	return
}
