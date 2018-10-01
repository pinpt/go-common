package db

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/pinpt/go-common/log"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
)

var databaseName = "" // fill this out
var databasePort = 3306
var databaseHost = "localhost"
var databseUser = "root"
var databasePassword = ""
var databaseTLS = "false"

// TestSQLCache new test
func TestSQLCache(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	assert.NotEmpty(databaseName)

	ctx := context.Background()

	fakeCommand := createCommand()
	logger := log.NewCommandLogger(fakeCommand)
	defer logger.Close()

	cluster, err := GetDBCluster(ctx, fakeCommand, logger, true)
	defer cluster.Close()
	assert.NoError(err)
	assert.NoError(err)

	db := cluster.Replicas
	table := ""  // fill this out
	column := "" // fill this out
	limit := 10
	assert.NotEmpty(table, column)

	rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT %v FROM `%s` LIMIT %v", column, table, limit))
	assert.NoError(err)
	defer rows.Close()

	cacheRows := &CacheRows{}
	err = cacheRows.Fill(rows)
	assert.NoError(err)

	iterations := 5
	names := []string{}
	for i := 0; i < iterations; i++ {
		cacheRows.Reset()
		for cacheRows.Next() {
			var name sql.NullString
			cacheRows.Scan(&name)
			names = append(names, name.String)
		}
	}
	// With CacheRows we can iterate as much as we can, so the result should be:
	// iterations * (number of rows)
	assert.Equal(len(names), limit*iterations)

	// =============================================================================================

	rows, err = db.QueryContext(ctx, fmt.Sprintf("SELECT %v FROM `%s` LIMIT %v", column, table, 10))
	assert.NoError(err)
	defer rows.Close()
	names = []string{}
	for i := 0; i < iterations; i++ {
		for rows.Next() {
			var name sql.NullString
			rows.Scan(&name)
			names = append(names, name.String)
		}
	}
	// With sql.Row we can only iterate once
	assert.Equal(len(names), limit)

}

func createCommand() *cobra.Command {
	command := &cobra.Command{}
	command.Flags().String("databaseName", databaseName, "")
	command.Flags().Int("databasePort", databasePort, "")
	command.Flags().String("databaseHostname", databaseHost, "")
	command.Flags().String("databaseUsername", databseUser, "")
	command.Flags().String("databasePassword", databasePassword, "")
	command.Flags().String("databaseTLS", databaseTLS, "")
	command.Flags().String("databaseClusterInitialConnectionURL", "", "")
	command.Flags().String("databaseClusterURLSuffix", "", "")
	command.Flags().Int("databaseClusterMaxConnectionsPerServer", 0, "")

	return command
}
