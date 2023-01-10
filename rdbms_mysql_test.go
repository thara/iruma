package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xo/dburl"
)

var dbHostPort string

func TestMain(m *testing.M) {
	pool, err := dockertest.NewPool("")
	pool.MaxWait = 10 * time.Second
	if err != nil {
		log.Fatalf("Could not construct pool: %s", err)
	}

	if err := pool.Client.Ping(); err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}

	opts := &dockertest.RunOptions{
		Repository:   "mysql",
		Tag:          "8.0",
		ExposedPorts: []string{"3306"},
		Env: []string{
			"MYSQL_USER=iruma",
			"MYSQL_PASSWORD=iruma",
			"MYSQL_DATABASE=iruma",
			"MYSQL_ROOT_PASSWORD=secret",
		},
	}

	resource, err := pool.RunWithOptions(opts, func(cfg *docker.HostConfig) {
		cfg.AutoRemove = true
		cfg.RestartPolicy = docker.NeverRestart()
	})
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}
	resource.Expire(60)

	dbHostPort = resource.GetHostPort("3306/tcp")

	if err := pool.Retry(func() error {
		db, err := sql.Open("mysql", fmt.Sprintf("root:secret@(%s)/iruma?parseTime=true", dbHostPort))
		if err != nil {
			return fmt.Errorf("fail to sql.Open: %w", err)
		}
		if err := db.Ping(); err != nil {
			return fmt.Errorf("fail to ping: %w", err)
		}
		if _, err := db.Exec(`
CREATE TABLE foo (
  col_bit          BIT(4)               NOT NULL COMMENT 'column type: BIT',
  col_s_tiny_int   TINYINT              NOT NULL COMMENT 'column type: TINYINT',
  col_u_tiny_int   TINYINT UNSIGNED     NOT NULL COMMENT 'column type: TINYINT UNSIGNED',
  col_s_small_int  SMALLINT             NOT NULL COMMENT 'column type: SMALLINT',
  col_u_small_int  SMALLINT UNSIGNED    NOT NULL COMMENT 'column type: SMALLINT UNSIGNED',
  col_s_medium_int MEDIUMINT            NOT NULL COMMENT 'column type: MEDIUMINT',
  col_u_medium_int MEDIUMINT UNSIGNED   NOT NULL COMMENT 'column type: MEDIUMINT UNSIGNED',
  col_s_int        INT                  NOT NULL COMMENT 'column type: INT',
  col_u_int        INT UNSIGNED         NOT NULL COMMENT 'column type: INT UNSIGNED',
  col_s_big_int    BIGINT               NOT NULL COMMENT 'column type: BIGINT',
  col_u_big_int    BIGINT UNSIGNED      NOT NULL COMMENT 'column type: BIGINT UNSIGNED',
  col_s_float      FLOAT                NOT NULL COMMENT 'column type: FLOAT',
  col_u_float      FLOAT UNSIGNED       NOT NULL COMMENT 'column type: FLOAT UNSIGNED',
  col_s_double     DOUBLE               NOT NULL COMMENT 'column type: DOUBLE',
  col_u_double     DOUBLE UNSIGNED      NOT NULL COMMENT 'column type: DOUBLE UNSIGNED',
  col_boolean      BOOLEAN              NOT NULL COMMENT 'column type: BOOLEAN',
  col_date         DATE                 NOT NULL COMMENT 'column type: DATE',
  col_time         TIME                 NOT NULL COMMENT 'column type: TIME',
  col_datetime     DATETIME             NOT NULL COMMENT 'column type: DATETIME',
  col_timestamp    TIMESTAMP            NOT NULL COMMENT 'column type: TIMESTAMP',
  col_varchar      VARCHAR(20)          NOT NULL COMMENT 'column type: VARCHAR',
  col_text         TEXT                 NOT NULL COMMENT 'column type: TEXT',
  col_enum         ENUM('a', 'b', 'c')  NOT NULL COMMENT 'column type: ENUM'
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COMMENT = 'this is the test table'
;
		`); err != nil {
			return fmt.Errorf("fail to create table: %w", err)
		}
		return nil
	}); err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	code := m.Run()

	if err := pool.Purge(resource); err != nil {
		log.Fatalf("Could not purge resource: %s", err)
	}

	os.Exit(code)
}

func TestMySQL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	url, err := dburl.Parse(fmt.Sprintf("mysql://iruma:iruma@%s/iruma", dbHostPort))
	require.NoError(t, err)

	db, err := lookupRDBMS(url)
	require.NoError(t, err)

	tables, err := db.getTables(ctx)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(tables), 1)

	table := tables[0]
	assert.Equal(t, "foo", table.Name)
	assert.Equal(t, "this is the test table", table.Comment)

	columns, err := db.getColumns(ctx, table.Name)
	require.NoError(t, err)
	require.Greater(t, len(columns), 1)

	colMap := map[string]*Column{}
	for _, c := range columns {
		colMap[c.Name] = c
	}

	tests := []struct {
		name    string
		sqlType string
		comment string
	}{
		{name: "col_bit", sqlType: "bit(4)", comment: "column type: BIT"},
		{name: "col_s_tiny_int", sqlType: "tinyint", comment: "column type: TINYINT"},
		{name: "col_u_tiny_int", sqlType: "tinyint unsigned", comment: "column type: TINYINT UNSIGNED"},
		{name: "col_s_small_int", sqlType: "smallint", comment: "column type: SMALLINT"},
		{name: "col_u_small_int", sqlType: "smallint unsigned", comment: "column type: SMALLINT UNSIGNED"},
		{name: "col_s_medium_int", sqlType: "mediumint", comment: "column type: MEDIUMINT"},
		{name: "col_u_medium_int", sqlType: "mediumint unsigned", comment: "column type: MEDIUMINT UNSIGNED"},
		{name: "col_s_int", sqlType: "int", comment: "column type: INT"},
		{name: "col_u_int", sqlType: "int unsigned", comment: "column type: INT UNSIGNED"},
		{name: "col_s_big_int", sqlType: "bigint", comment: "column type: BIGINT"},
		{name: "col_u_big_int", sqlType: "bigint unsigned", comment: "column type: BIGINT UNSIGNED"},
		{name: "col_s_float", sqlType: "float", comment: "column type: FLOAT"},
		{name: "col_u_float", sqlType: "float unsigned", comment: "column type: FLOAT UNSIGNED"},
		{name: "col_s_double", sqlType: "double", comment: "column type: DOUBLE"},
		{name: "col_u_double", sqlType: "double unsigned", comment: "column type: DOUBLE UNSIGNED"},
		{name: "col_boolean", sqlType: "tinyint(1)", comment: "column type: BOOLEAN"},
		{name: "col_date", sqlType: "date", comment: "column type: DATE"},
		{name: "col_time", sqlType: "time", comment: "column type: TIME"},
		{name: "col_datetime", sqlType: "datetime", comment: "column type: DATETIME"},
		{name: "col_timestamp", sqlType: "timestamp", comment: "column type: TIMESTAMP"},
		{name: "col_varchar", sqlType: "varchar(20)", comment: "column type: VARCHAR"},
		{name: "col_text", sqlType: "text", comment: "column type: TEXT"},
		{name: "col_enum", sqlType: "enum('a','b','c')", comment: "column type: ENUM"},
	}
	for _, tt := range tests {
		t.Run(tt.sqlType, func(t *testing.T) {
			col, ok := colMap[tt.name]
			require.True(t, ok)
			assert.Equal(t, tt.name, col.Name)
			assert.Equal(t, tt.sqlType, col.SQLType)
			assert.Equal(t, tt.comment, col.Comment)
		})
	}
}
