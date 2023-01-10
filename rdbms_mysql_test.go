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
  col_enum         ENUM('column type: a', 'b', 'c')  NOT NULL COMMENT 'column type: ENUM'
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

func TestMySQL_getTables(t *testing.T) {
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
}
