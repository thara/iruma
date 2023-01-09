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
		Repository: "mysql",
		Tag:        "8.0.31",
		Platform:   "linux/amd64",
		Env:        []string{"MYSQL_ROOT_PASSWORD=secret"},
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
		db, err := sql.Open("mysql", fmt.Sprintf("root:secret@(%s)/mysql?parseTime=true", dbHostPort))
		if err != nil {
			return err
		}
		return db.Ping()
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

	url, err := dburl.Parse(fmt.Sprintf("mysql://root:secret@%s/mysql", dbHostPort))
	require.NoError(t, err)

	db, err := lookupRDBMS(url)
	require.NoError(t, err)

	tables, err := db.getTables(ctx)
	require.NoError(t, err)
	assert.Greater(t, len(tables), 0)
}
