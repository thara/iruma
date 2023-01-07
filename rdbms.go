package main

import (
	"context"
	"fmt"

	"github.com/xo/dburl"
)

type rdbms interface {
	init(*dburl.URL) error
	getTables(ctx context.Context) ([]*Table, error)
	getColumns(ctx context.Context, tableName string) ([]*Column, error)
}

func lookupRDBMS(u *dburl.URL) (rdbms, error) {
	var r rdbms
	switch u.Driver {
	case "mysql":
		r = &mysql{}
	default:
		return nil, fmt.Errorf("unsupported DB driver: %s", u.Driver)
	}

	r.init(u)
	return r, nil
}
