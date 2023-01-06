package main

import "context"

type rdbms interface {
	getTables(ctx context.Context) ([]*table, error)
	getColumns(ctx context.Context, tableName string) ([]*column, error)
}
