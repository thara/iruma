package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/xo/dburl"

	_ "github.com/go-sql-driver/mysql"
)

type mysql struct {
	db     *sql.DB
	dbName string
}

func (d *mysql) init(u *dburl.URL) error {
	db, err := sql.Open(u.Driver, u.DSN)
	if err != nil {
		return fmt.Errorf("fail to open database connection: %w", err)
	}
	if err := db.Ping(); err != nil {
		return fmt.Errorf("fail to ping database: %w", err)
	}
	d.db = db
	d.dbName = strings.TrimPrefix(u.Path, "/")
	return nil
}

func (d *mysql) getTables(ctx context.Context) ([]*Table, error) {
	query := `
SELECT
	TABLE_NAME,
	TABLE_COMMENT
FROM
	INFORMATION_SCHEMA.TABLES
WHERE
	TABLE_SCHEMA = ?
`
	rows, err := d.db.QueryContext(ctx, query, d.dbName)
	if err != nil {
		return nil, fmt.Errorf("fail to table query: %w", err)
	}

	var tables []*Table
	for rows.Next() {
		var name string
		var comment sql.NullString
		err := rows.Scan(&name, &comment)
		if err != nil {
			return nil, fmt.Errorf("fail to scan row: %w", err)
		}
		tables = append(tables, &Table{
			Name:    name,
			Comment: comment.String,
		})
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("fail to close rows: %w", err)
	}
	return tables, nil
}

func (d *mysql) getColumns(ctx context.Context, tableName string) ([]*Column, error) {
	query := `
SELECT
	COLUMN_NAME,
	COLUMN_TYPE,
	COLUMN_COMMENT,
	EXTRA
FROM
	INFORMATION_SCHEMA.COLUMNS
WHERE
	TABLE_SCHEMA = ?
AND TABLE_NAME = ?
`
	rows, err := d.db.QueryContext(ctx, query, d.dbName, tableName)
	if err != nil {
		return nil, fmt.Errorf("fail to column query: %w", err)
	}

	var columns []*Column
	for rows.Next() {
		var col Column
		var extra string
		err := rows.Scan(&col.Name, &col.SQLType, &col.Comment, &extra)
		if err != nil {
			return nil, fmt.Errorf("fail to scan row: %w", err)
		}
		col.AutoIncrement = strings.Contains(extra, "auto_increment")

		columns = append(columns, &col)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("fail to close rows: %w", err)
	}
	return columns, nil
}
