package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"github.com/xo/dburl"
)

func main() {
	url := flag.Args()[0]

	if err := run(url); err != nil {
		log.Fatal(err)
	}
}

func run(urlString string) error {
	url, err := dburl.Parse(urlString)
	if err != nil {
		return fmt.Errorf("fail to parse URL: %w", err)
	}

	db, err := lookupRDBMS(url)
	if err != nil {
		return err
	}

	ctx := context.Background()

	tables, err := db.getTables(ctx)
	if err != nil {
		return err
	}

	columnsMap := map[string][]*Column{}
	for _, t := range tables {
		columns, err := db.getColumns(ctx, t.Name)
		if err != nil {
			return fmt.Errorf("fail to get columns for %s: %w", t.Name, err)
		}
		columnsMap[t.Name] = columns
	}

	data := templateData{Tables: tables, columnsMap: columnsMap}
	fmt.Println(data)
	return nil
}
