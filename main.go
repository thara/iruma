package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"text/template"

	"github.com/xo/dburl"
)

func main() {
	url := flag.Args()[0]
	templatePath := flag.Args()[1]

	if err := run(url, templatePath); err != nil {
		log.Fatal(err)
	}
}

func run(urlString, templatePath string) error {
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

	data := struct {
		Tables []*Table
	}{
		Tables: tables,
	}

	t, err := template.ParseFiles(templatePath)
	if err != nil {
		return fmt.Errorf("fail to parse template at %s: %w", templatePath, err)
	}

	t.Funcs(template.FuncMap{
		"getColumns": func(t *Table) []*Column {
			cs, ok := columnsMap[t.Name]
			if !ok {
				return nil
			}
			return cs
		},
	})

	err = t.Execute(os.Stdout, data)
	if err != nil {
		return fmt.Errorf("fail to apply template at %s: %w", templatePath, err)
	}

	return nil
}
