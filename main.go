package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"text/template"

	"github.com/iancoleman/strcase"
	"github.com/xo/dburl"
	"gopkg.in/yaml.v2"
)

var mappingPath string

func init() {
	flag.StringVar(&mappingPath, "m", "", "mapping path(YAML)")
}

func main() {
	flag.Parse()

	url := flag.Args()[0]
	templatePath := flag.Args()[1]

	if err := run(url, templatePath); err != nil {
		log.Fatal(err)
	}
}

func run(urlString, templatePath string) error {
	f, err := os.Open(templatePath)
	if err != nil {
		return fmt.Errorf("fail to open %s: %w", templatePath, err)
	}
	b, err := io.ReadAll(f)
	if err != nil {
		return fmt.Errorf("fail to read %s: %w", templatePath, err)
	}
	templateText := string(b)

	columnTypeMapper, err := loadColumnTypeMapper()
	if err != nil {
		return err
	}

	return generateContent(urlString, templateText, columnTypeMapper)
}

func generateContent(urlString, templateText string, columnTypeMapper map[string]string) error {
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

	t, err := template.New("").Funcs(template.FuncMap{
		"getColumns": func(t *Table) []*Column {
			cs, ok := columnsMap[t.Name]
			if !ok {
				return nil
			}
			return cs
		},
		"mapColumnType": func(c *Column) string {
			return columnTypeMapper[c.SQLType]
		},

		"toUpper": strings.ToUpper,
		"toLower": strings.ToLower,

		"toSnake":      strcase.ToSnake,
		"toKebab":      strcase.ToKebab,
		"toCamel":      strcase.ToCamel,
		"toLowerCamel": strcase.ToLowerCamel,
	}).Parse(templateText)
	if err != nil {
		return fmt.Errorf("fail to parse template: %w", err)
	}

	err = t.Execute(os.Stdout, data)
	if err != nil {
		return fmt.Errorf("fail to apply template: %w", err)
	}

	return nil
}

func loadColumnTypeMapper() (map[string]string, error) {
	if len(mappingPath) == 0 {
		return nil, nil
	}

	mapping := make(map[interface{}]interface{})
	b, err := os.ReadFile(mappingPath)
	if err != nil {
		return nil, fmt.Errorf("fail to read file at %s: %w", mappingPath, err)
	}
	err = yaml.Unmarshal(b, &mapping)
	if err != nil {
		return nil, fmt.Errorf("fail to unmarshal at %s: %w", mappingPath, err)
	}

	var columnTypeMapper map[string]string
	if v, ok := mapping["column_types"]; ok {
		if m, ok := v.(map[string]string); ok {
			columnTypeMapper = m
		}
	}
	return columnTypeMapper, nil
}
