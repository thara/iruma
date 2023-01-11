package main

type Table struct {
	Name    string
	Comment string
}

type Column struct {
	Name    string
	SQLType string
	Comment string

	AutoIncrement bool
}
