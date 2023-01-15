package main

type templateData struct {
	Tables []*Table

	columnsMap map[string][]*Column
}

func (d *templateData) GetColumns(t *Table) []*Column {
	cs, ok := d.columnsMap[t.Name]
	if !ok {
		return nil
	}
	return cs
}
