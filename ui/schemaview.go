package ui

import (
	"fmt"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/widget"
)

type SchemaField struct {
	Name        string
	Type        string
	Mode        string
	Description string
}

type SchemaView struct {
	table    *widget.Table
	titleBar *widget.Label
	fields   []SchemaField

	Container fyne.CanvasObject
}

var schemaColumns = []string{"Name", "Type", "Mode", "Description"}

func NewSchemaView() *SchemaView {
	s := &SchemaView{
		titleBar: widget.NewLabel("Select a table to view schema"),
	}

	s.table = widget.NewTableWithHeaders(
		func() (int, int) {
			return len(s.fields), 4
		},
		func() fyne.CanvasObject {
			return widget.NewLabel("")
		},
		func(id widget.TableCellID, obj fyne.CanvasObject) {
			label := obj.(*widget.Label)
			if id.Row >= len(s.fields) {
				return
			}
			f := s.fields[id.Row]
			switch id.Col {
			case 0:
				label.SetText(f.Name)
			case 1:
				label.SetText(f.Type)
			case 2:
				label.SetText(f.Mode)
			case 3:
				label.SetText(f.Description)
			}
		},
	)

	s.table.UpdateHeader = func(id widget.TableCellID, template fyne.CanvasObject) {
		label := template.(*widget.Label)
		if id.Row < 0 && id.Col >= 0 && id.Col < len(schemaColumns) {
			label.SetText(schemaColumns[id.Col])
		} else if id.Col < 0 && id.Row >= 0 {
			label.SetText(fmt.Sprintf("%d", id.Row+1))
		}
	}

	s.table.SetColumnWidth(0, 200)
	s.table.SetColumnWidth(1, 120)
	s.table.SetColumnWidth(2, 100)
	s.table.SetColumnWidth(3, 300)

	s.Container = container.NewBorder(s.titleBar, nil, nil, nil, s.table)
	return s
}

func (s *SchemaView) SetSchema(project, dataset, table string, fields []SchemaField) {
	s.titleBar.SetText(fmt.Sprintf("%s.%s.%s", project, dataset, table))
	s.fields = fields
	s.table.Refresh()
}

func (s *SchemaView) Clear() {
	s.titleBar.SetText("Select a table to view schema")
	s.fields = nil
	s.table.Refresh()
}
