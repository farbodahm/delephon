package ui

import (
	"fmt"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/widget"
)

type Results struct {
	table     *widget.Table
	statusBar *widget.Label

	columns []string
	rows    [][]string

	Container fyne.CanvasObject
}

func NewResults() *Results {
	r := &Results{
		statusBar: widget.NewLabel("Ready"),
	}

	r.table = widget.NewTableWithHeaders(
		func() (int, int) {
			if len(r.columns) == 0 {
				return 0, 0
			}
			return len(r.rows), len(r.columns)
		},
		func() fyne.CanvasObject {
			return widget.NewLabel("")
		},
		func(id widget.TableCellID, obj fyne.CanvasObject) {
			label := obj.(*widget.Label)
			if id.Row < len(r.rows) && id.Col < len(r.rows[id.Row]) {
				label.SetText(r.rows[id.Row][id.Col])
			}
		},
	)

	r.table.UpdateHeader = func(id widget.TableCellID, template fyne.CanvasObject) {
		label := template.(*widget.Label)
		if id.Row < 0 && id.Col >= 0 && id.Col < len(r.columns) {
			label.SetText(r.columns[id.Col])
		} else if id.Col < 0 && id.Row >= 0 {
			label.SetText(fmt.Sprintf("%d", id.Row+1))
		}
	}

	r.Container = container.NewBorder(nil, r.statusBar, nil, nil, r.table)
	return r
}

func (r *Results) SetData(columns []string, rows [][]string) {
	r.columns = columns
	r.rows = rows
	fyne.Do(func() {
		for i := range columns {
			r.table.SetColumnWidth(i, 150)
		}
		r.table.Refresh()
	})
}

func (r *Results) SetStatus(text string) {
	fyne.Do(func() {
		r.statusBar.SetText(text)
	})
}

func (r *Results) Clear() {
	r.columns = nil
	r.rows = nil
	fyne.Do(func() {
		r.table.Refresh()
		r.statusBar.SetText("Ready")
	})
}
