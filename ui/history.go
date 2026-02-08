package ui

import (
	"fmt"
	"time"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/widget"
)

type HistoryEntry struct {
	ID        int64
	SQL       string
	Project   string
	Timestamp time.Time
	Duration  time.Duration
	RowCount  int64
	Error     string
}

type OnHistorySelectFunc func(sql string)

type History struct {
	list    *widget.List
	entries []HistoryEntry

	OnSelect    OnHistorySelectFunc
	OnRefresh   func()

	Container fyne.CanvasObject
}

func NewHistory() *History {
	h := &History{}

	refreshBtn := widget.NewButton("Refresh", func() {
		if h.OnRefresh != nil {
			h.OnRefresh()
		}
	})
	clearBtn := widget.NewButton("Clear", func() {
		h.entries = nil
		h.list.Refresh()
	})
	toolbar := container.NewHBox(refreshBtn, clearBtn)

	h.list = widget.NewList(
		func() int { return len(h.entries) },
		func() fyne.CanvasObject {
			return widget.NewLabel("")
		},
		func(id widget.ListItemID, obj fyne.CanvasObject) {
			label := obj.(*widget.Label)
			if id >= len(h.entries) {
				return
			}
			e := h.entries[id]
			ts := e.Timestamp.Format("15:04:05")
			sql := e.SQL
			if len(sql) > 80 {
				sql = sql[:80] + "..."
			}
			if e.Error != "" {
				label.SetText(fmt.Sprintf("[%s] ERR: %s", ts, sql))
			} else {
				label.SetText(fmt.Sprintf("[%s] %s (%d rows, %s)", ts, sql, e.RowCount, e.Duration.Round(time.Millisecond)))
			}
		},
	)

	h.list.OnSelected = func(id widget.ListItemID) {
		if id < len(h.entries) && h.OnSelect != nil {
			h.OnSelect(h.entries[id].SQL)
		}
		h.list.UnselectAll()
	}

	h.Container = container.NewBorder(toolbar, nil, nil, nil, h.list)
	return h
}

func (h *History) SetEntries(entries []HistoryEntry) {
	h.entries = entries
	h.list.Refresh()
}
