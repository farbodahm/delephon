package ui

import (
	"fmt"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/widget"
)

type FavoriteEntry struct {
	ID      int64
	Name    string
	SQL     string
	Project string
}

type OnFavoriteSelectFunc func(sql string)
type OnFavoriteDeleteFunc func(id int64)

type Favorites struct {
	list    *widget.List
	entries []FavoriteEntry

	OnSelect OnFavoriteSelectFunc
	OnDelete OnFavoriteDeleteFunc
	OnRefresh func()

	Container fyne.CanvasObject
}

func NewFavorites() *Favorites {
	f := &Favorites{}

	refreshBtn := widget.NewButton("Refresh", func() {
		if f.OnRefresh != nil {
			f.OnRefresh()
		}
	})
	toolbar := container.NewHBox(refreshBtn)

	f.list = widget.NewList(
		func() int { return len(f.entries) },
		func() fyne.CanvasObject {
			return widget.NewLabel("")
		},
		func(id widget.ListItemID, obj fyne.CanvasObject) {
			label := obj.(*widget.Label)
			if id >= len(f.entries) {
				return
			}
			e := f.entries[id]
			label.SetText(fmt.Sprintf("%s — %s", e.Name, truncate(e.SQL, 60)))
		},
	)

	f.list.OnSelected = func(id widget.ListItemID) {
		if id < len(f.entries) && f.OnSelect != nil {
			f.OnSelect(f.entries[id].SQL)
		}
		f.list.UnselectAll()
	}

	f.Container = container.NewBorder(toolbar, nil, nil, nil, f.list)
	return f
}

func (f *Favorites) SetEntries(entries []FavoriteEntry) {
	f.entries = entries
	fyne.Do(func() {
		f.list.Refresh()
	})
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
