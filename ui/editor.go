package ui

import (
	"fmt"
	"sync"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/layout"
	"fyne.io/fyne/v2/widget"
)

type RunQueryFunc func(project, sql string)

type queryTab struct {
	entry   *widget.Entry
	cancel  func()
	project string
}

type Editor struct {
	tabs     *container.DocTabs
	projects *widget.Select
	runBtn   *widget.Button
	stopBtn  *widget.Button

	mu       sync.Mutex
	tabData  map[*container.TabItem]*queryTab
	tabCount int

	RunQuery RunQueryFunc
	OnStop   func()

	Container fyne.CanvasObject
}

func NewEditor() *Editor {
	e := &Editor{
		tabData: make(map[*container.TabItem]*queryTab),
	}

	e.projects = widget.NewSelect([]string{}, func(s string) {
		e.mu.Lock()
		if tab := e.tabs.Selected(); tab != nil {
			if qt, ok := e.tabData[tab]; ok {
				qt.project = s
			}
		}
		e.mu.Unlock()
	})
	e.projects.PlaceHolder = "Select Project"

	e.runBtn = widget.NewButton("Run", e.run)
	e.stopBtn = widget.NewButton("Stop", func() {
		if e.OnStop != nil {
			e.OnStop()
		}
	})

	e.tabs = container.NewDocTabs()
	e.tabs.OnClosed = func(tab *container.TabItem) {
		e.mu.Lock()
		delete(e.tabData, tab)
		e.mu.Unlock()
	}
	e.tabs.CreateTab = func() *container.TabItem {
		return e.newTab()
	}

	// Start with one tab
	first := e.newTab()
	e.tabs.Append(first)
	e.tabs.Select(first)

	toolbar := container.NewHBox(e.projects, e.runBtn, e.stopBtn, layout.NewSpacer())
	e.Container = container.NewBorder(toolbar, nil, nil, nil, e.tabs)

	return e
}

func (e *Editor) newTab() *container.TabItem {
	e.tabCount++
	entry := widget.NewMultiLineEntry()
	entry.SetPlaceHolder("Enter SQL query...")
	entry.Wrapping = fyne.TextWrapWord

	tab := container.NewTabItem(fmt.Sprintf("Query %d", e.tabCount), container.NewScroll(entry))

	e.mu.Lock()
	e.tabData[tab] = &queryTab{
		entry:   entry,
		project: e.projects.Selected,
	}
	e.mu.Unlock()
	return tab
}

func (e *Editor) run() {
	e.mu.Lock()
	tab := e.tabs.Selected()
	qt, ok := e.tabData[tab]
	e.mu.Unlock()

	if !ok {
		return
	}
	project := qt.project
	if project == "" {
		project = e.projects.Selected
	}
	sql := qt.entry.Text
	if sql == "" || project == "" {
		return
	}
	if e.RunQuery != nil {
		e.RunQuery(project, sql)
	}
}

func (e *Editor) SetProjects(projects []string) {
	fyne.Do(func() {
		e.projects.Options = projects
		if len(projects) > 0 && e.projects.Selected == "" {
			e.projects.SetSelected(projects[0])
		}
	})
}

func (e *Editor) SetProject(project string) {
	fyne.Do(func() {
		e.projects.SetSelected(project)
	})
}

func (e *Editor) GetCurrentSQL() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	tab := e.tabs.Selected()
	if qt, ok := e.tabData[tab]; ok {
		return qt.entry.Text
	}
	return ""
}

func (e *Editor) GetCurrentProject() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	tab := e.tabs.Selected()
	if qt, ok := e.tabData[tab]; ok && qt.project != "" {
		return qt.project
	}
	return e.projects.Selected
}

func (e *Editor) SetSQL(sql string) {
	e.mu.Lock()
	tab := e.tabs.Selected()
	qt, ok := e.tabData[tab]
	e.mu.Unlock()
	if ok {
		fyne.Do(func() {
			qt.entry.SetText(sql)
		})
	}
}
