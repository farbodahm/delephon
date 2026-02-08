package main

import (
	"context"
	"fmt"
	"sort"
	"time"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/driver/desktop"
	"fyne.io/fyne/v2/widget"

	"github.com/farbod/delephon/bq"
	"github.com/farbod/delephon/store"
	"github.com/farbod/delephon/ui"
)

type App struct {
	window fyne.Window
	store  *store.Store
	bqMgr  *bq.Client

	explorer  *ui.Explorer
	editor    *ui.Editor
	results   *ui.Results
	schema    *ui.SchemaView
	history   *ui.History
	favorites *ui.Favorites

	ctx       context.Context
	cancelRun context.CancelFunc
	projects  []string
}

func NewApp(window fyne.Window, st *store.Store, ctx context.Context) *App {
	a := &App{
		window: window,
		store:  st,
		bqMgr:  bq.NewManager(ctx),
		ctx:    ctx,
	}

	a.explorer = ui.NewExplorer()
	a.editor = ui.NewEditor()
	a.results = ui.NewResults()
	a.schema = ui.NewSchemaView()
	a.history = ui.NewHistory()
	a.favorites = ui.NewFavorites()

	a.wireCallbacks()
	return a
}

func (a *App) wireCallbacks() {
	// Explorer: load children
	a.explorer.LoadChildren = func(nodeID string) ([]string, error) {
		kind, project, dataset, _ := ui.ParseNodeID(nodeID)
		switch kind {
		case "p":
			datasets, err := a.bqMgr.ListDatasets(a.ctx, project)
			if err != nil {
				return nil, err
			}
			sort.Strings(datasets)
			ids := make([]string, len(datasets))
			for i, ds := range datasets {
				ids[i] = ui.DatasetNodeID(project, ds)
			}
			return ids, nil
		case "d":
			tables, err := a.bqMgr.ListTables(a.ctx, project, dataset)
			if err != nil {
				return nil, err
			}
			sort.Strings(tables)
			ids := make([]string, len(tables))
			for i, t := range tables {
				ids[i] = ui.TableNodeID(project, dataset, t)
			}
			return ids, nil
		}
		return nil, nil
	}

	// Explorer: favorite project selected -> set project in editor & expand in tree
	a.explorer.OnFavSelected = func(project string) {
		a.editor.SetProject(project)
		a.explorer.Tree.OpenBranch(ui.ProjectNodeID(project))
	}

	// Explorer: table selected -> show schema
	a.explorer.OnTableSelected = func(project, dataset, table string) {
		go func() {
			schema, err := a.bqMgr.GetTableSchema(a.ctx, project, dataset, table)
			if err != nil {
				a.showError("Schema Error", err)
				return
			}
			fields := make([]ui.SchemaField, len(schema.Fields))
			for i, f := range schema.Fields {
				fields[i] = ui.SchemaField{
					Name:        f.Name,
					Type:        f.Type,
					Mode:        f.Mode,
					Description: f.Description,
				}
			}
			a.schema.SetSchema(project, dataset, table, fields)
		}()
	}

	// Editor: run query
	a.editor.RunQuery = func(project, sql string) {
		go a.runQuery(project, sql)
	}

	// Editor: stop
	a.editor.OnStop = func() {
		if a.cancelRun != nil {
			a.cancelRun()
		}
	}

	// History: select -> load SQL
	a.history.OnSelect = func(sql string) {
		a.editor.SetSQL(sql)
	}
	a.history.OnRefresh = func() {
		go a.refreshHistory()
	}

	// Favorites: select -> load SQL
	a.favorites.OnSelect = func(sql string) {
		a.editor.SetSQL(sql)
	}
	a.favorites.OnRefresh = func() {
		go a.refreshFavorites()
	}
}

func (a *App) runQuery(project, sqlText string) {
	if a.cancelRun != nil {
		a.cancelRun()
	}
	ctx, cancel := context.WithCancel(a.ctx)
	a.cancelRun = cancel
	defer cancel()

	a.results.SetStatus("Running query...")
	start := time.Now()

	result, err := a.bqMgr.RunQuery(ctx, project, sqlText)
	dur := time.Since(start)

	if err != nil {
		a.results.SetStatus(fmt.Sprintf("Error: %v", err))
		_ = a.store.AddHistory(sqlText, project, dur, 0, err.Error())
		a.refreshHistory()
		return
	}

	a.results.SetData(result.Columns, result.Rows)
	a.results.SetStatus(fmt.Sprintf("%d rows | %s | %.2f MB processed",
		result.RowCount,
		result.Duration.Round(time.Millisecond),
		float64(result.BytesProcessed)/(1024*1024),
	))

	_ = a.store.AddHistory(sqlText, project, dur, result.RowCount, "")
	a.refreshHistory()
}

func (a *App) refreshHistory() {
	entries, err := a.store.ListHistory(200)
	if err != nil {
		return
	}
	uiEntries := make([]ui.HistoryEntry, len(entries))
	for i, e := range entries {
		uiEntries[i] = ui.HistoryEntry{
			ID:        e.ID,
			SQL:       e.SQL,
			Project:   e.Project,
			Timestamp: e.Timestamp,
			Duration:  e.Duration,
			RowCount:  e.RowCount,
			Error:     e.Error,
		}
	}
	a.history.SetEntries(uiEntries)
}

func (a *App) refreshFavorites() {
	entries, err := a.store.ListFavorites()
	if err != nil {
		return
	}
	uiEntries := make([]ui.FavoriteEntry, len(entries))
	for i, e := range entries {
		uiEntries[i] = ui.FavoriteEntry{
			ID:      e.ID,
			Name:    e.Name,
			SQL:     e.SQL,
			Project: e.Project,
		}
	}
	a.favorites.SetEntries(uiEntries)
}

func (a *App) saveFavorite() {
	sql := a.editor.GetCurrentSQL()
	if sql == "" {
		return
	}
	nameEntry := widget.NewEntry()
	nameEntry.SetPlaceHolder("Favorite name")
	dialog.ShowForm("Save Favorite", "Save", "Cancel",
		[]*widget.FormItem{widget.NewFormItem("Name", nameEntry)},
		func(ok bool) {
			if !ok || nameEntry.Text == "" {
				return
			}
			project := a.editor.GetCurrentProject()
			if err := a.store.AddFavorite(nameEntry.Text, sql, project); err != nil {
				a.showError("Save Error", err)
				return
			}
			a.refreshFavorites()
		},
		a.window,
	)
}

func (a *App) addProject() {
	entry := widget.NewEntry()
	entry.SetPlaceHolder("GCP Project ID")
	dialog.ShowForm("Add Project", "Add", "Cancel",
		[]*widget.FormItem{widget.NewFormItem("Project ID", entry)},
		func(ok bool) {
			if !ok || entry.Text == "" {
				return
			}
			a.explorer.AddProject(entry.Text)
			a.projects = append(a.projects, entry.Text)
			sort.Strings(a.projects)
			a.editor.SetProjects(a.projects)
		},
		a.window,
	)
}

func (a *App) LoadProjects() {
	go func() {
		projects, err := a.bqMgr.ListProjects(a.ctx)
		if err != nil {
			fmt.Printf("Failed to list projects: %v\n", err)
			return
		}
		sort.Strings(projects)
		a.projects = projects
		a.explorer.SetProjects(projects)
		a.editor.SetProjects(projects)
		a.refreshFavProjects()
	}()
}

func (a *App) refreshFavProjects() {
	favs, err := a.store.ListFavoriteProjects()
	if err != nil {
		return
	}
	a.explorer.SetFavProjects(favs)
}

func (a *App) toggleFavProject() {
	project := a.editor.GetCurrentProject()
	if project == "" {
		return
	}
	isFav, _ := a.store.IsFavoriteProject(project)
	if isFav {
		_ = a.store.RemoveFavoriteProject(project)
	} else {
		_ = a.store.AddFavoriteProject(project)
	}
	a.refreshFavProjects()
}

func (a *App) BuildUI() fyne.CanvasObject {
	// Bottom tabs: Results | Schema | History | Favorites
	bottomTabs := container.NewAppTabs(
		container.NewTabItem("Results", a.results.Container),
		container.NewTabItem("Schema", a.schema.Container),
		container.NewTabItem("History", a.history.Container),
		container.NewTabItem("Favorites", a.favorites.Container),
	)

	// Right side: editor (top) | bottom tabs
	rightSplit := container.NewVSplit(a.editor.Container, bottomTabs)
	rightSplit.Offset = 0.4

	// Main: explorer (left) | right
	mainSplit := container.NewHSplit(a.explorer.Container, rightSplit)
	mainSplit.Offset = 0.2

	// Toolbar
	toolbar := container.NewHBox(
		widget.NewButton("Run", func() {
			project := a.editor.GetCurrentProject()
			sql := a.editor.GetCurrentSQL()
			if project != "" && sql != "" {
				go a.runQuery(project, sql)
			}
		}),
		widget.NewButton("Stop", func() {
			if a.cancelRun != nil {
				a.cancelRun()
			}
		}),
		widget.NewButton("Save Favorite", a.saveFavorite),
		widget.NewButton("Star Project", a.toggleFavProject),
		widget.NewButton("Add Project", a.addProject),
	)

	// Register Ctrl+Enter shortcut
	a.window.Canvas().AddShortcut(
		&desktop.CustomShortcut{KeyName: fyne.KeyReturn, Modifier: fyne.KeyModifierControl},
		func(shortcut fyne.Shortcut) {
			project := a.editor.GetCurrentProject()
			sql := a.editor.GetCurrentSQL()
			if project != "" && sql != "" {
				go a.runQuery(project, sql)
			}
		},
	)

	return container.NewBorder(toolbar, nil, nil, nil, mainSplit)
}

func (a *App) showError(title string, err error) {
	dialog.ShowError(err, a.window)
}

func (a *App) Close() {
	a.bqMgr.Close()
}
