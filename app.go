package main

import (
	"context"
	"fmt"
	"log"
	"sort"
	"time"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/driver/desktop"
	"fyne.io/fyne/v2/layout"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"

	"github.com/farbodahm/delephon/bq"
	"github.com/farbodahm/delephon/store"
	"github.com/farbodahm/delephon/ui"
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
			log.Printf("app: datasets for %s: %v", project, datasets)
			ids := make([]string, len(datasets))
			for i, ds := range datasets {
				ids[i] = ui.DatasetNodeID(project, ds)
			}
			log.Printf("app: dataset node IDs: %v", ids)
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

	// Explorer: project node clicked -> set project in editor
	a.explorer.OnProjectSelected = func(project string) {
		a.editor.SetProject(project)
	}

	// Explorer: load all projects on demand
	a.explorer.OnLoadAllProjects = func() {
		go func() {
			projects, err := a.bqMgr.ListProjects(a.ctx)
			if err != nil {
				log.Printf("Failed to list projects: %v", err)
				return
			}
			sort.Strings(projects)
			a.explorer.SetAllProjects(projects)
			a.editor.SetProjects(a.explorer.AllKnownProjects())
		}()
	}

	// Explorer: table selected -> show schema + generate SELECT query
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

			// Generate SELECT query
			fqn := fmt.Sprintf("`%s.%s.%s`", project, dataset, table)
			sql := fmt.Sprintf("SELECT *\nFROM %s", fqn)
			if schema.PartitionField != "" {
				sql += "\nWHERE " + a.partitionWhereClause(schema.PartitionField, schema.PartitionType)
			}
			sql += "\nLIMIT 1000"
			a.editor.SetSQL(sql)
			a.editor.SetProject(project)
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
		a.refreshRecentProjects()
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
	a.refreshRecentProjects()
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
			a.editor.SetProjects(a.explorer.AllKnownProjects())
		},
		a.window,
	)
}

// LoadInitialProjects loads favorites and recent projects from the local DB (no GCP API call).
func (a *App) LoadInitialProjects() {
	go func() {
		a.refreshFavProjects()
		a.refreshRecentProjects()
		a.editor.SetProjects(a.explorer.AllKnownProjects())
	}()
}

func (a *App) refreshFavProjects() {
	favs, err := a.store.ListFavoriteProjects()
	if err != nil {
		return
	}
	a.explorer.SetFavProjects(favs)
}

func (a *App) refreshRecentProjects() {
	projects, err := a.store.ListRecentProjects(20)
	if err != nil {
		return
	}
	a.explorer.SetRecentProjects(projects)
	a.editor.SetProjects(a.explorer.AllKnownProjects())
}

func (a *App) partitionWhereClause(field, partType string) string {
	if field == "_PARTITIONTIME" {
		// Ingestion-time partitioning: always TIMESTAMP type
		switch partType {
		case "HOUR":
			return "_PARTITIONTIME >= TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), HOUR)"
		default:
			return "_PARTITIONTIME >= TIMESTAMP(CURRENT_DATE())"
		}
	}
	// Column-based partitioning: use DATE() to handle both DATE and TIMESTAMP columns
	switch partType {
	case "DAY":
		return fmt.Sprintf("DATE(%s) = CURRENT_DATE()", field)
	case "HOUR":
		return fmt.Sprintf("DATE(%s) = CURRENT_DATE()", field)
	case "MONTH":
		return fmt.Sprintf("DATE(%s) >= DATE_TRUNC(CURRENT_DATE(), MONTH)", field)
	case "YEAR":
		return fmt.Sprintf("DATE(%s) >= DATE_TRUNC(CURRENT_DATE(), YEAR)", field)
	default:
		return fmt.Sprintf("%s IS NOT NULL", field)
	}
}

func (a *App) toggleTheme() {
	if appTheme.Variant() == theme.VariantDark {
		appTheme.SetVariant(theme.VariantLight)
		_ = a.store.SetSetting("theme_variant", "light")
	} else {
		appTheme.SetVariant(theme.VariantDark)
		_ = a.store.SetSetting("theme_variant", "dark")
	}
	fyne.CurrentApp().Settings().SetTheme(appTheme)
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
	runBtn := widget.NewButtonWithIcon("Run", theme.Icon(theme.IconNameMediaPlay), func() {
		project := a.editor.GetCurrentProject()
		sql := a.editor.GetCurrentSQL()
		if project != "" && sql != "" {
			go a.runQuery(project, sql)
		}
	})
	runBtn.Importance = widget.HighImportance

	stopBtn := widget.NewButtonWithIcon("Stop", theme.Icon(theme.IconNameMediaStop), func() {
		if a.cancelRun != nil {
			a.cancelRun()
		}
	})
	stopBtn.Importance = widget.DangerImportance

	toolbar := container.NewHBox(
		runBtn,
		stopBtn,
		widget.NewButtonWithIcon("Save Favorite", theme.Icon(theme.IconNameDocumentSave), a.saveFavorite),
		widget.NewButton("Star Project", a.toggleFavProject),
		widget.NewButtonWithIcon("Add Project", theme.Icon(theme.IconNameContentAdd), a.addProject),
		layout.NewSpacer(),
		widget.NewButtonWithIcon("", theme.Icon(theme.IconNameColorPalette), a.toggleTheme),
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
	fyne.Do(func() {
		dialog.ShowError(err, a.window)
	})
}

func (a *App) Close() {
	a.bqMgr.Close()
}
