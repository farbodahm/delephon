package main

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/layout"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"

	"github.com/farbodahm/delephon/ai"
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
	assistant *ui.Assistant

	aiClient        *ai.Client
	schemaCache     string                        // cached schema context for AI
	tableSchemaCache map[string]*bq.TableSchema   // cached per-table schemas: "project.dataset.table" -> schema

	topArea           *fyne.Container
	editorSchemaSplit *container.Split
	rightSplit        *container.Split

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
	a.assistant = ui.NewAssistant()

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
			a.updateCompletions()
		}()
	}

	// Explorer: load datasets+tables for a project (search background loading)
	a.explorer.OnSearchProject = func(project string) {
		datasets, err := a.bqMgr.ListDatasets(a.ctx, project)
		if err != nil {
			return
		}
		sort.Strings(datasets)

		result := make(map[string][]string)
		var mu sync.Mutex
		var wg sync.WaitGroup
		for _, ds := range datasets {
			ds := ds
			wg.Add(1)
			go func() {
				defer wg.Done()
				tables, _ := a.bqMgr.ListTables(a.ctx, project, ds)
				sort.Strings(tables)
				mu.Lock()
				result[ds] = tables
				mu.Unlock()
			}()
		}
		wg.Wait()
		a.explorer.CacheProjectData(project, result)
		a.updateCompletions()
	}

	// Explorer: children loaded/cached → refresh completions
	a.explorer.OnChildrenChanged = func() {
		a.updateCompletions()
	}

	// Editor: project data needed for autocomplete → load datasets+tables
	a.editor.SetOnProjectNeeded(func(project string) {
		a.loadProjectDataForAutocomplete(project)
	})

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
			fyne.Do(func() { a.showSchema() })

			// Pass column names + all known names to editor for autocomplete.
			columnNames := make([]string, len(fields))
			for i, f := range fields {
				columnNames[i] = f.Name
			}
			a.updateCompletions(columnNames...)

			// Generate SELECT query
			fqn := fmt.Sprintf("`%s.%s.%s`", project, dataset, table)
			sql := fmt.Sprintf("SELECT *\nFROM %s", fqn)
			if schema.PartitionField != "" {
				sql += "\nWHERE " + a.partitionWhereClause(schema.PartitionField, schema.PartitionType)
			}
			sql += "\nLIMIT 1000"
			a.editor.SetSQL(sql)
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

	// Assistant: send message
	a.assistant.OnSendMessage = func(userMsg string) {
		go a.handleAIMessage(userMsg)
	}

	// Assistant: settings dialog
	a.assistant.SetOnShowSettings(func() {
		a.showAPIKeyDialog()
	})
}

func (a *App) runQuery(project, sqlText string) {
	if a.cancelRun != nil {
		a.cancelRun()
	}
	ctx, cancel := context.WithCancel(a.ctx)
	a.cancelRun = cancel
	defer cancel()

	a.results.SetStatus("Running query...")
	fyne.Do(func() { a.rightSplit.SetOffset(0.4) })
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
		a.updateCompletions()
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
	a.updateCompletions()
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

func (a *App) showSchema() {
	a.topArea.Objects = []fyne.CanvasObject{a.editorSchemaSplit}
	a.topArea.Refresh()
}

func (a *App) hideSchema() {
	a.topArea.Objects = []fyne.CanvasObject{a.editor.Container}
	a.topArea.Refresh()
}

func (a *App) BuildUI() fyne.CanvasObject {
	// Bottom tabs: Results | History | Favorites | AI Assistant
	bottomTabs := container.NewAppTabs(
		container.NewTabItem("Results", a.results.Container),
		container.NewTabItem("History", a.history.Container),
		container.NewTabItem("Favorites", a.favorites.Container),
		container.NewTabItem("AI Assistant", a.assistant.Container),
	)

	// Top area: editor only by default, schema appears on demand
	a.editorSchemaSplit = container.NewHSplit(a.editor.Container, a.schema.Container)
	a.editorSchemaSplit.Offset = 0.75
	a.topArea = container.NewStack(a.editor.Container)

	a.schema.OnClose = func() { a.hideSchema() }

	// Right side: top area | bottom tabs (minimized until query runs)
	a.rightSplit = container.NewVSplit(a.topArea, bottomTabs)
	a.rightSplit.Offset = 0.9

	// Main: explorer (left) | right
	mainSplit := container.NewHSplit(a.explorer.Container, a.rightSplit)
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

	return container.NewBorder(toolbar, nil, nil, nil, mainSplit)
}

func (a *App) showError(title string, err error) {
	fyne.Do(func() {
		dialog.ShowError(err, a.window)
	})
}

// updateCompletions gathers all known names (projects, datasets, tables)
// plus any extra items (e.g. column names) and sends them to the editor.
func (a *App) updateCompletions(extra ...string) {
	projects := a.explorer.AllKnownProjects()
	datasets, tables := a.explorer.AllCachedNames()

	all := make([]string, 0, len(projects)+len(datasets)+len(tables)+len(extra))
	all = append(all, projects...)
	all = append(all, datasets...)
	all = append(all, tables...)
	all = append(all, extra...)
	a.editor.SetCompletions(all)
	a.editor.SetProjectData(a.explorer.CachedHierarchy())
}

// loadProjectDataForAutocomplete loads all datasets and tables for a project
// and updates the editor's autocomplete data. Called when the editor detects
// a dotted path referencing a project whose data isn't cached yet.
func (a *App) loadProjectDataForAutocomplete(project string) {
	datasets, err := a.bqMgr.ListDatasets(a.ctx, project)
	if err != nil {
		log.Printf("autocomplete: failed to list datasets for %s: %v", project, err)
		return
	}
	sort.Strings(datasets)

	result := make(map[string][]string)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, ds := range datasets {
		ds := ds
		wg.Add(1)
		go func() {
			defer wg.Done()
			tables, _ := a.bqMgr.ListTables(a.ctx, project, ds)
			sort.Strings(tables)
			mu.Lock()
			result[ds] = tables
			mu.Unlock()
		}()
	}
	wg.Wait()
	a.explorer.CacheProjectData(project, result)
	a.updateCompletions()
}

func (a *App) handleAIMessage(userMsg string) {
	log.Printf("ai: user message: %s", userMsg)
	a.assistant.AddMessage("user", userMsg, "")
	a.assistant.SetStatus("Initializing...")

	// Initialize AI client lazily
	if a.aiClient == nil {
		apiKey, _ := a.store.GetSetting("anthropic_api_key")
		if apiKey != "" {
			log.Print("ai: using API key from settings")
			a.aiClient = ai.NewWithKey(apiKey)
		} else {
			log.Print("ai: trying ANTHROPIC_API_KEY env var")
			client, err := ai.New()
			if err != nil {
				log.Printf("ai: no API key available: %v", err)
				a.assistant.SetStatus("")
				a.assistant.AddMessage("assistant", "Please set your Anthropic API key via the Settings button or ANTHROPIC_API_KEY environment variable.", "")
				return
			}
			a.aiClient = client
		}
	}

	// Build schema context
	a.assistant.SetStatus("Gathering schema from favorite projects...")
	schemaCtx := a.buildSchemaContext()
	log.Printf("ai: schema context length: %d chars", len(schemaCtx))

	systemPrompt := "You are a BigQuery SQL expert. Generate SQL queries based on the user's description. " +
		"Always use fully-qualified table names (`project.dataset.table`). " +
		"Return SQL in a ```sql code block. " +
		"Be concise in your explanations.\n\n" +
		"Available schemas:\n" + schemaCtx

	// Build conversation history from assistant messages
	msgs := a.assistant.Messages()
	aiMsgs := make([]ai.Message, len(msgs))
	for i, m := range msgs {
		aiMsgs[i] = ai.Message{Role: m.Role, Content: m.Content}
	}

	log.Printf("ai: sending %d messages to Claude", len(aiMsgs))
	a.assistant.SetStatus("Sending to Claude...")
	resp, err := a.aiClient.Chat(a.ctx, systemPrompt, aiMsgs)
	if err != nil {
		log.Printf("ai: Claude API error: %v", err)
		a.assistant.SetStatus("")
		a.assistant.AddMessage("assistant", fmt.Sprintf("Error: %v", err), "")
		return
	}
	log.Printf("ai: got response (%d chars)", len(resp))

	// Extract SQL from response
	sql := ui.ExtractSQL(resp)
	a.assistant.AddMessage("assistant", resp, sql)
	a.assistant.SetStatus("")

	// Auto-run if SQL was found
	if sql != "" {
		sql = enforceLimitTen(sql)
		project := a.editor.GetCurrentProject()
		if project == "" {
			log.Print("ai: no project selected, skipping auto-run")
			a.assistant.SetStatus("No project selected. Please select a project in the editor.")
			return
		}
		log.Printf("ai: auto-running query on project %s", project)
		a.assistant.SetStatus("Running generated query...")
		a.runQuery(project, sql)
		a.assistant.SetStatus("")
		fyne.Do(func() { a.rightSplit.SetOffset(0.4) })
	} else {
		log.Print("ai: no SQL block found in response")
	}
}

func (a *App) buildSchemaContext() string {
	if a.schemaCache != "" {
		log.Print("ai: using cached schema context")
		return a.schemaCache
	}

	if a.tableSchemaCache == nil {
		a.tableSchemaCache = make(map[string]*bq.TableSchema)
	}

	favProjects, err := a.store.ListFavoriteProjects()
	if err != nil || len(favProjects) == 0 {
		log.Print("ai: no favorite projects for schema context")
		return "(No favorite projects found. Star a project to provide schema context.)"
	}
	log.Printf("ai: building schema for %d favorite projects", len(favProjects))

	hierarchy := a.explorer.CachedHierarchy()

	// Collect all tables that need schema fetching
	type tableRef struct {
		project, dataset, table string
	}
	var toFetch []tableRef
	for _, project := range favProjects {
		dsMap, ok := hierarchy[project]
		if !ok {
			a.loadProjectDataForAutocomplete(project)
			hierarchy = a.explorer.CachedHierarchy()
			dsMap = hierarchy[project]
		}
		if dsMap == nil {
			continue
		}
		for dataset, tables := range dsMap {
			for _, table := range tables {
				key := project + "." + dataset + "." + table
				if _, cached := a.tableSchemaCache[key]; !cached {
					toFetch = append(toFetch, tableRef{project, dataset, table})
				}
			}
		}
	}

	// Fetch uncached schemas in parallel with bounded concurrency
	if len(toFetch) > 0 {
		log.Printf("ai: fetching schemas for %d tables", len(toFetch))
		sem := make(chan struct{}, 10) // max 10 concurrent requests
		var mu sync.Mutex
		var wg sync.WaitGroup
		for _, ref := range toFetch {
			ref := ref
			wg.Add(1)
			go func() {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()
				schema, err := a.bqMgr.GetTableSchema(a.ctx, ref.project, ref.dataset, ref.table)
				key := ref.project + "." + ref.dataset + "." + ref.table
				mu.Lock()
				if err == nil {
					a.tableSchemaCache[key] = schema
				} else {
					a.tableSchemaCache[key] = nil // mark as attempted
				}
				mu.Unlock()
			}()
		}
		wg.Wait()
		log.Printf("ai: fetched %d table schemas", len(toFetch))
	}

	// Build the context string from cached data
	var b strings.Builder
	for _, project := range favProjects {
		dsMap := hierarchy[project]
		if dsMap == nil {
			continue
		}
		fmt.Fprintf(&b, "Project: %s\n", project)
		for dataset, tables := range dsMap {
			fmt.Fprintf(&b, "  Dataset: %s\n", dataset)
			for _, table := range tables {
				key := project + "." + dataset + "." + table
				schema := a.tableSchemaCache[key]
				if schema == nil {
					fmt.Fprintf(&b, "    Table: %s\n", table)
					continue
				}
				cols := make([]string, len(schema.Fields))
				for i, f := range schema.Fields {
					cols[i] = f.Name + " " + f.Type
				}
				fmt.Fprintf(&b, "    Table: %s (columns: %s)\n", table, strings.Join(cols, ", "))
			}
		}
	}

	result := b.String()
	if result == "" {
		return "(No schema data available. Star a project and expand its datasets.)"
	}
	a.schemaCache = result
	return result
}

// enforceLimitTen ensures the SQL has LIMIT 10 (no higher).
func enforceLimitTen(sql string) string {
	limitRe := regexp.MustCompile(`(?i)\bLIMIT\s+(\d+)`)
	matches := limitRe.FindStringSubmatch(sql)
	if len(matches) < 2 {
		return strings.TrimRight(sql, " \t\n;") + "\nLIMIT 10"
	}
	// Replace any existing LIMIT with 10
	return limitRe.ReplaceAllString(sql, "LIMIT 10")
}

func (a *App) showAPIKeyDialog() {
	currentKey, _ := a.store.GetSetting("anthropic_api_key")
	entry := widget.NewPasswordEntry()
	entry.SetText(currentKey)
	entry.SetPlaceHolder("sk-ant-...")
	dialog.ShowForm("Anthropic API Key", "Save", "Cancel",
		[]*widget.FormItem{widget.NewFormItem("API Key", entry)},
		func(ok bool) {
			if !ok {
				return
			}
			key := strings.TrimSpace(entry.Text)
			if err := a.store.SetSetting("anthropic_api_key", key); err != nil {
				a.showError("Settings Error", err)
				return
			}
			// Reset AI client so it picks up the new key
			if key != "" {
				a.aiClient = ai.NewWithKey(key)
			} else {
				a.aiClient = nil
			}
		},
		a.window,
	)
}

func (a *App) Close() {
	a.bqMgr.Close()
}
