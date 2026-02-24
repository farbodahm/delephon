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

// aiQueryLimit is the maximum number of rows that AI-generated queries will return.
const aiQueryLimit = 10

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

	aiClient         *ai.Client
	useTools         bool                       // feature flag: use Claude tool calling
	tableListCache   string                     // cached table list context for AI (tool-use mode)
	schemaCache      string                     // cached schema context for AI (legacy mode)
	tableSchemaCache map[string]*bq.TableSchema // cached per-table schemas (legacy mode)

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

	// Load use-tools setting (defaults to true on first use)
	if v, _ := a.store.GetSetting("use_claude_tools"); v == "false" {
		a.useTools = false
	} else {
		a.useTools = true
		_ = a.store.SetSetting("use_claude_tools", "true")
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
	a.tableListCache = "" // invalidate AI table list cache
	a.schemaCache = ""    // invalidate legacy schema cache
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

	var resp string
	var err error

	if a.useTools {
		resp, err = a.handleAIMessageWithTools()
	} else {
		resp, err = a.handleAIMessageLegacy()
	}

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
		sql = enforceQueryLimit(sql)
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

func (a *App) handleAIMessageWithTools() (string, error) {
	// List favorite projects so Claude knows which projects exist
	favProjects, _ := a.store.ListFavoriteProjects()
	projectList := ""
	if len(favProjects) > 0 {
		projectList = "Available projects: " + strings.Join(favProjects, ", ") + "\n\n"
	}

	systemPrompt := "You are a BigQuery SQL expert. Help users write and run SQL queries.\n" +
		"Always use fully-qualified table names (`project.dataset.table`).\n\n" +
		projectList +
		"STRICT RULES:\n" +
		"- Use list_datasets and list_tables to discover datasets and tables. Do NOT guess table names.\n" +
		"- NEVER guess column names or types. ALWAYS call get_table_schema FIRST before writing any SQL.\n" +
		"- Pay close attention to column types returned by get_table_schema. Use correct type casts " +
		"(e.g. use TIMESTAMP functions for TIMESTAMP columns, not DATE comparisons).\n" +
		"- After writing the query, use run_sql_query to verify it works.\n" +
		"- Briefly explain what the query does.\n"

	msgs := toAIMessages(a.assistant.Messages())
	sdkMsgs := ai.ConvertMessages(msgs)

	model, _ := a.store.GetSetting("anthropic_model")
	log.Printf("ai: sending %d messages to Claude with tools (model=%s)", len(sdkMsgs), model)

	executor := a.buildToolExecutor()
	statusFn := func(text string) { a.assistant.SetStatus(text) }
	toolCallFn := func(info ai.ToolCallInfo, result string, isError bool) {
		a.assistant.AddToolCallMessage(info.Name, info.Input, result, isError)
	}

	result, err := a.aiClient.ChatWithTools(a.ctx, model, systemPrompt, sdkMsgs, executor, statusFn, toolCallFn)
	if err != nil {
		return "", err
	}

	resp := result.Response

	// Always append the last SQL that was actually executed via tool
	if result.LastSQL != "" {
		log.Printf("ai: last tool SQL:\n%s", result.LastSQL)
		resp += "\n\n```sql\n" + result.LastSQL + "\n```"
	}

	return resp, nil
}

func (a *App) handleAIMessageLegacy() (string, error) {
	a.assistant.SetStatus("Gathering schema from favorite projects...")
	schemaCtx := a.buildSchemaContext()
	log.Printf("ai: schema context length: %d chars", len(schemaCtx))

	systemPrompt := "You are a BigQuery SQL expert. Generate SQL queries based on the user's description. " +
		"Always use fully-qualified table names (`project.dataset.table`). " +
		"Return SQL in a ```sql code block. " +
		"Be concise in your explanations.\n\n" +
		"Available schemas:\n" + schemaCtx

	msgs := toAIMessages(a.assistant.Messages())

	model, _ := a.store.GetSetting("anthropic_model")
	log.Printf("ai: sending %d messages to Claude (model=%s)", len(msgs), model)
	a.assistant.SetStatus("Sending to Claude...")
	return a.aiClient.Chat(a.ctx, model, systemPrompt, msgs)
}

func (a *App) buildTableListContext() string {
	if a.tableListCache != "" {
		log.Print("ai: using cached table list context")
		return a.tableListCache
	}

	favProjects, err := a.store.ListFavoriteProjects()
	if err != nil || len(favProjects) == 0 {
		log.Print("ai: no favorite projects for table list context")
		return "No favorite projects found. Star a project to provide table context, or use list_datasets/list_tables tools to explore."
	}
	log.Printf("ai: building table list for %d favorite projects", len(favProjects))

	hierarchy := a.explorer.CachedHierarchy()

	// Ensure all favorite projects have their data loaded
	for _, project := range favProjects {
		if _, ok := hierarchy[project]; !ok {
			a.loadProjectDataForAutocomplete(project)
		}
	}
	hierarchy = a.explorer.CachedHierarchy()

	var b strings.Builder
	b.WriteString("Available tables:\n")
	count := 0
	for _, project := range favProjects {
		dsMap := hierarchy[project]
		if dsMap == nil {
			continue
		}
		for dataset, tables := range dsMap {
			for _, table := range tables {
				fmt.Fprintf(&b, "- %s.%s.%s\n", project, dataset, table)
				count++
			}
		}
	}

	if count == 0 {
		return "No tables found in favorite projects. Use list_datasets and list_tables tools to explore."
	}

	result := b.String()
	a.tableListCache = result
	log.Printf("ai: table list context: %d tables", count)
	return result
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

	if len(toFetch) > 0 {
		log.Printf("ai: fetching schemas for %d tables", len(toFetch))
		sem := make(chan struct{}, 10)
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
					a.tableSchemaCache[key] = nil
				}
				mu.Unlock()
			}()
		}
		wg.Wait()
		log.Printf("ai: fetched %d table schemas", len(toFetch))
	}

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

func (a *App) buildToolExecutor() ai.ToolExecutor {
	return ai.ToolExecutor{
		GetTableSchema: func(ctx context.Context, project, dataset, table string) (string, error) {
			schema, err := a.bqMgr.GetTableSchema(ctx, project, dataset, table)
			if err != nil {
				return "", err
			}
			var b strings.Builder
			fmt.Fprintf(&b, "Table: %s.%s.%s\n", project, dataset, table)
			if schema.PartitionField != "" {
				fmt.Fprintf(&b, "Partitioned by: %s (%s)\n", schema.PartitionField, schema.PartitionType)
			}
			fmt.Fprintf(&b, "Columns:\n")
			for _, f := range schema.Fields {
				desc := ""
				if f.Description != "" {
					desc = " -- " + f.Description
				}
				fmt.Fprintf(&b, "  %s %s %s%s\n", f.Name, f.Type, f.Mode, desc)
			}
			return b.String(), nil
		},
		RunSQLQuery: func(ctx context.Context, project, sql string) (string, error) {
			sql = enforceQueryLimit(sql)
			result, err := a.bqMgr.RunQuery(ctx, project, sql)
			if err != nil {
				return "", err
			}
			var b strings.Builder
			fmt.Fprintf(&b, "Columns: %s\n", strings.Join(result.Columns, ", "))
			fmt.Fprintf(&b, "Rows: %d | %.2f MB processed\n", result.RowCount, float64(result.BytesProcessed)/(1024*1024))
			for i, row := range result.Rows {
				if i >= 20 { // limit rows in tool result to keep context manageable
					fmt.Fprintf(&b, "... (%d more rows)\n", int(result.RowCount)-20)
					break
				}
				fmt.Fprintf(&b, "%s\n", strings.Join(row, " | "))
			}
			return b.String(), nil
		},
		ListDatasets: func(ctx context.Context, project string) (string, error) {
			datasets, err := a.bqMgr.ListDatasets(ctx, project)
			if err != nil {
				return "", err
			}
			sort.Strings(datasets)
			return strings.Join(datasets, "\n"), nil
		},
		ListTables: func(ctx context.Context, project, dataset string) (string, error) {
			tables, err := a.bqMgr.ListTables(ctx, project, dataset)
			if err != nil {
				return "", err
			}
			sort.Strings(tables)
			return strings.Join(tables, "\n"), nil
		},
	}
}

func toAIMessages(msgs []ui.AssistantMessage) []ai.Message {
	out := make([]ai.Message, len(msgs))
	for i, m := range msgs {
		out[i] = ai.Message{Role: m.Role, Content: m.Content}
	}
	return out
}

// enforceQueryLimit ensures the SQL has a LIMIT clause capped at aiQueryLimit.
func enforceQueryLimit(sql string) string {
	limitRe := regexp.MustCompile(`(?i)\bLIMIT\s+(\d+)`)
	matches := limitRe.FindStringSubmatch(sql)
	limit := fmt.Sprintf("LIMIT %d", aiQueryLimit)
	if len(matches) < 2 {
		return strings.TrimRight(sql, " \t\n;") + "\n" + limit
	}
	return limitRe.ReplaceAllString(sql, limit)
}

func (a *App) showAPIKeyDialog() {
	currentKey, _ := a.store.GetSetting("anthropic_api_key")
	keyEntry := widget.NewPasswordEntry()
	keyEntry.SetText(currentKey)
	keyEntry.SetPlaceHolder("sk-ant-...")

	currentModel, _ := a.store.GetSetting("anthropic_model")

	// Ensure AI client is initialized if a key is available
	if a.aiClient == nil {
		if currentKey != "" {
			log.Printf("ai settings: initializing client from stored API key")
			a.aiClient = ai.NewWithKey(currentKey)
		} else if envClient, err := ai.New(); err == nil {
			log.Printf("ai settings: initializing client from ANTHROPIC_API_KEY env var")
			a.aiClient = envClient
		} else {
			log.Printf("ai settings: no API key available (no stored key, env var not set)")
		}
	}

	models := []string{"default"}
	if a.aiClient != nil {
		log.Printf("ai settings: fetching models from API...")
		if fetched, err := a.aiClient.ListModels(a.ctx); err == nil && len(fetched) > 0 {
			log.Printf("ai settings: got %d models from API: %v", len(fetched), fetched)
			models = fetched
		} else if err != nil {
			log.Printf("ai settings: failed to fetch models: %v", err)
		}
	}
	modelSelect := widget.NewSelect(models, nil)
	if currentModel != "" {
		modelSelect.SetSelected(currentModel)
	} else if len(models) > 0 {
		modelSelect.SetSelected(models[0])
	}

	useToolsCheck := widget.NewCheck("", nil)
	useToolsCheck.SetChecked(a.useTools)

	dialog.ShowForm("AI Assistant Settings", "Save", "Cancel",
		[]*widget.FormItem{
			widget.NewFormItem("API Key", keyEntry),
			widget.NewFormItem("Model", modelSelect),
			widget.NewFormItem("Use Tools", useToolsCheck),
		},
		func(ok bool) {
			if !ok {
				return
			}
			key := strings.TrimSpace(keyEntry.Text)
			if err := a.store.SetSetting("anthropic_api_key", key); err != nil {
				a.showError("Settings Error", err)
				return
			}
			if err := a.store.SetSetting("anthropic_model", modelSelect.Selected); err != nil {
				a.showError("Settings Error", err)
				return
			}
			// Save use-tools setting
			a.useTools = useToolsCheck.Checked
			if a.useTools {
				_ = a.store.SetSetting("use_claude_tools", "true")
			} else {
				_ = a.store.SetSetting("use_claude_tools", "false")
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
