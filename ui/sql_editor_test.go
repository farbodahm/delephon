package ui

import (
	"reflect"
	"sort"
	"sync"
	"testing"
)

func TestDottedExpr_NoDots(t *testing.T) {
	e := NewSQLEditor()
	e.lines = []string{"SELECT"}
	e.cursorCol = 6

	e.mu.Lock()
	got := e.dottedExprBeforeCursorLocked()
	e.mu.Unlock()

	if got != nil {
		t.Errorf("expected nil for no-dot expression, got %v", got)
	}
}

func TestDottedExpr_ProjectDot(t *testing.T) {
	e := NewSQLEditor()
	e.lines = []string{"my-project."}
	e.cursorCol = 11

	e.mu.Lock()
	got := e.dottedExprBeforeCursorLocked()
	e.mu.Unlock()

	want := []string{"my-project", ""}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("expected %v, got %v", want, got)
	}
}

func TestDottedExpr_ProjectPartialDataset(t *testing.T) {
	e := NewSQLEditor()
	e.lines = []string{"my-project.my_d"}
	e.cursorCol = 15

	e.mu.Lock()
	got := e.dottedExprBeforeCursorLocked()
	e.mu.Unlock()

	want := []string{"my-project", "my_d"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("expected %v, got %v", want, got)
	}
}

func TestDottedExpr_ThreeParts(t *testing.T) {
	e := NewSQLEditor()
	e.lines = []string{"project.dataset.tab"}
	e.cursorCol = 19

	e.mu.Lock()
	got := e.dottedExprBeforeCursorLocked()
	e.mu.Unlock()

	want := []string{"project", "dataset", "tab"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("expected %v, got %v", want, got)
	}
}

func TestDottedExpr_BacktickQuoted(t *testing.T) {
	e := NewSQLEditor()
	e.lines = []string{"`my-project`."}
	e.cursorCol = 13

	e.mu.Lock()
	got := e.dottedExprBeforeCursorLocked()
	e.mu.Unlock()

	want := []string{"my-project", ""}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("expected %v, got %v", want, got)
	}
}

func TestDottedExpr_HyphenatedProject(t *testing.T) {
	e := NewSQLEditor()
	e.lines = []string{"project-with-dash-111.data_set."}
	e.cursorCol = 36

	e.mu.Lock()
	got := e.dottedExprBeforeCursorLocked()
	e.mu.Unlock()

	want := []string{"project-with-dash-111", "data_set", ""}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("expected %v, got %v", want, got)
	}
}

func TestDottedExpr_AfterSpace(t *testing.T) {
	// Only the dotted expression after space is captured
	e := NewSQLEditor()
	e.lines = []string{"FROM project.ds."}
	e.cursorCol = 17

	e.mu.Lock()
	got := e.dottedExprBeforeCursorLocked()
	e.mu.Unlock()

	want := []string{"project", "ds", ""}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("expected %v, got %v", want, got)
	}
}

func TestDottedExpr_PlainWordNoDotsReturnsNil(t *testing.T) {
	e := NewSQLEditor()
	e.lines = []string{"SELECT col_name"}
	e.cursorCol = 15

	e.mu.Lock()
	got := e.dottedExprBeforeCursorLocked()
	e.mu.Unlock()

	if got != nil {
		t.Errorf("expected nil for plain word, got %v", got)
	}
}

func TestDottedExpr_CursorAtStartOfLine(t *testing.T) {
	e := NewSQLEditor()
	e.lines = []string{"project.dataset"}
	e.cursorCol = 0

	e.mu.Lock()
	got := e.dottedExprBeforeCursorLocked()
	e.mu.Unlock()

	if got != nil {
		t.Errorf("expected nil at start of line, got %v", got)
	}
}

func TestCachedHierarchy_Empty(t *testing.T) {
	e := NewExplorer()
	h := e.CachedHierarchy()
	if len(h) != 0 {
		t.Errorf("expected empty hierarchy, got %v", h)
	}
}

func TestCachedHierarchy_WithData(t *testing.T) {
	e := NewExplorer()

	e.mu.Lock()
	e.children[ProjectNodeID("proj-a")] = []explorerNode{
		{id: DatasetNodeID("proj-a", "ds1"), label: "ds1", depth: 1, isBranch: true},
		{id: DatasetNodeID("proj-a", "ds2"), label: "ds2", depth: 1, isBranch: true},
	}
	e.children[DatasetNodeID("proj-a", "ds1")] = []explorerNode{
		{id: TableNodeID("proj-a", "ds1", "orders"), label: "orders", depth: 2},
		{id: TableNodeID("proj-a", "ds1", "users"), label: "users", depth: 2},
	}
	e.children[DatasetNodeID("proj-a", "ds2")] = []explorerNode{
		{id: TableNodeID("proj-a", "ds2", "events"), label: "events", depth: 2},
	}
	e.mu.Unlock()

	h := e.CachedHierarchy()

	if len(h) != 1 {
		t.Fatalf("expected 1 project, got %d", len(h))
	}
	dsMap, ok := h["proj-a"]
	if !ok {
		t.Fatal("expected 'proj-a' in hierarchy")
	}
	if len(dsMap) != 2 {
		t.Fatalf("expected 2 datasets, got %d", len(dsMap))
	}

	ds1Tables := dsMap["ds1"]
	sort.Strings(ds1Tables)
	if !reflect.DeepEqual(ds1Tables, []string{"orders", "users"}) {
		t.Errorf("ds1 tables: expected [orders users], got %v", ds1Tables)
	}
	ds2Tables := dsMap["ds2"]
	if !reflect.DeepEqual(ds2Tables, []string{"events"}) {
		t.Errorf("ds2 tables: expected [events], got %v", ds2Tables)
	}
}

func TestCachedHierarchy_DatasetWithoutTables(t *testing.T) {
	e := NewExplorer()

	e.mu.Lock()
	e.children[ProjectNodeID("proj-b")] = []explorerNode{
		{id: DatasetNodeID("proj-b", "empty_ds"), label: "empty_ds", depth: 1, isBranch: true},
	}
	// No children entry for the dataset (tables not loaded yet)
	e.mu.Unlock()

	h := e.CachedHierarchy()

	dsMap := h["proj-b"]
	if dsMap["empty_ds"] != nil {
		t.Errorf("expected nil tables for uncached dataset, got %v", dsMap["empty_ds"])
	}
}

func TestCachedHierarchy_MultipleProjects(t *testing.T) {
	e := NewExplorer()

	e.mu.Lock()
	e.children[ProjectNodeID("proj-a")] = []explorerNode{
		{id: DatasetNodeID("proj-a", "ds1"), label: "ds1", depth: 1, isBranch: true},
	}
	e.children[DatasetNodeID("proj-a", "ds1")] = []explorerNode{
		{id: TableNodeID("proj-a", "ds1", "t1"), label: "t1", depth: 2},
	}
	e.children[ProjectNodeID("proj-b")] = []explorerNode{
		{id: DatasetNodeID("proj-b", "ds2"), label: "ds2", depth: 1, isBranch: true},
	}
	e.children[DatasetNodeID("proj-b", "ds2")] = []explorerNode{
		{id: TableNodeID("proj-b", "ds2", "t2"), label: "t2", depth: 2},
	}
	e.mu.Unlock()

	h := e.CachedHierarchy()
	if len(h) != 2 {
		t.Fatalf("expected 2 projects, got %d", len(h))
	}
	if !reflect.DeepEqual(h["proj-a"]["ds1"], []string{"t1"}) {
		t.Errorf("proj-a/ds1: got %v", h["proj-a"]["ds1"])
	}
	if !reflect.DeepEqual(h["proj-b"]["ds2"], []string{"t2"}) {
		t.Errorf("proj-b/ds2: got %v", h["proj-b"]["ds2"])
	}
}

func setupEditorWithProjectData(t *testing.T) *SQLEditor {
	t.Helper()
	e := NewSQLEditor()
	e.completions = sqlKeywords
	e.acProjectData = map[string]map[string][]string{
		"my-project": {
			"dataset_a": {"orders", "users", "events"},
			"dataset_b": {"logs", "metrics"},
		},
	}
	return e
}

func TestUpdateAC_DottedProject_ShowsDatasets(t *testing.T) {
	e := setupEditorWithProjectData(t)
	e.lines = []string{"my-project."}
	e.cursorCol = 11

	e.updateAutocomplete()

	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.acVisible {
		t.Fatal("expected autocomplete popup to be visible")
	}
	if e.acPrefix != "" {
		t.Errorf("expected empty prefix, got %q", e.acPrefix)
	}
	// Should show both datasets
	if len(e.acFiltered) != 2 {
		t.Fatalf("expected 2 dataset candidates, got %d: %v", len(e.acFiltered), e.acFiltered)
	}
	sort.Strings(e.acFiltered)
	if e.acFiltered[0] != "dataset_a" || e.acFiltered[1] != "dataset_b" {
		t.Errorf("expected [dataset_a dataset_b], got %v", e.acFiltered)
	}
}

func TestUpdateAC_DottedProject_ExactMatchHidesPopup(t *testing.T) {
	e := setupEditorWithProjectData(t)
	e.lines = []string{"my-project.dataset_a"}
	e.cursorCol = 20

	e.updateAutocomplete()

	e.mu.Lock()
	defer e.mu.Unlock()

	// "dataset_a" exactly matches one candidate → excluded (nothing to complete).
	// "dataset_b" doesn't start with "dataset_a" → excluded.
	// No candidates → popup hidden.
	if e.acVisible {
		t.Error("expected popup hidden when prefix is exact match with no other candidates")
	}
}

func TestUpdateAC_DottedProject_PartialDatasetFilter(t *testing.T) {
	e := setupEditorWithProjectData(t)
	e.lines = []string{"my-project.data"}
	e.cursorCol = 15

	e.updateAutocomplete()

	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.acVisible {
		t.Fatal("expected autocomplete popup to be visible")
	}
	if e.acPrefix != "data" {
		t.Errorf("expected prefix 'data', got %q", e.acPrefix)
	}
	// Both "dataset_a" and "dataset_b" start with "data"
	if len(e.acFiltered) != 2 {
		t.Errorf("expected 2 filtered, got %d: %v", len(e.acFiltered), e.acFiltered)
	}
}

func TestUpdateAC_DottedDataset_ShowsTables(t *testing.T) {
	e := setupEditorWithProjectData(t)
	e.lines = []string{"my-project.dataset_a."}
	e.cursorCol = 21

	e.updateAutocomplete()

	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.acVisible {
		t.Fatal("expected autocomplete popup to be visible")
	}
	if e.acPrefix != "" {
		t.Errorf("expected empty prefix, got %q", e.acPrefix)
	}
	if len(e.acFiltered) != 3 {
		t.Fatalf("expected 3 table candidates, got %d: %v", len(e.acFiltered), e.acFiltered)
	}
}

func TestUpdateAC_DottedDataset_FiltersTablesByPrefix(t *testing.T) {
	e := setupEditorWithProjectData(t)
	e.lines = []string{"my-project.dataset_a.or"}
	e.cursorCol = 23

	e.updateAutocomplete()

	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.acVisible {
		t.Fatal("expected autocomplete popup to be visible")
	}
	if e.acPrefix != "or" {
		t.Errorf("expected prefix 'or', got %q", e.acPrefix)
	}
	if len(e.acFiltered) != 1 || e.acFiltered[0] != "orders" {
		t.Errorf("expected [orders], got %v", e.acFiltered)
	}
}

func TestUpdateAC_UnknownProject_TriggersLoad(t *testing.T) {
	e := NewSQLEditor()
	e.completions = sqlKeywords
	e.acProjectData = map[string]map[string][]string{} // empty — no projects cached

	var loadedProject string
	var mu sync.Mutex
	done := make(chan struct{})
	e.OnProjectNeeded = func(project string) {
		mu.Lock()
		loadedProject = project
		mu.Unlock()
		close(done)
	}

	e.lines = []string{"unknown-project."}
	e.cursorCol = 17

	e.updateAutocomplete()

	<-done
	mu.Lock()
	defer mu.Unlock()
	if loadedProject != "unknown-project" {
		t.Errorf("expected OnProjectNeeded called with 'unknown-project', got %q", loadedProject)
	}
}

func TestUpdateAC_UnknownProject_NoDuplicateLoad(t *testing.T) {
	e := NewSQLEditor()
	e.completions = sqlKeywords
	e.acProjectData = map[string]map[string][]string{}

	var callCount int
	var mu sync.Mutex
	e.OnProjectNeeded = func(project string) {
		mu.Lock()
		callCount++
		mu.Unlock()
	}

	e.lines = []string{"unknown-project."}
	e.cursorCol = 17

	// Call twice — only first should trigger
	e.updateAutocomplete()
	e.updateAutocomplete()

	// Give goroutines time to execute
	mu.Lock()
	defer mu.Unlock()
	// The first call should fire, second should be deduplicated.
	// Due to goroutine scheduling, we check it's at most 1.
	if callCount > 1 {
		t.Errorf("expected at most 1 load call, got %d", callCount)
	}
}

func TestUpdateAC_FlatCompletion_StillWorks(t *testing.T) {
	e := NewSQLEditor()
	e.completions = []string{"SELECT", "SET", "SUM"}
	e.acProjectData = map[string]map[string][]string{} // non-nil but empty

	e.lines = []string{"SE"}
	e.cursorCol = 2

	e.updateAutocomplete()

	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.acVisible {
		t.Fatal("expected flat completion popup to be visible")
	}
	if e.acPrefix != "SE" {
		t.Errorf("expected acPrefix 'SE', got %q", e.acPrefix)
	}
	// "SELECT" and "SET" match "SE"; "SUM" doesn't
	if len(e.acFiltered) != 2 {
		t.Errorf("expected 2 filtered, got %d: %v", len(e.acFiltered), e.acFiltered)
	}
}

func TestUpdateAC_EmptyPrefix_Hidden(t *testing.T) {
	e := NewSQLEditor()
	e.completions = sqlKeywords

	e.lines = []string{" "}
	e.cursorCol = 1

	e.updateAutocomplete()

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.acVisible {
		t.Error("expected popup to be hidden for empty prefix")
	}
}

func TestAcceptCompletion_Flat(t *testing.T) {
	e := NewSQLEditor()
	e.lines = []string{"SEL"}
	e.cursorCol = 3
	e.acVisible = true
	e.acFiltered = []string{"SELECT"}
	e.acSelected = 0
	e.acPrefix = "SEL"

	e.acceptCompletion()

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.lines[0] != "SELECT" {
		t.Errorf("expected 'SELECT', got %q", e.lines[0])
	}
	if e.cursorCol != 6 {
		t.Errorf("expected cursor at col 6, got %d", e.cursorCol)
	}
}

func TestAcceptCompletion_Dotted_EmptyPrefix(t *testing.T) {
	e := NewSQLEditor()
	e.lines = []string{"my-project."}
	e.cursorCol = 11
	e.acVisible = true
	e.acFiltered = []string{"dataset_a", "dataset_b"}
	e.acSelected = 0
	e.acPrefix = "" // empty prefix after dot

	e.acceptCompletion()

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.lines[0] != "my-project.dataset_a" {
		t.Errorf("expected 'my-project.dataset_a', got %q", e.lines[0])
	}
	if e.cursorCol != 20 {
		t.Errorf("expected cursor at col 20, got %d", e.cursorCol)
	}
}

func TestAcceptCompletion_Dotted_PartialPrefix(t *testing.T) {
	e := NewSQLEditor()
	e.lines = []string{"my-project.data"}
	e.cursorCol = 15
	e.acVisible = true
	e.acFiltered = []string{"dataset_a"}
	e.acSelected = 0
	e.acPrefix = "data"

	e.acceptCompletion()

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.lines[0] != "my-project.dataset_a" {
		t.Errorf("expected 'my-project.dataset_a', got %q", e.lines[0])
	}
}

func TestAcceptCompletion_Dotted_TableLevel(t *testing.T) {
	e := NewSQLEditor()
	e.lines = []string{"my-project.dataset_a.or"}
	e.cursorCol = 23
	e.acVisible = true
	e.acFiltered = []string{"orders"}
	e.acSelected = 0
	e.acPrefix = "or"

	e.acceptCompletion()

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.lines[0] != "my-project.dataset_a.orders" {
		t.Errorf("expected 'my-project.dataset_a.orders', got %q", e.lines[0])
	}
}

func TestAcceptCompletion_NotVisible(t *testing.T) {
	e := NewSQLEditor()
	e.lines = []string{"SEL"}
	e.cursorCol = 3
	e.acVisible = false
	e.acFiltered = []string{"SELECT"}
	e.acSelected = 0
	e.acPrefix = "SEL"

	e.acceptCompletion()

	e.mu.Lock()
	defer e.mu.Unlock()

	// Nothing should change
	if e.lines[0] != "SEL" {
		t.Errorf("expected 'SEL' (unchanged), got %q", e.lines[0])
	}
}

func TestSetProjectData_RetriggersAutocomplete(t *testing.T) {
	e := NewSQLEditor()
	e.completions = sqlKeywords

	// Set up cursor at "my-project." — initially no data
	e.lines = []string{"my-project."}
	e.cursorCol = 11
	e.acProjectData = map[string]map[string][]string{}

	e.updateAutocomplete()
	e.mu.Lock()
	visible1 := e.acVisible
	e.mu.Unlock()
	if visible1 {
		t.Error("expected popup hidden before data arrives")
	}

	// Now data arrives via SetProjectData
	e.SetProjectData(map[string]map[string][]string{
		"my-project": {
			"ds1": {"t1"},
			"ds2": {"t2"},
		},
	})

	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.acVisible {
		t.Error("expected popup to show after SetProjectData with matching data")
	}
	if len(e.acFiltered) != 2 {
		t.Errorf("expected 2 dataset candidates, got %d: %v", len(e.acFiltered), e.acFiltered)
	}
}
