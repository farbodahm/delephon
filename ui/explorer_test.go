package ui

import (
	"os"
	"testing"

	"fyne.io/fyne/v2/test"
)

func TestMain(m *testing.M) {
	test.NewApp()
	os.Exit(m.Run())
}

func TestParseNodeID_Project(t *testing.T) {
	kind, project, dataset, table := ParseNodeID("p:my-proj")
	if kind != "p" {
		t.Errorf("expected kind 'p', got %q", kind)
	}
	if project != "my-proj" {
		t.Errorf("expected project 'my-proj', got %q", project)
	}
	if dataset != "" {
		t.Errorf("expected empty dataset, got %q", dataset)
	}
	if table != "" {
		t.Errorf("expected empty table, got %q", table)
	}
}

func TestParseNodeID_Dataset(t *testing.T) {
	kind, project, dataset, table := ParseNodeID("d:proj/ds")
	if kind != "d" {
		t.Errorf("expected kind 'd', got %q", kind)
	}
	if project != "proj" {
		t.Errorf("expected project 'proj', got %q", project)
	}
	if dataset != "ds" {
		t.Errorf("expected dataset 'ds', got %q", dataset)
	}
	if table != "" {
		t.Errorf("expected empty table, got %q", table)
	}
}

func TestParseNodeID_Table(t *testing.T) {
	kind, project, dataset, table := ParseNodeID("t:proj/ds/tbl")
	if kind != "t" {
		t.Errorf("expected kind 't', got %q", kind)
	}
	if project != "proj" {
		t.Errorf("expected project 'proj', got %q", project)
	}
	if dataset != "ds" {
		t.Errorf("expected dataset 'ds', got %q", dataset)
	}
	if table != "tbl" {
		t.Errorf("expected table 'tbl', got %q", table)
	}
}

func TestParseNodeID_Invalid(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"empty", ""},
		{"single char", "x"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			kind, project, dataset, table := ParseNodeID(tc.input)
			if kind != "" || project != "" || dataset != "" || table != "" {
				t.Errorf("expected all empty for %q, got kind=%q project=%q dataset=%q table=%q",
					tc.input, kind, project, dataset, table)
			}
		})
	}
}

func TestNodeIDConstructors(t *testing.T) {
	if got := ProjectNodeID("my-proj"); got != "p:my-proj" {
		t.Errorf("ProjectNodeID: expected 'p:my-proj', got %q", got)
	}
	if got := DatasetNodeID("proj", "ds"); got != "d:proj/ds" {
		t.Errorf("DatasetNodeID: expected 'd:proj/ds', got %q", got)
	}
	if got := TableNodeID("proj", "ds", "tbl"); got != "t:proj/ds/tbl" {
		t.Errorf("TableNodeID: expected 't:proj/ds/tbl', got %q", got)
	}
}

func TestSearchMatchesTableNames(t *testing.T) {
	e := NewExplorer()

	e.mu.Lock()
	e.favProjects = []string{"proj-a"}
	// Cache datasets and tables for proj-a
	e.children[ProjectNodeID("proj-a")] = []explorerNode{
		{id: DatasetNodeID("proj-a", "raw_data"), label: "raw_data", depth: 1, isBranch: true},
	}
	e.children[DatasetNodeID("proj-a", "raw_data")] = []explorerNode{
		{id: TableNodeID("proj-a", "raw_data", "orders"), label: "orders", depth: 2},
		{id: TableNodeID("proj-a", "raw_data", "users"), label: "users", depth: 2},
	}
	e.searchFilter = "orders"
	e.mu.Unlock()

	e.rebuildVisible()

	e.mu.Lock()
	defer e.mu.Unlock()

	// Should find proj-a with matching table "raw_data.orders"
	if len(e.visible) < 2 {
		t.Fatalf("expected at least 2 visible nodes (project + table match), got %d", len(e.visible))
	}
	if e.visible[0].label != "proj-a" {
		t.Errorf("expected first node to be 'proj-a', got %q", e.visible[0].label)
	}
	if e.visible[1].label != "raw_data.orders" {
		t.Errorf("expected second node to be 'raw_data.orders', got %q", e.visible[1].label)
	}
}

func TestSearchRanksTableMatchesFirst(t *testing.T) {
	e := NewExplorer()

	e.mu.Lock()
	// proj-name-orders matches by name, proj-b matches by table
	e.favProjects = []string{"proj-name-orders", "proj-b"}
	// Cache tables for proj-b only
	e.children[ProjectNodeID("proj-b")] = []explorerNode{
		{id: DatasetNodeID("proj-b", "ds1"), label: "ds1", depth: 1, isBranch: true},
	}
	e.children[DatasetNodeID("proj-b", "ds1")] = []explorerNode{
		{id: TableNodeID("proj-b", "ds1", "orders"), label: "orders", depth: 2},
	}
	e.searchFilter = "orders"
	e.mu.Unlock()

	e.rebuildVisible()

	e.mu.Lock()
	defer e.mu.Unlock()

	// proj-b (table match) should appear before proj-name-orders (name match)
	if len(e.visible) < 3 {
		t.Fatalf("expected at least 3 visible nodes, got %d", len(e.visible))
	}
	if e.visible[0].label != "proj-b" {
		t.Errorf("expected first project to be 'proj-b' (table match), got %q", e.visible[0].label)
	}
	// Find the name-only match
	foundNameMatch := false
	for _, n := range e.visible {
		if n.label == "proj-name-orders" {
			foundNameMatch = true
			break
		}
	}
	if !foundNameMatch {
		t.Error("expected to find 'proj-name-orders' as name match in results")
	}
}

func TestSearchNameMatchWithCachedChildren(t *testing.T) {
	e := NewExplorer()

	e.mu.Lock()
	e.recentProjects = []string{"my-orders-project"}
	// Project has cached children but none match "orders"
	e.children[ProjectNodeID("my-orders-project")] = []explorerNode{
		{id: DatasetNodeID("my-orders-project", "ds1"), label: "ds1", depth: 1, isBranch: true},
	}
	e.children[DatasetNodeID("my-orders-project", "ds1")] = []explorerNode{
		{id: TableNodeID("my-orders-project", "ds1", "users"), label: "users", depth: 2},
	}
	e.searchFilter = "orders"
	e.mu.Unlock()

	e.rebuildVisible()

	e.mu.Lock()
	defer e.mu.Unlock()

	found := false
	for _, n := range e.visible {
		if n.label == "my-orders-project" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected 'my-orders-project' to appear (name matches 'orders') even when cached tables don't match")
	}
}

func TestSearchAllProjectsByName(t *testing.T) {
	e := NewExplorer()

	e.mu.Lock()
	e.favProjects = []string{"fav-proj"}
	e.recentProjects = []string{"recent-proj"}
	e.allProjects = []string{"all-proj-match"}
	e.allLoaded = true
	e.searchFilter = "match"
	e.mu.Unlock()

	e.rebuildVisible()

	e.mu.Lock()
	defer e.mu.Unlock()

	// "all-proj-match" matches the filter by name and should appear
	found := false
	for _, n := range e.visible {
		if n.label == "all-proj-match" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected 'all-proj-match' to appear in search results (name match from allProjects)")
	}
}

func TestSearchTableOnlyForFavAndRecent(t *testing.T) {
	e := NewExplorer()

	e.mu.Lock()
	e.favProjects = []string{"fav-proj"}
	e.allProjects = []string{"all-proj"}
	e.allLoaded = true
	// Cache tables for all-proj with a matching table
	e.children[ProjectNodeID("all-proj")] = []explorerNode{
		{id: DatasetNodeID("all-proj", "ds1"), label: "ds1", depth: 1, isBranch: true},
	}
	e.children[DatasetNodeID("all-proj", "ds1")] = []explorerNode{
		{id: TableNodeID("all-proj", "ds1", "orders"), label: "orders", depth: 2},
	}
	e.searchFilter = "orders"
	e.mu.Unlock()

	e.rebuildVisible()

	e.mu.Lock()
	defer e.mu.Unlock()

	// "all-proj" has a matching table but is only in allProjects — table search doesn't apply
	for _, n := range e.visible {
		if n.label == "all-proj" {
			t.Error("'all-proj' should not appear via table match (table search is only for fav+recent)")
		}
	}
}

func TestAllKnownProjects(t *testing.T) {
	e := NewExplorer()

	// Set overlapping project lists
	e.mu.Lock()
	e.favProjects = []string{"proj-c", "proj-a"}
	e.recentProjects = []string{"proj-a", "proj-b"}
	e.allProjects = []string{"proj-b", "proj-d"}
	e.mu.Unlock()

	result := e.AllKnownProjects()

	expected := []string{"proj-a", "proj-b", "proj-c", "proj-d"}
	if len(result) != len(expected) {
		t.Fatalf("expected %d projects, got %d: %v", len(expected), len(result), result)
	}
	for i, p := range expected {
		if result[i] != p {
			t.Errorf("index %d: expected %q, got %q", i, p, result[i])
		}
	}
}
