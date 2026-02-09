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
