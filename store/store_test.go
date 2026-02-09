package store

import (
	"database/sql"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open in-memory db: %v", err)
	}
	s, err := newWithDB(db)
	if err != nil {
		t.Fatalf("newWithDB: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestAddAndListHistory(t *testing.T) {
	s := newTestStore(t)

	// Add 3 entries with small delays so timestamps differ
	s.AddHistory("SELECT 1", "proj-a", 100*time.Millisecond, 1, "")
	s.AddHistory("SELECT 2", "proj-b", 200*time.Millisecond, 5, "")
	s.AddHistory("SELECT 3", "proj-a", 50*time.Millisecond, 0, "some error")

	// List all
	entries, err := s.ListHistory(10)
	if err != nil {
		t.Fatalf("ListHistory: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}

	// Newest first
	if entries[0].SQL != "SELECT 3" {
		t.Errorf("expected newest entry first, got %q", entries[0].SQL)
	}
	if entries[2].SQL != "SELECT 1" {
		t.Errorf("expected oldest entry last, got %q", entries[2].SQL)
	}

	// Verify fields on third entry
	if entries[0].Error != "some error" {
		t.Errorf("expected error field, got %q", entries[0].Error)
	}
	if entries[0].Project != "proj-a" {
		t.Errorf("expected project proj-a, got %q", entries[0].Project)
	}

	// List with limit
	limited, err := s.ListHistory(2)
	if err != nil {
		t.Fatalf("ListHistory(2): %v", err)
	}
	if len(limited) != 2 {
		t.Fatalf("expected 2 entries with limit, got %d", len(limited))
	}
}

func TestClearHistory(t *testing.T) {
	s := newTestStore(t)

	s.AddHistory("SELECT 1", "proj", 0, 0, "")
	s.AddHistory("SELECT 2", "proj", 0, 0, "")

	if err := s.ClearHistory(); err != nil {
		t.Fatalf("ClearHistory: %v", err)
	}

	entries, err := s.ListHistory(10)
	if err != nil {
		t.Fatalf("ListHistory: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected 0 entries after clear, got %d", len(entries))
	}
}

func TestAddAndListFavorites(t *testing.T) {
	s := newTestStore(t)

	if err := s.AddFavorite("my-query", "SELECT * FROM t", "proj-1"); err != nil {
		t.Fatalf("AddFavorite: %v", err)
	}

	favs, err := s.ListFavorites()
	if err != nil {
		t.Fatalf("ListFavorites: %v", err)
	}
	if len(favs) != 1 {
		t.Fatalf("expected 1 favorite, got %d", len(favs))
	}
	if favs[0].Name != "my-query" {
		t.Errorf("expected name 'my-query', got %q", favs[0].Name)
	}
	if favs[0].SQL != "SELECT * FROM t" {
		t.Errorf("expected SQL, got %q", favs[0].SQL)
	}
	if favs[0].Project != "proj-1" {
		t.Errorf("expected project 'proj-1', got %q", favs[0].Project)
	}
}

func TestDeleteFavorite(t *testing.T) {
	s := newTestStore(t)

	s.AddFavorite("to-delete", "SELECT 1", "proj")
	favs, _ := s.ListFavorites()
	if len(favs) != 1 {
		t.Fatalf("expected 1 favorite, got %d", len(favs))
	}

	if err := s.DeleteFavorite(favs[0].ID); err != nil {
		t.Fatalf("DeleteFavorite: %v", err)
	}

	favs, _ = s.ListFavorites()
	if len(favs) != 0 {
		t.Fatalf("expected 0 favorites after delete, got %d", len(favs))
	}
}

func TestGetSetSetting(t *testing.T) {
	s := newTestStore(t)

	// Get missing key returns empty
	val, err := s.GetSetting("nonexistent")
	if err != nil {
		t.Fatalf("GetSetting: %v", err)
	}
	if val != "" {
		t.Errorf("expected empty for missing key, got %q", val)
	}

	// Set and get
	if err := s.SetSetting("theme", "dark"); err != nil {
		t.Fatalf("SetSetting: %v", err)
	}
	val, err = s.GetSetting("theme")
	if err != nil {
		t.Fatalf("GetSetting: %v", err)
	}
	if val != "dark" {
		t.Errorf("expected 'dark', got %q", val)
	}

	// Overwrite
	if err := s.SetSetting("theme", "light"); err != nil {
		t.Fatalf("SetSetting overwrite: %v", err)
	}
	val, _ = s.GetSetting("theme")
	if val != "light" {
		t.Errorf("expected 'light' after overwrite, got %q", val)
	}
}

func TestFavoriteProjects(t *testing.T) {
	s := newTestStore(t)

	// Add favorite projects
	s.AddFavoriteProject("proj-b")
	s.AddFavoriteProject("proj-a")

	// List returns sorted
	projects, err := s.ListFavoriteProjects()
	if err != nil {
		t.Fatalf("ListFavoriteProjects: %v", err)
	}
	if len(projects) != 2 {
		t.Fatalf("expected 2 projects, got %d", len(projects))
	}
	if projects[0] != "proj-a" || projects[1] != "proj-b" {
		t.Errorf("expected [proj-a, proj-b], got %v", projects)
	}

	// IsFavoriteProject
	isFav, err := s.IsFavoriteProject("proj-a")
	if err != nil {
		t.Fatalf("IsFavoriteProject: %v", err)
	}
	if !isFav {
		t.Error("expected proj-a to be a favorite")
	}

	isFav, _ = s.IsFavoriteProject("proj-c")
	if isFav {
		t.Error("expected proj-c to NOT be a favorite")
	}

	// Remove
	s.RemoveFavoriteProject("proj-a")
	projects, _ = s.ListFavoriteProjects()
	if len(projects) != 1 {
		t.Fatalf("expected 1 project after remove, got %d", len(projects))
	}
	if projects[0] != "proj-b" {
		t.Errorf("expected [proj-b], got %v", projects)
	}

	// Add duplicate (INSERT OR IGNORE)
	s.AddFavoriteProject("proj-b")
	projects, _ = s.ListFavoriteProjects()
	if len(projects) != 1 {
		t.Fatalf("expected 1 project after duplicate add, got %d", len(projects))
	}
}

func TestListRecentProjects(t *testing.T) {
	s := newTestStore(t)

	// Add history for different projects
	s.AddHistory("SELECT 1", "proj-old", 0, 0, "")
	s.AddHistory("SELECT 2", "proj-mid", 0, 0, "")
	s.AddHistory("SELECT 3", "proj-new", 0, 0, "")
	// Add another entry for proj-old to make it the most recent
	s.AddHistory("SELECT 4", "proj-old", 0, 0, "")

	projects, err := s.ListRecentProjects(10)
	if err != nil {
		t.Fatalf("ListRecentProjects: %v", err)
	}
	if len(projects) != 3 {
		t.Fatalf("expected 3 unique projects, got %d", len(projects))
	}

	// Most recent first (proj-old had the latest entry)
	if projects[0] != "proj-old" {
		t.Errorf("expected proj-old first (most recent), got %q", projects[0])
	}
	if projects[1] != "proj-new" {
		t.Errorf("expected proj-new second, got %q", projects[1])
	}
	if projects[2] != "proj-mid" {
		t.Errorf("expected proj-mid third, got %q", projects[2])
	}

	// Limit
	limited, _ := s.ListRecentProjects(2)
	if len(limited) != 2 {
		t.Fatalf("expected 2 projects with limit, got %d", len(limited))
	}
}
