package store

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

type HistoryEntry struct {
	ID        int64
	SQL       string
	Project   string
	Timestamp time.Time
	Duration  time.Duration
	RowCount  int64
	Error     string
}

type Favorite struct {
	ID      int64
	Name    string
	SQL     string
	Project string
}

type Conversation struct {
	ID        int64
	Title     string
	CreatedAt time.Time
	UpdatedAt time.Time
}

type ConversationMessage struct {
	ID             int64
	ConversationID int64
	Role           string
	Content        string
	SQL            string
	CreatedAt      time.Time
}

type Store struct {
	db *sql.DB
}

func dbPath() (string, error) {
	configDir, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	dir := filepath.Join(configDir, "delephon")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", err
	}
	return filepath.Join(dir, "delephon.db"), nil
}

func New() (*Store, error) {
	path, err := dbPath()
	if err != nil {
		return nil, fmt.Errorf("config dir: %w", err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	return newWithDB(db)
}

func newWithDB(db *sql.DB) (*Store, error) {
	if _, err := db.Exec(`PRAGMA foreign_keys = ON`); err != nil {
		db.Close()
		return nil, fmt.Errorf("enable foreign keys: %w", err)
	}
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return s, nil
}

func (s *Store) migrate() error {
	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS history (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			sql_text TEXT NOT NULL,
			project TEXT NOT NULL,
			timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			duration_ms INTEGER NOT NULL DEFAULT 0,
			row_count INTEGER NOT NULL DEFAULT 0,
			error TEXT NOT NULL DEFAULT ''
		);
		CREATE TABLE IF NOT EXISTS favorites (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL,
			sql_text TEXT NOT NULL,
			project TEXT NOT NULL DEFAULT ''
		);
		CREATE TABLE IF NOT EXISTS settings (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL DEFAULT ''
		);
		CREATE TABLE IF NOT EXISTS favorite_projects (
			project_id TEXT PRIMARY KEY
		);
		CREATE TABLE IF NOT EXISTS ai_conversations (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			title TEXT NOT NULL DEFAULT '',
			created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
		);
		CREATE TABLE IF NOT EXISTS ai_messages (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			conversation_id INTEGER NOT NULL REFERENCES ai_conversations(id) ON DELETE CASCADE,
			role TEXT NOT NULL,
			content TEXT NOT NULL,
			sql_text TEXT NOT NULL DEFAULT '',
			created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
		);
	`)
	return err
}

func (s *Store) Close() error {
	return s.db.Close()
}

// History

func (s *Store) AddHistory(sqlText, project string, dur time.Duration, rowCount int64, queryErr string) error {
	_, err := s.db.Exec(
		`INSERT INTO history (sql_text, project, timestamp, duration_ms, row_count, error) VALUES (?, ?, ?, ?, ?, ?)`,
		sqlText, project, time.Now(), dur.Milliseconds(), rowCount, queryErr,
	)
	return err
}

func (s *Store) ListHistory(limit int) ([]HistoryEntry, error) {
	if limit <= 0 {
		limit = 200
	}
	rows, err := s.db.Query(
		`SELECT id, sql_text, project, timestamp, duration_ms, row_count, error FROM history ORDER BY timestamp DESC LIMIT ?`,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var entries []HistoryEntry
	for rows.Next() {
		var e HistoryEntry
		var ms int64
		if err := rows.Scan(&e.ID, &e.SQL, &e.Project, &e.Timestamp, &ms, &e.RowCount, &e.Error); err != nil {
			return nil, err
		}
		e.Duration = time.Duration(ms) * time.Millisecond
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

func (s *Store) ClearHistory() error {
	_, err := s.db.Exec(`DELETE FROM history`)
	return err
}

// Favorites

func (s *Store) AddFavorite(name, sqlText, project string) error {
	_, err := s.db.Exec(
		`INSERT INTO favorites (name, sql_text, project) VALUES (?, ?, ?)`,
		name, sqlText, project,
	)
	return err
}

func (s *Store) ListFavorites() ([]Favorite, error) {
	rows, err := s.db.Query(`SELECT id, name, sql_text, project FROM favorites ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var favs []Favorite
	for rows.Next() {
		var f Favorite
		if err := rows.Scan(&f.ID, &f.Name, &f.SQL, &f.Project); err != nil {
			return nil, err
		}
		favs = append(favs, f)
	}
	return favs, rows.Err()
}

func (s *Store) DeleteFavorite(id int64) error {
	_, err := s.db.Exec(`DELETE FROM favorites WHERE id = ?`, id)
	return err
}

// Settings

func (s *Store) GetSetting(key string) (string, error) {
	var val string
	err := s.db.QueryRow(`SELECT value FROM settings WHERE key = ?`, key).Scan(&val)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return val, err
}

func (s *Store) SetSetting(key, value string) error {
	_, err := s.db.Exec(
		`INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
		key, value,
	)
	return err
}

// Favorite Projects

func (s *Store) AddFavoriteProject(projectID string) error {
	_, err := s.db.Exec(
		`INSERT OR IGNORE INTO favorite_projects (project_id) VALUES (?)`,
		projectID,
	)
	return err
}

func (s *Store) RemoveFavoriteProject(projectID string) error {
	_, err := s.db.Exec(`DELETE FROM favorite_projects WHERE project_id = ?`, projectID)
	return err
}

func (s *Store) ListFavoriteProjects() ([]string, error) {
	rows, err := s.db.Query(`SELECT project_id FROM favorite_projects ORDER BY project_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var projects []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, err
		}
		projects = append(projects, p)
	}
	return projects, rows.Err()
}

// Recent Projects (derived from history)

func (s *Store) ListRecentProjects(limit int) ([]string, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.db.Query(
		`SELECT project FROM history WHERE project != '' GROUP BY project ORDER BY MAX(timestamp) DESC LIMIT ?`,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var projects []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, err
		}
		projects = append(projects, p)
	}
	return projects, rows.Err()
}

func (s *Store) IsFavoriteProject(projectID string) (bool, error) {
	var count int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM favorite_projects WHERE project_id = ?`, projectID).Scan(&count)
	return count > 0, err
}

// AI Conversations

func (s *Store) CreateConversation(title string) (int64, error) {
	res, err := s.db.Exec(
		`INSERT INTO ai_conversations (title, created_at, updated_at) VALUES (?, ?, ?)`,
		title, time.Now(), time.Now(),
	)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (s *Store) AddConversationMessage(conversationID int64, role, content, sqlText string) error {
	_, err := s.db.Exec(
		`INSERT INTO ai_messages (conversation_id, role, content, sql_text, created_at) VALUES (?, ?, ?, ?, ?)`,
		conversationID, role, content, sqlText, time.Now(),
	)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(`UPDATE ai_conversations SET updated_at = ? WHERE id = ?`, time.Now(), conversationID)
	return err
}

func (s *Store) ListConversations(limit int) ([]Conversation, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.db.Query(
		`SELECT id, title, created_at, updated_at FROM ai_conversations ORDER BY updated_at DESC LIMIT ?`,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var convos []Conversation
	for rows.Next() {
		var c Conversation
		if err := rows.Scan(&c.ID, &c.Title, &c.CreatedAt, &c.UpdatedAt); err != nil {
			return nil, err
		}
		convos = append(convos, c)
	}
	return convos, rows.Err()
}

func (s *Store) GetConversationMessages(conversationID int64) ([]ConversationMessage, error) {
	rows, err := s.db.Query(
		`SELECT id, conversation_id, role, content, sql_text, created_at FROM ai_messages WHERE conversation_id = ? ORDER BY id ASC`,
		conversationID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var msgs []ConversationMessage
	for rows.Next() {
		var m ConversationMessage
		if err := rows.Scan(&m.ID, &m.ConversationID, &m.Role, &m.Content, &m.SQL, &m.CreatedAt); err != nil {
			return nil, err
		}
		msgs = append(msgs, m)
	}
	return msgs, rows.Err()
}

func (s *Store) DeleteConversation(id int64) error {
	_, err := s.db.Exec(`DELETE FROM ai_conversations WHERE id = ?`, id)
	return err
}

func (s *Store) ClearConversations() error {
	_, err := s.db.Exec(`DELETE FROM ai_conversations`)
	return err
}
