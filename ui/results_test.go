package ui

import (
	"encoding/json"
	"strings"
	"testing"

	"fyne.io/fyne/v2"
)

func TestResults_CopyButtonDisabledInitially(t *testing.T) {
	r := NewResults()
	if !r.copyBtn.Disabled() {
		t.Error("copy button should be disabled when there's no data")
	}
}

func TestResults_CopyJSON_CopiesToClipboard(t *testing.T) {
	r := NewResults()
	r.columns = []string{"id", "name"}
	r.rows = [][]string{{"1", "Alice"}, {"2", "Bob"}}

	r.copyJSON()

	got := fyne.CurrentApp().Clipboard().Content()
	var records []map[string]string
	if err := json.Unmarshal([]byte(got), &records); err != nil {
		t.Fatalf("clipboard does not contain valid JSON: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}
	if records[0]["id"] != "1" || records[0]["name"] != "Alice" {
		t.Errorf("unexpected first record: %v", records[0])
	}
	if records[1]["id"] != "2" || records[1]["name"] != "Bob" {
		t.Errorf("unexpected second record: %v", records[1])
	}
}

func TestResults_CopyJSON_EmptyDataNoOp(t *testing.T) {
	fyne.CurrentApp().Clipboard().SetContent("sentinel")

	r := NewResults()
	// no columns or rows set
	r.copyJSON()

	got := fyne.CurrentApp().Clipboard().Content()
	if got != "sentinel" {
		t.Errorf("clipboard should be unchanged when no data, got %q", got)
	}
}

func TestResults_CopyJSON_NoColumnsNoOp(t *testing.T) {
	fyne.CurrentApp().Clipboard().SetContent("sentinel")

	r := NewResults()
	r.rows = [][]string{{"1"}}
	// no columns set
	r.copyJSON()

	got := fyne.CurrentApp().Clipboard().Content()
	if got != "sentinel" {
		t.Errorf("clipboard should be unchanged when no columns, got %q", got)
	}
}

func TestResults_CopyJSON_NoRowsNoOp(t *testing.T) {
	fyne.CurrentApp().Clipboard().SetContent("sentinel")

	r := NewResults()
	r.columns = []string{"x"}
	// no rows set
	r.copyJSON()

	got := fyne.CurrentApp().Clipboard().Content()
	if got != "sentinel" {
		t.Errorf("clipboard should be unchanged when no rows, got %q", got)
	}
}

func TestResults_CopyJSON_TooLargeNotCopied(t *testing.T) {
	fyne.CurrentApp().Clipboard().SetContent("sentinel")

	r := NewResults()
	r.columns = []string{"data"}
	bigVal := strings.Repeat("x", 1024) // 1 KB per row
	r.rows = make([][]string, 1100)     // ~1.1 MB of data
	for i := range r.rows {
		r.rows[i] = []string{bigVal}
	}

	r.copyJSON()

	got := fyne.CurrentApp().Clipboard().Content()
	if got != "sentinel" {
		t.Error("clipboard should not be updated when result exceeds 1 MB")
	}
}

func TestResults_CopyJSON_JustUnderLimit(t *testing.T) {
	r := NewResults()
	r.columns = []string{"d"}
	// Each JSON row is ~{"d":"xxx..."} ≈ overhead + value len.
	// Use a value size that keeps total just under 1 MB.
	valSize := 900
	rowCount := 1000 // ~900 KB total payload
	r.rows = make([][]string, rowCount)
	for i := range r.rows {
		r.rows[i] = []string{strings.Repeat("a", valSize)}
	}

	r.copyJSON()

	got := fyne.CurrentApp().Clipboard().Content()
	if got == "" || got == "sentinel" {
		t.Error("expected clipboard to contain JSON for result just under 1 MB")
	}
	if len(got) > maxCopySize {
		t.Errorf("expected clipboard content under %d bytes, got %d", maxCopySize, len(got))
	}
}

func TestResults_CopyJSON_PreservesSpecialChars(t *testing.T) {
	r := NewResults()
	r.columns = []string{"text"}
	r.rows = [][]string{
		{`value with "quotes" and \backslash`},
		{"line1\nline2\ttab"},
	}

	r.copyJSON()

	got := fyne.CurrentApp().Clipboard().Content()
	var records []map[string]string
	if err := json.Unmarshal([]byte(got), &records); err != nil {
		t.Fatalf("clipboard JSON is invalid: %v", err)
	}
	if records[0]["text"] != `value with "quotes" and \backslash` {
		t.Errorf("special chars not preserved: %q", records[0]["text"])
	}
	if records[1]["text"] != "line1\nline2\ttab" {
		t.Errorf("escape chars not preserved: %q", records[1]["text"])
	}
}

func TestResults_CopyJSON_SingleRow(t *testing.T) {
	r := NewResults()
	r.columns = []string{"a", "b", "c"}
	r.rows = [][]string{{"1", "2", "3"}}

	r.copyJSON()

	got := fyne.CurrentApp().Clipboard().Content()
	var records []map[string]string
	if err := json.Unmarshal([]byte(got), &records); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if records[0]["a"] != "1" || records[0]["b"] != "2" || records[0]["c"] != "3" {
		t.Errorf("unexpected record: %v", records[0])
	}
}

// buildResultsJSON unit tests

func TestBuildResultsJSON_Basic(t *testing.T) {
	columns := []string{"id", "name"}
	rows := [][]string{
		{"1", "Alice"},
		{"2", "Bob"},
	}

	data, err := buildResultsJSON(columns, rows)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var records []map[string]string
	if err := json.Unmarshal(data, &records); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}
	if records[0]["id"] != "1" || records[0]["name"] != "Alice" {
		t.Errorf("unexpected first record: %v", records[0])
	}
}

func TestBuildResultsJSON_Empty(t *testing.T) {
	data, err := buildResultsJSON([]string{"a"}, [][]string{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != "[]" {
		t.Errorf("expected '[]', got %q", string(data))
	}
}

func TestBuildResultsJSON_RowShorterThanColumns(t *testing.T) {
	columns := []string{"a", "b", "c"}
	rows := [][]string{{"1", "2"}} // missing column c

	data, err := buildResultsJSON(columns, rows)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var records []map[string]string
	if err := json.Unmarshal(data, &records); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if records[0]["a"] != "1" || records[0]["b"] != "2" {
		t.Errorf("unexpected record: %v", records[0])
	}
	if _, ok := records[0]["c"]; ok {
		t.Error("column c should be absent when row is shorter than columns")
	}
}

func TestBuildResultsJSON_SizeOver1MB(t *testing.T) {
	columns := []string{"data"}
	bigVal := strings.Repeat("x", 1024)
	rows := make([][]string, 1100)
	for i := range rows {
		rows[i] = []string{bigVal}
	}

	data, err := buildResultsJSON(columns, rows)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(data) <= maxCopySize {
		t.Errorf("expected data over %d bytes, got %d", maxCopySize, len(data))
	}
}

// Button state and status bar tests (require fyne.Do processing via test driver)

func TestResults_SetDataEnablesCopyButton(t *testing.T) {
	r := NewResults()
	if !r.copyBtn.Disabled() {
		t.Fatal("button should start disabled")
	}
	r.SetData([]string{"x"}, [][]string{{"1"}})
	if r.copyBtn.Disabled() {
		t.Error("button should be enabled after SetData")
	}
}

func TestResults_ClearDisablesCopyButton(t *testing.T) {
	r := NewResults()
	r.SetData([]string{"x"}, [][]string{{"1"}})
	r.Clear()
	if !r.copyBtn.Disabled() {
		t.Error("button should be disabled after Clear")
	}
}

func TestResults_CopyJSON_StatusBarShowsSuccess(t *testing.T) {
	r := NewResults()
	r.columns = []string{"x"}
	r.rows = [][]string{{"1"}}
	r.copyJSON()
	if r.statusBar.Text != "Copied results as JSON to clipboard" {
		t.Errorf("expected success status message, got %q", r.statusBar.Text)
	}
}

func TestResults_CopyJSON_StatusBarShowsTooLarge(t *testing.T) {
	r := NewResults()
	r.columns = []string{"data"}
	bigVal := strings.Repeat("x", 1024)
	r.rows = make([][]string, 1100)
	for i := range r.rows {
		r.rows[i] = []string{bigVal}
	}

	r.copyJSON()

	if !strings.Contains(r.statusBar.Text, "too large") {
		t.Errorf("expected 'too large' in status, got %q", r.statusBar.Text)
	}
}
