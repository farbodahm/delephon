package ui

import "testing"

func TestExtractSQL_WithSQLBlock(t *testing.T) {
	input := "Here's the query:\n```sql\nSELECT * FROM t\n```\nDone."
	got := ExtractSQL(input)
	if got != "SELECT * FROM t" {
		t.Errorf("expected 'SELECT * FROM t', got %q", got)
	}
}

func TestExtractSQL_WithGenericCodeBlock(t *testing.T) {
	input := "```\nSELECT 1\n```"
	got := ExtractSQL(input)
	if got != "SELECT 1" {
		t.Errorf("expected 'SELECT 1', got %q", got)
	}
}

func TestExtractSQL_NoBlock(t *testing.T) {
	got := ExtractSQL("just some text without code blocks")
	if got != "" {
		t.Errorf("expected empty string, got %q", got)
	}
}

func TestExtractSQL_MultiLine(t *testing.T) {
	input := "```sql\nSELECT\n  id,\n  name\nFROM users\nWHERE active = true\n```"
	got := ExtractSQL(input)
	want := "SELECT\n  id,\n  name\nFROM users\nWHERE active = true"
	if got != want {
		t.Errorf("expected %q, got %q", want, got)
	}
}

func TestExtractSQL_FirstBlockOnly(t *testing.T) {
	input := "```sql\nSELECT 1\n```\nand also\n```sql\nSELECT 2\n```"
	got := ExtractSQL(input)
	if got != "SELECT 1" {
		t.Errorf("expected first block 'SELECT 1', got %q", got)
	}
}

func TestSplitAroundSQL_NoSQL(t *testing.T) {
	parts := splitAroundSQL("just text")
	if len(parts) != 1 {
		t.Fatalf("expected 1 part, got %d", len(parts))
	}
	if parts[0].isSQL {
		t.Error("expected non-SQL part")
	}
	if parts[0].text != "just text" {
		t.Errorf("expected 'just text', got %q", parts[0].text)
	}
}

func TestSplitAroundSQL_TextAndSQL(t *testing.T) {
	input := "Here's the query:\n```sql\nSELECT 1\n```\nDone."
	parts := splitAroundSQL(input)
	if len(parts) != 3 {
		t.Fatalf("expected 3 parts, got %d", len(parts))
	}
	if parts[0].isSQL {
		t.Error("expected first part to be text")
	}
	if !parts[1].isSQL {
		t.Error("expected second part to be SQL")
	}
	if parts[1].text != "SELECT 1" {
		t.Errorf("expected SQL 'SELECT 1', got %q", parts[1].text)
	}
	if parts[2].isSQL {
		t.Error("expected third part to be text")
	}
}

func TestSplitAroundSQL_SQLOnly(t *testing.T) {
	input := "```sql\nSELECT 1\n```"
	parts := splitAroundSQL(input)
	if len(parts) != 1 {
		t.Fatalf("expected 1 part, got %d", len(parts))
	}
	if !parts[0].isSQL {
		t.Error("expected SQL part")
	}
}

func TestSplitAroundSQL_MultipleSQLBlocks(t *testing.T) {
	input := "First:\n```sql\nSELECT 1\n```\nSecond:\n```sql\nSELECT 2\n```"
	parts := splitAroundSQL(input)
	sqlCount := 0
	for _, p := range parts {
		if p.isSQL {
			sqlCount++
		}
	}
	if sqlCount != 2 {
		t.Errorf("expected 2 SQL parts, got %d", sqlCount)
	}
}

func TestAssistantMessages(t *testing.T) {
	a := NewAssistant()
	if len(a.Messages()) != 0 {
		t.Fatalf("expected 0 messages initially, got %d", len(a.Messages()))
	}

	a.messages = append(a.messages, AssistantMessage{Role: "user", Content: "hello"})
	a.messages = append(a.messages, AssistantMessage{Role: "assistant", Content: "hi", SQL: "SELECT 1"})

	msgs := a.Messages()
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(msgs))
	}
	if msgs[0].Role != "user" {
		t.Errorf("expected user role, got %q", msgs[0].Role)
	}
	if msgs[1].SQL != "SELECT 1" {
		t.Errorf("expected SQL 'SELECT 1', got %q", msgs[1].SQL)
	}
}

func TestAssistantClear(t *testing.T) {
	a := NewAssistant()
	a.messages = append(a.messages, AssistantMessage{Role: "user", Content: "hello"})
	a.messages = nil // simulate Clear's data operation
	if len(a.Messages()) != 0 {
		t.Fatalf("expected 0 messages after clear, got %d", len(a.Messages()))
	}
}
