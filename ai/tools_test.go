package ai

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
)

func TestExecuteTool_GetTableSchema(t *testing.T) {
	var calledProject, calledDataset, calledTable string
	executor := ToolExecutor{
		GetTableSchema: func(ctx context.Context, project, dataset, table string) (string, error) {
			calledProject = project
			calledDataset = dataset
			calledTable = table
			return "schema result", nil
		},
	}
	input := json.RawMessage(`{"project":"p1","dataset":"d1","table":"t1"}`)
	result, isError := executeTool(context.Background(), "get_table_schema", input, executor)
	if isError {
		t.Fatalf("unexpected error: %s", result)
	}
	if result != "schema result" {
		t.Errorf("expected 'schema result', got %q", result)
	}
	if calledProject != "p1" || calledDataset != "d1" || calledTable != "t1" {
		t.Errorf("wrong args: %s.%s.%s", calledProject, calledDataset, calledTable)
	}
}

func TestExecuteTool_RunSQLQuery(t *testing.T) {
	var calledProject, calledSQL string
	executor := ToolExecutor{
		RunSQLQuery: func(ctx context.Context, project, sql string) (string, error) {
			calledProject = project
			calledSQL = sql
			return "query result", nil
		},
	}
	input := json.RawMessage(`{"project":"p1","sql":"SELECT 1"}`)
	result, isError := executeTool(context.Background(), "run_sql_query", input, executor)
	if isError {
		t.Fatalf("unexpected error: %s", result)
	}
	if result != "query result" {
		t.Errorf("expected 'query result', got %q", result)
	}
	if calledProject != "p1" || calledSQL != "SELECT 1" {
		t.Errorf("wrong args: project=%s sql=%s", calledProject, calledSQL)
	}
}

func TestExecuteTool_ListDatasets(t *testing.T) {
	var calledProject string
	executor := ToolExecutor{
		ListDatasets: func(ctx context.Context, project string) (string, error) {
			calledProject = project
			return "ds1\nds2", nil
		},
	}
	input := json.RawMessage(`{"project":"p1"}`)
	result, isError := executeTool(context.Background(), "list_datasets", input, executor)
	if isError {
		t.Fatalf("unexpected error: %s", result)
	}
	if result != "ds1\nds2" {
		t.Errorf("expected 'ds1\\nds2', got %q", result)
	}
	if calledProject != "p1" {
		t.Errorf("wrong project: %s", calledProject)
	}
}

func TestExecuteTool_ListTables(t *testing.T) {
	var calledProject, calledDataset string
	executor := ToolExecutor{
		ListTables: func(ctx context.Context, project, dataset string) (string, error) {
			calledProject = project
			calledDataset = dataset
			return "t1\nt2", nil
		},
	}
	input := json.RawMessage(`{"project":"p1","dataset":"d1"}`)
	result, isError := executeTool(context.Background(), "list_tables", input, executor)
	if isError {
		t.Fatalf("unexpected error: %s", result)
	}
	if result != "t1\nt2" {
		t.Errorf("expected 't1\\nt2', got %q", result)
	}
	if calledProject != "p1" || calledDataset != "d1" {
		t.Errorf("wrong args: %s.%s", calledProject, calledDataset)
	}
}

func TestExecuteTool_GetAllTables(t *testing.T) {
	called := false
	executor := ToolExecutor{
		GetAllTables: func(ctx context.Context) (string, error) {
			called = true
			return "proj.ds.t1\nproj.ds.t2", nil
		},
	}
	input := json.RawMessage(`{}`)
	result, isError := executeTool(context.Background(), "get_all_tables", input, executor)
	if isError {
		t.Fatalf("unexpected error: %s", result)
	}
	if !called {
		t.Error("expected GetAllTables to be called")
	}
	if result != "proj.ds.t1\nproj.ds.t2" {
		t.Errorf("unexpected result: %q", result)
	}
}

func TestSummarizeInput_GetAllTables(t *testing.T) {
	input := json.RawMessage(`{}`)
	s := summarizeInput("get_all_tables", input)
	if s != "all projects" {
		t.Errorf("expected 'all projects', got %q", s)
	}
}

func TestExecuteTool_UnknownTool(t *testing.T) {
	executor := ToolExecutor{}
	input := json.RawMessage(`{}`)
	result, isError := executeTool(context.Background(), "nonexistent", input, executor)
	if !isError {
		t.Fatal("expected error for unknown tool")
	}
	if result != "unknown tool: nonexistent" {
		t.Errorf("unexpected result: %q", result)
	}
}

func TestExecuteTool_InvalidJSON(t *testing.T) {
	executor := ToolExecutor{
		GetTableSchema: func(ctx context.Context, project, dataset, table string) (string, error) {
			t.Fatal("should not be called")
			return "", nil
		},
	}
	input := json.RawMessage(`{invalid`)
	result, isError := executeTool(context.Background(), "get_table_schema", input, executor)
	if !isError {
		t.Fatal("expected error for invalid JSON")
	}
	if result == "" {
		t.Error("expected non-empty error message")
	}
}

func TestExecuteTool_CallbackError(t *testing.T) {
	executor := ToolExecutor{
		GetTableSchema: func(ctx context.Context, project, dataset, table string) (string, error) {
			return "", fmt.Errorf("permission denied")
		},
	}
	input := json.RawMessage(`{"project":"p1","dataset":"d1","table":"t1"}`)
	result, isError := executeTool(context.Background(), "get_table_schema", input, executor)
	if !isError {
		t.Fatal("expected error when callback returns error")
	}
	if result != "error: permission denied" {
		t.Errorf("unexpected result: %q", result)
	}
}

func TestSummarizeInput_GetTableSchema(t *testing.T) {
	input := json.RawMessage(`{"project":"p","dataset":"d","table":"t"}`)
	s := summarizeInput("get_table_schema", input)
	if s != "p.d.t" {
		t.Errorf("expected 'p.d.t', got %q", s)
	}
}

func TestSummarizeInput_RunSQLQuery(t *testing.T) {
	input := json.RawMessage(`{"project":"myproj","sql":"SELECT * FROM foo"}`)
	s := summarizeInput("run_sql_query", input)
	if s != "myproj: SELECT * FROM foo" {
		t.Errorf("unexpected summary: %q", s)
	}
}

func TestSummarizeInput_RunSQLQuery_LongSQL(t *testing.T) {
	longSQL := ""
	for i := range 100 {
		longSQL += fmt.Sprintf("col%d, ", i)
	}
	input := json.RawMessage(fmt.Sprintf(`{"project":"p","sql":"%s"}`, longSQL))
	s := summarizeInput("run_sql_query", input)
	// Should be truncated to 80 chars + "..."
	if len(s) > len("p: ")+80+3+5 { // some margin
		t.Errorf("summary too long: %d chars", len(s))
	}
}

func TestSummarizeInput_ListDatasets(t *testing.T) {
	input := json.RawMessage(`{"project":"myproj"}`)
	s := summarizeInput("list_datasets", input)
	if s != "myproj" {
		t.Errorf("expected 'myproj', got %q", s)
	}
}

func TestSummarizeInput_ListTables(t *testing.T) {
	input := json.RawMessage(`{"project":"p","dataset":"d"}`)
	s := summarizeInput("list_tables", input)
	if s != "p.d" {
		t.Errorf("expected 'p.d', got %q", s)
	}
}

func TestSummarizeInput_InvalidJSON(t *testing.T) {
	input := json.RawMessage(`{bad}`)
	s := summarizeInput("get_table_schema", input)
	if s != "(invalid input)" {
		t.Errorf("expected '(invalid input)', got %q", s)
	}
}

func TestTruncateResult_Short(t *testing.T) {
	s := truncateResult("hello", 10)
	if s != "hello" {
		t.Errorf("expected 'hello', got %q", s)
	}
}

func TestTruncateResult_Exact(t *testing.T) {
	s := truncateResult("hello", 5)
	if s != "hello" {
		t.Errorf("expected 'hello', got %q", s)
	}
}

func TestTruncateResult_Long(t *testing.T) {
	s := truncateResult("hello world", 5)
	if s != "hello..." {
		t.Errorf("expected 'hello...', got %q", s)
	}
}

func TestTruncateResult_Empty(t *testing.T) {
	s := truncateResult("", 5)
	if s != "" {
		t.Errorf("expected empty, got %q", s)
	}
}

func TestToolDefinitions_Count(t *testing.T) {
	tools := toolDefinitions()
	if len(tools) != 5 {
		t.Fatalf("expected 5 tools, got %d", len(tools))
	}
}

func TestToolDefinitions_Names(t *testing.T) {
	tools := toolDefinitions()
	expectedNames := map[string]bool{
		"get_table_schema": true,
		"run_sql_query":    true,
		"list_datasets":    true,
		"list_tables":      true,
		"get_all_tables":   true,
	}
	for _, tool := range tools {
		if tool.OfTool == nil {
			t.Fatal("expected OfTool to be non-nil")
		}
		if !expectedNames[tool.OfTool.Name] {
			t.Errorf("unexpected tool name: %s", tool.OfTool.Name)
		}
		delete(expectedNames, tool.OfTool.Name)
	}
	if len(expectedNames) > 0 {
		t.Errorf("missing tools: %v", expectedNames)
	}
}
