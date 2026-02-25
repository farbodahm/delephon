package ai

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/anthropics/anthropic-sdk-go"
)

const maxToolIterations = 20

// ToolExecutor holds callbacks for executing each tool.
// Injected from app.go to keep the ai package decoupled from bq.
type ToolExecutor struct {
	GetTableSchema func(ctx context.Context, project, dataset, table string) (string, error)
	RunSQLQuery    func(ctx context.Context, project, sql string) (string, error)
	ListDatasets   func(ctx context.Context, project string) (string, error)
	ListTables     func(ctx context.Context, project, dataset string) (string, error)
	GetAllTables   func(ctx context.Context) (string, error)
}

// StatusFunc is called to update the UI status label.
type StatusFunc func(text string)

// ToolCallInfo describes a tool invocation for the UI.
type ToolCallInfo struct {
	Name    string
	Input   string // human-readable summary
	FullSQL string // full SQL text for run_sql_query calls (empty for other tools)
}

// ToolCallNotifyFunc is called after each tool execution to update the UI.
type ToolCallNotifyFunc func(info ToolCallInfo, result string, isError bool)

// ChatWithToolsResult holds the return values from ChatWithTools.
type ChatWithToolsResult struct {
	Response string // final text response from Claude
	LastSQL  string // last SQL executed via run_sql_query tool (empty if none)
}

// toolDefinitions returns the tool definitions sent to the Claude API.
func toolDefinitions() []anthropic.ToolUnionParam {
	return []anthropic.ToolUnionParam{
		{OfTool: &anthropic.ToolParam{
			Name:        "get_table_schema",
			Description: anthropic.String("Get the schema (columns, types, modes) of a BigQuery table. Use this to understand a table's structure before writing queries."),
			InputSchema: anthropic.ToolInputSchemaParam{
				Properties: map[string]any{
					"project": map[string]any{"type": "string", "description": "GCP project ID"},
					"dataset": map[string]any{"type": "string", "description": "BigQuery dataset ID"},
					"table":   map[string]any{"type": "string", "description": "BigQuery table ID"},
				},
				Required: []string{"project", "dataset", "table"},
			},
		}},
		{OfTool: &anthropic.ToolParam{
			Name:        "run_sql_query",
			Description: anthropic.String("Execute a BigQuery SQL query and return the results. The query will be automatically limited to a small number of rows. Use this to verify your queries or explore data."),
			InputSchema: anthropic.ToolInputSchemaParam{
				Properties: map[string]any{
					"project": map[string]any{"type": "string", "description": "GCP project ID to run the query against"},
					"sql":     map[string]any{"type": "string", "description": "The SQL query to execute"},
				},
				Required: []string{"project", "sql"},
			},
		}},
		{OfTool: &anthropic.ToolParam{
			Name:        "list_datasets",
			Description: anthropic.String("List all datasets in a BigQuery project."),
			InputSchema: anthropic.ToolInputSchemaParam{
				Properties: map[string]any{
					"project": map[string]any{"type": "string", "description": "GCP project ID"},
				},
				Required: []string{"project"},
			},
		}},
		{OfTool: &anthropic.ToolParam{
			Name:        "list_tables",
			Description: anthropic.String("List all tables in a BigQuery dataset."),
			InputSchema: anthropic.ToolInputSchemaParam{
				Properties: map[string]any{
					"project": map[string]any{"type": "string", "description": "GCP project ID"},
					"dataset": map[string]any{"type": "string", "description": "BigQuery dataset ID"},
				},
				Required: []string{"project", "dataset"},
			},
		}},
		{OfTool: &anthropic.ToolParam{
			Name:        "get_all_tables",
			Description: anthropic.String("Get all known tables across all favorite projects. Returns fully-qualified table names (project.dataset.table). Use this to see what tables are available before writing queries."),
			InputSchema: anthropic.ToolInputSchemaParam{
				Properties: map[string]any{},
			},
		}},
	}
}

// ChatWithTools sends a conversation to Claude with tool use enabled and
// handles the tool execution loop until Claude produces a final text response.
func (c *Client) ChatWithTools(
	ctx context.Context,
	model string,
	systemPrompt string,
	messages []anthropic.MessageParam,
	executor ToolExecutor,
	onStatus StatusFunc,
	onToolCall ToolCallNotifyFunc,
) (*ChatWithToolsResult, error) {
	if model == "" || model == "default" {
		resolved, err := c.resolveDefaultModel(ctx)
		if err != nil {
			return nil, err
		}
		model = resolved
	}
	log.Printf("ai: ChatWithTools using model %s", model)

	tools := toolDefinitions()
	var lastSQL string

	for i := range maxToolIterations {
		onStatus(fmt.Sprintf("Sending to Claude (turn %d)...", i+1))

		params := anthropic.MessageNewParams{
			Model:     anthropic.Model(model),
			MaxTokens: 4096,
			System: []anthropic.TextBlockParam{
				{Text: systemPrompt},
			},
			Messages: messages,
			Tools:    tools,
		}

		resp, err := c.client.Messages.New(ctx, params)
		if err != nil {
			return nil, fmt.Errorf("claude API error: %w", err)
		}

		log.Printf("ai: response stop_reason=%s, %d content blocks", resp.StopReason, len(resp.Content))

		// If not a tool_use stop, extract text and return
		if resp.StopReason != anthropic.StopReasonToolUse {
			var text string
			for _, block := range resp.Content {
				if block.Type == "text" {
					text += block.Text
				}
			}
			return &ChatWithToolsResult{Response: text, LastSQL: lastSQL}, nil
		}

		// Convert response content blocks to assistant message param
		var assistantBlocks []anthropic.ContentBlockParamUnion
		for _, block := range resp.Content {
			switch block.Type {
			case "text":
				assistantBlocks = append(assistantBlocks, anthropic.NewTextBlock(block.Text))
			case "tool_use":
				assistantBlocks = append(assistantBlocks, anthropic.NewToolUseBlock(block.ID, block.Input, block.Name))
			}
		}
		messages = append(messages, anthropic.NewAssistantMessage(assistantBlocks...))

		// Execute each tool_use block and collect results
		var toolResults []anthropic.ContentBlockParamUnion
		for _, block := range resp.Content {
			if block.Type != "tool_use" {
				continue
			}

			// Extract full SQL for run_sql_query calls
			var fullSQL string
			if block.Name == "run_sql_query" {
				var input struct {
					SQL string `json:"sql"`
				}
				if err := json.Unmarshal(block.Input, &input); err == nil {
					fullSQL = input.SQL
					lastSQL = input.SQL
					log.Printf("ai: run_sql_query SQL:\n%s", input.SQL)
				}
			}

			log.Printf("ai: executing tool %s (id=%s)", block.Name, block.ID)
			onStatus(fmt.Sprintf("Running tool: %s...", block.Name))

			result, isError := executeTool(ctx, block.Name, block.Input, executor)

			if block.Name == "run_sql_query" {
				if isError {
					log.Printf("ai: run_sql_query FAILED: %s", result)
				} else {
					log.Printf("ai: run_sql_query SUCCESS")
				}
			}

			if onToolCall != nil {
				info := ToolCallInfo{
					Name:    block.Name,
					Input:   summarizeInput(block.Name, block.Input),
					FullSQL: fullSQL,
				}
				onToolCall(info, truncateResult(result, 200), isError)
			}

			toolResults = append(toolResults, anthropic.NewToolResultBlock(block.ID, result, isError))
		}

		messages = append(messages, anthropic.NewUserMessage(toolResults...))
	}

	return nil, fmt.Errorf("tool use loop exceeded %d iterations", maxToolIterations)
}

// executeTool dispatches a tool call to the appropriate ToolExecutor callback.
func executeTool(ctx context.Context, name string, rawInput json.RawMessage, executor ToolExecutor) (result string, isError bool) {
	switch name {
	case "get_table_schema":
		var input struct {
			Project string `json:"project"`
			Dataset string `json:"dataset"`
			Table   string `json:"table"`
		}
		if err := json.Unmarshal(rawInput, &input); err != nil {
			return fmt.Sprintf("invalid input: %v", err), true
		}
		res, err := executor.GetTableSchema(ctx, input.Project, input.Dataset, input.Table)
		if err != nil {
			return fmt.Sprintf("error: %v", err), true
		}
		return res, false

	case "run_sql_query":
		var input struct {
			Project string `json:"project"`
			SQL     string `json:"sql"`
		}
		if err := json.Unmarshal(rawInput, &input); err != nil {
			return fmt.Sprintf("invalid input: %v", err), true
		}
		res, err := executor.RunSQLQuery(ctx, input.Project, input.SQL)
		if err != nil {
			return fmt.Sprintf("error: %v", err), true
		}
		return res, false

	case "list_datasets":
		var input struct {
			Project string `json:"project"`
		}
		if err := json.Unmarshal(rawInput, &input); err != nil {
			return fmt.Sprintf("invalid input: %v", err), true
		}
		res, err := executor.ListDatasets(ctx, input.Project)
		if err != nil {
			return fmt.Sprintf("error: %v", err), true
		}
		return res, false

	case "list_tables":
		var input struct {
			Project string `json:"project"`
			Dataset string `json:"dataset"`
		}
		if err := json.Unmarshal(rawInput, &input); err != nil {
			return fmt.Sprintf("invalid input: %v", err), true
		}
		res, err := executor.ListTables(ctx, input.Project, input.Dataset)
		if err != nil {
			return fmt.Sprintf("error: %v", err), true
		}
		return res, false

	case "get_all_tables":
		res, err := executor.GetAllTables(ctx)
		if err != nil {
			return fmt.Sprintf("error: %v", err), true
		}
		return res, false

	default:
		return fmt.Sprintf("unknown tool: %s", name), true
	}
}

// summarizeInput returns a short human-readable summary of tool input.
func summarizeInput(name string, rawInput json.RawMessage) string {
	var m map[string]string
	if err := json.Unmarshal(rawInput, &m); err != nil {
		return "(invalid input)"
	}
	switch name {
	case "get_table_schema":
		return fmt.Sprintf("%s.%s.%s", m["project"], m["dataset"], m["table"])
	case "run_sql_query":
		sql := m["sql"]
		if len(sql) > 80 {
			sql = sql[:80] + "..."
		}
		return fmt.Sprintf("%s: %s", m["project"], sql)
	case "list_datasets":
		return m["project"]
	case "list_tables":
		return fmt.Sprintf("%s.%s", m["project"], m["dataset"])
	case "get_all_tables":
		return "all projects"
	default:
		return name
	}
}

// truncateResult shortens a string for UI display.
func truncateResult(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
