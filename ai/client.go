package ai

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"
)

const model = "claude-sonnet-4-5-20250929"

type Message struct {
	Role    string // "user" or "assistant"
	Content string
}

type Client struct {
	client anthropic.Client
}

// New creates a Client reading the API key from the ANTHROPIC_API_KEY env var.
func New() (*Client, error) {
	key := os.Getenv("ANTHROPIC_API_KEY")
	if key == "" {
		return nil, errors.New("ANTHROPIC_API_KEY environment variable is not set")
	}
	return NewWithKey(key), nil
}

// NewWithKey creates a Client with the given API key.
func NewWithKey(apiKey string) *Client {
	return &Client{
		client: anthropic.NewClient(option.WithAPIKey(apiKey)),
	}
}

// Chat sends the conversation history with a system prompt to Claude and returns the assistant response.
func (c *Client) Chat(ctx context.Context, systemPrompt string, messages []Message) (string, error) {
	params := anthropic.MessageNewParams{
		Model:     model,
		MaxTokens: 4096,
		System: []anthropic.TextBlockParam{
			{Text: systemPrompt},
		},
		Messages: convertMessages(messages),
	}

	resp, err := c.client.Messages.New(ctx, params)
	if err != nil {
		return "", fmt.Errorf("claude API error: %w", err)
	}

	// Extract text from response content blocks
	var text string
	for _, block := range resp.Content {
		if block.Type == "text" {
			text += block.Text
		}
	}
	return text, nil
}

func convertMessages(msgs []Message) []anthropic.MessageParam {
	params := make([]anthropic.MessageParam, len(msgs))
	for i, m := range msgs {
		switch m.Role {
		case "assistant":
			params[i] = anthropic.NewAssistantMessage(anthropic.NewTextBlock(m.Content))
		default:
			params[i] = anthropic.NewUserMessage(anthropic.NewTextBlock(m.Content))
		}
	}
	return params
}
