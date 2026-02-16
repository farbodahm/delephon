package ai

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"
)

type Message struct {
	Role    string // "user" or "assistant"
	Content string
}

type Client struct {
	client       anthropic.Client
	cachedModels []string // cached result from ListModels
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
// If model is empty or "default", the first model from the API's model list is used.
func (c *Client) Chat(ctx context.Context, model string, systemPrompt string, messages []Message) (string, error) {
	if model == "" || model == "default" {
		resolved, err := c.resolveDefaultModel(ctx)
		if err != nil {
			return "", err
		}
		model = resolved
	}
	log.Printf("ai: using model %s", model)
	params := anthropic.MessageNewParams{
		Model:     anthropic.Model(model),
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

	var text string
	for _, block := range resp.Content {
		if block.Type == "text" {
			text += block.Text
		}
	}
	return text, nil
}

// ListModels fetches available models from the API.
func (c *Client) ListModels(ctx context.Context) ([]string, error) {
	if len(c.cachedModels) > 0 {
		log.Printf("ai: using %d cached models", len(c.cachedModels))
		return c.cachedModels, nil
	}

	page, err := c.client.Models.List(ctx, anthropic.ModelListParams{})
	if err != nil {
		log.Printf("ai: failed to list models from API: %v", err)
		return nil, fmt.Errorf("list models: %w", err)
	}
	var models []string
	for _, m := range page.Data {
		models = append(models, m.ID)
	}
	log.Printf("ai: fetched %d models from API: %v", len(models), models)
	c.cachedModels = models
	return models, nil
}

// resolveDefaultModel returns the first model from the API's model list.
func (c *Client) resolveDefaultModel(ctx context.Context) (string, error) {
	models, err := c.ListModels(ctx)
	if err != nil {
		return "", fmt.Errorf("cannot resolve default model: %w", err)
	}
	if len(models) == 0 {
		return "", errors.New("no models available from API")
	}
	log.Printf("ai: resolved default model to %s", models[0])
	return models[0], nil
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
