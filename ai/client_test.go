package ai

import (
	"testing"

	"github.com/anthropics/anthropic-sdk-go"
)

func TestNewWithKey(t *testing.T) {
	c := NewWithKey("sk-test-key")
	if c == nil {
		t.Fatal("expected non-nil client")
	}
}

func TestNew_MissingKey(t *testing.T) {
	t.Setenv("ANTHROPIC_API_KEY", "")
	_, err := New()
	if err == nil {
		t.Fatal("expected error when ANTHROPIC_API_KEY is unset")
	}
}

func TestNew_WithEnvKey(t *testing.T) {
	t.Setenv("ANTHROPIC_API_KEY", "sk-test-from-env")
	c, err := New()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c == nil {
		t.Fatal("expected non-nil client")
	}
}

func TestConvertMessages_Empty(t *testing.T) {
	params := convertMessages(nil)
	if len(params) != 0 {
		t.Fatalf("expected 0 params, got %d", len(params))
	}
}

func TestConvertMessages_UserAndAssistant(t *testing.T) {
	msgs := []Message{
		{Role: "user", Content: "hello"},
		{Role: "assistant", Content: "hi there"},
		{Role: "user", Content: "thanks"},
	}
	params := convertMessages(msgs)
	if len(params) != 3 {
		t.Fatalf("expected 3 params, got %d", len(params))
	}
	if params[0].Role != anthropic.MessageParamRoleUser {
		t.Errorf("expected user role, got %v", params[0].Role)
	}
	if params[1].Role != anthropic.MessageParamRoleAssistant {
		t.Errorf("expected assistant role, got %v", params[1].Role)
	}
	if params[2].Role != anthropic.MessageParamRoleUser {
		t.Errorf("expected user role, got %v", params[2].Role)
	}
}

func TestConvertMessages_UnknownRoleDefaultsToUser(t *testing.T) {
	msgs := []Message{{Role: "unknown", Content: "test"}}
	params := convertMessages(msgs)
	if params[0].Role != anthropic.MessageParamRoleUser {
		t.Errorf("expected unknown role to default to user, got %v", params[0].Role)
	}
}

func TestNewWithKey_CachedModelsEmpty(t *testing.T) {
	c := NewWithKey("sk-test-key")
	if len(c.cachedModels) != 0 {
		t.Error("expected empty cached models on new client")
	}
}
