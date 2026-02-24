package ui

import (
	"regexp"
	"strings"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/canvas"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"
)

var sqlBlockRe = regexp.MustCompile("(?s)```(?:sql)?\\s*\n?(.*?)```")

type AssistantMessage struct {
	Role    string // "user" or "assistant"
	Content string
	SQL     string // extracted SQL from assistant messages (empty for user messages)
}

type Assistant struct {
	messages  []AssistantMessage
	chatBox   *fyne.Container // VBox holding message widgets
	scroll    *container.Scroll
	input     *widget.Entry
	sendBtn   *widget.Button
	statusLbl *widget.Label

	OnSendMessage func(userMsg string)
	OnRunSQL      func(project, sql string)

	Container fyne.CanvasObject
}

func NewAssistant() *Assistant {
	a := &Assistant{}

	a.statusLbl = widget.NewLabel("")

	a.chatBox = container.NewVBox()
	a.scroll = container.NewVScroll(a.chatBox)

	a.input = widget.NewMultiLineEntry()
	a.input.SetPlaceHolder("Describe what you want to query...")
	a.input.SetMinRowsVisible(2)

	a.sendBtn = widget.NewButton("Send", func() {
		text := strings.TrimSpace(a.input.Text)
		if text == "" {
			return
		}
		a.input.SetText("")
		if a.OnSendMessage != nil {
			a.OnSendMessage(text)
		}
	})
	a.sendBtn.Importance = widget.HighImportance

	settingsBtn := widget.NewButton("Settings", func() {
		a.showSettingsDialog()
	})

	inputRow := container.NewBorder(nil, nil, nil, container.NewHBox(a.sendBtn, settingsBtn), a.input)

	a.Container = container.NewBorder(nil, container.NewVBox(a.statusLbl, inputRow), nil, nil, a.scroll)
	return a
}

// AddMessage appends a message and refreshes the chat.
func (a *Assistant) AddMessage(role, content, sql string) {
	a.messages = append(a.messages, AssistantMessage{
		Role:    role,
		Content: content,
		SQL:     sql,
	})

	msgWidget := a.buildMessageWidget(role, content)

	fyne.Do(func() {
		a.chatBox.Add(msgWidget)
		a.scroll.ScrollToBottom()
	})
}

// buildMessageWidget creates a styled widget for a chat message.
func (a *Assistant) buildMessageWidget(role, content string) fyne.CanvasObject {
	if role == "user" {
		lbl := widget.NewLabel("You: " + content)
		lbl.Wrapping = fyne.TextWrapWord
		return lbl
	}

	// For assistant messages, separate text and SQL blocks for better readability
	parts := splitAroundSQL(content)
	if len(parts) == 1 {
		lbl := widget.NewLabel("AI: " + content)
		lbl.Wrapping = fyne.TextWrapWord
		return lbl
	}

	box := container.NewVBox()
	for i, part := range parts {
		if part.isSQL {
			box.Add(buildSQLBlock(part.text))
		} else {
			text := strings.TrimSpace(part.text)
			if text == "" {
				continue
			}
			prefix := ""
			if i == 0 {
				prefix = "AI: "
			}
			lbl := widget.NewLabel(prefix + text)
			lbl.Wrapping = fyne.TextWrapWord
			box.Add(lbl)
		}
	}
	return box
}

// buildSQLBlock creates a selectable, copyable SQL code block.
func buildSQLBlock(sql string) fyne.CanvasObject {
	// Use a disabled Entry so text is selectable and copyable (Cmd+C)
	entry := widget.NewMultiLineEntry()
	entry.SetText(sql)
	entry.TextStyle.Monospace = true
	entry.Wrapping = fyne.TextWrapBreak
	// Count lines to size the entry appropriately
	lines := strings.Count(sql, "\n") + 1
	entry.SetMinRowsVisible(lines)
	entry.Disable()

	copyBtn := widget.NewButtonWithIcon("Copy", theme.ContentCopyIcon(), func() {
		cb := fyne.CurrentApp().Clipboard()
		if cb != nil {
			cb.SetContent(sql)
		}
	})

	bg := canvas.NewRectangle(theme.Color(theme.ColorNameInputBackground))
	codeArea := container.NewStack(bg, container.NewPadded(entry))

	return container.NewBorder(nil, container.NewHBox(copyBtn), nil, nil, codeArea)
}

type messagePart struct {
	text  string
	isSQL bool
}

// splitAroundSQL splits a response into text and SQL code block parts.
func splitAroundSQL(content string) []messagePart {
	indices := sqlBlockRe.FindAllStringIndex(content, -1)
	if len(indices) == 0 {
		return []messagePart{{text: content}}
	}

	var parts []messagePart
	cursor := 0
	for _, loc := range indices {
		if loc[0] > cursor {
			parts = append(parts, messagePart{text: content[cursor:loc[0]]})
		}
		match := sqlBlockRe.FindStringSubmatch(content[loc[0]:loc[1]])
		if len(match) >= 2 {
			parts = append(parts, messagePart{text: strings.TrimSpace(match[1]), isSQL: true})
		}
		cursor = loc[1]
	}
	if cursor < len(content) {
		parts = append(parts, messagePart{text: content[cursor:]})
	}
	return parts
}

// SetStatus updates the status label.
func (a *Assistant) SetStatus(text string) {
	fyne.Do(func() {
		a.statusLbl.SetText(text)
	})
}

// Clear clears the chat history.
func (a *Assistant) Clear() {
	a.messages = nil
	fyne.Do(func() {
		a.chatBox.RemoveAll()
	})
}

// Messages returns the current message history.
func (a *Assistant) Messages() []AssistantMessage {
	return a.messages
}

// ExtractSQL extracts SQL from a ```sql ... ``` code block in the response.
func ExtractSQL(response string) string {
	matches := sqlBlockRe.FindStringSubmatch(response)
	if len(matches) < 2 {
		return ""
	}
	return strings.TrimSpace(matches[1])
}

// showSettingsDialog is a placeholder that the app layer will replace via a callback.
var onShowSettings func()

func (a *Assistant) showSettingsDialog() {
	if onShowSettings != nil {
		onShowSettings()
	}
}

// AddToolCallMessage displays a tool call as a compact label in the chat.
// It is display-only and not added to a.messages (tool calls are internal to a single turn).
func (a *Assistant) AddToolCallMessage(toolName, inputSummary, resultSummary string, isError bool) {
	w := buildToolCallWidget(toolName, inputSummary, resultSummary, isError)
	fyne.Do(func() {
		a.chatBox.Add(w)
		a.scroll.ScrollToBottom()
	})
}

func buildToolCallWidget(toolName, inputSummary, resultSummary string, isError bool) fyne.CanvasObject {
	prefix := "  \u2713 " // checkmark
	status := "OK"
	if isError {
		prefix = "  \u2717 " // X mark
		status = "Error"
	}
	header := prefix + toolName + "(" + inputSummary + ") - " + status
	headerLbl := widget.NewLabel(header)
	headerLbl.TextStyle = fyne.TextStyle{Italic: true}
	headerLbl.Wrapping = fyne.TextWrapWord

	if resultSummary == "" {
		return headerLbl
	}

	resultLbl := widget.NewLabel("  " + resultSummary)
	resultLbl.TextStyle = fyne.TextStyle{Italic: true}
	resultLbl.Wrapping = fyne.TextWrapWord
	return container.NewVBox(headerLbl, resultLbl)
}

// SetOnShowSettings sets the callback for the settings button.
func (a *Assistant) SetOnShowSettings(fn func()) {
	onShowSettings = fn
}
