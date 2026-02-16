package ui

import (
	"image/color"
	"sort"
	"strings"
	"sync"
	"time"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/canvas"
	"fyne.io/fyne/v2/driver/desktop"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"

	"github.com/alecthomas/chroma/v2"
	"github.com/alecthomas/chroma/v2/lexers"
)

const maxACDisplay = 8

// SQLEditor is a custom TextGrid-based SQL editor with syntax highlighting.
type SQLEditor struct {
	widget.BaseWidget
	grid      *widget.TextGrid
	lines     []string
	cursorRow int
	cursorCol int
	focused   bool
	blinkOn   bool
	onChanged func(string)
	OnSubmit  func() // called on Cmd+Enter / Ctrl+Enter

	// Selection state: anchor is where selection started, cursor is the other end.
	hasSelection bool
	anchorRow    int
	anchorCol    int

	// Shift key tracking for Shift+Arrow selection (via desktop.Keyable).
	shifting bool

	// Mouse drag state.
	dragging bool

	// Undo/redo stacks.
	undoStack []undoEntry
	redoStack []undoEntry

	mu          sync.Mutex
	placeholder string
	lexer       chroma.Lexer
	stopBlink   chan struct{}

	// Autocomplete state.
	completions     []string                       // full list: SQL keywords + column names
	acProjectData   map[string]map[string][]string // project -> dataset -> []tables
	acPrefix        string                         // prefix used for current filtering (for accept)
	acFiltered      []string                       // filtered by current prefix
	acVisible       bool
	acSelected      int
	acLoadRequested map[string]bool      // projects we've already requested loading for
	OnProjectNeeded func(project string) // callback: request loading data for a project

	// AC rendering (canvas primitives, created in CreateRenderer).
	acBg         *canvas.Rectangle
	acSelBg      *canvas.Rectangle
	acTexts      [maxACDisplay]*canvas.Text
	acItemHeight float32
	acDropdownX  float32
	acDropdownY  float32
	acDropdownW  float32
	acDropdownH  float32
}

const maxUndoStack = 500

type undoEntry struct {
	lines     []string
	cursorRow int
	cursorCol int
}

// Compile-time interface checks.
var (
	_ fyne.Focusable    = (*SQLEditor)(nil)
	_ fyne.Tappable     = (*SQLEditor)(nil)
	_ fyne.Draggable    = (*SQLEditor)(nil)
	_ fyne.Shortcutable = (*SQLEditor)(nil)
	_ fyne.Tabbable     = (*SQLEditor)(nil)
	_ desktop.Keyable   = (*SQLEditor)(nil)
)

// NewSQLEditor creates a new SQL editor with syntax highlighting.
func NewSQLEditor() *SQLEditor {
	grid := widget.NewTextGrid()
	grid.TabWidth = 4
	grid.Scroll = fyne.ScrollNone

	e := &SQLEditor{
		grid:  grid,
		lines: []string{""},
		lexer: lexers.Get("sql"),
	}
	e.ExtendBaseWidget(e)
	return e
}

// Text returns the full editor content.
func (e *SQLEditor) Text() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return strings.Join(e.lines, "\n")
}

// SetText replaces the editor content.
func (e *SQLEditor) SetText(text string) {
	e.mu.Lock()
	if text == "" {
		e.lines = []string{""}
	} else {
		e.lines = strings.Split(text, "\n")
	}
	e.cursorRow = len(e.lines) - 1
	e.cursorCol = len(e.lines[e.cursorRow])
	e.hasSelection = false
	e.mu.Unlock()
	e.refreshContent()
	e.notifyChanged()
}

// SetOnChanged sets a callback invoked after every edit.
func (e *SQLEditor) SetOnChanged(fn func(string)) {
	e.mu.Lock()
	e.onChanged = fn
	e.mu.Unlock()
}

// SetPlaceHolder sets placeholder text shown when the editor is empty and unfocused.
func (e *SQLEditor) SetPlaceHolder(text string) {
	e.mu.Lock()
	e.placeholder = text
	e.mu.Unlock()
	e.refreshContent()
}

func (e *SQLEditor) notifyChanged() {
	e.mu.Lock()
	fn := e.onChanged
	e.mu.Unlock()
	if fn != nil {
		fn(e.Text())
	}
}

// orderedSelection returns selection bounds with start before end.
func (e *SQLEditor) orderedSelection() (sRow, sCol, eRow, eCol int) {
	if e.anchorRow < e.cursorRow || (e.anchorRow == e.cursorRow && e.anchorCol <= e.cursorCol) {
		return e.anchorRow, e.anchorCol, e.cursorRow, e.cursorCol
	}
	return e.cursorRow, e.cursorCol, e.anchorRow, e.anchorCol
}

// selectedTextLocked returns the text within the selection. Caller must hold mu.
func (e *SQLEditor) selectedTextLocked() string {
	sRow, sCol, eRow, eCol := e.orderedSelection()
	if sRow == eRow {
		return e.lines[sRow][sCol:eCol]
	}
	var parts []string
	parts = append(parts, e.lines[sRow][sCol:])
	for i := sRow + 1; i < eRow; i++ {
		parts = append(parts, e.lines[i])
	}
	parts = append(parts, e.lines[eRow][:eCol])
	return strings.Join(parts, "\n")
}

// deleteSelectionLocked removes selected text and positions cursor. Caller must hold mu.
func (e *SQLEditor) deleteSelectionLocked() {
	if !e.hasSelection {
		return
	}
	sRow, sCol, eRow, eCol := e.orderedSelection()
	before := e.lines[sRow][:sCol]
	after := e.lines[eRow][eCol:]
	e.lines[sRow] = before + after
	if eRow > sRow {
		e.lines = append(e.lines[:sRow+1], e.lines[eRow+1:]...)
	}
	e.cursorRow = sRow
	e.cursorCol = sCol
	e.hasSelection = false
}

// beginSelectionLocked starts a new selection at the current cursor if none exists.
func (e *SQLEditor) beginSelectionLocked() {
	if !e.hasSelection {
		e.anchorRow = e.cursorRow
		e.anchorCol = e.cursorCol
		e.hasSelection = true
	}
}

func (e *SQLEditor) saveUndoLocked() {
	snap := undoEntry{
		lines:     make([]string, len(e.lines)),
		cursorRow: e.cursorRow,
		cursorCol: e.cursorCol,
	}
	copy(snap.lines, e.lines)
	e.undoStack = append(e.undoStack, snap)
	if len(e.undoStack) > maxUndoStack {
		e.undoStack = e.undoStack[1:]
	}
	e.redoStack = e.redoStack[:0]
}

func (e *SQLEditor) doUndo() {
	e.mu.Lock()
	if len(e.undoStack) == 0 {
		e.mu.Unlock()
		return
	}
	// Save current state to redo stack.
	current := undoEntry{
		lines:     make([]string, len(e.lines)),
		cursorRow: e.cursorRow,
		cursorCol: e.cursorCol,
	}
	copy(current.lines, e.lines)
	e.redoStack = append(e.redoStack, current)

	// Pop from undo stack.
	snap := e.undoStack[len(e.undoStack)-1]
	e.undoStack = e.undoStack[:len(e.undoStack)-1]
	e.lines = snap.lines
	e.cursorRow = snap.cursorRow
	e.cursorCol = snap.cursorCol
	e.hasSelection = false
	e.mu.Unlock()
	e.resetBlink()
	e.refreshContent()
	e.notifyChanged()
}

func (e *SQLEditor) doRedo() {
	e.mu.Lock()
	if len(e.redoStack) == 0 {
		e.mu.Unlock()
		return
	}
	// Save current state to undo stack.
	current := undoEntry{
		lines:     make([]string, len(e.lines)),
		cursorRow: e.cursorRow,
		cursorCol: e.cursorCol,
	}
	copy(current.lines, e.lines)
	e.undoStack = append(e.undoStack, current)

	// Pop from redo stack.
	snap := e.redoStack[len(e.redoStack)-1]
	e.redoStack = e.redoStack[:len(e.redoStack)-1]
	e.lines = snap.lines
	e.cursorRow = snap.cursorRow
	e.cursorCol = snap.cursorCol
	e.hasSelection = false
	e.mu.Unlock()
	e.resetBlink()
	e.refreshContent()
	e.notifyChanged()
}

func (e *SQLEditor) cursorLeftLocked() {
	if e.cursorCol > 0 {
		e.cursorCol--
	} else if e.cursorRow > 0 {
		e.cursorRow--
		e.cursorCol = len(e.lines[e.cursorRow])
	}
}

func (e *SQLEditor) cursorRightLocked() {
	if e.cursorCol < len(e.lines[e.cursorRow]) {
		e.cursorCol++
	} else if e.cursorRow < len(e.lines)-1 {
		e.cursorRow++
		e.cursorCol = 0
	}
}

func (e *SQLEditor) cursorUpLocked() {
	if e.cursorRow > 0 {
		e.cursorRow--
		if e.cursorCol > len(e.lines[e.cursorRow]) {
			e.cursorCol = len(e.lines[e.cursorRow])
		}
	}
}

func (e *SQLEditor) cursorDownLocked() {
	if e.cursorRow < len(e.lines)-1 {
		e.cursorRow++
		if e.cursorCol > len(e.lines[e.cursorRow]) {
			e.cursorCol = len(e.lines[e.cursorRow])
		}
	}
}

func isWordByte(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_'
}

func (e *SQLEditor) wordLeftLocked() {
	line := e.lines[e.cursorRow]
	if e.cursorCol == 0 {
		if e.cursorRow > 0 {
			e.cursorRow--
			e.cursorCol = len(e.lines[e.cursorRow])
		}
		return
	}
	col := e.cursorCol
	// Skip non-word chars backward
	for col > 0 && !isWordByte(line[col-1]) {
		col--
	}
	// Skip word chars backward
	for col > 0 && isWordByte(line[col-1]) {
		col--
	}
	e.cursorCol = col
}

func (e *SQLEditor) wordRightLocked() {
	line := e.lines[e.cursorRow]
	if e.cursorCol >= len(line) {
		if e.cursorRow < len(e.lines)-1 {
			e.cursorRow++
			e.cursorCol = 0
		}
		return
	}
	col := e.cursorCol
	// Skip word chars forward
	for col < len(line) && isWordByte(line[col]) {
		col++
	}
	// Skip non-word chars forward
	for col < len(line) && !isWordByte(line[col]) {
		col++
	}
	e.cursorCol = col
}

func (e *SQLEditor) startBlink() {
	e.stopBlinkTimer()
	stop := make(chan struct{})
	e.mu.Lock()
	e.stopBlink = stop
	e.blinkOn = true
	e.mu.Unlock()
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				e.mu.Lock()
				e.blinkOn = !e.blinkOn
				e.mu.Unlock()
				e.refreshContent()
			}
		}
	}()
}

func (e *SQLEditor) stopBlinkTimer() {
	e.mu.Lock()
	if e.stopBlink != nil {
		close(e.stopBlink)
		e.stopBlink = nil
	}
	e.mu.Unlock()
}

func (e *SQLEditor) resetBlink() {
	e.mu.Lock()
	e.blinkOn = true
	e.mu.Unlock()
	e.startBlink()
}

func (e *SQLEditor) KeyDown(ev *fyne.KeyEvent) {
	if ev.Name == desktop.KeyShiftLeft || ev.Name == desktop.KeyShiftRight {
		e.mu.Lock()
		e.shifting = true
		e.mu.Unlock()
	}
}

func (e *SQLEditor) KeyUp(ev *fyne.KeyEvent) {
	if ev.Name == desktop.KeyShiftLeft || ev.Name == desktop.KeyShiftRight {
		e.mu.Lock()
		e.shifting = false
		e.mu.Unlock()
	}
}

func (e *SQLEditor) FocusGained() {
	e.mu.Lock()
	e.focused = true
	e.blinkOn = true
	e.mu.Unlock()
	e.startBlink()
	e.refreshContent()
}

func (e *SQLEditor) FocusLost() {
	e.hideACPopup()
	e.stopBlinkTimer()
	e.mu.Lock()
	e.focused = false
	e.hasSelection = false
	e.shifting = false
	e.mu.Unlock()
	e.refreshContent()
}

func (e *SQLEditor) TypedRune(r rune) {
	e.mu.Lock()
	e.saveUndoLocked()
	if e.hasSelection {
		e.deleteSelectionLocked()
	}
	line := e.lines[e.cursorRow]
	e.lines[e.cursorRow] = line[:e.cursorCol] + string(r) + line[e.cursorCol:]
	e.cursorCol++
	e.mu.Unlock()
	e.resetBlink()
	e.refreshContent()
	e.notifyChanged()
	e.updateAutocomplete()
}

func (e *SQLEditor) TypedKey(ev *fyne.KeyEvent) {
	// Intercept keys when autocomplete is visible.
	e.mu.Lock()
	acVis := e.acVisible
	e.mu.Unlock()
	if acVis {
		switch ev.Name {
		case fyne.KeyUp:
			e.mu.Lock()
			if e.acSelected > 0 {
				e.acSelected--
			}
			e.mu.Unlock()
			e.refreshAC()
			return
		case fyne.KeyDown, fyne.KeyTab:
			e.mu.Lock()
			maxIdx := len(e.acFiltered) - 1
			if maxIdx > maxACDisplay-1 {
				maxIdx = maxACDisplay - 1
			}
			if e.acSelected < maxIdx {
				e.acSelected++
			} else {
				e.acSelected = 0
			}
			e.mu.Unlock()
			e.refreshAC()
			return
		case fyne.KeyReturn:
			e.acceptCompletion()
			return
		case fyne.KeyEscape:
			e.hideACPopup()
			return
		}
	}

	e.mu.Lock()
	edited := true
	// Save undo state before destructive operations.
	switch ev.Name {
	case fyne.KeyReturn, fyne.KeyBackspace, fyne.KeyDelete, fyne.KeyTab:
		e.saveUndoLocked()
	}
	switch ev.Name {
	case fyne.KeyReturn:
		if e.hasSelection {
			e.deleteSelectionLocked()
		}
		line := e.lines[e.cursorRow]
		before := line[:e.cursorCol]
		after := line[e.cursorCol:]
		e.lines[e.cursorRow] = before
		newLines := make([]string, len(e.lines)+1)
		copy(newLines, e.lines[:e.cursorRow+1])
		newLines[e.cursorRow+1] = after
		copy(newLines[e.cursorRow+2:], e.lines[e.cursorRow+1:])
		e.lines = newLines
		e.cursorRow++
		e.cursorCol = 0

	case fyne.KeyBackspace:
		if e.hasSelection {
			e.deleteSelectionLocked()
		} else if e.cursorCol > 0 {
			line := e.lines[e.cursorRow]
			e.lines[e.cursorRow] = line[:e.cursorCol-1] + line[e.cursorCol:]
			e.cursorCol--
		} else if e.cursorRow > 0 {
			prevLen := len(e.lines[e.cursorRow-1])
			e.lines[e.cursorRow-1] += e.lines[e.cursorRow]
			e.lines = append(e.lines[:e.cursorRow], e.lines[e.cursorRow+1:]...)
			e.cursorRow--
			e.cursorCol = prevLen
		}

	case fyne.KeyDelete:
		if e.hasSelection {
			e.deleteSelectionLocked()
		} else {
			line := e.lines[e.cursorRow]
			if e.cursorCol < len(line) {
				e.lines[e.cursorRow] = line[:e.cursorCol] + line[e.cursorCol+1:]
			} else if e.cursorRow < len(e.lines)-1 {
				e.lines[e.cursorRow] += e.lines[e.cursorRow+1]
				e.lines = append(e.lines[:e.cursorRow+1], e.lines[e.cursorRow+2:]...)
			}
		}

	case fyne.KeyLeft:
		edited = false
		if e.shifting {
			e.beginSelectionLocked()
			e.cursorLeftLocked()
		} else if e.hasSelection {
			sRow, sCol, _, _ := e.orderedSelection()
			e.cursorRow, e.cursorCol = sRow, sCol
			e.hasSelection = false
		} else {
			e.cursorLeftLocked()
		}

	case fyne.KeyRight:
		edited = false
		if e.shifting {
			e.beginSelectionLocked()
			e.cursorRightLocked()
		} else if e.hasSelection {
			_, _, eRow, eCol := e.orderedSelection()
			e.cursorRow, e.cursorCol = eRow, eCol
			e.hasSelection = false
		} else {
			e.cursorRightLocked()
		}

	case fyne.KeyUp:
		edited = false
		if e.shifting {
			e.beginSelectionLocked()
			e.cursorUpLocked()
		} else if e.hasSelection {
			sRow, sCol, _, _ := e.orderedSelection()
			e.cursorRow, e.cursorCol = sRow, sCol
			e.hasSelection = false
		} else {
			e.cursorUpLocked()
		}

	case fyne.KeyDown:
		edited = false
		if e.shifting {
			e.beginSelectionLocked()
			e.cursorDownLocked()
		} else if e.hasSelection {
			_, _, eRow, eCol := e.orderedSelection()
			e.cursorRow, e.cursorCol = eRow, eCol
			e.hasSelection = false
		} else {
			e.cursorDownLocked()
		}

	case fyne.KeyHome:
		edited = false
		if e.shifting {
			e.beginSelectionLocked()
		} else {
			e.hasSelection = false
		}
		e.cursorCol = 0

	case fyne.KeyEnd:
		edited = false
		if e.shifting {
			e.beginSelectionLocked()
		} else {
			e.hasSelection = false
		}
		e.cursorCol = len(e.lines[e.cursorRow])

	case fyne.KeyTab:
		if e.hasSelection {
			e.deleteSelectionLocked()
		}
		line := e.lines[e.cursorRow]
		e.lines[e.cursorRow] = line[:e.cursorCol] + "    " + line[e.cursorCol:]
		e.cursorCol += 4

	default:
		e.mu.Unlock()
		return
	}
	e.mu.Unlock()
	e.resetBlink()
	e.refreshContent()
	if edited {
		e.notifyChanged()
		e.updateAutocomplete()
	}
}

func (e *SQLEditor) clampPositionLocked(row, col int) (int, int) {
	if row < 0 {
		row = 0
	}
	if row >= len(e.lines) {
		row = len(e.lines) - 1
	}
	if col < 0 {
		col = 0
	}
	if col > len(e.lines[row]) {
		col = len(e.lines[row])
	}
	return row, col
}

func (e *SQLEditor) Tapped(ev *fyne.PointEvent) {
	// Check if tap is on an autocomplete item.
	e.mu.Lock()
	if e.acVisible {
		pos := ev.Position
		if pos.X >= e.acDropdownX && pos.X <= e.acDropdownX+e.acDropdownW &&
			pos.Y >= e.acDropdownY && pos.Y <= e.acDropdownY+e.acDropdownH {
			idx := int((pos.Y - e.acDropdownY) / e.acItemHeight)
			if idx >= 0 && idx < len(e.acFiltered) && idx < maxACDisplay {
				e.acSelected = idx
				e.mu.Unlock()
				e.acceptCompletion()
				return
			}
		}
	}
	e.mu.Unlock()

	c := fyne.CurrentApp().Driver().CanvasForObject(e)
	if c != nil {
		c.Focus(e)
	}

	e.hideACPopup()

	row, col := e.grid.CursorLocationForPosition(ev.Position)
	e.mu.Lock()
	row, col = e.clampPositionLocked(row, col)
	e.cursorRow = row
	e.cursorCol = col
	e.hasSelection = false
	e.mu.Unlock()
	e.resetBlink()
	e.refreshContent()
}

func (e *SQLEditor) Dragged(ev *fyne.DragEvent) {
	c := fyne.CurrentApp().Driver().CanvasForObject(e)
	if c != nil {
		c.Focus(e)
	}

	e.mu.Lock()
	if !e.dragging {
		// First drag event: compute start position and set anchor there.
		startPos := fyne.NewPos(ev.Position.X-ev.Dragged.DX, ev.Position.Y-ev.Dragged.DY)
		row, col := e.grid.CursorLocationForPosition(startPos)
		row, col = e.clampPositionLocked(row, col)
		e.anchorRow = row
		e.anchorCol = col
		e.hasSelection = true
		e.dragging = true
	}
	// Update cursor to current drag position.
	row, col := e.grid.CursorLocationForPosition(ev.Position)
	row, col = e.clampPositionLocked(row, col)
	e.cursorRow = row
	e.cursorCol = col
	e.mu.Unlock()
	e.refreshContent()
}

func (e *SQLEditor) DragEnd() {
	e.mu.Lock()
	e.dragging = false
	// If anchor == cursor, clear selection (was just a click-drag with no movement).
	if e.hasSelection && e.anchorRow == e.cursorRow && e.anchorCol == e.cursorCol {
		e.hasSelection = false
	}
	e.mu.Unlock()
	e.refreshContent()
}

func (e *SQLEditor) TypedShortcut(s fyne.Shortcut) {
	// Handle CustomShortcut (modifier + key combinations)
	if cs, ok := s.(*desktop.CustomShortcut); ok {
		e.handleCustomShortcut(cs)
		return
	}

	switch s.(type) {
	case *fyne.ShortcutCopy:
		e.doCopy()
	case *fyne.ShortcutPaste:
		e.doPaste()
	case *fyne.ShortcutCut:
		e.doCut()
	case *fyne.ShortcutSelectAll:
		e.doSelectAll()
	case *fyne.ShortcutUndo:
		e.doUndo()
	case *fyne.ShortcutRedo:
		e.doRedo()
	}
}

func (e *SQLEditor) handleCustomShortcut(cs *desktop.CustomShortcut) {
	// Ctrl/Cmd+Enter → run query
	if cs.KeyName == fyne.KeyReturn {
		if e.OnSubmit != nil {
			e.OnSubmit()
		}
		return
	}

	hasWordMod := cs.Modifier&(fyne.KeyModifierSuper|fyne.KeyModifierControl|fyne.KeyModifierAlt) != 0
	hasShift := cs.Modifier&fyne.KeyModifierShift != 0
	hasCmdOrCtrl := cs.Modifier&(fyne.KeyModifierSuper|fyne.KeyModifierControl) != 0

	switch cs.KeyName {
	case fyne.KeyZ:
		if hasCmdOrCtrl {
			if hasShift {
				e.doRedo()
			} else {
				e.doUndo()
			}
			return
		}
	case fyne.KeyLeft:
		if hasWordMod {
			e.mu.Lock()
			if hasShift {
				e.beginSelectionLocked()
			} else {
				e.hasSelection = false
			}
			e.wordLeftLocked()
			e.mu.Unlock()
			e.resetBlink()
			e.refreshContent()
		}
	case fyne.KeyRight:
		if hasWordMod {
			e.mu.Lock()
			if hasShift {
				e.beginSelectionLocked()
			} else {
				e.hasSelection = false
			}
			e.wordRightLocked()
			e.mu.Unlock()
			e.resetBlink()
			e.refreshContent()
		}
	case fyne.KeyUp:
		if hasShift {
			e.mu.Lock()
			e.beginSelectionLocked()
			e.cursorUpLocked()
			e.mu.Unlock()
			e.resetBlink()
			e.refreshContent()
		}
	case fyne.KeyDown:
		if hasShift {
			e.mu.Lock()
			e.beginSelectionLocked()
			e.cursorDownLocked()
			e.mu.Unlock()
			e.resetBlink()
			e.refreshContent()
		}
	case fyne.KeyHome:
		if hasShift {
			e.mu.Lock()
			e.beginSelectionLocked()
			e.cursorCol = 0
			e.mu.Unlock()
			e.resetBlink()
			e.refreshContent()
		}
	case fyne.KeyEnd:
		if hasShift {
			e.mu.Lock()
			e.beginSelectionLocked()
			e.cursorCol = len(e.lines[e.cursorRow])
			e.mu.Unlock()
			e.resetBlink()
			e.refreshContent()
		}
	case fyne.KeyBackspace:
		// Cmd+Backspace: delete to start of line; Alt+Backspace: delete previous word
		e.mu.Lock()
		e.saveUndoLocked()
		if e.hasSelection {
			e.deleteSelectionLocked()
		} else if cs.Modifier&(fyne.KeyModifierSuper|fyne.KeyModifierControl) != 0 {
			// Delete to start of line
			line := e.lines[e.cursorRow]
			e.lines[e.cursorRow] = line[e.cursorCol:]
			e.cursorCol = 0
		} else if cs.Modifier&fyne.KeyModifierAlt != 0 {
			// Delete previous word
			oldCol := e.cursorCol
			e.wordLeftLocked()
			line := e.lines[e.cursorRow]
			e.lines[e.cursorRow] = line[:e.cursorCol] + line[oldCol:]
		}
		e.mu.Unlock()
		e.resetBlink()
		e.refreshContent()
		e.notifyChanged()
	}
}

func (e *SQLEditor) doSelectAll() {
	e.mu.Lock()
	if len(e.lines) == 1 && e.lines[0] == "" {
		e.mu.Unlock()
		return
	}
	e.anchorRow = 0
	e.anchorCol = 0
	e.cursorRow = len(e.lines) - 1
	e.cursorCol = len(e.lines[e.cursorRow])
	e.hasSelection = true
	e.mu.Unlock()
	e.refreshContent()
}

func (e *SQLEditor) doCopy() {
	e.mu.Lock()
	var text string
	if e.hasSelection {
		text = e.selectedTextLocked()
	}
	e.mu.Unlock()
	if text != "" {
		fyne.CurrentApp().Clipboard().SetContent(text)
	}
}

func (e *SQLEditor) doCut() {
	e.mu.Lock()
	if !e.hasSelection {
		e.mu.Unlock()
		return
	}
	e.saveUndoLocked()
	text := e.selectedTextLocked()
	e.deleteSelectionLocked()
	e.mu.Unlock()
	if text != "" {
		fyne.CurrentApp().Clipboard().SetContent(text)
	}
	e.resetBlink()
	e.refreshContent()
	e.notifyChanged()
}

func (e *SQLEditor) doPaste() {
	content := fyne.CurrentApp().Clipboard().Content()
	if content == "" {
		return
	}

	pasteLines := strings.Split(content, "\n")

	e.mu.Lock()
	e.saveUndoLocked()
	if e.hasSelection {
		e.deleteSelectionLocked()
	}
	line := e.lines[e.cursorRow]
	before := line[:e.cursorCol]
	after := line[e.cursorCol:]

	if len(pasteLines) == 1 {
		e.lines[e.cursorRow] = before + pasteLines[0] + after
		e.cursorCol += len(pasteLines[0])
	} else {
		e.lines[e.cursorRow] = before + pasteLines[0]
		newLines := make([]string, 0, len(e.lines)+len(pasteLines)-1)
		newLines = append(newLines, e.lines[:e.cursorRow+1]...)
		for i := 1; i < len(pasteLines)-1; i++ {
			newLines = append(newLines, pasteLines[i])
		}
		lastPaste := pasteLines[len(pasteLines)-1]
		newLines = append(newLines, lastPaste+after)
		newLines = append(newLines, e.lines[e.cursorRow+1:]...)
		e.lines = newLines
		e.cursorRow += len(pasteLines) - 1
		e.cursorCol = len(lastPaste)
	}
	e.mu.Unlock()
	e.resetBlink()
	e.refreshContent()
	e.notifyChanged()
}

func (e *SQLEditor) AcceptsTab() bool {
	return true
}

func (e *SQLEditor) refreshContent() {
	e.mu.Lock()
	lines := make([]string, len(e.lines))
	copy(lines, e.lines)
	focused := e.focused
	blinkOn := e.blinkOn
	placeholder := e.placeholder
	curRow := e.cursorRow
	curCol := e.cursorCol
	hasSel := e.hasSelection
	var selSRow, selSCol, selERow, selECol int
	if hasSel {
		selSRow, selSCol, selERow, selECol = e.orderedSelection()
	}
	e.mu.Unlock()

	fullText := strings.Join(lines, "\n")

	if fullText == "" && !focused && placeholder != "" {
		e.showPlaceholder(placeholder)
		return
	}

	rows := e.buildGridRows(fullText, lines, curRow, curCol, focused, blinkOn, hasSel, selSRow, selSCol, selERow, selECol)

	fyne.Do(func() {
		e.grid.Rows = rows
		e.grid.Refresh()
	})
}

func (e *SQLEditor) showPlaceholder(text string) {
	th := fyne.CurrentApp().Settings().Theme()
	v := fyne.CurrentApp().Settings().ThemeVariant()
	placeholderColor := th.Color(theme.ColorNamePlaceHolder, v)
	style := &widget.CustomTextGridStyle{FGColor: placeholderColor}

	phLines := strings.Split(text, "\n")
	rows := make([]widget.TextGridRow, len(phLines))
	for i, line := range phLines {
		cells := make([]widget.TextGridCell, len(line))
		for j, r := range line {
			cells[j] = widget.TextGridCell{Rune: r, Style: style}
		}
		rows[i] = widget.TextGridRow{Cells: cells}
	}

	fyne.Do(func() {
		e.grid.Rows = rows
		e.grid.Refresh()
	})
}

func (e *SQLEditor) buildGridRows(fullText string, lines []string, curRow, curCol int, focused, blinkOn, hasSel bool, selSRow, selSCol, selERow, selECol int) []widget.TextGridRow {
	th := fyne.CurrentApp().Settings().Theme()
	v := fyne.CurrentApp().Settings().ThemeVariant()

	// Theme colors
	syntaxColors := map[string]color.Color{
		"sqlKeyword":  th.Color("sqlKeyword", v),
		"sqlFunction": th.Color("sqlFunction", v),
		"sqlString":   th.Color("sqlString", v),
		"sqlNumber":   th.Color("sqlNumber", v),
		"sqlComment":  th.Color("sqlComment", v),
	}
	selectionColor := th.Color(theme.ColorNameSelection, v)
	cursorColor := th.Color(theme.ColorNamePrimary, v)
	cursorTextColor := th.Color(theme.ColorNameForegroundOnPrimary, v)

	// Build a map of (row, col) -> syntax color name from chroma tokenization
	type pos struct{ r, c int }
	syntaxMap := map[pos]string{}
	if e.lexer != nil {
		iter, err := e.lexer.Tokenise(nil, fullText)
		if err == nil {
			row, col := 0, 0
			for _, tok := range iter.Tokens() {
				name := tokenColorName(tok.Type)
				for _, ch := range tok.Value {
					if ch == '\n' {
						row++
						col = 0
						continue
					}
					if name != "" {
						syntaxMap[pos{row, col}] = name
					}
					col++
				}
			}
		}
	}

	// Build rows with syntax + selection + cursor styles
	rows := make([]widget.TextGridRow, len(lines))
	for i, line := range lines {
		cells := make([]widget.TextGridCell, len(line))
		for j, r := range line {
			cell := widget.TextGridCell{Rune: r}

			var fgColor color.Color
			if name, ok := syntaxMap[pos{i, j}]; ok {
				fgColor = syntaxColors[name]
			}

			inSel := hasSel && inSelection(i, j, selSRow, selSCol, selERow, selECol)
			isCursor := focused && blinkOn && i == curRow && j == curCol && !hasSel

			if isCursor {
				cell.Style = &widget.CustomTextGridStyle{
					FGColor: cursorTextColor,
					BGColor: cursorColor,
				}
			} else if inSel {
				cell.Style = &widget.CustomTextGridStyle{
					FGColor: fgColor,
					BGColor: selectionColor,
				}
			} else if fgColor != nil {
				cell.Style = &widget.CustomTextGridStyle{FGColor: fgColor}
			}

			cells[j] = cell
		}

		// Handle cursor/selection at end of line (past last character)
		if focused && blinkOn && i == curRow && curCol == len(line) && !hasSel {
			cells = append(cells, widget.TextGridCell{
				Rune: ' ',
				Style: &widget.CustomTextGridStyle{
					FGColor: cursorTextColor,
					BGColor: cursorColor,
				},
			})
		} else if hasSel && inSelection(i, len(line), selSRow, selSCol, selERow, selECol) {
			cells = append(cells, widget.TextGridCell{
				Rune:  ' ',
				Style: &widget.CustomTextGridStyle{BGColor: selectionColor},
			})
		}

		rows[i] = widget.TextGridRow{Cells: cells}
	}

	return rows
}

func inSelection(row, col, sRow, sCol, eRow, eCol int) bool {
	if row < sRow || row > eRow {
		return false
	}
	if row == sRow && col < sCol {
		return false
	}
	if row == eRow && col >= eCol {
		return false
	}
	return true
}

func tokenColorName(t chroma.TokenType) string {
	if t == chroma.NameBuiltin || t == chroma.NameFunction {
		return "sqlFunction"
	}
	switch {
	case t.InCategory(chroma.Keyword):
		return "sqlKeyword"
	case t.InCategory(chroma.LiteralString):
		return "sqlString"
	case t.InCategory(chroma.LiteralNumber):
		return "sqlNumber"
	case t.InCategory(chroma.Comment):
		return "sqlComment"
	}
	return ""
}

var sqlKeywords = []string{
	// SQL keywords
	"SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IN", "BETWEEN", "LIKE",
	"IS", "NULL", "AS", "ON", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER",
	"CROSS", "FULL", "GROUP", "BY", "ORDER", "ASC", "DESC", "LIMIT", "OFFSET",
	"HAVING", "DISTINCT", "UNION", "ALL", "EXISTS", "CASE", "WHEN", "THEN",
	"ELSE", "END", "CAST", "IF", "TRUE", "FALSE", "WITH", "OVER", "PARTITION",
	"ROWS", "RANGE", "UNNEST", "EXCEPT", "INTERSECT", "INSERT", "INTO",
	"VALUES", "UPDATE", "SET", "DELETE", "CREATE", "TABLE", "STRUCT", "ARRAY",

	// Aggregate functions
	"COUNT", "SUM", "AVG", "MIN", "MAX", "ANY_VALUE", "ARRAY_AGG",
	"STRING_AGG", "COUNTIF", "LOGICAL_AND", "LOGICAL_OR", "APPROX_COUNT_DISTINCT",
	"APPROX_QUANTILES", "APPROX_TOP_COUNT", "APPROX_TOP_SUM",

	// Analytic / window functions
	"ROW_NUMBER", "RANK", "DENSE_RANK", "PERCENT_RANK", "CUME_DIST",
	"NTILE", "LAG", "LEAD", "FIRST_VALUE", "LAST_VALUE", "NTH_VALUE",
	"PERCENTILE_CONT", "PERCENTILE_DISC",

	// Date / time functions
	"CURRENT_DATE", "CURRENT_TIMESTAMP", "CURRENT_DATETIME", "CURRENT_TIME",
	"DATE", "DATETIME", "TIME", "TIMESTAMP",
	"DATE_ADD", "DATE_SUB", "DATE_DIFF", "DATE_TRUNC",
	"DATETIME_ADD", "DATETIME_SUB", "DATETIME_DIFF", "DATETIME_TRUNC",
	"TIMESTAMP_ADD", "TIMESTAMP_SUB", "TIMESTAMP_DIFF", "TIMESTAMP_TRUNC",
	"TIME_ADD", "TIME_SUB", "TIME_DIFF", "TIME_TRUNC",
	"EXTRACT", "FORMAT_DATE", "FORMAT_DATETIME", "FORMAT_TIMESTAMP", "FORMAT_TIME",
	"PARSE_DATE", "PARSE_DATETIME", "PARSE_TIMESTAMP", "PARSE_TIME",
	"UNIX_SECONDS", "UNIX_MILLIS", "UNIX_MICROS",
	"TIMESTAMP_SECONDS", "TIMESTAMP_MILLIS", "TIMESTAMP_MICROS",

	// String functions
	"CONCAT", "LENGTH", "LOWER", "UPPER", "TRIM", "LTRIM", "RTRIM",
	"SUBSTR", "SUBSTRING", "REPLACE", "REVERSE", "REPEAT",
	"STARTS_WITH", "ENDS_WITH", "CONTAINS_SUBSTR",
	"REGEXP_CONTAINS", "REGEXP_EXTRACT", "REGEXP_EXTRACT_ALL", "REGEXP_REPLACE",
	"SPLIT", "FORMAT", "LPAD", "RPAD", "LEFT", "RIGHT",
	"SAFE_CONVERT_BYTES_TO_STRING", "TO_CODE_POINTS", "CODE_POINTS_TO_STRING",
	"NORMALIZE", "NORMALIZE_AND_CASEFOLD",
	"BYTE_LENGTH", "CHAR_LENGTH", "CHARACTER_LENGTH",

	// Null handling
	"IFNULL", "NULLIF", "COALESCE",

	// Conversion / casting
	"SAFE_CAST",

	// Math functions
	"ABS", "SIGN", "ROUND", "TRUNC", "CEIL", "CEILING", "FLOOR",
	"MOD", "DIV", "SAFE_DIVIDE", "SAFE_MULTIPLY", "SAFE_NEGATE", "SAFE_ADD", "SAFE_SUBTRACT",
	"POWER", "POW", "SQRT", "EXP", "LN", "LOG", "LOG10", "LOG2",
	"GREATEST", "LEAST", "IEEE_DIVIDE", "RAND", "GENERATE_ARRAY", "GENERATE_DATE_ARRAY",

	// JSON functions
	"JSON_EXTRACT", "JSON_EXTRACT_SCALAR", "JSON_EXTRACT_ARRAY",
	"JSON_EXTRACT_STRING_ARRAY", "JSON_VALUE", "JSON_VALUE_ARRAY",
	"JSON_QUERY", "JSON_QUERY_ARRAY", "TO_JSON_STRING", "TO_JSON",
	"PARSE_JSON", "JSON_TYPE",

	// Array functions
	"ARRAY_LENGTH", "ARRAY_TO_STRING", "ARRAY_REVERSE", "ARRAY_CONCAT",
	"GENERATE_ARRAY", "GENERATE_TIMESTAMP_ARRAY",

	// Hash / fingerprint
	"FARM_FINGERPRINT", "MD5", "SHA1", "SHA256", "SHA512",

	// Other common functions
	"GENERATE_UUID", "ERROR", "STRUCT",
	"IF", "IIF", "NULLIF",
}

// SetCompletions merges SQL keywords with the provided items (e.g. column names)
// and stores them sorted for autocomplete.
func (e *SQLEditor) SetCompletions(items []string) {
	seen := make(map[string]bool, len(sqlKeywords)+len(items))
	var merged []string
	for _, kw := range sqlKeywords {
		upper := strings.ToUpper(kw)
		if !seen[upper] {
			seen[upper] = true
			merged = append(merged, kw)
		}
	}
	for _, item := range items {
		if !seen[strings.ToUpper(item)] {
			seen[strings.ToUpper(item)] = true
			merged = append(merged, item)
		}
	}
	sort.Strings(merged)
	e.mu.Lock()
	e.completions = merged
	e.mu.Unlock()
}

// SetProjectData stores the project hierarchy data for context-aware dotted-path completion.
// It also re-triggers autocomplete so freshly loaded data appears immediately.
func (e *SQLEditor) SetProjectData(data map[string]map[string][]string) {
	e.mu.Lock()
	e.acProjectData = data
	e.acLoadRequested = make(map[string]bool)
	e.mu.Unlock()
	e.updateAutocomplete()
}

// dottedExprBeforeCursorLocked walks left from the cursor to extract a dotted expression
// (e.g. "project.dataset.tab"). Returns nil if no dots are found (caller should use flat completion).
// Caller must hold mu.
func (e *SQLEditor) dottedExprBeforeCursorLocked() []string {
	line := e.lines[e.cursorRow]
	col := e.cursorCol
	if col > len(line) {
		col = len(line)
	}
	start := col
	for start > 0 {
		b := line[start-1]
		if isWordByte(b) || b == '.' || b == '`' || b == '-' {
			start--
		} else {
			break
		}
	}
	expr := line[start:col]
	// Strip backticks
	expr = strings.ReplaceAll(expr, "`", "")
	if !strings.Contains(expr, ".") {
		return nil
	}
	return strings.Split(expr, ".")
}

// wordBeforeCursorLocked returns the word prefix left of the cursor. Caller must hold mu.
func (e *SQLEditor) wordBeforeCursorLocked() string {
	line := e.lines[e.cursorRow]
	col := e.cursorCol
	start := col
	for start > 0 && isWordByte(line[start-1]) {
		start--
	}
	return line[start:col]
}

// updateAutocomplete filters completions by the current prefix and shows/hides the popup.
func (e *SQLEditor) updateAutocomplete() {
	e.mu.Lock()
	parts := e.dottedExprBeforeCursorLocked()
	projectData := e.acProjectData
	e.mu.Unlock()

	// Dotted-path branch: context-aware completion for project.dataset.table
	if parts != nil && projectData != nil {
		var candidates []string
		var prefix string
		var project string
		switch len(parts) {
		case 2:
			// project.partial → show datasets
			project = parts[0]
			prefix = parts[1]
			if dsMap, ok := projectData[project]; ok {
				for ds := range dsMap {
					candidates = append(candidates, ds)
				}
			}
			sort.Strings(candidates)
		case 3:
			// project.dataset.partial → show tables
			project = parts[0]
			dataset := parts[1]
			prefix = parts[2]
			if dsMap, ok := projectData[project]; ok {
				if tables, ok := dsMap[dataset]; ok {
					candidates = tables
				}
			}
		}

		// If project not in cache, trigger async load
		if project != "" {
			if _, ok := projectData[project]; !ok {
				e.mu.Lock()
				if e.acLoadRequested == nil {
					e.acLoadRequested = make(map[string]bool)
				}
				alreadyRequested := e.acLoadRequested[project]
				if !alreadyRequested {
					e.acLoadRequested[project] = true
				}
				fn := e.OnProjectNeeded
				e.mu.Unlock()
				if !alreadyRequested && fn != nil {
					go fn(project)
				}
			}
		}

		if len(candidates) > 0 || prefix == "" {
			upperPrefix := strings.ToUpper(prefix)
			var filtered []string
			for _, c := range candidates {
				if strings.HasPrefix(strings.ToUpper(c), upperPrefix) {
					if upperPrefix == "" || strings.ToUpper(c) != upperPrefix {
						filtered = append(filtered, c)
					}
				}
			}
			if len(filtered) > 0 {
				e.mu.Lock()
				e.acPrefix = prefix
				e.acFiltered = filtered
				e.acSelected = 0
				e.mu.Unlock()
				e.showACPopup()
				return
			}
		}
		e.hideACPopup()
		return
	}

	// Flat completion path
	e.mu.Lock()
	prefix := e.wordBeforeCursorLocked()
	completions := e.completions
	e.acPrefix = prefix
	e.mu.Unlock()

	if len(prefix) == 0 || len(completions) == 0 {
		e.hideACPopup()
		return
	}

	upperPrefix := strings.ToUpper(prefix)
	var filtered []string
	for _, c := range completions {
		if strings.HasPrefix(strings.ToUpper(c), upperPrefix) && strings.ToUpper(c) != upperPrefix {
			filtered = append(filtered, c)
		}
	}
	if len(filtered) == 0 {
		e.hideACPopup()
		return
	}

	e.mu.Lock()
	e.acFiltered = filtered
	e.acSelected = 0
	e.mu.Unlock()

	e.showACPopup()
}

// showACPopup sets autocomplete visible and computes dropdown geometry.
func (e *SQLEditor) showACPopup() {
	e.mu.Lock()
	e.acVisible = true
	curRow := e.cursorRow
	curCol := e.cursorCol
	prefix := e.acPrefix
	n := len(e.acFiltered)
	if n > maxACDisplay {
		n = maxACDisplay
	}
	e.mu.Unlock()

	charSize := fyne.MeasureText("M", theme.TextSize(), fyne.TextStyle{Monospace: true})
	itemH := charSize.Height + theme.Padding()

	e.mu.Lock()
	e.acDropdownX = float32(curCol-len(prefix)) * charSize.Width
	e.acDropdownY = float32(curRow+1) * charSize.Height
	e.acDropdownW = float32(220)
	e.acDropdownH = float32(n) * itemH
	e.acItemHeight = itemH
	e.mu.Unlock()

	e.refreshAC()
}

// hideACPopup hides the autocomplete dropdown.
func (e *SQLEditor) hideACPopup() {
	e.mu.Lock()
	e.acVisible = false
	e.mu.Unlock()
	e.refreshAC()
}

// refreshAC updates the autocomplete canvas primitives.
func (e *SQLEditor) refreshAC() {
	e.mu.Lock()
	visible := e.acVisible
	var filtered []string
	var selected int
	var x, y, w, itemH float32
	if visible {
		filtered = make([]string, len(e.acFiltered))
		copy(filtered, e.acFiltered)
		selected = e.acSelected
		x = e.acDropdownX
		y = e.acDropdownY
		w = e.acDropdownW
		itemH = e.acItemHeight
	}
	bg := e.acBg
	selBg := e.acSelBg
	texts := e.acTexts
	e.mu.Unlock()

	// Canvas objects not yet created (renderer not initialized).
	if bg == nil {
		return
	}

	fyne.Do(func() {
		if !visible || len(filtered) == 0 {
			bg.Hide()
			selBg.Hide()
			for i := range texts {
				if texts[i] != nil {
					texts[i].Hide()
				}
			}
			return
		}

		th := fyne.CurrentApp().Settings().Theme()
		v := fyne.CurrentApp().Settings().ThemeVariant()

		n := len(filtered)
		if n > maxACDisplay {
			n = maxACDisplay
		}
		h := float32(n) * itemH

		// Background
		bg.FillColor = th.Color(theme.ColorNameMenuBackground, v)
		bg.StrokeColor = th.Color(theme.ColorNameSeparator, v)
		bg.StrokeWidth = 1
		bg.Resize(fyne.NewSize(w, h))
		bg.Move(fyne.NewPos(x, y))
		bg.Show()
		bg.Refresh()

		// Selection highlight
		if selected >= 0 && selected < n {
			selBg.FillColor = th.Color(theme.ColorNameSelection, v)
			selBg.Resize(fyne.NewSize(w, itemH))
			selBg.Move(fyne.NewPos(x, y+float32(selected)*itemH))
			selBg.Show()
			selBg.Refresh()
		} else {
			selBg.Hide()
		}

		// Text items
		fgColor := th.Color(theme.ColorNameForeground, v)
		pad := theme.Padding()
		for i := 0; i < maxACDisplay; i++ {
			if texts[i] == nil {
				continue
			}
			if i < n {
				texts[i].Text = filtered[i]
				texts[i].Color = fgColor
				texts[i].TextSize = theme.TextSize()
				texts[i].Move(fyne.NewPos(x+pad, y+float32(i)*itemH))
				texts[i].Show()
				texts[i].Refresh()
			} else {
				texts[i].Hide()
			}
		}
	})
}

// acceptCompletion inserts the remaining suffix of the selected completion at the cursor.
func (e *SQLEditor) acceptCompletion() {
	e.mu.Lock()
	if !e.acVisible || len(e.acFiltered) == 0 {
		e.mu.Unlock()
		return
	}
	sel := e.acSelected
	if sel < 0 || sel >= len(e.acFiltered) {
		sel = 0
	}
	completion := e.acFiltered[sel]
	prefix := e.acPrefix
	suffix := completion[len(prefix):]

	e.saveUndoLocked()
	line := e.lines[e.cursorRow]
	e.lines[e.cursorRow] = line[:e.cursorCol] + suffix + line[e.cursorCol:]
	e.cursorCol += len(suffix)
	e.mu.Unlock()

	e.hideACPopup()
	e.resetBlink()
	e.refreshContent()
	e.notifyChanged()
}

type sqlEditorRenderer struct {
	editor  *SQLEditor
	grid    *widget.TextGrid
	objects []fyne.CanvasObject
}

func (e *SQLEditor) CreateRenderer() fyne.WidgetRenderer {
	e.ExtendBaseWidget(e)

	// Create AC canvas primitives.
	e.acBg = canvas.NewRectangle(color.Transparent)
	e.acBg.Hide()
	e.acSelBg = canvas.NewRectangle(color.Transparent)
	e.acSelBg.Hide()
	for i := range e.acTexts {
		t := canvas.NewText("", color.White)
		t.TextStyle = fyne.TextStyle{Monospace: true}
		t.TextSize = theme.TextSize()
		t.Hide()
		e.acTexts[i] = t
	}

	objects := make([]fyne.CanvasObject, 0, 2+maxACDisplay+1)
	objects = append(objects, e.grid, e.acBg, e.acSelBg)
	for _, t := range e.acTexts {
		objects = append(objects, t)
	}

	return &sqlEditorRenderer{editor: e, grid: e.grid, objects: objects}
}

func (r *sqlEditorRenderer) Layout(size fyne.Size) {
	r.grid.Resize(size)
	r.grid.Move(fyne.NewPos(0, 0))
}

func (r *sqlEditorRenderer) MinSize() fyne.Size {
	return r.grid.MinSize()
}

func (r *sqlEditorRenderer) Objects() []fyne.CanvasObject {
	return r.objects
}

func (r *sqlEditorRenderer) Refresh() {
	r.grid.Refresh()
}

func (r *sqlEditorRenderer) Destroy() {
	r.editor.stopBlinkTimer()
}
