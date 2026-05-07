package ui

import (
	"encoding/json"
	"fmt"
	"image/color"
	"time"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/canvas"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/layout"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"
)

// maxCopySize is the maximum JSON size (in bytes) allowed for clipboard copy.
const maxCopySize = 1 << 20 // 1 MB

// maxCellDisplayLen is an absolute safety cap on cell text length.
const maxCellDisplayLen = 500

// maxMeasureLen caps text length for column-width measurement.
// Strings longer than this get maxWidth without expensive MeasureText calls.
const maxMeasureLen = 200

type Results struct {
	table     *widget.Table
	statusBar *widget.Label
	copyBtn   *widget.Button

	columns       []string
	rows          [][]string
	colCharLimits []int // per-column display char limit based on column width

	// Double-click detection for cell copy.
	lastTapCell widget.TableCellID
	lastTapTime time.Time

	Container fyne.CanvasObject
}

func NewResults() *Results {
	r := &Results{
		statusBar: widget.NewLabel("Ready"),
	}

	r.copyBtn = widget.NewButtonWithIcon("Copy JSON", theme.ContentCopyIcon(), func() {
		r.copyJSON()
	})
	r.copyBtn.Disable()

	r.table = widget.NewTableWithHeaders(
		func() (int, int) {
			if len(r.columns) == 0 {
				return 0, 0
			}
			return len(r.rows), len(r.columns)
		},
		func() fyne.CanvasObject {
			txt := canvas.NewText("", color.White)
			txt.TextSize = theme.Size(theme.SizeNameText)
			return txt
		},
		func(id widget.TableCellID, obj fyne.CanvasObject) {
			txt := obj.(*canvas.Text)
			txt.TextSize = theme.Size(theme.SizeNameText)
			txt.Color = theme.Color(theme.ColorNameForeground)
			if id.Row < len(r.rows) && id.Col < len(r.rows[id.Row]) {
				val := r.rows[id.Row][id.Col]
				// Truncate to the column's character limit so text doesn't overflow.
				limit := maxCellDisplayLen
				if id.Col < len(r.colCharLimits) && r.colCharLimits[id.Col] < limit {
					limit = r.colCharLimits[id.Col]
				}
				if len(val) > limit {
					val = val[:limit] + "…"
				}
				txt.Text = val
			} else {
				txt.Text = ""
			}
			txt.Refresh()
		},
	)

	r.table.CreateHeader = func() fyne.CanvasObject {
		txt := canvas.NewText("", color.White)
		txt.TextSize = theme.Size(theme.SizeNameText)
		txt.TextStyle = fyne.TextStyle{Bold: true}
		return txt
	}
	r.table.UpdateHeader = func(id widget.TableCellID, template fyne.CanvasObject) {
		txt := template.(*canvas.Text)
		txt.TextSize = theme.Size(theme.SizeNameText)
		txt.Color = theme.Color(theme.ColorNameForeground)
		txt.TextStyle = fyne.TextStyle{Bold: true}
		if id.Row < 0 && id.Col >= 0 && id.Col < len(r.columns) {
			txt.Text = r.columns[id.Col]
		} else if id.Col < 0 && id.Row >= 0 {
			txt.Text = fmt.Sprintf("%d", id.Row+1)
		} else {
			txt.Text = ""
		}
		txt.Refresh()
	}

	r.table.OnSelected = func(id widget.TableCellID) {
		now := time.Now()
		if id == r.lastTapCell && now.Sub(r.lastTapTime) < 400*time.Millisecond {
			// Double-tap: copy the full cell value to clipboard.
			if id.Row < len(r.rows) && id.Col < len(r.rows[id.Row]) {
				val := r.rows[id.Row][id.Col]
				if cb := fyne.CurrentApp().Clipboard(); cb != nil {
					cb.SetContent(val)
				}
				col := ""
				if id.Col < len(r.columns) {
					col = r.columns[id.Col]
				}
				r.statusBar.SetText(fmt.Sprintf("Copied %s (row %d) to clipboard", col, id.Row+1))
			}
		}
		r.lastTapCell = id
		r.lastTapTime = now
		r.table.UnselectAll()
	}

	bottomBar := container.NewHBox(r.statusBar, layout.NewSpacer(), r.copyBtn)
	r.Container = container.NewBorder(nil, bottomBar, nil, nil, r.table)
	return r
}

func (r *Results) SetData(columns []string, rows [][]string) {
	r.columns = columns
	r.rows = rows
	widths := r.computeColumnWidths()
	fyne.Do(func() {
		for i, w := range widths {
			r.table.SetColumnWidth(i, w)
		}
		r.table.Refresh()
		r.copyBtn.Enable()
	})
}

// SetColumns sets the column headers and clears any previous rows.
// Call this before streaming rows via AppendRows.
func (r *Results) SetColumns(columns []string) {
	r.columns = columns
	r.rows = nil
	r.colCharLimits = nil
	fyne.Do(func() {
		r.table.Refresh()
		r.copyBtn.Disable()
	})
}

// AppendRows adds rows incrementally and refreshes the table.
// On the first batch, column widths are measured from the data.
func (r *Results) AppendRows(newRows [][]string) {
	firstBatch := len(r.rows) == 0
	r.rows = append(r.rows, newRows...)

	if firstBatch {
		widths := r.computeColumnWidths()
		fyne.Do(func() {
			for i, w := range widths {
				r.table.SetColumnWidth(i, w)
			}
			r.table.Refresh()
			r.copyBtn.Enable()
		})
	} else {
		fyne.Do(func() {
			r.table.Refresh()
		})
	}
}

// computeColumnWidths measures column widths from headers and sampled rows.
// Also computes per-column character limits for display truncation.
func (r *Results) computeColumnWidths() []float32 {
	textSize := fyne.CurrentApp().Settings().Theme().Size("text")
	boldStyle := fyne.TextStyle{Bold: true}
	normalStyle := fyne.TextStyle{}
	const padding float32 = 24
	const minWidth float32 = 80
	const maxWidth float32 = 400
	sampleRows := len(r.rows)
	if sampleRows > 100 {
		sampleRows = 100
	}

	widths := make([]float32, len(r.columns))
	for i, col := range r.columns {
		w := fyne.MeasureText(col, textSize, boldStyle).Width + padding
		widths[i] = w
	}

	for j := 0; j < sampleRows; j++ {
		for i := 0; i < len(r.columns) && i < len(r.rows[j]); i++ {
			if widths[i] >= maxWidth {
				continue
			}
			val := r.rows[j][i]
			if len(val) > maxMeasureLen {
				widths[i] = maxWidth
				continue
			}
			w := fyne.MeasureText(val, textSize, normalStyle).Width + padding
			if w > widths[i] {
				widths[i] = w
			}
		}
	}

	for i := range widths {
		if widths[i] < minWidth {
			widths[i] = minWidth
		}
		if widths[i] > maxWidth {
			widths[i] = maxWidth
		}
	}

	// Compute per-column character limits from final widths.
	charW := fyne.MeasureText("M", textSize, normalStyle).Width
	if charW < 1 {
		charW = 8
	}
	r.colCharLimits = make([]int, len(widths))
	for i, w := range widths {
		chars := int(w/charW) + 2 // small buffer for proportional fonts
		if chars < 10 {
			chars = 10
		}
		r.colCharLimits[i] = chars
	}

	return widths
}

func (r *Results) SetStatus(text string) {
	fyne.Do(func() {
		r.statusBar.SetText(text)
	})
}

func (r *Results) Clear() {
	r.columns = nil
	r.rows = nil
	r.colCharLimits = nil
	fyne.Do(func() {
		r.table.Refresh()
		r.statusBar.SetText("Ready")
		r.copyBtn.Disable()
	})
}

func (r *Results) copyJSON() {
	if len(r.columns) == 0 || len(r.rows) == 0 {
		return
	}

	data, err := buildResultsJSON(r.columns, r.rows)
	if err != nil {
		fyne.Do(func() { r.statusBar.SetText(fmt.Sprintf("Copy failed: %v", err)) })
		return
	}

	if len(data) > maxCopySize {
		fyne.Do(func() {
			r.statusBar.SetText(fmt.Sprintf("Result too large to copy (%.2f MB > 1 MB)", float64(len(data))/(1024*1024)))
		})
		return
	}

	cb := fyne.CurrentApp().Clipboard()
	if cb != nil {
		cb.SetContent(string(data))
	}
	fyne.Do(func() { r.statusBar.SetText("Copied results as JSON to clipboard") })
}

// buildResultsJSON converts columns and rows into a JSON array of objects.
func buildResultsJSON(columns []string, rows [][]string) ([]byte, error) {
	records := make([]map[string]string, len(rows))
	for i, row := range rows {
		rec := make(map[string]string, len(columns))
		for j, col := range columns {
			if j < len(row) {
				rec[col] = row[j]
			}
		}
		records[i] = rec
	}
	return json.Marshal(records)
}
