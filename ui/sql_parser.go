package ui

import (
	"regexp"
	"sort"
	"strings"
	"unicode"
)

// TableRef represents a fully-qualified BigQuery table reference.
type TableRef struct {
	Project string
	Dataset string
	Table   string
}

// Key returns the dot-separated key for the table reference.
func (t TableRef) Key() string {
	return t.Project + "." + t.Dataset + "." + t.Table
}

// QuerySection represents a section of a SQL query (CTE body or main query).
type QuerySection struct {
	Start  int        // byte offset in full SQL
	End    int        // byte offset in full SQL
	Tables []TableRef // real table references in this section
}

// SectionCompletion maps a text range to its available column completions.
type SectionCompletion struct {
	Start   int
	End     int
	Columns []string
}

// afterFromJoinRe matches the first identifier expression after FROM or JOIN keywords.
// Captures dotted identifiers that may include backticks (e.g. `project`.`dataset`.`table`).
var afterFromJoinRe = regexp.MustCompile(`(?i)\b(?:FROM|JOIN)\s+([^\s,()]+)`)

// ExtractTableRefs finds all fully-qualified table references (project.dataset.table) in SQL.
func ExtractTableRefs(sql string) []TableRef {
	matches := afterFromJoinRe.FindAllStringSubmatch(sql, -1)
	seen := make(map[string]bool)
	var refs []TableRef
	for _, m := range matches {
		ref := strings.ReplaceAll(m[1], "`", "")
		parts := strings.Split(ref, ".")
		if len(parts) != 3 {
			continue
		}
		key := parts[0] + "." + parts[1] + "." + parts[2]
		if !seen[key] {
			seen[key] = true
			refs = append(refs, TableRef{Project: parts[0], Dataset: parts[1], Table: parts[2]})
		}
	}
	return refs
}

// ParseQuerySections splits SQL into sections (CTE bodies and main query),
// each with their table references. CTE bodies only get refs from their own body.
// The main query section gets ALL table refs from the entire query.
func ParseQuerySections(sql string) []QuerySection {
	if strings.TrimSpace(sql) == "" {
		return nil
	}

	trimmed := strings.TrimLeft(sql, " \t\n\r")

	// Check for WITH keyword
	if len(trimmed) < 4 || !strings.EqualFold(trimmed[:4], "WITH") {
		refs := ExtractTableRefs(sql)
		return []QuerySection{{Start: 0, End: len(sql), Tables: refs}}
	}

	// Check that WITH is followed by a word boundary (not part of another keyword)
	withOffset := len(sql) - len(trimmed)
	afterWith := withOffset + 4
	if afterWith < len(sql) && isAlpha(sql[afterWith]) {
		// "WITH" is part of a longer word, not a CTE keyword
		refs := ExtractTableRefs(sql)
		return []QuerySection{{Start: 0, End: len(sql), Tables: refs}}
	}

	var sections []QuerySection
	pos := afterWith

	// Skip optional RECURSIVE keyword
	rest := strings.TrimLeft(sql[pos:], " \t\n\r")
	if len(rest) >= 9 && strings.EqualFold(rest[:9], "RECURSIVE") {
		afterRecursive := len(sql) - len(rest) + 9
		if afterRecursive >= len(sql) || !isAlpha(sql[afterRecursive]) {
			pos = afterRecursive
		}
	}

	asParenRe := regexp.MustCompile(`(?i)\bAS\s*\(`)

	for pos < len(sql) {
		remaining := sql[pos:]
		loc := asParenRe.FindStringIndex(remaining)
		if loc == nil {
			break
		}

		// Find the '(' in the match
		openParen := -1
		for i := pos + loc[0]; i < pos+loc[1]; i++ {
			if sql[i] == '(' {
				openParen = i
				break
			}
		}
		if openParen < 0 {
			break
		}

		closeParen := findMatchingParen(sql, openParen)
		if closeParen < 0 {
			break
		}

		bodyStart := openParen + 1
		bodyEnd := closeParen
		body := sql[bodyStart:bodyEnd]
		refs := ExtractTableRefs(body)

		sections = append(sections, QuerySection{
			Start:  bodyStart,
			End:    bodyEnd,
			Tables: refs,
		})

		// Move past the closing paren
		pos = closeParen + 1

		// Skip comma and whitespace
		for pos < len(sql) && (sql[pos] == ',' || unicode.IsSpace(rune(sql[pos]))) {
			pos++
		}

		// Check if next token is a main query keyword
		nextWord := nextSQLKeyword(sql[pos:])
		if isMainQueryKeyword(nextWord) {
			break
		}
	}

	// Main query section: from pos to end, with ALL table refs from entire query
	if pos < len(sql) {
		allRefs := ExtractTableRefs(sql)
		sections = append(sections, QuerySection{
			Start:  pos,
			End:    len(sql),
			Tables: allRefs,
		})
	}

	return sections
}

// CursorOffset converts (row, col) to byte offset in multi-line text joined by newlines.
func CursorOffset(lines []string, row, col int) int {
	offset := 0
	for i := 0; i < row && i < len(lines); i++ {
		offset += len(lines[i]) + 1 // +1 for \n
	}
	if row < len(lines) {
		if col > len(lines[row]) {
			col = len(lines[row])
		}
		offset += col
	}
	return offset
}

// findMatchingParen finds the closing ')' matching the '(' at openPos.
// Returns -1 if no match is found. Handles string literals, quoted identifiers, and comments.
func findMatchingParen(sql string, openPos int) int {
	if openPos >= len(sql) || sql[openPos] != '(' {
		return -1
	}
	depth := 1
	i := openPos + 1
	for i < len(sql) && depth > 0 {
		switch sql[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i
			}
		case '\'':
			// Skip single-quoted string literal
			i++
			for i < len(sql) {
				if sql[i] == '\'' {
					if i+1 < len(sql) && sql[i+1] == '\'' {
						i += 2 // escaped quote
						continue
					}
					break
				}
				i++
			}
		case '"':
			// Skip double-quoted identifier
			i++
			for i < len(sql) && sql[i] != '"' {
				i++
			}
		case '`':
			// Skip backtick-quoted identifier
			i++
			for i < len(sql) && sql[i] != '`' {
				i++
			}
		case '-':
			// Check for -- line comment
			if i+1 < len(sql) && sql[i+1] == '-' {
				for i < len(sql) && sql[i] != '\n' {
					i++
				}
				continue // don't increment again
			}
		case '/':
			// Check for /* block comment */
			if i+1 < len(sql) && sql[i+1] == '*' {
				i += 2
				for i+1 < len(sql) {
					if sql[i] == '*' && sql[i+1] == '/' {
						i++ // skip past '/'
						break
					}
					i++
				}
			}
		}
		i++
	}
	return -1
}

// mergeCompletionLists merges two sorted completion lists, deduplicating case-insensitively.
func mergeCompletionLists(base, extra []string) []string {
	if len(extra) == 0 {
		return base
	}
	seen := make(map[string]bool, len(base))
	for _, s := range base {
		seen[strings.ToUpper(s)] = true
	}
	var newItems []string
	for _, s := range extra {
		if !seen[strings.ToUpper(s)] {
			seen[strings.ToUpper(s)] = true
			newItems = append(newItems, s)
		}
	}
	if len(newItems) == 0 {
		return base
	}
	merged := make([]string, 0, len(base)+len(newItems))
	merged = append(merged, base...)
	merged = append(merged, newItems...)
	sort.Strings(merged)
	return merged
}

func nextSQLKeyword(sql string) string {
	s := strings.TrimLeft(sql, " \t\n\r")
	end := 0
	for end < len(s) && isAlpha(s[end]) {
		end++
	}
	return s[:end]
}

func isMainQueryKeyword(word string) bool {
	switch strings.ToUpper(word) {
	case "SELECT", "INSERT", "UPDATE", "DELETE", "MERGE", "CREATE", "DROP", "ALTER":
		return true
	}
	return false
}

func isAlpha(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || b == '_'
}
