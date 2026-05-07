package ui

import (
	"reflect"
	"testing"
)

// --- ExtractTableRefs tests ---

func TestExtractTableRefs_Simple(t *testing.T) {
	sql := "SELECT * FROM my-project.dataset.orders"
	refs := ExtractTableRefs(sql)
	if len(refs) != 1 {
		t.Fatalf("expected 1 ref, got %d", len(refs))
	}
	if refs[0].Project != "my-project" || refs[0].Dataset != "dataset" || refs[0].Table != "orders" {
		t.Errorf("unexpected ref: %+v", refs[0])
	}
}

func TestExtractTableRefs_BacktickQuotedFull(t *testing.T) {
	sql := "SELECT * FROM `my-project.dataset.orders`"
	refs := ExtractTableRefs(sql)
	if len(refs) != 1 {
		t.Fatalf("expected 1 ref, got %d", len(refs))
	}
	if refs[0].Key() != "my-project.dataset.orders" {
		t.Errorf("unexpected key: %s", refs[0].Key())
	}
}

func TestExtractTableRefs_BacktickPerComponent(t *testing.T) {
	sql := "SELECT * FROM `my-project`.`dataset`.`orders`"
	refs := ExtractTableRefs(sql)
	if len(refs) != 1 {
		t.Fatalf("expected 1 ref, got %d", len(refs))
	}
	if refs[0].Key() != "my-project.dataset.orders" {
		t.Errorf("unexpected key: %s", refs[0].Key())
	}
}

func TestExtractTableRefs_MultipleJoins(t *testing.T) {
	sql := `SELECT *
FROM project.ds.table_a a
JOIN project.ds.table_b b ON a.id = b.id
LEFT JOIN project.ds.table_c c ON a.id = c.id`
	refs := ExtractTableRefs(sql)
	if len(refs) != 3 {
		t.Fatalf("expected 3 refs, got %d: %+v", len(refs), refs)
	}
	keys := make([]string, len(refs))
	for i, r := range refs {
		keys[i] = r.Key()
	}
	expected := []string{
		"project.ds.table_a",
		"project.ds.table_b",
		"project.ds.table_c",
	}
	if !reflect.DeepEqual(keys, expected) {
		t.Errorf("expected %v, got %v", expected, keys)
	}
}

func TestExtractTableRefs_TwoPartIgnored(t *testing.T) {
	sql := "SELECT * FROM dataset.table_name"
	refs := ExtractTableRefs(sql)
	if len(refs) != 0 {
		t.Errorf("expected 0 refs for 2-part reference, got %d: %+v", len(refs), refs)
	}
}

func TestExtractTableRefs_Subquery(t *testing.T) {
	sql := "SELECT * FROM (SELECT * FROM project.ds.inner_table) sub"
	refs := ExtractTableRefs(sql)
	if len(refs) != 1 {
		t.Fatalf("expected 1 ref, got %d", len(refs))
	}
	if refs[0].Key() != "project.ds.inner_table" {
		t.Errorf("unexpected key: %s", refs[0].Key())
	}
}

func TestExtractTableRefs_NoMatch(t *testing.T) {
	sql := "SELECT 1 + 2"
	refs := ExtractTableRefs(sql)
	if len(refs) != 0 {
		t.Errorf("expected 0 refs, got %d", len(refs))
	}
}

func TestExtractTableRefs_Deduplicates(t *testing.T) {
	sql := `SELECT * FROM project.ds.t1
UNION ALL
SELECT * FROM project.ds.t1`
	refs := ExtractTableRefs(sql)
	if len(refs) != 1 {
		t.Errorf("expected 1 ref (deduped), got %d", len(refs))
	}
}

func TestExtractTableRefs_CaseInsensitiveKeywords(t *testing.T) {
	sql := "select * from project.ds.orders"
	refs := ExtractTableRefs(sql)
	if len(refs) != 1 {
		t.Fatalf("expected 1 ref, got %d", len(refs))
	}
	if refs[0].Key() != "project.ds.orders" {
		t.Errorf("unexpected key: %s", refs[0].Key())
	}
}

// --- ParseQuerySections tests ---

func TestParseQuerySections_NoCTEs(t *testing.T) {
	sql := "SELECT * FROM project.ds.orders WHERE id = 1"
	sections := ParseQuerySections(sql)
	if len(sections) != 1 {
		t.Fatalf("expected 1 section, got %d", len(sections))
	}
	if sections[0].Start != 0 || sections[0].End != len(sql) {
		t.Errorf("unexpected section bounds: [%d, %d]", sections[0].Start, sections[0].End)
	}
	if len(sections[0].Tables) != 1 {
		t.Errorf("expected 1 table ref, got %d", len(sections[0].Tables))
	}
}

func TestParseQuerySections_SingleCTE(t *testing.T) {
	sql := `WITH cte AS (
  SELECT * FROM project.ds.orders
)
SELECT * FROM cte`

	sections := ParseQuerySections(sql)
	if len(sections) != 2 {
		t.Fatalf("expected 2 sections (1 CTE + main), got %d", len(sections))
	}

	// CTE section should have the orders table
	cte := sections[0]
	if len(cte.Tables) != 1 || cte.Tables[0].Table != "orders" {
		t.Errorf("CTE section: expected 1 ref to 'orders', got %+v", cte.Tables)
	}

	// Main section should have all refs (orders from the whole query)
	main := sections[1]
	if len(main.Tables) != 1 || main.Tables[0].Table != "orders" {
		t.Errorf("main section: expected 1 ref to 'orders', got %+v", main.Tables)
	}
}

func TestParseQuerySections_MultipleCTEs(t *testing.T) {
	sql := `WITH
  first_cte AS (
    SELECT * FROM project.ds.table_a
  ),
  second_cte AS (
    SELECT * FROM project.ds.table_b
  )
SELECT * FROM first_cte JOIN second_cte`

	sections := ParseQuerySections(sql)
	if len(sections) != 3 {
		t.Fatalf("expected 3 sections (2 CTEs + main), got %d", len(sections))
	}

	// First CTE: only table_a
	if len(sections[0].Tables) != 1 || sections[0].Tables[0].Table != "table_a" {
		t.Errorf("first CTE: expected table_a, got %+v", sections[0].Tables)
	}

	// Second CTE: only table_b
	if len(sections[1].Tables) != 1 || sections[1].Tables[0].Table != "table_b" {
		t.Errorf("second CTE: expected table_b, got %+v", sections[1].Tables)
	}

	// Main query: all table refs from entire query (table_a + table_b)
	if len(sections[2].Tables) != 2 {
		t.Errorf("main section: expected 2 refs, got %d: %+v", len(sections[2].Tables), sections[2].Tables)
	}
}

func TestParseQuerySections_NestedParens(t *testing.T) {
	sql := `WITH cte AS (
  SELECT * FROM project.ds.orders
  WHERE id IN (SELECT id FROM project.ds.filter)
)
SELECT * FROM cte`

	sections := ParseQuerySections(sql)
	if len(sections) != 2 {
		t.Fatalf("expected 2 sections, got %d", len(sections))
	}

	// CTE body should contain both table refs
	cte := sections[0]
	if len(cte.Tables) != 2 {
		t.Errorf("CTE section: expected 2 refs, got %d: %+v", len(cte.Tables), cte.Tables)
	}
}

func TestParseQuerySections_StringWithParens(t *testing.T) {
	sql := `WITH cte AS (
  SELECT * FROM project.ds.orders
  WHERE name = '(not a paren)'
)
SELECT * FROM cte`

	sections := ParseQuerySections(sql)
	if len(sections) != 2 {
		t.Fatalf("expected 2 sections, got %d", len(sections))
	}

	cte := sections[0]
	if len(cte.Tables) != 1 || cte.Tables[0].Table != "orders" {
		t.Errorf("CTE section: expected 1 ref to 'orders', got %+v", cte.Tables)
	}
}

func TestParseQuerySections_EmptySQL(t *testing.T) {
	sections := ParseQuerySections("")
	if len(sections) != 0 {
		t.Errorf("expected 0 sections for empty SQL, got %d", len(sections))
	}

	sections = ParseQuerySections("   ")
	if len(sections) != 0 {
		t.Errorf("expected 0 sections for whitespace SQL, got %d", len(sections))
	}
}

func TestParseQuerySections_CursorInCTEBody(t *testing.T) {
	sql := `WITH
  first_cte AS (
    SELECT * FROM project.ds.table_a
  ),
  second_cte AS (
    SELECT * FROM project.ds.table_b
  )
SELECT * FROM first_cte`

	sections := ParseQuerySections(sql)
	if len(sections) != 3 {
		t.Fatalf("expected 3 sections, got %d", len(sections))
	}

	// Verify section boundaries don't overlap
	for i := 0; i < len(sections)-1; i++ {
		if sections[i].End > sections[i+1].Start {
			t.Errorf("sections %d and %d overlap: [%d,%d] and [%d,%d]",
				i, i+1, sections[i].Start, sections[i].End, sections[i+1].Start, sections[i+1].End)
		}
	}

	// First CTE should only have table_a
	if len(sections[0].Tables) != 1 || sections[0].Tables[0].Table != "table_a" {
		t.Errorf("first CTE tables: %+v", sections[0].Tables)
	}

	// Second CTE should only have table_b
	if len(sections[1].Tables) != 1 || sections[1].Tables[0].Table != "table_b" {
		t.Errorf("second CTE tables: %+v", sections[1].Tables)
	}
}

func TestParseQuerySections_CommentInCTE(t *testing.T) {
	sql := `WITH cte AS (
  -- This is a comment with ) in it
  SELECT * FROM project.ds.orders
)
SELECT * FROM cte`

	sections := ParseQuerySections(sql)
	if len(sections) != 2 {
		t.Fatalf("expected 2 sections, got %d", len(sections))
	}
	if len(sections[0].Tables) != 1 {
		t.Errorf("expected 1 table ref in CTE, got %d", len(sections[0].Tables))
	}
}

func TestParseQuerySections_BlockCommentInCTE(t *testing.T) {
	sql := `WITH cte AS (
  /* block comment with ) paren */
  SELECT * FROM project.ds.orders
)
SELECT * FROM cte`

	sections := ParseQuerySections(sql)
	if len(sections) != 2 {
		t.Fatalf("expected 2 sections, got %d", len(sections))
	}
	if len(sections[0].Tables) != 1 {
		t.Errorf("expected 1 table ref in CTE, got %d", len(sections[0].Tables))
	}
}

// --- CursorOffset tests ---

func TestCursorOffset_FirstLine(t *testing.T) {
	lines := []string{"SELECT *", "FROM table"}
	offset := CursorOffset(lines, 0, 4)
	if offset != 4 {
		t.Errorf("expected 4, got %d", offset)
	}
}

func TestCursorOffset_SecondLine(t *testing.T) {
	lines := []string{"SELECT *", "FROM table"}
	// Line 0 is "SELECT *" (8 chars) + 1 newline = 9
	// Col 5 on line 1 = 14
	offset := CursorOffset(lines, 1, 5)
	if offset != 14 {
		t.Errorf("expected 14, got %d", offset)
	}
}

func TestCursorOffset_StartOfFile(t *testing.T) {
	lines := []string{"hello"}
	offset := CursorOffset(lines, 0, 0)
	if offset != 0 {
		t.Errorf("expected 0, got %d", offset)
	}
}

func TestCursorOffset_EndOfFile(t *testing.T) {
	lines := []string{"abc", "def"}
	// "abc\ndef" → offset of end = 3+1+3 = 7
	offset := CursorOffset(lines, 1, 3)
	if offset != 7 {
		t.Errorf("expected 7, got %d", offset)
	}
}

func TestCursorOffset_ColBeyondLineLength(t *testing.T) {
	lines := []string{"ab"}
	offset := CursorOffset(lines, 0, 100)
	if offset != 2 {
		t.Errorf("expected 2 (clamped), got %d", offset)
	}
}

// --- findMatchingParen tests ---

func TestFindMatchingParen_Simple(t *testing.T) {
	sql := "(hello)"
	got := findMatchingParen(sql, 0)
	if got != 6 {
		t.Errorf("expected 6, got %d", got)
	}
}

func TestFindMatchingParen_Nested(t *testing.T) {
	sql := "(a(b)c)"
	got := findMatchingParen(sql, 0)
	if got != 6 {
		t.Errorf("expected 6, got %d", got)
	}
}

func TestFindMatchingParen_StringWithParen(t *testing.T) {
	sql := "(a ')' b)"
	got := findMatchingParen(sql, 0)
	if got != 8 {
		t.Errorf("expected 8, got %d", got)
	}
}

func TestFindMatchingParen_Unmatched(t *testing.T) {
	sql := "(abc"
	got := findMatchingParen(sql, 0)
	if got != -1 {
		t.Errorf("expected -1, got %d", got)
	}
}

func TestFindMatchingParen_LineComment(t *testing.T) {
	sql := "(-- )\nhello)"
	got := findMatchingParen(sql, 0)
	if got != 11 {
		t.Errorf("expected 11, got %d", got)
	}
}

func TestFindMatchingParen_BlockComment(t *testing.T) {
	sql := "(/* ) */ done)"
	got := findMatchingParen(sql, 0)
	if got != 13 {
		t.Errorf("expected 13, got %d", got)
	}
}

// --- mergeCompletionLists tests ---

func TestMergeCompletionLists_Empty(t *testing.T) {
	base := []string{"A", "B"}
	result := mergeCompletionLists(base, nil)
	if !reflect.DeepEqual(result, base) {
		t.Errorf("expected base unchanged, got %v", result)
	}
}

func TestMergeCompletionLists_NoDuplicates(t *testing.T) {
	base := []string{"A", "C"}
	extra := []string{"B", "D"}
	result := mergeCompletionLists(base, extra)
	expected := []string{"A", "B", "C", "D"}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestMergeCompletionLists_CaseInsensitiveDedup(t *testing.T) {
	base := []string{"SELECT", "from"}
	extra := []string{"select", "new_col"}
	result := mergeCompletionLists(base, extra)
	// "select" should be deduped (case-insensitive match with "SELECT")
	if len(result) != 3 {
		t.Errorf("expected 3 items, got %d: %v", len(result), result)
	}
}

// --- TableRef.Key tests ---

func TestTableRef_Key(t *testing.T) {
	ref := TableRef{Project: "proj", Dataset: "ds", Table: "tbl"}
	if ref.Key() != "proj.ds.tbl" {
		t.Errorf("expected 'proj.ds.tbl', got %q", ref.Key())
	}
}
