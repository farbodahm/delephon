package main

import (
	"strings"
	"testing"
)

func TestPartitionWhere_IngestionTimeDay(t *testing.T) {
	a := &App{}
	got := a.partitionWhereClause("_PARTITIONTIME", "")
	want := "_PARTITIONTIME >= TIMESTAMP(CURRENT_DATE())"
	if got != want {
		t.Errorf("expected %q, got %q", want, got)
	}
}

func TestPartitionWhere_IngestionTimeHour(t *testing.T) {
	a := &App{}
	got := a.partitionWhereClause("_PARTITIONTIME", "HOUR")
	want := "_PARTITIONTIME >= TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), HOUR)"
	if got != want {
		t.Errorf("expected %q, got %q", want, got)
	}
}

func TestPartitionWhere_ColumnDay(t *testing.T) {
	a := &App{}
	got := a.partitionWhereClause("created_at", "DAY")
	want := "DATE(created_at) = CURRENT_DATE()"
	if got != want {
		t.Errorf("expected %q, got %q", want, got)
	}
}

func TestPartitionWhere_ColumnHour(t *testing.T) {
	a := &App{}
	got := a.partitionWhereClause("ts", "HOUR")
	want := "DATE(ts) = CURRENT_DATE()"
	if got != want {
		t.Errorf("expected %q, got %q", want, got)
	}
}

func TestPartitionWhere_ColumnMonth(t *testing.T) {
	a := &App{}
	got := a.partitionWhereClause("created_at", "MONTH")
	if !strings.Contains(got, "DATE_TRUNC(CURRENT_DATE(), MONTH)") {
		t.Errorf("expected DATE_TRUNC(CURRENT_DATE(), MONTH) in %q", got)
	}
}

func TestPartitionWhere_ColumnYear(t *testing.T) {
	a := &App{}
	got := a.partitionWhereClause("created_at", "YEAR")
	if !strings.Contains(got, "DATE_TRUNC(CURRENT_DATE(), YEAR)") {
		t.Errorf("expected DATE_TRUNC(CURRENT_DATE(), YEAR) in %q", got)
	}
}

func TestPartitionWhere_UnknownType(t *testing.T) {
	a := &App{}
	got := a.partitionWhereClause("my_field", "RANGE")
	want := "my_field IS NOT NULL"
	if got != want {
		t.Errorf("expected %q, got %q", want, got)
	}
}

func TestEnforceQueryLimit_NoLimit(t *testing.T) {
	got := enforceQueryLimit("SELECT * FROM t")
	if !strings.HasSuffix(got, "\nLIMIT 10") {
		t.Errorf("expected LIMIT 10 appended, got %q", got)
	}
}

func TestEnforceQueryLimit_HigherLimit(t *testing.T) {
	got := enforceQueryLimit("SELECT * FROM t LIMIT 1000")
	if !strings.Contains(got, "LIMIT 10") {
		t.Errorf("expected LIMIT 10, got %q", got)
	}
	if strings.Contains(got, "LIMIT 1000") {
		t.Errorf("expected LIMIT 1000 to be replaced, got %q", got)
	}
}

func TestEnforceQueryLimit_ExactlyTen(t *testing.T) {
	input := "SELECT * FROM t LIMIT 10"
	got := enforceQueryLimit(input)
	if !strings.Contains(got, "LIMIT 10") {
		t.Errorf("expected LIMIT 10 preserved, got %q", got)
	}
}

func TestEnforceQueryLimit_LowerLimit(t *testing.T) {
	got := enforceQueryLimit("SELECT * FROM t LIMIT 5")
	if !strings.Contains(got, "LIMIT 10") {
		t.Errorf("expected LIMIT replaced with 10, got %q", got)
	}
}

func TestEnforceQueryLimit_CaseInsensitive(t *testing.T) {
	got := enforceQueryLimit("SELECT * FROM t limit 500")
	if !strings.Contains(got, "LIMIT 10") {
		t.Errorf("expected case-insensitive LIMIT replacement, got %q", got)
	}
}

func TestEnforceQueryLimit_TrailingSemicolon(t *testing.T) {
	got := enforceQueryLimit("SELECT * FROM t;")
	if !strings.HasSuffix(got, "\nLIMIT 10") {
		t.Errorf("expected semicolon stripped and LIMIT 10 appended, got %q", got)
	}
}

func TestEnforceQueryLimit_MultilineSQL(t *testing.T) {
	input := "SELECT\n  id,\n  name\nFROM users\nWHERE active = true"
	got := enforceQueryLimit(input)
	if !strings.HasSuffix(got, "\nLIMIT 10") {
		t.Errorf("expected LIMIT 10 appended to multiline SQL, got %q", got)
	}
}
