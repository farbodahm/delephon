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
