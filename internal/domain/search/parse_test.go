package search

import (
	"strings"
	"testing"
)

func TestParseOperators(t *testing.T) {
	q := Parse(`deploy plan from:"Alice Chen" in:#general has:link is:resolved after:2019-01-01 before:2020-02-03`)
	if q.Text != "deploy plan" {
		t.Fatalf("text = %q", q.Text)
	}
	if q.From != "Alice Chen" {
		t.Fatalf("from = %q", q.From)
	}
	if q.InChannel != "general" {
		t.Fatalf("in = %q (# should be stripped)", q.InChannel)
	}
	if !q.HasLink {
		t.Fatal("has:link not set")
	}
	if q.Resolved == nil || !*q.Resolved {
		t.Fatal("is:resolved not set")
	}
	if q.After == nil || q.After.Year() != 2019 {
		t.Fatalf("after = %v", q.After)
	}
	if q.Before == nil || q.Before.Month() != 2 {
		t.Fatalf("before = %v", q.Before)
	}
}

func TestParseFallthroughAndPhrases(t *testing.T) {
	// Unknown operator and a bad date fall through to free text; a quoted
	// phrase is preserved (quotes kept for websearch_to_tsquery).
	q := Parse(`color:blue before:notadate "exact phrase"`)
	if q.Before != nil {
		t.Fatal("bad date should not set Before")
	}
	if q.From != "" || q.HasLink {
		t.Fatal("unexpected operator set")
	}
	for _, want := range []string{"color:blue", "before:notadate", `"exact phrase"`} {
		if !strings.Contains(q.Text, want) {
			t.Fatalf("text %q missing %q", q.Text, want)
		}
	}
}

func TestParseUnresolvedAndEmpty(t *testing.T) {
	if r := Parse("is:unresolved").Resolved; r == nil || *r {
		t.Fatal("is:unresolved should set Resolved=false")
	}
	if !Parse("   ").Empty() {
		t.Fatal("whitespace-only query should be Empty")
	}
	if Parse("from:bob").Empty() {
		t.Fatal("operator-only query is not empty")
	}
	if !Parse("").Empty() {
		t.Fatal("blank query should be Empty")
	}
}
