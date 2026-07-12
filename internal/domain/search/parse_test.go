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

func TestParseMoreOperators(t *testing.T) {
	q := Parse(`report has:attachment has:image is:dm from:42`)
	if q.Text != "report" {
		t.Fatalf("text = %q", q.Text)
	}
	if !q.HasAttachment {
		t.Fatal("has:attachment not set")
	}
	if !q.HasImage {
		t.Fatal("has:image not set")
	}
	if !q.IsDM {
		t.Fatal("is:dm not set")
	}
	if q.FromID != 42 || q.From != "" {
		t.Fatalf("from:42 should set FromID=42 not From: FromID=%d From=%q", q.FromID, q.From)
	}

	// A non-numeric from: value stays name/email resolution, never an id.
	nq := Parse("from:bob@x.test")
	if nq.From != "bob@x.test" || nq.FromID != 0 {
		t.Fatalf("from:bob@x.test wrong: From=%q FromID=%d", nq.From, nq.FromID)
	}

	// A digit string that overflows int64 is not a real id: fall back to
	// name/email resolution (which will simply match nothing) rather than
	// silently coercing it to a bogus id.
	oq := Parse("from:99999999999999999999999999")
	if oq.FromID != 0 || oq.From != "99999999999999999999999999" {
		t.Fatalf("overflow from: wrong: From=%q FromID=%d", oq.From, oq.FromID)
	}

	// A filters-only query with just has:image is a valid (non-empty) search.
	if Parse("has:image").Empty() {
		t.Fatal("has:image alone should not be Empty")
	}
	if Parse("from:7").Empty() {
		t.Fatal("from:<id> alone should not be Empty")
	}

	// Unknown has:/is: values are literal text, never operators (no error path).
	fq := Parse("has:frobnicate is:wibble")
	if fq.HasLink || fq.HasAttachment || fq.HasImage || fq.IsDM || fq.Resolved != nil {
		t.Fatalf("unknown has:/is: values must set no filter: %+v", fq)
	}
	for _, want := range []string{"has:frobnicate", "is:wibble"} {
		if !strings.Contains(fq.Text, want) {
			t.Fatalf("text %q missing literal %q", fq.Text, want)
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
