// Package search is the unified, ACL-scoped message search (ADR-010).
//
// Cross-entity by design (S-1): search legitimately reads across modules'
// tables (messages, and resolves user/channel names) — it is READ-ONLY and is
// the one place the module-ownership rule yields to the query layer, exactly
// as the ADR intends. Results are scoped to what the actor can see (S-2).
package search

import (
	"strings"
	"time"
)

// Query is a parsed search: free text (→ Postgres FTS) plus the v1 operator
// subset of ADR-010 S-4. Unknown or malformed operators fall through to free
// text, so `foo:bar` a user didn't intend as an operator still searches
// literally rather than erroring.
type Query struct {
	Text      string // free text (quotes preserved for phrase search)
	From      string // from: author full-name or email
	InChannel string // in: channel name
	HasLink   bool   // has:link
	Resolved  *bool  // is:resolved / is:unresolved
	After     *time.Time
	Before    *time.Time
}

// Empty reports whether the query has no criteria at all.
func (q Query) Empty() bool {
	return q.Text == "" && q.From == "" && q.InChannel == "" &&
		!q.HasLink && q.Resolved == nil && q.After == nil && q.Before == nil
}

var knownOps = map[string]bool{
	"from": true, "in": true, "has": true, "is": true, "before": true, "after": true,
}

const dateLayout = "2006-01-02"

// Parse turns a raw query string into a Query.
func Parse(raw string) Query {
	var q Query
	var text []string
	keep := func(tok string) { text = append(text, tok) } // preserve quotes

	for _, tok := range tokenize(raw) {
		key, val, isOp := splitOp(tok)
		if !isOp || !knownOps[key] {
			keep(tok)
			continue
		}
		v := stripQuotes(val)
		switch key {
		case "from":
			if v == "" {
				keep(tok)
			} else {
				q.From = v
			}
		case "in":
			if v == "" {
				keep(tok)
			} else {
				q.InChannel = strings.TrimPrefix(v, "#")
			}
		case "has":
			if v == "link" {
				q.HasLink = true
			} else {
				keep(tok)
			}
		case "is":
			switch v {
			case "resolved":
				t := true
				q.Resolved = &t
			case "unresolved":
				f := false
				q.Resolved = &f
			default:
				keep(tok)
			}
		case "before":
			if d, err := time.Parse(dateLayout, v); err == nil {
				q.Before = &d
			} else {
				keep(tok)
			}
		case "after":
			if d, err := time.Parse(dateLayout, v); err == nil {
				q.After = &d
			} else {
				keep(tok)
			}
		}
	}
	q.Text = strings.TrimSpace(strings.Join(text, " "))
	return q
}

// tokenize splits on spaces but keeps double-quoted regions together, so
// `from:"Alice Chen"` and `"exact phrase"` each stay one token.
func tokenize(s string) []string {
	var toks []string
	var cur strings.Builder
	inQuote := false
	flush := func() {
		if cur.Len() > 0 {
			toks = append(toks, cur.String())
			cur.Reset()
		}
	}
	for _, r := range s {
		switch {
		case r == '"':
			inQuote = !inQuote
			cur.WriteRune(r)
		case r == ' ' && !inQuote:
			flush()
		default:
			cur.WriteRune(r)
		}
	}
	flush()
	return toks
}

// splitOp classifies a token as `key:value` iff the key is a bare word before
// the first colon (no quote/space in the key).
func splitOp(tok string) (key, val string, ok bool) {
	i := strings.IndexByte(tok, ':')
	if i <= 0 {
		return "", "", false
	}
	key = tok[:i]
	if strings.ContainsAny(key, "\" ") {
		return "", "", false
	}
	return strings.ToLower(key), tok[i+1:], true
}

func stripQuotes(s string) string {
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return s[1 : len(s)-1]
	}
	return s
}
