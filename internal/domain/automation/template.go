package automation

// Templating lets a post_message step interpolate the trigger event's payload
// into its Content via {{event.path}} spans (the same path grammar conditions
// use). The load-bearing security property is the MENTION-INJECTION GUARD:
// payloads are attacker-influenceable (P-23 adds webhook bodies and slash
// text), and a value carrying "@**Real Name**" would, once posted, resolve to
// a real user and fan out notifications through doc.Mentions(). The guard is
// STRUCTURAL, not escaping:
//
//   - Write time: reject any definition whose literal step content templates
//     INSIDE mention syntax (a mention node's label containing a span).
//   - Execute time: parse the literal content and the EXPANDED content with a
//     no-op resolver and require their mention-label MULTISETS to match — any
//     drift fails the step. Multiset (not node count) equality is required
//     because a crafted value can backslash-suppress one literal mention while
//     smuggling another for a net-zero count.
//
// Formatting injection (a value introducing bold/code spans) is cosmetic and
// accepted — recorded as a gap, not guarded here.

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/abhinavjha0239/weft/internal/domain/content"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

const maxSpans = 20

// noopResolver is the mention resolver used for guard parses: it leaves every
// mention unresolved (no user_id) so no lookup happens, while the parser still
// creates the mention NODE carrying its label — which is all the guard reads.
func noopResolver(string) (int64, bool) { return 0, false }

// span is one {{…}} occurrence located by byte offset, with the raw text
// between the braces.
type span struct {
	start int
	end   int
	inner string
}

// findSpans locates every {{…}} span, each closing at the FIRST following }}.
// A trailing {{ with no closer is not a span (left as literal text). The scan
// is shared by validation, the templated-step probe, and expansion so all
// three agree on what a span is.
func findSpans(s string) []span {
	var out []span
	i := 0
	for {
		rel := strings.Index(s[i:], "{{")
		if rel < 0 {
			break
		}
		openIdx := i + rel
		crel := strings.Index(s[openIdx+2:], "}}")
		if crel < 0 {
			break
		}
		closeIdx := openIdx + 2 + crel
		out = append(out, span{start: openIdx, end: closeIdx + 2, inner: s[openIdx+2 : closeIdx]})
		i = closeIdx + 2
	}
	return out
}

// validateStepContent enforces the templating grammar and the write-time half
// of the mention guard on one post_message step's literal content.
func validateStepContent(step int, c string) error {
	spans := findSpans(c)
	if len(spans) > maxSpans {
		return apperr.Invalid(fmt.Sprintf("definition: step %d: at most %d template spans", step, maxSpans))
	}
	for _, sp := range spans {
		if _, ok := parsePath(strings.TrimSpace(sp.inner)); !ok {
			return apperr.Invalid(fmt.Sprintf("definition: step %d: invalid template span {{%s}}", step, sp.inner))
		}
	}
	for _, label := range mentionLabels(content.Parse(c, noopResolver)) {
		if strings.Contains(label, "{{") {
			return apperr.Invalid(fmt.Sprintf("definition: step %d: template spans are not allowed inside a mention", step))
		}
	}
	return nil
}

// compiledStep caches, per rule load, whatever a step needs at execute so the
// literal-side parse happens once per load and not once per event.
type compiledStep struct {
	// templated is true iff the step has at least one valid-path span; a
	// static step (every legacy step) posts its content verbatim, exactly as
	// before P-22.
	templated bool
	// litLabels is the sorted mention-label multiset of the LITERAL content,
	// meaningful only when templated.
	litLabels []string
}

func compileSteps(def Definition) []compiledStep {
	out := make([]compiledStep, len(def.Steps))
	for i, st := range def.Steps {
		if !hasTemplateSpan(st.Content) {
			continue
		}
		out[i] = compiledStep{
			templated: true,
			litLabels: mentionLabels(content.Parse(st.Content, noopResolver)),
		}
	}
	return out
}

func hasTemplateSpan(s string) bool {
	for _, sp := range findSpans(s) {
		if _, ok := parsePath(strings.TrimSpace(sp.inner)); ok {
			return true
		}
	}
	return false
}

// renderStep expands a templated step against the UseNumber-decoded payload
// and enforces the length bound and the execute-side mention guard. On failure
// it returns an error whose message becomes the step's run trace.
func renderStep(step int, tmpl string, litLabels []string, root any) (string, error) {
	expanded, err := expandTemplate(step, tmpl, root)
	if err != nil {
		return "", err
	}
	if len(expanded) == 0 || len(expanded) > maxContentLen {
		return "", fmt.Errorf("step %d: post-expansion content must be 1..%d chars", step, maxContentLen)
	}
	if !equalLabels(litLabels, mentionLabels(content.Parse(expanded, noopResolver))) {
		return "", fmt.Errorf("step %d: template expansion may not alter mentions", step)
	}
	return expanded, nil
}

// expandTemplate substitutes each valid-path span with its resolved scalar:
// a string or json.Number verbatim, a bool as true/false, a missing/null path
// as "". A path resolving to an object or array fails the step. Text outside
// valid spans is untouched, and an invalid span (only reachable via a legacy
// definition, since validation rejects them at write) is emitted verbatim.
func expandTemplate(step int, tmpl string, root any) (string, error) {
	spans := findSpans(tmpl)
	if len(spans) == 0 {
		return tmpl, nil
	}
	var b strings.Builder
	last := 0
	for _, sp := range spans {
		b.WriteString(tmpl[last:sp.start])
		last = sp.end
		inner := strings.TrimSpace(sp.inner)
		segs, ok := parsePath(inner)
		if !ok {
			b.WriteString(tmpl[sp.start:sp.end])
			continue
		}
		val, present := resolvePath(root, segs)
		if !present {
			continue
		}
		s, ok := scalarString(val)
		if !ok {
			return "", fmt.Errorf("step %d: path %q resolves to a non-scalar", step, inner)
		}
		b.WriteString(s)
	}
	b.WriteString(tmpl[last:])
	return b.String(), nil
}

func scalarString(v any) (string, bool) {
	switch t := v.(type) {
	case string:
		return t, true
	case json.Number:
		return string(t), true
	case bool:
		if t {
			return "true", true
		}
		return "false", true
	}
	return "", false
}

// mentionLabels returns the sorted multiset of mention-node labels in a parsed
// document. Sorting makes slice equality a multiset comparison.
func mentionLabels(n *content.Node) []string {
	var out []string
	var walk func(*content.Node)
	walk = func(nd *content.Node) {
		if nd.Type == content.NodeMention {
			if lbl, ok := nd.Attrs["label"].(string); ok {
				out = append(out, lbl)
			}
		}
		for _, c := range nd.Content {
			walk(c)
		}
	}
	walk(n)
	sort.Strings(out)
	return out
}

func equalLabels(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
