package automation

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/abhinavjha0239/weft/internal/domain/content"
)

func mustParse(s string) *content.Node { return content.Parse(s, noopResolver) }

func TestFindSpans(t *testing.T) {
	cases := []struct {
		in    string
		inner []string
	}{
		{"no spans here", nil},
		{"{{event.x}}", []string{"event.x"}},
		{"a {{event.x}} b {{event.y}} c", []string{"event.x", "event.y"}},
		{"{{ event.x }}", []string{" event.x "}},
		{"{{event.x}", nil},      // no closer
		{"{{ unterminated", nil}, // no closer
		{"}}{{event.x}}", []string{"event.x"}},
	}
	for _, tc := range cases {
		spans := findSpans(tc.in)
		if len(spans) != len(tc.inner) {
			t.Errorf("findSpans(%q) = %d spans, want %d", tc.in, len(spans), len(tc.inner))
			continue
		}
		for i, sp := range spans {
			if sp.inner != tc.inner[i] {
				t.Errorf("findSpans(%q)[%d].inner = %q, want %q", tc.in, i, sp.inner, tc.inner[i])
			}
			// Offsets must bracket exactly "{{inner}}".
			if got := tc.in[sp.start:sp.end]; got != "{{"+sp.inner+"}}" {
				t.Errorf("findSpans(%q)[%d] offsets slice %q, want %q", tc.in, i, got, "{{"+sp.inner+"}}")
			}
		}
	}
}

func TestValidateStepContent(t *testing.T) {
	ok := []string{
		"plain text, no spans",
		"hi {{event.key}}",
		"{{ event.channel_id }} spaced",
		"@**Alice Chen** please review {{event.key}}", // mention beside a span is fine
		"{{event.a.b.c}}",
	}
	for _, c := range ok {
		if err := validateStepContent(0, c); err != nil {
			t.Errorf("validateStepContent(%q) = %v, want nil", c, err)
		}
	}
	bad := map[string]string{
		"invalid path":        "{{event.bad path}}",
		"no event prefix":     "{{whatever}}",
		"template in mention": "@**{{event.x}}**",
		"mention prefix span": "@**Bob {{event.x}}**",
	}
	for name, c := range bad {
		if err := validateStepContent(0, c); err == nil {
			t.Errorf("%s: validateStepContent(%q) = nil, want error", name, c)
		}
	}
	// More than 20 spans is rejected.
	many := strings.Repeat("{{event.x}}", 21)
	if err := validateStepContent(0, many); err == nil {
		t.Error("21 spans: validateStepContent = nil, want error")
	}
}

func TestExpandTemplate(t *testing.T) {
	root := decodeUseNumber(json.RawMessage(
		`{"key":"OPS-7","n":42,"big":9007199254740993,"flag":true,"nul":null,"obj":{"a":1},"arr":[1,2]}`))
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"static", "hello", "hello"},
		{"string verbatim", "item {{event.key}}", "item OPS-7"},
		{"number verbatim", "n={{event.n}}", "n=42"},
		{"bigint verbatim", "id={{event.big}}", "id=9007199254740993"},
		{"bool true", "f={{event.flag}}", "f=true"},
		{"missing empty", "x=[{{event.missing}}]", "x=[]"},
		{"null empty", "x=[{{event.nul}}]", "x=[]"},
		{"spaced path", "k={{ event.key }}", "k=OPS-7"},
		{"two spans", "{{event.key}}/{{event.n}}", "OPS-7/42"},
	}
	for _, tc := range cases {
		got, err := expandTemplate(0, tc.in, root)
		if err != nil {
			t.Errorf("%s: expandTemplate(%q) error %v", tc.name, tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("%s: expandTemplate(%q) = %q, want %q", tc.name, tc.in, got, tc.want)
		}
	}
	// Object/array paths are non-scalar: the step fails.
	for _, path := range []string{"event.obj", "event.arr"} {
		if _, err := expandTemplate(0, "v={{"+path+"}}", root); err == nil ||
			!strings.Contains(err.Error(), "resolves to a non-scalar") {
			t.Errorf("expandTemplate(%q) err = %v, want non-scalar", path, err)
		}
	}
}

func TestRenderStepMentionGuard(t *testing.T) {
	root := decodeUseNumber(json.RawMessage(`{"who":"@**Bob Ray**","key":"OPS-7"}`))

	// A value that expands to no new mention passes and preserves any literal
	// mention's label multiset.
	if _, err := renderStep(0, "New item {{event.key}}",
		mentionLabels(mustParse("New item {{event.key}}")), root); err != nil {
		t.Fatalf("clean render = %v, want ok", err)
	}
	if _, err := renderStep(0, "@**Alice Chen** re {{event.key}}",
		mentionLabels(mustParse("@**Alice Chen** re {{event.key}}")), root); err != nil {
		t.Fatalf("literal-mention render = %v, want ok", err)
	}

	// A value that SMUGGLES a mention alters the multiset — the step fails.
	litLabels := mentionLabels(mustParse("{{event.who}}"))
	_, err := renderStep(0, "{{event.who}}", litLabels, root)
	if err == nil || !strings.Contains(err.Error(), "template expansion may not alter mentions") {
		t.Fatalf("smuggle render err = %v, want mention-guard failure", err)
	}

	// Overflow past the content cap fails rather than truncating.
	big := decodeUseNumber(json.RawMessage(
		`{"x":"` + strings.Repeat("a", maxContentLen+1) + `"}`))
	_, err = renderStep(0, "{{event.x}}", nil, big)
	if err == nil || !strings.Contains(err.Error(), "post-expansion content") {
		t.Fatalf("overflow render err = %v, want length failure", err)
	}
}
