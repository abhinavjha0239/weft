package automation

// Conditions are the AU-1 in-memory filters that run inside match(), before
// any DB work: a rule with conditions fires only when EVERY condition holds
// against the trigger event's payload. A condition miss creates NO run row —
// deliberate at Slack scale, where an org-wide message.created rule must not
// write a row per non-matching message (the AU-2 dry-run debugger is the
// recorded gap for "why didn't it fire").
//
// Typing is STRICT and coercion-free: "42" never equals 42. The payload is
// decoded ONCE per evaluation with json.Decoder.UseNumber() so BIGINT ids
// (which exceed float64's 2^53) compare as canonical integer strings, not as
// lossy floats.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

const (
	maxConditions   = 10
	maxInValues     = 20
	maxPathSegments = 5
)

// Condition is one ANDed filter. Value carries the comparison operand as raw
// JSON so absence (exists/not_exists take none) is distinguishable from an
// explicit null, and so integer operands survive without float rounding.
type Condition struct {
	Path  string          `json:"path"`
	Op    string          `json:"op"`
	Value json.RawMessage `json:"value,omitempty"`
}

// parsePath validates and splits a path of the form `event.` + 1..5 dot
// segments of [a-z0-9_]+, returning the segments AFTER the event prefix (the
// keys to walk into the event payload). The grammar is shared verbatim by
// conditions and by {{event.path}} template spans.
func parsePath(p string) ([]string, bool) {
	const prefix = "event."
	if !strings.HasPrefix(p, prefix) {
		return nil, false
	}
	rest := p[len(prefix):]
	if rest == "" {
		return nil, false
	}
	segs := strings.Split(rest, ".")
	if len(segs) > maxPathSegments {
		return nil, false
	}
	for _, s := range segs {
		if !validSegment(s) {
			return nil, false
		}
	}
	return segs, true
}

func validSegment(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if !(r == '_' || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')) {
			return false
		}
	}
	return true
}

// resolvePath walks the decoded payload to the path's value. It reports
// PRESENT=false when any segment is missing, when a non-object blocks the
// descent, OR when the resolved value is null — a present-but-null value
// counts as absent (only exists/not_exists observe the distinction, and
// templating renders it as the empty string).
func resolvePath(root any, segs []string) (any, bool) {
	cur := root
	for _, seg := range segs {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		v, ok := m[seg]
		if !ok {
			return nil, false
		}
		cur = v
	}
	if cur == nil {
		return nil, false
	}
	return cur, true
}

// decodeUseNumber decodes raw JSON into an any tree with numbers preserved as
// json.Number (never float64). Used for both the event payload and condition
// operands, so integer comparisons stay exact past 2^53.
func decodeUseNumber(raw json.RawMessage) any {
	if len(raw) == 0 {
		return nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if dec.Decode(&v) != nil {
		return nil
	}
	return v
}

// validateConditions enforces the grammar at write time: ≤10 conditions, each
// with a valid path, a known op, and an operand whose JSON type the op
// accepts. A bad path, unknown op, or type-invalid operand is a 400.
func validateConditions(conds []Condition) error {
	if len(conds) > maxConditions {
		return apperr.Invalid(fmt.Sprintf("definition: at most %d conditions", maxConditions))
	}
	for i, c := range conds {
		if _, ok := parsePath(c.Path); !ok {
			return apperr.Invalid(fmt.Sprintf("definition: condition %d: invalid path %q", i, c.Path))
		}
		if err := validateConditionValue(i, c); err != nil {
			return err
		}
	}
	return nil
}

func validateConditionValue(i int, c Condition) error {
	present := len(bytes.TrimSpace(c.Value)) > 0
	switch c.Op {
	case "exists", "not_exists":
		if present {
			return apperr.Invalid(fmt.Sprintf("definition: condition %d: %s takes no value", i, c.Op))
		}
		return nil
	case "eq", "ne":
		if !present {
			return apperr.Invalid(fmt.Sprintf("definition: condition %d: %s requires a value", i, c.Op))
		}
		if _, ok := jsonScalarType(c.Value); !ok {
			return apperr.Invalid(fmt.Sprintf("definition: condition %d: %s value must be a number, string, or bool", i, c.Op))
		}
		return nil
	case "gt", "lt", "gte", "lte":
		if !present {
			return apperr.Invalid(fmt.Sprintf("definition: condition %d: %s requires a value", i, c.Op))
		}
		if t, _ := jsonScalarType(c.Value); t != 'n' {
			return apperr.Invalid(fmt.Sprintf("definition: condition %d: %s value must be a number", i, c.Op))
		}
		return nil
	case "contains":
		if !present {
			return apperr.Invalid(fmt.Sprintf("definition: condition %d: contains requires a value", i))
		}
		if t, _ := jsonScalarType(c.Value); t != 's' {
			return apperr.Invalid(fmt.Sprintf("definition: condition %d: contains value must be a string", i))
		}
		return nil
	case "in":
		return validateInValue(i, c.Value)
	default:
		return apperr.Invalid(fmt.Sprintf("definition: condition %d: unknown op %q", i, c.Op))
	}
}

// validateInValue requires a non-empty array of at most 20 scalars that share
// one JSON type (mixed-type membership would defeat strict typing).
func validateInValue(i int, raw json.RawMessage) error {
	if len(bytes.TrimSpace(raw)) == 0 {
		return apperr.Invalid(fmt.Sprintf("definition: condition %d: in requires a value", i))
	}
	var arr []json.RawMessage
	if json.Unmarshal(raw, &arr) != nil {
		return apperr.Invalid(fmt.Sprintf("definition: condition %d: in value must be an array", i))
	}
	if len(arr) == 0 || len(arr) > maxInValues {
		return apperr.Invalid(fmt.Sprintf("definition: condition %d: in requires 1..%d values", i, maxInValues))
	}
	var typ byte
	for j, el := range arr {
		t, ok := jsonScalarType(el)
		if !ok {
			return apperr.Invalid(fmt.Sprintf("definition: condition %d: in values must be scalars", i))
		}
		if j == 0 {
			typ = t
		} else if t != typ {
			return apperr.Invalid(fmt.Sprintf("definition: condition %d: in values must share one type", i))
		}
	}
	return nil
}

// jsonScalarType classifies a raw JSON operand: 's' string, 'n' number, 'b'
// bool; ok=false for null, arrays, and objects (never scalar operands). The
// bytes are already well-formed JSON — they arrived through the definition
// decode — so a first-rune check is sufficient.
func jsonScalarType(raw json.RawMessage) (byte, bool) {
	s := bytes.TrimSpace(raw)
	if len(s) == 0 {
		return 0, false
	}
	switch s[0] {
	case '"':
		return 's', true
	case 't', 'f':
		return 'b', true
	case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		return 'n', true
	}
	return 0, false
}

// matchConditions reports whether EVERY condition holds. The payload is
// decoded exactly once here (UseNumber), then shared across the conditions.
func matchConditions(conds []Condition, payload json.RawMessage) bool {
	if len(conds) == 0 {
		return true
	}
	root := decodeUseNumber(payload)
	for _, c := range conds {
		if !evalCondition(c, root) {
			return false
		}
	}
	return true
}

func evalCondition(c Condition, root any) bool {
	segs, ok := parsePath(c.Path)
	if !ok {
		return false
	}
	val, present := resolvePath(root, segs)
	switch c.Op {
	case "exists":
		return present
	case "not_exists":
		return !present
	}
	// Every remaining op needs a present value; a missing/null path is a miss,
	// not an error (a strict-type mismatch is likewise simply false).
	if !present {
		return false
	}
	switch c.Op {
	case "eq":
		return scalarEqual(val, c.Value)
	case "ne":
		return !scalarEqual(val, c.Value)
	case "gt", "lt", "gte", "lte":
		return numCompare(c.Op, val, c.Value)
	case "contains":
		return strContains(val, c.Value)
	case "in":
		return inArray(val, c.Value)
	}
	return false
}

// scalarEqual is strict same-JSON-type equality with no coercion. Numbers
// compare exactly: two integer forms (no '.', 'e', or 'E') by canonical
// string so BIGINT ids never collide through float64, otherwise by Float64.
func scalarEqual(val any, raw json.RawMessage) bool {
	rhs := decodeUseNumber(raw)
	switch l := val.(type) {
	case string:
		r, ok := rhs.(string)
		return ok && l == r
	case bool:
		r, ok := rhs.(bool)
		return ok && l == r
	case json.Number:
		r, ok := rhs.(json.Number)
		return ok && numberEqual(l, r)
	}
	return false
}

func numberEqual(a, b json.Number) bool {
	if isIntForm(a) && isIntForm(b) {
		return string(a) == string(b)
	}
	fa, ea := a.Float64()
	fb, eb := b.Float64()
	return ea == nil && eb == nil && fa == fb
}

func isIntForm(n json.Number) bool {
	return !strings.ContainsAny(string(n), ".eE")
}

// numCompare orders two numbers via Float64. A non-number operand on either
// side (a strict-type mismatch) is simply false.
func numCompare(op string, val any, raw json.RawMessage) bool {
	ln, ok := val.(json.Number)
	if !ok {
		return false
	}
	rn, ok := decodeUseNumber(raw).(json.Number)
	if !ok {
		return false
	}
	lf, e1 := ln.Float64()
	rf, e2 := rn.Float64()
	if e1 != nil || e2 != nil {
		return false
	}
	switch op {
	case "gt":
		return lf > rf
	case "lt":
		return lf < rf
	case "gte":
		return lf >= rf
	case "lte":
		return lf <= rf
	}
	return false
}

func strContains(val any, raw json.RawMessage) bool {
	l, ok := val.(string)
	if !ok {
		return false
	}
	r, ok := decodeUseNumber(raw).(string)
	if !ok {
		return false
	}
	return strings.Contains(l, r)
}

// inArray reports strict membership: val must equal some array element under
// the same same-JSON-type rules as eq.
func inArray(val any, raw json.RawMessage) bool {
	var arr []json.RawMessage
	if json.Unmarshal(raw, &arr) != nil {
		return false
	}
	for _, el := range arr {
		if scalarEqual(val, el) {
			return true
		}
	}
	return false
}
