package automation

import (
	"encoding/json"
	"testing"
)

func TestParsePath(t *testing.T) {
	ok := []string{
		"event.x", "event.channel_id", "event.body.x",
		"event.a.b.c.d", "event.a.b.c.d.e", "event.a_1.b2",
	}
	for _, p := range ok {
		if _, valid := parsePath(p); !valid {
			t.Errorf("parsePath(%q) = invalid, want valid", p)
		}
	}
	bad := []string{
		"", "event", "event.", "x.y", "channel_id",
		"event.a.b.c.d.e.f", // 6 segments
		"event.A",           // uppercase
		"event.a-b",         // dash
		"event.a..b",        // empty segment
		"event. x",          // space
		"event.x ",          // trailing space
		"Event.x",           // wrong prefix case
	}
	for _, p := range bad {
		if _, valid := parsePath(p); valid {
			t.Errorf("parsePath(%q) = valid, want invalid", p)
		}
	}
}

func TestMatchConditions(t *testing.T) {
	cond := func(path, op, value string) Condition {
		c := Condition{Path: path, Op: op}
		if value != "" {
			c.Value = json.RawMessage(value)
		}
		return c
	}
	cases := []struct {
		name    string
		payload string
		conds   []Condition
		want    bool
	}{
		// eq / ne, strings.
		{"eq string hit", `{"s":"go"}`, []Condition{cond("event.s", "eq", `"go"`)}, true},
		{"eq string miss", `{"s":"go"}`, []Condition{cond("event.s", "eq", `"rust"`)}, false},
		{"ne string hit", `{"s":"go"}`, []Condition{cond("event.s", "ne", `"rust"`)}, true},
		{"ne string miss", `{"s":"go"}`, []Condition{cond("event.s", "ne", `"go"`)}, false},

		// STRICT typing — no coercion, either direction.
		{"eq num vs num", `{"n":42}`, []Condition{cond("event.n", "eq", `42`)}, true},
		{"eq num vs string is strict-false", `{"n":42}`, []Condition{cond("event.n", "eq", `"42"`)}, false},
		{"eq string vs num is strict-false", `{"s":"42"}`, []Condition{cond("event.s", "eq", `42`)}, false},
		{"eq bool", `{"b":true}`, []Condition{cond("event.b", "eq", `true`)}, true},
		{"eq bool vs num is strict-false", `{"b":true}`, []Condition{cond("event.b", "eq", `1`)}, false},

		// Integer-exact equality beyond float64's 2^53.
		{"eq bigint exact hit", `{"id":9007199254740993}`,
			[]Condition{cond("event.id", "eq", `9007199254740993`)}, true},
		{"eq bigint neighbour miss (float would collide)", `{"id":9007199254740993}`,
			[]Condition{cond("event.id", "eq", `9007199254740992`)}, false},

		// Ordering — numbers only.
		{"gt hit", `{"n":10}`, []Condition{cond("event.n", "gt", `5`)}, true},
		{"gt miss", `{"n":3}`, []Condition{cond("event.n", "gt", `5`)}, false},
		{"gte edge", `{"n":5}`, []Condition{cond("event.n", "gte", `5`)}, true},
		{"lt hit", `{"n":3}`, []Condition{cond("event.n", "lt", `5`)}, true},
		{"lte edge", `{"n":5}`, []Condition{cond("event.n", "lte", `5`)}, true},
		{"gt on string is strict-false", `{"s":"9"}`, []Condition{cond("event.s", "gt", `5`)}, false},

		// contains — string substring.
		{"contains hit", `{"s":"hello world"}`, []Condition{cond("event.s", "contains", `"o w"`)}, true},
		{"contains miss", `{"s":"hello"}`, []Condition{cond("event.s", "contains", `"xyz"`)}, false},
		{"contains on number is false", `{"n":123}`, []Condition{cond("event.n", "contains", `"2"`)}, false},

		// in — membership with strict typing.
		{"in number hit", `{"n":2}`, []Condition{cond("event.n", "in", `[1,2,3]`)}, true},
		{"in number miss", `{"n":9}`, []Condition{cond("event.n", "in", `[1,2,3]`)}, false},
		{"in string hit", `{"s":"b"}`, []Condition{cond("event.s", "in", `["a","b"]`)}, true},
		{"in string vs number array is strict-false", `{"s":"2"}`, []Condition{cond("event.s", "in", `[1,2,3]`)}, false},

		// exists / not_exists, with present-but-null counting as ABSENT.
		{"exists present", `{"x":5}`, []Condition{cond("event.x", "exists", "")}, true},
		{"exists missing", `{}`, []Condition{cond("event.x", "exists", "")}, false},
		{"exists null-is-absent", `{"x":null}`, []Condition{cond("event.x", "exists", "")}, false},
		{"not_exists missing", `{}`, []Condition{cond("event.x", "not_exists", "")}, true},
		{"not_exists null-is-absent", `{"x":null}`, []Condition{cond("event.x", "not_exists", "")}, true},
		{"not_exists present", `{"x":5}`, []Condition{cond("event.x", "not_exists", "")}, false},

		// A value op on a missing/null path is a miss, never an error.
		{"eq on missing path", `{}`, []Condition{cond("event.x", "eq", `5`)}, false},
		{"eq on null path", `{"x":null}`, []Condition{cond("event.x", "eq", `5`)}, false},

		// Nested paths and multi-condition AND.
		{"nested hit", `{"body":{"x":"v"}}`, []Condition{cond("event.body.x", "eq", `"v"`)}, true},
		{"nested through non-object miss", `{"body":7}`, []Condition{cond("event.body.x", "exists", "")}, false},
		{"AND all hit", `{"a":1,"b":"y"}`,
			[]Condition{cond("event.a", "eq", `1`), cond("event.b", "eq", `"y"`)}, true},
		{"AND one miss", `{"a":1,"b":"y"}`,
			[]Condition{cond("event.a", "eq", `1`), cond("event.b", "eq", `"z"`)}, false},

		// No conditions always matches (legacy definitions).
		{"empty conditions", `{"a":1}`, nil, true},
	}
	for _, tc := range cases {
		if got := matchConditions(tc.conds, json.RawMessage(tc.payload)); got != tc.want {
			t.Errorf("%s: matchConditions = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestValidateConditions(t *testing.T) {
	c := func(path, op, value string) Condition {
		cc := Condition{Path: path, Op: op}
		if value != "" {
			cc.Value = json.RawMessage(value)
		}
		return cc
	}
	valid := [][]Condition{
		{c("event.x", "eq", `5`)},
		{c("event.x", "eq", `"s"`)},
		{c("event.x", "ne", `true`)},
		{c("event.x", "gt", `3`)},
		{c("event.x", "lte", `3.5`)},
		{c("event.s", "contains", `"a"`)},
		{c("event.n", "in", `[1,2,3]`)},
		{c("event.s", "in", `["a","b"]`)},
		{c("event.x", "exists", "")},
		{c("event.x", "not_exists", "")},
	}
	for i, conds := range valid {
		if err := validateConditions(conds); err != nil {
			t.Errorf("valid[%d] = %v, want nil", i, err)
		}
	}
	// Eleven conditions exceeds the cap.
	tooMany := make([]Condition, 11)
	for i := range tooMany {
		tooMany[i] = c("event.x", "exists", "")
	}
	inTwentyOne := "["
	for i := 0; i < 21; i++ {
		if i > 0 {
			inTwentyOne += ","
		}
		inTwentyOne += "1"
	}
	inTwentyOne += "]"
	invalid := map[string][]Condition{
		"too many":          tooMany,
		"bad path":          {c("x.y", "eq", `5`)},
		"unknown op":        {c("event.x", "like", `"a"`)},
		"eq array value":    {c("event.x", "eq", `[1,2]`)},
		"eq object value":   {c("event.x", "eq", `{"a":1}`)},
		"eq null value":     {c("event.x", "eq", `null`)},
		"eq no value":       {c("event.x", "eq", "")},
		"gt string value":   {c("event.x", "gt", `"s"`)},
		"contains number":   {c("event.x", "contains", `5`)},
		"in not array":      {c("event.x", "in", `5`)},
		"in empty":          {c("event.x", "in", `[]`)},
		"in mixed types":    {c("event.x", "in", `[1,"a"]`)},
		"in too many":       {c("event.x", "in", inTwentyOne)},
		"in non-scalar":     {c("event.x", "in", `[[1]]`)},
		"exists with value": {c("event.x", "exists", `5`)},
	}
	for name, conds := range invalid {
		if err := validateConditions(conds); err == nil {
			t.Errorf("%s: validateConditions = nil, want error", name)
		}
	}
}
