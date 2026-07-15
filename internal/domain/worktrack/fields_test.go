package worktrack

import "testing"

// TestFieldValidate is the per-type table for the custom-field value validator
// (P-13). The "number rejects a string" case is the red/green pin: dropping the
// checked type assertion in fieldValidate's ftNumber arm makes it fail.
func TestFieldValidate(t *testing.T) {
	sel := FieldOptions{Choices: []string{"low", "high"}}
	multi := FieldOptions{Choices: []string{"a", "b", "c"}}
	cases := []struct {
		name  string
		def   fieldDef
		value any
		valid bool
	}{
		{"text_short accepts a string", fieldDef{key: "note", ftype: ftTextShort}, "hi", true},
		{"text_short rejects a number", fieldDef{key: "note", ftype: ftTextShort}, 3.0, false},
		{"text_long accepts a string", fieldDef{key: "body", ftype: ftTextLong}, "a longer body", true},
		{"number accepts a float", fieldDef{key: "pts", ftype: ftNumber}, 5.0, true},
		{"number rejects a string", fieldDef{key: "pts", ftype: ftNumber}, "5", false},
		{"number rejects a bool", fieldDef{key: "pts", ftype: ftNumber}, true, false},
		{"date accepts an ISO date", fieldDef{key: "due", ftype: ftDate}, "2026-07-16", true},
		{"date rejects garbage", fieldDef{key: "due", ftype: ftDate}, "16/07/2026", false},
		{"date rejects a number", fieldDef{key: "due", ftype: ftDate}, 20260716.0, false},
		{"checkbox accepts a bool", fieldDef{key: "flag", ftype: ftCheckbox}, true, true},
		{"checkbox rejects a string", fieldDef{key: "flag", ftype: ftCheckbox}, "true", false},
		{"select accepts a choice", fieldDef{key: "pri", ftype: ftSelect, options: sel}, "high", true},
		{"select rejects a non-choice", fieldDef{key: "pri", ftype: ftSelect, options: sel}, "urgent", false},
		{"select rejects a non-string", fieldDef{key: "pri", ftype: ftSelect, options: sel}, 1.0, false},
		{"multi_select accepts choices", fieldDef{key: "tags", ftype: ftMultiSelect, options: multi}, []any{"a", "c"}, true},
		{"multi_select accepts empty", fieldDef{key: "tags", ftype: ftMultiSelect, options: multi}, []any{}, true},
		{"multi_select rejects a non-choice element", fieldDef{key: "tags", ftype: ftMultiSelect, options: multi}, []any{"a", "z"}, false},
		{"multi_select rejects a non-array", fieldDef{key: "tags", ftype: ftMultiSelect, options: multi}, "a", false},
		{"multi_select rejects a non-string element", fieldDef{key: "tags", ftype: ftMultiSelect, options: multi}, []any{1.0}, false},
		{"unsupported type rejects", fieldDef{key: "x", ftype: ftURL}, "https://x", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := fieldValidate(c.def, c.value)
			if c.valid && err != nil {
				t.Fatalf("want valid, got error: %v", err)
			}
			if !c.valid && err == nil {
				t.Fatalf("want error, got nil")
			}
		})
	}
}
