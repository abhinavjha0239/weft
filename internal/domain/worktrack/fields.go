// Custom fields (ADR-009 W-2): ONE typed field system per Space — a taxonomy
// of field_defs (key/name/type/applies_to/required/options) plus per-item
// values stored in work_item.fields JSONB (GIN-indexed since 0005). This file
// owns the field_def CRUD, the Go type registry, the per-type value validator
// (fieldValidate), and the create/update helpers that weave validation into
// item writes. All of it rides VerbEditItems org-wide in v1 (a per-space admin
// verb is a recorded gap — docs/ROADMAP.md P-13).
package worktrack

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/internal/eventlog"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

// Field type registry. Codes are DB/wire contract (field_def.field_type
// SMALLINT) and follow the schema's taxonomy order (0005 lines 50-53) so a
// future type slots in by position without renumbering. `supported` marks the
// v1 subset that has a validator in fieldValidate; every other taxonomy type
// 400s at CreateFieldDef until its validator lands (recorded gap — P-13).
const (
	ftTextShort   int16 = 1
	ftTextLong    int16 = 2
	ftNumber      int16 = 3
	ftDate        int16 = 4
	ftDatetime    int16 = 5
	ftCheckbox    int16 = 6
	ftRadio       int16 = 7
	ftSelect      int16 = 8
	ftMultiSelect int16 = 9
	ftCascading   int16 = 10
	ftURL         int16 = 11
	ftUser        int16 = 12
	ftMultiUser   int16 = 13
	ftGroup       int16 = 14
	ftVersion     int16 = 15
	ftLabels      int16 = 16
	ftProject     int16 = 17
	ftReadonly    int16 = 18
	ftImportID    int16 = 19
)

type fieldTypeInfo struct {
	code      int16
	supported bool
}

// fieldTypes is the full schema taxonomy; the v1 slice enables a subset.
var fieldTypes = map[string]fieldTypeInfo{
	"text_short":   {ftTextShort, true},
	"text_long":    {ftTextLong, true},
	"number":       {ftNumber, true},
	"date":         {ftDate, true},
	"datetime":     {ftDatetime, false},
	"checkbox":     {ftCheckbox, true},
	"radio":        {ftRadio, false},
	"select":       {ftSelect, true},
	"multi_select": {ftMultiSelect, true},
	"cascading":    {ftCascading, false},
	"url":          {ftURL, false},
	"user":         {ftUser, false},
	"multi_user":   {ftMultiUser, false},
	"group":        {ftGroup, false},
	"version":      {ftVersion, false},
	"labels":       {ftLabels, false},
	"project":      {ftProject, false},
	"readonly":     {ftReadonly, false},
	"import_id":    {ftImportID, false},
}

// fieldTypeNames is the reverse of fieldTypes, for rendering defs on read.
var fieldTypeNames = func() map[int16]string {
	m := make(map[int16]string, len(fieldTypes))
	for name, info := range fieldTypes {
		m[info.code] = name
	}
	return m
}()

// fieldKeyRe mirrors the schema CHECK on field_def.key (0005 line 48); we
// pre-validate in Go so a bad key is a clean 400, not an aborted tx.
var fieldKeyRe = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)

// FieldOptions is the field_def.options JSONB payload. v1 uses only choices
// (for select/multi_select); other keys (defaults, cascading tree) are future.
type FieldOptions struct {
	Choices []string `json:"choices,omitempty"`
}

// FieldDef is the API/JSON shape of a custom-field definition.
type FieldDef struct {
	ID        int64        `json:"id"`
	Key       string       `json:"key"`
	Name      string       `json:"name"`
	FieldType string       `json:"field_type"`
	AppliesTo []int64      `json:"applies_to"`
	Required  bool         `json:"required"`
	Options   FieldOptions `json:"options"`
	Position  int          `json:"position"`
}

// fieldDef is the internal representation used for value validation.
type fieldDef struct {
	id      int64
	key     string
	ftype   int16
	options FieldOptions
}

type CreateFieldDefParams struct {
	Key       string
	Name      string
	FieldType string
	AppliesTo []int64
	Required  bool
	Options   FieldOptions
}

// CreateFieldDef defines a custom field on a Space. Key + field_type are set
// once and IMMUTABLE thereafter (a type change would strand stored values);
// name/required/options/position stay mutable via UpdateFieldDef.
func (s *Service) CreateFieldDef(ctx context.Context, actor auth.Identity, spaceID int64, p CreateFieldDefParams) (FieldDef, error) {
	key := strings.TrimSpace(p.Key)
	if !fieldKeyRe.MatchString(key) {
		return FieldDef{}, apperr.Invalid("key must match ^[a-z][a-z0-9_]{0,62}$")
	}
	name := strings.TrimSpace(p.Name)
	if name == "" {
		return FieldDef{}, apperr.Invalid("name required")
	}
	info, ok := fieldTypes[p.FieldType]
	if !ok {
		return FieldDef{}, apperr.Invalid("unknown field type " + p.FieldType)
	}
	if !info.supported {
		return FieldDef{}, apperr.Invalid("field type " + p.FieldType + " is not supported yet")
	}
	if info.code == ftSelect || info.code == ftMultiSelect {
		if len(p.Options.Choices) == 0 {
			return FieldDef{}, apperr.Invalid(p.FieldType + " requires options.choices (a non-empty string array)")
		}
		for _, c := range p.Options.Choices {
			if strings.TrimSpace(c) == "" {
				return FieldDef{}, apperr.Invalid("options.choices must be non-empty strings")
			}
		}
	}
	appliesTo := dedupeIDs(p.AppliesTo)
	out := FieldDef{Key: key, Name: name, FieldType: p.FieldType,
		AppliesTo: appliesTo, Required: p.Required, Options: p.Options}
	err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.perms.Require(ctx, tx, actor, perms.VerbEditItems,
			perms.OrgScope(actor.OrgID)); err != nil {
			return err
		}
		if _, err := loadSpace(ctx, tx, actor.OrgID, spaceID); err != nil {
			return err
		}
		// applies_to must name this space's OWN item types (else 400).
		if len(appliesTo) > 0 {
			var n int
			if err := tx.QueryRow(ctx,
				`SELECT count(*) FROM item_type WHERE space_id = $1 AND id = ANY($2)`,
				spaceID, appliesTo).Scan(&n); err != nil {
				return apperr.Internal("applies_to check", err)
			}
			if n != len(appliesTo) {
				return apperr.Invalid("applies_to must be item types of this space")
			}
		}
		// Pre-check UNIQUE(space_id,key) (a violation would abort the tx) —
		// the CreateChannel/CreateSpace precedent.
		var taken bool
		if err := tx.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM field_def WHERE space_id = $1 AND key = $2)`,
			spaceID, key).Scan(&taken); err != nil {
			return apperr.Internal("key check", err)
		}
		if taken {
			return apperr.Conflict("field key already in use in this space")
		}
		optsJSON, err := json.Marshal(p.Options)
		if err != nil {
			return apperr.Internal("options", err)
		}
		if err := tx.QueryRow(ctx, `
			INSERT INTO field_def (space_id, key, name, field_type, applies_to, required, options, position)
			VALUES ($1,$2,$3,$4,$5,$6,$7,
			        COALESCE((SELECT MAX(position)+1 FROM field_def WHERE space_id = $1), 0))
			RETURNING id, position`,
			spaceID, key, name, info.code, appliesTo, p.Required,
			json.RawMessage(optsJSON)).Scan(&out.ID, &out.Position); err != nil {
			return apperr.Internal("create field def", err)
		}
		if _, err := eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
			EntityType: enum.EntityFieldDef, EntityID: out.ID, Verb: "field_def.created",
			Payload: eventlog.MustPayload(map[string]any{
				"field_def_id": out.ID, "space_id": spaceID,
				"key": key, "field_type": p.FieldType}),
		}); err != nil {
			return apperr.Internal("append event", err)
		}
		return nil
	})
	if err != nil {
		return FieldDef{}, err
	}
	return out, nil
}

// ListFieldDefs returns a Space's custom-field definitions ordered by position
// then id. An org-scoped read (the ListItems/Statuses precedent — the write
// endpoints carry the VerbEditItems gate).
func (s *Service) ListFieldDefs(ctx context.Context, actor auth.Identity, spaceID int64) ([]FieldDef, error) {
	var exists bool
	if err := s.pool.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM space
		 WHERE id = $1 AND org_id = $2 AND archived_at IS NULL AND trashed_at IS NULL)`,
		spaceID, actor.OrgID).Scan(&exists); err != nil {
		return nil, apperr.Internal("space check", err)
	}
	if !exists {
		return nil, apperr.NotFound("space not found")
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, key, name, field_type, applies_to, required, options, position
		FROM field_def WHERE space_id = $1 ORDER BY position, id`, spaceID)
	if err != nil {
		return nil, apperr.Internal("list field defs", err)
	}
	defer rows.Close()
	var out []FieldDef
	for rows.Next() {
		var fd FieldDef
		var code int16
		var optsRaw []byte
		if err := rows.Scan(&fd.ID, &fd.Key, &fd.Name, &code, &fd.AppliesTo,
			&fd.Required, &optsRaw, &fd.Position); err != nil {
			return nil, apperr.Internal("scan field def", err)
		}
		fd.FieldType = fieldTypeNames[code]
		if fd.AppliesTo == nil {
			fd.AppliesTo = []int64{}
		}
		if len(optsRaw) > 0 {
			if err := json.Unmarshal(optsRaw, &fd.Options); err != nil {
				return nil, apperr.Internal("decode options", err)
			}
		}
		out = append(out, fd)
	}
	return out, rows.Err()
}

type UpdateFieldDefParams struct {
	Name     *string
	Required *bool
	Options  *FieldOptions
	Position *int
}

// UpdateFieldDef mutates a def's name/required/options/position. key and
// field_type are absent from the params by design — they are immutable.
func (s *Service) UpdateFieldDef(ctx context.Context, actor auth.Identity, defID int64, p UpdateFieldDefParams) error {
	if p.Name == nil && p.Required == nil && p.Options == nil && p.Position == nil {
		return apperr.Invalid("nothing to update")
	}
	if p.Name != nil && strings.TrimSpace(*p.Name) == "" {
		return apperr.Invalid("name cannot be empty")
	}
	return db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.perms.Require(ctx, tx, actor, perms.VerbEditItems,
			perms.OrgScope(actor.OrgID)); err != nil {
			return err
		}
		var spaceID int64
		var code int16
		if err := tx.QueryRow(ctx, `
			SELECT fd.space_id, fd.field_type FROM field_def fd
			JOIN space sp ON sp.id = fd.space_id
			WHERE fd.id = $1 AND sp.org_id = $2`, defID, actor.OrgID).
			Scan(&spaceID, &code); err != nil {
			return apperr.NotFound("field def not found")
		}
		if p.Options != nil && (code == ftSelect || code == ftMultiSelect) {
			if len(p.Options.Choices) == 0 {
				return apperr.Invalid("this field requires options.choices (a non-empty string array)")
			}
			for _, c := range p.Options.Choices {
				if strings.TrimSpace(c) == "" {
					return apperr.Invalid("options.choices must be non-empty strings")
				}
			}
		}
		if p.Name != nil {
			if _, err := tx.Exec(ctx,
				`UPDATE field_def SET name = $1 WHERE id = $2`,
				strings.TrimSpace(*p.Name), defID); err != nil {
				return apperr.Internal("rename field def", err)
			}
		}
		if p.Required != nil {
			if _, err := tx.Exec(ctx,
				`UPDATE field_def SET required = $1 WHERE id = $2`,
				*p.Required, defID); err != nil {
				return apperr.Internal("field def required", err)
			}
		}
		if p.Position != nil {
			if _, err := tx.Exec(ctx,
				`UPDATE field_def SET position = $1 WHERE id = $2`,
				*p.Position, defID); err != nil {
				return apperr.Internal("field def position", err)
			}
		}
		if p.Options != nil {
			optsJSON, err := json.Marshal(*p.Options)
			if err != nil {
				return apperr.Internal("options", err)
			}
			if _, err := tx.Exec(ctx,
				`UPDATE field_def SET options = $1 WHERE id = $2`,
				json.RawMessage(optsJSON), defID); err != nil {
				return apperr.Internal("field def options", err)
			}
		}
		if _, err := eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
			EntityType: enum.EntityFieldDef, EntityID: defID, Verb: "field_def.updated",
			Payload: eventlog.MustPayload(map[string]any{
				"field_def_id": defID, "space_id": spaceID}),
		}); err != nil {
			return apperr.Internal("append event", err)
		}
		return nil
	})
}

// DeleteFieldDef removes the definition only. Existing values in
// work_item.fields are left as inert orphans — no mass UPDATE, so delete stays
// O(1) at scale (an orphan strip-sweep is a recorded gap — P-13).
func (s *Service) DeleteFieldDef(ctx context.Context, actor auth.Identity, defID int64) error {
	return db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.perms.Require(ctx, tx, actor, perms.VerbEditItems,
			perms.OrgScope(actor.OrgID)); err != nil {
			return err
		}
		var spaceID int64
		if err := tx.QueryRow(ctx, `
			SELECT fd.space_id FROM field_def fd JOIN space sp ON sp.id = fd.space_id
			WHERE fd.id = $1 AND sp.org_id = $2`, defID, actor.OrgID).
			Scan(&spaceID); err != nil {
			return apperr.NotFound("field def not found")
		}
		if _, err := tx.Exec(ctx, `DELETE FROM field_def WHERE id = $1`, defID); err != nil {
			return apperr.Internal("delete field def", err)
		}
		if _, err := eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
			EntityType: enum.EntityFieldDef, EntityID: defID, Verb: "field_def.deleted",
			Payload: eventlog.MustPayload(map[string]any{
				"field_def_id": defID, "space_id": spaceID}),
		}); err != nil {
			return apperr.Internal("append event", err)
		}
		return nil
	})
}

// loadFieldDefs reads a Space's defs keyed by field key, for value validation
// on item writes. Loaded ONCE per write.
func loadFieldDefs(ctx context.Context, tx pgx.Tx, spaceID int64) (map[string]fieldDef, error) {
	rows, err := tx.Query(ctx,
		`SELECT id, key, field_type, options FROM field_def WHERE space_id = $1`, spaceID)
	if err != nil {
		return nil, apperr.Internal("load field defs", err)
	}
	defer rows.Close()
	out := map[string]fieldDef{}
	for rows.Next() {
		var d fieldDef
		var optsRaw []byte
		if err := rows.Scan(&d.id, &d.key, &d.ftype, &optsRaw); err != nil {
			return nil, apperr.Internal("scan field def", err)
		}
		if len(optsRaw) > 0 {
			if err := json.Unmarshal(optsRaw, &d.options); err != nil {
				return nil, apperr.Internal("decode options", err)
			}
		}
		out[d.key] = d
	}
	return out, rows.Err()
}

// fieldValidate checks a SUPPLIED value against its definition's type (and
// options for the choice types). v1 validates supplied values only; it does
// NOT force required-field presence (recorded gap — P-13). Returns a 400.
func fieldValidate(def fieldDef, value any) error {
	switch def.ftype {
	case ftTextShort, ftTextLong:
		if _, ok := value.(string); !ok {
			return apperr.Invalid(fmt.Sprintf("field %q expects text", def.key))
		}
	case ftNumber:
		// RED/GREEN pin (P-13): dropping this checked assertion lets a string
		// land in a number field — TestFieldValidate's "string into number"
		// case then fails (the bad value is accepted and stored unvalidated).
		if _, ok := value.(float64); !ok {
			return apperr.Invalid(fmt.Sprintf("field %q expects a number", def.key))
		}
	case ftDate:
		s, ok := value.(string)
		if !ok {
			return apperr.Invalid(fmt.Sprintf("field %q expects a date string", def.key))
		}
		if _, err := time.Parse("2006-01-02", s); err != nil {
			return apperr.Invalid(fmt.Sprintf("field %q expects an ISO date (YYYY-MM-DD)", def.key))
		}
	case ftCheckbox:
		if _, ok := value.(bool); !ok {
			return apperr.Invalid(fmt.Sprintf("field %q expects true or false", def.key))
		}
	case ftSelect:
		s, ok := value.(string)
		if !ok || !containsChoice(def.options.Choices, s) {
			return apperr.Invalid(fmt.Sprintf("field %q value is not an allowed choice", def.key))
		}
	case ftMultiSelect:
		arr, ok := value.([]any)
		if !ok {
			return apperr.Invalid(fmt.Sprintf("field %q expects an array of choices", def.key))
		}
		for _, el := range arr {
			s, ok := el.(string)
			if !ok || !containsChoice(def.options.Choices, s) {
				return apperr.Invalid(fmt.Sprintf("field %q has a value that is not an allowed choice", def.key))
			}
		}
	default:
		// Unsupported types cannot be created (CreateFieldDef rejects them),
		// so a stored def of an unknown type is a defensive 400.
		return apperr.Invalid(fmt.Sprintf("field %q has an unsupported type", def.key))
	}
	return nil
}

func containsChoice(choices []string, v string) bool {
	for _, c := range choices {
		if c == v {
			return true
		}
	}
	return false
}

// buildItemFields validates the fields supplied at item CREATE against the
// space's defs and returns the JSONB to store (never nil — "{}" when empty)
// plus the stored map for the response. A null value at create means "unset"
// (skipped); an unknown key is a 400.
func buildItemFields(ctx context.Context, tx pgx.Tx, spaceID int64, supplied map[string]any) (json.RawMessage, map[string]any, error) {
	if len(supplied) == 0 {
		return json.RawMessage("{}"), nil, nil
	}
	defs, err := loadFieldDefs(ctx, tx, spaceID)
	if err != nil {
		return nil, nil, err
	}
	stored := map[string]any{}
	for k, v := range supplied {
		def, ok := defs[k]
		if !ok {
			return nil, nil, apperr.Invalid("unknown field key " + k)
		}
		if v == nil {
			continue
		}
		if err := fieldValidate(def, v); err != nil {
			return nil, nil, err
		}
		stored[k] = v
	}
	b, err := json.Marshal(stored)
	if err != nil {
		return nil, nil, apperr.Internal("marshal fields", err)
	}
	if len(stored) == 0 {
		stored = nil
	}
	return b, stored, nil
}

// mergeItemFields merges the fields supplied at item UPDATE into the item's
// stored values (absent key = leave, explicit null = clear, else validate +
// set), writes them back, and returns the changed keys sorted (for the
// workitem.updated payload — keys only, never values).
func mergeItemFields(ctx context.Context, tx pgx.Tx, spaceID, itemID int64, supplied map[string]any) ([]string, error) {
	if len(supplied) == 0 {
		return nil, nil
	}
	defs, err := loadFieldDefs(ctx, tx, spaceID)
	if err != nil {
		return nil, err
	}
	var raw []byte
	if err := tx.QueryRow(ctx,
		`SELECT fields FROM work_item WHERE id = $1`, itemID).Scan(&raw); err != nil {
		return nil, apperr.Internal("load fields", err)
	}
	cur := map[string]any{}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &cur); err != nil {
			return nil, apperr.Internal("decode fields", err)
		}
	}
	var changed []string
	for k, v := range supplied {
		def, ok := defs[k]
		if !ok {
			return nil, apperr.Invalid("unknown field key " + k)
		}
		if v == nil {
			delete(cur, k)
		} else {
			if err := fieldValidate(def, v); err != nil {
				return nil, err
			}
			cur[k] = v
		}
		changed = append(changed, k)
	}
	sort.Strings(changed)
	b, err := json.Marshal(cur)
	if err != nil {
		return nil, apperr.Internal("marshal fields", err)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE work_item SET fields = $1, updated_at = now() WHERE id = $2`,
		json.RawMessage(b), itemID); err != nil {
		return nil, apperr.Internal("update fields", err)
	}
	return changed, nil
}

// dedupeIDs returns the distinct ids in order, never nil (a nil []int64 would
// send SQL NULL and override the applies_to column DEFAULT '{}').
func dedupeIDs(ids []int64) []int64 {
	seen := make(map[int64]bool, len(ids))
	out := make([]int64, 0, len(ids))
	for _, id := range ids {
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	return out
}
