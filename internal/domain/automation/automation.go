// Package automation wakes ADR-014: rules are event-log subscriptions with
// filters (AU-1), owned by the SCOPE rather than a user (AU-2 — the
// creator-orphaning footgun designed out), stored in one canonical
// definition format (AU-3), and executed with default-safe guardrails
// (AU-4: per-event idempotent runs, loop guard opt-in, self-trigger hard
// block).
//
// The F-13 consent rule is enforced here: an automation may act as a HUMAN
// only with that human's consent, and ANY definition edit bumps the version,
// clears the consent, and disables the rule — nobody's identity runs a
// definition they haven't seen.
package automation

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/internal/eventlog"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

type Service struct {
	pool  *pgxpool.Pool
	perms *perms.Service
}

func New(pool *pgxpool.Pool, p *perms.Service) *Service {
	return &Service{pool: pool, perms: p}
}

// Automation scope types (automation.scope_type). Like retention scopes,
// only the rungs with real gates exist: org (manage_org) and channel
// (administer_channel). Workspace/space rungs arrive with their admin verbs.
const (
	ScopeOrg     int16 = 1
	ScopeChannel int16 = 3
)

// Definition is the AU-3 canonical automations-as-code document. v1 is the
// minimal honest vocabulary: one event-pattern trigger, post_message steps.
// Conditions, templating, schedules, webhooks, LLM steps and approval gates
// extend this format — they never replace it. Conditions is optional: a
// definition without the key (every rule stored before P-22) stays valid
// forever, DisallowUnknownFields notwithstanding.
type Definition struct {
	Trigger    Trigger     `json:"trigger"`
	Conditions []Condition `json:"conditions,omitempty"`
	Steps      []Step      `json:"steps"`
}

type Trigger struct {
	Verb string `json:"verb"`
}

type Step struct {
	Kind string `json:"kind"`
	// post_message: target channel (required for org scope; a channel-scope
	// rule may only post into its own channel) and static content.
	ChannelID int64  `json:"channel_id,omitempty"`
	Content   string `json:"content,omitempty"`
}

// triggerVerbs is the v1 catalog — only verbs the log actually emits.
// The event log makes chat and work-tracking one trigger vocabulary (AU-1).
var triggerVerbs = map[string]bool{
	"message.created":         true,
	"reaction.added":          true,
	"workitem.created":        true,
	"workitem.status_changed": true,
}

const (
	maxSteps      = 5
	maxContentLen = 4000
)

type Automation struct {
	ID               int64           `json:"automation_id"`
	ScopeType        int16           `json:"scope_type"`
	ScopeID          int64           `json:"scope_id"`
	Name             string          `json:"name"`
	Enabled          bool            `json:"enabled"`
	Definition       json.RawMessage `json:"definition"`
	Version          int32           `json:"version"`
	ActorUserID      *int64          `json:"actor_user_id,omitempty"`
	ActorConsented   bool            `json:"actor_consented"`
	AllowRuleTrigger bool            `json:"allow_rule_trigger"`
	CreatedBy        int64           `json:"created_by"`
	CreatedAt        time.Time       `json:"created_at"`
}

type CreateParams struct {
	ScopeType   int16
	ScopeID     int64
	Name        string
	Definition  json.RawMessage
	ActorUserID *int64
}

// requireScopeAdmin gates by the automation's OWNING scope: org rules need
// manage_org, channel rules administer_channel (resolved up the chain).
func (s *Service) requireScopeAdmin(ctx context.Context, tx pgx.Tx, actor auth.Identity, scopeType int16, scopeID int64) error {
	switch scopeType {
	case ScopeOrg:
		if scopeID != actor.OrgID {
			return apperr.Invalid("org-scope automation must target your org")
		}
		return s.perms.Require(ctx, tx, actor, perms.VerbManageOrg,
			perms.OrgScope(actor.OrgID))
	case ScopeChannel:
		chain, err := s.perms.ChannelScope(ctx, tx, actor.OrgID, scopeID)
		if err != nil {
			return err
		}
		return s.perms.Require(ctx, tx, actor, perms.VerbAdministerChannel, chain)
	default:
		return apperr.Invalid("scope_type must be 1 (org) or 3 (channel) — workspace/space scopes arrive with their admin verbs")
	}
}

// validateDefinition parses and normalizes the document, checking every
// step's target against the owning scope inside the caller's transaction.
func (s *Service) validateDefinition(ctx context.Context, tx pgx.Tx, orgID int64, scopeType int16, scopeID int64, raw json.RawMessage) (Definition, error) {
	var def Definition
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&def); err != nil {
		return def, apperr.Invalid("definition: " + err.Error())
	}
	if !triggerVerbs[def.Trigger.Verb] {
		return def, apperr.Invalid(fmt.Sprintf("definition: unknown trigger verb %q", def.Trigger.Verb))
	}
	if err := validateConditions(def.Conditions); err != nil {
		return def, err
	}
	if len(def.Steps) == 0 || len(def.Steps) > maxSteps {
		return def, apperr.Invalid(fmt.Sprintf("definition: 1..%d steps required", maxSteps))
	}
	for i, st := range def.Steps {
		if st.Kind != "post_message" {
			return def, apperr.Invalid(fmt.Sprintf("definition: step %d: unknown kind %q", i, st.Kind))
		}
		if st.Content == "" || len(st.Content) > maxContentLen {
			return def, apperr.Invalid(fmt.Sprintf("definition: step %d: content 1..%d chars", i, maxContentLen))
		}
		if err := validateStepContent(i, st.Content); err != nil {
			return def, err
		}
		switch scopeType {
		case ScopeChannel:
			// A channel-scope rule may not reach outside its channel.
			if st.ChannelID != 0 && st.ChannelID != scopeID {
				return def, apperr.Invalid(fmt.Sprintf("definition: step %d: a channel-scope rule posts only to its own channel", i))
			}
		case ScopeOrg:
			if st.ChannelID == 0 {
				return def, apperr.Invalid(fmt.Sprintf("definition: step %d: channel_id required for org scope", i))
			}
			var ok bool
			if err := tx.QueryRow(ctx, `
				SELECT EXISTS (SELECT 1 FROM channel
				  WHERE id = $1 AND org_id = $2 AND archived_at IS NULL)`,
				st.ChannelID, orgID).Scan(&ok); err != nil {
				return def, apperr.Internal("channel lookup", err)
			}
			if !ok {
				return def, apperr.NotFound(fmt.Sprintf("definition: step %d: channel not found", i))
			}
		}
	}
	return def, nil
}

// Create stores a rule DISABLED — create, review, then enable. A human
// actor_user_id needs that human's consent: naming yourself consents
// immediately; naming someone else leaves the rule unconsentable to enable
// until they POST /consent.
func (s *Service) Create(ctx context.Context, actor auth.Identity, p CreateParams) (Automation, error) {
	if p.Name == "" || len(p.Name) > 120 {
		return Automation{}, apperr.Invalid("name 1..120 chars required")
	}
	var out Automation
	err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.requireScopeAdmin(ctx, tx, actor, p.ScopeType, p.ScopeID); err != nil {
			return err
		}
		if _, err := s.validateDefinition(ctx, tx, actor.OrgID, p.ScopeType, p.ScopeID, p.Definition); err != nil {
			return err
		}
		var consentAt *time.Time
		if p.ActorUserID != nil {
			var ok bool
			if err := tx.QueryRow(ctx, `
				SELECT EXISTS (SELECT 1 FROM user_account
				  WHERE id = $1 AND org_id = $2 AND kind = 1 AND deactivated_at IS NULL)`,
				*p.ActorUserID, actor.OrgID).Scan(&ok); err != nil {
				return apperr.Internal("actor lookup", err)
			}
			if !ok {
				return apperr.NotFound("actor user not found")
			}
			if *p.ActorUserID == actor.UserID {
				now := time.Now()
				consentAt = &now
			}
		}
		if err := tx.QueryRow(ctx, `
			INSERT INTO automation (org_id, scope_type, scope_id, name, definition,
				actor_user_id, actor_consent_at, created_by)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			RETURNING id, version, created_at`,
			actor.OrgID, p.ScopeType, p.ScopeID, p.Name, p.Definition,
			p.ActorUserID, consentAt, actor.UserID).Scan(&out.ID, &out.Version, &out.CreatedAt); err != nil {
			return apperr.Internal("create automation", err)
		}
		out.ScopeType, out.ScopeID, out.Name = p.ScopeType, p.ScopeID, p.Name
		out.Definition, out.ActorUserID = p.Definition, p.ActorUserID
		out.ActorConsented = consentAt != nil
		out.CreatedBy = actor.UserID
		_, err := eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
			EntityType: enum.EntityAutomation, EntityID: out.ID, Verb: "automation.created",
			Payload: eventlog.MustPayload(map[string]any{
				"automation_id": out.ID, "name": p.Name,
				"scope_type": p.ScopeType, "scope_id": p.ScopeID}),
		})
		return err
	})
	if err != nil {
		return Automation{}, err
	}
	return out, nil
}

type UpdateParams struct {
	Name             *string
	Definition       json.RawMessage
	Enabled          *bool
	AllowRuleTrigger *bool
}

// Update patches a rule. A definition change bumps the version and — when
// the rule acts as a human — clears the consent AND disables it (F-13:
// nobody's identity runs a definition they haven't re-approved). Enabling
// requires consent to be present for human-actor rules.
func (s *Service) Update(ctx context.Context, actor auth.Identity, id int64, p UpdateParams) error {
	return db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		var scopeType int16
		var scopeID int64
		var actorUserID *int64
		var consentAt *time.Time
		var enabled bool
		err := tx.QueryRow(ctx, `
			SELECT scope_type, scope_id, actor_user_id, actor_consent_at, enabled
			FROM automation
			WHERE id = $1 AND org_id = $2 AND deleted_at IS NULL FOR UPDATE`,
			id, actor.OrgID).Scan(&scopeType, &scopeID, &actorUserID, &consentAt, &enabled)
		if err != nil {
			return apperr.NotFound("automation not found")
		}
		if err := s.requireScopeAdmin(ctx, tx, actor, scopeType, scopeID); err != nil {
			return err
		}
		changed := map[string]any{"automation_id": id}
		if p.Name != nil {
			if *p.Name == "" || len(*p.Name) > 120 {
				return apperr.Invalid("name 1..120 chars required")
			}
			if _, err := tx.Exec(ctx,
				`UPDATE automation SET name = $2, updated_at = now() WHERE id = $1`,
				id, *p.Name); err != nil {
				return apperr.Internal("rename automation", err)
			}
			changed["name"] = *p.Name
		}
		if p.Definition != nil {
			if _, err := s.validateDefinition(ctx, tx, actor.OrgID, scopeType, scopeID, p.Definition); err != nil {
				return err
			}
			clearConsent := actorUserID != nil
			if _, err := tx.Exec(ctx, `
				UPDATE automation SET definition = $2, version = version + 1,
				       actor_consent_at = CASE WHEN $3 THEN NULL ELSE actor_consent_at END,
				       enabled = CASE WHEN $3 THEN false ELSE enabled END,
				       updated_at = now()
				WHERE id = $1`, id, p.Definition, clearConsent); err != nil {
				return apperr.Internal("update definition", err)
			}
			if clearConsent {
				consentAt = nil
				enabled = false
			}
			changed["definition_version_bumped"] = true
			changed["consent_cleared"] = clearConsent
		}
		if p.AllowRuleTrigger != nil {
			if _, err := tx.Exec(ctx,
				`UPDATE automation SET allow_rule_trigger = $2, updated_at = now() WHERE id = $1`,
				id, *p.AllowRuleTrigger); err != nil {
				return apperr.Internal("update loop flag", err)
			}
			changed["allow_rule_trigger"] = *p.AllowRuleTrigger
		}
		if p.Enabled != nil {
			if *p.Enabled && actorUserID != nil && consentAt == nil {
				return apperr.Conflict("actor consent required before enabling")
			}
			if _, err := tx.Exec(ctx,
				`UPDATE automation SET enabled = $2, updated_at = now() WHERE id = $1`,
				id, *p.Enabled); err != nil {
				return apperr.Internal("toggle automation", err)
			}
			changed["enabled"] = *p.Enabled
		}
		_, err = eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
			EntityType: enum.EntityAutomation, EntityID: id, Verb: "automation.updated",
			Payload: eventlog.MustPayload(changed),
		})
		return err
	})
}

// Consent records the named actor's approval of the CURRENT definition.
// Only the named human may give it; idempotent.
func (s *Service) Consent(ctx context.Context, actor auth.Identity, id int64) error {
	return db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		var actorUserID *int64
		err := tx.QueryRow(ctx, `
			SELECT actor_user_id FROM automation
			WHERE id = $1 AND org_id = $2 AND deleted_at IS NULL FOR UPDATE`,
			id, actor.OrgID).Scan(&actorUserID)
		if err != nil {
			return apperr.NotFound("automation not found")
		}
		if actorUserID == nil {
			return apperr.Invalid("automation has no human actor")
		}
		if *actorUserID != actor.UserID {
			return apperr.Forbidden("only the named actor may consent")
		}
		if _, err := tx.Exec(ctx,
			`UPDATE automation SET actor_consent_at = now(), updated_at = now() WHERE id = $1`,
			id); err != nil {
			return apperr.Internal("consent", err)
		}
		_, err = eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
			EntityType: enum.EntityAutomation, EntityID: id, Verb: "automation.consented",
			Payload: eventlog.MustPayload(map[string]any{"automation_id": id}),
		})
		return err
	})
}

// Delete soft-deletes (runs and the audit trail keep their history).
func (s *Service) Delete(ctx context.Context, actor auth.Identity, id int64) error {
	return db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		var scopeType int16
		var scopeID int64
		err := tx.QueryRow(ctx, `
			SELECT scope_type, scope_id FROM automation
			WHERE id = $1 AND org_id = $2 AND deleted_at IS NULL FOR UPDATE`,
			id, actor.OrgID).Scan(&scopeType, &scopeID)
		if err != nil {
			return apperr.NotFound("automation not found")
		}
		if err := s.requireScopeAdmin(ctx, tx, actor, scopeType, scopeID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`UPDATE automation SET enabled = false, deleted_at = now() WHERE id = $1`, id); err != nil {
			return apperr.Internal("delete automation", err)
		}
		_, err = eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
			EntityType: enum.EntityAutomation, EntityID: id, Verb: "automation.deleted",
			Payload: eventlog.MustPayload(map[string]any{"automation_id": id}),
		})
		return err
	})
}

// List returns one scope's rules (the scope's admins see them).
func (s *Service) List(ctx context.Context, actor auth.Identity, scopeType int16, scopeID int64) ([]Automation, error) {
	out := []Automation{}
	err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.requireScopeAdmin(ctx, tx, actor, scopeType, scopeID); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `
			SELECT id, scope_type, scope_id, name, enabled, definition, version,
			       actor_user_id, actor_consent_at IS NOT NULL, allow_rule_trigger,
			       created_by, created_at
			FROM automation
			WHERE org_id = $1 AND scope_type = $2 AND scope_id = $3 AND deleted_at IS NULL
			ORDER BY id`, actor.OrgID, scopeType, scopeID)
		if err != nil {
			return apperr.Internal("list automations", err)
		}
		defer rows.Close()
		for rows.Next() {
			var a Automation
			if err := rows.Scan(&a.ID, &a.ScopeType, &a.ScopeID, &a.Name, &a.Enabled,
				&a.Definition, &a.Version, &a.ActorUserID, &a.ActorConsented,
				&a.AllowRuleTrigger, &a.CreatedBy, &a.CreatedAt); err != nil {
				return apperr.Internal("scan automation", err)
			}
			out = append(out, a)
		}
		return rows.Err()
	})
	return out, err
}

type Run struct {
	ID             int64           `json:"run_id"`
	AutomationID   int64           `json:"automation_id"`
	TriggerEventID *int64          `json:"trigger_event_id,omitempty"`
	Status         int16           `json:"status"`
	Steps          json.RawMessage `json:"steps"`
	StartedAt      time.Time       `json:"started_at"`
	FinishedAt     *time.Time      `json:"finished_at,omitempty"`
}

// ListRuns is the AU-2 debugging surface: full per-run step traces, newest
// first, gated like the rule itself.
func (s *Service) ListRuns(ctx context.Context, actor auth.Identity, automationID int64, limit int) ([]Run, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	out := []Run{}
	err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		var scopeType int16
		var scopeID int64
		err := tx.QueryRow(ctx, `
			SELECT scope_type, scope_id FROM automation
			WHERE id = $1 AND org_id = $2 AND deleted_at IS NULL`,
			automationID, actor.OrgID).Scan(&scopeType, &scopeID)
		if err != nil {
			return apperr.NotFound("automation not found")
		}
		if err := s.requireScopeAdmin(ctx, tx, actor, scopeType, scopeID); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `
			SELECT id, automation_id, trigger_event_id, status, steps, started_at, finished_at
			FROM automation_run
			WHERE automation_id = $1
			ORDER BY id DESC LIMIT $2`, automationID, limit)
		if err != nil {
			return apperr.Internal("list runs", err)
		}
		defer rows.Close()
		for rows.Next() {
			var r Run
			if err := rows.Scan(&r.ID, &r.AutomationID, &r.TriggerEventID,
				&r.Status, &r.Steps, &r.StartedAt, &r.FinishedAt); err != nil {
				return apperr.Internal("scan run", err)
			}
			out = append(out, r)
		}
		return rows.Err()
	})
	return out, err
}
