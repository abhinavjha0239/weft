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
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/internal/eventlog"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

type Service struct {
	pool  *pgxpool.Pool
	perms *perms.Service
	// msg backs the slash-command invocation's channel-send gate; wired via
	// SetMessaging so access control lives in messaging, never duplicated here.
	msg *messaging.Service
}

func New(pool *pgxpool.Pool, p *perms.Service) *Service {
	return &Service{pool: pool, perms: p}
}

// SetMessaging wires the messaging service used by the slash-command gate.
func (s *Service) SetMessaging(m *messaging.Service) { s.msg = m }

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

// Trigger selects what fires a rule. Kind absent = "event" (every definition
// stored before P-23 stays valid; normalized at validate and on load). Each
// kind reads only its own fields — a field foreign to the kind must be absent.
type Trigger struct {
	Kind     string    `json:"kind,omitempty"`
	Verb     string    `json:"verb,omitempty"`     // event: an event-log verb from triggerVerbs
	Schedule *Schedule `json:"schedule,omitempty"` // schedule: the cadence (required)
	Command  string    `json:"command,omitempty"`  // slash: the command name (required)
}

// Trigger kinds (Definition.Trigger.Kind).
const (
	kindEvent    = "event"
	kindSchedule = "schedule"
	kindWebhook  = "webhook"
	kindSlash    = "slash"
)

// The internal verbs the schedule/webhook/slash lanes append. They are
// deliberately NOT in triggerVerbs: they are not event-pattern-subscribable,
// so matching them is TARGETED (schedule/webhook by payload.automation_id,
// slash by command + scope coverage) rather than by open verb subscription.
const (
	verbScheduleDue     = "automation.schedule_due"
	verbWebhookReceived = "automation.webhook_received"
	verbSlashInvoked    = "automation.slash_invoked"
)

// slashCommandRe bounds a slash command name — shared by the definition and
// the invocation path.
var slashCommandRe = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,31}$`)

type Step struct {
	Kind string `json:"kind"`
	// post_message: target channel (required for org scope; a channel-scope
	// rule may only post into its own channel) and static content.
	ChannelID int64  `json:"channel_id,omitempty"`
	Content   string `json:"content,omitempty"`
	// http_request (P-24): a STATIC destination (never templated) and ≤5
	// optional custom headers. The step ENQUEUES a webhook_delivery; the send
	// happens later in the delivery lane through the SSRF-guarded egress client.
	URL     string            `json:"url,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
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
	// WebhookToken is the capability token for a webhook rule, surfaced only to
	// scope admins (Create and List are requireScopeAdmin-gated). Nil for every
	// non-webhook rule.
	WebhookToken *string   `json:"webhook_token,omitempty"`
	CreatedBy    int64     `json:"created_by"`
	CreatedAt    time.Time `json:"created_at"`
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
	if err := validateTrigger(&def.Trigger); err != nil {
		return def, err
	}
	if err := validateConditions(def.Conditions); err != nil {
		return def, err
	}
	if len(def.Steps) == 0 || len(def.Steps) > maxSteps {
		return def, apperr.Invalid(fmt.Sprintf("definition: 1..%d steps required", maxSteps))
	}
	httpSteps := 0
	for i, st := range def.Steps {
		switch st.Kind {
		case stepPostMessage:
			if st.URL != "" || len(st.Headers) > 0 {
				return def, apperr.Invalid(fmt.Sprintf("definition: step %d: a post_message step takes no url or headers", i))
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
		case stepHTTPRequest:
			// ≤3 outbound calls per rule, and a foreign field (content/channel)
			// on an http step is a misconfiguration, never silently ignored.
			httpSteps++
			if httpSteps > maxHTTPSteps {
				return def, apperr.Invalid(fmt.Sprintf("definition: at most %d http_request steps", maxHTTPSteps))
			}
			if st.Content != "" || st.ChannelID != 0 {
				return def, apperr.Invalid(fmt.Sprintf("definition: step %d: an http_request step takes no content or channel_id", i))
			}
			if err := validateHTTPStep(i, st.URL, st.Headers); err != nil {
				return def, err
			}
		default:
			return def, apperr.Invalid(fmt.Sprintf("definition: step %d: unknown kind %q", i, st.Kind))
		}
	}
	return def, nil
}

// validateTrigger normalizes the kind (absent = event) and enforces the
// per-kind shape: an "event" trigger names a verb from triggerVerbs;
// "schedule" carries a valid schedule; "webhook" carries nothing more;
// "slash" names a valid command. A field foreign to the kind must be ABSENT —
// a stray verb on a webhook rule is a misconfiguration, never silently ignored.
func validateTrigger(t *Trigger) error {
	if t.Kind == "" {
		t.Kind = kindEvent
	}
	switch t.Kind {
	case kindEvent:
		if !triggerVerbs[t.Verb] {
			return apperr.Invalid(fmt.Sprintf("definition: unknown trigger verb %q", t.Verb))
		}
		if t.Schedule != nil || t.Command != "" {
			return apperr.Invalid("definition: an event trigger takes only a verb")
		}
	case kindSchedule:
		if t.Verb != "" || t.Command != "" {
			return apperr.Invalid("definition: a schedule trigger takes only a schedule")
		}
		if err := validateSchedule(t.Schedule); err != nil {
			return err
		}
	case kindWebhook:
		if t.Verb != "" || t.Schedule != nil || t.Command != "" {
			return apperr.Invalid("definition: a webhook trigger takes no verb, schedule, or command")
		}
	case kindSlash:
		if t.Verb != "" || t.Schedule != nil {
			return apperr.Invalid("definition: a slash trigger takes only a command")
		}
		if !slashCommandRe.MatchString(t.Command) {
			return apperr.Invalid("definition: slash command must match ^[a-z0-9][a-z0-9_-]{0,31}$")
		}
	default:
		return apperr.Invalid(fmt.Sprintf("definition: unknown trigger kind %q", t.Kind))
	}
	return nil
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
		def, err := s.validateDefinition(ctx, tx, actor.OrgID, p.ScopeType, p.ScopeID, p.Definition)
		if err != nil {
			return err
		}
		// A webhook trigger mints its capability token at birth (Create stores
		// the rule disabled, so no schedule fire time is ever set here).
		var webhookToken *string
		if def.Trigger.Kind == kindWebhook {
			tok, terr := newWebhookToken()
			if terr != nil {
				return terr
			}
			webhookToken = &tok
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
				actor_user_id, actor_consent_at, created_by, webhook_token)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			RETURNING id, version, created_at`,
			actor.OrgID, p.ScopeType, p.ScopeID, p.Name, p.Definition,
			p.ActorUserID, consentAt, actor.UserID, webhookToken).Scan(&out.ID, &out.Version, &out.CreatedAt); err != nil {
			return apperr.Internal("create automation", err)
		}
		out.ScopeType, out.ScopeID, out.Name = p.ScopeType, p.ScopeID, p.Name
		out.Definition, out.ActorUserID = p.Definition, p.ActorUserID
		out.ActorConsented = consentAt != nil
		out.WebhookToken = webhookToken
		out.CreatedBy = actor.UserID
		_, err = eventlog.Append(ctx, tx, eventlog.Event{
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
		var storedDef json.RawMessage
		var webhookToken *string
		err := tx.QueryRow(ctx, `
			SELECT scope_type, scope_id, actor_user_id, actor_consent_at, enabled,
			       definition, webhook_token
			FROM automation
			WHERE id = $1 AND org_id = $2 AND deleted_at IS NULL FOR UPDATE`,
			id, actor.OrgID).Scan(&scopeType, &scopeID, &actorUserID, &consentAt,
			&enabled, &storedDef, &webhookToken)
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
			newDef, err := s.validateDefinition(ctx, tx, actor.OrgID, scopeType, scopeID, p.Definition)
			if err != nil {
				return err
			}
			// webhook_token lifecycle: mint when the trigger becomes a webhook
			// (keeping any existing token so a URL already handed out survives
			// unrelated edits), NULL when it stops being one. Rotation is a
			// separate, explicit action.
			var nextToken *string
			if newDef.Trigger.Kind == kindWebhook {
				if webhookToken != nil {
					nextToken = webhookToken
				} else {
					tok, terr := newWebhookToken()
					if terr != nil {
						return terr
					}
					nextToken = &tok
				}
			}
			if _, err := tx.Exec(ctx,
				`UPDATE automation SET webhook_token = $2 WHERE id = $1`, id, nextToken); err != nil {
				return apperr.Internal("update webhook token", err)
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
			// Enabling resets the delivery-health counter (AU-4): a self-serve
			// re-enable after an auto-disable must get a fresh failure window,
			// not insta-disable on the stale streak.
			if _, err := tx.Exec(ctx, `
				UPDATE automation
				SET enabled = $2,
				    delivery_failures = CASE WHEN $2 THEN 0 ELSE delivery_failures END,
				    updated_at = now()
				WHERE id = $1`,
				id, *p.Enabled); err != nil {
				return apperr.Internal("toggle automation", err)
			}
			enabled = *p.Enabled
			changed["enabled"] = *p.Enabled
		}
		// Schedule lifecycle: recompute schedule_next_at whenever enablement or
		// the definition changed. An enabled schedule rule carries its next
		// fire; a disabled rule or a non-schedule trigger carries NULL. Renames
		// and the loop-flag toggle leave it alone, so a schedule never drifts
		// on an unrelated edit. This also composes the F-13 arc: a definition
		// edit that disables a human-actor rule lands here with enabled=false
		// and NULLs the fire time.
		if p.Enabled != nil || p.Definition != nil {
			effectiveDef := storedDef
			if p.Definition != nil {
				effectiveDef = p.Definition
			}
			if _, err := tx.Exec(ctx,
				`UPDATE automation SET schedule_next_at = $2 WHERE id = $1`,
				id, scheduleNextAt(enabled, effectiveDef)); err != nil {
				return apperr.Internal("update schedule fire", err)
			}
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
			       created_by, created_at, webhook_token
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
				&a.AllowRuleTrigger, &a.CreatedBy, &a.CreatedAt, &a.WebhookToken); err != nil {
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

// Delivery is one outbound-webhook attempt record (P-24), the AU-4 health
// dashboard's row. Payload is intentionally omitted from the read model — the
// dashboard shows delivery health, not the (potentially large) event snapshot.
type Delivery struct {
	ID             int64      `json:"delivery_id"`
	AutomationID   int64      `json:"automation_id"`
	RunID          int64      `json:"run_id"`
	URL            string     `json:"url"`
	Status         int16      `json:"status"`
	Attempts       int32      `json:"attempts"`
	NextAttemptAt  time.Time  `json:"next_attempt_at"`
	LastStatusCode *int32     `json:"last_status_code,omitempty"`
	LastError      string     `json:"last_error,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	DeliveredAt    *time.Time `json:"delivered_at,omitempty"`
}

// ListDeliveries is the AU-4 delivery-health dashboard (P-24): a rule's
// outbound webhook attempts, newest first, gated exactly like ListRuns. Rides
// webhook_delivery_rule_idx (automation_id, id DESC).
func (s *Service) ListDeliveries(ctx context.Context, actor auth.Identity, automationID int64, limit int) ([]Delivery, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	out := []Delivery{}
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
			SELECT id, automation_id, run_id, url, status, attempts, next_attempt_at,
			       last_status_code, last_error, created_at, delivered_at
			FROM webhook_delivery
			WHERE automation_id = $1
			ORDER BY id DESC LIMIT $2`, automationID, limit)
		if err != nil {
			return apperr.Internal("list deliveries", err)
		}
		defer rows.Close()
		for rows.Next() {
			var d Delivery
			if err := rows.Scan(&d.ID, &d.AutomationID, &d.RunID, &d.URL, &d.Status,
				&d.Attempts, &d.NextAttemptAt, &d.LastStatusCode, &d.LastError,
				&d.CreatedAt, &d.DeliveredAt); err != nil {
				return apperr.Internal("scan delivery", err)
			}
			out = append(out, d)
		}
		return rows.Err()
	})
	return out, err
}
