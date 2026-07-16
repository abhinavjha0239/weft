package automation

// Inbound webhooks let an external service fire a rule by POSTing to a
// capability URL — /api/v1/hooks/rules/{id}/{token} — which is UNAUTHENTICATED
// (no Weft identity). The token IS the capability, so the whole surface is
// oracle-free: every authentication failure looks identical, and the token is
// compared in constant time and never written to any event payload or log.

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"

	"github.com/jackc/pgx/v5"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/internal/eventlog"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

// newWebhookToken mints a 32-byte crypto-random hex capability token.
func newWebhookToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", apperr.Internal("mint webhook token", err)
	}
	return hex.EncodeToString(b), nil
}

// triggerKind reports a stored definition's normalized trigger kind (absent =
// event), or "" if it cannot be parsed.
func triggerKind(rawDef json.RawMessage) string {
	var d Definition
	if json.Unmarshal(rawDef, &d) != nil {
		return ""
	}
	if d.Trigger.Kind == "" {
		return kindEvent
	}
	return d.Trigger.Kind
}

// AuthenticateWebhook verifies an inbound capability URL and returns the rule's
// org. EVERY failure — absent id, disabled, deleted, not a webhook rule, or a
// token mismatch — is one indistinguishable NotFound (404 at the edge), so the
// unauthenticated endpoint reveals nothing. The token compare is constant-time
// over sha256s, so a partially correct token leaks no timing signal. This is
// the load-bearing security check; the wrong-token 404 is its red/green pin.
func (s *Service) AuthenticateWebhook(ctx context.Context, id int64, token string) (int64, error) {
	var orgID int64
	var enabled bool
	var rawDef json.RawMessage
	var storedToken *string
	err := s.pool.QueryRow(ctx, `
		SELECT org_id, enabled, definition, webhook_token
		FROM automation
		WHERE id = $1 AND deleted_at IS NULL`, id).
		Scan(&orgID, &enabled, &rawDef, &storedToken)
	if err != nil {
		return 0, apperr.NotFound("not found")
	}
	if !enabled || storedToken == nil || triggerKind(rawDef) != kindWebhook {
		return 0, apperr.NotFound("not found")
	}
	got := sha256.Sum256([]byte(token))
	want := sha256.Sum256([]byte(*storedToken))
	if subtle.ConstantTimeCompare(got[:], want[:]) != 1 {
		return 0, apperr.NotFound("not found")
	}
	return orgID, nil
}

// RecordWebhook appends an automation.webhook_received event carrying the raw
// JSON body, which the runner turns into a run keyed by (automation, event) —
// so a sender's retries create distinct runs (at-least-once, a dedupe key is a
// recorded gap). The token never enters the payload. Called only after
// AuthenticateWebhook passed and the handler validated the body.
func (s *Service) RecordWebhook(ctx context.Context, orgID, id int64, body json.RawMessage) error {
	return db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		_, err := eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: orgID, ActorKind: enum.ActorSystem,
			EntityType: enum.EntityAutomation, EntityID: id, Verb: verbWebhookReceived,
			Payload: eventlog.MustPayload(map[string]any{
				"automation_id": id, "body": body}),
		})
		return err
	})
}

// RotateWebhookToken mints a fresh capability token for a webhook rule
// (scope-admin gated) and returns it to the caller — the capability-URL model.
// The rotation is event-logged, but the token itself never appears in the
// event payload or any log line.
func (s *Service) RotateWebhookToken(ctx context.Context, actor auth.Identity, id int64) (string, error) {
	var token string
	err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		var scopeType int16
		var scopeID int64
		var rawDef json.RawMessage
		err := tx.QueryRow(ctx, `
			SELECT scope_type, scope_id, definition FROM automation
			WHERE id = $1 AND org_id = $2 AND deleted_at IS NULL FOR UPDATE`,
			id, actor.OrgID).Scan(&scopeType, &scopeID, &rawDef)
		if err != nil {
			return apperr.NotFound("automation not found")
		}
		if err := s.requireScopeAdmin(ctx, tx, actor, scopeType, scopeID); err != nil {
			return err
		}
		if triggerKind(rawDef) != kindWebhook {
			return apperr.Invalid("automation is not a webhook trigger")
		}
		tok, err := newWebhookToken()
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`UPDATE automation SET webhook_token = $2, updated_at = now() WHERE id = $1`,
			id, tok); err != nil {
			return apperr.Internal("rotate webhook token", err)
		}
		token = tok
		_, err = eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
			EntityType: enum.EntityAutomation, EntityID: id, Verb: "automation.webhook_token_rotated",
			Payload: eventlog.MustPayload(map[string]any{"automation_id": id}),
		})
		return err
	})
	if err != nil {
		return "", err
	}
	return token, nil
}
