package notification

import (
	"context"
	"encoding/base64"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
	"github.com/abhinavjha0239/weft/internal/platform/egress"
	"github.com/abhinavjha0239/weft/internal/platform/webpush"
)

// Web Push subscription management (P-21). A subscription is a browser-supplied
// endpoint URL plus its ECDH public key and auth secret. Self-scoped: a caller
// only ever sees or deletes their OWN rows, and DELETE is oracle-free (the
// sessions precedent). Sends ride the push lane (pushworker.go).

// maxSubscriptions caps live registrations per user (a phone, laptop, tablet…
// but not an unbounded set).
const maxSubscriptions = 10

// SetPush wires the configured VAPID identity, set at composition time like
// SetFanout. nil (the default) means push is unconfigured.
func (s *Service) SetPush(sender *webpush.Sender) { s.push = sender }

// VAPIDKey returns the public key clients need to subscribe, and whether push
// is configured at all (GET /api/v1/push/vapid-key 404s when it is not).
func (s *Service) VAPIDKey() (string, bool) {
	if s.push == nil {
		return "", false
	}
	return s.push.PublicKey(), true
}

// PushSubscriptionView is the caller-facing shape. The endpoint is TRUNCATED to
// its origin: it is a capability URL (whoever holds it can push to the device),
// so the full path is never echoed back.
type PushSubscriptionView struct {
	ID        int64      `json:"id"`
	Endpoint  string     `json:"endpoint"`
	CreatedAt time.Time  `json:"created_at"`
	LastOKAt  *time.Time `json:"last_ok_at,omitempty"`
}

// Subscribe registers (or refreshes, on the (user, endpoint) unique key) one
// push subscription for the actor. 409 when push is unconfigured or the user is
// already at the cap; 400 for a bad endpoint shape or wrong key lengths. The
// endpoint must pass the egress shape gate AND be https; the private-address
// check runs at SEND (registration-time DNS vetting would be a TOCTOU lie — a
// row pointing at an internal address dies on first delivery via ErrDisallowed).
func (s *Service) Subscribe(ctx context.Context, actor auth.Identity, endpoint, p256dhB64, authB64 string) (int64, error) {
	if s.push == nil {
		return 0, apperr.Conflict("push not configured")
	}
	if endpoint == "" {
		return 0, apperr.Invalid("endpoint is required")
	}
	if err := egress.VetURLShape(endpoint); err != nil {
		return 0, apperr.Invalid("endpoint must be a valid https URL on a standard port")
	}
	if u, err := url.Parse(endpoint); err != nil || u.Scheme != "https" {
		return 0, apperr.Invalid("endpoint must be https")
	}
	p256dh, err := decodeKey(p256dhB64)
	if err != nil || len(p256dh) != webpush.KeyLen {
		return 0, apperr.Invalid("p256dh must be a 65-byte base64url key")
	}
	authKey, err := decodeKey(authB64)
	if err != nil || len(authKey) != webpush.AuthLen {
		return 0, apperr.Invalid("auth must be a 16-byte base64url secret")
	}
	var id int64
	err = db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		var count int
		var hasThis bool
		if err := tx.QueryRow(ctx, `
			SELECT count(*), COALESCE(bool_or(endpoint = $2), false)
			FROM push_subscription WHERE user_id = $1`,
			actor.UserID, endpoint).Scan(&count, &hasThis); err != nil {
			return apperr.Internal("count subscriptions", err)
		}
		if count >= maxSubscriptions && !hasThis {
			return apperr.Conflict("too many push subscriptions (max 10)")
		}
		if err := tx.QueryRow(ctx, `
			INSERT INTO push_subscription (org_id, user_id, endpoint, p256dh, auth)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (user_id, endpoint)
			DO UPDATE SET p256dh = EXCLUDED.p256dh, auth = EXCLUDED.auth, org_id = EXCLUDED.org_id
			RETURNING id`,
			actor.OrgID, actor.UserID, endpoint, p256dh, authKey).Scan(&id); err != nil {
			return apperr.Internal("upsert subscription", err)
		}
		return nil
	})
	return id, err
}

// ListSubscriptions returns the caller's own subscriptions, newest first, with
// truncated endpoints.
func (s *Service) ListSubscriptions(ctx context.Context, actor auth.Identity) ([]PushSubscriptionView, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, endpoint, created_at, last_ok_at
		FROM push_subscription WHERE user_id = $1 ORDER BY id DESC`, actor.UserID)
	if err != nil {
		return nil, apperr.Internal("list subscriptions", err)
	}
	defer rows.Close()
	out := []PushSubscriptionView{}
	for rows.Next() {
		var v PushSubscriptionView
		var endpoint string
		if err := rows.Scan(&v.ID, &endpoint, &v.CreatedAt, &v.LastOKAt); err != nil {
			return nil, apperr.Internal("scan subscription", err)
		}
		v.Endpoint = truncateEndpoint(endpoint)
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		return nil, apperr.Internal("list subscriptions", err)
	}
	return out, nil
}

// DeleteSubscription removes one of the caller's OWN subscriptions. Strictly
// self-scoped and oracle-free: a foreign or absent id is one indistinguishable
// 404 (the sessions precedent).
func (s *Service) DeleteSubscription(ctx context.Context, actor auth.Identity, id int64) error {
	tag, err := s.pool.Exec(ctx,
		`DELETE FROM push_subscription WHERE id = $1 AND user_id = $2`, id, actor.UserID)
	if err != nil {
		return apperr.Internal("delete subscription", err)
	}
	if tag.RowsAffected() == 0 {
		return apperr.NotFound("subscription not found")
	}
	return nil
}

// decodeKey decodes base64url with or without padding (browsers vary).
func decodeKey(s string) ([]byte, error) {
	return base64.RawURLEncoding.DecodeString(strings.TrimRight(s, "="))
}

// truncateEndpoint reduces a capability URL to its origin so the list never
// echoes the secret path segment.
func truncateEndpoint(endpoint string) string {
	if u, err := url.Parse(endpoint); err == nil && u.Host != "" {
		return u.Scheme + "://" + u.Host + "/…"
	}
	if len(endpoint) > 24 {
		return endpoint[:24] + "…"
	}
	return endpoint
}
