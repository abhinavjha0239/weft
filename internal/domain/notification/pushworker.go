package notification

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/platform/egress"
	"github.com/abhinavjha0239/weft/internal/platform/webpush"
)

// PushWorker is the Web Push lane (P-21), beside EmailWorker. Where email is a
// delayed digest, push is the IMMEDIACY medium: no age delay, one encrypted
// message per notification to EVERY live subscription of the recipient (a phone
// and a laptop both ring). Rows are marked BEFORE sending — AT-MOST-ONCE (the
// email trade, right for notifications): a crash between mark and send loses a
// push, never duplicates one, and the in-app row remains the system of record.
// A seen-races-send extra push is therefore possible and acceptable. Payloads
// carry who/where ONLY, never message content (the email-digest privacy rule).
type PushWorker struct {
	pool   *pgxpool.Pool
	sender *webpush.Sender
	eg     *egress.Client
	log    *slog.Logger
}

// NewPushWorker builds the lane. sender nil (push unconfigured) makes RunOnce a
// structural no-op. eg is the SSRF-guarded egress client — the ONLY path a
// user-registered endpoint may be dialed on (production wiring sets no test
// options).
func NewPushWorker(pool *pgxpool.Pool, sender *webpush.Sender, eg *egress.Client, log *slog.Logger) *PushWorker {
	return &PushWorker{pool: pool, sender: sender, eg: eg, log: log}
}

// Run sweeps every 30s until ctx ends.
func (w *PushWorker) Run(ctx context.Context) {
	if w.sender == nil {
		return
	}
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if n, err := w.RunOnce(ctx); err != nil && ctx.Err() == nil {
				w.log.Warn("push: sweep failed", "err", err)
			} else if n > 0 {
				w.log.Info("push: sent", "count", n)
			}
		}
	}
}

const pushBatch = 500

type pendingPush struct {
	orgID      int64
	userID     int64
	kind       int16
	entityType int16
	entityID   int64
	actor      string
	channel    string
}

// RunOnce claims every unseen, unpushed, push-enabled notification for a user
// who has at least one live subscription — stored medium-3 pref first, the
// dm+mention default otherwise — marks it pushed, then delivers. Returns the
// number of successful (2xx) deliveries. No age delay: push is immediate.
func (w *PushWorker) RunOnce(ctx context.Context) (int, error) {
	if w.sender == nil {
		return 0, nil
	}
	sent := 0
	for {
		var pending []pendingPush
		err := db.WithTx(ctx, w.pool, func(tx pgx.Tx) error {
			pending = pending[:0]
			rows, err := tx.Query(ctx, `
				SELECT n.id, n.org_id, n.user_id, n.kind, n.entity_type, n.entity_id,
				       COALESCE(a.full_name, ''), COALESCE(c.name, '')
				FROM notification n
				JOIN user_account u ON u.id = n.user_id
				LEFT JOIN user_account a ON a.id = n.actor_id
				LEFT JOIN message m ON n.entity_type = 1 AND m.id = n.entity_id
				LEFT JOIN channel c ON c.id = m.channel_id
				WHERE n.seen_at IS NULL AND n.pushed_at IS NULL
				  AND u.deactivated_at IS NULL
				  -- Only users with a live subscription: there is nothing to send
				  -- otherwise, and marking their rows pushed would be a lie (this
				  -- is push's analogue of the email JOIN's "u.email IS NOT NULL").
				  AND EXISTS (SELECT 1 FROM push_subscription ps WHERE ps.user_id = n.user_id)
				  -- Zero-rows default: push ON for dm (1) + mention (2) only.
				  -- Keyword's opt-in-by-setting-a-word logic does NOT carry to
				  -- push; a stored medium-3 pref row overrides either way.
				  AND COALESCE((SELECT p.enabled FROM notification_medium_pref p
				        WHERE p.user_id = n.user_id AND p.kind = n.kind AND p.medium = $1),
				      n.kind IN (1, 2))
				  -- N-2 DND: skip rows for a snoozed recipient unless the actor is
				  -- a VIP. Left UNMARKED (pushed_at stays NULL) so the next sweep
				  -- after the snooze lapses delivers them — identical to email.
				  AND NOT EXISTS (
				      SELECT 1 FROM dnd_setting d
				      WHERE d.user_id = n.user_id AND d.snoozed_until > now()
				        AND NOT EXISTS (
				            SELECT 1 FROM priority_contact pc
				            WHERE pc.user_id = n.user_id AND pc.contact_id = n.actor_id))
				ORDER BY n.user_id, n.id
				LIMIT $2
				FOR UPDATE OF n SKIP LOCKED`,
				MediumPush, pushBatch)
			if err != nil {
				return fmt.Errorf("push: scan: %w", err)
			}
			ids := []int64{}
			for rows.Next() {
				var id int64
				var p pendingPush
				if err := rows.Scan(&id, &p.orgID, &p.userID, &p.kind,
					&p.entityType, &p.entityID, &p.actor, &p.channel); err != nil {
					rows.Close()
					return fmt.Errorf("push: scan row: %w", err)
				}
				ids = append(ids, id)
				pending = append(pending, p)
			}
			rows.Close()
			if err := rows.Err(); err != nil {
				return fmt.Errorf("push: scan: %w", err)
			}
			if len(ids) == 0 {
				return nil
			}
			if _, err := tx.Exec(ctx, `
				UPDATE notification SET pushed_at = now() WHERE id = ANY($1)`, ids); err != nil {
				return fmt.Errorf("push: mark: %w", err)
			}
			return nil
		})
		if err != nil {
			return sent, err
		}
		if len(pending) == 0 {
			return sent, nil
		}
		sent += w.deliver(ctx, pending)
		if len(pending) < pushBatch {
			return sent, nil
		}
	}
}

// subRow is one loaded subscription's send material.
type subRow struct {
	id       int64
	endpoint string
	p256dh   []byte
	auth     []byte
}

// deliver fans each claimed notification out to every live subscription of its
// recipient. The rows are already marked (at-most-once); a send failure is a
// drop, not a dup. Returns successful deliveries.
func (w *PushWorker) deliver(ctx context.Context, pending []pendingPush) int {
	byUser := map[int64][]pendingPush{}
	order := []int64{}
	for _, p := range pending {
		if _, ok := byUser[p.userID]; !ok {
			order = append(order, p.userID)
		}
		byUser[p.userID] = append(byUser[p.userID], p)
	}
	sent := 0
	for _, uid := range order {
		subs, err := w.loadSubs(ctx, uid)
		if err != nil {
			w.log.Warn("push: load subscriptions", "user", uid, "err", err)
			continue
		}
		for _, p := range byUser[uid] {
			payload, err := json.Marshal(pushPayload{
				Kind: p.kind, OrgID: p.orgID, EntityType: p.entityType,
				EntityID: p.entityID, ActorName: p.actor, ChannelName: p.channel,
			})
			if err != nil {
				w.log.Warn("push: marshal payload", "user", uid, "err", err)
				continue
			}
			for _, sub := range subs {
				if w.send(ctx, sub, payload) {
					sent++
				}
			}
		}
	}
	return sent
}

// pushPayload is the encrypted body: who/where ONLY, NEVER message content.
type pushPayload struct {
	Kind        int16  `json:"kind"`
	OrgID       int64  `json:"org_id"`
	EntityType  int16  `json:"entity_type"`
	EntityID    int64  `json:"entity_id"`
	ActorName   string `json:"actor_name,omitempty"`
	ChannelName string `json:"channel_name,omitempty"`
}

// send encrypts and POSTs one payload to one subscription through the egress
// guard, applying the endpoint lifecycle: 2xx → last_ok_at bumped; 404/410 →
// the browser revoked it, delete the row; ErrDisallowed → a private/odd
// endpoint that slipped registration, delete the row and log; 429/5xx/timeout →
// leave the row (the notification is already marked). Returns true on 2xx.
func (w *PushWorker) send(ctx context.Context, sub subRow, payload []byte) bool {
	body, headers, err := w.sender.Push(
		webpush.Subscription{Endpoint: sub.endpoint, P256dh: sub.p256dh, Auth: sub.auth}, payload)
	if err != nil {
		// A malformed key (impossible past the API length check, but a hand-
		// seeded row could carry a non-point) can never encrypt — leave it.
		w.log.Warn("push: encrypt", "sub", sub.id, "err", err)
		return false
	}
	resp, err := w.eg.PostRaw(ctx, sub.endpoint, headers, body, webpush.ContentType)
	if err != nil {
		if errors.Is(err, egress.ErrDisallowed) {
			w.deleteSub(ctx, sub.id)
			w.log.Warn("push: endpoint disallowed, deleted", "sub", sub.id, "err", err)
		}
		return false
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
	switch {
	case resp.StatusCode == 200 || resp.StatusCode == 201:
		if _, err := w.pool.Exec(ctx,
			`UPDATE push_subscription SET last_ok_at = now() WHERE id = $1`, sub.id); err != nil {
			w.log.Warn("push: mark ok", "sub", sub.id, "err", err)
		}
		return true
	case resp.StatusCode == 404 || resp.StatusCode == 410:
		w.deleteSub(ctx, sub.id)
		return false
	default:
		return false
	}
}

func (w *PushWorker) loadSubs(ctx context.Context, userID int64) ([]subRow, error) {
	rows, err := w.pool.Query(ctx, `
		SELECT id, endpoint, p256dh, auth FROM push_subscription
		WHERE user_id = $1 ORDER BY id`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []subRow
	for rows.Next() {
		var s subRow
		if err := rows.Scan(&s.id, &s.endpoint, &s.p256dh, &s.auth); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

func (w *PushWorker) deleteSub(ctx context.Context, id int64) {
	if _, err := w.pool.Exec(ctx, `DELETE FROM push_subscription WHERE id = $1`, id); err != nil {
		w.log.Warn("push: delete subscription", "sub", id, "err", err)
	}
}
