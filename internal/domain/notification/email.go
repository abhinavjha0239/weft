package notification

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/brand"
	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/platform/mail"
)

// EmailWorker is the offline lane (N-1 step 4): a notification still
// UNSEEN after the delay gets one email — opening the app cancels
// anything pending, which is the offline proxy until cross-node presence
// lands. Rows are marked BEFORE sending (at-most-once: a crash between
// mark and send loses an email, never duplicates one; the in-app row
// remains the system of record either way). Bodies carry who/where,
// never message content — the privacy-safe default.
type EmailWorker struct {
	pool   *pgxpool.Pool
	sender mail.Sender
	log    *slog.Logger
	// Delay is how long a notification stays unseen before it earns an
	// email (Zulip's ~2-minute batching window).
	Delay time.Duration
}

func NewEmailWorker(pool *pgxpool.Pool, sender mail.Sender, log *slog.Logger) *EmailWorker {
	return &EmailWorker{pool: pool, sender: sender, log: log, Delay: 2 * time.Minute}
}

// Run sweeps until ctx ends.
func (w *EmailWorker) Run(ctx context.Context) {
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if n, err := w.RunOnce(ctx, time.Now().Add(-w.Delay)); err != nil && ctx.Err() == nil {
				w.log.Warn("email: sweep failed", "err", err)
			} else if n > 0 {
				w.log.Info("email: sent", "count", n)
			}
		}
	}
}

const emailBatch = 500

type pendingEmail struct {
	userID  int64
	email   string
	kind    int16
	actor   string
	channel string
}

// RunOnce claims every notification created before olderThan that is
// unseen, unemailed, and email-enabled for its (user, kind) — stored
// pref first, the dm+mention default otherwise — marks it, then sends
// one digest per user. Returns emails sent.
func (w *EmailWorker) RunOnce(ctx context.Context, olderThan time.Time) (int, error) {
	sent := 0
	for {
		var pending []pendingEmail
		err := db.WithTx(ctx, w.pool, func(tx pgx.Tx) error {
			pending = pending[:0]
			rows, err := tx.Query(ctx, `
				SELECT n.id, n.user_id, u.email, n.kind,
				       COALESCE(a.full_name, ''), COALESCE(c.name, '')
				FROM notification n
				JOIN user_account u ON u.id = n.user_id
				LEFT JOIN user_account a ON a.id = n.actor_id
				LEFT JOIN message m ON n.entity_type = 1 AND m.id = n.entity_id
				LEFT JOIN channel c ON c.id = m.channel_id
				WHERE n.seen_at IS NULL AND n.emailed_at IS NULL
				  AND n.created_at < $1
				  AND u.email IS NOT NULL AND u.deactivated_at IS NULL
				  -- The zero-rows default must match defaultEmailEnabled
				  -- (dm/mention/keyword): setting an alert word IS the opt-in,
				  -- so a keyword row with no stored pref earns an email too.
				  AND COALESCE((SELECT p.enabled FROM notification_medium_pref p
				        WHERE p.user_id = n.user_id AND p.kind = n.kind AND p.medium = $2),
				      n.kind IN (1, 2, 4))
				  -- N-2 DND: skip rows for a recipient who is snoozed right now,
				  -- unless the row's actor is one of their VIPs. Left UNMARKED
				  -- (emailed_at stays NULL), so once the snooze lapses the next
				  -- sweep picks them up — a delay, never a drop.
				  AND NOT EXISTS (
				      SELECT 1 FROM dnd_setting d
				      WHERE d.user_id = n.user_id AND d.snoozed_until > now()
				        AND NOT EXISTS (
				            SELECT 1 FROM priority_contact pc
				            WHERE pc.user_id = n.user_id AND pc.contact_id = n.actor_id))
				ORDER BY n.user_id, n.id
				LIMIT $3
				FOR UPDATE OF n SKIP LOCKED`,
				olderThan, MediumEmail, emailBatch)
			if err != nil {
				return fmt.Errorf("email: scan: %w", err)
			}
			ids := []int64{}
			for rows.Next() {
				var id int64
				var p pendingEmail
				if err := rows.Scan(&id, &p.userID, &p.email, &p.kind, &p.actor, &p.channel); err != nil {
					rows.Close()
					return fmt.Errorf("email: scan row: %w", err)
				}
				ids = append(ids, id)
				pending = append(pending, p)
			}
			rows.Close()
			if err := rows.Err(); err != nil {
				return fmt.Errorf("email: scan: %w", err)
			}
			if len(ids) == 0 {
				return nil
			}
			if _, err := tx.Exec(ctx, `
				UPDATE notification SET emailed_at = now() WHERE id = ANY($1)`, ids); err != nil {
				return fmt.Errorf("email: mark: %w", err)
			}
			return nil
		})
		if err != nil {
			return sent, err
		}
		if len(pending) == 0 {
			return sent, nil
		}
		sent += w.deliver(pending)
		if len(pending) < emailBatch {
			return sent, nil
		}
	}
}

// deliver groups the claimed rows per user into one digest each. Send
// failures are logged and dropped — the rows are already marked
// (at-most-once), and the in-app feed still holds everything.
func (w *EmailWorker) deliver(pending []pendingEmail) int {
	byUser := map[int64][]pendingEmail{}
	order := []int64{}
	for _, p := range pending {
		if _, ok := byUser[p.userID]; !ok {
			order = append(order, p.userID)
		}
		byUser[p.userID] = append(byUser[p.userID], p)
	}
	sent := 0
	for _, uid := range order {
		batch := byUser[uid]
		var lines []string
		for _, p := range batch {
			lines = append(lines, line(p))
		}
		subject := fmt.Sprintf("[%s] %d unread notification%s",
			brand.Name, len(batch), plural(len(batch)))
		body := strings.Join(lines, "\n") +
			fmt.Sprintf("\n\nOpen %s to read and reply.\n", brand.Name)
		if err := w.sender.Send(mail.Message{
			To: batch[0].email, Subject: subject, Text: body,
		}); err != nil {
			w.log.Warn("email: send failed", "user", uid, "err", err)
			continue
		}
		sent++
	}
	return sent
}

// line renders one notification as who/where — never message content.
func line(p pendingEmail) string {
	who := p.actor
	if who == "" {
		who = "Someone"
	}
	where := ""
	if p.channel != "" {
		where = " in #" + p.channel
	}
	switch p.kind {
	case KindDM:
		return fmt.Sprintf("- %s sent you a direct message", who)
	case KindMention:
		return fmt.Sprintf("- %s mentioned you%s", who, where)
	case KindFollowedThread:
		return fmt.Sprintf("- %s posted in a thread you follow%s", who, where)
	case KindKeyword:
		return fmt.Sprintf("- %s used one of your alert words%s", who, where)
	default:
		return fmt.Sprintf("- New activity%s", where)
	}
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}
