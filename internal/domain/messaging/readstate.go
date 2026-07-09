package messaging

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

// Read state is the F-7 hybrid: a per-(user, thread) watermark, never a dense
// per-(user, message) row. Unread = messages after the watermark, authored by
// someone else, that the user can still see. The channel-root thread's
// watermark covers the flat-channel case for free.
//
// Mark-read is DELIBERATELY not event-logged (scale contract): it is among the
// highest-volume actions in a chat product (one per channel view), so writing
// it to the durable spine would bloat the log for millions of users. The
// watermark table is the durable source of truth; a reconnecting client reads
// current state via the REST endpoints below. Live multi-device sync rides the
// EPHEMERAL path (ADR-002 P5, alongside typing/presence) when that lands — it
// must never enter the event log.

// MarkRead advances a thread's read watermark to upTo (monotone — a stale mark
// never rewinds). upTo <= 0 means "up to the latest message". The value is
// clamped to a real message id in the thread so a client cannot push it past
// reality.
func (s *Service) MarkRead(ctx context.Context, actor auth.Identity, threadID, upTo int64) (int64, error) {
	var applied int64
	err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		var channelID *int64
		if err := tx.QueryRow(ctx,
			`SELECT channel_id FROM thread WHERE id = $1 AND org_id = $2`,
			threadID, actor.OrgID).Scan(&channelID); err != nil {
			return apperr.NotFound("thread not found")
		}
		if channelID == nil {
			return apperr.Invalid("only channel threads are supported here")
		}
		if err := s.requireMember(ctx, tx, *channelID, actor.UserID); err != nil {
			return err
		}
		var newest int64
		if err := tx.QueryRow(ctx,
			`SELECT COALESCE(MAX(id), 0) FROM message WHERE thread_id = $1`,
			threadID).Scan(&newest); err != nil {
			return apperr.Internal("thread head", err)
		}
		if upTo <= 0 || upTo > newest {
			upTo = newest
		}
		// Monotone upsert (GREATEST): concurrent/out-of-order marks never rewind.
		if err := tx.QueryRow(ctx, `
			INSERT INTO thread_read_watermark (user_id, thread_id, last_read_message_id, updated_at)
			VALUES ($1, $2, $3, now())
			ON CONFLICT (user_id, thread_id)
			DO UPDATE SET last_read_message_id =
			       GREATEST(thread_read_watermark.last_read_message_id, EXCLUDED.last_read_message_id),
			       updated_at = now()
			RETURNING last_read_message_id`,
			actor.UserID, threadID, upTo).Scan(&applied); err != nil {
			return apperr.Internal("mark read", err)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return applied, nil
}

type ChannelUnread struct {
	ChannelID   int64 `json:"channel_id"`
	UnreadCount int   `json:"unread_count"`
	Mentioned   bool  `json:"mentioned"`
}

// Unreads returns per-channel unread counts for the actor across their
// subscribed channels. Unread messages are those after each thread's watermark
// (or all, if never read), excluding the user's own and deleted messages.
//
// Scale note (docs/SCHEMA.md): this is a per-request aggregate over the user's
// channels. It reads live here for correctness; the scale-tier design keeps a
// maintained per-(user,channel) counter updated on the notification path
// (F-17 deliverability sets) — same result, O(1) read. Documented, not built.
func (s *Service) Unreads(ctx context.Context, actor auth.Identity) ([]ChannelUnread, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT m.channel_id, count(*) AS unread
		FROM channel_member cm
		JOIN message m
		  ON m.channel_id = cm.channel_id
		 AND m.author_id <> cm.user_id
		 AND m.deleted_at IS NULL
		LEFT JOIN thread_read_watermark w
		  ON w.user_id = cm.user_id AND w.thread_id = m.thread_id
		WHERE cm.user_id = $1
		  AND cm.unsubscribed_at IS NULL
		  AND m.id > COALESCE(w.last_read_message_id, 0)
		GROUP BY m.channel_id`,
		actor.UserID)
	if err != nil {
		return nil, apperr.Internal("unreads", err)
	}
	defer rows.Close()
	var out []ChannelUnread
	for rows.Next() {
		var u ChannelUnread
		if err := rows.Scan(&u.ChannelID, &u.UnreadCount); err != nil {
			return nil, apperr.Internal("scan unread", err)
		}
		out = append(out, u)
	}
	return out, rows.Err()
}
