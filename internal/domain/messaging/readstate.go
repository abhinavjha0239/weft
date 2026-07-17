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
		var channelID, dmSpaceID *int64
		if err := tx.QueryRow(ctx,
			`SELECT channel_id, dm_space_id FROM thread WHERE id = $1 AND org_id = $2`,
			threadID, actor.OrgID).Scan(&channelID, &dmSpaceID); err != nil {
			return apperr.NotFound("thread not found")
		}
		switch {
		case channelID != nil:
			if err := s.requireMember(ctx, tx, actor.OrgID, *channelID, actor.UserID); err != nil {
				return err
			}
		case dmSpaceID != nil:
			if err := s.requireParticipant(ctx, tx, *dmSpaceID, actor.UserID); err != nil {
				return err
			}
		default:
			return apperr.Invalid("space threads have no read watermark yet")
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
		// S6: reset the O(1) unread counter for this container to the exact
		// post-watermark truth, in the SAME tx as the watermark write. A space
		// thread has no counter (rejected above), so exactly one of the two is
		// set here.
		cid, did := int64(0), int64(0)
		if channelID != nil {
			cid = *channelID
		}
		if dmSpaceID != nil {
			did = *dmSpaceID
		}
		if err := s.resetContainerUnread(ctx, tx, actor.OrgID, actor.UserID, cid, did); err != nil {
			return err
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

type DMUnread struct {
	DMSpaceID   int64 `json:"dm_space_id"`
	UnreadCount int   `json:"unread_count"`
}

// DMUnreads mirrors Unreads over the DM plane: per-conversation counts of
// messages after the watermark, authored by someone else. S6: a plain O(1)
// index scan of the dm_space leg of the counter (the live aggregate survives
// as recomputeContainerUnread, the reconciliation truth).
func (s *Service) DMUnreads(ctx context.Context, actor auth.Identity) ([]DMUnread, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT dm_space_id, unread_count
		FROM container_unread_counter
		WHERE user_id = $1 AND org_id = $2
		  AND dm_space_id IS NOT NULL AND unread_count > 0`,
		actor.UserID, actor.OrgID)
	if err != nil {
		return nil, apperr.Internal("dm unreads", err)
	}
	defer rows.Close()
	var out []DMUnread
	for rows.Next() {
		var u DMUnread
		if err := rows.Scan(&u.DMSpaceID, &u.UnreadCount); err != nil {
			return nil, apperr.Internal("scan dm unread", err)
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

// Unreads returns per-channel unread counts for the actor across their
// subscribed channels. Unread messages are those after each thread's watermark
// (or all, if never read), excluding the user's own and deleted messages.
//
// S6 (the F-17 twin the scale note reserved, now built): this is a plain O(1)
// index scan of the maintained per-(user,channel) counter — O(the user's
// channels), never a re-aggregation over messages. The counter is kept current
// off the notification consumer pass (ApplyMessageUnread), MarkRead, and delete;
// the live aggregate that used to run HERE survives only as
// recomputeContainerUnread, the reconciliation truth. mention_count wakes the
// long-declared Mentioned badge (ChannelUnread.Mentioned).
func (s *Service) Unreads(ctx context.Context, actor auth.Identity) ([]ChannelUnread, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT channel_id, unread_count, mention_count
		FROM container_unread_counter
		WHERE user_id = $1 AND org_id = $2
		  AND channel_id IS NOT NULL AND unread_count > 0`,
		actor.UserID, actor.OrgID)
	if err != nil {
		return nil, apperr.Internal("unreads", err)
	}
	defer rows.Close()
	var out []ChannelUnread
	for rows.Next() {
		var u ChannelUnread
		var mention int
		if err := rows.Scan(&u.ChannelID, &u.UnreadCount, &mention); err != nil {
			return nil, apperr.Internal("scan unread", err)
		}
		u.Mentioned = mention > 0
		out = append(out, u)
	}
	return out, rows.Err()
}
