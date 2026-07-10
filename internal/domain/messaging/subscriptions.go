package messaging

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

// Notification-shaping settings (ADR-011 N-1 steps 2–3). These are durable
// PERSONAL preferences: like read watermarks they are deliberately not
// event-logged — the notification materializer reads them at resolution
// time, and nothing else reacts to them.

// thread_subscription.state (0004): 1 followed · 2 muted · 3 unmuted
// (revives activity inside a muted channel). 0 = clear the row (inherit).
const (
	ThreadStateFollowed = 1
	ThreadStateMuted    = 2
	ThreadStateUnmuted  = 3
)

// SetThreadSubscription upserts the caller's follow/mute state on a channel
// thread (state 0 clears back to inherit). Channel-root threads are not
// followable (F-15); DM threads always notify and space threads get their
// own semantics with item watching.
func (s *Service) SetThreadSubscription(ctx context.Context, actor auth.Identity, threadID int64, state int16) error {
	if state < 0 || state > 3 {
		return apperr.Invalid("state must be 0 (clear), 1 (followed), 2 (muted) or 3 (unmuted)")
	}
	return db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		var channelID *int64
		var kind int16
		if err := tx.QueryRow(ctx,
			`SELECT channel_id, kind FROM thread WHERE id = $1 AND org_id = $2`,
			threadID, actor.OrgID).Scan(&channelID, &kind); err != nil {
			return apperr.NotFound("thread not found")
		}
		if channelID == nil {
			return apperr.Invalid("only channel threads carry follow/mute state")
		}
		if kind == 2 {
			return apperr.Invalid("the channel root thread cannot be followed or muted")
		}
		if err := s.requireMember(ctx, tx, *channelID, actor.UserID); err != nil {
			return err
		}
		if state == 0 {
			_, err := tx.Exec(ctx,
				`DELETE FROM thread_subscription WHERE thread_id = $1 AND user_id = $2`,
				threadID, actor.UserID)
			if err != nil {
				return apperr.Internal("clear subscription", err)
			}
			return nil
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO thread_subscription (thread_id, user_id, state, updated_at)
			VALUES ($1, $2, $3, now())
			ON CONFLICT (thread_id, user_id)
			DO UPDATE SET state = EXCLUDED.state, updated_at = now()`,
			threadID, actor.UserID, state); err != nil {
			return apperr.Internal("set subscription", err)
		}
		return nil
	})
}

// channel_member.level (0003): 0 inherit · 1 all · 2 mentions · 3 nothing.
const maxChannelLevel = 3

type ChannelNotificationParams struct {
	Level *int16
	Muted *bool
}

// SetChannelNotification updates the caller's OWN membership row: the
// activity level (N-1 step 2) and the separate mute flag (step 3 — mute is
// not a level; direct mentions and DMs break through it).
func (s *Service) SetChannelNotification(ctx context.Context, actor auth.Identity, channelID int64, p ChannelNotificationParams) error {
	if p.Level == nil && p.Muted == nil {
		return apperr.Invalid("nothing to update")
	}
	if p.Level != nil && (*p.Level < 0 || *p.Level > maxChannelLevel) {
		return apperr.Invalid("level must be 0 (inherit), 1 (all), 2 (mentions) or 3 (nothing)")
	}
	return db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.requireMember(ctx, tx, channelID, actor.UserID); err != nil {
			return err
		}
		if p.Level != nil {
			if _, err := tx.Exec(ctx, `
				UPDATE channel_member SET level = $1
				WHERE channel_id = $2 AND user_id = $3`,
				*p.Level, channelID, actor.UserID); err != nil {
				return apperr.Internal("set level", err)
			}
		}
		if p.Muted != nil {
			if _, err := tx.Exec(ctx, `
				UPDATE channel_member SET muted = $1
				WHERE channel_id = $2 AND user_id = $3`,
				*p.Muted, channelID, actor.UserID); err != nil {
				return apperr.Internal("set muted", err)
			}
		}
		return nil
	})
}
