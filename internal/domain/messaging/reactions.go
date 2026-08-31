package messaging

import (
	"context"
	"strings"
	"time"
	"unicode"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/internal/eventlog"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

// ReactionAgg is one emoji's aggregate on a message, viewer-aware.
type ReactionAgg struct {
	Emoji   string  `json:"emoji"`
	Count   int     `json:"count"`
	Me      bool    `json:"me"`
	UserIDs []int64 `json:"user_ids"`
}

// ReactionState is the post-toggle answer for one emoji.
type ReactionState struct {
	MessageID int64  `json:"message_id"`
	Emoji     string `json:"emoji"`
	Count     int    `json:"count"`
	Me        bool   `json:"me"`
}

// validEmoji bounds the stored token: a unicode emoji or a custom-emoji
// name — never empty, whitespace, or control characters.
func validEmoji(emoji string) bool {
	if emoji == "" || len(emoji) > 64 {
		return false
	}
	for _, r := range emoji {
		if unicode.IsSpace(r) || unicode.IsControl(r) {
			return false
		}
	}
	return true
}

// loadReactable resolves a message the actor may READ — reacting needs
// exactly message visibility (the same three-way container rule as Get):
// channel membership, DM participation, or an org-visible space thread.
// Container ids come back for the event payload (the gateway routes by
// them), and so does the message's createdAt: the gateway's protected-history
// floor judges the MESSAGE an event is about, not the event's own stamp
// (eventlog.MessageCreatedAtKey). Invisible and nonexistent are
// indistinguishable (no id oracle).
func (s *Service) loadReactable(ctx context.Context, tx pgx.Tx, actor auth.Identity, msgID int64) (threadID int64, channelID, dmSpaceID *int64, createdAt time.Time, err error) {
	err = tx.QueryRow(ctx, `
		SELECT m.thread_id, m.channel_id, m.dm_space_id, m.created_at
		FROM message m
		WHERE m.id = $1 AND m.org_id = $3 AND m.deleted_at IS NULL
		  AND ((m.channel_id IS NOT NULL AND EXISTS (
		         SELECT 1 FROM channel_member cm
		         WHERE cm.channel_id = m.channel_id AND cm.user_id = $2
		           AND cm.unsubscribed_at IS NULL))
		    OR (m.dm_space_id IS NOT NULL AND EXISTS (
		         SELECT 1 FROM dm_participant dp
		         WHERE dp.dm_space_id = m.dm_space_id AND dp.user_id = $2))
		    OR (m.channel_id IS NULL AND m.dm_space_id IS NULL))`,
		msgID, actor.UserID, actor.OrgID).
		Scan(&threadID, &channelID, &dmSpaceID, &createdAt)
	if err != nil {
		return 0, nil, nil, time.Time{}, apperr.NotFound("message not found")
	}
	return threadID, channelID, dmSpaceID, createdAt, nil
}

// AddReaction is an idempotent ensure-present: the first add records the
// row and a reaction.added event; re-adding changes nothing and emits
// nothing. Anyone who can read the message may react.
func (s *Service) AddReaction(ctx context.Context, actor auth.Identity, msgID int64, emoji string) (ReactionState, error) {
	return s.toggleReaction(ctx, actor, msgID, emoji, true)
}

// RemoveReaction is the idempotent ensure-absent twin — only ever the
// actor's own row.
func (s *Service) RemoveReaction(ctx context.Context, actor auth.Identity, msgID int64, emoji string) (ReactionState, error) {
	return s.toggleReaction(ctx, actor, msgID, emoji, false)
}

func (s *Service) toggleReaction(ctx context.Context, actor auth.Identity, msgID int64, emoji string, want bool) (ReactionState, error) {
	emoji = strings.TrimSpace(emoji)
	if !validEmoji(emoji) {
		return ReactionState{}, apperr.Invalid("emoji must be 1..64 chars with no whitespace or control characters")
	}
	out := ReactionState{MessageID: msgID, Emoji: emoji, Me: want}
	err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		threadID, channelID, dmSpaceID, createdAt, err := s.loadReactable(ctx, tx, actor, msgID)
		if err != nil {
			return err
		}
		var ct pgconn.CommandTag
		if want {
			ct, err = tx.Exec(ctx, `
				INSERT INTO reaction (message_id, user_id, emoji, kind)
				VALUES ($1, $2, $3, 1) ON CONFLICT DO NOTHING`,
				msgID, actor.UserID, emoji)
		} else {
			ct, err = tx.Exec(ctx, `
				DELETE FROM reaction
				WHERE message_id = $1 AND user_id = $2 AND emoji = $3`,
				msgID, actor.UserID, emoji)
		}
		if err != nil {
			return apperr.Internal("toggle reaction", err)
		}
		if ct.RowsAffected() > 0 {
			verb := "reaction.added"
			if !want {
				verb = "reaction.removed"
			}
			payload := map[string]any{
				"message_id": msgID, "thread_id": threadID,
				"emoji": emoji, "user_id": actor.UserID,
				// The reaction is new; the MESSAGE it is on may predate a
				// member's protected-history floor
				// (eventlog.MessageCreatedAtKey).
				eventlog.MessageCreatedAtKey: createdAt}
			if channelID != nil {
				payload["channel_id"] = *channelID
			}
			if dmSpaceID != nil {
				payload["dm_space_id"] = *dmSpaceID
			}
			if _, err := eventlog.Append(ctx, tx, eventlog.Event{
				OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
				EntityType: enum.EntityMessage, EntityID: msgID, Verb: verb,
				Payload: eventlog.MustPayload(payload),
			}); err != nil {
				return apperr.Internal("append event", err)
			}
		}
		if err := tx.QueryRow(ctx, `
			SELECT count(*) FROM reaction WHERE message_id = $1 AND emoji = $2`,
			msgID, emoji).Scan(&out.Count); err != nil {
			return apperr.Internal("count reactions", err)
		}
		return nil
	})
	if err != nil {
		return ReactionState{}, err
	}
	return out, nil
}

// querier is the shared Query shape of pgxpool.Pool and pgx.Tx, so the
// aggregate loader serves both the paged list (in-tx) and single Get.
type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// attachReactions fills the Reactions aggregates for a fetched message
// page in one grouped query, chips ordered by first-reacted.
func attachReactions(ctx context.Context, q querier, viewerID int64, msgs []Message) error {
	if len(msgs) == 0 {
		return nil
	}
	ids := make([]int64, len(msgs))
	byID := make(map[int64]*Message, len(msgs))
	for i := range msgs {
		ids[i] = msgs[i].ID
		byID[msgs[i].ID] = &msgs[i]
	}
	rows, err := q.Query(ctx, `
		SELECT message_id, emoji, count(*),
		       bool_or(user_id = $2),
		       array_agg(user_id ORDER BY created_at)
		FROM reaction
		WHERE message_id = ANY($1)
		GROUP BY message_id, emoji
		ORDER BY message_id, min(created_at)`, ids, viewerID)
	if err != nil {
		return apperr.Internal("load reactions", err)
	}
	defer rows.Close()
	for rows.Next() {
		var msgID int64
		var agg ReactionAgg
		if err := rows.Scan(&msgID, &agg.Emoji, &agg.Count, &agg.Me, &agg.UserIDs); err != nil {
			return apperr.Internal("scan reaction", err)
		}
		if m := byID[msgID]; m != nil {
			m.Reactions = append(m.Reactions, agg)
		}
	}
	return rows.Err()
}
