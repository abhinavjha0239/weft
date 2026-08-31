package messaging

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/internal/eventlog"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

// Move a message between threads (P-04) — Zulip's "move message", recorded as
// a kind-3 message_revision carrying prev_thread_id. v1 is INTRA-channel only:
// the target thread must live in the SAME channel as the source (cross-channel
// moves are a later slice). DM/space messages cannot move (channel messages
// only), nor can a thread's root message (it anchors the thread, F-15).
// Permission mirrors delete (edit.go): the author, or moderate_messages
// resolved through the channel chain.

// revisionMove is kind 3 in the 0004 message_revision schema (1 content,
// 2 title/topic, 3 move, 4 delete).
const revisionMove = 3

// MoveMessage relocates a channel message to targetThreadID in the same
// channel. Everything runs in one transaction: the message row is locked
// (loadForWrite, sharing the delete/edit shape), authorized, recorded as a
// kind-3 revision with prev_thread_id, repointed, and both threads' denormalized
// counters are fixed; a message.moved event carries channel_id (gateway
// routing) and both thread ids (clients reload both threads).
func (s *Service) MoveMessage(ctx context.Context, actor auth.Identity, msgID, targetThreadID int64) error {
	return db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		m, _, _, authorID, err := s.loadForWrite(ctx, tx, actor, msgID, false)
		if err != nil {
			return err
		}
		// Channel messages only: the move is defined over channel threads. A
		// DM or space message (no channel) is rejected for author and
		// moderator alike.
		if m.channelID == nil {
			return apperr.Invalid("only channel messages can be moved")
		}
		// A thread's root message anchors it (F-15); moving it would orphan
		// the thread. Channel-root (kind 2) threads have no root message, so
		// their chat messages move freely.
		var isRoot bool
		if err := tx.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM thread WHERE org_id = $1 AND root_message_id = $2)`,
			actor.OrgID, msgID).Scan(&isRoot); err != nil {
			return apperr.Internal("root check", err)
		}
		if isRoot {
			return apperr.Invalid("a thread's root message cannot be moved")
		}
		// Permission: the author, else moderate_messages on the channel (the
		// delete precedent — a plain member without the verb is 403).
		if authorID != actor.UserID {
			chain, err := s.perms.ChannelScope(ctx, tx, actor.OrgID, *m.channelID)
			if err != nil {
				return err
			}
			if err := s.perms.Require(ctx, tx, actor, perms.VerbModerateMessages, chain); err != nil {
				return err
			}
		}
		if targetThreadID == m.threadID {
			return apperr.Invalid("message is already in that thread")
		}
		// The target must exist in the SAME channel (intra-channel only).
		// Locked so its counter bump is consistent with concurrent sends.
		var targetChannelID *int64
		var targetKind int16
		err = tx.QueryRow(ctx,
			`SELECT channel_id, kind FROM thread WHERE id = $1 AND org_id = $2 FOR UPDATE`,
			targetThreadID, actor.OrgID).Scan(&targetChannelID, &targetKind)
		if errors.Is(err, pgx.ErrNoRows) {
			return apperr.NotFound("thread not found")
		}
		if err != nil {
			return apperr.Internal("load target thread", err)
		}
		if targetChannelID == nil || *targetChannelID != *m.channelID {
			return apperr.Invalid("target must be in the same channel")
		}

		revNo, err := nextRevision(ctx, tx, msgID)
		if err != nil {
			return apperr.Internal("revision no", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO message_revision
				(message_id, revision_no, kind, prev_thread_id, edited_by)
			VALUES ($1, $2, $3, $4, $5)`,
			msgID, revNo, revisionMove, m.threadID, actor.UserID); err != nil {
			return apperr.Internal("append revision", err)
		}
		// Repoint the message. channel_id is unchanged (intra-channel), so the
		// search index and channel ACL are untouched.
		if _, err := tx.Exec(ctx,
			`UPDATE message SET thread_id = $1 WHERE id = $2`, targetThreadID, msgID); err != nil {
			return apperr.Internal("repoint message", err)
		}
		// Fix denormalized counters; kind-2 channel-root threads carry none
		// (F-15), so the kind=1 filter no-ops them.
		if _, err := tx.Exec(ctx, `
			UPDATE thread SET message_count = GREATEST(message_count - 1, 0)
			WHERE id = $1 AND kind = 1`, m.threadID); err != nil {
			return apperr.Internal("source count", err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE thread SET message_count = message_count + 1, last_activity_at = now()
			WHERE id = $1 AND kind = 1`, targetThreadID); err != nil {
			return apperr.Internal("target count", err)
		}
		if _, err := eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
			EntityType: enum.EntityMessage, EntityID: msgID, Verb: "message.moved",
			Payload: eventlog.MustPayload(map[string]any{
				"message_id":     msgID,
				"from_thread_id": m.threadID,
				"to_thread_id":   targetThreadID,
				"channel_id":     *m.channelID,
				// A move never rewrites created_at, so this stays the value
				// REST's floor compares (eventlog.MessageCreatedAtKey).
				eventlog.MessageCreatedAtKey: m.createdAt,
			}),
		}); err != nil {
			return apperr.Internal("append event", err)
		}
		return nil
	})
}
