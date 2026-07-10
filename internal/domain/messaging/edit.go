package messaging

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/domain/content"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/internal/eventlog"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

// Edit and delete (ADR-007 M-3 + F-8). Policy in the v1 slice:
//   - Only the AUTHOR edits content — moderation deletes, it never rewrites.
//   - Delete is revision-append + live-field scrub (F-8): the prior content
//     moves into a kind=4 revision row and the live row empties, which also
//     drops it from the search index (search_tsv generates from source).
//   - Channel messages honor the frozen-archive rule; authors can act on
//     their own messages in any container they authored into.

// revision kinds (0004 schema comment).
const (
	revisionContent = 1
	revisionDelete  = 4
)

type editedMessage struct {
	threadID  int64
	channelID *int64
	dmSpaceID *int64
}

// loadForWrite locks the message row (serializing revisions) and applies the
// author/container rules shared by edit and delete.
func (s *Service) loadForWrite(ctx context.Context, tx pgx.Tx, actor auth.Identity, msgID int64, requireAuthor bool) (editedMessage, string, []byte, int64, error) {
	var m editedMessage
	var source string
	var ast []byte
	var authorID int64
	err := tx.QueryRow(ctx, `
		SELECT thread_id, channel_id, dm_space_id, author_id, source, ast
		FROM message
		WHERE id = $1 AND org_id = $2 AND deleted_at IS NULL
		FOR UPDATE`, msgID, actor.OrgID).
		Scan(&m.threadID, &m.channelID, &m.dmSpaceID, &authorID, &source, &ast)
	if err != nil {
		return m, "", nil, 0, apperr.NotFound("message not found")
	}
	if requireAuthor && authorID != actor.UserID {
		return m, "", nil, 0, apperr.Forbidden("only the author can edit a message")
	}
	if m.channelID != nil {
		if err := s.requireLiveChannel(ctx, tx, actor.OrgID, *m.channelID); err != nil {
			return m, "", nil, 0, err
		}
	}
	return m, source, ast, authorID, nil
}

func nextRevision(ctx context.Context, tx pgx.Tx, msgID int64) (int16, error) {
	var n int16
	err := tx.QueryRow(ctx,
		`SELECT COALESCE(MAX(revision_no), 0) + 1 FROM message_revision WHERE message_id = $1`,
		msgID).Scan(&n)
	return n, err
}

// EditMessage replaces a message's content: prior content becomes a
// revision, the new source re-parses through the content engine (mentions
// re-resolved in-tx), and the event carries the NEWLY-added mention ids so
// the notification pipeline pings only them.
func (s *Service) EditMessage(ctx context.Context, actor auth.Identity, msgID int64, newSource string) error {
	if newSource == "" {
		return apperr.Invalid("content required")
	}
	if len(newSource) > 50_000 {
		return apperr.Invalid("content too long (max 50000)")
	}
	return db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		m, oldSource, oldAST, _, err := s.loadForWrite(ctx, tx, actor, msgID, true)
		if err != nil {
			return err
		}
		if newSource == oldSource {
			return nil // idempotent no-op, no revision noise
		}
		revNo, err := nextRevision(ctx, tx, msgID)
		if err != nil {
			return apperr.Internal("revision no", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO message_revision
				(message_id, revision_no, kind, prev_source, prev_ast, edited_by)
			VALUES ($1, $2, $3, $4, $5, $6)`,
			msgID, revNo, revisionContent, oldSource, oldAST, actor.UserID); err != nil {
			return apperr.Internal("append revision", err)
		}
		doc := content.Parse(newSource, func(label string) (int64, bool) {
			var uid int64
			err := tx.QueryRow(ctx, `
				SELECT id FROM user_account
				WHERE org_id = $1 AND full_name = $2 AND deactivated_at IS NULL
				ORDER BY id LIMIT 1`, actor.OrgID, label).Scan(&uid)
			return uid, err == nil
		})
		if _, err := tx.Exec(ctx, `
			UPDATE message SET source = $1, ast = $2, rendered = $3,
			       render_version = $4, has_link = $5, edited_at = now()
			WHERE id = $6`,
			newSource, doc.JSON(), content.RenderHTML(doc), content.RenderVersion,
			doc.HasLink(), msgID); err != nil {
			return apperr.Internal("apply edit", err)
		}

		before := map[int64]bool{}
		for _, id := range content.MentionIDs(oldAST) {
			before[id] = true
		}
		newMentions := []int64{}
		for _, id := range doc.Mentions() {
			if !before[id] {
				newMentions = append(newMentions, id)
			}
		}
		payload := map[string]any{
			"message_id": msgID, "thread_id": m.threadID, "new_mentions": newMentions}
		if m.channelID != nil {
			payload["channel_id"] = *m.channelID
		}
		if m.dmSpaceID != nil {
			payload["dm_space_id"] = *m.dmSpaceID
		}
		if _, err := eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
			EntityType: enum.EntityMessage, EntityID: msgID, Verb: "message.edited",
			Payload: eventlog.MustPayload(payload),
		}); err != nil {
			return apperr.Internal("append event", err)
		}
		return nil
	})
}

// DeleteMessage is the F-8 scrub: the author always may; in channels,
// moderate_messages (resolved through the channel chain) may delete others'
// messages — DM and space messages stay author-only (a DM has no moderator).
func (s *Service) DeleteMessage(ctx context.Context, actor auth.Identity, msgID int64) error {
	return db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		m, oldSource, oldAST, authorID, err := s.loadForWrite(ctx, tx, actor, msgID, false)
		if err != nil {
			return err
		}
		if authorID != actor.UserID {
			if m.channelID == nil {
				return apperr.Forbidden("only the author can delete this message")
			}
			chain, err := s.perms.ChannelScope(ctx, tx, actor.OrgID, *m.channelID)
			if err != nil {
				return err
			}
			if err := s.perms.Require(ctx, tx, actor, perms.VerbModerateMessages, chain); err != nil {
				return err
			}
		}
		revNo, err := nextRevision(ctx, tx, msgID)
		if err != nil {
			return apperr.Internal("revision no", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO message_revision
				(message_id, revision_no, kind, prev_source, prev_ast, edited_by)
			VALUES ($1, $2, $3, $4, $5, $6)`,
			msgID, revNo, revisionDelete, oldSource, oldAST, actor.UserID); err != nil {
			return apperr.Internal("append revision", err)
		}
		// Scrub the live row: emptying source also clears the generated
		// search vector; the revision row is the purgeable capture.
		if _, err := tx.Exec(ctx, `
			UPDATE message SET source = '', ast = '{}', rendered = '',
			       has_link = false, has_attachment = false, has_image = false,
			       deleted_at = now()
			WHERE id = $1`, msgID); err != nil {
			return apperr.Internal("scrub", err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE thread SET message_count = GREATEST(message_count - 1, 0)
			WHERE id = $1 AND kind = 1`, m.threadID); err != nil {
			return apperr.Internal("thread count", err)
		}
		payload := map[string]any{"message_id": msgID, "thread_id": m.threadID}
		if m.channelID != nil {
			payload["channel_id"] = *m.channelID
		}
		if m.dmSpaceID != nil {
			payload["dm_space_id"] = *m.dmSpaceID
		}
		if _, err := eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
			EntityType: enum.EntityMessage, EntityID: msgID, Verb: "message.deleted",
			Payload: eventlog.MustPayload(payload),
		}); err != nil {
			return apperr.Internal("append event", err)
		}
		return nil
	})
}
