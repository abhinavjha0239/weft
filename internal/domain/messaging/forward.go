package messaging

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

// Message forwarding and quoting (P-03). Forwarding copies ONE message into
// another thread the actor may post to, as a normal new message whose source
// is: an optional leading comment, a quoted block of the original, and a plain
// attribution line. forwarded_from_message_id records the immediate parent
// (forwarding a forward just chains it). Attachments do NOT re-attach: the
// file links in the quote are inert text (deliverToThreadOpts(attachFiles=
// false)), so a reader of the forward can download a referenced file only if
// some message THEY can already read references it (ADR-012 union-ACL) — no
// new file_reference rows are created.

const (
	// The quoted original is capped at this many runes, then an ellipsis.
	forwardQuoteRunes = 1000
	// zwsp breaks the @** mention token so a quoted mention never notifies.
	zwsp = "\u200b"
)

// ForwardMessage copies srcMsgID into targetThreadID with an optional comment.
// Both gates run in one transaction: the actor must pass the three-way READ
// ACL on the source (oracle-free 404) and the FULL send gate on the target
// (deliverToThreadOpts). Returns the new message id.
func (s *Service) ForwardMessage(ctx context.Context, actor auth.Identity, srcMsgID, targetThreadID int64, comment string) (int64, error) {
	comment = strings.TrimSpace(comment)
	if len(comment) > 50_000 {
		return 0, apperr.Invalid("comment too long (max 50000)")
	}
	var newMsgID int64
	err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		// Source READ gate (oracle-free 404) + the content to quote. A
		// deleted/tombstoned source is filtered out → 404.
		src, authorName, err := s.loadForwardSource(ctx, tx, actor, srcMsgID)
		if err != nil {
			return err
		}
		fwdSource := buildForwardSource(comment, src, authorName)
		// Target SEND gate + insert WITHOUT re-attaching files + activity bump,
		// in the same tx as the source gate.
		newMsgID, err = s.deliverToThreadOpts(ctx, tx, actor, targetThreadID, fwdSource, false)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`UPDATE message SET forwarded_from_message_id = $1 WHERE id = $2`,
			srcMsgID, newMsgID); err != nil {
			return apperr.Internal("record forward source", err)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return newMsgID, nil
}

// loadForwardSource resolves a message the actor may READ (the same three-way
// container ACL as Get / loadReactable, oracle-free 404) and returns its raw
// source plus the author's display name for the attribution line.
func (s *Service) loadForwardSource(ctx context.Context, tx pgx.Tx, actor auth.Identity, msgID int64) (source, authorName string, err error) {
	err = tx.QueryRow(ctx, `
		SELECT m.source, ua.full_name
		FROM message m
		JOIN user_account ua ON ua.id = m.author_id
		WHERE m.id = $1 AND m.org_id = $3 AND m.deleted_at IS NULL
		  AND ((m.channel_id IS NOT NULL AND EXISTS (
		         SELECT 1 FROM channel_member cm
		         WHERE cm.channel_id = m.channel_id AND cm.user_id = $2
		           AND cm.unsubscribed_at IS NULL))
		    OR (m.dm_space_id IS NOT NULL AND EXISTS (
		         SELECT 1 FROM dm_participant dp
		         WHERE dp.dm_space_id = m.dm_space_id AND dp.user_id = $2))
		    OR (m.channel_id IS NULL AND m.dm_space_id IS NULL))`,
		msgID, actor.UserID, actor.OrgID).Scan(&source, &authorName)
	if err != nil {
		return "", "", apperr.NotFound("message not found")
	}
	return source, authorName, nil
}

// buildForwardSource assembles the forwarded message body: the optional
// comment (the forwarder's own words — mentions there DO notify), then a
// `> `-quoted copy of the original capped at forwardQuoteRunes, then a plain
// attribution line. Mentions inside the quote AND the attribution are
// neutralized so forwarding never notifies the people named in the original
// nor pings the original author — a forward is not a mention. The comment is
// left untouched.
func buildForwardSource(comment, source, authorName string) string {
	runes := []rune(source)
	truncated := false
	if len(runes) > forwardQuoteRunes {
		source = string(runes[:forwardQuoteRunes])
		truncated = true
	}
	var b strings.Builder
	for i, line := range strings.Split(source, "\n") {
		if i > 0 {
			b.WriteByte('\n')
		}
		b.WriteString("> ")
		b.WriteString(line)
	}
	if truncated {
		b.WriteString("…")
	}
	b.WriteString("\n— forwarded from @**")
	b.WriteString(authorName)
	b.WriteString("**")
	// Neutralize every mention token in the quote + attribution (never the
	// comment): a zero-width space between @ and ** breaks the parser's
	// exact "@**" match, so the name still renders but resolves to no mention
	// id and therefore fires no notification.
	quoted := strings.ReplaceAll(b.String(), "@**", "@"+zwsp+"**")
	if comment == "" {
		return quoted
	}
	return comment + "\n\n" + quoted
}
