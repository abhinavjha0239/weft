package messaging

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

// Saved items (ADR-007 M-6, saved_item kind 1 "saved-for-later"). PERSONAL
// state, like read-state and drafts: never event-logged, never visible to
// anyone else. Saving requires read visibility (the same three-way container
// ACL as a message fetch, oracle-free 404); removing does NOT — a user may
// always clear their own entry, including a tombstone for a message that was
// deleted or a channel they have since left. A saved message that is later
// deleted STAYS in the list as a tombstone (empty excerpt, deleted:true) — it
// is never silently dropped. The kind-2 "star" is a separate later slice; this
// list is scoped to kind 1.

const savedExcerptLen = 160

// SavedItem is one entry in the caller's saved list, newest save first.
type SavedItem struct {
	MessageID int64  `json:"message_id"`
	ThreadID  int64  `json:"thread_id"`
	ChannelID int64  `json:"channel_id,omitempty"`
	DMSpaceID int64  `json:"dm_space_id,omitempty"`
	AuthorID  int64  `json:"author_id"`
	Excerpt   string `json:"excerpt"`
	Deleted   bool   `json:"deleted"`
	// Accessible=false masks the excerpt: the caller saved this message but
	// can no longer read its container (left the channel), so the CURRENT
	// content — including edits made after they left — must not leak. The
	// row itself stays (their bookmark, their business).
	Accessible bool      `json:"accessible"`
	SavedAt    time.Time `json:"saved_at"`
}

// SaveMessage records the message in the caller's saved list. Idempotent
// (a second save changes nothing). Requires read visibility: a message the
// caller cannot see — or one already deleted — is an oracle-free 404.
func (s *Service) SaveMessage(ctx context.Context, actor auth.Identity, msgID int64) error {
	return db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		// The three-way read ACL (shared with reactions): channel membership,
		// DM participation, or an org-visible space thread; deleted → 404.
		if _, _, _, err := s.loadReactable(ctx, tx, actor, msgID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO saved_item (user_id, message_id, kind)
			VALUES ($1, $2, 1) ON CONFLICT DO NOTHING`,
			actor.UserID, msgID); err != nil {
			return apperr.Internal("save message", err)
		}
		return nil
	})
}

// UnsaveMessage removes the caller's saved entry. Idempotent and ungated:
// it only ever touches the caller's own row, so removing a tombstone or an
// entry for a now-inaccessible message always succeeds (nothing leaks).
func (s *Service) UnsaveMessage(ctx context.Context, actor auth.Identity, msgID int64) error {
	if _, err := s.pool.Exec(ctx,
		`DELETE FROM saved_item WHERE user_id = $1 AND message_id = $2`,
		actor.UserID, msgID); err != nil {
		return apperr.Internal("unsave message", err)
	}
	return nil
}

// ListSaved returns the caller's saved messages, newest save first, capped at
// 200. Container ids and an excerpt ride each row; a deleted source renders as
// a tombstone (its live source was scrubbed on delete, so the excerpt empty).
// No per-row ACL re-check at list time (personal state, read-state precedent):
// visibility was enforced at save time. The org pin lives on the message join.
func (s *Service) ListSaved(ctx context.Context, actor auth.Identity) ([]SavedItem, error) {
	// The read-time ACL re-check: the same three-way container rule as a
	// fetch. Losing access does not drop the bookmark, but it MASKS the
	// excerpt — post-departure edits never leak through a saved list.
	rows, err := s.pool.Query(ctx, `
		SELECT si.message_id, m.thread_id, COALESCE(m.channel_id, 0),
		       COALESCE(m.dm_space_id, 0), m.author_id, m.source,
		       m.deleted_at IS NOT NULL, si.created_at,
		       ((m.channel_id IS NOT NULL AND EXISTS (
		           SELECT 1 FROM channel_member cm
		           WHERE cm.channel_id = m.channel_id AND cm.user_id = $1
		             AND cm.unsubscribed_at IS NULL))
		        OR (m.dm_space_id IS NOT NULL AND EXISTS (
		           SELECT 1 FROM dm_participant dp
		           WHERE dp.dm_space_id = m.dm_space_id AND dp.user_id = $1))
		        OR (m.channel_id IS NULL AND m.dm_space_id IS NULL)) AS accessible
		FROM saved_item si
		JOIN message m ON m.id = si.message_id AND m.org_id = $2
		WHERE si.user_id = $1 AND si.kind = 1
		ORDER BY si.created_at DESC, si.message_id DESC
		LIMIT 200`, actor.UserID, actor.OrgID)
	if err != nil {
		return nil, apperr.Internal("list saved", err)
	}
	defer rows.Close()
	out := []SavedItem{}
	for rows.Next() {
		var it SavedItem
		var source string
		if err := rows.Scan(&it.MessageID, &it.ThreadID, &it.ChannelID,
			&it.DMSpaceID, &it.AuthorID, &source, &it.Deleted, &it.SavedAt,
			&it.Accessible); err != nil {
			return nil, apperr.Internal("scan saved", err)
		}
		if it.Accessible {
			it.Excerpt = excerpt(source, savedExcerptLen)
		}
		out = append(out, it)
	}
	return out, rows.Err()
}

// excerpt returns at most max runes of s (rune-safe so a multibyte character
// is never split). A deleted message's scrubbed empty source yields "".
func excerpt(s string, maxRunes int) string {
	r := []rune(s)
	if len(r) <= maxRunes {
		return s
	}
	return string(r[:maxRunes])
}
