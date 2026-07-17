// Package messaging: channels, threads, messages, reactions, read state
// (ARCHITECTURE.md module map). Owns: channel*, thread*, message*, reaction,
// pin, custom_emoji, draft, scheduled_message, reminder, *_watermark/flag tables.
package messaging

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

// FileAttacher records attachment references at message-write time; the
// files service implements it (an interface keeps messaging free of the
// files import, and nil-safe for tests that never attach).
type FileAttacher interface {
	AttachMessageReferences(ctx context.Context, tx pgx.Tx, actor auth.Identity, messageID int64, fileIDs []int64) (int, error)
	// Scheduled sends pin their attachments until delivery or cancellation.
	AttachEntityReferences(ctx context.Context, tx pgx.Tx, actor auth.Identity, entity enum.EntityType, entityID int64, fileIDs []int64) (int, error)
	ReleaseEntityReferences(ctx context.Context, tx pgx.Tx, entity enum.EntityType, entityID int64) error
}

// DeliverabilityPatcher maintains the F-17 materialized notification
// candidate set when a notification-shaping setting this module owns
// changes (channel level, thread follow state): the patch rides the SAME
// transaction as the setting write. The notification module implements it
// (an interface keeps messaging free of a notification import, and nil —
// compositions that never materialize notifications — skips maintenance).
type DeliverabilityPatcher interface {
	PatchChannelUser(ctx context.Context, tx pgx.Tx, orgID, channelID, userID int64) error
}

type Service struct {
	pool  *pgxpool.Pool
	perms *perms.Service
	files FileAttacher
	deliv DeliverabilityPatcher
}

// SetFiles wires the attachment hook (same pattern as the gateway's
// MarkReader — set at composition time).
func (s *Service) SetFiles(f FileAttacher) { s.files = f }

// SetDeliverability wires the F-17 candidate-set patcher (the SetFiles
// pattern — set at composition time).
func (s *Service) SetDeliverability(d DeliverabilityPatcher) { s.deliv = d }

func New(pool *pgxpool.Pool, p *perms.Service) *Service {
	return &Service{pool: pool, perms: p}
}

type SendParams struct {
	ChannelID int64
	ThreadID  int64 // 0 = channel root
	Content   string
}

// Send is the canonical write path (ARCHITECTURE.md §2): permission check,
// domain writes, event append — one transaction.
func (s *Service) Send(ctx context.Context, actor auth.Identity, p SendParams) (int64, error) {
	if p.Content == "" {
		return 0, apperr.Invalid("content required")
	}
	if len(p.Content) > 50_000 {
		return 0, apperr.Invalid("content too long (max 50000)")
	}
	var msgID int64
	err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		// The (verb,scope)→group check (ADR-006): most-specific assignment in
		// the channel→workspace→org chain, membership via the closure.
		chain, err := s.perms.ChannelScope(ctx, tx, actor.OrgID, p.ChannelID)
		if err != nil {
			return err
		}
		if err := s.perms.Require(ctx, tx, actor, perms.VerbSendMessage, chain); err != nil {
			return err
		}
		// Visibility gate, separate from the verb (P-34): a member passes; a
		// non-member of a PRIVATE channel is masked (the oracle-free 404 that
		// makes send indistinguishable from an absent channel), while a
		// non-member of a PUBLIC channel keeps the send-before-join 403.
		if err := s.requireMember(ctx, tx, actor.OrgID, p.ChannelID, actor.UserID); err != nil {
			return err
		}
		if err := s.requireLiveChannel(ctx, tx, actor.OrgID, p.ChannelID); err != nil {
			return err
		}

		threadID := p.ThreadID
		var threadKind int16
		if threadID == 0 {
			if err := tx.QueryRow(ctx,
				`SELECT root_thread_id, 2 FROM channel WHERE id = $1 AND org_id = $2`,
				p.ChannelID, actor.OrgID).Scan(&threadID, &threadKind); err != nil {
				return apperr.NotFound("channel not found")
			}
		} else {
			if err := tx.QueryRow(ctx,
				`SELECT kind FROM thread WHERE id = $1 AND channel_id = $2 AND org_id = $3`,
				threadID, p.ChannelID, actor.OrgID).Scan(&threadKind); err != nil {
				return apperr.NotFound("thread not found")
			}
		}

		// Shared write path: AST parse (in-tx mention resolution), insert,
		// message.created event with mention ids (F-4: ids, never content).
		msgID, err = s.InsertThreadMessage(ctx, tx, actor, threadID, &p.ChannelID, nil, p.Content)
		if err != nil {
			return err
		}
		// F-15: channel-root threads carry no denormalized counters.
		if threadKind == 1 {
			if _, err := tx.Exec(ctx, `
				UPDATE thread SET last_activity_at = now(),
				       message_count = message_count + 1 WHERE id = $1`,
				threadID); err != nil {
				return apperr.Internal("bump thread", err)
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return msgID, nil
}

type Message struct {
	ID        int64         `json:"id"`
	ChannelID int64         `json:"channel_id"`
	ThreadID  int64         `json:"thread_id"`
	AuthorID  int64         `json:"author_id"`
	Source    string        `json:"source"`
	Rendered  string        `json:"rendered"`
	Reactions []ReactionAgg `json:"reactions,omitempty"`
	// LinkPreviews are the unfurled links (P-15), document-ordered.
	LinkPreviews []LinkPreview `json:"link_previews,omitempty"`
	// ForwardedFrom is the source message id when this message was created by
	// a forward (P-03); null otherwise. Clients resolve it to render the
	// original's context. Populated by Get; list endpoints leave it null.
	ForwardedFrom *int64 `json:"forwarded_from"`
}

// Get fetches one message the actor may read — the same visibility rule as
// ListMessages: channel messages require membership of the governing channel
// or the channel being live web-public (P-16); DM messages require
// participation; space-thread messages (neither container) are org-visible in
// the v1 slice.
func (s *Service) Get(ctx context.Context, actor auth.Identity, msgID int64) (Message, error) {
	var m Message
	err := s.pool.QueryRow(ctx, `
		SELECT m.id, COALESCE(m.channel_id, 0), m.thread_id, m.author_id, m.source,
		       m.rendered, m.forwarded_from_message_id
		FROM message m
		WHERE m.id = $1 AND m.org_id = $3 AND m.deleted_at IS NULL
		  AND ((m.channel_id IS NOT NULL AND (EXISTS (
		         SELECT 1 FROM channel_member cm
		         WHERE cm.channel_id = m.channel_id AND cm.user_id = $2
		           AND cm.unsubscribed_at IS NULL)
		       OR EXISTS (
		         SELECT 1 FROM channel c
		         WHERE c.id = m.channel_id AND c.visibility = 3
		           AND c.archived_at IS NULL)))
		    OR (m.dm_space_id IS NOT NULL AND EXISTS (
		         SELECT 1 FROM dm_participant dp
		         WHERE dp.dm_space_id = m.dm_space_id AND dp.user_id = $2))
		    OR (m.channel_id IS NULL AND m.dm_space_id IS NULL))
		  AND NOT EXISTS (
		         SELECT 1 FROM channel c2
		         JOIN channel_member cm2 ON cm2.channel_id = c2.id
		           AND cm2.user_id = $2 AND cm2.unsubscribed_at IS NULL
		         WHERE c2.id = m.channel_id AND c2.history_mode = 2
		           AND cm2.history_from IS NOT NULL
		           AND m.created_at < cm2.history_from)`,
		msgID, actor.UserID, actor.OrgID).Scan(&m.ID, &m.ChannelID, &m.ThreadID,
		&m.AuthorID, &m.Source, &m.Rendered, &m.ForwardedFrom)
	if errors.Is(err, pgx.ErrNoRows) {
		return Message{}, apperr.NotFound("message not found")
	}
	if err != nil {
		return Message{}, apperr.Internal("get message", err)
	}
	one := []Message{m}
	if err := attachReactions(ctx, s.pool, actor.UserID, one); err != nil {
		return Message{}, err
	}
	if err := attachLinkPreviews(ctx, s.pool, one); err != nil {
		return Message{}, err
	}
	return one[0], nil
}
