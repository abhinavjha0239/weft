// Package messaging: channels, threads, messages, reactions, read state
// (ARCHITECTURE.md module map). Owns: channel*, thread*, message*, reaction,
// pin, draft, scheduled_message, reminder, *_watermark/flag tables.
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

type Service struct {
	pool  *pgxpool.Pool
	perms *perms.Service
	files FileAttacher
}

// SetFiles wires the attachment hook (same pattern as the gateway's
// MarkReader — set at composition time).
func (s *Service) SetFiles(f FileAttacher) { s.files = f }

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
		// Visibility gate, separate from the verb: private-channel content is
		// member-only (ADR-008 C-2 read model; conservative for public too
		// until the read model lands).
		var member bool
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS (SELECT 1 FROM channel_member
			 WHERE channel_id = $1 AND user_id = $2 AND unsubscribed_at IS NULL)`,
			p.ChannelID, actor.UserID).Scan(&member); err != nil {
			return apperr.Internal("membership check", err)
		}
		if !member {
			return apperr.Forbidden("not a channel member")
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
}

// Get fetches one message the actor may read — the same visibility rule as
// ListMessages: channel messages require membership of the governing
// channel; DM messages require participation; space-thread messages
// (neither container) are org-visible in the v1 slice.
func (s *Service) Get(ctx context.Context, actor auth.Identity, msgID int64) (Message, error) {
	var m Message
	err := s.pool.QueryRow(ctx, `
		SELECT m.id, COALESCE(m.channel_id, 0), m.thread_id, m.author_id, m.source, m.rendered
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
		msgID, actor.UserID, actor.OrgID).Scan(&m.ID, &m.ChannelID, &m.ThreadID,
		&m.AuthorID, &m.Source, &m.Rendered)
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
	return one[0], nil
}
