package messaging

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

// Drafts are private per-user compose state — never ACL'd beyond ownership,
// never event-logged, never visible to anyone else (the read-state
// precedent). Container hints (channel/thread/dm) are optional and only
// checked for org-locality; sending is where the real gates run.

const maxDraftBytes = 100 << 10 // 100 KiB — far beyond any message

type Draft struct {
	ID        int64     `json:"draft_id"`
	ChannelID *int64    `json:"channel_id,omitempty"`
	ThreadID  *int64    `json:"thread_id,omitempty"`
	DMSpaceID *int64    `json:"dm_space_id,omitempty"`
	Source    string    `json:"source"`
	UpdatedAt time.Time `json:"updated_at"`
}

type DraftParams struct {
	ChannelID *int64
	ThreadID  *int64
	DMSpaceID *int64
	Source    string
}

func validateDraft(p DraftParams) error {
	if p.Source == "" || len(p.Source) > maxDraftBytes {
		return apperr.Invalid("draft source must be 1 byte to 100 KiB")
	}
	return nil
}

// checkDraftTargets pins any provided container hint to the actor's org —
// a foreign id is a 404, keeping rows tidy without leaking anything.
func (s *Service) checkDraftTargets(ctx context.Context, tx pgx.Tx, actor auth.Identity, p DraftParams) error {
	for _, c := range []struct {
		id    *int64
		table string
	}{
		{p.ChannelID, "channel"},
		{p.ThreadID, "thread"},
		{p.DMSpaceID, "dm_space"},
	} {
		if c.id == nil {
			continue
		}
		var ok bool
		if err := tx.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM `+c.table+` WHERE id = $1 AND org_id = $2)`,
			*c.id, actor.OrgID).Scan(&ok); err != nil {
			return apperr.Internal("draft target check", err)
		}
		if !ok {
			return apperr.NotFound(c.table + " not found")
		}
	}
	return nil
}

// CreateDraft stores a new draft for the actor.
func (s *Service) CreateDraft(ctx context.Context, actor auth.Identity, p DraftParams) (Draft, error) {
	if err := validateDraft(p); err != nil {
		return Draft{}, err
	}
	var d Draft
	err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.checkDraftTargets(ctx, tx, actor, p); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `
			INSERT INTO draft (user_id, channel_id, thread_id, dm_space_id, source)
			VALUES ($1, $2, $3, $4, $5) RETURNING id, updated_at`,
			actor.UserID, p.ChannelID, p.ThreadID, p.DMSpaceID, p.Source).
			Scan(&d.ID, &d.UpdatedAt); err != nil {
			return apperr.Internal("create draft", err)
		}
		return nil
	})
	if err != nil {
		return Draft{}, err
	}
	d.ChannelID, d.ThreadID, d.DMSpaceID, d.Source = p.ChannelID, p.ThreadID, p.DMSpaceID, p.Source
	return d, nil
}

// UpdateDraft replaces the actor's own draft (content and targets).
func (s *Service) UpdateDraft(ctx context.Context, actor auth.Identity, draftID int64, p DraftParams) error {
	if err := validateDraft(p); err != nil {
		return err
	}
	return db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.checkDraftTargets(ctx, tx, actor, p); err != nil {
			return err
		}
		ct, err := tx.Exec(ctx, `
			UPDATE draft SET channel_id = $3, thread_id = $4, dm_space_id = $5,
			       source = $6, updated_at = now()
			WHERE id = $1 AND user_id = $2`,
			draftID, actor.UserID, p.ChannelID, p.ThreadID, p.DMSpaceID, p.Source)
		if err != nil {
			return apperr.Internal("update draft", err)
		}
		if ct.RowsAffected() == 0 {
			return apperr.NotFound("draft not found")
		}
		return nil
	})
}

// DeleteDraft removes the actor's own draft; deleting a missing one 404s.
func (s *Service) DeleteDraft(ctx context.Context, actor auth.Identity, draftID int64) error {
	ct, err := s.pool.Exec(ctx,
		`DELETE FROM draft WHERE id = $1 AND user_id = $2`, draftID, actor.UserID)
	if err != nil {
		return apperr.Internal("delete draft", err)
	}
	if ct.RowsAffected() == 0 {
		return apperr.NotFound("draft not found")
	}
	return nil
}

// ListDrafts returns the actor's drafts, most recently touched first.
func (s *Service) ListDrafts(ctx context.Context, actor auth.Identity) ([]Draft, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, channel_id, thread_id, dm_space_id, source, updated_at
		FROM draft WHERE user_id = $1
		ORDER BY updated_at DESC, id DESC LIMIT 200`, actor.UserID)
	if err != nil {
		return nil, apperr.Internal("list drafts", err)
	}
	defer rows.Close()
	out := []Draft{}
	for rows.Next() {
		var d Draft
		if err := rows.Scan(&d.ID, &d.ChannelID, &d.ThreadID, &d.DMSpaceID,
			&d.Source, &d.UpdatedAt); err != nil {
			return nil, apperr.Internal("scan draft", err)
		}
		out = append(out, d)
	}
	return out, rows.Err()
}
