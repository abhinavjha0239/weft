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
// precedent). Container hints (channel/thread/dm) are optional; each is
// validated through the SAME read-visibility gate its container's read path
// uses, so a stored hint can never reveal more than a read would. Sending is
// where the full send/permission gates run.

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

// checkDraftTargets validates any provided container hint through the SAME
// read-visibility gate its container's read path uses, so a stored hint can
// never reveal more than a read would. Each hint the actor cannot see is an
// oracle-free 404, byte-identical to a nonexistent or foreign-org id:
//   - channel → requireChannelRead (a private channel the actor is not in is
//     the masked "channel not found", the P-34 decision table verbatim);
//   - dm_space → requireParticipant (a DM they are not in is "conversation not
//     found", the P-33 mask);
//   - thread → its container's gate, every denial mapped to one "thread not
//     found" (below).
//
// Without this a draft POST/PATCH was a success-vs-404 EXISTENCE ORACLE:
// probing an id returned 201 when a private channel / foreign DM / hidden
// thread existed and 404 when it did not — stronger than the 403 P-34 closed
// and, via the dm_space hint, a re-opening of the P-33 DM mask.
func (s *Service) checkDraftTargets(ctx context.Context, tx pgx.Tx, actor auth.Identity, p DraftParams) error {
	if p.ChannelID != nil {
		if err := s.requireChannelRead(ctx, tx, actor.OrgID, *p.ChannelID, actor.UserID); err != nil {
			return err
		}
	}
	if p.DMSpaceID != nil {
		if err := s.requireParticipant(ctx, tx, *p.DMSpaceID, actor.UserID); err != nil {
			return err
		}
	}
	if p.ThreadID != nil {
		if err := s.checkDraftThread(ctx, tx, actor, *p.ThreadID); err != nil {
			return err
		}
	}
	return nil
}

// checkDraftThread masks a thread hint by its container's read gate. A thread
// in a private channel the actor cannot read, or in a DM they are not in, is
// the SAME "thread not found" 404 as a nonexistent thread — every container
// denial is mapped to that one body so the hint is fully oracle-free at the
// thread id (absent and masked are indistinguishable). Space-governed threads
// follow v1 org-wide space visibility (a recorded gap), so they are accepted.
func (s *Service) checkDraftThread(ctx context.Context, tx pgx.Tx, actor auth.Identity, threadID int64) error {
	var channelID, dmSpaceID, spaceID *int64
	if err := tx.QueryRow(ctx, `
		SELECT channel_id, dm_space_id, space_id FROM thread
		WHERE id = $1 AND org_id = $2`, threadID, actor.OrgID).
		Scan(&channelID, &dmSpaceID, &spaceID); err != nil {
		return apperr.NotFound("thread not found")
	}
	switch {
	case channelID != nil:
		if err := s.requireChannelRead(ctx, tx, actor.OrgID, *channelID, actor.UserID); err != nil {
			return apperr.NotFound("thread not found")
		}
	case dmSpaceID != nil:
		if err := s.requireParticipant(ctx, tx, *dmSpaceID, actor.UserID); err != nil {
			return apperr.NotFound("thread not found")
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
