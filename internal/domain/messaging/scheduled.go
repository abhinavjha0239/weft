package messaging

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/domain/content"
	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

// Scheduled sends (ADR-007): the full send gate runs TWICE — at schedule
// time (fail fast) and again at fire time (access revoked in between must
// fail the delivery, recorded on the row for its author to see). Attachment
// links claim EntityScheduled file references immediately, so the GC's
// unclaimed lane can never reap an upload that a pending send still needs
// (the ADR-012 not-yet-sent rule); delivery or cancellation releases them,
// the live message re-claims its own.

const maxScheduleHorizon = 365 * 24 * time.Hour

type ScheduledMessage struct {
	ID           int64     `json:"scheduled_id"`
	ThreadID     *int64    `json:"thread_id,omitempty"`
	ChannelID    *int64    `json:"channel_id,omitempty"`
	DMSpaceID    *int64    `json:"dm_space_id,omitempty"`
	Source       string    `json:"source"`
	ScheduledFor time.Time `json:"scheduled_for"`
	FailedReason *string   `json:"failed_reason,omitempty"`
}

// ScheduleMessage queues a send into a thread at a future time.
func (s *Service) ScheduleMessage(ctx context.Context, actor auth.Identity, threadID int64, source string, scheduledFor time.Time) (ScheduledMessage, error) {
	if source == "" {
		return ScheduledMessage{}, apperr.Invalid("content required")
	}
	now := time.Now()
	if !scheduledFor.After(now) {
		return ScheduledMessage{}, apperr.Invalid("scheduled_for must be in the future")
	}
	if scheduledFor.After(now.Add(maxScheduleHorizon)) {
		return ScheduledMessage{}, apperr.Invalid("scheduled_for must be within a year")
	}
	var out ScheduledMessage
	err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		channelID, dmSpaceID, _, err := s.requireThreadSend(ctx, tx, actor, threadID)
		if err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `
			INSERT INTO scheduled_message
				(org_id, author_id, channel_id, thread_id, dm_space_id, source, scheduled_for)
			VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id`,
			actor.OrgID, actor.UserID, channelID, threadID, dmSpaceID,
			source, scheduledFor).Scan(&out.ID); err != nil {
			return apperr.Internal("schedule message", err)
		}
		out.ThreadID = &threadID
		out.ChannelID, out.DMSpaceID = channelID, dmSpaceID
		out.Source, out.ScheduledFor = source, scheduledFor
		return s.syncScheduledFileRefs(ctx, tx, actor, out.ID, source)
	})
	if err != nil {
		return ScheduledMessage{}, err
	}
	return out, nil
}

// syncScheduledFileRefs re-derives the pending send's file pins from its
// current content: release everything, re-attach what the links claim.
func (s *Service) syncScheduledFileRefs(ctx context.Context, tx pgx.Tx, actor auth.Identity, scheduledID int64, source string) error {
	if s.files == nil {
		return nil
	}
	if err := s.files.ReleaseEntityReferences(ctx, tx, enum.EntityScheduled, scheduledID); err != nil {
		return err
	}
	doc := content.Parse(source, func(string) (int64, bool) { return 0, false })
	if ids := fileIDsFromLinks(doc.Links()); len(ids) > 0 {
		if _, err := s.files.AttachEntityReferences(ctx, tx, actor,
			enum.EntityScheduled, scheduledID, ids); err != nil {
			return err
		}
	}
	return nil
}

// UpdateScheduled edits a pending send's content and/or fire time.
func (s *Service) UpdateScheduled(ctx context.Context, actor auth.Identity, scheduledID int64, source *string, scheduledFor *time.Time) error {
	if source != nil && *source == "" {
		return apperr.Invalid("content required")
	}
	if scheduledFor != nil {
		now := time.Now()
		if !scheduledFor.After(now) || scheduledFor.After(now.Add(maxScheduleHorizon)) {
			return apperr.Invalid("scheduled_for must be in the future, within a year")
		}
	}
	return db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		var current string
		err := tx.QueryRow(ctx, `
			SELECT source FROM scheduled_message
			WHERE id = $1 AND author_id = $2 AND org_id = $3
			  AND sent_message_id IS NULL AND failed_reason IS NULL
			FOR UPDATE`,
			scheduledID, actor.UserID, actor.OrgID).Scan(&current)
		if err != nil {
			return apperr.NotFound("scheduled message not found")
		}
		newSource := current
		if source != nil {
			newSource = *source
		}
		if _, err := tx.Exec(ctx, `
			UPDATE scheduled_message
			SET source = $2, scheduled_for = COALESCE($3, scheduled_for)
			WHERE id = $1`, scheduledID, newSource, scheduledFor); err != nil {
			return apperr.Internal("update scheduled", err)
		}
		return s.syncScheduledFileRefs(ctx, tx, actor, scheduledID, newSource)
	})
}

// CancelScheduled removes a pending or failed send and releases its pins.
func (s *Service) CancelScheduled(ctx context.Context, actor auth.Identity, scheduledID int64) error {
	return db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		ct, err := tx.Exec(ctx, `
			DELETE FROM scheduled_message
			WHERE id = $1 AND author_id = $2 AND org_id = $3 AND sent_message_id IS NULL`,
			scheduledID, actor.UserID, actor.OrgID)
		if err != nil {
			return apperr.Internal("cancel scheduled", err)
		}
		if ct.RowsAffected() == 0 {
			return apperr.NotFound("scheduled message not found")
		}
		if s.files == nil {
			return nil
		}
		return s.files.ReleaseEntityReferences(ctx, tx, enum.EntityScheduled, scheduledID)
	})
}

// ListScheduled returns the actor's undelivered sends — pending AND failed
// (failures must be seen, not swallowed), soonest first.
func (s *Service) ListScheduled(ctx context.Context, actor auth.Identity) ([]ScheduledMessage, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, thread_id, channel_id, dm_space_id, source, scheduled_for, failed_reason
		FROM scheduled_message
		WHERE author_id = $1 AND org_id = $2 AND sent_message_id IS NULL
		ORDER BY scheduled_for, id LIMIT 200`, actor.UserID, actor.OrgID)
	if err != nil {
		return nil, apperr.Internal("list scheduled", err)
	}
	defer rows.Close()
	out := []ScheduledMessage{}
	for rows.Next() {
		var m ScheduledMessage
		if err := rows.Scan(&m.ID, &m.ThreadID, &m.ChannelID, &m.DMSpaceID,
			&m.Source, &m.ScheduledFor, &m.FailedReason); err != nil {
			return nil, apperr.Internal("scan scheduled", err)
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// RunDueScheduled claims and delivers every due send (the injected clock
// keeps tests real-time-free). One transaction per row: the FOR UPDATE
// SKIP LOCKED claim makes concurrent runners safe; the delivery runs in a
// savepoint so a gate failure (revoked access, archived channel) records
// failed_reason on the still-locked row instead of rolling the claim back.
func (s *Service) RunDueScheduled(ctx context.Context, now time.Time) (sent, failed int, err error) {
	for {
		var advanced bool
		err = db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
			var id, orgID, authorID, threadID int64
			var source string
			err := tx.QueryRow(ctx, `
				SELECT id, org_id, author_id, thread_id, source
				FROM scheduled_message
				WHERE scheduled_for <= $1 AND sent_message_id IS NULL AND failed_reason IS NULL
				ORDER BY scheduled_for, id LIMIT 1
				FOR UPDATE SKIP LOCKED`, now).
				Scan(&id, &orgID, &authorID, &threadID, &source)
			if err == pgx.ErrNoRows {
				return nil
			}
			if err != nil {
				return apperr.Internal("claim scheduled", err)
			}
			advanced = true
			actor := auth.Identity{UserID: authorID, OrgID: orgID}
			sp, err := tx.Begin(ctx)
			if err != nil {
				return apperr.Internal("savepoint", err)
			}
			msgID, deliverErr := s.deliverToThread(ctx, sp, actor, threadID, source)
			if deliverErr != nil {
				_ = sp.Rollback(ctx)
				if _, err := tx.Exec(ctx, `
					UPDATE scheduled_message SET failed_reason = $2 WHERE id = $1`,
					id, apperr.ClientMessage(deliverErr)); err != nil {
					return apperr.Internal("mark failed", err)
				}
				failed++
				return nil
			}
			if err := sp.Commit(ctx); err != nil {
				return apperr.Internal("step commit", err)
			}
			if _, err := tx.Exec(ctx, `
				UPDATE scheduled_message SET sent_message_id = $2 WHERE id = $1`,
				id, msgID); err != nil {
				return apperr.Internal("mark sent", err)
			}
			if s.files != nil {
				if err := s.files.ReleaseEntityReferences(ctx, tx, enum.EntityScheduled, id); err != nil {
					return err
				}
			}
			sent++
			return nil
		})
		if err != nil || !advanced {
			return sent, failed, err
		}
	}
}

// RunScheduledLoop delivers due sends until ctx ends.
func (s *Service) RunScheduledLoop(ctx context.Context, log *slog.Logger) {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if _, failed, err := s.RunDueScheduled(ctx, time.Now()); err != nil && ctx.Err() == nil {
				log.Warn("scheduled: run failed", "err", err)
			} else if failed > 0 {
				log.Info("scheduled: deliveries failed", "count", failed)
			}
		}
	}
}
