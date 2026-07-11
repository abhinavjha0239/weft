package notification

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

// DM breakthrough (ADR-011 N-2): a DM sender's one "notify anyway" per
// recipient per UTC day. When a recipient has snoozed (DND) and the sender is
// not on their VIP list, the sender's DM still lands as an in-app row but the
// live ping is withheld (Runner.dndSuppressed, N-4: the badge accrues
// regardless). Breakthrough lets the sender spend a single daily allowance to
// re-deliver that suppressed ping — the row already exists, so this only
// re-fans it over the live seam, bypassing the DND gate (that is the point).
// No email side (the pending digest rides the normal post-snooze sweep) and no
// event (a personal signal, symmetric with the DND/VIP precedent). The
// allowance is the dm_breakthrough PK (sender, recipient, used_on), and it is
// spent ONLY after the "recipient is snoozed" and "something is pending"
// checks pass, so a no-op call never burns the day's use.

// BreakthroughResult reports which pending notification was re-delivered.
type BreakthroughResult struct {
	MessageID int64 `json:"message_id"`
	ThreadID  int64 `json:"thread_id"`
}

// Breakthrough spends the actor's daily allowance to re-ping one snoozed
// recipient in a 1:1 conversation. dmSpaceID identifies the conversation; the
// actor must be a participant (nonexistent and non-participant are both an
// oracle-free 404). Group and self conversations are rejected (400): the
// allowance is keyed per single recipient, so it only makes sense 1:1.
func (s *Service) Breakthrough(ctx context.Context, actor auth.Identity, dmSpaceID int64) (BreakthroughResult, error) {
	var out BreakthroughResult
	var recipientID int64
	err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		// Oracle-free container gate: kind comes back only when the actor is a
		// participant; otherwise the conversation is indistinguishably absent.
		var kind int16
		err := tx.QueryRow(ctx, `
			SELECT ds.kind FROM dm_space ds
			WHERE ds.id = $1 AND ds.org_id = $2
			  AND EXISTS (SELECT 1 FROM dm_participant dp
			              WHERE dp.dm_space_id = ds.id AND dp.user_id = $3)`,
			dmSpaceID, actor.OrgID, actor.UserID).Scan(&kind)
		if errors.Is(err, pgx.ErrNoRows) {
			return apperr.NotFound("conversation not found")
		}
		if err != nil {
			return apperr.Internal("load conversation", err)
		}
		switch kind {
		case 2:
			return apperr.Invalid("breakthrough is for direct conversations")
		case 3:
			return apperr.Invalid("breakthrough is not available in a self-conversation")
		}
		// A 1:1 conversation has exactly one other participant — the recipient.
		if err := tx.QueryRow(ctx, `
			SELECT dp.user_id FROM dm_participant dp
			WHERE dp.dm_space_id = $1 AND dp.user_id <> $2`,
			dmSpaceID, actor.UserID).Scan(&recipientID); err != nil {
			return apperr.Internal("resolve recipient", err)
		}

		// The recipient must currently be snoozed, else there is nothing to
		// break through — and the daily use is NOT burned.
		var snoozed bool
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS (SELECT 1 FROM dnd_setting
			 WHERE user_id = $1 AND snoozed_until > now())`,
			recipientID).Scan(&snoozed); err != nil {
			return apperr.Internal("check recipient dnd", err)
		}
		if !snoozed {
			return apperr.Conflict("recipient is not in do-not-disturb")
		}

		// The most recent unseen DM notification (kind 1) from this sender in
		// THIS conversation is what we re-ping; none → nothing pending and the
		// use is NOT burned. The join pins the message to the conversation
		// (notification rows carry only entity_type=message + entity_id).
		err = tx.QueryRow(ctx, `
			SELECT n.entity_id, m.thread_id
			FROM notification n
			JOIN message m ON m.id = n.entity_id AND m.org_id = n.org_id
			WHERE n.org_id = $1 AND n.user_id = $2 AND n.kind = $3
			  AND n.actor_id = $4 AND n.seen_at IS NULL
			  AND m.dm_space_id = $5
			ORDER BY n.id DESC
			LIMIT 1`,
			actor.OrgID, recipientID, int16(KindDM), actor.UserID, dmSpaceID).
			Scan(&out.MessageID, &out.ThreadID)
		if errors.Is(err, pgx.ErrNoRows) {
			return apperr.Conflict("nothing pending to break through")
		}
		if err != nil {
			return apperr.Internal("find pending notification", err)
		}

		// Spend the allowance. The date boundary is UTC current_date (not the
		// server session zone): the PK (sender, recipient, used_on) makes a
		// second same-day call conflict → 409, while a new UTC day is a fresh
		// row (tests backdate used_on to prove the next-day path).
		ct, err := tx.Exec(ctx, `
			INSERT INTO dm_breakthrough (sender_id, recipient_id, used_on)
			VALUES ($1, $2, (now() AT TIME ZONE 'UTC')::date)
			ON CONFLICT DO NOTHING`,
			actor.UserID, recipientID)
		if err != nil {
			return apperr.Internal("consume breakthrough", err)
		}
		if ct.RowsAffected() == 0 {
			return apperr.Conflict("breakthrough already used today")
		}
		return nil
	})
	if err != nil {
		return BreakthroughResult{}, err
	}
	// Re-fan the suppressed ping over the live seam, bypassing the DND gate —
	// the whole point of breakthrough. Best-effort (the row is the truth); the
	// payload mirrors Runner.insert's live ping so clients handle it uniformly.
	if s.fan != nil {
		payload, _ := json.Marshal(map[string]any{
			"kind": int16(KindDM), "entity_type": int16(enum.EntityMessage),
			"entity_id": out.MessageID, "thread_id": out.ThreadID,
			"actor_id": actor.UserID,
		})
		s.fan.NotifyUser(ctx, actor.OrgID, recipientID, payload)
	}
	return out, nil
}
