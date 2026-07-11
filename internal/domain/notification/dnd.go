package notification

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

// DND snooze + VIP pierce (ADR-011 N-2). The notification module owns
// dnd_setting and priority_contact because they gate delivery: a snooze
// suppresses live pings and offline emails (never the in-app row — N-4: the
// badge accrues regardless), and a VIP (priority_contact) pierces the snooze
// for messages whose actor the recipient has listed. The dnd_setting.schedule
// column (weekly quiet hours) stays DORMANT this slice — it needs per-user
// timezones before it can be honored, so we neither parse nor expose it. DND
// and VIP changes are not event-logged (per-user settings, read-state
// precedent).

const (
	maxSnoozeHorizon = 30 * 24 * time.Hour
	maxVIPs          = 50
)

// SetDND upserts the actor's snooze. A nil time clears it; a set time must be
// in the future and at most 30 days out.
func (s *Service) SetDND(ctx context.Context, actor auth.Identity, snoozedUntil *time.Time) error {
	if snoozedUntil != nil {
		now := time.Now()
		if !snoozedUntil.After(now) {
			return apperr.Invalid("snoozed_until must be in the future")
		}
		if snoozedUntil.After(now.Add(maxSnoozeHorizon)) {
			return apperr.Invalid("snoozed_until must be within 30 days")
		}
	}
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO dnd_setting (user_id, snoozed_until) VALUES ($1, $2)
		ON CONFLICT (user_id) DO UPDATE SET snoozed_until = EXCLUDED.snoozed_until`,
		actor.UserID, snoozedUntil); err != nil {
		return apperr.Internal("set dnd", err)
	}
	return nil
}

// GetDND returns the actor's snooze time, nil when unset (or never set).
func (s *Service) GetDND(ctx context.Context, actor auth.Identity) (*time.Time, error) {
	var until *time.Time
	err := s.pool.QueryRow(ctx,
		`SELECT snoozed_until FROM dnd_setting WHERE user_id = $1`, actor.UserID).Scan(&until)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, apperr.Internal("get dnd", err)
	}
	return until, nil
}

// SetVIPs replaces the actor's priority-contact list (replace-set, like alert
// words). The actor's own id is silently dropped — a self-VIP is pointless —
// and duplicates collapse. Every remaining id must be a live member of the
// actor's org; a single count query gates the whole set and any mismatch is a
// 404 (oracle-free: a foreign or nonexistent id is indistinguishable from a
// deactivated one). Capped at 50. Returns the stored list.
func (s *Service) SetVIPs(ctx context.Context, actor auth.Identity, userIDs []int64) ([]int64, error) {
	seen := map[int64]bool{}
	clean := make([]int64, 0, len(userIDs))
	for _, uid := range userIDs {
		if uid == actor.UserID || seen[uid] {
			continue
		}
		seen[uid] = true
		clean = append(clean, uid)
	}
	if len(clean) > maxVIPs {
		return nil, apperr.Invalid("too many priority contacts (max 50)")
	}
	err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if len(clean) > 0 {
			var live int
			if err := tx.QueryRow(ctx, `
				SELECT count(*) FROM user_account
				WHERE org_id = $1 AND id = ANY($2) AND deactivated_at IS NULL`,
				actor.OrgID, clean).Scan(&live); err != nil {
				return apperr.Internal("validate priority contacts", err)
			}
			if live != len(clean) {
				return apperr.NotFound("one or more priority contacts not found")
			}
		}
		if _, err := tx.Exec(ctx,
			`DELETE FROM priority_contact WHERE user_id = $1`, actor.UserID); err != nil {
			return apperr.Internal("clear priority contacts", err)
		}
		for _, uid := range clean {
			if _, err := tx.Exec(ctx,
				`INSERT INTO priority_contact (user_id, contact_id) VALUES ($1, $2)`,
				actor.UserID, uid); err != nil {
				return apperr.Internal("store priority contact", err)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return clean, nil
}

// ListVIPs returns the actor's priority contacts, ascending by id.
func (s *Service) ListVIPs(ctx context.Context, actor auth.Identity) ([]int64, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT contact_id FROM priority_contact WHERE user_id = $1 ORDER BY contact_id`,
		actor.UserID)
	if err != nil {
		return nil, apperr.Internal("list priority contacts", err)
	}
	defer rows.Close()
	out := []int64{}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, apperr.Internal("scan priority contact", err)
		}
		out = append(out, id)
	}
	return out, rows.Err()
}
