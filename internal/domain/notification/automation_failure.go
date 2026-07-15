package notification

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5"

	"github.com/abhinavjha0239/weft/internal/enum"
)

// Automation-failure notifications (P-25). When an automation run fails, its
// scope's admins get a kind-6 in-app row (email default OFF — the doorbell,
// while the failure detail lives in the run's step trace via ListRuns). The
// notification module owns the write because it owns the notification table
// and the DND gate; the automation runner resolves the recipients and calls
// in during its finish transaction.

// RecordAutomationFailure inserts a kind-6 notification for each recipient
// (entity = the automation_run) in the caller's transaction. The dedupe key
// (user, kind, entity_type, entity_id) makes a re-execution replay a no-op and
// caps it at one row per admin per failed run. It returns the users actually
// inserted (conflicting rows are skipped) so the caller pings exactly those
// after the tx commits.
func (s *Service) RecordAutomationFailure(ctx context.Context, tx pgx.Tx, orgID, runID int64, recipients []int64) ([]int64, error) {
	rows, err := tx.Query(ctx, `
		INSERT INTO notification (org_id, user_id, kind, entity_type, entity_id, actor_id)
		SELECT $1, u, $2, $3, $4, NULL
		FROM unnest($5::bigint[]) AS u
		ON CONFLICT (user_id, kind, entity_type, entity_id) DO NOTHING
		RETURNING user_id`,
		orgID, int16(KindAutomationFailure), int16(enum.EntityAutomationRun), runID, recipients)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var inserted []int64
	for rows.Next() {
		var uid int64
		if err := rows.Scan(&uid); err != nil {
			return nil, err
		}
		inserted = append(inserted, uid)
	}
	return inserted, rows.Err()
}

// PingNotification delivers a live in-app ping for a row materialized outside
// the message pipeline (e.g. an automation-failure alert) — DND-gated exactly
// like the materializer's own ping, with a system actor (0) that earns no VIP
// pierce, so a snoozed admin is not live-pinged. The payload matches the
// materializer shape {kind, entity_type, entity_id, actor_id} (thread_id
// omitted). Best-effort: the notification ROW is the durable truth an offline
// admin reads on its next fetch.
func (s *Service) PingNotification(ctx context.Context, orgID, userID int64, kind, entityType int16, entityID int64) {
	if s.fan == nil {
		return
	}
	suppressed, err := dndSuppressed(ctx, s.pool, userID, 0)
	if err != nil || suppressed {
		return
	}
	payload, _ := json.Marshal(map[string]any{
		"kind": kind, "entity_type": entityType, "entity_id": entityID, "actor_id": 0,
	})
	s.fan.NotifyUser(ctx, orgID, userID, payload)
}
