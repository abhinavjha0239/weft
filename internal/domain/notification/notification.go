// Package notification is the first slice of the ADR-011 pipeline: a named
// event-log consumer materializes in-app notifications for the two reasons
// whose data already rides every message event — direct mentions and DMs
// (N-1 step 1). Muting layers, medium preferences, and the scheme matrix
// arrive as later steps of the same pipeline; today every materialized
// notification is in-app and unsuppressed.
package notification

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

// Reason classes (notification.kind), in the schema comment's order
// (dm/mention/followed/keyword/item-event); channel activity extends it.
const (
	KindDM              = 1
	KindMention         = 2
	KindFollowedThread  = 3
	KindKeyword         = 4 // alert words (N-1 kind 4)
	KindChannelActivity = 5
)

type Service struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Service { return &Service{pool: pool} }

type Notification struct {
	ID         int64     `json:"id"`
	Kind       int16     `json:"kind"`
	EntityType int16     `json:"entity_type"`
	EntityID   int64     `json:"entity_id"`
	ActorID    *int64    `json:"actor_id,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
	Seen       bool      `json:"seen"`
}

type Inbox struct {
	Notifications []Notification `json:"notifications"`
	Unseen        int            `json:"unseen"`
}

// List returns the actor's newest notifications plus their unseen count
// (the badge number — served by the partial unseen index). Both reads run
// on ONE snapshot: the badge count must never disagree with the page it
// ships with (the materializer runs concurrently — CI caught the skew).
func (s *Service) List(ctx context.Context, actor auth.Identity, limit int) (Inbox, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	out := Inbox{Notifications: []Notification{}}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return out, apperr.Internal("inbox snapshot", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, `
		SELECT id, kind, entity_type, entity_id, actor_id, created_at,
		       seen_at IS NOT NULL
		FROM notification
		WHERE org_id = $1 AND user_id = $2
		ORDER BY id DESC
		LIMIT $3`, actor.OrgID, actor.UserID, limit)
	if err != nil {
		return out, apperr.Internal("list notifications", err)
	}
	defer rows.Close()
	for rows.Next() {
		var n Notification
		if err := rows.Scan(&n.ID, &n.Kind, &n.EntityType, &n.EntityID,
			&n.ActorID, &n.CreatedAt, &n.Seen); err != nil {
			return out, apperr.Internal("scan notification", err)
		}
		out.Notifications = append(out.Notifications, n)
	}
	if err := rows.Err(); err != nil {
		return out, err
	}
	rows.Close()
	if err := tx.QueryRow(ctx, `
		SELECT count(*) FROM notification
		WHERE user_id = $1 AND seen_at IS NULL`,
		actor.UserID).Scan(&out.Unseen); err != nil {
		return out, apperr.Internal("unseen count", err)
	}
	return out, nil
}

// MarkSeen stamps everything up to upTo (0 = everything) — the badge-clear
// action. read_at (per-item engagement) is a later slice.
func (s *Service) MarkSeen(ctx context.Context, actor auth.Identity, upTo int64) error {
	q := `UPDATE notification SET seen_at = now()
	      WHERE user_id = $1 AND seen_at IS NULL`
	args := []any{actor.UserID}
	if upTo > 0 {
		q += ` AND id <= $2`
		args = append(args, upTo)
	}
	if _, err := s.pool.Exec(ctx, q, args...); err != nil {
		return apperr.Internal("mark seen", err)
	}
	return nil
}
