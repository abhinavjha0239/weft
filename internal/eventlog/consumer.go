package eventlog

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/enum"
)

// Consumer reads an org's events in id order with a durable cursor
// (event_consumer_cursor, ADR-003 E2: each consumer tracks its own position;
// replay = reset cursor).
//
// The WHERE clause is the load-bearing line (F-1): identity ids are assigned
// at insert but commit in arbitrary order, so the scan is bounded by the
// oldest in-flight transaction. A row whose transaction hasn't committed can
// never be jumped over — the cursor waits for it.
//
// SCALE CONTRACT (docs/SCHEMA.md "Scale contract"):
//   - The xmin gate is DATABASE-GLOBAL: one long-running transaction anywhere
//     stalls delivery for every org. Writers run with short transactions and
//     idle_in_transaction_session_timeout; analytics belong on replicas. The
//     scale-tier replacement (same Consumer interface) is a logical-decoding
//     feed, where WAL order = commit order and no gate is needed.
//   - Poll is per-org because the org is the ordering domain — but the
//     RUNTIME must be NOTIFY-driven: a dispatcher LISTENs and schedules polls
//     only for orgs with pending signals (plus a slow sweep). Idle orgs cost
//     zero; never run 100k tickers.
type Consumer struct {
	pool  *pgxpool.Pool
	name  string
	batch int
}

// NewConsumer creates a named cursor-tracked consumer. Names are stable
// identifiers ("gateway", "search", "automations") — renaming one means
// starting over from id 0.
func NewConsumer(pool *pgxpool.Pool, name string, batchSize int) *Consumer {
	if batchSize <= 0 {
		batchSize = 500
	}
	return &Consumer{pool: pool, name: name, batch: batchSize}
}

// Poll returns the next batch after the cursor, respecting the txid gate.
// An empty result means either nothing new or older transactions still in
// flight — callers just poll again (or wait for NOTIFY).
func (c *Consumer) Poll(ctx context.Context, orgID int64) ([]Row, error) {
	rows, err := c.pool.Query(ctx, `
		SELECT e.id, e.org_id, e.workspace_id, e.actor_kind, e.actor_id,
		       e.entity_type, e.entity_id, e.verb, e.payload, e.hint,
		       e.occurred_at, e.recorded_at, e.origin
		FROM event_log e
		WHERE e.org_id = $1
		  AND e.id > COALESCE((
		        SELECT last_id FROM event_consumer_cursor
		        WHERE consumer = $2 AND org_id = $1), 0)
		  AND e.txid < pg_snapshot_xmin(pg_current_snapshot())
		ORDER BY e.id
		LIMIT $3`,
		orgID, c.name, c.batch)
	if err != nil {
		return nil, fmt.Errorf("eventlog: poll: %w", err)
	}
	defer rows.Close()

	var out []Row
	for rows.Next() {
		var r Row
		var actorKind, entityType int16
		if err := rows.Scan(&r.ID, &r.OrgID, &r.WorkspaceID, &actorKind,
			&r.ActorID, &entityType, &r.EntityID, &r.Verb, &r.Payload,
			&r.Hint, &r.OccurredAt, &r.RecordedAt, &r.Origin); err != nil {
			return nil, fmt.Errorf("eventlog: scan: %w", err)
		}
		r.ActorKind = enum.ActorKind(actorKind)
		r.EntityType = enum.EntityType(entityType)
		out = append(out, r)
	}
	return out, rows.Err()
}

// Ack durably advances the cursor. Consumers must be idempotent anyway
// (at-least-once delivery): a crash between processing and Ack replays.
func (c *Consumer) Ack(ctx context.Context, orgID, lastID int64) error {
	_, err := c.pool.Exec(ctx, `
		INSERT INTO event_consumer_cursor (consumer, org_id, last_id, updated_at)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (consumer, org_id)
		DO UPDATE SET last_id = GREATEST(event_consumer_cursor.last_id, EXCLUDED.last_id),
		              updated_at = now()`,
		c.name, orgID, lastID)
	if err != nil {
		return fmt.Errorf("eventlog: ack: %w", err)
	}
	return nil
}
