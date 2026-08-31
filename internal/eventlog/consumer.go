package eventlog

import (
	"context"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/internal/platform/metrics"
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
//     idle_in_transaction_session_timeout; analytics belong on replicas. It is
//     a DELIVERY gate only: Lag deliberately measures WITHOUT it, because a
//     backlog measured through the horizon that caused the backlog reads 0.
//   - The gate does NOT close the whole skip class. txid is stamped at a
//     transaction's FIRST write, the id at APPEND, so a transaction with the
//     LOWER txid can hold the HIGHER id; when it commits first the gate admits
//     it, the cursor passes a lower id still in flight, and that event is
//     never delivered (TestLogicalFeedNoCommitOrderSkip demonstrates it).
//   - Both are why the logical-decoding feed exists (S4, logical.go): WAL
//     order = commit order, so no gate is needed and the crossing cannot
//     happen. It is a driver behind the same Feed interface
//     (WEFT_EVENT_FEED_DRIVER=logical); this poller stays the DEFAULT, so a
//     small install needs no replication slot.
//   - Poll is per-org because the org is the ordering domain — but the
//     RUNTIME must be NOTIFY-driven: a dispatcher LISTENs and schedules polls
//     only for orgs with pending signals (plus a slow sweep). Idle orgs cost
//     zero; never run 100k tickers.
type Consumer struct {
	pool  *pgxpool.Pool
	name  string
	batch int
	// Metrics are optional (default Nop): events counts what this consumer
	// pulls; lag publishes its backlog. Set once at wiring, before Poll runs.
	events metrics.Counter
	lag    metrics.Gauge
}

// NewConsumer creates a named cursor-tracked consumer. Names are stable
// identifiers ("gateway", "search", "automations") — renaming one means
// starting over from id 0.
func NewConsumer(pool *pgxpool.Pool, name string, batchSize int) *Consumer {
	if batchSize <= 0 {
		batchSize = 500
	}
	c := &Consumer{pool: pool, name: name, batch: batchSize}
	c.SetMetrics(metrics.Nop())
	return c
}

// SetMetrics wires an observability registry (S0). Optional — the default is
// Nop, so an un-instrumented consumer pays nothing. Call once before Poll.
func (c *Consumer) SetMetrics(reg metrics.Registry) {
	c.events = reg.Counter("fanout_events_total", "consumer")
	c.lag = reg.Gauge("consumer_lag", "consumer", "org")
}

// OnWake is a no-op, and that is the whole reason the DEFAULT driver's
// behaviour is unchanged by the push wake: a polled row is readable the moment
// its transaction commits, which is exactly when Append's NOTIFY fires, so
// this driver's callers are already woken at the right instant and there is
// nothing for a second wake to add. (The logical driver is where the two
// instants differ — see Feed.OnWake.)
func (c *Consumer) OnWake(func(int64)) {}

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
	if err := rows.Err(); err != nil {
		return nil, err
	}
	c.events.Add(float64(len(out)), c.name)
	return out, nil
}

// Lag reports this consumer's backlog for one org — how far the durable cursor
// trails the org's newest COMMITTED event — and publishes it as the
// consumer_lag{consumer,org} gauge. Lag is THE health signal of the whole
// design (docs/ARCHITECTURE.md §6, docs/SCHEMA.md scale contract): it rises
// when a consumer falls behind and falls to 0 when it catches up. One query
// per (consumer, org).
//
// NO xmin GATE HERE, deliberately — this is the line between a health signal
// and a flat green line. Poll's `txid < pg_snapshot_xmin(...)` is a DELIVERY
// gate: it withholds committed rows that are not YET safe to hand over, and it
// is DATABASE-GLOBAL, so one long write transaction anywhere freezes it.
// Measuring the backlog through that same horizon made the numerator stop
// advancing at exactly the moment the cursor also stopped, so during the one
// failure this gauge exists to surface, it reported 0 while the backlog grew
// without bound (found by S4; TestConsumerLagSeesGlobalStall pins it). Ordinary
// MVCC visibility is the only filter a MEASUREMENT needs: MAX(e.id) on this
// statement's snapshot counts committed rows and only committed rows, so an
// in-flight transaction's event is still never counted (same test). Events that
// are committed but not yet deliverable now COUNT as lag, which is what they
// are — work this consumer owes and cannot yet do.
//
// Semantic change to a shipped series, accepted: under normal operation lag
// reads transiently higher than before, by whatever committed inside the window
// between a commit and the horizon moving past it. That is MORE CORRECT, not
// merely different — those events are undelivered, and a number that calls them
// delivered is wrong in the direction that hides outages. It is also the
// reading the logical driver has always published, so the series now means the
// same thing under both drivers.
//
// WHICH POSITION IT MEASURES AGAINST (the Feed contract is one number; the
// cursor underneath is not the same object per driver):
//   - xmin (here): event_consumer_cursor.last_id, in EVENT-ID space. The value
//     is an id delta — the exact undelivered count over an unbroken id run, an
//     upper bound on it when other orgs' events interleave in the shared
//     sequence. Deliberate: MAX(id) under event_log_org_consume_idx is an index
//     max, so the measurement stays cheap when the backlog is huge, which is
//     precisely when an operator needs it. (Dropping the gate makes it cheaper
//     too: txid is not in that index, so the gated form could not be an
//     index-only max.)
//   - logical: event_consumer_cursor.lsn, in COMMIT order — an exact count of
//     decoded-committed-unacked entries (logical.go), or of unread history rows
//     while a consumer is still in the bootstrap lane.
func (c *Consumer) Lag(ctx context.Context, orgID int64) (int64, error) {
	var lag int64
	err := c.pool.QueryRow(ctx, `
		SELECT COALESCE(MAX(e.id), 0) - COALESCE((
		        SELECT last_id FROM event_consumer_cursor
		        WHERE consumer = $2 AND org_id = $1), 0)
		FROM event_log e
		WHERE e.org_id = $1`,
		orgID, c.name).Scan(&lag)
	if err != nil {
		return 0, fmt.Errorf("eventlog: lag: %w", err)
	}
	c.lag.Set(float64(lag), c.name, strconv.FormatInt(orgID, 10))
	return lag, nil
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
