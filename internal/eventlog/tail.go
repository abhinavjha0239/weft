package eventlog

import (
	"context"
	"fmt"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Position is a driver-opaque DELIVERY position: "everything up to here has
// already been handed over". The xmin driver counts in EVENT IDS, the logical
// driver in COMMIT LSNs. A caller holds one, passes it back, and never
// interprets it — which is what lets the gateway keep its cursor in memory
// while the meaning of "next" changes underneath it.
type Position struct {
	id  int64         // xmin driver: the last delivered event id
	lsn pglogrepl.LSN // logical driver: the last delivered commit LSN
}

// Tail is the EPHEMERAL half of the driver seam — a CURSOR-FREE reader for a
// consumer that holds no durable state. Feed is the durable half: its position
// lives in event_consumer_cursor, shared by every node of the cell, and
// advancing it means "nobody needs this event again". The gateway cannot use
// that: ADR-002 gives it no durable state (a client resumes by replaying the
// log with WHERE id > seq), and every gateway NODE must see every event, so a
// shared acked cursor would be exactly wrong. It gets this instead: the same
// two drivers, the same commit-order guarantee, a position it keeps in memory
// and throws away when the last connection leaves.
//
// Contract, identical for every driver:
//   - Head is "everything committed for this org right now".
//   - Next returns committed events after a position, in the driver's DELIVERY
//     order, and the position to pass back. An empty batch means "nothing new
//     right now", never "you are done forever".
//   - History replays in EVENT-ID order, because a client's resume cursor is
//     an event id (ADR-002 F-2), not a driver position.
//   - Ordered says whether the delivery order is event-id monotone, which is
//     the ONE thing a caller may not assume: see below.
type Tail interface {
	// Head is the org's current live position PLUS the highest event id
	// committed at it. A fresh gateway shard seeds its reader from the
	// position (so it only ever reads NEW events) and resolves tail mode
	// (last_id < 0) from the id.
	//
	// The two are sampled in the one order that cannot lose an event: the
	// POSITION first, the id second. An event committing between the samples
	// then sits ABOVE the position (so Next delivers it) and at or below the
	// id (so a tail-mode client may see it as a duplicate) — duplicate, not
	// drop. Sampling the id first inverts that: such an event would be above
	// the id (tail mode skips it) and below the position (Next never returns
	// it), i.e. a silent drop.
	Head(ctx context.Context, orgID int64) (pos Position, headID int64, err error)

	// Next returns up to limit events committed strictly after pos, in the
	// driver's DELIVERY order, with the position to pass back next. The
	// logical driver may overshoot limit to finish a commit (batches cut on
	// commit boundaries), so callers treat limit as a floor.
	Next(ctx context.Context, orgID int64, pos Position, limit int) ([]Row, Position, error)

	// History replays an org's log in EVENT-ID order after afterID — the
	// resume lane. The xmin driver applies its visibility gate here (its live
	// lane cannot deliver a late-committing lower id, so the gate is the only
	// thing standing between it and a skip); the logical driver does not,
	// because its live lane DOES deliver that event, which is the whole point
	// of this slice. Same rule, same reason, as logicalConsumer's bootstrap.
	History(ctx context.Context, orgID, afterID int64, limit int) ([]Row, error)

	// OnWake registers a callback the driver invokes, with an org id, when
	// that org's newly committed events have become READABLE through Next.
	// May be called more than once; every registered callback fires. The
	// callback runs on the driver's own goroutine, so it must not block.
	//
	// This exists because LISTEN/NOTIFY cannot serve the role under a
	// streaming driver, and the failure is quiet rather than obvious. Append's
	// notification fires at COMMIT; the events only become readable when the
	// WAL reader decodes that commit a moment later. A consumer woken by the
	// notification therefore reliably reads NOTHING, and then waits for its
	// slow fallback sweep — turning a feed whose whole point is lower latency
	// into a multi-second one, with no error anywhere (measured on the gateway
	// pin: 22.7s → 6.4s for the same test when the sweep was shortened).
	// The reader is the only thing that knows when an event is readable, so
	// the reader is what wakes.
	//
	// The xmin driver registers nothing: its rows are readable exactly when
	// the notification fires, which is what its callers already use.
	OnWake(fn func(orgID int64))

	// Ordered reports whether this feed's delivery order is EVENT-ID MONOTONE.
	//
	// The xmin driver says TRUE: it scans ORDER BY id behind a visibility
	// gate, so nothing at or below a delivered id can still arrive, and "id <=
	// what I last saw" is an exact, unbounded "already delivered" test.
	//
	// The logical driver says FALSE, and this is the load-bearing bit. txid is
	// stamped at a transaction's first write and the id at append, so a
	// transaction can hold the LOWER id and commit LAST; WAL order is commit
	// order, so that lower id legitimately arrives after a higher one. A
	// caller that reads Ordered as true anyway does not merely reorder — it
	// DROPS that event, silently, which is the failure logical.go exists to
	// remove. Callers must track what they have ACTUALLY delivered.
	Ordered() bool
}

// NewTail returns the DEFAULT (xmin) ephemeral tail: the same visibility gate
// the default Consumer uses, and byte-for-byte the read the gateway has always
// run. It is the NewConsumer twin — a component's default, before an operator
// swaps the driver in via Source.Tail.
func NewTail(pool *pgxpool.Pool) Tail { return &xminTail{pool: pool} }

// Tail returns this driver's ephemeral reader.
func (s *xminSource) Tail() Tail { return &xminTail{pool: s.pool} }

// xminTail is the default driver's ephemeral reader: one gated scan, shared by
// both lanes so the live read and the resume replay can never drift apart.
type xminTail struct{ pool *pgxpool.Pool }

func (t *xminTail) Ordered() bool { return true }

// OnWake is a no-op: a polled row is readable the moment its transaction
// commits, which is exactly when Append's NOTIFY fires, so this driver's
// callers are already woken at the right instant.
func (t *xminTail) OnWake(func(int64)) {}

func (t *xminTail) Head(ctx context.Context, orgID int64) (Position, int64, error) {
	var id int64
	err := t.pool.QueryRow(ctx,
		`SELECT COALESCE(MAX(id), 0) FROM event_log WHERE org_id = $1`, orgID).Scan(&id)
	if err != nil {
		return Position{}, 0, fmt.Errorf("eventlog: tail head: %w", err)
	}
	// Position and id are the same number here, so the sampling-order rule in
	// the interface doc is trivially satisfied: ONE statement, one snapshot.
	return Position{id: id}, id, nil
}

func (t *xminTail) Next(ctx context.Context, orgID int64, pos Position, limit int) ([]Row, Position, error) {
	rows, err := t.scan(ctx, orgID, pos.id, limit)
	if err != nil {
		return nil, pos, err
	}
	if len(rows) > 0 {
		pos = Position{id: rows[len(rows)-1].ID}
	}
	return rows, pos, nil
}

func (t *xminTail) History(ctx context.Context, orgID, afterID int64, limit int) ([]Row, error) {
	return t.scan(ctx, orgID, afterID, limit)
}

// scan is the gated read both lanes share (the F-1 gate, same predicate as
// Consumer.Poll): a scan never passes an id whose transaction was still in
// flight. It does not close the whole skip class — see Consumer's scale
// contract — which is why the logical driver exists.
func (t *xminTail) scan(ctx context.Context, orgID, afterID int64, limit int) ([]Row, error) {
	rows, err := t.pool.Query(ctx, `
		SELECT e.id, e.org_id, e.workspace_id, e.actor_kind, e.actor_id,
		       e.entity_type, e.entity_id, e.verb, e.payload, e.hint,
		       e.occurred_at, e.recorded_at, e.origin
		FROM event_log e
		WHERE e.org_id = $1 AND e.id > $2
		  AND e.txid < pg_snapshot_xmin(pg_current_snapshot())
		ORDER BY e.id
		LIMIT $3`, orgID, afterID, limit)
	if err != nil {
		return nil, fmt.Errorf("eventlog: tail scan: %w", err)
	}
	return scanRows(rows)
}

// Tail returns the logical driver's ephemeral reader.
func (s *LogicalSource) Tail() Tail { return &logicalTail{src: s} }

// logicalTail serves the in-memory commit-order index built by the WAL reader.
// It holds no cursor of its own: the caller's Position is the only state.
type logicalTail struct{ src *LogicalSource }

// Ordered is false: commit order is not id order. See the Tail doc.
func (t *logicalTail) Ordered() bool { return false }

// OnWake rides the WAL reader: it fires when a commit has been DECODED, which
// is the first instant its events can be read back through Next.
func (t *logicalTail) OnWake(fn func(orgID int64)) { t.src.AddWake(fn) }

func (t *logicalTail) Head(ctx context.Context, orgID int64) (Position, int64, error) {
	// The LSN FIRST (see the Head contract): a reader that is not streaming
	// says so instead of handing out a position that means nothing.
	lsn, ready := t.src.Head()
	if !ready {
		return Position{}, 0, ErrFeedNotReady
	}
	var id int64
	err := t.src.pool.QueryRow(ctx,
		`SELECT COALESCE(MAX(id), 0) FROM event_log WHERE org_id = $1`, orgID).Scan(&id)
	if err != nil {
		return Position{}, 0, fmt.Errorf("eventlog: tail head: %w", err)
	}
	return Position{lsn: lsn}, id, nil
}

func (t *logicalTail) Next(ctx context.Context, orgID int64, pos Position, limit int) ([]Row, Position, error) {
	ents, err := t.src.after(orgID, pos.lsn, limit)
	if err != nil {
		return nil, pos, err
	}
	if len(ents) == 0 {
		return nil, pos, nil
	}
	ids := make([]int64, len(ents))
	for i, e := range ents {
		ids[i] = e.id
	}
	byID, err := t.src.readRows(ctx, orgID, ids)
	if err != nil {
		return nil, pos, err
	}
	out := make([]Row, 0, len(ents))
	for _, e := range ents {
		r, ok := byID[e.id]
		if !ok {
			// The row is gone (a retention partition drop between decode and
			// read). Skip the body, still advance past its commit.
			continue
		}
		out = append(out, r)
	}
	// The position advances past the WHOLE window even when bodies vanished,
	// so an ephemeral reader can never spin on a dead span.
	return out, Position{lsn: ents[len(ents)-1].lsn}, nil
}

func (t *logicalTail) History(ctx context.Context, orgID, afterID int64, limit int) ([]Row, error) {
	// UNGATED, exactly like the bootstrap lane: this is a replay of history a
	// client asked for by id, and the gate would only reimport the global
	// stall this driver exists to remove. An event that was in flight during
	// the walk arrives on the LIVE lane instead, where a lower id is delivered
	// rather than skipped.
	return t.src.historyRows(ctx, orgID, afterID, limit)
}
