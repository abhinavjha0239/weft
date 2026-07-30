package eventlog

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// sweepLockClass is the advisory-lock CLASS every maintenance sweep claims
// under; a sweep's Key is the objid inside it. Postgres keeps the
// two-argument (classid, objid) advisory space DISJOINT from the
// one-argument bigint space, so nothing here can collide with worktrack's
// pg_advisory_xact_lock(space_id) — the tree's only other advisory-lock user
// (verified against pg_locks: the two forms differ in objsubid).
const sweepLockClass int32 = 0x53575000

// SweepID is the durable identity of one cell-wide maintenance sweep. Name is
// what an operator sees and what the sweep's per-org state is keyed by
// (sweep_org_state.sweep); Key is its objid inside sweepLockClass. Both are
// APPEND-ONLY, exactly like consumer names and enum values: renaming a sweep
// abandons its settled markers (every org reverts to "never verified", which
// is safe but costs one full pass), and reusing a Key silently serialises two
// unrelated sweeps against each other.
//
// Consumer names the event-log consumer whose progress the swept cache
// depends on. A sweep must not declare an org verified while that consumer is
// behind the org's head: the maintenance the cache is being checked against
// has not happened yet, so a "clean" verdict would freeze legitimate
// staleness. Both of today's sweeps ride the notifications consumer.
type SweepID struct {
	Name     string
	Key      int32
	Consumer string
}

// Sweeper gives a periodic maintenance sweep the multi-node exclusion every
// other background worker in this tree already has. The others (
// perms.RebuildWorker, messaging's scheduled sender, the notification email
// and push workers, the compliance export worker) claim with FOR UPDATE SKIP
// LOCKED on the ROW they are about to work. A sweep has no such row — it is a
// pass over everything — so the claim is a SESSION-level advisory lock taken
// with pg_try_advisory_lock: the same "one holder, every other caller moves
// on immediately" semantics, on a resource that is not a row.
//
// Why not the row-claim pattern here, given the standing preference for it:
// holding a row lock for the duration of a sweep means holding a TRANSACTION
// open for the duration of a sweep, and the scale contract (docs/SCHEMA.md)
// forbids precisely that — the default event feed is gated on
// pg_snapshot_xmin, so one long-lived transaction ANYWHERE stalls event
// delivery for every org in the cell. The row claim is safe for the other
// workers because their claim spans ONE short unit of work. A session
// advisory lock pins no snapshot and holds no transaction, so per-item short
// transactions and the savepoint discipline are unaffected, and the server
// drops the lock the instant the connection dies — a node that crashes
// mid-sweep frees its claim with no lease timer and no reaper row.
//
// DELIVERY SEMANTICS (the standing background-worker rule): AT-MOST-ONCE per
// window. A contended window is DROPPED, never queued — the loser returns
// cleanly and waits for its own next tick. That is the correct trade for a
// convergence sweep, where the two failure modes are not symmetric: a skipped
// window costs one more interval of drift on a cache that is itself only a
// backstop, while a doubled window costs a redundant full pass plus lock
// contention on the very channel/counter rows the live path is using.
type Sweeper struct {
	pool *pgxpool.Pool
	id   SweepID
}

// NewSweeper binds a sweep identity to a pool. Cheap; callers keep one per
// sweep for the process lifetime.
func NewSweeper(pool *pgxpool.Pool, id SweepID) *Sweeper {
	return &Sweeper{pool: pool, id: id}
}

// Claim takes this sweep's cell-wide exclusion. ok=false means another node
// (or another goroutine in this one) is mid-pass: the caller must return
// WITHOUT error and without doing any work — that is the at-most-once
// contract, not a failure.
//
// release must always be called when ok is true. It unlocks and only then
// returns the connection to the pool: pgxpool does NOT reset session state on
// release, so a connection handed back still holding the lock would wedge the
// sweep for the life of the process. The unlock deliberately runs on a
// non-cancellable context so a sweep torn down by shutdown still leaves a
// clean session behind.
func (s *Sweeper) Claim(ctx context.Context) (release func(), ok bool, err error) {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("eventlog: acquire sweep conn (%s): %w", s.id.Name, err)
	}
	var got bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1, $2)`,
		sweepLockClass, s.id.Key).Scan(&got); err != nil {
		conn.Release()
		return nil, false, fmt.Errorf("eventlog: claim sweep %s: %w", s.id.Name, err)
	}
	if !got {
		conn.Release()
		return nil, false, nil
	}
	return func() {
		uctx := context.WithoutCancel(ctx)
		if _, err := conn.Exec(uctx, `SELECT pg_advisory_unlock($1, $2)`,
			sweepLockClass, s.id.Key); err != nil {
			// An unlock only fails on a session this process can no longer
			// trust. Destroy it rather than return a possibly-still-locked
			// connection to the pool; closing the session releases the lock
			// server-side anyway.
			_ = conn.Hijack().Close(uctx)
			return
		}
		conn.Release()
	}, true, nil
}

// SettleTTL bounds how long a settled marker may suppress work: once an org's
// settle is older than this, the next pass walks it regardless of activity.
// The guarantee a sweep can therefore state in TIME rather than in window
// counts is
//
//	an idle org costs nothing for up to SettleTTL, and EVERY org is fully
//	verified at least once per SettleTTL no matter what it did.
//
// It exists because the activity signal is the event log, and not every write
// that can move a maintained cache appends an event: the settings legs of the
// deliverability set (channel level, thread follow, alert words) and the
// concurrent first-ever mark-read window (#118 drift 2) append none. Without
// an expiry, drift that entered an already-settled org through one of those
// and then went quiet would sit there INDEFINITELY, which is not a word this
// ledger may contain when the fix is one predicate. A day is the trade: at
// the hourly cadence it leaves ~23 of every 24 windows free for an idle org
// while holding worst-case repair latency to one day instead of "until
// somebody posts".
const SettleTTL = 24 * time.Hour

// OrgPass is one org's pass-start snapshot: everything a sweep needs to
// decide whether to walk the org at all, and what to record if the walk comes
// back clean.
type OrgPass struct {
	OrgID int64
	// HighWater is max(event_log.id) for the org AT PASS START. It is
	// deliberately read before the work rather than after: an event appended
	// mid-pass may land behind the item the pass already visited, so settling
	// on an end-of-pass mark could swallow it. Reading it first can only make
	// the next window redundant, never blind.
	HighWater int64
	// Settled is true when a previous pass verified this org clean at exactly
	// HighWater, RECENTLY ENOUGH (within SettleTTL) — nothing has happened in
	// the org since, and the forced-verification deadline has not arrived. The
	// sweep skips it.
	Settled bool
	// Lagging is true when the maintenance consumer has not reached
	// HighWater. The pass should still repair, but must not settle: the
	// maintenance it would be verifying has not run yet.
	Lagging bool
}

// Orgs returns every org with its pass-start snapshot — ONE query for the
// whole cell, all of it index work: the high-water mark per org rides
// event_log_org_consume_idx (org_id, id), the cursor and the settled marker
// are PK reads. Nothing here is a new write on any path; the activity signal
// is state the system already maintains.
//
// The staleness term is what turns a settled marker from a permanent excuse
// into a lease: a marker older than SettleTTL stops suppressing work, so a
// full verification happens at least that often for EVERY org. Age is
// measured in DATABASE time on both sides (settled_at is written with now()),
// so no node's clock can extend another node's lease.
func (s *Sweeper) Orgs(ctx context.Context) ([]OrgPass, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT o.id,
		       COALESCE(h.high, 0),
		       COALESCE(st.settled_event_id IS NOT NULL
		                AND st.settled_event_id = COALESCE(h.high, 0)
		                AND st.settled_at > now() - make_interval(
		                        secs => $3::double precision), false),
		       COALESCE(c.last_id, 0) < COALESCE(h.high, 0)
		FROM org o
		LEFT JOIN LATERAL (
		    SELECT max(e.id) AS high FROM event_log e WHERE e.org_id = o.id
		) h ON true
		LEFT JOIN sweep_org_state st ON st.sweep = $1 AND st.org_id = o.id
		LEFT JOIN event_consumer_cursor c ON c.consumer = $2 AND c.org_id = o.id
		ORDER BY o.id`, s.id.Name, s.id.Consumer, SettleTTL.Seconds())
	if err != nil {
		return nil, fmt.Errorf("eventlog: sweep %s orgs: %w", s.id.Name, err)
	}
	defer rows.Close()
	var out []OrgPass
	for rows.Next() {
		var p OrgPass
		if err := rows.Scan(&p.OrgID, &p.HighWater, &p.Settled, &p.Lagging); err != nil {
			return nil, fmt.Errorf("eventlog: sweep %s scan org: %w", s.id.Name, err)
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("eventlog: sweep %s orgs: %w", s.id.Name, err)
	}
	return out, nil
}

// Settle records that this pass walked orgID completely, found NOTHING to
// repair, and did so with the maintenance consumer caught up — the only
// combination that may mark an org skippable. Callers must not call it after
// a pass that repaired or errored: an org that needed repair is precisely the
// org whose next window must look again, and skipping repair of drift that
// already exists is the one thing this marker must never cause.
//
// settled_at is stamped with DATABASE now() and is what SettleTTL ages, so
// every settle also renews the org's lease: an org that keeps coming back
// clean keeps being skipped, but never for longer than SettleTTL at a time.
func (s *Sweeper) Settle(ctx context.Context, orgID, highWater int64) error {
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO sweep_org_state (sweep, org_id, settled_event_id, settled_at)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (sweep, org_id) DO UPDATE
		SET settled_event_id = EXCLUDED.settled_event_id,
		    settled_at       = EXCLUDED.settled_at`,
		s.id.Name, orgID, highWater); err != nil {
		return fmt.Errorf("eventlog: settle sweep %s org %d: %w", s.id.Name, orgID, err)
	}
	return nil
}
