package eventlog

import (
	"context"
	"fmt"

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
// what an operator sees and what any per-sweep durable state is keyed by; Key
// is its objid inside sweepLockClass. Both are APPEND-ONLY, exactly like
// consumer names and enum values: renaming a sweep abandons whatever durable
// state it had accumulated, and reusing a Key silently serialises two
// unrelated sweeps against each other.
type SweepID struct {
	Name string
	Key  int32
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
