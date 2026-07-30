package eventlog

import (
	"context"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/migrations"
)

// statementCounter is a pgx QueryTracer that counts every statement the pool
// sends. It is the instrument for the S4 NOTIFY-coalescing claim: "one
// pg_notify per (transaction, org), not one per append" is only meaningful if
// the round trips actually disappear, and a listener cannot see the difference
// (Postgres already collapses duplicate (channel,payload) pairs at commit, so
// the DELIVERED count was 1 before this change too — the cost was the extra
// statement per append, and that is what this counts).
type statementCounter struct{ n atomic.Int64 }

func (s *statementCounter) TraceQueryStart(ctx context.Context, _ *pgx.Conn,
	_ pgx.TraceQueryStartData) context.Context {
	s.n.Add(1)
	return ctx
}

func (s *statementCounter) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

// tracedPool is testPool with a statement counter attached. It needs its own
// pool (the tracer is per-connection-config), so it also runs the migrations.
func tracedPool(t *testing.T) (*pgxpool.Pool, *statementCounter) {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping integration test")
	}
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	counter := &statementCounter{}
	cfg.ConnConfig.Tracer = counter
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(context.Background(),
		`DROP SCHEMA public CASCADE; CREATE SCHEMA public`); err != nil {
		t.Fatalf("reset schema: %v", err)
	}
	if err := db.Migrate(context.Background(), pool, migrations.FS); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return pool, counter
}

// TestAppendCoalescesWake pins the S4 NOTIFY fold-in on two independent
// observations.
//
//  1. The PRIMITIVE: event_log_wake signals at most once per (transaction,
//     org) and separately per org, and a fresh transaction signals again —
//     asserted on the function's own boolean return, not on a delivery count
//     (Postgres deduplicates identical notifications at commit, so deliveries
//     cannot tell the two implementations apart).
//  2. The COST: appending N events for one org inside one transaction costs
//     exactly N statements. Before this change Append issued its own
//     `SELECT pg_notify(...)` after every INSERT, i.e. 2N.
func TestAppendCoalescesWake(t *testing.T) {
	pool, counter := tracedPool(t)
	ctx := context.Background()
	orgA := seedOrg(t, pool)
	var orgB int64
	if err := pool.QueryRow(ctx,
		`INSERT INTO org (name, slug) VALUES ('Other', 'other') RETURNING id`).Scan(&orgB); err != nil {
		t.Fatalf("seed org B: %v", err)
	}

	// (1) The primitive, exercised directly inside one transaction.
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	// A t.Fatal below must not strand the connection: pool.Close (t.Cleanup)
	// blocks until every acquired conn is released, so an un-rolled-back tx
	// would turn a legitimate failure into a hung test run.
	defer func() { _ = tx.Rollback(ctx) }()
	wake := func(org int64) bool {
		t.Helper()
		var sent bool
		if err := tx.QueryRow(ctx, `SELECT event_log_wake($1, $2)`,
			NotifyChannel, org).Scan(&sent); err != nil {
			t.Fatalf("wake(%d): %v", org, err)
		}
		return sent
	}
	if !wake(orgA) {
		t.Fatal("first wake for org A did not signal")
	}
	if wake(orgA) {
		t.Fatal("second wake for org A signalled again: not coalesced per (tx, org)")
	}
	if !wake(orgB) {
		t.Fatal("org B was coalesced against org A: the guard is not per-org")
	}
	if wake(orgB) {
		t.Fatal("second wake for org B signalled again: not coalesced per (tx, org)")
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	// SET LOCAL semantics: the memo dies with the transaction, so the next one
	// signals again. Without this the feed would go permanently silent after a
	// connection's first transaction.
	tx2, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin 2: %v", err)
	}
	defer func() { _ = tx2.Rollback(ctx) }()
	var again bool
	if err := tx2.QueryRow(ctx, `SELECT event_log_wake($1, $2)`,
		NotifyChannel, orgA).Scan(&again); err != nil {
		t.Fatalf("wake after commit: %v", err)
	}
	if !again {
		t.Fatal("the tx-local memo outlived its transaction: a later tx never wakes consumers")
	}

	// (2) The cost, measured on a real Append transaction. The counter is
	// sampled INSIDE the transaction, around the appends only, so pgx's own
	// traced BEGIN/COMMIT stay out of the delta.
	const appends = 5
	var cost int64
	err = db.WithTx(ctx, pool, func(tx pgx.Tx) error {
		start := counter.n.Load()
		for i := 0; i < appends; i++ {
			if _, err := Append(ctx, tx, Event{
				OrgID: orgA, ActorKind: enum.ActorSystem,
				EntityType: enum.EntityMessage, EntityID: int64(i + 1),
				Verb: "thing.happened",
			}); err != nil {
				return err
			}
		}
		cost = counter.n.Load() - start
		return nil
	})
	if err != nil {
		t.Fatalf("append tx: %v", err)
	}
	if cost != appends {
		t.Fatalf("%d appends cost %d statements, want %d (one per append: the "+
			"wake must ride the INSERT's RETURNING, not a second round trip)",
			appends, cost, appends)
	}

	// The events are all there — coalescing the wake must not touch the writes.
	var n int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM event_log WHERE org_id = $1`, orgA).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != appends {
		t.Fatalf("event_log has %d rows for org A, want %d", n, appends)
	}
}

// TestAppendWakeDelivers is the liveness half: the coalesced wake must still
// reach a LISTENer, exactly once per (transaction, org), carrying the org id.
// A guard that never signals would pass TestAppendCoalescesWake's cost assert.
func TestAppendWakeDelivers(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	orgID := seedOrg(t, pool)

	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, "LISTEN "+NotifyChannel); err != nil {
		t.Fatalf("listen: %v", err)
	}

	err = db.WithTx(ctx, pool, func(tx pgx.Tx) error {
		for i := 0; i < 3; i++ {
			if _, err := Append(ctx, tx, Event{
				OrgID: orgID, ActorKind: enum.ActorSystem,
				EntityType: enum.EntityMessage, EntityID: int64(i + 1),
				Verb: "thing.happened",
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("append tx: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	n, err := conn.Conn().WaitForNotification(waitCtx)
	if err != nil {
		t.Fatalf("no wake delivered for a committed 3-event transaction: %v", err)
	}
	if n.Channel != NotifyChannel {
		t.Fatalf("wake on channel %q, want %q", n.Channel, NotifyChannel)
	}
	if want := strconv.FormatInt(orgID, 10); n.Payload != want {
		t.Fatalf("wake payload %q, want the org id %q", n.Payload, want)
	}
}
