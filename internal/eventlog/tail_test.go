package eventlog

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/enum"
)

// newTail is the ONE place the driver under test is chosen, the newFeed twin.
// It defaults to the logical tail; TEST_FEED_DRIVER=xmin reruns these exact
// pins against the default poller's tail, which is how the RED runs documented
// on each test below are reproduced — same test, same assertions, a different
// delivery mechanism underneath.
func newTail(t *testing.T, pool *pgxpool.Pool, src *LogicalSource) Tail {
	t.Helper()
	driver := os.Getenv("TEST_FEED_DRIVER")
	if driver == "" {
		driver = "logical"
	}
	switch driver {
	case "xmin":
		return (&xminSource{pool: pool}).Tail()
	case "logical":
		return src.Tail()
	default:
		t.Fatalf("TEST_FEED_DRIVER=%q is not a driver these pins can run against", driver)
		return nil
	}
}

func tailIDs(rows []Row) []int64 { return rowIDs(rows) }

// TestFeedTailCommitOrder is the ephemeral half of the S4 correctness pin: the
// crossing that TestLogicalFeedNoCommitOrderSkip proved for the DURABLE feed,
// proved again on the cursor-free Tail the gateway consumes (P-45).
//
// The construction is the same: txid is stamped at a transaction's first
// write, the event id at append, so the transaction with the LOWER txid can
// hold the HIGHER id. When it commits first, an id-ordered gated scan hands
// over the higher id and can never come back for the lower one.
//
// RED (observed): TEST_FEED_DRIVER=xmin — the lower id is never returned and
// the "delivered in commit order" assert fails.
func TestFeedTailCommitOrder(t *testing.T) {
	pool := testPool(t)
	requireLogicalWAL(t, pool)
	ctx := context.Background()
	orgID := seedOrg(t, pool)
	src := startLogical(t, pool)
	tail := newTail(t, pool, src)

	// A tail starts at the org's LIVE head: history is already "delivered", so
	// a fresh gateway shard only ever reads new events.
	seeded := appendInTx(t, pool, orgID, "before.the.tail")
	waitDecoded(t, ctx, src, orgID, seeded)
	pos, headID, err := tail.Head(ctx, orgID)
	if err != nil {
		t.Fatalf("head: %v", err)
	}
	if headID != seeded {
		t.Fatalf("head id = %d, want the org's newest committed event %d", headID, seeded)
	}
	if rows, _, err := tail.Next(ctx, orgID, pos, 100); err != nil || len(rows) != 0 {
		t.Fatalf("a fresh tail replayed history: err=%v rows=%v", err, tailIDs(rows))
	}

	// txEarly takes its txid FIRST...
	txEarly, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin early: %v", err)
	}
	defer func() { _ = txEarly.Rollback(ctx) }()
	var xid string
	if err := txEarly.QueryRow(ctx, `SELECT pg_current_xact_id()::text`).Scan(&xid); err != nil {
		t.Fatalf("assign early xid: %v", err)
	}
	// ...txLate takes a HIGHER txid second...
	txLate, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin late: %v", err)
	}
	defer func() { _ = txLate.Rollback(ctx) }()
	if err := txLate.QueryRow(ctx, `SELECT pg_current_xact_id()::text`).Scan(&xid); err != nil {
		t.Fatalf("assign late xid: %v", err)
	}
	// ...but txLate appends FIRST, so it holds the LOWER event id.
	idLow, err := Append(ctx, txLate, Event{
		OrgID: orgID, ActorKind: enum.ActorSystem,
		EntityType: enum.EntityMessage, EntityID: 1, Verb: "late.tx.low.id",
	})
	if err != nil {
		t.Fatalf("append low: %v", err)
	}
	idHigh, err := Append(ctx, txEarly, Event{
		OrgID: orgID, ActorKind: enum.ActorSystem,
		EntityType: enum.EntityMessage, EntityID: 2, Verb: "early.tx.high.id",
	})
	if err != nil {
		t.Fatalf("append high: %v", err)
	}
	if idHigh <= idLow {
		t.Fatalf("scenario not constructed: ids %d (late tx) and %d (early tx)", idLow, idHigh)
	}

	if err := txEarly.Commit(ctx); err != nil {
		t.Fatalf("commit early: %v", err)
	}
	waitDecoded(t, ctx, src, orgID, idHigh)
	first, pos, err := tail.Next(ctx, orgID, pos, 100)
	if err != nil {
		t.Fatalf("next after the early commit: %v", err)
	}

	if err := txLate.Commit(ctx); err != nil {
		t.Fatalf("commit late: %v", err)
	}
	waitDecoded(t, ctx, src, orgID, idLow)
	second, pos, err := tail.Next(ctx, orgID, pos, 100)
	if err != nil {
		t.Fatalf("next after the late commit: %v", err)
	}

	delivered := append(tailIDs(first), tailIDs(second)...)
	if !contains(delivered, idLow) {
		t.Fatalf("event %d was SKIPPED FOREVER by the tail: the lower id committed "+
			"after a higher id the position already passed. delivered=%v", idLow, delivered)
	}
	if len(delivered) != 2 || delivered[0] != idHigh || delivered[1] != idLow {
		t.Fatalf("tail delivery order %v, want commit order [%d %d]", delivered, idHigh, idLow)
	}
	// Nothing repeats once the position has passed it.
	if rows, _, err := tail.Next(ctx, orgID, pos, 100); err != nil || len(rows) != 0 {
		t.Fatalf("tail re-delivered a passed position: err=%v rows=%v", err, tailIDs(rows))
	}

	// Ordered is the contract that makes the above SAFE to consume: a caller
	// told "true" is entitled to treat id <= cursor as already-delivered, and
	// that is exactly what would have dropped idLow here.
	if tail.Ordered() {
		t.Fatal("a commit-ordered tail advertised Ordered() = true: a caller that " +
			"believes it will skip every late-committing lower id")
	}

	// History is the RESUME lane and is id-ordered whatever the commit order
	// was — a client's resume cursor is an event id (ADR-002 F-2).
	hist, err := tail.History(ctx, orgID, seeded, 100)
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if got := tailIDs(hist); len(got) != 2 || got[0] != idLow || got[1] != idHigh {
		t.Fatalf("history replay = %v, want id order [%d %d]", got, idLow, idHigh)
	}
}

// TestFeedTailNoGlobalStall pins the OTHER half for the gateway's lanes: one
// unrelated org's long write transaction must not hold up a second org's live
// read or its resume replay. The xmin gate is DATABASE-GLOBAL, so under it a
// single open transaction anywhere freezes every gateway connection on the
// cell; this is the pin that says the tail is off that gate.
//
// RED (observed): TEST_FEED_DRIVER=xmin — org B's committed event is withheld
// from both Next and History while org A's transaction is open.
func TestFeedTailNoGlobalStall(t *testing.T) {
	pool := testPool(t)
	requireLogicalWAL(t, pool)
	ctx := context.Background()
	orgA := seedOrg(t, pool)
	var orgB int64
	if err := pool.QueryRow(ctx,
		`INSERT INTO org (name, slug) VALUES ('B', 'b') RETURNING id`).Scan(&orgB); err != nil {
		t.Fatalf("seed org B: %v", err)
	}
	src := startLogical(t, pool)
	tail := newTail(t, pool, src)

	posB, headB, err := tail.Head(ctx, orgB)
	if err != nil {
		t.Fatalf("head B: %v", err)
	}

	// Org A opens a long write transaction and keeps it open: this is what
	// holds pg_snapshot_xmin down for the whole DATABASE.
	txA, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin A: %v", err)
	}
	defer func() { _ = txA.Rollback(ctx) }()
	if _, err := Append(ctx, txA, Event{
		OrgID: orgA, ActorKind: enum.ActorSystem,
		EntityType: enum.EntityMessage, EntityID: 1, Verb: "a.slow",
	}); err != nil {
		t.Fatalf("append A: %v", err)
	}

	idB := appendInTx(t, pool, orgB, "b.created")
	waitDecoded(t, ctx, src, orgB, idB)

	rows, _, err := tail.Next(ctx, orgB, posB, 100)
	if err != nil {
		t.Fatalf("next B: %v", err)
	}
	if len(rows) != 1 || rows[0].ID != idB {
		t.Fatalf("org B's live lane stalled behind org A's open transaction: got %v, want [%d]",
			tailIDs(rows), idB)
	}
	hist, err := tail.History(ctx, orgB, headB, 100)
	if err != nil {
		t.Fatalf("history B: %v", err)
	}
	if len(hist) != 1 || hist[0].ID != idB {
		t.Fatalf("org B's resume lane stalled behind org A's open transaction: got %v, want [%d]",
			tailIDs(hist), idB)
	}
	// And nothing uncommitted leaked while proving it.
	rowsA, _, err := tail.Next(ctx, orgA, Position{}, 100)
	if err != nil {
		t.Fatalf("next A: %v", err)
	}
	for _, r := range rowsA {
		if r.Verb == "a.slow" {
			t.Fatalf("the tail delivered an UNCOMMITTED event (%d)", r.ID)
		}
	}
}

func waitDecoded(t *testing.T, ctx context.Context, src *LogicalSource, orgID, id int64) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := src.WaitFor(waitCtx, orgID, id); err != nil {
		t.Fatalf("feed never decoded event %d for org %d: %v", id, orgID, err)
	}
}
