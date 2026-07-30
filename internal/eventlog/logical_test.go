package eventlog

import (
	"context"
	"encoding/json"
	"errors"
	"expvar"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/internal/platform/metrics"
)

// The logical feed needs a server started with wal_level=logical and a spare
// replication slot. A default Postgres runs at wal_level=replica, so a
// developer box and the CI service container may not have it — the tests skip
// there rather than fail, EXCEPT when TEST_LOGICAL_DECODING is set, which is
// how CI demands that these pins actually run instead of quietly evaporating.
func requireLogicalWAL(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	var level string
	if err := pool.QueryRow(context.Background(), `SHOW wal_level`).Scan(&level); err != nil {
		t.Fatalf("read wal_level: %v", err)
	}
	if level == "logical" {
		return
	}
	msg := "server runs wal_level=" + level + "; the logical feed needs wal_level=logical " +
		"(postgres -c wal_level=logical)"
	if os.Getenv("TEST_LOGICAL_DECODING") != "" {
		t.Fatal("TEST_LOGICAL_DECODING is set but the " + msg)
	}
	t.Skip(msg)
}

// startLogical provisions the slot/publication, starts the reader, and tears
// both down after the test (a leftover slot would retain WAL for every later
// test run — the very failure mode this slice documents).
func startLogical(t *testing.T, pool *pgxpool.Pool) *LogicalSource {
	t.Helper()
	ctx := context.Background()
	opts := LogicalOptions{Slot: "eventlog_feed_test", Publication: "eventlog_pub_test"}
	// A slot survives DROP SCHEMA, so a previous run's slot may still be here.
	if err := DropLogical(ctx, pool, opts); err != nil {
		t.Fatalf("pre-clean: %v", err)
	}
	if err := ProvisionLogical(ctx, pool, opts); err != nil {
		t.Fatalf("provision: %v", err)
	}
	src, err := NewLogicalSource(pool, testLogger(), opts)
	if err != nil {
		t.Fatalf("new logical source: %v", err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { defer close(done); src.Run(runCtx) }()
	t.Cleanup(func() {
		cancel()
		<-done
		// The slot only drops once the walsender has gone.
		for i := 0; i < 100; i++ {
			if err := DropLogical(ctx, pool, opts); err == nil {
				var still bool
				_ = pool.QueryRow(ctx,
					`SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)`,
					opts.Slot).Scan(&still)
				if !still {
					return
				}
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Errorf("test slot %q outlived the test: it will retain WAL", opts.Slot)
	})
	readyCtx, readyCancel := context.WithTimeout(ctx, 15*time.Second)
	defer readyCancel()
	if err := src.WaitReady(readyCtx); err != nil {
		t.Fatalf("logical feed never became ready: %v", err)
	}
	return src
}

// newFeed is the ONE place the driver under test is chosen. It defaults to the
// logical feed; TEST_FEED_DRIVER=xmin reruns these exact pins against the old
// poller, which is how the RED runs documented on each test are reproduced —
// same test, same assertions, a different delivery mechanism underneath.
func newFeed(t *testing.T, pool *pgxpool.Pool, src *LogicalSource, name string, batch int) Feed {
	t.Helper()
	driver := os.Getenv("TEST_FEED_DRIVER")
	if driver == "" {
		driver = "logical"
	}
	switch driver {
	case "xmin":
		s := &xminSource{pool: pool}
		s.SetMetrics(metrics.NewExpvar())
		return s.Consumer(name, batch)
	case "logical":
		src.SetMetrics(metrics.NewExpvar())
		f := src.Consumer(name, batch)
		f.SetMetrics(metrics.NewExpvar())
		return f
	default:
		t.Fatalf("TEST_FEED_DRIVER=%q is not a driver these pins can run against", driver)
		return nil
	}
}

// drain polls until the feed is caught up, returning every row it handed over
// in order. It acks each batch, so it also exercises the cursor round trip.
func drain(t *testing.T, f Feed, orgID int64) []Row {
	t.Helper()
	ctx := context.Background()
	var all []Row
	for i := 0; i < 200; i++ {
		batch, err := f.Poll(ctx, orgID)
		if err != nil {
			t.Fatalf("poll: %v", err)
		}
		if len(batch) == 0 {
			return all
		}
		all = append(all, batch...)
		if err := f.Ack(ctx, orgID, batch[len(batch)-1].ID); err != nil {
			t.Fatalf("ack: %v", err)
		}
	}
	t.Fatal("drain did not converge")
	return nil
}

// TestLogicalFeedNoGlobalStall is the S4 headline pin: a long-running write
// transaction in org A must not delay org B by one event.
//
// RED (observed): TEST_FEED_DRIVER=xmin — org B's committed event is held
// behind org A's open transaction and the DELIVERY assert fails, before the run
// ever reaches the lag assert. That is the whole difference between the drivers
// and it is not fixable in the poller. What WAS fixable — and has since been
// fixed — is that the xmin driver's Lag could not see that stall either;
// TestConsumerLagSeesGlobalStall pins the xmin gauge on its own.
func TestLogicalFeedNoGlobalStall(t *testing.T) {
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
	feed := newFeed(t, pool, src, "stalltest", 100)

	// Both orgs start caught up, so the cursors are on the live lane and the
	// assertions below are about NEW events only.
	drain(t, feed, orgA)
	drain(t, feed, orgB)

	// Org A opens a long write transaction and keeps it open. This is the
	// blowup consumer.go:22-26 describes: it holds pg_snapshot_xmin down for
	// the whole DATABASE.
	txA, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin A: %v", err)
	}
	defer func() { _ = txA.Rollback(ctx) }()
	idA, err := Append(ctx, txA, Event{
		OrgID: orgA, ActorKind: enum.ActorSystem,
		EntityType: enum.EntityMessage, EntityID: 1, Verb: "a.slow",
	})
	if err != nil {
		t.Fatalf("append A: %v", err)
	}

	// Org B, an unrelated tenant, sends and commits.
	idB := appendInTx(t, pool, orgB, "b.created")
	waitCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := src.WaitFor(waitCtx, orgB, idB); err != nil {
		t.Fatalf("feed never decoded org B's event: %v", err)
	}

	// THE headline assert: org B's committed event is delivered while org A's
	// transaction is still open. Poll does not move the cursor, so the lag
	// assert below still sees the same pending event.
	rows, err := feed.Poll(ctx, orgB)
	if err != nil {
		t.Fatalf("poll B: %v", err)
	}
	if len(rows) != 1 || rows[0].ID != idB {
		t.Fatalf("org B stalled behind org A's open transaction: got %d rows %v, want [%d]",
			len(rows), rowIDs(rows), idB)
	}

	// The gauge must SEE the backlog. Here that is an exact entry count against
	// the LSN cursor; the xmin driver measures an id delta against last_id and,
	// since the horizon came out of its Lag query, now also sees a stall instead
	// of reporting 0 through it (TestConsumerLagSeesGlobalStall). Neither driver
	// is allowed to call an undelivered event delivered.
	lag, err := feed.Lag(ctx, orgB)
	if err != nil {
		t.Fatalf("lag B: %v", err)
	}
	if lag != 1 {
		t.Fatalf("org B lag = %d, want 1: one committed, unconsumed event is "+
			"pending while org A's transaction is open", lag)
	}
	if got := lagGauge(t, "stalltest", orgB); got != 1 {
		t.Fatalf("consumer_lag{consumer=stalltest,org=B} = %v, want 1", got)
	}

	if err := feed.Ack(ctx, orgB, idB); err != nil {
		t.Fatalf("ack B: %v", err)
	}
	lag, err = feed.Lag(ctx, orgB)
	if err != nil {
		t.Fatalf("lag B after ack: %v", err)
	}
	if lag != 0 {
		t.Fatalf("org B lag after consuming = %d, want 0", lag)
	}
	if got := lagGauge(t, "stalltest", orgB); got != 0 {
		t.Fatalf("consumer_lag{consumer=stalltest,org=B} after ack = %v, want 0", got)
	}

	// Org A's own event is still correctly INVISIBLE — nothing uncommitted may
	// ever be delivered. This is the guard that stops "no stall" from being
	// bought with a dirty read.
	rows, err = feed.Poll(ctx, orgA)
	if err != nil {
		t.Fatalf("poll A: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("uncommitted event delivered: %v", rowIDs(rows))
	}

	// When A finally commits, its event arrives too — delayed for A, never lost.
	if err := txA.Commit(ctx); err != nil {
		t.Fatalf("commit A: %v", err)
	}
	waitCtx2, cancel2 := context.WithTimeout(ctx, 15*time.Second)
	defer cancel2()
	if err := src.WaitFor(waitCtx2, orgA, idA); err != nil {
		t.Fatalf("feed never decoded org A's event after commit: %v", err)
	}
	got := drain(t, feed, orgA)
	if len(got) != 1 || got[0].ID != idA {
		t.Fatalf("org A got %v after commit, want [%d]", rowIDs(got), idA)
	}
}

// TestLogicalFeedNoCommitOrderSkip is the correctness pin, and the reason S4
// is not merely a latency slice.
//
// event_log.txid is stamped at a transaction's FIRST write; the event id comes
// from a sequence at APPEND. So a transaction that started EARLIER (lower
// txid) can hold a LATER id. Under the xmin gate that crossing loses an event
// permanently: the gate admits the earlier transaction's HIGHER id, the cursor
// moves past the lower id still in flight, and when that transaction commits
// its event is below the cursor forever — no error, no gap anyone can detect.
//
// RED (observed): TEST_FEED_DRIVER=xmin — the lower id is never
// delivered and the "no event skipped" assert fails.
func TestLogicalFeedNoCommitOrderSkip(t *testing.T) {
	pool := testPool(t)
	requireLogicalWAL(t, pool)
	ctx := context.Background()
	orgID := seedOrg(t, pool)
	src := startLogical(t, pool)
	feed := newFeed(t, pool, src, "crossing", 100)
	drain(t, feed, orgID)

	// txEarly takes its txid FIRST...
	txEarly, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin early: %v", err)
	}
	defer func() { _ = txEarly.Rollback(ctx) }()
	var xidEarly, xidLate string
	if err := txEarly.QueryRow(ctx, `SELECT pg_current_xact_id()::text`).Scan(&xidEarly); err != nil {
		t.Fatalf("assign early xid: %v", err)
	}
	// ...txLate takes a HIGHER txid second...
	txLate, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin late: %v", err)
	}
	defer func() { _ = txLate.Rollback(ctx) }()
	if err := txLate.QueryRow(ctx, `SELECT pg_current_xact_id()::text`).Scan(&xidLate); err != nil {
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
	t.Logf("crossing: early tx xid=%s holds id %d; late tx xid=%s holds id %d",
		xidEarly, idHigh, xidLate, idLow)

	// The early transaction commits while the late one is still open. Under the
	// xmin gate this is the fatal moment: idHigh becomes deliverable and the
	// cursor advances past idLow.
	if err := txEarly.Commit(ctx); err != nil {
		t.Fatalf("commit early: %v", err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := src.WaitFor(waitCtx, orgID, idHigh); err != nil {
		t.Fatalf("feed never decoded the early commit: %v", err)
	}
	first := drain(t, feed, orgID)

	if err := txLate.Commit(ctx); err != nil {
		t.Fatalf("commit late: %v", err)
	}
	waitCtx2, cancel2 := context.WithTimeout(ctx, 15*time.Second)
	defer cancel2()
	if err := src.WaitFor(waitCtx2, orgID, idLow); err != nil {
		t.Fatalf("feed never decoded the late commit: %v", err)
	}
	second := drain(t, feed, orgID)

	delivered := append(rowIDs(first), rowIDs(second)...)
	if !contains(delivered, idLow) {
		t.Fatalf("event %d was SKIPPED FOREVER: the lower id committed after a "+
			"higher id that the cursor already passed. delivered=%v", idLow, delivered)
	}
	if !contains(delivered, idHigh) {
		t.Fatalf("event %d was not delivered: delivered=%v", idHigh, delivered)
	}
	// Commit order, not id order: the early transaction committed first.
	if len(delivered) != 2 || delivered[0] != idHigh || delivered[1] != idLow {
		t.Fatalf("delivery order %v, want commit order [%d %d]", delivered, idHigh, idLow)
	}
}

// TestLogicalFeedParity pins the Consumer-contract behaviours every existing
// consumer relies on, on the logical driver: history replay for a fresh
// consumer name (ADR-003 E2), in-order delivery with nothing skipped, a
// monotone cursor, and durability of the cursor across a new Feed instance.
func TestLogicalFeedParity(t *testing.T) {
	pool := testPool(t)
	requireLogicalWAL(t, pool)
	ctx := context.Background()
	orgID := seedOrg(t, pool)

	// History written BEFORE the feed exists: a fresh consumer must still see
	// it, because "replay = reset the cursor" has to keep meaning something
	// once the position is an LSN.
	var history []int64
	for i := 0; i < 7; i++ {
		history = append(history, appendInTx(t, pool, orgID, "history"))
	}

	src := startLogical(t, pool)
	feed := newFeed(t, pool, src, "parity", 3) // batch 3: forces multiple pages

	got := rowIDs(drain(t, feed, orgID))
	if !equalIDs(got, history) {
		t.Fatalf("history replay = %v, want %v", got, history)
	}

	// Live events, several transactions, some multi-event.
	var live []int64
	live = append(live, appendInTx(t, pool, orgID, "live.one"))
	err := db.WithTx(ctx, pool, func(tx pgx.Tx) error {
		for i := 0; i < 4; i++ {
			id, err := Append(ctx, tx, Event{
				OrgID: orgID, ActorKind: enum.ActorSystem,
				EntityType: enum.EntityMessage, EntityID: int64(i), Verb: "live.batch",
			})
			if err != nil {
				return err
			}
			live = append(live, id)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("live batch: %v", err)
	}
	live = append(live, appendInTx(t, pool, orgID, "live.last"))
	waitCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := src.WaitFor(waitCtx, orgID, live[len(live)-1]); err != nil {
		t.Fatalf("feed never decoded the last live event: %v", err)
	}
	got = rowIDs(drain(t, feed, orgID))
	if !equalIDs(got, live) {
		t.Fatalf("live delivery = %v, want %v (nothing skipped, commit order)", got, live)
	}

	// A stale ack never rewinds, and a NEW Feed instance picks up the DURABLE
	// cursor rather than replaying (the position survives a process restart).
	if err := feed.Ack(ctx, orgID, history[0]); err != nil {
		t.Fatalf("stale ack: %v", err)
	}
	again := src.Consumer("parity", 3)
	if rows, err := again.Poll(ctx, orgID); err != nil || len(rows) != 0 {
		t.Fatalf("cursor rewound or not durable: err=%v rows=%v", err, rowIDs(rows))
	}

	// A different consumer name starts from zero (ADR-003 E2).
	fresh := src.Consumer("parity-fresh", 100)
	all := rowIDs(drain(t, fresh, orgID))
	if len(all) != len(history)+len(live) {
		t.Fatalf("fresh consumer replayed %d events, want %d", len(all), len(history)+len(live))
	}

	// Payload fidelity: the body comes back through pgx, not a hand-rolled WAL
	// text decoder, so the jsonb/timestamptz columns must be intact.
	past := time.Date(2019, 5, 1, 12, 0, 0, 0, time.UTC)
	var backdated int64
	err = db.WithTx(ctx, pool, func(tx pgx.Tx) error {
		id, err := Append(ctx, tx, Event{
			OrgID: orgID, ActorKind: enum.ActorImporter,
			EntityType: enum.EntityMessage, EntityID: 99, Verb: "imported",
			Payload: MustPayload(map[string]any{"k": "v"}), OccurredAt: past,
		})
		backdated = id
		return err
	})
	if err != nil {
		t.Fatalf("backdated append: %v", err)
	}
	waitCtx2, cancel2 := context.WithTimeout(ctx, 15*time.Second)
	defer cancel2()
	if err := src.WaitFor(waitCtx2, orgID, backdated); err != nil {
		t.Fatalf("feed never decoded the backdated event: %v", err)
	}
	rows := drain(t, feed, orgID)
	if len(rows) != 1 || rows[0].ID != backdated {
		t.Fatalf("backdated delivery = %v", rowIDs(rows))
	}
	r := rows[0]
	if !r.OccurredAt.Equal(past) {
		t.Fatalf("occurred_at = %v, want %v", r.OccurredAt, past)
	}
	if !r.RecordedAt.After(past) {
		t.Fatalf("recorded_at should be ingest time, got %v", r.RecordedAt)
	}
	// Compare decoded, not byte-for-byte: jsonb re-renders its own canonical
	// text, so a literal string compare would pin Postgres formatting, not
	// fidelity.
	var payload map[string]string
	if err := json.Unmarshal(r.Payload, &payload); err != nil {
		t.Fatalf("payload %s: %v", r.Payload, err)
	}
	if payload["k"] != "v" {
		t.Fatalf("payload = %v, want k=v", payload)
	}
	if r.ActorKind != enum.ActorImporter {
		t.Fatalf("actor_kind = %v, want importer", r.ActorKind)
	}
}

// TestLogicalFeedBootstrapCrashReplay pins the bootstrap hand-off's
// durability. A fresh consumer replays history through the table (the
// bootstrap lane) and then splices onto the live feed at a head LSN. If that
// LSN were stamped when the batch was HANDED OUT rather than when it was
// ACKED, a crash in between would bring the consumer back LIVE at a head above
// events it never processed — history silently lost, exactly the failure mode
// the LSN cursor exists to prevent.
//
// RED: move the writeCursor(..., &head) call in logicalConsumer.bootstrap so it
// runs at Poll time instead of on the Ack — the replay below comes back empty.
func TestLogicalFeedBootstrapCrashReplay(t *testing.T) {
	pool := testPool(t)
	requireLogicalWAL(t, pool)
	ctx := context.Background()
	orgID := seedOrg(t, pool)
	var history []int64
	for i := 0; i < 3; i++ {
		history = append(history, appendInTx(t, pool, orgID, "pre.feed"))
	}
	src := startLogical(t, pool)

	feed := src.Consumer("crashy", 100)
	rows, err := feed.Poll(ctx, orgID)
	if err != nil {
		t.Fatalf("bootstrap poll: %v", err)
	}
	if !equalIDs(rowIDs(rows), history) {
		t.Fatalf("bootstrap poll = %v, want %v", rowIDs(rows), history)
	}
	// The process dies here — no Ack. A new instance has the same DURABLE
	// cursor and none of the in-memory hand-off state.
	revived := src.Consumer("crashy", 100)
	again, err := revived.Poll(ctx, orgID)
	if err != nil {
		t.Fatalf("poll after crash: %v", err)
	}
	if !equalIDs(rowIDs(again), history) {
		t.Fatalf("history lost across a crash before Ack: replay = %v, want %v",
			rowIDs(again), history)
	}
	if err := revived.Ack(ctx, orgID, history[len(history)-1]); err != nil {
		t.Fatalf("ack: %v", err)
	}
	// Now spliced: the cursor carries an LSN and the history is not replayed.
	var lsn *string
	if err := pool.QueryRow(ctx,
		`SELECT lsn::text FROM event_consumer_cursor WHERE consumer='crashy' AND org_id=$1`,
		orgID).Scan(&lsn); err != nil {
		t.Fatalf("cursor: %v", err)
	}
	if lsn == nil {
		t.Fatal("acking the final bootstrap batch did not splice onto the live lane")
	}
	if more, err := revived.Poll(ctx, orgID); err != nil || len(more) != 0 {
		t.Fatalf("after the splice: err=%v rows=%v, want none", err, rowIDs(more))
	}
	// And the live lane works from there.
	live := appendInTx(t, pool, orgID, "after.splice")
	waitCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := src.WaitFor(waitCtx, orgID, live); err != nil {
		t.Fatalf("feed never decoded the post-splice event: %v", err)
	}
	got := rowIDs(drain(t, revived, orgID))
	if len(got) != 1 || got[0] != live {
		t.Fatalf("post-splice delivery = %v, want [%d]", got, live)
	}
}

// TestLogicalFeedSlotLagMetric pins the stuck-slot instrument the pre-mortem
// requires: a slot retains WAL for the SLOWEST consumer, and the gauge that
// tells an operator so must move with reality, not report a constant.
func TestLogicalFeedSlotLagMetric(t *testing.T) {
	pool := testPool(t)
	requireLogicalWAL(t, pool)
	ctx := context.Background()
	orgID := seedOrg(t, pool)
	src := startLogical(t, pool)
	src.SetMetrics(metrics.NewExpvar())
	feed := src.Consumer("slotlag", 100)
	drain(t, feed, orgID)

	src.publishSlotLag(ctx)
	before := slotLagGauge(t, "eventlog_feed_test")
	if before < 0 {
		t.Fatal("eventlog_slot_lag_bytes was never published")
	}

	// Write a lot without letting the slot advance: the retained WAL — the
	// disk-fill risk — must show up in the gauge.
	for i := 0; i < 50; i++ {
		appendInTx(t, pool, orgID, "unconsumed")
	}
	src.publishSlotLag(ctx)
	after := slotLagGauge(t, "eventlog_feed_test")
	if after <= before {
		t.Fatalf("eventlog_slot_lag_bytes did not rise with unconsumed WAL: %v -> %v", before, after)
	}

	// The retention RULE: the position the reader reports to the server (which
	// is what lets Postgres recycle WAL) is the MINIMUM durable cursor across
	// consumers, never the reader's head — otherwise a restart would drop
	// everything a slow consumer had not processed yet.
	slow := src.Consumer("slotlag-slow", 100)
	drain(t, slow, orgID) // slow is now at the current head
	last := appendInTx(t, pool, orgID, "after.slow.stopped")
	waitCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := src.WaitFor(waitCtx, orgID, last); err != nil {
		t.Fatalf("feed never decoded the trailing event: %v", err)
	}
	drain(t, feed, orgID) // only the FAST consumer moves past it
	safe, err := src.safeFlushLSN(ctx)
	if err != nil {
		t.Fatalf("safe flush lsn: %v", err)
	}
	want := minCursorLSN(t, pool)
	if safe.String() != want {
		t.Fatalf("slot would be advanced to %s, want the slowest cursor %s "+
			"(a faster consumer must not let the slot discard the slow one's WAL)",
			safe, want)
	}

	// Drop-and-resync (the runbook's remedy) needs the slot to be droppable
	// once the reader lets go, and the consumers to keep working afterwards by
	// resetting their cursors. The reader's own Cleanup asserts the drop; here
	// we assert the resync half: a cursor reset replays from the start.
	if _, err := pool.Exec(ctx,
		`UPDATE event_consumer_cursor SET last_id = 0, lsn = NULL WHERE consumer = $1`,
		"slotlag"); err != nil {
		t.Fatalf("reset cursor: %v", err)
	}
	var total int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM event_log WHERE org_id = $1`, orgID).Scan(&total); err != nil {
		t.Fatalf("count: %v", err)
	}
	replayed := drain(t, feed, orgID)
	if len(replayed) != total {
		t.Fatalf("cursor reset replayed %d events, want the org's whole log (%d)",
			len(replayed), total)
	}
}

// TestLogicalFeedErrors pins the loud failures: an unknown driver name is a
// config error (never a silent fallback to the poller), and a consumer polling
// before the reader is streaming gets ErrFeedNotReady rather than a
// convincing-looking empty batch.
func TestLogicalFeedErrors(t *testing.T) {
	pool := testPool(t)
	if _, err := Open("nope", pool, testLogger(), LogicalOptions{}); err == nil {
		t.Fatal("unknown feed driver was accepted")
	}
	if _, err := Open("", pool, testLogger(), LogicalOptions{}); err != nil {
		t.Fatalf("empty driver must default to xmin: %v", err)
	}
	if _, err := Open("xmin", pool, testLogger(), LogicalOptions{}); err != nil {
		t.Fatalf("xmin driver: %v", err)
	}
	src, err := NewLogicalSource(pool, testLogger(), LogicalOptions{Slot: "never_started"})
	if err != nil {
		t.Fatalf("new logical source: %v", err)
	}
	feed := src.Consumer("notready", 10)
	if _, err := feed.Poll(context.Background(), 1); !errors.Is(err, ErrFeedNotReady) {
		t.Fatalf("Poll on a non-streaming feed returned %v, want ErrFeedNotReady", err)
	}
	if _, err := feed.Lag(context.Background(), 1); !errors.Is(err, ErrFeedNotReady) {
		t.Fatalf("Lag on a non-streaming feed returned %v, want ErrFeedNotReady", err)
	}
}

// --- small shared helpers -------------------------------------------------

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func rowIDs(rows []Row) []int64 {
	out := make([]int64, len(rows))
	for i, r := range rows {
		out[i] = r.ID
	}
	return out
}

func contains(ids []int64, want int64) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}

func equalIDs(got, want []int64) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// slotLagGauge reads the published eventlog_slot_lag_bytes{slot} series; -1 if
// absent.
func slotLagGauge(t *testing.T, slot string) float64 {
	t.Helper()
	v := expvar.Get("eventlog_slot_lag_bytes")
	if v == nil {
		return -1
	}
	m, ok := v.(*expvar.Map)
	if !ok {
		t.Fatalf("eventlog_slot_lag_bytes is %T, want *expvar.Map", v)
	}
	kv := m.Get("slot=" + slot)
	if kv == nil {
		return -1
	}
	return kv.(*expvar.Float).Value()
}

// minCursorLSN derives the expected slot-retention floor independently of the
// code under test: the smallest non-null cursor LSN in the table.
func minCursorLSN(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	var lsn *string
	err := pool.QueryRow(context.Background(),
		`SELECT min(lsn)::text FROM event_consumer_cursor WHERE lsn IS NOT NULL`).Scan(&lsn)
	if err != nil {
		t.Fatalf("min cursor lsn: %v", err)
	}
	if lsn == nil {
		t.Fatal("no consumer has a durable LSN")
	}
	return *lsn
}
