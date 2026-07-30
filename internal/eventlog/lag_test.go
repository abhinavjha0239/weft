package eventlog

import (
	"context"
	"expvar"
	"strconv"
	"testing"

	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/internal/platform/metrics"
)

// lagGauge reads the published consumer_lag{consumer,org} series; -1 if absent.
func lagGauge(t *testing.T, consumer string, orgID int64) float64 {
	t.Helper()
	v := expvar.Get("consumer_lag")
	if v == nil {
		t.Fatal("consumer_lag not published")
	}
	m, ok := v.(*expvar.Map)
	if !ok {
		t.Fatalf("consumer_lag is %T, want *expvar.Map", v)
	}
	kv := m.Get("consumer=" + consumer + ",org=" + strconv.FormatInt(orgID, 10))
	if kv == nil {
		return -1
	}
	return kv.(*expvar.Float).Value()
}

// TestConsumerLagMetric is the S0 proof that the instrument reads reality: the
// consumer_lag gauge rises by EXACTLY the un-consumed backlog and falls to 0
// once the cursor catches up. Lag is THE health signal of the whole design, so
// this pins that it tracks the real event_log / cursor delta.
func TestConsumerLagMetric(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	orgID := seedOrg(t, pool)

	consumer := NewConsumer(pool, "lagtest", 100)
	consumer.SetMetrics(metrics.NewExpvar())

	// Nothing appended, no cursor row → zero lag.
	lag, err := consumer.Lag(ctx, orgID)
	if err != nil {
		t.Fatalf("lag (empty): %v", err)
	}
	if lag != 0 {
		t.Fatalf("empty lag = %d, want 0", lag)
	}
	if got := lagGauge(t, "lagtest", orgID); got != 0 {
		t.Fatalf("consumer_lag gauge (empty) = %v, want 0", got)
	}

	// Append a backlog and DO NOT consume: lag must rise by exactly the count.
	const backlog = 5
	var lastID int64
	for i := 0; i < backlog; i++ {
		lastID = appendInTx(t, pool, orgID, "thing.happened")
	}
	lag, err = consumer.Lag(ctx, orgID)
	if err != nil {
		t.Fatalf("lag (backlog): %v", err)
	}
	if lag != backlog {
		t.Fatalf("backlog lag = %d, want %d", lag, backlog)
	}
	if got := lagGauge(t, "lagtest", orgID); got != backlog {
		t.Fatalf("consumer_lag gauge (backlog) = %v, want %d", got, backlog)
	}

	// Consume the whole backlog and advance the cursor: lag falls to 0.
	rows, err := consumer.Poll(ctx, orgID)
	if err != nil || len(rows) != backlog {
		t.Fatalf("poll: err=%v rows=%d want %d", err, len(rows), backlog)
	}
	if err := consumer.Ack(ctx, orgID, lastID); err != nil {
		t.Fatalf("ack: %v", err)
	}
	lag, err = consumer.Lag(ctx, orgID)
	if err != nil {
		t.Fatalf("lag (drained): %v", err)
	}
	if lag != 0 {
		t.Fatalf("drained lag = %d, want 0", lag)
	}
	if got := lagGauge(t, "lagtest", orgID); got != 0 {
		t.Fatalf("consumer_lag gauge (drained) = %v, want 0", got)
	}
}

// TestConsumerLagSeesGlobalStall pins that the xmin driver's health signal can
// see its OWN failure mode.
//
// Poll's `txid < pg_snapshot_xmin(...)` gate is DATABASE-GLOBAL: one long write
// transaction anywhere freezes delivery for every org. Lag used to be measured
// through that same horizon, so during exactly that stall the max id it could
// see stopped advancing along with the cursor it subtracts — the gauge read 0
// while an unrelated org's committed backlog grew without bound. A flat green
// line reads as proof, which makes a blind signal worse than none. S4 found it
// (its spec named an "org B lag stays ~0" pin that could not go red, for this
// reason); this is the fix.
//
// Both halves of the claim are asserted, because dropping a filter must buy
// visibility and nothing else:
//   - org B's delivery really IS stalled (Poll hands back nothing) and the
//     gauge reports the backlog anyway;
//   - org A's own UNCOMMITTED event is still counted as nothing, and becomes
//     countable the instant it commits — the visibility gained is ordinary MVCC,
//     not a dirty read.
//
// RED: put `AND e.txid < pg_snapshot_xmin(pg_current_snapshot())` back into
// Consumer.Lag's WHERE clause → the during-stall assert reports lag 0.
func TestConsumerLagSeesGlobalStall(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	orgA := seedOrg(t, pool)
	var orgB int64
	if err := pool.QueryRow(ctx,
		`INSERT INTO org (name, slug) VALUES ('B', 'b') RETURNING id`).Scan(&orgB); err != nil {
		t.Fatalf("seed org B: %v", err)
	}

	consumer := NewConsumer(pool, "stallwatch", 100)
	consumer.SetMetrics(metrics.NewExpvar())

	// Org B starts caught up, so everything below is about NEW events.
	base := appendInTx(t, pool, orgB, "b.seed")
	if rows, err := consumer.Poll(ctx, orgB); err != nil || len(rows) != 1 {
		t.Fatalf("seed poll: err=%v rows=%d, want 1", err, len(rows))
	}
	if err := consumer.Ack(ctx, orgB, base); err != nil {
		t.Fatalf("seed ack: %v", err)
	}
	if lag, err := consumer.Lag(ctx, orgB); err != nil || lag != 0 {
		t.Fatalf("caught-up lag = %d (err %v), want 0", lag, err)
	}

	// Org A opens a long write transaction and holds it. The write stamps a
	// txid, and pg_snapshot_xmin cannot advance past a RUNNING transaction — so
	// this one connection pins the delivery horizon for the whole database.
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

	// Org B — an unrelated tenant — commits behind that pinned horizon.
	idB1 := appendInTx(t, pool, orgB, "b.one")
	idB2 := appendInTx(t, pool, orgB, "b.two")

	// The stall is real and not simulated: both org B events are COMMITTED, and
	// Poll still hands back nothing because their txids sit above the horizon
	// org A pinned. This is the exact condition consumer_lag exists to surface.
	rows, err := consumer.Poll(ctx, orgB)
	if err != nil {
		t.Fatalf("stalled poll: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("setup did not reproduce the stall: org B delivered %d rows "+
			"while org A's transaction is open", len(rows))
	}

	// THE assert. Lag is "newest committed id for the org, minus the durable
	// cursor", so it is idB2 - base — derived from the definition, not read off
	// the implementation. Before the fix this whole expression read 0, because
	// the max was also taken through org A's horizon and so stayed at `base`.
	wantLag := idB2 - base
	if wantLag < 2 {
		t.Fatalf("setup: org B ids did not advance (base=%d idB2=%d)", base, idB2)
	}
	lag, err := consumer.Lag(ctx, orgB)
	if err != nil {
		t.Fatalf("stalled lag: %v", err)
	}
	if lag != wantLag {
		t.Fatalf("org B lag during the global stall = %d, want %d: 2 committed, "+
			"unconsumed events (%d, %d) are piling up behind org A's open "+
			"transaction and the gauge must see them", lag, wantLag, idB1, idB2)
	}
	if got := lagGauge(t, "stallwatch", orgB); got != float64(wantLag) {
		t.Fatalf("consumer_lag{consumer=stallwatch,org=B} = %v, want %d", got, wantLag)
	}

	// No dirty read: org A's event is written but NOT committed, so it counts
	// for nothing — the gauge gained MVCC visibility, not uncommitted rows.
	if lagA, err := consumer.Lag(ctx, orgA); err != nil || lagA != 0 {
		t.Fatalf("org A lag while its own write is in flight = %d (err %v), want 0: "+
			"an uncommitted event must never be counted as backlog", lagA, err)
	}

	// Commit it, and the same event becomes countable immediately — the other
	// half of "MVCC is the only filter a measurement needs".
	if err := txA.Commit(ctx); err != nil {
		t.Fatalf("commit A: %v", err)
	}
	if lagA, err := consumer.Lag(ctx, orgA); err != nil || lagA != idA {
		t.Fatalf("org A lag after its transaction committed = %d (err %v), want %d",
			lagA, err, idA)
	}

	// Horizon released: org B's backlog delivers in id order and drains to 0.
	rows, err = consumer.Poll(ctx, orgB)
	if err != nil {
		t.Fatalf("drain poll: %v", err)
	}
	if len(rows) != 2 || rows[0].ID != idB1 || rows[1].ID != idB2 {
		t.Fatalf("drain delivered %v, want [%d %d]", rowIDs(rows), idB1, idB2)
	}
	if err := consumer.Ack(ctx, orgB, idB2); err != nil {
		t.Fatalf("drain ack: %v", err)
	}
	if lag, err := consumer.Lag(ctx, orgB); err != nil || lag != 0 {
		t.Fatalf("org B lag after draining = %d (err %v), want 0", lag, err)
	}
	if got := lagGauge(t, "stallwatch", orgB); got != 0 {
		t.Fatalf("consumer_lag{consumer=stallwatch,org=B} after draining = %v, want 0", got)
	}
}
