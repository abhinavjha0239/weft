package rest

import (
	"context"
	"fmt"
	"log/slog"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/domain/automation"
	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/notification"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/domain/unfurl"
	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/internal/eventlog"
	"github.com/abhinavjha0239/weft/internal/gateway"
	"github.com/abhinavjha0239/weft/internal/platform/egress"
)

// longSweep is a fallback sweep interval far outside every bound this file
// asserts. With it set, NOTHING inside the test's lifetime can deliver an
// event except a wake — which is what turns the latency assert below into a
// measurement of the driver's push wake instead of a measurement of luck.
const longSweep = 60 * time.Second

// TestConsumerPushWakeOnLogicalFeed pins the S4 latency claim for the DURABLE
// consumers, which is the half #124 left unearned.
//
// P-45 found the mechanism the hard way: LISTEN/NOTIFY is not a DEPENDABLE wake
// for a streaming driver, because Append's notification fires at COMMIT while
// the events only become readable once the WAL reader DECODES that commit a
// moment later. A NOTIFY-woken Poll can therefore read nothing and leave the
// consumer waiting for its fallback sweep, with no error anywhere to say so.
// P-45 wired the driver's own push wake into the gateway and left the
// notification, automation and unfurl runners on NOTIFY; this is that follow-up.
//
// All THREE durable runners run here on ONE logical source, each with its own
// cursor and its own OnWake subscription, and a fallback sweep 60s away.
//
// The test has two phases, and only the second is load-bearing for the wake:
//
//   - END TO END: a real mention through the REST API. This proves the real
//     materialiser produces the real row on this driver with all three runners
//     live — but it does NOT isolate WHICH wake delivered it. The NOTIFY lane
//     races the WAL decoder, and on a fast local box it often wins (measured:
//     ~13ms with the push wake removed entirely). A latency bound here would be
//     a bound on that race, not a pin on this slice.
//   - THE PUSH WAKE, ISOLATED: an append whose NOTIFY is SUPPRESSED, so the
//     LISTEN lane cannot deliver it at all and the sweep is a minute away. Then
//     the driver's push wake is the only mechanism left, and the latency bound
//     measures exactly it.
//
// RED (observed): delete the `c.OnWake(r.wake.Signal)` line from all three
// SetSource methods — phase one still passes (that is the point of splitting
// them), and phase two never finishes, because the cursors sit behind the event
// until the 60s sweep. That is exactly the "materialisation latency is a sweep
// interval" gap docs/REALITY.md recorded.
func TestConsumerPushWakeOnLogicalFeed(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	pool := runnerPool(t, ctx, dbURL)
	defer func() { cancel(); pool.Close() }()
	requireLogicalWAL(t, ctx, pool)
	resetAndMigrate(t, ctx, pool)

	src, stopFeed := startLogicalFeed(t, ctx, pool, eventlog.LogicalOptions{
		Slot: "eventlog_feed_wake", Publication: "eventlog_pub_wake"})
	defer stopFeed()

	log := slog.Default()
	permsSvc := perms.New(pool)
	msgSvc := messaging.New(pool, permsSvc)
	msgSvc.SetDeliverability(notification.NewDeliverability(pool, log))
	notifSvc := notification.New(pool)
	// The hub is wired as the materialiser's Fanout but deliberately NOT Run:
	// this test opens no socket, the gateway's own behaviour on this driver is
	// pinned by TestGatewayOnLogicalFeed, and hub.Run would hold a fourth
	// pooled connection in a LISTEN loop for the whole test (see runnerPool).
	hub := gateway.NewHub(pool, log)

	// All THREE durable runners on the SAME driver. Each mints its own named
	// consumer and registers its own wake callback, so this also pins that the
	// reader's subscribers ACCUMULATE: a second, third and fourth registrant
	// must not displace the first.
	notifRunner := notification.NewRunner(pool, hub, log)
	notifRunner.SweepInterval = longSweep
	notifRunner.SetSource(src)
	autoRunner := automation.NewRunner(pool, msgSvc, permsSvc, notifSvc, log)
	autoRunner.SweepInterval = longSweep
	autoRunner.SetSource(src)
	// No egress options: the unfurl lane never dials here (the org toggle is
	// off by default, so handle returns before any fetch) — what is under test
	// is its CURSOR moving, not a preview.
	unfurlRunner := unfurl.NewRunner(pool, unfurl.New(pool, egress.New(egress.Options{})), log)
	unfurlRunner.SweepInterval = longSweep
	unfurlRunner.SetSource(src)

	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: log,
		Identity:      identity.New(pool, permsSvc),
		Messaging:     msgSvc,
		Notifications: notifSvc,
	}))
	defer ts.Close()

	var boot struct {
		OrgID     int64  `json:"org_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "pwk", "email": "a@pwk.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"bob@pwk.test", "Bob Ray", "bobpwktok")
	var bobID int64
	if err := pool.QueryRow(ctx,
		`SELECT id FROM user_account WHERE org_id=$1 AND email='bob@pwk.test'`,
		boot.OrgID).Scan(&bobID); err != nil {
		t.Fatalf("bob id: %v", err)
	}

	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	go notifRunner.Run(runCtx)
	go autoRunner.Run(runCtx)
	go unfurlRunner.Run(runCtx)

	// WARMUP. A cursor with no LSN takes this driver's BOOTSTRAP lane (an
	// id-ordered walk of history), so drive that to completion first: the
	// measurement below then times the LIVE lane, which is the steady state an
	// operator actually runs in. Reaching a non-NULL LSN at all is only
	// possible through the logical driver, so this doubles as the proof of swap.
	var warm struct {
		MessageID int64 `json:"message_id"`
	}
	postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, boot.ChannelID),
		boot.Token, map[string]any{"content": "warmup"}, &warm)
	waitConsumersLive(t, ctx, pool, boot.OrgID, 25*time.Second)

	// ---- PHASE ONE: END TO END ---------------------------------------------
	// The real materialiser, the real API, all three runners live on the
	// logical driver, and a fallback sweep a minute away. What this pins is the
	// STATE — the mention row, its kind, its anchor, and every cursor past the
	// event. It deliberately carries no tight latency bound: the NOTIFY lane
	// races the WAL decoder here and either lane may win.
	var sent struct {
		MessageID int64 `json:"message_id"`
	}
	postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, boot.ChannelID),
		boot.Token, map[string]any{"content": "@**Bob Ray** ship it"}, &sent)
	head := maxEventID(t, ctx, pool, boot.OrgID)
	waitConsumersPast(t, ctx, pool, boot.OrgID, head, 20*time.Second,
		func() bool { return mentionCount(t, ctx, pool, boot.OrgID, bobID) > 0 })

	var entityID int64
	if err := pool.QueryRow(ctx, `
		SELECT entity_id FROM notification
		WHERE org_id=$1 AND user_id=$2 AND kind=$3`,
		boot.OrgID, bobID, notification.KindMention).Scan(&entityID); err != nil {
		t.Fatalf("no mention row of kind %d: %v", notification.KindMention, err)
	}
	if entityID != sent.MessageID {
		t.Fatalf("mention anchored on entity %d, want message %d", entityID, sent.MessageID)
	}

	// ---- PHASE TWO: THE PUSH WAKE, ISOLATED --------------------------------
	// Let every NOTIFY-started pass above finish first. Without this a pass that
	// is still draining could sweep up the probe event below and the measurement
	// would be of the wrong mechanism. (The RED run confirms it does not: with
	// OnWake removed the probe is never delivered at all.)
	time.Sleep(250 * time.Millisecond)

	// A dedicated LISTENer, so the "the NOTIFY lane was not what delivered
	// this" claim is an OBSERVATION rather than an assumption about the wake
	// function's memo.
	//
	// It DRAINS continuously on its own goroutine rather than sitting idle and
	// being read once at the end. Two reasons, both real: a LISTENing backend
	// that never consumes is what pins the server's async notify queue (the
	// queue can only be trimmed past its slowest listener), which is a hazard to
	// every other test sharing the cluster; and recording the whole window is a
	// STRONGER assertion than a single poll, because it cannot miss a
	// notification that arrived and was then overtaken.
	probe, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire notify probe: %v", err)
	}
	if _, err := probe.Exec(ctx, "LISTEN "+eventlog.NotifyChannel); err != nil {
		probe.Release()
		t.Fatalf("listen: %v", err)
	}
	probeCtx, probeCancel := context.WithCancel(ctx)
	var probeMu sync.Mutex
	var heard []string
	probeDone := make(chan struct{})
	go func() {
		defer close(probeDone)
		for {
			n, err := probe.Conn().WaitForNotification(probeCtx)
			if err != nil {
				return // the window closed (or the conn died); nothing more to record
			}
			probeMu.Lock()
			heard = append(heard, n.Channel+"/"+n.Payload)
			probeMu.Unlock()
		}
	}()
	// Ordered before the pool's own deferred Close: pgxpool.Close blocks until
	// every acquired connection is released, so a t.Fatal below must not be able
	// to strand this one and turn a failure into a hang.
	defer func() {
		probeCancel()
		<-probeDone
		probe.Release()
	}()

	const bound = 1500 * time.Millisecond
	for round := 1; round <= 2; round++ {
		// Timed from the moment the append's COMMIT returned, which is the
		// earliest instant any wake for it can fire.
		silent := appendWithoutWake(t, ctx, pool, boot.OrgID,
			fmt.Sprintf("wake.probe.%d", round))
		elapsed := waitConsumersPast(t, ctx, pool, boot.OrgID, silent, 20*time.Second, nil)
		t.Logf("push wake, isolated (round %d): all three cursors passed event %d in %v "+
			"(NOTIFY suppressed, fallback sweep %v)", round, silent, elapsed, longSweep)
		if elapsed > bound {
			t.Fatalf("round %d: with the NOTIFY lane suppressed, delivery took %v, want under "+
				"%v — that is sweep-interval latency, not feed latency", round, elapsed, bound)
		}
	}
	// Two rounds, because a wake that fires once and then goes silent (a stale
	// token left in the coalescing queue) would pass a single round.

	// The suppression really held: NOTHING was notified across the whole probe
	// window. NOTIFY is transactional and delivered at COMMIT, so a settle pass
	// is enough to catch a straggler that the drain has not yet recorded.
	time.Sleep(250 * time.Millisecond)
	probeMu.Lock()
	got := append([]string(nil), heard...)
	probeMu.Unlock()
	if len(got) > 0 {
		t.Fatalf("the probe appends notified %v after all: the isolated measurement above "+
			"could have been the LISTEN lane, not the push wake", got)
	}
}

// appendWithoutWake appends one real event through the real producer path with
// its LISTEN/NOTIFY wake SUPPRESSED, and returns the event id.
//
// The suppression is not a code patch: it claims the org's wake slot in
// event_log_wake's own per-(transaction, org) memo (migration 0024) before
// appending, so Append's RETURNING call correctly declines to signal a second
// time for this org. The row itself is written by eventlog.Append exactly as
// production writes it — txid stamp, defaults and all — and the WAL reader
// decodes it exactly as it decodes any other commit. Only the notification is
// missing, which is what leaves the driver's push wake as the sole live wake.
func appendWithoutWake(t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	orgID int64, verb string) int64 {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx,
		fmt.Sprintf("SET LOCAL eventlog.notified = ',%d,'", orgID)); err != nil {
		t.Fatalf("claim the wake memo: %v", err)
	}
	id, err := eventlog.Append(ctx, tx, eventlog.Event{
		OrgID: orgID, ActorKind: enum.ActorSystem,
		EntityType: enum.EntityMessage, EntityID: 1, Verb: verb,
	})
	if err != nil {
		t.Fatalf("append %s: %v", verb, err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	return id
}

// waitConsumersPast blocks until every wakeConsumer's durable cursor has passed
// eventID (and `also`, when given, reports true), and returns how long that
// took. It FAILS with the diagnosis rather than the symptom: which consumers
// are still behind, and how far the fallback sweep still is.
func waitConsumersPast(t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	orgID, eventID int64, within time.Duration, also func() bool) time.Duration {
	t.Helper()
	start := time.Now()
	deadline := start.Add(within)
	for {
		behind := consumersBehind(t, ctx, pool, orgID, eventID)
		if len(behind) == 0 && (also == nil || also()) {
			return time.Since(start)
		}
		if time.Now().After(deadline) {
			t.Fatalf("waited %v: consumers still behind event %d = %v (side condition met=%v). "+
				"The fallback sweep is %v away, so a wake is the only thing that could have "+
				"delivered this", time.Since(start), eventID, behind, also == nil || also(), longSweep)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestConsumerWakeLossConvergesOnSweep is the correctness half of the same
// slice. A wake is a HINT, so losing every one of them must cost LATENCY and
// nothing else — the durable cursor plus the fallback sweep are what make that
// true, and this pins it instead of asserting it.
//
// The loss is CONSTRUCTED, not simulated. The mention commits and is DECODED
// while the logical source has ZERO wake subscribers, so the reader's push wake
// for that commit fires into nothing and can never be replayed. The runner is
// built and SetSource'd only afterwards: its subscription exists, but no wake
// will ever name that commit again. Its LISTEN wake is equally dead — Postgres
// does not replay a notification to a connection that was not listening when it
// fired. The fallback sweep is therefore the ONLY mechanism left standing.
//
// RED (observed): delete `go r.sweep(ctx)` from notification.Runner.Run and the
// mention never materialises. (That deletion leaves the latency pin above
// green, which is the point: the two mechanisms are independent.)
func TestConsumerWakeLossConvergesOnSweep(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	pool := runnerPool(t, ctx, dbURL)
	defer func() { cancel(); pool.Close() }()
	requireLogicalWAL(t, ctx, pool)
	resetAndMigrate(t, ctx, pool)

	src, stopFeed := startLogicalFeed(t, ctx, pool, eventlog.LogicalOptions{
		Slot: "eventlog_feed_wakeloss", Publication: "eventlog_pub_wakeloss"})
	defer stopFeed()

	log := slog.Default()
	permsSvc := perms.New(pool)
	msgSvc := messaging.New(pool, permsSvc)
	msgSvc.SetDeliverability(notification.NewDeliverability(pool, log))
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Log: log,
		Identity:      identity.New(pool, permsSvc),
		Messaging:     msgSvc,
		Notifications: notification.New(pool),
	}))
	defer ts.Close()

	var boot struct {
		OrgID     int64  `json:"org_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "wls", "email": "a@wls.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"bob@wls.test", "Bob Ray", "bobwlstok")
	var bobID int64
	if err := pool.QueryRow(ctx,
		`SELECT id FROM user_account WHERE org_id=$1 AND email='bob@wls.test'`,
		boot.OrgID).Scan(&bobID); err != nil {
		t.Fatalf("bob id: %v", err)
	}

	// The mention commits with NO consumer wired at all.
	var sent struct {
		MessageID int64 `json:"message_id"`
	}
	postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, boot.ChannelID),
		boot.Token, map[string]any{"content": "@**Bob Ray** ship it"}, &sent)
	head := maxEventID(t, ctx, pool, boot.OrgID)
	// Decoded — so the reader's wake for this commit has already fired, into a
	// source with zero subscribers. It is gone.
	waitDecoded(t, ctx, src, boot.OrgID, head)
	if n := mentionCount(t, ctx, pool, boot.OrgID, bobID); n != 0 {
		t.Fatalf("%d mention rows exist with no runner wired: the test cannot tell a lost "+
			"wake from a delivered one", n)
	}

	runner := notification.NewRunner(pool, nil, log)
	runner.SweepInterval = 300 * time.Millisecond
	// Subscribed AFTER the commit was decoded: this callback will never be
	// invoked for the event under test.
	runner.SetSource(src)
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	start := time.Now()
	go runner.Run(runCtx)

	deadline := time.Now().Add(15 * time.Second)
	var elapsed time.Duration
	for {
		// Both, because the row alone can be observed mid-pass, before the ack:
		// the claim is that the sweep CONVERGES, i.e. it also commits progress.
		if mentionCount(t, ctx, pool, boot.OrgID, bobID) > 0 &&
			cursorAt(t, ctx, pool, "notifications", boot.OrgID) >= head {
			elapsed = time.Since(start)
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the mention never materialised in %v with the wake lost (cursor at %d, "+
				"event %d): the fallback sweep (%v) is the backstop that makes a wake a HINT "+
				"rather than a delivery, and it did not converge", time.Since(start),
				cursorAt(t, ctx, pool, "notifications", boot.OrgID), head, runner.SweepInterval)
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Logf("wake lost: the fallback sweep converged in %v (sweep interval %v)",
		elapsed, runner.SweepInterval)

	// STATE: the same row the wake path produces, and the cursor really did
	// advance past the event through the LOGICAL driver (a non-NULL LSN).
	var entityID int64
	if err := pool.QueryRow(ctx, `
		SELECT entity_id FROM notification
		WHERE org_id=$1 AND user_id=$2 AND kind=$3`,
		boot.OrgID, bobID, notification.KindMention).Scan(&entityID); err != nil {
		t.Fatalf("mention row: %v", err)
	}
	if entityID != sent.MessageID {
		t.Fatalf("mention anchored on entity %d, want message %d", entityID, sent.MessageID)
	}
	var lsn *string
	if err := pool.QueryRow(ctx, `
		SELECT lsn::text FROM event_consumer_cursor
		WHERE consumer='notifications' AND org_id=$1`, boot.OrgID).Scan(&lsn); err != nil {
		t.Fatalf("notifications cursor: %v", err)
	}
	if lsn == nil {
		t.Fatal("the notifications cursor has no LSN: the sweep converged on the xmin poller, " +
			"not on the logical feed")
	}
}

// cursorAt reports a consumer's durable last_id for one org (0 when it has no
// cursor row yet).
func cursorAt(t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	consumer string, orgID int64) int64 {
	t.Helper()
	var at int64
	if err := pool.QueryRow(ctx, `
		SELECT COALESCE((SELECT last_id FROM event_consumer_cursor
		                 WHERE consumer = $1 AND org_id = $2), 0)`,
		consumer, orgID).Scan(&at); err != nil {
		t.Fatalf("cursor for %s: %v", consumer, err)
	}
	return at
}

// runnerPool builds the harness pool through db.Connect — the PRODUCTION
// constructor — instead of pgxpool.New, and then asserts the budget it needs.
//
// This is not incidental tidiness; it is the fix for a real failure. A test
// that starts N event-log runners holds ONE pooled connection per runner for
// the entire life of its LISTEN loop (notification/automation/unfurl
// listenLoop, and gateway.Hub.listenLoop, all `pool.Acquire` then park in
// WaitForNotification). pgxpool defaults MaxConns to max(4, NumCPU), which is
// 4 on a CI runner and 8+ on a developer laptop — so a composition holding
// four of them passes locally and STARVES on CI, where it presents not as an
// error but as an API call that blocks for minutes and then succeeds once the
// test's own context expires and the listen loops release. (Observed: a
// message POST with ms=148733, and `pg_stat_activity` showing exactly four
// backends, all idle on `LISTEN event_log`, with pg_notification_queue_usage()
// at 0 — the request never reached Postgres at all.)
//
// db.Connect floors MaxConns at 25 for precisely this reason, so a test that
// composes what `weftd serve` composes must build its pool the same way rather
// than on a budget production refuses to run under. The explicit check below
// keeps the requirement stated and self-diagnosing instead of latent.
func runnerPool(t *testing.T, ctx context.Context, dbURL string) *pgxpool.Pool {
	t.Helper()
	pool, err := db.Connect(ctx, dbURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	// Three runner LISTEN loops + the NOTIFY probe + whatever request is in
	// flight, with headroom for each runner's sweep and poll queries.
	const need = 8
	if got := pool.Config().MaxConns; got < need {
		pool.Close()
		t.Fatalf("harness pool has MaxConns=%d, need at least %d: this composition parks "+
			"one connection per runner in a LISTEN loop, so a smaller pool starves the "+
			"request path instead of failing (that is what broke CI)", got, need)
	}
	return pool
}

// startLogicalFeed provisions this cell's slot + publication, starts the WAL
// reader, and waits for it to stream. The returned stop MUST be deferred by the
// caller (not registered with t.Cleanup): cleanups run after every deferred
// call, so the pool would already be closed and the slot would survive the test
// — retaining WAL for every later run.
func startLogicalFeed(t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	opts eventlog.LogicalOptions) (*eventlog.LogicalSource, func()) {
	t.Helper()
	if err := eventlog.DropLogical(ctx, pool, opts); err != nil {
		t.Fatalf("pre-clean slot: %v", err)
	}
	if err := eventlog.ProvisionLogical(ctx, pool, opts); err != nil {
		t.Fatalf("provision slot: %v", err)
	}
	src, err := eventlog.NewLogicalSource(pool, slog.Default(), opts)
	if err != nil {
		t.Fatalf("logical source: %v", err)
	}
	feedCtx, feedCancel := context.WithCancel(ctx)
	feedDone := make(chan struct{})
	go func() { defer close(feedDone); src.Run(feedCtx) }()
	stop := func() {
		feedCancel()
		<-feedDone
		for i := 0; i < 250; i++ {
			if err := eventlog.DropLogical(context.Background(), pool, opts); err == nil {
				var still bool
				_ = pool.QueryRow(context.Background(),
					`SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)`,
					opts.Slot).Scan(&still)
				if !still {
					return
				}
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Errorf("test slot %q outlived the test: it will retain WAL", opts.Slot)
	}
	readyCtx, readyCancel := context.WithTimeout(ctx, 15*time.Second)
	defer readyCancel()
	if err := src.WaitReady(readyCtx); err != nil {
		stop()
		t.Fatalf("logical feed never became ready: %v", err)
	}
	return src, stop
}

// wakeConsumers are the three durable event-log consumers this slice wires the
// driver's push wake into. Names are the cursor identities, so this list is
// also what event_consumer_cursor is keyed by.
var wakeConsumers = []string{"notifications", "automations", "unfurl"}

// waitConsumersLive blocks until every wakeConsumer has left this driver's
// bootstrap lane, i.e. carries a real LSN cursor.
func waitConsumersLive(t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	orgID int64, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		var live int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM event_consumer_cursor
			WHERE org_id = $1 AND lsn IS NOT NULL AND consumer = ANY($2::text[])`,
			orgID, wakeConsumers).Scan(&live); err != nil {
			t.Fatalf("cursor lsn: %v", err)
		}
		if live == len(wakeConsumers) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d consumers reached the logical feed's live lane in %v: "+
				"an LSN cursor is the only proof the driver swap took effect",
				live, len(wakeConsumers), within)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// consumersBehind names the wakeConsumers whose durable cursor has not reached
// eventID. Empty means every one of them has processed and acked past it.
func consumersBehind(t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	orgID, eventID int64) []string {
	t.Helper()
	rows, err := pool.Query(ctx, `
		SELECT c.name, COALESCE(cur.last_id, 0)
		FROM unnest($2::text[]) AS c(name)
		LEFT JOIN event_consumer_cursor cur
		  ON cur.consumer = c.name AND cur.org_id = $1
		WHERE COALESCE(cur.last_id, 0) < $3`, orgID, wakeConsumers, eventID)
	if err != nil {
		t.Fatalf("cursor scan: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		var at int64
		if err := rows.Scan(&name, &at); err != nil {
			t.Fatalf("cursor scan: %v", err)
		}
		out = append(out, fmt.Sprintf("%s@%d", name, at))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("cursor scan: %v", err)
	}
	return out
}

func maxEventID(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID int64) int64 {
	t.Helper()
	var id int64
	if err := pool.QueryRow(ctx,
		`SELECT COALESCE(max(id), 0) FROM event_log WHERE org_id = $1`, orgID).Scan(&id); err != nil {
		t.Fatalf("max event id: %v", err)
	}
	return id
}

func mentionCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID, userID int64) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM notification
		WHERE org_id = $1 AND user_id = $2 AND kind = $3`,
		orgID, userID, notification.KindMention).Scan(&n); err != nil {
		t.Fatalf("mention count: %v", err)
	}
	return n
}
