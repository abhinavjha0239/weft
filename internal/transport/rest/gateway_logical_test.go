package rest

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/internal/eventlog"
	"github.com/abhinavjha0239/weft/internal/gateway"
	"github.com/abhinavjha0239/weft/internal/platform/metrics"
)

// TestGatewayOnLogicalFeed is the P-45 pin: the S4 correctness proof, extended
// from the durable consumers to LIVE fan-out.
//
// S4 moved the materialisers onto the commit-ordered logical feed and left the
// gateway on the xmin gate on purpose, because moving it is a WIRE-contract
// decision: `seq` IS the event id and the live lane treated `id <= lastID` as
// "already delivered". Under commit order a lower id legitimately arrives
// after a higher one, so that test does not reorder — it DROPS. This test
// drives the crossing all the way to a WebSocket subscriber and asserts it
// arrives.
//
// RED (observed): restore the `if r.id <= c.lastID { continue }` skip in
// deliverShared (i.e. make client.passed ignore `ordered`) and the
// late-committing lower id never reaches the socket — the same undetectable
// loss S4 proved at the consumer layer, now proved at the connection.
//
// Everything else here is the non-regression half: the shared per-org read
// stays O(1) on the new driver, the ACL filter still runs per connection (the
// outsider receives NOTHING it may not see, asserted against a connection
// proven to be a live fan target), the resume lane still replays a gap through
// an id-ordered read, and no event is duplicated in steady state.
func TestGatewayOnLogicalFeed(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		cancel()
		t.Fatalf("connect: %v", err)
	}
	defer func() { cancel(); pool.Close() }()
	requireLogicalWAL(t, ctx, pool)
	resetAndMigrate(t, ctx, pool)

	opts := eventlog.LogicalOptions{Slot: "eventlog_feed_gw", Publication: "eventlog_pub_gw"}
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
	// A deferred teardown, NOT t.Cleanup: cleanups run after every deferred
	// call, so the pool would already be closed and the slot would survive the
	// test — retaining WAL for every later run.
	defer func() {
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
	}()
	readyCtx, readyCancel := context.WithTimeout(ctx, 15*time.Second)
	defer readyCancel()
	if err := src.WaitReady(readyCtx); err != nil {
		t.Fatalf("logical feed never became ready: %v", err)
	}

	hub := gateway.NewHub(pool, slog.Default())
	hub.SetMetrics(metrics.NewExpvar())
	hub.SetSource(src) // the ONE line that swaps the gateway's delivery mechanism
	go hub.Run(ctx)
	permsSvc := perms.New(pool)
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity:  identity.New(pool, permsSvc),
		Messaging: messaging.New(pool, permsSvc),
	}))
	defer ts.Close()

	var boot struct {
		OrgID     int64  `json:"org_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "gwl", "email": "a@gwl.test", "password": "password123",
		"full_name": "Alice",
	}, &boot)

	// A pool of channel members on LIVE (tail-mode) connections — the S3 fan —
	// plus one org member who is in NO channel: the ACL negative.
	const conns = 12
	memberTokens := bulkChannelMembers(t, ctx, pool, boot.OrgID, boot.ChannelID, "gwl", conns)
	subs := make([]*wsClient, conns)
	for i, tok := range memberTokens {
		subs[i] = dialClientLast(t, ctx, ts.URL, tok, "-1")
		defer subs[i].conn.CloseNow()
	}
	bob := subs[0]
	outsiderTok := bareOrgMember(t, ctx, pool, boot.OrgID, "out@gwl.test", "Outsider", "gwl-outsider")
	outsider := dialClientLast(t, ctx, ts.URL, outsiderTok, "-1")
	defer outsider.conn.CloseNow()
	for _, s := range subs {
		s.waitFor(t, "ready")
	}
	outsider.waitFor(t, "ready")

	// The outsider is a LIVE FAN TARGET on this feed — proved, not assumed, so
	// that every "the outsider heard nothing" assert below is load-bearing
	// instead of vacuous. A container-less event is org-visible by design
	// (gateway.filter), so it must reach it.
	start := time.Now()
	idWide := commitEvent(t, ctx, pool, boot.OrgID, "space.updated", nil)
	outsider.waitFor(t, "space.updated")
	bob.waitFor(t, "space.updated")

	// LIVE LATENCY, and the reason it is asserted rather than assumed: under a
	// streaming driver the LISTEN/NOTIFY wake is USELESS on its own. Append's
	// notification fires at COMMIT, before the WAL reader has decoded that
	// commit, so the woken read finds nothing and delivery silently falls back
	// to the 5s fallback sweep — a realtime plane turned into a several-second
	// one, with no error anywhere. The fix is the driver's own push wake
	// (eventlog.Tail.OnWake), and this bound is what keeps it wired.
	//
	// RED: delete the `h.tail.OnWake(h.wakeOrg)` line in gateway.SetSource —
	// delivery lands on the sweep and this assert fails at ~5s.
	if elapsed := time.Since(start); elapsed > 2500*time.Millisecond {
		t.Fatalf("live delivery on the logical feed took %v; that is the fallback sweep, "+
			"not the feed. The driver's push wake is not reaching the hub", elapsed)
	}

	// Warmup: draining one send to every member leaves the per-org reader idle
	// at the head before the counter is sampled — no sleep.
	sendChannel(t, ts.URL, boot.Token, boot.ChannelID, "warmup")
	for _, s := range subs {
		s.waitFor(t, "message.created")
	}

	// THE S3 CARRY-OVER: one send to `conns` live connections still costs ONE
	// hoisted per-org read on the new driver — the connections-axis O(1)
	// invariant is a property of the multicast reader, and swapping the feed
	// underneath it must not turn it back into a per-connection query. The
	// bound is generous because the 5s fallback sweep may land inside the
	// window; an O(N) regression would push the delta to ~conns.
	before := readPumpQueries(t)
	msgID := sendChannel(t, ts.URL, boot.Token, boot.ChannelID, "one to many")
	for i, s := range subs {
		ev := s.waitFor(t, "message.created")
		var p struct {
			MessageID int64 `json:"message_id"`
		}
		_ = json.Unmarshal(ev.Payload, &p)
		if p.MessageID != msgID {
			t.Fatalf("connection %d got message id %d, want %d", i, p.MessageID, msgID)
		}
	}
	delta := readPumpQueries(t) - before
	if delta > float64(conns)/3 {
		t.Fatalf("pump queries rose by %g for one send to %d live connections on the logical feed; "+
			"want O(1) per-org multicast, not O(N)", delta, conns)
	}
	t.Logf("multicast on the logical feed: 1 send → %d live deliveries, pump-queries +%g", conns, delta)

	// The ACL filter still runs PER CONNECTION on the shared batch.
	outsider.expectSilence(t, "message.created", 500*time.Millisecond)

	// ---- THE CROSSING ------------------------------------------------------
	// txEarly takes its txid FIRST but appends SECOND, so it holds the HIGHER
	// id; txLate holds the LOWER id and commits LAST. An id-ordered gated feed
	// hands over the higher id and can never come back for the lower one.
	txEarly, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin early: %v", err)
	}
	defer func() { _ = txEarly.Rollback(ctx) }()
	var xid string
	if err := txEarly.QueryRow(ctx, `SELECT pg_current_xact_id()::text`).Scan(&xid); err != nil {
		t.Fatalf("assign early xid: %v", err)
	}
	txLate, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin late: %v", err)
	}
	defer func() { _ = txLate.Rollback(ctx) }()
	if err := txLate.QueryRow(ctx, `SELECT pg_current_xact_id()::text`).Scan(&xid); err != nil {
		t.Fatalf("assign late xid: %v", err)
	}
	chanPayload := eventlog.MustPayload(map[string]any{"channel_id": boot.ChannelID})
	idLow := appendEvent(t, ctx, txLate, boot.OrgID, "crossing.low", chanPayload)
	idHigh := appendEvent(t, ctx, txEarly, boot.OrgID, "crossing.high", chanPayload)
	if idHigh <= idLow {
		t.Fatalf("scenario not constructed: ids %d (late tx) and %d (early tx)", idLow, idHigh)
	}
	if err := txEarly.Commit(ctx); err != nil {
		t.Fatalf("commit early: %v", err)
	}
	waitDecoded(t, ctx, src, boot.OrgID, idHigh)
	if err := txLate.Commit(ctx); err != nil {
		t.Fatalf("commit late: %v", err)
	}
	waitDecoded(t, ctx, src, boot.OrgID, idLow)

	stream := drainStream(t, bob, func(got []gateway.Envelope) bool {
		return countType(got, "crossing.low") > 0
	}, 700*time.Millisecond)
	if n := countType(stream, "crossing.low"); n != 1 {
		t.Fatalf("the live connection received the late-committing LOWER id %d times, want exactly 1: "+
			"a lower id that commits after a higher one is a legitimate event, not an already-delivered one. stream=%v",
			n, typesOf(stream))
	}
	if n := countType(stream, "crossing.high"); n != 1 {
		t.Fatalf("crossing.high delivered %d times, want exactly 1. stream=%v", n, typesOf(stream))
	}
	if got := seqOfType(stream, "crossing.low"); got != idLow {
		t.Fatalf("crossing.low arrived with seq %d, want the event id %d (seq IS the event id)", got, idLow)
	}
	if got := seqOfType(stream, "crossing.high"); got != idHigh {
		t.Fatalf("crossing.high arrived with seq %d, want the event id %d", got, idHigh)
	}
	if indexOfType(stream, "crossing.high") > indexOfType(stream, "crossing.low") {
		t.Fatalf("the socket saw the crossing in ID order, not COMMIT order: %v", typesOf(stream))
	}
	// Every other live member saw it too — the crossing rides the SHARED fan,
	// not one lucky connection.
	for i, s := range subs[1:] {
		if ev := s.waitFor(t, "crossing.low"); ev.Seq != idLow {
			t.Fatalf("connection %d got crossing.low with seq %d, want %d", i+1, ev.Seq, idLow)
		}
	}
	// ...and the outsider still saw neither, on a connection just proved live.
	outsider.expectSilence(t, "crossing.low", 500*time.Millisecond)
	outsider.expectSilence(t, "crossing.high", 300*time.Millisecond)

	// ---- THE HAND-OFF DUPLICATE CASE ---------------------------------------
	// A connection resumes with a last_id ABOVE an event that commits later.
	// The contract says that event must arrive (a duplicate for a client that
	// already applied it), never be dropped.
	txHold, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin hold: %v", err)
	}
	defer func() { _ = txHold.Rollback(ctx) }()
	idLow2 := appendEvent(t, ctx, txHold, boot.OrgID, "handoff.low", chanPayload)
	idHigh2 := commitEvent(t, ctx, pool, boot.OrgID, "handoff.high", chanPayload)
	if idHigh2 <= idLow2 {
		t.Fatalf("hand-off scenario not constructed: %d then %d", idLow2, idHigh2)
	}
	// Synchronise on the ORG READER, not just the decoder: once a live
	// connection has been handed handoff.high, the shard's position is past it,
	// so the connection dialed below cannot legitimately be sent it again and
	// the "exactly one event" assert is exact rather than racy.
	bob.waitFor(t, "handoff.high")

	resumer := dialClientLast(t, ctx, ts.URL, memberTokens[1], fmt.Sprintf("%d", idHigh2))
	defer resumer.conn.CloseNow()
	resumer.waitFor(t, "ready")
	if err := txHold.Commit(ctx); err != nil {
		t.Fatalf("commit hold: %v", err)
	}
	waitDecoded(t, ctx, src, boot.OrgID, idLow2)

	handoff := drainStream(t, resumer, func(got []gateway.Envelope) bool {
		return countType(got, "handoff.low") > 0
	}, 700*time.Millisecond)
	if n := countType(handoff, "handoff.low"); n != 1 {
		t.Fatalf("a connection resumed at %d received the later-committing lower id %d %d times, "+
			"want exactly 1 (a duplicate is allowed, a DROP is not). stream=%v",
			idHigh2, idLow2, n, typesOf(handoff))
	}
	if n := countType(handoff, "handoff.high"); n != 0 {
		t.Fatalf("the hand-off re-sent %d event(s) the resume point already covered; "+
			"duplicates are bounded to the overlap, not a storm. stream=%v", n, typesOf(handoff))
	}

	// ---- THE RESUME LANE ---------------------------------------------------
	// A connection reconnecting far behind replays its gap through the
	// id-ordered history read and then joins the live lane. Everything after
	// its resume point must arrive EXACTLY once — including both crossing
	// events, whose ids are out of commit order.
	replayer := dialClientLast(t, ctx, ts.URL, memberTokens[2], fmt.Sprintf("%d", idWide))
	defer replayer.conn.CloseNow()
	replayer.waitFor(t, "ready")
	replayed := drainStream(t, replayer, func(got []gateway.Envelope) bool {
		return countType(got, "handoff.low") > 0 && countType(got, "handoff.high") > 0
	}, 700*time.Millisecond)
	for _, verb := range []string{"crossing.low", "crossing.high", "handoff.low", "handoff.high"} {
		if n := countType(replayed, verb); n != 1 {
			t.Fatalf("resume replay delivered %q %d times, want exactly 1. stream=%v",
				verb, n, typesOf(replayed))
		}
	}
	if n := countType(replayed, "message.created"); n != 2 {
		t.Fatalf("resume replay delivered %d message.created, want the 2 sent after the resume point. stream=%v",
			n, typesOf(replayed))
	}
	if n := countType(replayed, "space.updated"); n != 0 {
		t.Fatalf("resume replay re-sent the event AT the resume point (%d): last_id is exclusive", idWide)
	}

	// ---- STEADY STATE ------------------------------------------------------
	// Live traffic after all that: each send arrives once, in order, with seq
	// still the event id.
	for i := 0; i < 3; i++ {
		sendChannel(t, ts.URL, boot.Token, boot.ChannelID, fmt.Sprintf("steady %d", i))
	}
	steady := drainStream(t, bob, func(got []gateway.Envelope) bool {
		return countType(got, "message.created") >= 3
	}, 700*time.Millisecond)
	if n := countType(steady, "message.created"); n != 3 {
		t.Fatalf("3 sends produced %d deliveries in steady state: duplicates are bounded to the "+
			"hand-off, not the steady lane. stream=%v", n, typesOf(steady))
	}
	var last int64
	for _, e := range steady {
		if e.Type != "message.created" {
			continue
		}
		if e.Seq <= last {
			t.Fatalf("steady-state seqs are not ascending: %d after %d", e.Seq, last)
		}
		last = e.Seq
	}

	// PROOF OF SWAP: the durable consumers prove the driver swap with an LSN
	// cursor; the gateway holds no cursor at all (ADR-002), so the proof is
	// behavioural — the crossing above is impossible on the xmin gate, and the
	// slot's confirmed position must have advanced, i.e. this cell really did
	// stream the WAL rather than fall back to the poller.
	var slotActive bool
	if err := pool.QueryRow(ctx,
		`SELECT active FROM pg_replication_slots WHERE slot_name = $1`, opts.Slot).Scan(&slotActive); err != nil {
		t.Fatalf("slot state: %v", err)
	}
	if !slotActive {
		t.Fatal("the replication slot was never active: the run did not stream the logical feed")
	}
}

// commitEvent appends one event in its own transaction and returns its id.
func commitEvent(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID int64,
	verb string, payload json.RawMessage) int64 {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	id := appendEvent(t, ctx, tx, orgID, verb, payload)
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	return id
}

func appendEvent(t *testing.T, ctx context.Context, tx pgx.Tx, orgID int64,
	verb string, payload json.RawMessage) int64 {
	t.Helper()
	id, err := eventlog.Append(ctx, tx, eventlog.Event{
		OrgID: orgID, ActorKind: enum.ActorSystem,
		EntityType: enum.EntityMessage, EntityID: 1, Verb: verb, Payload: payload,
	})
	if err != nil {
		t.Fatalf("append %s: %v", verb, err)
	}
	return id
}

func waitDecoded(t *testing.T, ctx context.Context, src *eventlog.LogicalSource, orgID, id int64) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := src.WaitFor(waitCtx, orgID, id); err != nil {
		t.Fatalf("feed never decoded event %d: %v", id, err)
	}
}

// drainStream reads a connection's envelopes until done() is satisfied, then
// keeps reading for `quiet` longer so a LATE duplicate still lands in the
// result. Assertions then run over the whole stream — "exactly once" cannot
// pass by looking away at the right moment.
func drainStream(t *testing.T, c *wsClient, done func([]gateway.Envelope) bool,
	quiet time.Duration) []gateway.Envelope {
	t.Helper()
	var out []gateway.Envelope
	deadline := time.After(15 * time.Second)
	for !done(out) {
		select {
		case e, ok := <-c.events:
			if !ok {
				t.Fatalf("connection closed mid-stream; got %v", typesOf(out))
			}
			out = append(out, e)
		case <-deadline:
			t.Fatalf("timed out draining the stream; got %v", typesOf(out))
		}
	}
	settle := time.After(quiet)
	for {
		select {
		case e, ok := <-c.events:
			if !ok {
				return out
			}
			out = append(out, e)
		case <-settle:
			return out
		}
	}
}

func countType(evts []gateway.Envelope, typ string) int {
	n := 0
	for _, e := range evts {
		if e.Type == typ {
			n++
		}
	}
	return n
}

func seqOfType(evts []gateway.Envelope, typ string) int64 {
	for _, e := range evts {
		if e.Type == typ {
			return e.Seq
		}
	}
	return 0
}

func indexOfType(evts []gateway.Envelope, typ string) int {
	for i, e := range evts {
		if e.Type == typ {
			return i
		}
	}
	return -1
}

func typesOf(evts []gateway.Envelope) []string {
	out := make([]string, len(evts))
	for i, e := range evts {
		out[i] = fmt.Sprintf("%s#%d", e.Type, e.Seq)
	}
	return out
}
