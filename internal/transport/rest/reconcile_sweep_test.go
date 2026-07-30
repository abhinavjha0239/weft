package rest

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/notification"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/eventlog"
	"github.com/abhinavjha0239/weft/internal/gateway"
)

// sweepFixture is the shared stage for the reconcile-sweep pins: one org, one
// channel, alice sending and bob receiving, both hourly caches populated and
// the notification consumer drained so the maintained state is settled truth
// before a test corrupts it.
type sweepFixture struct {
	pool      *pgxpool.Pool
	ts        *httptest.Server
	msg       *messaging.Service
	deliv     *notification.Deliverability
	orgID     int64
	channelID int64
	aliceTok  string
	bobID     int64
	drain     func()
	// sent is how many messages alice sent to the channel; bob has read none,
	// so it is ALSO bob's true unread count — derived from the test's own
	// actions, never read back from the counter under test.
	sent int
}

func newSweepFixture(t *testing.T, ctx context.Context, pool *pgxpool.Pool, slug string) *sweepFixture {
	t.Helper()
	hub := gateway.NewHub(pool, slog.Default())
	go hub.Run(ctx)
	permsSvc := perms.New(pool)
	msgSvc := messaging.New(pool, permsSvc)
	delivSvc := notification.NewDeliverability(pool, slog.Default())
	msgSvc.SetDeliverability(delivSvc)
	runner := notification.NewRunner(pool, hub, slog.Default())
	runner.SetUnread(msgSvc)
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity:      identity.New(pool, permsSvc),
		Messaging:     msgSvc,
		Notifications: notification.New(pool),
	}))
	t.Cleanup(ts.Close)

	var boot struct {
		OrgID     int64  `json:"org_id"`
		UserID    int64  `json:"user_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": slug, "email": "alice@" + slug + ".test",
		"password": "password123", "full_name": "Alice Chen",
	}, &boot)
	bobTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"bob@"+slug+".test", "Bob Ray", "bob"+slug+"tok")
	var bobID int64
	if err := pool.QueryRow(ctx,
		`SELECT id FROM user_account WHERE org_id=$1 AND email=$2`,
		boot.OrgID, "bob@"+slug+".test").Scan(&bobID); err != nil {
		t.Fatalf("bob id: %v", err)
	}
	f := &sweepFixture{
		pool: pool, ts: ts, msg: msgSvc, deliv: delivSvc,
		orgID: boot.OrgID, channelID: boot.ChannelID, aliceTok: boot.Token,
		bobID: bobID,
		drain: func() {
			drainConsumer(t, ctx, pool, "notifications", boot.OrgID, runner.ProcessOrg)
		},
	}
	// Two messages: builds the channel's deliverability set (the lazy first
	// build rides the consumer) and mints bob's counter row at 2.
	sendChannel(t, ts.URL, boot.Token, boot.ChannelID, "one")
	sendChannel(t, ts.URL, boot.Token, boot.ChannelID, "two")
	f.sent = 2
	f.drain()
	// bob takes level=all so he holds a reason-2 deliverability row for the
	// set sweep to lose and re-derive.
	if code := putJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/notification", ts.URL, boot.ChannelID),
		bobTok, map[string]any{"level": 1}); code != http.StatusOK {
		t.Fatalf("bob level=all = %d, want 200", code)
	}
	f.drain()
	if got := f.setRows(t, ctx); got != 1 {
		t.Fatalf("fixture: bob holds %d reason-2 set rows, want 1 "+
			"(the sweep pins need a row to lose)", got)
	}
	if u, _ := counterRow(t, ctx, pool, bobID, boot.ChannelID); u != f.sent {
		t.Fatalf("fixture: bob's counter = %d, want %d", u, f.sent)
	}
	return f
}

// setRows counts bob's level=all (reason 2) deliverability rows.
func (f *sweepFixture) setRows(t *testing.T, ctx context.Context) int {
	t.Helper()
	var n int
	if err := f.pool.QueryRow(ctx, `
		SELECT count(*) FROM channel_deliverability
		WHERE channel_id = $1 AND user_id = $2 AND reason = 2 AND medium = 1`,
		f.channelID, f.bobID).Scan(&n); err != nil {
		t.Fatalf("count set rows: %v", err)
	}
	return n
}

// seedDrift corrupts BOTH hourly caches in the way each sweep exists to
// repair: an unread counter far from truth, and a lost deliverability row
// (the miss-capable direction — a row that never appears is a notification
// nobody gets).
func (f *sweepFixture) seedDrift(t *testing.T, ctx context.Context, unread int) {
	t.Helper()
	if _, err := f.pool.Exec(ctx, `
		UPDATE container_unread_counter SET unread_count = $3
		WHERE user_id = $1 AND channel_id = $2`, f.bobID, f.channelID, unread); err != nil {
		t.Fatalf("seed counter drift: %v", err)
	}
	if _, err := f.pool.Exec(ctx, `
		DELETE FROM channel_deliverability
		WHERE channel_id = $1 AND user_id = $2 AND reason = 2`,
		f.channelID, f.bobID); err != nil {
		t.Fatalf("seed set drift: %v", err)
	}
}

// TestReconcileSweepClaim pins the multi-node exclusion on both hourly
// reconcile sweeps. Every runner instance starts its own reconcileLoop, so
// before this an N-node cell ran N redundant full passes per window, all
// contending on the same channel row locks the live patch path needs.
//
// The proof is STATE, not timing: a second node holds the sweep's claim while
// the sweep runs, and the seeded drift must SURVIVE untouched — that is the
// only way to observe "this pass did no work". Releasing the claim and
// re-running then repairs it, so the survival is exclusion and not a broken
// sweep.
func TestReconcileSweepClaim(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		cancel()
		t.Fatalf("connect: %v", err)
	}
	defer func() { cancel(); pool.Close() }()
	resetAndMigrate(t, ctx, pool)
	f := newSweepFixture(t, ctx, pool, "swc")

	const bogus = 4242
	f.seedDrift(t, ctx, bogus)

	// A second node takes both claims, through the production API.
	unreadClaim := eventlog.NewSweeper(pool, messaging.UnreadCounterSweep)
	delivClaim := eventlog.NewSweeper(pool, notification.DeliverabilitySweep)
	rawUnread, ok, err := unreadClaim.Claim(ctx)
	if err != nil || !ok {
		t.Fatalf("second node could not take the unread claim: ok=%v err=%v", ok, err)
	}
	// Release exactly once, and ALWAYS: a claim holds a pooled connection, so
	// a Fatalf that skipped the release would wedge pool.Close() instead of
	// reporting the assertion.
	releaseUnread := sync.OnceFunc(rawUnread)
	defer releaseUnread()
	rawDeliv, ok, err := delivClaim.Claim(ctx)
	if err != nil || !ok {
		t.Fatalf("second node could not take the deliverability claim: ok=%v err=%v", ok, err)
	}
	releaseDeliv := sync.OnceFunc(rawDeliv)
	defer releaseDeliv()

	// The claim is exclusive: a third attempt gets nothing, cleanly.
	if _, ok, err := unreadClaim.Claim(ctx); ok || err != nil {
		t.Fatalf("unread claim granted twice: ok=%v err=%v, want ok=false err=nil", ok, err)
	}
	if _, ok, err := delivClaim.Claim(ctx); ok || err != nil {
		t.Fatalf("deliverability claim granted twice: ok=%v err=%v, want ok=false err=nil", ok, err)
	}

	// The contending sweeps return cleanly and touch NOTHING.
	if err := f.msg.ReconcileUnreadOnce(ctx); err != nil {
		t.Fatalf("contended unread sweep returned %v, want nil (a lost claim is "+
			"the at-most-once contract, not a failure)", err)
	}
	if err := f.deliv.ReconcileOnce(ctx); err != nil {
		t.Fatalf("contended deliverability sweep returned %v, want nil", err)
	}
	if u, _ := counterRow(t, ctx, pool, f.bobID, f.channelID); u != bogus {
		t.Fatalf("counter = %d while another node held the claim, want the seeded "+
			"%d untouched (this node swept anyway — no exclusion)", u, bogus)
	}
	if n := f.setRows(t, ctx); n != 0 {
		t.Fatalf("deliverability rows = %d while another node held the claim, want "+
			"the seeded 0 (this node swept anyway — no exclusion)", n)
	}

	// Release, sweep again: both caches converge on truth, so the survival
	// above was exclusion and not an inert sweep.
	releaseUnread()
	releaseDeliv()
	if err := f.msg.ReconcileUnreadOnce(ctx); err != nil {
		t.Fatalf("uncontended unread sweep: %v", err)
	}
	if err := f.deliv.ReconcileOnce(ctx); err != nil {
		t.Fatalf("uncontended deliverability sweep: %v", err)
	}
	if u, _ := counterRow(t, ctx, pool, f.bobID, f.channelID); u != f.sent {
		t.Fatalf("after the claim was released, counter = %d, want %d", u, f.sent)
	}
	assertLiveEquals(t, ctx, pool, f.bobID, f.channelID, f.sent)
	if n := f.setRows(t, ctx); n != 1 {
		t.Fatalf("after the claim was released, bob holds %d set rows, want 1", n)
	}
}
