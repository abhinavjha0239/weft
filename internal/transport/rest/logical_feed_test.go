package rest

import (
	"context"
	"fmt"
	"log/slog"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/notification"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/eventlog"
	"github.com/abhinavjha0239/weft/internal/gateway"
	"github.com/abhinavjha0239/weft/internal/platform/metrics"
)

// TestNotificationRunnerOnLogicalFeed is the interface-preservation proof for
// S4: a REAL consumer — the notification materialiser, unmodified below its
// SetSource line — runs end to end on the logical-decoding feed and produces
// the same state it produces on the xmin poller.
//
// The load-bearing assert is not "a notification appeared" (that would pass on
// either driver and prove nothing about the swap): it is that the
// notifications cursor carries an LSN, which ONLY the logical driver writes.
//
// RED (observed): delete the runner.SetSource(src) line — the runner falls
// back to the xmin poller, the notification still appears, and the cursor's
// lsn stays NULL, so the "delivered through the logical feed" assert fails.
func TestNotificationRunnerOnLogicalFeed(t *testing.T) {
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
	requireLogicalWAL(t, ctx, pool)
	resetAndMigrate(t, ctx, pool)

	opts := eventlog.LogicalOptions{Slot: "eventlog_feed_rest", Publication: "eventlog_pub_rest"}
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
	go hub.Run(ctx)
	runner := notification.NewRunner(pool, hub, slog.Default())
	runner.SetMetrics(metrics.NewExpvar())
	runner.SetSource(src) // the ONE line that swaps the delivery mechanism
	permsSvc := perms.New(pool)
	msgSvc := messaging.New(pool, permsSvc)
	msgSvc.SetDeliverability(notification.NewDeliverability(pool, slog.Default()))
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity:      identity.New(pool, permsSvc),
		Messaging:     msgSvc,
		Notifications: notification.New(pool),
	}))
	defer ts.Close()

	var boot struct {
		OrgID     int64  `json:"org_id"`
		UserID    int64  `json:"user_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "lfd", "email": "a@lfd.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"bob@lfd.test", "Bob Ray", "boblfdtok")
	var bobID int64
	if err := pool.QueryRow(ctx,
		`SELECT id FROM user_account WHERE org_id=$1 AND email='bob@lfd.test'`,
		boot.OrgID).Scan(&bobID); err != nil {
		t.Fatalf("bob id: %v", err)
	}

	var sent struct {
		MessageID int64 `json:"message_id"`
	}
	postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, boot.ChannelID),
		boot.Token, map[string]any{"content": "@**Bob Ray** ship it"}, &sent)
	drainConsumer(t, ctx, pool, "notifications", boot.OrgID, runner.ProcessOrg)

	// STATE: the materialiser produced exactly the mention row it produces on
	// the xmin poller — same kind, same recipient, anchored on this message.
	var kind int16
	var entityID int64
	if err := pool.QueryRow(ctx, `
		SELECT kind, entity_id FROM notification
		WHERE org_id = $1 AND user_id = $2`, boot.OrgID, bobID).Scan(&kind, &entityID); err != nil {
		t.Fatalf("no notification materialised on the logical feed: %v", err)
	}
	if kind != notification.KindMention {
		t.Fatalf("notification kind = %d, want KindMention (%d)", kind, notification.KindMention)
	}
	if entityID != sent.MessageID {
		t.Fatalf("notification anchored on entity %d, want message %d", entityID, sent.MessageID)
	}

	// PROOF OF SWAP: only the logical driver writes an LSN cursor. A NULL here
	// means the runner quietly kept polling with the xmin gate and the state
	// above proved nothing about the feed.
	var lsn *string
	if err := pool.QueryRow(ctx,
		`SELECT lsn::text FROM event_consumer_cursor WHERE consumer='notifications' AND org_id=$1`,
		boot.OrgID).Scan(&lsn); err != nil {
		t.Fatalf("notifications cursor: %v", err)
	}
	if lsn == nil {
		t.Fatal("the notifications cursor has no LSN: the runner did NOT consume " +
			"through the logical feed (SetSource had no effect)")
	}

	// A second message rides the same swapped feed and the cursor advances —
	// the swap is not a one-shot that silently wedges after the first batch.
	postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, boot.ChannelID),
		boot.Token, map[string]any{"content": "@**Bob Ray** and again"}, &sent)
	drainConsumer(t, ctx, pool, "notifications", boot.OrgID, runner.ProcessOrg)
	var count int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM notification WHERE org_id=$1 AND user_id=$2 AND kind=$3`,
		boot.OrgID, bobID, notification.KindMention).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 2 {
		t.Fatalf("mention notifications = %d, want 2", count)
	}
	var lsn2 *string
	if err := pool.QueryRow(ctx,
		`SELECT lsn::text FROM event_consumer_cursor WHERE consumer='notifications' AND org_id=$1`,
		boot.OrgID).Scan(&lsn2); err != nil {
		t.Fatalf("notifications cursor 2: %v", err)
	}
	if lsn2 == nil || *lsn2 == *lsn {
		t.Fatalf("cursor LSN did not advance past %v (got %v)", *lsn, lsn2)
	}
}

// requireLogicalWAL skips unless the server can do logical decoding, and FAILS
// instead of skipping when TEST_LOGICAL_DECODING demands the pin actually run.
func requireLogicalWAL(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	var level string
	if err := pool.QueryRow(ctx, `SHOW wal_level`).Scan(&level); err != nil {
		t.Fatalf("read wal_level: %v", err)
	}
	if level == "logical" {
		return
	}
	msg := "server runs wal_level=" + level + "; the logical feed needs wal_level=logical"
	if os.Getenv("TEST_LOGICAL_DECODING") != "" {
		t.Fatal("TEST_LOGICAL_DECODING is set but the " + msg)
	}
	t.Skip(msg)
}
