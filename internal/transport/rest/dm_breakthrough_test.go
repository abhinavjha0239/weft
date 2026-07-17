package rest

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/domain/dm"
	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/notification"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/gateway"
)

// TestDMBreakthrough: ADR-011 N-2 — a DM sender's one "notify anyway" per
// recipient per UTC day. A snoozed recipient's suppressed DM ping is re-fanned
// over the live seam (bypassing the DND gate) exactly once per day; the daily
// use is burned ONLY when there is genuinely something to break through, and
// only for 1:1 conversations. Driven through the same capturing-fanout seam as
// the DND test, with the materializer run synchronously via ProcessOrg.
func TestDMBreakthrough(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		cancel()
		t.Fatalf("connect: %v", err)
	}
	defer func() { cancel(); pool.Close() }()
	resetAndMigrate(t, ctx, pool)

	// The capture stands in for the gateway on BOTH the materializer (to prove
	// the ping was suppressed) and the notification service (to prove the
	// breakthrough re-fans it). No background runner: ProcessOrg is synchronous.
	capture := &captureFan{}
	hub := gateway.NewHub(pool, slog.Default())
	permsSvc := perms.New(pool)
	notifSvc := notification.New(pool)
	notifSvc.SetFanout(capture)
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity:      identity.New(pool, permsSvc),
		Messaging:     messaging.New(pool, permsSvc),
		DM:            dm.New(pool),
		Notifications: notifSvc,
	}))
	defer ts.Close()
	runner := notification.NewRunner(pool, capture, slog.Default())

	var boot struct {
		OrgID     int64  `json:"org_id"`
		UserID    int64  `json:"user_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "brk", "email": "alice@brk.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	bobTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"bob@brk.test", "Bob Ray", "bobbrktok")
	charlieTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"charlie@brk.test", "Charlie Kim", "charliebrktok")
	daveTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"dave@brk.test", "Dave Lin", "davebrktok")
	uid := func(email string) int64 {
		t.Helper()
		var id int64
		if err := pool.QueryRow(ctx, `SELECT id FROM user_account WHERE email = $1`, email).Scan(&id); err != nil {
			t.Fatalf("uid %s: %v", email, err)
		}
		return id
	}
	bobID, charlieID, daveID := uid("bob@brk.test"), uid("charlie@brk.test"), uid("dave@brk.test")

	process := func() {
		t.Helper()
		drainConsumer(t, ctx, pool, "notifications", boot.OrgID, runner.ProcessOrg)
	}
	// openDM returns (dm_space id, root thread id) for the given target set.
	openDM := func(fromTok string, toIDs ...int64) (int64, int64) {
		t.Helper()
		var conv struct {
			ID           int64 `json:"id"`
			RootThreadID int64 `json:"root_thread_id"`
		}
		postJSON(t, ts.URL+"/api/v1/dms", fromTok,
			map[string]any{"user_ids": toIDs}, &conv)
		return conv.ID, conv.RootThreadID
	}
	sendDM := func(fromTok string, rootThreadID int64, content string) {
		t.Helper()
		postJSON(t, fmt.Sprintf("%s/api/v1/threads/%d/messages", ts.URL, rootThreadID),
			fromTok, map[string]any{"content": content}, nil)
	}
	breakthrough := func(tok string, dmSpaceID int64) int {
		t.Helper()
		return postJSONStatus(t, fmt.Sprintf("%s/api/v1/dms/%d/breakthrough", ts.URL, dmSpaceID), tok, nil)
	}
	snooze := func(tok string) {
		t.Helper()
		if code := putJSON(t, ts.URL+"/api/v1/dnd", tok,
			map[string]any{"snoozed_until": time.Now().Add(time.Hour).Format(time.RFC3339)}); code != http.StatusOK {
			t.Fatalf("snooze = %d", code)
		}
	}
	usesRow := func(senderID, recipientID int64) int {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM dm_breakthrough WHERE sender_id = $1 AND recipient_id = $2`,
			senderID, recipientID).Scan(&n); err != nil {
			t.Fatalf("count breakthrough rows: %v", err)
		}
		return n
	}

	// 1. Happy path: bob snoozes, alice DMs bob → the row lands but the live
	//    ping is suppressed; breakthrough re-fans it to bob and burns one use.
	snooze(bobTok)
	aliceBob, aliceBobThread := openDM(boot.Token, bobID)
	sendDM(boot.Token, aliceBobThread, "hi bob")
	process()
	if calls := capture.take(); len(calls) != 0 {
		t.Fatalf("snoozed DM fan = %+v, want suppressed", calls)
	}
	if code := breakthrough(boot.Token, aliceBob); code != http.StatusOK {
		t.Fatalf("breakthrough = %d, want 200", code)
	}
	if calls := capture.take(); len(calls) != 1 || calls[0].userID != bobID {
		t.Fatalf("breakthrough fan = %+v, want one call to bob", calls)
	}
	if usesRow(boot.UserID, bobID) != 1 {
		t.Fatalf("breakthrough uses = %d, want 1", usesRow(boot.UserID, bobID))
	}

	// 2. Second call the same day → 409, no extra fan (the allowance is spent).
	if code := breakthrough(boot.Token, aliceBob); code != http.StatusConflict {
		t.Fatalf("second breakthrough = %d, want 409", code)
	}
	if calls := capture.take(); len(calls) != 0 {
		t.Fatalf("second breakthrough fan = %+v, want none", calls)
	}

	// 3. Next day works: backdate the use (never sleep) → a fresh row + re-fan.
	if _, err := pool.Exec(ctx,
		`UPDATE dm_breakthrough SET used_on = used_on - 1 WHERE sender_id = $1 AND recipient_id = $2`,
		boot.UserID, bobID); err != nil {
		t.Fatalf("backdate use: %v", err)
	}
	if code := breakthrough(boot.Token, aliceBob); code != http.StatusOK {
		t.Fatalf("next-day breakthrough = %d, want 200", code)
	}
	if calls := capture.take(); len(calls) != 1 || calls[0].userID != bobID {
		t.Fatalf("next-day fan = %+v, want one call to bob", calls)
	}
	if usesRow(boot.UserID, bobID) != 2 {
		t.Fatalf("uses after next day = %d, want 2 (yesterday + today)", usesRow(boot.UserID, bobID))
	}

	// 4. Recipient not snoozed → 409, use NOT burned. charlie never snoozes, so
	//    alice's DM pinged live; breakthrough has nothing to pierce.
	aliceCharlie, aliceCharlieThread := openDM(boot.Token, charlieID)
	sendDM(boot.Token, aliceCharlieThread, "hi charlie")
	process()
	capture.take() // charlie's live ping (not snoozed) — clear it
	if code := breakthrough(boot.Token, aliceCharlie); code != http.StatusConflict {
		t.Fatalf("not-snoozed breakthrough = %d, want 409", code)
	}
	if calls := capture.take(); len(calls) != 0 {
		t.Fatalf("not-snoozed fan = %+v, want none", calls)
	}
	if usesRow(boot.UserID, charlieID) != 0 {
		t.Fatalf("not-snoozed burned a use = %d, want 0", usesRow(boot.UserID, charlieID))
	}

	// 5. Nothing pending → 409, use NOT burned. dave is snoozed but alice never
	//    sent, so there is no unseen DM notification to re-deliver.
	snooze(daveTok)
	aliceDave, _ := openDM(boot.Token, daveID)
	process()
	if code := breakthrough(boot.Token, aliceDave); code != http.StatusConflict {
		t.Fatalf("no-pending breakthrough = %d, want 409", code)
	}
	if usesRow(boot.UserID, daveID) != 0 {
		t.Fatalf("no-pending burned a use = %d, want 0", usesRow(boot.UserID, daveID))
	}

	// 6. Group and self conversations are not 1:1 → 400 (no use burned).
	group, _ := openDM(boot.Token, bobID, charlieID)
	if code := breakthrough(boot.Token, group); code != http.StatusBadRequest {
		t.Fatalf("group breakthrough = %d, want 400", code)
	}
	self, _ := openDM(boot.Token) // no targets → self conversation
	if code := breakthrough(boot.Token, self); code != http.StatusBadRequest {
		t.Fatalf("self breakthrough = %d, want 400", code)
	}

	// 7. A non-participant gets an oracle-free 404 on someone else's DM.
	if code := breakthrough(charlieTok, aliceBob); code != http.StatusNotFound {
		t.Fatalf("outsider breakthrough = %d, want 404", code)
	}
}
