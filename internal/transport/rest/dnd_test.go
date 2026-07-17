package rest

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
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

// captureFan records NotifyUser calls instead of delivering them — the test
// double behind the notification Fanout seam, so live-ping suppression is
// asserted directly (mutex-guarded: the interface may be called from any
// goroutine, though this test drives ProcessOrg synchronously).
type captureFan struct {
	mu    sync.Mutex
	calls []fanCall
}

type fanCall struct {
	orgID, userID int64
}

func (c *captureFan) NotifyUser(_ context.Context, orgID, userID int64, _ json.RawMessage) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, fanCall{orgID, userID})
}

func (c *captureFan) take() []fanCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := c.calls
	c.calls = nil
	return out
}

// TestDNDSuppression: ADR-011 N-2 — a snooze suppresses live pings and offline
// emails while the in-app row still lands (N-4: the badge is structural); a
// VIP (priority_contact) pierces the snooze; suppression is a delay, not a
// drop (the email rides the next sweep once the snooze lapses). Validation of
// both settings surfaces is covered too.
func TestDNDSuppression(t *testing.T) {
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

	// The hub is only the REST server's dependency here; live pings are
	// captured by the fake fanout wired into the runner, and the materializer
	// is driven synchronously via ProcessOrg.
	hub := gateway.NewHub(pool, slog.Default())
	permsSvc := perms.New(pool)
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity:      identity.New(pool, permsSvc),
		Messaging:     messaging.New(pool, permsSvc),
		DM:            dm.New(pool),
		Notifications: notification.New(pool),
	}))
	defer ts.Close()
	capture := &captureFan{}
	runner := notification.NewRunner(pool, capture, slog.Default())
	capSender := &captureSender{}
	worker := notification.NewEmailWorker(pool, capSender, slog.Default())

	var boot struct {
		OrgID     int64  `json:"org_id"`
		UserID    int64  `json:"user_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "dnd", "email": "alice@dnd.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	aliceID := boot.UserID
	bobTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"bob@dnd.test", "Bob Ray", "bobdndtok")
	charlieTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"charlie@dnd.test", "Charlie Kim", "charliedndtok")
	var bobID, charlieID int64
	if err := pool.QueryRow(ctx, `SELECT id FROM user_account WHERE email = 'bob@dnd.test'`).Scan(&bobID); err != nil {
		t.Fatalf("bob id: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT id FROM user_account WHERE email = 'charlie@dnd.test'`).Scan(&charlieID); err != nil {
		t.Fatalf("charlie id: %v", err)
	}

	dndURL := ts.URL + "/api/v1/dnd"
	vipsURL := ts.URL + "/api/v1/vips"

	process := func() {
		t.Helper()
		drainConsumer(t, ctx, pool, "notifications", boot.OrgID, runner.ProcessOrg)
	}
	due := func() time.Time { return time.Now().Add(time.Minute) }
	openDM := func(fromTok string, toID int64) int64 {
		t.Helper()
		var conv struct {
			RootThreadID int64 `json:"root_thread_id"`
		}
		postJSON(t, ts.URL+"/api/v1/dms", fromTok,
			map[string]any{"user_ids": []int64{toID}}, &conv)
		return conv.RootThreadID
	}
	sendDM := func(fromTok string, rootThreadID int64, content string) {
		t.Helper()
		postJSON(t, fmt.Sprintf("%s/api/v1/threads/%d/messages", ts.URL, rootThreadID),
			fromTok, map[string]any{"content": content}, nil)
	}
	bobRows := func() int {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM notification WHERE user_id = $1`, bobID).Scan(&n); err != nil {
			t.Fatalf("count bob rows: %v", err)
		}
		return n
	}
	setSnooze := func(when time.Time) int {
		t.Helper()
		return putJSON(t, dndURL, bobTok,
			map[string]any{"snoozed_until": when.Format(time.RFC3339)})
	}

	// GetDND with no row yet → null (the ErrNoRows path).
	var dndState struct {
		SnoozedUntil *time.Time `json:"snoozed_until"`
	}
	if code := getJSON(t, dndURL, bobTok, &dndState); code != http.StatusOK || dndState.SnoozedUntil != nil {
		t.Fatalf("unset dnd = %d %+v, want null", code, dndState.SnoozedUntil)
	}

	// 1. Baseline: alice DMs bob → in-app row + ONE fan call + one due email.
	aliceBob := openDM(boot.Token, bobID)
	sendDM(boot.Token, aliceBob, "hi bob")
	process()
	if bobRows() != 1 {
		t.Fatalf("baseline in-app rows = %d, want 1", bobRows())
	}
	if calls := capture.take(); len(calls) != 1 || calls[0].userID != bobID {
		t.Fatalf("baseline fan = %+v, want one call for bob", calls)
	}
	if n, err := worker.RunOnce(ctx, due()); err != nil || n != 1 {
		t.Fatalf("baseline email = %d (%v), want 1", n, err)
	}
	if mails := capSender.take(); len(mails) != 1 || mails[0].to != "bob@dnd.test" {
		t.Fatalf("baseline mail = %+v", mails)
	}

	// 2. Bob snoozes → the row still LANDS (badge), but no live ping and no
	//    email; the pending row keeps emailed_at NULL for a later sweep.
	if code := setSnooze(time.Now().Add(time.Hour)); code != http.StatusOK {
		t.Fatalf("snooze = %d", code)
	}
	dndState.SnoozedUntil = nil
	if code := getJSON(t, dndURL, bobTok, &dndState); code != http.StatusOK || dndState.SnoozedUntil == nil {
		t.Fatalf("get dnd after snooze = %d %+v, want a time", code, dndState.SnoozedUntil)
	}
	sendDM(boot.Token, aliceBob, "while snoozed")
	process()
	if bobRows() != 2 {
		t.Fatalf("snoozed in-app rows = %d, want 2 (badge still accrues)", bobRows())
	}
	if calls := capture.take(); len(calls) != 0 {
		t.Fatalf("snoozed fan = %+v, want none", calls)
	}
	if n, err := worker.RunOnce(ctx, due()); err != nil || n != 0 {
		t.Fatalf("snoozed email = %d (%v), want 0", n, err)
	}
	var emailedNull bool
	if err := pool.QueryRow(ctx, `
		SELECT emailed_at IS NULL FROM notification
		WHERE user_id = $1 ORDER BY id DESC LIMIT 1`, bobID).Scan(&emailedNull); err != nil || !emailedNull {
		t.Fatalf("suppressed row emailed_at null = %v (%v), want unmarked", emailedNull, err)
	}

	// 3. Snooze lapses (backdated via SQL, never sleep) → the delayed email
	//    rides the next sweep, exactly once.
	if _, err := pool.Exec(ctx,
		`UPDATE dnd_setting SET snoozed_until = now() - interval '1 minute' WHERE user_id = $1`,
		bobID); err != nil {
		t.Fatalf("lapse snooze: %v", err)
	}
	if n, err := worker.RunOnce(ctx, due()); err != nil || n != 1 {
		t.Fatalf("post-lapse email = %d (%v), want the delayed 1", n, err)
	}
	capSender.take()
	if n, err := worker.RunOnce(ctx, due()); err != nil || n != 0 {
		t.Fatalf("post-lapse re-sweep = %d (%v), want exactly-once silence", n, err)
	}

	// 4. VIP pierce: bob re-snoozes and lists alice as a VIP → alice's DM
	//    pings AND emails despite the snooze; a non-VIP (charlie) is suppressed.
	if code := setSnooze(time.Now().Add(time.Hour)); code != http.StatusOK {
		t.Fatalf("re-snooze = %d", code)
	}
	if code := putJSON(t, vipsURL, bobTok, map[string]any{"user_ids": []int64{aliceID}}); code != http.StatusOK {
		t.Fatalf("set vip = %d", code)
	}
	sendDM(boot.Token, aliceBob, "vip ping")
	process()
	if calls := capture.take(); len(calls) != 1 || calls[0].userID != bobID {
		t.Fatalf("vip fan = %+v, want alice's ping to pierce", calls)
	}
	if n, err := worker.RunOnce(ctx, due()); err != nil || n != 1 {
		t.Fatalf("vip email = %d (%v), want 1 (pierced)", n, err)
	}
	capSender.take()

	charlieBob := openDM(charlieTok, bobID)
	sendDM(charlieTok, charlieBob, "charlie ping")
	process()
	if calls := capture.take(); len(calls) != 0 {
		t.Fatalf("non-vip fan = %+v, want suppressed", calls)
	}
	if n, err := worker.RunOnce(ctx, due()); err != nil || n != 0 {
		t.Fatalf("non-vip email = %d (%v), want suppressed", n, err)
	}

	// 5. Validation: past and >30d snoozes 400, a foreign VIP id 404, and the
	//    VIP set is replace-semantics (self silently dropped, second PUT wins).
	if code := setSnooze(time.Now().Add(-time.Hour)); code != http.StatusBadRequest {
		t.Fatalf("past snooze = %d, want 400", code)
	}
	if code := setSnooze(time.Now().Add(31 * 24 * time.Hour)); code != http.StatusBadRequest {
		t.Fatalf("far snooze = %d, want 400", code)
	}
	// Clearing: a null snooze wipes the setting (GET reads null again).
	if code := putJSON(t, dndURL, bobTok, map[string]any{"snoozed_until": nil}); code != http.StatusOK {
		t.Fatalf("clear snooze = %d", code)
	}
	dndState.SnoozedUntil = nil
	if code := getJSON(t, dndURL, bobTok, &dndState); code != http.StatusOK || dndState.SnoozedUntil != nil {
		t.Fatalf("cleared dnd = %d %+v, want null", code, dndState.SnoozedUntil)
	}
	if code := putJSON(t, vipsURL, bobTok, map[string]any{"user_ids": []int64{999999}}); code != http.StatusNotFound {
		t.Fatalf("foreign vip = %d, want 404", code)
	}
	// Replace-set: self is dropped, then a second PUT replaces the whole list.
	if code := putJSON(t, vipsURL, bobTok, map[string]any{"user_ids": []int64{bobID, aliceID}}); code != http.StatusOK {
		t.Fatalf("vip self-drop = %d", code)
	}
	var vipList struct {
		UserIDs []int64 `json:"user_ids"`
	}
	getJSON(t, vipsURL, bobTok, &vipList)
	if len(vipList.UserIDs) != 1 || vipList.UserIDs[0] != aliceID {
		t.Fatalf("vip list after self-drop = %+v, want [alice]", vipList.UserIDs)
	}
	if code := putJSON(t, vipsURL, bobTok, map[string]any{"user_ids": []int64{charlieID}}); code != http.StatusOK {
		t.Fatalf("vip replace = %d", code)
	}
	vipList.UserIDs = nil
	getJSON(t, vipsURL, bobTok, &vipList)
	if len(vipList.UserIDs) != 1 || vipList.UserIDs[0] != charlieID {
		t.Fatalf("vip list after replace = %+v, want [charlie]", vipList.UserIDs)
	}
}
