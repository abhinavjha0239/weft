package rest

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
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

// captureSender records instead of sending — the test double behind the
// mail seam.
type captureSender struct {
	mu   sync.Mutex
	sent []capturedMail
}

type capturedMail struct{ to, subject, body string }

func (c *captureSender) Send(to, subject, body string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sent = append(c.sent, capturedMail{to, subject, body})
	return nil
}

func (c *captureSender) take() []capturedMail {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := c.sent
	c.sent = nil
	return out
}

// TestNotificationEmails: N-1 step 4 — the offline email lane. Unseen
// notifications older than the delay become ONE digest per user, at most
// once each; seeing the notification first cancels the email; the
// per-kind prefs (defaults: dm+mention on) gate it; and the digest names
// who/where without message content.
func TestNotificationEmails(t *testing.T) {
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

	hub := gateway.NewHub(pool, slog.Default())
	go hub.Run(ctx)
	permsSvc := perms.New(pool)
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity:      identity.New(pool, permsSvc),
		Messaging:     messaging.New(pool, permsSvc),
		DM:            dm.New(pool),
		Notifications: notification.New(pool),
	}))
	defer ts.Close()
	runner := notification.NewRunner(pool, hub, slog.Default())
	capture := &captureSender{}
	worker := notification.NewEmailWorker(pool, capture, slog.Default())

	var boot struct {
		OrgID     int64  `json:"org_id"`
		UserID    int64  `json:"user_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "eml", "email": "a@eml.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	bobTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"bob@eml.test", "Bob Ray", "bobemltok")
	var bobID int64
	if err := pool.QueryRow(ctx, `SELECT id FROM user_account WHERE email = 'bob@eml.test'`).Scan(&bobID); err != nil {
		t.Fatalf("bob id: %v", err)
	}
	process := func() {
		t.Helper()
		if err := runner.ProcessOrg(ctx, boot.OrgID); err != nil {
			t.Fatalf("materialize: %v", err)
		}
	}
	// The worker claims rows created BEFORE the cutoff: future = everything
	// due, past = nothing due yet.
	due := func() time.Time { return time.Now().Add(time.Minute) }

	// Default matrix: dm+mention on, followed+activity off.
	var prefs struct {
		Prefs []notification.MediumPref `json:"prefs"`
	}
	if code := getJSON(t, ts.URL+"/api/v1/notification-prefs", bobTok, &prefs); code != 200 || len(prefs.Prefs) != 4 {
		t.Fatalf("prefs = %d %+v", code, prefs.Prefs)
	}
	for _, p := range prefs.Prefs {
		want := p.Kind == 1 || p.Kind == 2
		if p.Enabled != want {
			t.Fatalf("default pref kind %d = %v, want %v", p.Kind, p.Enabled, want)
		}
	}

	// A DM to bob: not due yet → no email; due → exactly one, with the
	// who-line and no message content.
	var conv struct {
		RootThreadID int64 `json:"root_thread_id"`
	}
	postJSON(t, ts.URL+"/api/v1/dms", boot.Token, map[string]any{"user_ids": []int64{bobID}}, &conv)
	postJSON(t, fmt.Sprintf("%s/api/v1/threads/%d/messages", ts.URL, conv.RootThreadID),
		boot.Token, map[string]any{"content": "the secret plan"}, nil)
	process()
	if n, err := worker.RunOnce(ctx, time.Now().Add(-time.Hour)); err != nil || n != 0 {
		t.Fatalf("early sweep = %d (%v), want nothing due", n, err)
	}
	if n, err := worker.RunOnce(ctx, due()); err != nil || n != 1 {
		t.Fatalf("due sweep = %d (%v), want 1 email", n, err)
	}
	mails := capture.take()
	if len(mails) != 1 || mails[0].to != "bob@eml.test" ||
		!strings.Contains(mails[0].subject, "1 unread") ||
		!strings.Contains(mails[0].body, "Alice Chen sent you a direct message") {
		t.Fatalf("dm email = %+v", mails)
	}
	if strings.Contains(mails[0].body, "the secret plan") {
		t.Fatal("email must not carry message content")
	}
	// At most once: the watermark blocks a re-send.
	if n, err := worker.RunOnce(ctx, due()); err != nil || n != 0 {
		t.Fatalf("re-sweep = %d (%v), want watermarked silence", n, err)
	}

	// Seen cancels: a second DM, seen before the sweep → no email.
	postJSON(t, fmt.Sprintf("%s/api/v1/threads/%d/messages", ts.URL, conv.RootThreadID),
		boot.Token, map[string]any{"content": "follow-up"}, nil)
	process()
	if code := postJSONStatus(t, ts.URL+"/api/v1/notifications/seen", bobTok,
		map[string]any{"up_to": int64(1) << 62}); code != http.StatusOK {
		t.Fatalf("mark seen = %d", code)
	}
	if n, err := worker.RunOnce(ctx, due()); err != nil || n != 0 {
		t.Fatalf("seen sweep = %d (%v), want nothing", n, err)
	}

	// Pref off: bob silences dm emails; a third DM earns no email even
	// when due — the in-app row still lands (the badge is structural).
	if code := putJSON(t, ts.URL+"/api/v1/notification-prefs", bobTok,
		map[string]any{"kind": 1, "medium": 2, "enabled": false}); code != http.StatusOK {
		t.Fatalf("pref off = %d", code)
	}
	postJSON(t, fmt.Sprintf("%s/api/v1/threads/%d/messages", ts.URL, conv.RootThreadID),
		boot.Token, map[string]any{"content": "third"}, nil)
	process()
	if n, err := worker.RunOnce(ctx, due()); err != nil || n != 0 {
		t.Fatalf("pref-off sweep = %d (%v), want none", n, err)
	}
	var inApp int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM notification WHERE user_id = $1`, bobID).Scan(&inApp); err != nil || inApp != 3 {
		t.Fatalf("in-app rows = %d (%v), want all 3 regardless of email prefs", inApp, err)
	}

	// Batching: pref back on; a DM and a mention pending together → ONE
	// digest with both lines and the channel name.
	if code := putJSON(t, ts.URL+"/api/v1/notification-prefs", bobTok,
		map[string]any{"kind": 1, "medium": 2, "enabled": true}); code != http.StatusOK {
		t.Fatalf("pref on = %d", code)
	}
	postJSON(t, fmt.Sprintf("%s/api/v1/threads/%d/messages", ts.URL, conv.RootThreadID),
		boot.Token, map[string]any{"content": "fourth"}, nil)
	postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, boot.ChannelID),
		boot.Token, map[string]any{"content": "ping @**Bob Ray**"}, nil)
	process()
	if n, err := worker.RunOnce(ctx, due()); err != nil || n != 1 {
		t.Fatalf("batch sweep = %d (%v), want one digest", n, err)
	}
	// Three lines, one email: the fourth DM, the mention — and the third
	// DM from the pref-off window, still unseen and unemailed, correctly
	// picked up now that the pref is back on (prefs read at sweep time).
	mails = capture.take()
	if len(mails) != 1 || !strings.Contains(mails[0].subject, "3 unread") ||
		!strings.Contains(mails[0].body, "sent you a direct message") ||
		!strings.Contains(mails[0].body, "mentioned you in #general") {
		t.Fatalf("digest = %+v", mails)
	}

	// Bad pref inputs: unknown kind, unsettable medium.
	if code := putJSON(t, ts.URL+"/api/v1/notification-prefs", bobTok,
		map[string]any{"kind": 4, "medium": 2, "enabled": true}); code != http.StatusBadRequest {
		t.Fatalf("reserved kind = %d, want 400", code)
	}
	if code := putJSON(t, ts.URL+"/api/v1/notification-prefs", bobTok,
		map[string]any{"kind": 1, "medium": 1, "enabled": false}); code != http.StatusBadRequest {
		t.Fatalf("in-app medium = %d, want 400", code)
	}
}
