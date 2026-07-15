package rest

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
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

const testUnsubSecret = "unsub-test-secret-key"

// unsubSig mirrors the server's MAC ("unsub|<org>|<user>") so the test can
// forge variants with a known (or a wrong) secret.
func unsubSig(secret string, orgID, userID int64) string {
	h := hmac.New(sha256.New, []byte(secret))
	fmt.Fprintf(h, "unsub|%d|%d", orgID, userID)
	return hex.EncodeToString(h.Sum(nil))
}

func doNoAuth(t *testing.T, method, url string) (int, []byte) {
	t.Helper()
	req, _ := http.NewRequest(method, url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

// emailEnabled fetches bob's effective per-kind email matrix.
func emailEnabled(t *testing.T, base, token string) map[int16]bool {
	t.Helper()
	var prefs struct {
		Prefs []notification.MediumPref `json:"prefs"`
	}
	if code := getJSON(t, base+"/api/v1/notification-prefs", token, &prefs); code != 200 {
		t.Fatalf("get prefs = %d", code)
	}
	out := map[int16]bool{}
	for _, p := range prefs.Prefs {
		out[p.Kind] = p.Enabled
	}
	return out
}

// TestUnsubscribe: P-20 one-click unsubscribe. A digest carries a signed link;
// the GET renders a confirmation form WITHOUT mutating (mail clients prefetch
// it); the POST turns off every email kind for the user and is idempotent; a
// forged or tampered signature is a constant-time 401; and with no secret
// configured both verbs 404.
func TestUnsubscribe(t *testing.T) {
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
	notifSvc := notification.New(pool)
	notifSvc.SetUnsubscribe(testUnsubSecret)
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity:      identity.New(pool, permsSvc),
		Messaging:     messaging.New(pool, permsSvc),
		DM:            dm.New(pool),
		Notifications: notifSvc,
	}))
	defer ts.Close()
	runner := notification.NewRunner(pool, hub, slog.Default())
	capture := &captureSender{}
	worker := notification.NewEmailWorker(pool, capture, slog.Default())
	worker.SetUnsubscribe(ts.URL, testUnsubSecret)
	due := func() time.Time { return time.Now().Add(time.Minute) }

	var boot struct {
		OrgID     int64  `json:"org_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "uns", "email": "a@uns.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	bobTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"bob@uns.test", "Bob Ray", "bobunstok")
	var bobID int64
	if err := pool.QueryRow(ctx, `SELECT id FROM user_account WHERE email = 'bob@uns.test'`).Scan(&bobID); err != nil {
		t.Fatalf("bob id: %v", err)
	}

	// A DM to bob → a due digest whose link parses to the expected MAC.
	var conv struct {
		RootThreadID int64 `json:"root_thread_id"`
	}
	postJSON(t, ts.URL+"/api/v1/dms", boot.Token, map[string]any{"user_ids": []int64{bobID}}, &conv)
	postJSON(t, fmt.Sprintf("%s/api/v1/threads/%d/messages", ts.URL, conv.RootThreadID),
		boot.Token, map[string]any{"content": "the plan"}, nil)
	if err := runner.ProcessOrg(ctx, boot.OrgID); err != nil {
		t.Fatalf("materialize: %v", err)
	}
	if n, err := worker.RunOnce(ctx, due()); err != nil || n != 1 {
		t.Fatalf("digest sweep = %d (%v), want 1", n, err)
	}
	mails := capture.take()
	if len(mails) != 1 {
		t.Fatalf("mails = %d, want 1", len(mails))
	}
	wantSig := unsubSig(testUnsubSecret, boot.OrgID, bobID)
	wantLink := fmt.Sprintf("%s/api/v1/unsubscribe?o=%d&u=%d&sig=%s", ts.URL, boot.OrgID, bobID, wantSig)
	if mails[0].listUnsub != wantLink || !mails[0].listUnsubPost {
		t.Fatalf("List-Unsubscribe = %q post=%v, want %q post=true", mails[0].listUnsub, mails[0].listUnsubPost, wantLink)
	}
	if !strings.Contains(mails[0].text, "Unsubscribe: "+wantLink) {
		t.Fatalf("text footer missing link:\n%s", mails[0].text)
	}
	if !strings.Contains(mails[0].html, wantSig) || !strings.Contains(mails[0].html, "<li>") {
		t.Fatalf("html digest missing link or list:\n%s", mails[0].html)
	}
	path := strings.TrimPrefix(mails[0].listUnsub, ts.URL)

	// GET renders the form and must NOT change state.
	code, body := doNoAuth(t, "GET", ts.URL+path)
	if code != 200 || !strings.Contains(string(body), `method="post"`) || !strings.Contains(string(body), "Unsubscribe") {
		t.Fatalf("GET page = %d, body:\n%s", code, body)
	}
	before := emailEnabled(t, ts.URL, bobTok)
	if !before[1] || !before[2] || !before[4] {
		t.Fatalf("GET must not mutate prefs; got %+v", before)
	}

	// POST flips every kind off.
	code, _ = doNoAuth(t, "POST", ts.URL+path)
	if code != 200 {
		t.Fatalf("POST = %d, want 200", code)
	}
	after := emailEnabled(t, ts.URL, bobTok)
	if len(after) != 6 {
		t.Fatalf("expected all 6 kinds, got %+v", after)
	}
	for k, enabled := range after {
		if enabled {
			t.Fatalf("kind %d still enabled after unsubscribe: %+v", k, after)
		}
	}

	// A now-due DM is NOT emailed (kind 1 is off).
	postJSON(t, fmt.Sprintf("%s/api/v1/threads/%d/messages", ts.URL, conv.RootThreadID),
		boot.Token, map[string]any{"content": "after unsub"}, nil)
	if err := runner.ProcessOrg(ctx, boot.OrgID); err != nil {
		t.Fatalf("materialize: %v", err)
	}
	if n, err := worker.RunOnce(ctx, due()); err != nil || n != 0 {
		t.Fatalf("post-unsub sweep = %d (%v), want 0", n, err)
	}

	// Idempotent: a second POST stays 200.
	if code, _ := doNoAuth(t, "POST", ts.URL+path); code != 200 {
		t.Fatalf("second POST = %d, want 200 (idempotent)", code)
	}

	// Forged signature (correct-length hex over the right o/u minted with the
	// WRONG secret): every other guard passes, so ONLY the constant-time HMAC
	// compare can reject it — neuter that compare and this goes 200 (red).
	forged := fmt.Sprintf("%s/api/v1/unsubscribe?o=%d&u=%d&sig=%s", ts.URL, boot.OrgID, bobID,
		unsubSig("not-the-server-secret", boot.OrgID, bobID))
	if code, _ := doNoAuth(t, "GET", forged); code != http.StatusUnauthorized {
		t.Fatalf("forged GET = %d, want 401", code)
	}
	if code, _ := doNoAuth(t, "POST", forged); code != http.StatusUnauthorized {
		t.Fatalf("forged POST = %d, want 401", code)
	}

	// One-nibble flip inside the real sig is rejected too.
	flipped := []byte(wantSig)
	if flipped[0] == '0' {
		flipped[0] = '1'
	} else {
		flipped[0] = '0'
	}
	oneOff := fmt.Sprintf("%s/api/v1/unsubscribe?o=%d&u=%d&sig=%s", ts.URL, boot.OrgID, bobID, string(flipped))
	if code, _ := doNoAuth(t, "GET", oneOff); code != http.StatusUnauthorized {
		t.Fatalf("flipped GET = %d, want 401", code)
	}

	// Unknown user under a VALID (server-minted) signature → 404 on POST.
	ghost := fmt.Sprintf("%s/api/v1/unsubscribe?o=%d&u=%d&sig=%s", ts.URL, boot.OrgID, bobID+9999,
		unsubSig(testUnsubSecret, boot.OrgID, bobID+9999))
	if code, _ := doNoAuth(t, "POST", ghost); code != http.StatusNotFound {
		t.Fatalf("ghost-user POST = %d, want 404", code)
	}

	// Secret unset → both verbs 404 (a second server with no secret wired).
	ts2 := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Log: slog.Default(),
		Notifications: notification.New(pool),
	}))
	defer ts2.Close()
	for _, m := range []string{"GET", "POST"} {
		if code, _ := doNoAuth(t, m, ts2.URL+path); code != http.StatusNotFound {
			t.Fatalf("no-secret %s = %d, want 404", m, code)
		}
	}
}
