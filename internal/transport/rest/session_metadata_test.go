package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/gateway"
)

// postJSONUA is postJSON with a caller-chosen User-Agent — the sessions under
// test are identified by the UA they were minted with.
func postJSONUA(t *testing.T, url, ua string, body any, out any) int {
	t.Helper()
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", url, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", ua)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	if out != nil {
		_ = json.NewDecoder(resp.Body).Decode(out)
	}
	return resp.StatusCode
}

// TestSessionMetadata: P-29 commit 1 — every session-minting path (bootstrap,
// login, invite accept) records the client's ip and a single-line, capped
// user agent on the auth_session row. Metadata only: nothing reads it for
// authorization.
func TestSessionMetadata(t *testing.T) {
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
		Identity:  identity.New(pool, permsSvc),
		Messaging: messaging.New(pool, permsSvc),
	}))
	defer ts.Close()

	// metaOf loads the newest session row for a user.
	metaOf := func(userID int64) (ip, ua string) {
		t.Helper()
		if err := pool.QueryRow(ctx, `
			SELECT COALESCE(ip, ''), COALESCE(user_agent, '')
			FROM auth_session WHERE user_id = $1 ORDER BY id DESC LIMIT 1`,
			userID).Scan(&ip, &ua); err != nil {
			t.Fatalf("session meta: %v", err)
		}
		return ip, ua
	}

	// Bootstrap: the owner's first session carries the boot request's ip/ua.
	var boot struct {
		OrgID  int64  `json:"org_id"`
		UserID int64  `json:"user_id"`
		Token  string `json:"token"`
	}
	if code := postJSONUA(t, ts.URL+"/api/v1/orgs/bootstrap", "boot-agent/1.0", map[string]any{
		"org_slug": "met", "email": "a@met.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot); code != http.StatusCreated {
		t.Fatalf("bootstrap = %d", code)
	}
	if ip, ua := metaOf(boot.UserID); ip != "127.0.0.1" || ua != "boot-agent/1.0" {
		t.Fatalf("bootstrap session meta = %q/%q, want 127.0.0.1 + boot-agent/1.0", ip, ua)
	}

	// Login: a fresh session records ITS request's ua, capped at 256 bytes.
	// (Go's client refuses to transmit CRLF in a header, so the single-line
	// flatten is pinned by TestRequestUserAgent below, not over the wire.)
	long := "long-agent/3.0 " + strings.Repeat("x", 300)
	var login struct {
		Token string `json:"token"`
	}
	if code := postJSONUA(t, ts.URL+"/api/v1/auth/login", long, map[string]any{
		"org_slug": "met", "email": "a@met.test", "password": "password123",
	}, &login); code != http.StatusOK || login.Token == "" {
		t.Fatalf("login = %d %q", code, login.Token)
	}
	if _, ua := metaOf(boot.UserID); len(ua) != 256 || !strings.HasPrefix(ua, "long-agent/3.0 ") {
		t.Fatalf("login session ua = %q (len %d), want 256-byte cap of the long UA", ua, len(ua))
	}

	// Invite accept: the provisioned user's first session carries the
	// ACCEPTOR's request metadata.
	var inv identity.Invite
	postJSON(t, ts.URL+"/api/v1/invites", boot.Token, map[string]any{}, &inv)
	if inv.Token == "" {
		t.Fatalf("invite = %+v", inv)
	}
	var accepted identity.AcceptInviteResult
	if code := postJSONUA(t, ts.URL+"/api/v1/invites/accept", "acceptor-agent/2.0", map[string]any{
		"token": inv.Token, "email": "bob@met.test", "password": "password123",
		"full_name": "Bob Ray",
	}, &accepted); code != http.StatusCreated || accepted.UserID == 0 {
		t.Fatalf("accept = %d %+v", code, accepted)
	}
	if ip, ua := metaOf(accepted.UserID); ip != "127.0.0.1" || ua != "acceptor-agent/2.0" {
		t.Fatalf("accept session meta = %q/%q, want 127.0.0.1 + acceptor-agent/2.0", ip, ua)
	}
}

// TestRequestUserAgent pins the sanitizer directly (Go's own client refuses
// to transmit CRLF, so the flatten can't be exercised over the wire): CRLF
// becomes spaces, invalid UTF-8 is stripped (the column is TEXT), and the
// 256-byte cap never splits a multibyte rune.
func TestRequestUserAgent(t *testing.T) {
	mk := func(ua string) *http.Request {
		r, _ := http.NewRequest("GET", "http://x/", nil)
		r.Header.Set("User-Agent", ua)
		return r
	}
	if got := requestUserAgent(mk("evil\r\nX-Injected: 1")); got != "evil  X-Injected: 1" {
		t.Fatalf("flatten = %q, want CRLF collapsed to spaces", got)
	}
	if got := requestUserAgent(mk("bad\xff\xfebytes")); got != "badbytes" {
		t.Fatalf("invalid utf-8 = %q, want stripped", got)
	}
	// 255 ascii bytes then a 2-byte rune straddling the 256 boundary: the cap
	// must drop the whole rune, never emit half of it.
	straddle := strings.Repeat("a", 255) + "é" + strings.Repeat("b", 10)
	got := requestUserAgent(mk(straddle))
	if len(got) != 255 || !strings.HasSuffix(got, "a") {
		t.Fatalf("cap = %d bytes ending %q, want 255 (the straddling rune dropped whole)", len(got), got[len(got)-1:])
	}
}
