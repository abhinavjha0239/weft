package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/gateway"
	"github.com/abhinavjha0239/weft/migrations"
)

// End-to-end M0 test: bootstrap → login-token → WebSocket → REST send →
// live event → disconnect → send while away → RESUME replays the gap (F-2).
// Needs TEST_DATABASE_URL (see eventlog tests).
func TestMessageEndToEnd(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)

	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		cancel()
		t.Fatalf("connect: %v", err)
	}
	// Cancel BEFORE closing the pool: the hub's LISTEN connection is released
	// by cancellation, otherwise Close blocks until the context deadline.
	defer func() { cancel(); pool.Close() }()
	resetAndMigrate(t, ctx, pool)

	hub := gateway.NewHub(pool, slog.Default())
	go hub.Run(ctx)
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity:  identity.New(pool, perms.New(pool)),
		Messaging: messaging.New(pool, perms.New(pool)),
	}))
	defer ts.Close()

	// 1. Bootstrap org.
	var boot struct {
		OrgID     int64  `json:"org_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_name": "Acme", "org_slug": "acme",
		"email": "a@acme.test", "password": "hunter2hunter2", "full_name": "Alice",
	}, &boot)
	if boot.Token == "" || boot.ChannelID == 0 {
		t.Fatalf("bootstrap incomplete: %+v", boot)
	}

	// 2. Connect the gateway from zero: must replay channel.created.
	wsURL := strings.Replace(ts.URL, "http://", "ws://", 1) +
		"/api/v1/gateway?token=" + boot.Token + "&last_id=0"
	conn1, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("ws dial: %v", err)
	}
	first := readEnvelope(t, ctx, conn1)
	if first.Type != "channel.created" {
		t.Fatalf("expected channel.created replay, got %q", first.Type)
	}

	// 3. REST send → live gateway delivery with matching id.
	var sent struct {
		MessageID int64 `json:"message_id"`
	}
	postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, boot.ChannelID),
		boot.Token, map[string]any{"content": "hello, world"}, &sent)
	ev := readEnvelope(t, ctx, conn1)
	if ev.Type != "message.created" {
		t.Fatalf("expected message.created, got %q", ev.Type)
	}
	var p1 struct {
		MessageID int64 `json:"message_id"`
	}
	_ = json.Unmarshal(ev.Payload, &p1)
	if p1.MessageID != sent.MessageID {
		t.Fatalf("event/message id mismatch: %d vs %d", p1.MessageID, sent.MessageID)
	}
	resumeFrom := ev.Seq
	conn1.Close(websocket.StatusNormalClosure, "bye")

	// 4. Send while disconnected, then RESUME: the gap must replay (F-2).
	var sent2 struct {
		MessageID int64 `json:"message_id"`
	}
	postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, boot.ChannelID),
		boot.Token, map[string]any{"content": "missed me?"}, &sent2)

	conn2, _, err := websocket.Dial(ctx, strings.Replace(ts.URL, "http://", "ws://", 1)+
		fmt.Sprintf("/api/v1/gateway?token=%s&last_id=%d", boot.Token, resumeFrom), nil)
	if err != nil {
		t.Fatalf("ws redial: %v", err)
	}
	defer conn2.Close(websocket.StatusNormalClosure, "bye")
	ev2 := readEnvelope(t, ctx, conn2)
	var p2 struct {
		MessageID int64 `json:"message_id"`
	}
	_ = json.Unmarshal(ev2.Payload, &p2)
	if ev2.Type != "message.created" || p2.MessageID != sent2.MessageID {
		t.Fatalf("resume failed: got %q id=%d, want message.created id=%d",
			ev2.Type, p2.MessageID, sent2.MessageID)
	}
	if ev2.Seq <= resumeFrom {
		t.Fatalf("seq must advance past resume point: %d <= %d", ev2.Seq, resumeFrom)
	}

	// 5. Content is fetched by reference (F-4): REST GET matches.
	req, _ := http.NewRequestWithContext(ctx, "GET",
		fmt.Sprintf("%s/api/v1/messages/%d", ts.URL, sent2.MessageID), nil)
	req.Header.Set("Authorization", "Bearer "+boot.Token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get message: %v", err)
	}
	defer resp.Body.Close()
	var got struct {
		Source   string `json:"source"`
		Rendered string `json:"rendered"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&got)
	if resp.StatusCode != 200 || got.Source != "missed me?" {
		t.Fatalf("get message: status=%d source=%q", resp.StatusCode, got.Source)
	}

	// 6. Auth negative: no token → 401.
	badReq, _ := http.NewRequestWithContext(ctx, "GET",
		fmt.Sprintf("%s/api/v1/messages/%d", ts.URL, sent2.MessageID), nil)
	badResp, _ := http.DefaultClient.Do(badReq)
	badResp.Body.Close()
	if badResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 without token, got %d", badResp.StatusCode)
	}
}

func resetAndMigrate(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, `DROP SCHEMA public CASCADE; CREATE SCHEMA public`); err != nil {
		t.Fatalf("reset: %v", err)
	}
	// The production runner against the embedded schema: tests exercise the
	// same migration path operators run.
	if err := db.Migrate(ctx, pool, migrations.FS); err != nil {
		t.Fatalf("migrate: %v", err)
	}
}

func postJSON(t *testing.T, url, token string, body any, out any) {
	t.Helper()
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", url, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		t.Fatalf("POST %s: status %d", url, resp.StatusCode)
	}
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			t.Fatalf("decode %s: %v", url, err)
		}
	}
}

func readEnvelope(t *testing.T, ctx context.Context, conn *websocket.Conn) gateway.Envelope {
	t.Helper()
	rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for {
		_, data, err := conn.Read(rctx)
		if err != nil {
			t.Fatalf("ws read: %v", err)
		}
		var e gateway.Envelope
		if err := json.Unmarshal(data, &e); err != nil {
			t.Fatalf("bad envelope: %v", err)
		}
		if e.Type == "checkpoint" || e.Type == "ready" {
			continue
		}
		return e
	}
}

// TestAuthRateLimit: the pre-auth limiter must trip on brute-force pace and
// return the taxonomy's 429.
func TestAuthRateLimit(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		cancel()
		t.Fatalf("connect: %v", err)
	}
	defer func() { cancel(); pool.Close() }()
	resetAndMigrate(t, ctx, pool)

	hub := gateway.NewHub(pool, slog.Default())
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity: identity.New(pool, perms.New(pool)), Messaging: messaging.New(pool, perms.New(pool)),
	}))
	defer ts.Close()

	body := []byte(`{"org_slug":"none","email":"x@x.test","password":"wrongwrong"}`)
	var got429 bool
	for i := 0; i < 30; i++ {
		resp, err := http.Post(ts.URL+"/api/v1/auth/login", "application/json",
			bytes.NewReader(body))
		if err != nil {
			t.Fatalf("post: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusTooManyRequests {
			got429 = true
			break
		}
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("unexpected status %d", resp.StatusCode)
		}
	}
	if !got429 {
		t.Fatal("expected 429 after hammering login; limiter never tripped")
	}
}

// TestErrorTaxonomy: not-found and malformed-body map through the single
// error path; a request id header is always present.
func TestErrorTaxonomy(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		cancel()
		t.Fatalf("connect: %v", err)
	}
	defer func() { cancel(); pool.Close() }()
	resetAndMigrate(t, ctx, pool)

	hub := gateway.NewHub(pool, slog.Default())
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity: identity.New(pool, perms.New(pool)), Messaging: messaging.New(pool, perms.New(pool)),
	}))
	defer ts.Close()

	var boot struct {
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "tax", "email": "t@t.test", "password": "password123",
	}, &boot)

	// Not found → 404 with error envelope + request id.
	req, _ := http.NewRequest("GET", ts.URL+"/api/v1/messages/999999", nil)
	req.Header.Set("Authorization", "Bearer "+boot.Token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("want 404, got %d", resp.StatusCode)
	}
	if resp.Header.Get("X-Request-Id") == "" {
		t.Fatal("missing X-Request-Id header")
	}
	var e struct {
		Error string `json:"error"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&e)
	if e.Error != "message not found" {
		t.Fatalf("unexpected error message %q", e.Error)
	}

	// Malformed body → 400.
	resp2, err := http.Post(ts.URL+"/api/v1/orgs/bootstrap", "application/json",
		strings.NewReader("{not json"))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", resp2.StatusCode)
	}
}

// TestRichContentEndToEnd: markdown + mention flow through the real engine —
// stored AST, sanitized render, resolved mention id in both render and event
// payload.
func TestRichContentEndToEnd(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		cancel()
		t.Fatalf("connect: %v", err)
	}
	defer func() { cancel(); pool.Close() }()
	resetAndMigrate(t, ctx, pool)

	hub := gateway.NewHub(pool, slog.Default())
	go hub.Run(ctx)
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity: identity.New(pool, perms.New(pool)), Messaging: messaging.New(pool, perms.New(pool)),
	}))
	defer ts.Close()

	var boot struct {
		UserID    int64  `json:"user_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "rich", "email": "r@r.test", "password": "password123",
		"full_name": "Rich Owner",
	}, &boot)

	wsURL := strings.Replace(ts.URL, "http://", "ws://", 1) +
		"/api/v1/gateway?token=" + boot.Token + "&last_id=0"
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("ws dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "bye")
	_ = readEnvelope(t, ctx, conn) // channel.created replay

	var sent struct {
		MessageID int64 `json:"message_id"`
	}
	postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, boot.ChannelID),
		boot.Token, map[string]any{
			"content": "**hi** @**Rich Owner** [x](javascript:alert(1)) :rocket:",
		}, &sent)

	ev := readEnvelope(t, ctx, conn)
	var payload struct {
		Mentions []int64 `json:"mentions"`
	}
	_ = json.Unmarshal(ev.Payload, &payload)
	if len(payload.Mentions) != 1 || payload.Mentions[0] != boot.UserID {
		t.Fatalf("event mentions = %v, want [%d]", payload.Mentions, boot.UserID)
	}

	req, _ := http.NewRequestWithContext(ctx, "GET",
		fmt.Sprintf("%s/api/v1/messages/%d", ts.URL, sent.MessageID), nil)
	req.Header.Set("Authorization", "Bearer "+boot.Token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	var got struct {
		Rendered string `json:"rendered"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&got)
	for _, want := range []string{
		"<strong>hi</strong>",
		fmt.Sprintf(`data-user-id="%d"`, boot.UserID),
		"🚀",
	} {
		if !strings.Contains(got.Rendered, want) {
			t.Fatalf("rendered missing %q:\n%s", want, got.Rendered)
		}
	}
	if strings.Contains(got.Rendered, "javascript:") {
		t.Fatalf("unsafe href survived:\n%s", got.Rendered)
	}
}

// TestBootstrapErrorMapping: a duplicate slug is the ONLY 409; infrastructure
// failures (e.g. an unmigrated database) must surface as 500 — the old code
// mapped ANY org-insert failure to "org slug unavailable", which sent a live
// debugging session hunting a phantom slug collision.
func TestBootstrapErrorMapping(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		cancel()
		t.Fatalf("connect: %v", err)
	}
	defer func() { cancel(); pool.Close() }()
	resetAndMigrate(t, ctx, pool)

	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Log: slog.Default(),
		Identity:  identity.New(pool, perms.New(pool)),
		Messaging: messaging.New(pool, perms.New(pool)),
	}))
	defer ts.Close()

	body := map[string]any{"org_slug": "dup", "email": "a@d.test",
		"password": "password123", "full_name": "A"}
	if code := postJSONStatus(t, ts.URL+"/api/v1/orgs/bootstrap", "", body); code != http.StatusCreated {
		t.Fatalf("first bootstrap = %d, want 201", code)
	}
	body["email"] = "b@d.test"
	if code := postJSONStatus(t, ts.URL+"/api/v1/orgs/bootstrap", "", body); code != http.StatusConflict {
		t.Fatalf("duplicate slug = %d, want 409", code)
	}

	// Empty schema (no tables): an infra failure, not a slug conflict.
	if _, err := pool.Exec(ctx, `DROP SCHEMA public CASCADE; CREATE SCHEMA public`); err != nil {
		t.Fatalf("reset: %v", err)
	}
	body["org_slug"] = "fresh"
	if code := postJSONStatus(t, ts.URL+"/api/v1/orgs/bootstrap", "", body); code != http.StatusInternalServerError {
		t.Fatalf("bootstrap on unmigrated db = %d, want 500", code)
	}
}
