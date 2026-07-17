package rest

import (
	"context"
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

	"github.com/abhinavjha0239/weft/internal/domain/automation"
	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/notification"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/gateway"
)

// postWebhook POSTs a raw body to an unauthenticated hook URL, returning the
// status and body so callers can assert oracle-free identical 404s.
func postWebhook(t *testing.T, url, body string) (int, string) {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// TestAutomationWebhooks: the inbound-webhook endpoint end to end. A valid
// capability URL fires a run with the body templated into the post; every
// authentication failure is one indistinguishable 404; body limits are
// enforced only after auth; and a burst trips the rate limit.
//
// RED/GREEN (the load-bearing security line): neuter the token comparison in
// AuthenticateWebhook (`return orgID, nil` before the ConstantTimeCompare, so
// any token is accepted) and the "wrong token -> 404" assertion below goes red
// (it becomes a 202). Restore to green. Verified manually while implementing.
func TestAutomationWebhooks(t *testing.T) {
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
	msgSvc := messaging.New(pool, permsSvc)
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity:    identity.New(pool, permsSvc),
		Messaging:   msgSvc,
		Automations: automation.New(pool, permsSvc),
	}))
	defer ts.Close()
	runner := automation.NewRunner(pool, msgSvc, permsSvc, notification.New(pool), slog.Default())

	var boot struct {
		OrgID     int64  `json:"org_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "hook", "email": "a@hook.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	drainConsumer(t, ctx, pool, "automations", boot.OrgID, runner.ProcessOrg)

	// A webhook rule whose step templates the inbound body.
	var rule automation.Automation
	postJSON(t, ts.URL+"/api/v1/automations", boot.Token, map[string]any{
		"scope_type": 3, "scope_id": boot.ChannelID, "name": "inbound",
		"definition": map[string]any{
			"trigger": map[string]any{"kind": "webhook"},
			"steps":   []any{map[string]any{"kind": "post_message", "content": "hook: {{event.body.x}}"}},
		}}, &rule)
	if rule.WebhookToken == nil || *rule.WebhookToken == "" {
		t.Fatalf("webhook rule has no token: %+v", rule)
	}
	token := *rule.WebhookToken
	if code := patchJSON(t, autoURL(ts, rule.ID), boot.Token,
		map[string]any{"enabled": true}); code != http.StatusOK {
		t.Fatalf("enable = %d", code)
	}
	// An event-kind rule for the "non-webhook rule" 404 case (it has no token).
	var eventRule automation.Automation
	postJSON(t, ts.URL+"/api/v1/automations", boot.Token, map[string]any{
		"scope_type": 3, "scope_id": boot.ChannelID, "name": "on-message",
		"definition": map[string]any{
			"trigger": map[string]any{"verb": "message.created"},
			"steps":   []any{map[string]any{"kind": "post_message", "content": "hi"}},
		}}, &eventRule)
	if eventRule.WebhookToken != nil {
		t.Fatalf("event rule should have no webhook token, got %v", eventRule.WebhookToken)
	}

	hookURL := func(id int64, tok string) string {
		return fmt.Sprintf("%s/api/v1/hooks/rules/%d/%s", ts.URL, id, tok)
	}
	countPosts := func(needle string) int {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM message
			WHERE channel_id = $1 AND source = $2 AND deleted_at IS NULL`,
			boot.ChannelID, needle).Scan(&n); err != nil {
			t.Fatalf("count: %v", err)
		}
		return n
	}

	// Happy path: 202, then the run posts the templated body.
	if code, body := postWebhook(t, hookURL(rule.ID, token), `{"x":"hello"}`); code != http.StatusAccepted {
		t.Fatalf("happy webhook = %d %s, want 202", code, body)
	}
	drainConsumer(t, ctx, pool, "automations", boot.OrgID, runner.ProcessOrg)
	if n := countPosts("hook: hello"); n != 1 {
		t.Fatalf("templated posts = %d, want 1", n)
	}

	// Three authentication failures, each an IDENTICAL 404 (oracle-free); the
	// disabled-rule 404 follows once we flip the rule off.
	wrongTokenCode, wrongTokenBody := postWebhook(t, hookURL(rule.ID, token+"x"), `{"x":"y"}`)
	unknownCode, unknownBody := postWebhook(t, hookURL(999999, token), `{"x":"y"}`)
	nonHookCode, nonHookBody := postWebhook(t, hookURL(eventRule.ID, token), `{"x":"y"}`)

	// The load-bearing assertion: a wrong token must be a 404, never a fire.
	if wrongTokenCode != http.StatusNotFound {
		t.Fatalf("wrong token = %d, want 404 (RED/GREEN pin: neutering the token compare makes this a 202)", wrongTokenCode)
	}
	if unknownCode != http.StatusNotFound || nonHookCode != http.StatusNotFound {
		t.Fatalf("unknown/non-webhook = %d/%d, want 404/404", unknownCode, nonHookCode)
	}
	for _, b := range []string{unknownBody, nonHookBody} {
		if b != wrongTokenBody {
			t.Fatalf("404 bodies differ (oracle): %q vs %q", b, wrongTokenBody)
		}
	}

	// Disabled rule: also a 404 with the correct token.
	if code := patchJSON(t, autoURL(ts, rule.ID), boot.Token,
		map[string]any{"enabled": false}); code != http.StatusOK {
		t.Fatalf("disable = %d", code)
	}
	if disabledCode, disabledBody := postWebhook(t, hookURL(rule.ID, token), `{"x":"y"}`); disabledCode != http.StatusNotFound || disabledBody != wrongTokenBody {
		t.Fatalf("disabled rule = %d %q, want an identical 404", disabledCode, disabledBody)
	}
	if code := patchJSON(t, autoURL(ts, rule.ID), boot.Token,
		map[string]any{"enabled": true}); code != http.StatusOK {
		t.Fatalf("re-enable = %d", code)
	}

	// Body checks run only AFTER auth: oversize is 413, invalid JSON is 400.
	big := `{"x":"` + strings.Repeat("a", 70*1024) + `"}`
	if code, _ := postWebhook(t, hookURL(rule.ID, token), big); code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversize = %d, want 413", code)
	}
	if code, _ := postWebhook(t, hookURL(rule.ID, token), `{not json`); code != http.StatusBadRequest {
		t.Fatalf("invalid json = %d, want 400", code)
	}
	// Neither of those should have fired a run.
	drainConsumer(t, ctx, pool, "automations", boot.OrgID, runner.ProcessOrg)
	if n := countPosts("hook: hello"); n != 1 {
		t.Fatalf("posts after bad bodies = %d, want still 1", n)
	}

	// A burst of authenticated hits trips the rate limit (429). From a single
	// test IP the per-IP authLimit and the per-rule hookLimit both burst at 10,
	// so the guard trips well within this loop; the per-rule limiter is what
	// bounds a single rule's ingest and external-service echo loops.
	var got429 bool
	for i := 0; i < 20; i++ {
		if code, _ := postWebhook(t, hookURL(rule.ID, token), `{"x":"z"}`); code == http.StatusTooManyRequests {
			got429 = true
			break
		}
	}
	if !got429 {
		t.Fatal("expected 429 after a burst of webhook posts; limiter never tripped")
	}
}
