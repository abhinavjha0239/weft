package rest

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/domain/automation"
	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/notification"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/gateway"
	"github.com/abhinavjha0239/weft/internal/platform/egress"
)

// TestAutomationHTTPSteps: P-24 outbound HTTP steps end to end. execute()
// ENQUEUES and never dials; the delivery lane sends the fixed server-marshaled
// envelope through the SSRF-guarded egress client with retry/backoff; a guard
// rejection is terminal immediately and the endpoint is proven NEVER dialed;
// and the AU-4 health ladder alerts at 5/15, auto-disables at 20, and gets a
// fresh window on self-serve re-enable.
//
// RED/GREEN pins (the load-bearing lines, both verified while implementing):
//  1. EGRESS BYPASS — replace r.egress.Post in sendDelivery with a plain
//     http.Client request: the loopback delivery in the SSRF phase SUCCEEDS,
//     so `private hits = 0` and the terminal-status assert both go red.
//  2. RESET DROP — remove the delivery_failures CASE reset from
//     Update(enabled=true): the re-enabled rule's next single failure lands on
//     a stale streak (21 >= 20) and insta-disables, so the final
//     `still enabled after one failure` assert goes red.
func TestAutomationHTTPSteps(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		cancel()
		t.Fatalf("connect: %v", err)
	}
	defer func() { cancel(); pool.Close() }()
	resetAndMigrate(t, ctx, pool)

	// The receiving endpoint. /ok accepts and captures; /flaky 500s twice then
	// accepts; /always500 always fails; /private counts hits (the SSRF phase
	// proves it stays at zero).
	var (
		okBody, okAuth, okCustom, okCT string
		flakyHits, privateHits         atomic.Int64
	)
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ok":
			b, _ := io.ReadAll(r.Body)
			okBody, okAuth = string(b), r.Header.Get("Authorization")
			okCustom, okCT = r.Header.Get("X-Custom-Test"), r.Header.Get("Content-Type")
			w.WriteHeader(http.StatusOK)
		case "/flaky":
			if flakyHits.Add(1) <= 2 {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
		case "/private":
			privateHits.Add(1)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer receiver.Close()

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
	// The main runner's egress allows loopback so tests can reach the httptest
	// receiver; the SSRF phase uses a STRICT production-shaped client instead.
	runner := automation.NewRunner(pool, msgSvc, permsSvc, notification.New(pool), slog.Default())
	runner.SetEgress(egress.New(egress.Options{UserAgent: "weftbot-test/1.0", AllowLoopbackForTests: true}))
	strictRunner := automation.NewRunner(pool, msgSvc, permsSvc, notification.New(pool), slog.Default())
	strictRunner.SetEgress(egress.New(egress.Options{UserAgent: "weftbot-test/1.0"}))

	var boot struct {
		OrgID     int64  `json:"org_id"`
		UserID    int64  `json:"user_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "outb", "email": "a@outb.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	if err := runner.ProcessOrg(ctx, boot.OrgID); err != nil {
		t.Fatalf("drain: %v", err)
	}

	// mkHTTPRule seeds an enabled org-scope rule by SQL: the write-time shape
	// gate is PRODUCTION-strict (standard ports only), so the httptest
	// receiver's random port can never pass POST /automations — the gate
	// itself is asserted separately below (post400 "non-standard port") and in
	// TestValidateHTTPStep; this test exercises the delivery lane behind it.
	mkHTTPRule := func(name, path string, headers map[string]string) automation.Automation {
		t.Helper()
		step := map[string]any{"kind": "http_request", "url": receiver.URL + path}
		if headers != nil {
			step["headers"] = headers
		}
		def, err := json.Marshal(map[string]any{
			"trigger": map[string]any{"verb": "message.created"},
			"steps":   []any{step},
		})
		if err != nil {
			t.Fatalf("marshal def: %v", err)
		}
		var a automation.Automation
		a.Name = name
		if err := pool.QueryRow(ctx, `
			INSERT INTO automation (org_id, scope_type, scope_id, name, definition, enabled, created_by)
			VALUES ($1, 1, $1, $2, $3, true, $4) RETURNING id`,
			boot.OrgID, name, def, boot.UserID).Scan(&a.ID); err != nil {
			t.Fatalf("seed rule %q: %v", name, err)
		}
		return a
	}
	setEnabled := func(id int64, on bool) {
		t.Helper()
		if code := patchJSON(t, autoURL(ts, id), boot.Token,
			map[string]any{"enabled": on}); code != http.StatusOK {
			t.Fatalf("toggle %d = %d", id, code)
		}
	}
	fire := func() {
		t.Helper()
		var sent struct {
			MessageID int64 `json:"message_id"`
		}
		postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, boot.ChannelID),
			boot.Token, map[string]any{"content": "ping"}, &sent)
		if sent.MessageID == 0 {
			t.Fatal("send failed")
		}
		if err := runner.ProcessOrg(ctx, boot.OrgID); err != nil {
			t.Fatalf("process: %v", err)
		}
	}
	lane := func(r *automation.Runner) int {
		t.Helper()
		n, err := r.RunDueDeliveries(ctx, time.Now())
		if err != nil {
			t.Fatalf("deliveries: %v", err)
		}
		return n
	}
	rewind := func(ruleID int64) {
		t.Helper()
		if _, err := pool.Exec(ctx, `
			UPDATE webhook_delivery SET next_attempt_at = now() - interval '1 hour'
			WHERE automation_id = $1 AND status = 1`, ruleID); err != nil {
			t.Fatalf("rewind: %v", err)
		}
	}
	lastDelivery := func(ruleID int64) (id int64, status int16, attempts int32, code *int32, lastErr string) {
		t.Helper()
		if err := pool.QueryRow(ctx, `
			SELECT id, status, attempts, last_status_code, last_error FROM webhook_delivery
			WHERE automation_id = $1 ORDER BY id DESC LIMIT 1`, ruleID).
			Scan(&id, &status, &attempts, &code, &lastErr); err != nil {
			t.Fatalf("last delivery for %d: %v", ruleID, err)
		}
		return
	}
	failures := func(ruleID int64) int {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx,
			`SELECT delivery_failures FROM automation WHERE id = $1`, ruleID).Scan(&n); err != nil {
			t.Fatalf("failures for %d: %v", ruleID, err)
		}
		return n
	}
	enabled := func(ruleID int64) bool {
		t.Helper()
		var on bool
		if err := pool.QueryRow(ctx,
			`SELECT enabled FROM automation WHERE id = $1`, ruleID).Scan(&on); err != nil {
			t.Fatalf("enabled for %d: %v", ruleID, err)
		}
		return on
	}

	// ---- Happy path: enqueue, deliver, envelope, dashboard. ----
	ruleA := mkHTTPRule("notify", "/ok", map[string]string{
		"Authorization": "Bearer tok", "X-Custom-Test": "yes"})
	fire()
	// The run finished success with a queued trace BEFORE any dial.
	var runStatus int16
	var runSteps string
	if err := pool.QueryRow(ctx, `
		SELECT status, steps::text FROM automation_run
		WHERE automation_id = $1 ORDER BY id DESC LIMIT 1`, ruleA.ID).
		Scan(&runStatus, &runSteps); err != nil {
		t.Fatalf("run: %v", err)
	}
	if runStatus != 2 || !strings.Contains(runSteps, `"queued"`) {
		t.Fatalf("run = status %d steps %s, want success with a queued trace", runStatus, runSteps)
	}
	if n := lane(runner); n != 1 {
		t.Fatalf("lane attempted %d, want 1", n)
	}
	delID, st, att, code, _ := lastDelivery(ruleA.ID)
	if st != 2 || att != 1 || code == nil || *code != 200 {
		t.Fatalf("delivery = status %d attempts %d code %v, want delivered/1/200", st, att, code)
	}
	if okAuth != "Bearer tok" || okCustom != "yes" || okCT != "application/json" {
		t.Fatalf("headers = auth %q custom %q ct %q", okAuth, okCustom, okCT)
	}
	var env struct {
		AutomationID   int64  `json:"automation_id"`
		AutomationName string `json:"automation_name"`
		RunID          int64  `json:"run_id"`
		DeliveryID     int64  `json:"delivery_id"`
		Attempt        int    `json:"attempt"`
		Event          struct {
			ID      int64           `json:"id"`
			Verb    string          `json:"verb"`
			Payload json.RawMessage `json:"payload"`
		} `json:"event"`
	}
	if err := json.Unmarshal([]byte(okBody), &env); err != nil {
		t.Fatalf("envelope: %v (%s)", err, okBody)
	}
	if env.AutomationID != ruleA.ID || env.AutomationName != "notify" ||
		env.RunID == 0 || env.DeliveryID != delID || env.Attempt != 1 ||
		env.Event.Verb != "message.created" ||
		!strings.Contains(string(env.Event.Payload), "channel_id") {
		t.Fatalf("envelope = %+v (%s)", env, okBody)
	}
	if n := failures(ruleA.ID); n != 0 {
		t.Fatalf("failures after success = %d, want 0", n)
	}
	var dash struct {
		Deliveries []automation.Delivery `json:"deliveries"`
	}
	if code := getJSON(t, autoURL(ts, ruleA.ID)+"/deliveries", boot.Token, &dash); code != http.StatusOK {
		t.Fatalf("deliveries dashboard = %d", code)
	}
	if len(dash.Deliveries) != 1 || dash.Deliveries[0].Status != 2 {
		t.Fatalf("dashboard = %+v, want the one delivered row", dash.Deliveries)
	}
	setEnabled(ruleA.ID, false)

	// ---- Retry: 500, 500, then 200 -> delivered on the third attempt. ----
	ruleB := mkHTTPRule("retry", "/flaky", nil)
	fire()
	for i := 0; i < 3; i++ {
		if n := lane(runner); n != 1 {
			t.Fatalf("retry lane %d attempted %d, want 1", i, n)
		}
		rewind(ruleB.ID)
	}
	_, st, att, code, _ = lastDelivery(ruleB.ID)
	if st != 2 || att != 3 || code == nil || *code != 200 {
		t.Fatalf("flaky delivery = status %d attempts %d code %v, want delivered/3/200", st, att, code)
	}
	if n := failures(ruleB.ID); n != 0 {
		t.Fatalf("failures after retried success = %d, want 0 (never terminal)", n)
	}
	setEnabled(ruleB.ID, false)

	// ---- Terminal: four failures exhaust the attempts. ----
	ruleC := mkHTTPRule("dead", "/always500", nil)
	fire()
	for i := 0; i < 4; i++ {
		lane(runner)
		rewind(ruleC.ID)
	}
	_, st, att, code, _ = lastDelivery(ruleC.ID)
	if st != 3 || att != 4 || code == nil || *code != 500 {
		t.Fatalf("dead delivery = status %d attempts %d code %v, want terminal/4/500", st, att, code)
	}
	if n := failures(ruleC.ID); n != 1 {
		t.Fatalf("failures after one terminal = %d, want 1", n)
	}
	setEnabled(ruleC.ID, false)

	// ---- Validation 400s (the unit table covers the full matrix). ----
	post400 := func(name string, steps []any) {
		t.Helper()
		var a automation.Automation
		if code := postJSONStatus2(t, ts.URL+"/api/v1/automations", boot.Token, map[string]any{
			"scope_type": 1, "scope_id": boot.OrgID, "name": name,
			"definition": map[string]any{
				"trigger": map[string]any{"verb": "message.created"},
				"steps":   steps,
			}}, &a); code != http.StatusBadRequest {
			t.Fatalf("%s = %d, want 400", name, code)
		}
	}
	post400("templated url", []any{map[string]any{
		"kind": "http_request", "url": "https://example.com/{{event.x}}"}})
	post400("content on http step", []any{map[string]any{
		"kind": "http_request", "url": "https://example.com/ok", "content": "hi"}})
	// The write-time shape gate is production-strict: the httptest receiver's
	// random port is exactly the odd-destination shape it exists to reject
	// (which is why this test seeds its rules by SQL).
	post400("non-standard port", []any{map[string]any{
		"kind": "http_request", "url": receiver.URL + "/ok"}})
	httpStep := map[string]any{"kind": "http_request", "url": "https://example.com/ok"}
	post400("four http steps", []any{httpStep, httpStep, httpStep, httpStep})

	// ---- SSRF PIN: the guard rejects a loopback destination pre-dial. ----
	// The strict runner's egress has NO test allowances, so the receiver's
	// 127.0.0.1 address is exactly the internal-destination shape. The
	// delivery goes terminal IMMEDIATELY (no retries — the destination will
	// never become allowed) and /private is proven never dialed.
	ruleE := mkHTTPRule("exfil", "/private", nil)
	fire()
	if n := lane(strictRunner); n != 1 {
		t.Fatalf("strict lane attempted %d, want 1", n)
	}
	_, st, att, code, lastErr := lastDelivery(ruleE.ID)
	if st != 3 || att != 1 || code != nil {
		t.Fatalf("guarded delivery = status %d attempts %d code %v, want terminal/1/no-code", st, att, code)
	}
	if !strings.Contains(lastErr, "not allowed") {
		t.Fatalf("guarded delivery error = %q, want the guard's rejection", lastErr)
	}
	if n := privateHits.Load(); n != 0 {
		t.Fatalf("private endpoint hits = %d, want 0 (the guard must reject before any dial)", n)
	}
	setEnabled(ruleE.ID, false)

	// ---- Health ladder: alerts at 5 and 15, auto-disable at 20, reset on
	// re-enable. One real delivered run seeds the run_id; the 20 terminal
	// failures are seeded pending rows with attempts=3 (the next failure is
	// each row's 4th = terminal), drained in one lane pass. ----
	ruleF := mkHTTPRule("health", "/ok", nil)
	fire()
	if n := lane(runner); n != 1 {
		t.Fatalf("health baseline lane attempted %d, want 1", n)
	}
	var seedRunID int64
	if err := pool.QueryRow(ctx, `
		SELECT run_id FROM webhook_delivery WHERE automation_id = $1 ORDER BY id DESC LIMIT 1`,
		ruleF.ID).Scan(&seedRunID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	seed := func(n int) {
		t.Helper()
		if _, err := pool.Exec(ctx, `
			INSERT INTO webhook_delivery (org_id, automation_id, run_id, url, payload, attempts, next_attempt_at)
			SELECT $1, $2, $3, $4, '{}'::jsonb, 3, now() - interval '1 hour'
			FROM generate_series(1, $5)`,
			boot.OrgID, ruleF.ID, seedRunID, receiver.URL+"/always500", n); err != nil {
			t.Fatalf("seed deliveries: %v", err)
		}
	}
	seed(20)
	if n := lane(runner); n != 20 {
		t.Fatalf("health lane attempted %d, want 20", n)
	}
	if on := enabled(ruleF.ID); on {
		t.Fatal("rule still enabled after 20 consecutive terminal failures, want auto-disabled")
	}
	if n := failures(ruleF.ID); n != 20 {
		t.Fatalf("failures = %d, want 20", n)
	}
	var disableEvents int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM event_log
		WHERE org_id = $1 AND verb = 'automation.auto_disabled' AND entity_id = $2`,
		boot.OrgID, ruleF.ID).Scan(&disableEvents); err != nil {
		t.Fatalf("disable events: %v", err)
	}
	if disableEvents != 1 {
		t.Fatalf("auto_disabled events = %d, want exactly 1 (transition-guarded)", disableEvents)
	}
	// The org admin heard about it exactly three times: at 5, at 15 ("disable
	// imminent"), and at 20 (the disable) — distinct deliveries, so the P-25
	// dedupe key never collides.
	var alerts int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM notification
		WHERE user_id = $1 AND kind = 6 AND entity_type = 21`,
		boot.UserID).Scan(&alerts); err != nil {
		t.Fatalf("alerts: %v", err)
	}
	if alerts != 3 {
		t.Fatalf("health alerts = %d, want 3 (at 5, 15, and 20)", alerts)
	}

	// RESET PIN: self-serve re-enable gets a FRESH window. Without the CASE
	// reset in Update(enabled=true), the stale streak (21 >= 20) would
	// insta-disable on the very next failure.
	setEnabled(ruleF.ID, true)
	if n := failures(ruleF.ID); n != 0 {
		t.Fatalf("failures after re-enable = %d, want 0 (the fresh window)", n)
	}
	seed(1)
	if n := lane(runner); n != 1 {
		t.Fatalf("post-re-enable lane attempted %d, want 1", n)
	}
	if n := failures(ruleF.ID); n != 1 {
		t.Fatalf("failures after one post-re-enable terminal = %d, want 1", n)
	}
	if on := enabled(ruleF.ID); !on {
		t.Fatal("rule disabled after ONE failure post-re-enable — still enabled wanted (the reset pin)")
	}
}
