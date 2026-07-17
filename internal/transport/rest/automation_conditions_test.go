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

	"github.com/abhinavjha0239/weft/internal/domain/automation"
	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/notification"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/domain/worktrack"
	"github.com/abhinavjha0239/weft/internal/gateway"
)

// TestAutomationConditions: P-22 AU-1 filters end to end. Conditions evaluate
// in match() before any DB work, so a non-matching event creates NO run row
// (deliberate at Slack scale). eq gates one channel and not another; in and
// exists gate as specified; and typing is STRICT — a string operand never
// coerces to a number.
func TestAutomationConditions(t *testing.T) {
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
		Worktrack:   worktrack.New(pool, permsSvc, msgSvc),
		Automations: automation.New(pool, permsSvc),
	}))
	defer ts.Close()
	runner := automation.NewRunner(pool, msgSvc, permsSvc, notification.New(pool), slog.Default())

	var boot struct {
		OrgID     int64  `json:"org_id"`
		UserID    int64  `json:"user_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "cond", "email": "a@cond.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	chanA := boot.ChannelID
	var annB, chanB struct {
		ChannelID int64 `json:"channel_id"`
	}
	postJSON(t, ts.URL+"/api/v1/channels", boot.Token, map[string]any{"name": "announce"}, &annB)
	postJSON(t, ts.URL+"/api/v1/channels", boot.Token, map[string]any{"name": "beta"}, &chanB)
	announce := annB.ChannelID

	process := func() {
		t.Helper()
		drainConsumer(t, ctx, pool, "automations", boot.OrgID, runner.ProcessOrg)
	}
	send := func(channelID int64, content string) {
		t.Helper()
		var sent struct {
			MessageID int64 `json:"message_id"`
		}
		postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, channelID),
			boot.Token, map[string]any{"content": content}, &sent)
		if sent.MessageID == 0 {
			t.Fatal("send failed")
		}
	}
	countInChannel := func(channelID int64, needle string) int {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM message
			WHERE channel_id = $1 AND source = $2 AND deleted_at IS NULL`,
			channelID, needle).Scan(&n); err != nil {
			t.Fatalf("count: %v", err)
		}
		return n
	}
	runCount := func(autoID int64) int {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM automation_run WHERE automation_id = $1`, autoID).Scan(&n); err != nil {
			t.Fatalf("run count: %v", err)
		}
		return n
	}
	mkRule := func(name string, verb string, conds []any, content string) automation.Automation {
		t.Helper()
		var a automation.Automation
		postJSON(t, ts.URL+"/api/v1/automations", boot.Token, map[string]any{
			"scope_type": 1, "scope_id": boot.OrgID, "name": name,
			"definition": map[string]any{
				"trigger":    map[string]any{"verb": verb},
				"conditions": conds,
				"steps": []any{map[string]any{
					"kind": "post_message", "channel_id": announce, "content": content}},
			}}, &a)
		if a.ID == 0 {
			t.Fatalf("create rule %q failed", name)
		}
		if code := patchJSON(t, fmt.Sprintf("%s/api/v1/automations/%d", ts.URL, a.ID),
			boot.Token, map[string]any{"enabled": true}); code != http.StatusOK {
			t.Fatalf("enable %q = %d", name, code)
		}
		return a
	}

	// Drain bootstrap history before any rules exist.
	process()

	eqGate := mkRule("eq-gate", "message.created",
		[]any{map[string]any{"path": "event.channel_id", "op": "eq", "value": chanA}}, "eq-fire")
	mkRule("in-gate", "message.created",
		[]any{map[string]any{"path": "event.channel_id", "op": "in", "value": []any{chanA, chanB.ChannelID}}}, "in-fire")
	// STRICT-TYPING PIN: the operand is chanA's id as a JSON STRING. With strict
	// typing a string never equals the numeric channel_id, so this rule fires on
	// neither channel. RED/GREEN: make scalarEqual coerce (e.g. compare a number
	// to string(json.Number)) and the string "N" matches the number N — this
	// rule then fires on chanA and strict-fire becomes 1, so the `== 0`
	// assertion below goes red.
	mkRule("strict", "message.created",
		[]any{map[string]any{"path": "event.channel_id", "op": "eq", "value": fmt.Sprintf("%d", chanA)}}, "strict-fire")

	send(chanA, "hello from A")
	send(chanB.ChannelID, "hello from B")
	process()

	if n := countInChannel(announce, "eq-fire"); n != 1 {
		t.Fatalf("eq-fire = %d, want 1 (only channel A matches)", n)
	}
	if n := countInChannel(announce, "in-fire"); n != 2 {
		t.Fatalf("in-fire = %d, want 2 (A and B both in the set)", n)
	}
	if n := countInChannel(announce, "strict-fire"); n != 0 {
		t.Fatalf("strict-fire = %d, want 0 (\"N\" must not coerce to N)", n)
	}
	// The no-run-on-miss guarantee: eq-gate ran ONCE (channel A), and channel
	// B's non-matching event wrote no run row at all.
	if n := runCount(eqGate.ID); n != 1 {
		t.Fatalf("eq-gate runs = %d, want 1 (a condition miss creates no run row)", n)
	}

	// exists gates on a payload key present only for the workitem verb.
	mkRule("exists-gate", "workitem.created",
		[]any{map[string]any{"path": "event.key", "op": "exists"}}, "exists-fire")
	var space struct {
		ID int64 `json:"id"`
	}
	postJSON(t, ts.URL+"/api/v1/spaces", boot.Token,
		map[string]any{"key": "OPS", "name": "Operations"}, &space)
	if code := postJSONStatus(t, fmt.Sprintf("%s/api/v1/spaces/%d/items", ts.URL, space.ID),
		boot.Token, map[string]any{"title": "fix the boiler"}); code != http.StatusCreated {
		t.Fatalf("create item failed")
	}
	process()
	if n := countInChannel(announce, "exists-fire"); n != 1 {
		t.Fatalf("exists-fire = %d, want 1 (event.key exists on workitem.created)", n)
	}
}
