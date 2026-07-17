package rest

import (
	"context"
	"fmt"
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
	"github.com/abhinavjha0239/weft/internal/domain/worktrack"
	"github.com/abhinavjha0239/weft/internal/gateway"
)

// TestAutomationRunner: AU-1/AU-4 end to end. Rules fire from the event
// log through real REST-created state; a run is keyed by (automation,
// event) so a full cursor-reset replay never double-fires; an automation's
// own events never re-trigger it and only trigger OTHER rules through the
// explicit allow_rule_trigger opt-in; the chat and work-tracking verbs are
// one trigger vocabulary; failures land as visible run traces, never
// silent drops.
func TestAutomationRunner(t *testing.T) {
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
		"org_slug": "run", "email": "a@run.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	bobTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"bob@run.test", "Bob Ray", "bobruntok")

	var announce struct {
		ChannelID int64 `json:"channel_id"`
	}
	postJSON(t, ts.URL+"/api/v1/channels", boot.Token, map[string]any{"name": "announce"}, &announce)

	process := func() {
		t.Helper()
		drainConsumer(t, ctx, pool, "automations", boot.OrgID, runner.ProcessOrg)
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
	send := func(tok string, channelID int64, content string) {
		t.Helper()
		var sent struct {
			MessageID int64 `json:"message_id"`
		}
		postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, channelID),
			tok, map[string]any{"content": content}, &sent)
		if sent.MessageID == 0 {
			t.Fatal("send failed")
		}
	}
	enable := func(id int64) {
		t.Helper()
		if code := patchJSON(t, fmt.Sprintf("%s/api/v1/automations/%d", ts.URL, id),
			boot.Token, map[string]any{"enabled": true}); code != http.StatusOK {
			t.Fatalf("enable %d = %d", id, code)
		}
	}

	// Drain bootstrap history before any rules exist.
	process()

	// Channel-scope auto-reply in #general.
	var reply automation.Automation
	postJSON(t, ts.URL+"/api/v1/automations", boot.Token, map[string]any{
		"scope_type": 3, "scope_id": boot.ChannelID, "name": "auto-reply",
		"definition": map[string]any{
			"trigger": map[string]any{"verb": "message.created"},
			"steps":   []any{map[string]any{"kind": "post_message", "content": "thanks!"}},
		}}, &reply)
	enable(reply.ID)

	// One trigger → exactly one reply, authored by the org's agent
	// principal, with a success run trace.
	send(bobTok, boot.ChannelID, "anyone around?")
	process()
	if n := countInChannel(boot.ChannelID, "thanks!"); n != 1 {
		t.Fatalf("replies = %d, want 1", n)
	}
	var authorKind int16
	if err := pool.QueryRow(ctx, `
		SELECT u.kind FROM message m JOIN user_account u ON u.id = m.author_id
		WHERE m.channel_id = $1 AND m.source = 'thanks!'`,
		boot.ChannelID).Scan(&authorKind); err != nil || authorKind != 2 {
		t.Fatalf("reply author kind = %d (%v), want 2 (agent)", authorKind, err)
	}

	// Loop guard: the reply's own message.created (actor = this automation)
	// must not re-trigger it, no matter how often we process.
	process()
	process()
	if n := countInChannel(boot.ChannelID, "thanks!"); n != 1 {
		t.Fatalf("replies after reprocess = %d, want still 1 (self-loop blocked)", n)
	}

	// Idempotency across replay: reset the cursor and re-consume EVERYTHING.
	// The (automation, event) key absorbs the redelivery — still one reply.
	if _, err := pool.Exec(ctx, `
		UPDATE event_consumer_cursor SET last_id = 0
		WHERE consumer = 'automations' AND org_id = $1`, boot.OrgID); err != nil {
		t.Fatalf("reset cursor: %v", err)
	}
	process()
	if n := countInChannel(boot.ChannelID, "thanks!"); n != 1 {
		t.Fatalf("replies after replay = %d, want 1 (idempotent runs)", n)
	}
	var runCount int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM automation_run WHERE automation_id = $1`, reply.ID).Scan(&runCount); err != nil || runCount != 1 {
		t.Fatalf("run rows = %d (%v), want 1", runCount, err)
	}

	// Rule→rule chaining is opt-in: an escalator in #general (allow_rule_
	// trigger=true) fires on BOTH bob's message and the automation's reply;
	// the auto-reply itself never fires on automation events (default off).
	var esc automation.Automation
	postJSON(t, ts.URL+"/api/v1/automations", boot.Token, map[string]any{
		"scope_type": 3, "scope_id": boot.ChannelID, "name": "escalator",
		"definition": map[string]any{
			"trigger": map[string]any{"verb": "message.created"},
			"steps":   []any{map[string]any{"kind": "post_message", "channel_id": 0, "content": "escalated"}},
		}}, &esc)
	if code := patchJSON(t, fmt.Sprintf("%s/api/v1/automations/%d", ts.URL, esc.ID),
		boot.Token, map[string]any{"allow_rule_trigger": true}); code != http.StatusOK {
		t.Fatal("set allow_rule_trigger")
	}
	enable(esc.ID)
	send(bobTok, boot.ChannelID, "second question")
	process()
	// bob's message → thanks (reply) + escalated (esc); the thanks event →
	// escalated again (esc allows rule triggers); escalated events → thanks?
	// NO (reply has allow_rule_trigger=false); esc's own events → self-block.
	if n := countInChannel(boot.ChannelID, "thanks!"); n != 2 {
		t.Fatalf("thanks after chain = %d, want 2", n)
	}
	if n := countInChannel(boot.ChannelID, "escalated"); n != 2 {
		t.Fatalf("escalated = %d, want 2 (bob's message + the reply, self blocked)", n)
	}

	// The fusion vocabulary: an org-scope rule on workitem.created posts an
	// announcement — a Jira-style trigger driving a chat-side step.
	var itemRule automation.Automation
	postJSON(t, ts.URL+"/api/v1/automations", boot.Token, map[string]any{
		"scope_type": 1, "scope_id": boot.OrgID, "name": "item-announce",
		"definition": map[string]any{
			"trigger": map[string]any{"verb": "workitem.created"},
			"steps": []any{map[string]any{
				"kind": "post_message", "channel_id": announce.ChannelID,
				"content": "a new item landed"}},
		}}, &itemRule)
	enable(itemRule.ID)
	var space struct {
		ID int64 `json:"id"`
	}
	postJSON(t, ts.URL+"/api/v1/spaces", boot.Token,
		map[string]any{"key": "OPS", "name": "Operations"}, &space)
	if code := postJSONStatus(t, fmt.Sprintf("%s/api/v1/spaces/%d/items", ts.URL, space.ID),
		boot.Token, map[string]any{"title": "fix the boiler"}); code != http.StatusCreated {
		t.Fatalf("create item = %d", code)
	}
	process()
	if n := countInChannel(announce.ChannelID, "a new item landed"); n != 1 {
		t.Fatalf("item announcements = %d, want 1", n)
	}

	// Disabled rules stay silent.
	if code := patchJSON(t, fmt.Sprintf("%s/api/v1/automations/%d", ts.URL, reply.ID),
		boot.Token, map[string]any{"enabled": false}); code != http.StatusOK {
		t.Fatal("disable")
	}
	send(bobTok, boot.ChannelID, "third question")
	process()
	if n := countInChannel(boot.ChannelID, "thanks!"); n != 2 {
		t.Fatalf("thanks after disable = %d, want still 2", n)
	}

	// A failing step (target archived after rule creation) records a FAILED
	// run with the error in its trace — visible, never silent.
	if code := patchJSON(t, fmt.Sprintf("%s/api/v1/channels/%d", ts.URL, announce.ChannelID),
		boot.Token, map[string]any{"archived": true}); code != http.StatusOK {
		t.Fatal("archive announce")
	}
	if code := postJSONStatus(t, fmt.Sprintf("%s/api/v1/spaces/%d/items", ts.URL, space.ID),
		boot.Token, map[string]any{"title": "fix the roof"}); code != http.StatusCreated {
		t.Fatal("create second item")
	}
	process()
	var runs struct {
		Runs []automation.Run `json:"runs"`
	}
	if code := getJSON(t, fmt.Sprintf("%s/api/v1/automations/%d/runs", ts.URL, itemRule.ID),
		boot.Token, &runs); code != http.StatusOK {
		t.Fatalf("list runs = %d", code)
	}
	if len(runs.Runs) != 2 {
		t.Fatalf("item-rule runs = %d, want 2", len(runs.Runs))
	}
	if runs.Runs[0].Status != 5 || runs.Runs[1].Status != 2 {
		t.Fatalf("run statuses = %d,%d, want failed(5) then success(2)",
			runs.Runs[0].Status, runs.Runs[1].Status)
	}
	if !strings.Contains(string(runs.Runs[0].Steps), "channel not found or archived") {
		t.Fatalf("failed trace = %s, want the archived-channel error", runs.Runs[0].Steps)
	}

	// Chain-depth cap (AU-4): two org-scope rules that each repost every
	// message into the other's channel, both opted into rule-triggering.
	// Self-loop blocking keeps each off its own messages, so they ping-pong
	// — bob's kick is depth 0, each hop +1 — until the run that would
	// exceed the cap lands as THROTTLED (status 6) and the cascade stops.
	// The earlier #general rules are disabled first for isolation.
	for _, id := range []int64{reply.ID, esc.ID} {
		if code := patchJSON(t, fmt.Sprintf("%s/api/v1/automations/%d", ts.URL, id),
			boot.Token, map[string]any{"enabled": false}); code != http.StatusOK {
			t.Fatalf("disable %d", id)
		}
	}
	var chanX, chanY struct {
		ChannelID int64 `json:"channel_id"`
	}
	postJSON(t, ts.URL+"/api/v1/channels", boot.Token, map[string]any{"name": "pingx"}, &chanX)
	postJSON(t, ts.URL+"/api/v1/channels", boot.Token, map[string]any{"name": "pingy"}, &chanY)
	mkPing := func(name string, targetID int64) automation.Automation {
		t.Helper()
		var a automation.Automation
		postJSON(t, ts.URL+"/api/v1/automations", boot.Token, map[string]any{
			"scope_type": 1, "scope_id": boot.OrgID, "name": name,
			"definition": map[string]any{
				"trigger": map[string]any{"verb": "message.created"},
				"steps": []any{map[string]any{
					"kind": "post_message", "channel_id": targetID, "content": "ping " + name}},
			}}, &a)
		if code := patchJSON(t, fmt.Sprintf("%s/api/v1/automations/%d", ts.URL, a.ID),
			boot.Token, map[string]any{"allow_rule_trigger": true}); code != http.StatusOK {
			t.Fatal("allow_rule_trigger")
		}
		enable(a.ID)
		return a
	}
	pa := mkPing("pa", chanY.ChannelID)
	pb := mkPing("pb", chanX.ChannelID)
	send(bobTok, boot.ChannelID, "kick")
	process()
	// Depths: kick=0 → each rule posts d1; each fires on the OTHER's d1 and
	// d2 posts; the d3 posts would spawn d4 → throttled instead.
	if n := countInChannel(chanY.ChannelID, "ping pa"); n != 3 {
		t.Fatalf("ping pa = %d, want 3 (d1..d3)", n)
	}
	if n := countInChannel(chanX.ChannelID, "ping pb"); n != 3 {
		t.Fatalf("ping pb = %d, want 3 (d1..d3)", n)
	}
	var throttled int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM automation_run
		WHERE automation_id = ANY($1) AND status = 6`,
		[]int64{pa.ID, pb.ID}).Scan(&throttled); err != nil || throttled != 2 {
		t.Fatalf("throttled runs = %d (%v), want 2 — the cascade must stop visibly", throttled, err)
	}
}
