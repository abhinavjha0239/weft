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

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/domain/automation"
	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/notification"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/domain/worktrack"
	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/internal/eventlog"
	"github.com/abhinavjha0239/weft/internal/gateway"
)

// TestAutomationTemplating: P-22 {{event.path}} interpolation and the
// load-bearing mention-injection guard, end to end. A real payload key renders
// into a post; a non-scalar path and a post-expansion overflow both surface as
// FAILED runs with their trace error; and an attacker-controlled value carrying
// "@**Name**" can never mint a mention — the step fails and no notification
// fans out.
func TestAutomationTemplating(t *testing.T) {
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
	notifRunner := notification.NewRunner(pool, nil, slog.Default())

	var boot struct {
		OrgID     int64  `json:"org_id"`
		UserID    int64  `json:"user_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "tmpl", "email": "a@tmpl.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	general := boot.ChannelID
	addChannelMember(t, ctx, pool, boot.OrgID, general, "bob@tmpl.test", "Bob Ray", "bobtmpltok")
	var bobID int64
	if err := pool.QueryRow(ctx, `SELECT id FROM user_account WHERE email = 'bob@tmpl.test'`).Scan(&bobID); err != nil {
		t.Fatalf("bob id: %v", err)
	}
	var ann struct {
		ChannelID int64 `json:"channel_id"`
	}
	postJSON(t, ts.URL+"/api/v1/channels", boot.Token, map[string]any{"name": "announce"}, &ann)
	announce := ann.ChannelID

	process := func() {
		t.Helper()
		if err := runner.ProcessOrg(ctx, boot.OrgID); err != nil {
			t.Fatalf("process: %v", err)
		}
	}
	notifProcess := func() {
		t.Helper()
		if err := notifRunner.ProcessOrg(ctx, boot.OrgID); err != nil {
			t.Fatalf("notif process: %v", err)
		}
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
	// appendMsgEvent injects a synthetic message.created carrying an
	// attacker-controlled field, exactly as a P-23 webhook/slash payload will.
	appendMsgEvent := func(channelID int64, extra map[string]any) {
		t.Helper()
		payload := map[string]any{"channel_id": channelID}
		for k, v := range extra {
			payload[k] = v
		}
		if err := db.WithTx(ctx, pool, func(tx pgx.Tx) error {
			_, e := eventlog.Append(ctx, tx, eventlog.Event{
				OrgID: boot.OrgID, ActorKind: enum.ActorHuman, ActorID: &boot.UserID,
				EntityType: enum.EntityMessage, EntityID: channelID, Verb: "message.created",
				Payload: eventlog.MustPayload(payload),
			})
			return e
		}); err != nil {
			t.Fatalf("append synthetic event: %v", err)
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
	lastRun := func(autoID int64) (int16, string) {
		t.Helper()
		var status int16
		var steps []byte
		if err := pool.QueryRow(ctx, `
			SELECT status, steps FROM automation_run
			WHERE automation_id = $1 ORDER BY id DESC LIMIT 1`, autoID).Scan(&status, &steps); err != nil {
			t.Fatalf("last run for %d: %v", autoID, err)
		}
		return status, string(steps)
	}
	mkRule := func(name string, scopeType int16, scopeID int64, verb, content string, channelID int64) automation.Automation {
		t.Helper()
		step := map[string]any{"kind": "post_message", "content": content}
		if channelID != 0 {
			step["channel_id"] = channelID
		}
		var a automation.Automation
		postJSON(t, ts.URL+"/api/v1/automations", boot.Token, map[string]any{
			"scope_type": scopeType, "scope_id": scopeID, "name": name,
			"definition": map[string]any{
				"trigger": map[string]any{"verb": verb},
				"steps":   []any{step},
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
	disable := func(id int64) {
		t.Helper()
		if code := patchJSON(t, fmt.Sprintf("%s/api/v1/automations/%d", ts.URL, id),
			boot.Token, map[string]any{"enabled": false}); code != http.StatusOK {
			t.Fatalf("disable %d = %d", id, code)
		}
	}

	// Drain bootstrap history on both consumers.
	process()
	notifProcess()

	// (1) Happy path: a workitem.created rule renders the real item key.
	r1 := mkRule("item-key", 1, boot.OrgID, "workitem.created", "New item {{event.key}}", announce)
	var space struct {
		ID int64 `json:"id"`
	}
	postJSON(t, ts.URL+"/api/v1/spaces", boot.Token,
		map[string]any{"key": "OPS", "name": "Operations"}, &space)
	var item struct {
		Key string `json:"key"`
	}
	postJSON(t, fmt.Sprintf("%s/api/v1/spaces/%d/items", ts.URL, space.ID),
		boot.Token, map[string]any{"title": "fix the boiler"}, &item)
	if item.Key == "" {
		t.Fatal("item key missing")
	}
	process()
	if n := countInChannel(announce, "New item "+item.Key); n != 1 {
		t.Fatalf("templated post = %d, want 1 (content %q)", n, "New item "+item.Key)
	}
	if status, _ := lastRun(r1.ID); status != 2 {
		t.Fatalf("item-key run status = %d, want 2 (success)", status)
	}
	disable(r1.ID)

	// (2) A path resolving to a non-scalar (the mentions array) fails the step.
	r2 := mkRule("nonscalar", 3, general, "message.created", "{{event.mentions}}", 0)
	send(general, "@**Alice Chen** ping")
	process()
	if status, steps := lastRun(r2.ID); status != 5 || !strings.Contains(steps, "resolves to a non-scalar") {
		t.Fatalf("nonscalar run = status %d steps %s, want failed with non-scalar trace", status, steps)
	}
	disable(r2.ID)

	// (3) Overflow past the content cap fails rather than truncating; (4) reuses
	// the same {{event.x}} rule to prove the mention guard.
	rx := mkRule("x-echo", 3, general, "message.created", "{{event.x}}", 0)
	appendMsgEvent(general, map[string]any{"x": strings.Repeat("a", 4001)})
	process()
	if status, steps := lastRun(rx.ID); status != 5 || !strings.Contains(steps, "post-expansion content") {
		t.Fatalf("overflow run = status %d steps %s, want failed with overflow trace", status, steps)
	}

	// (4) MENTION-SMUGGLE PIN. The synthetic value carries a real member's
	// @**Bob Ray** string; the expanded content's mention-label multiset differs
	// from the literal {{event.x}} (which has none), so the step FAILS and no
	// message posts. RED/GREEN: neuter the multiset check in renderStep (drop
	// the equalLabels guard, or compare node COUNT instead of the label
	// multiset) — the smuggled mention posts, insertThreadMessageAs resolves it
	// to Bob, doc.Mentions() fans out, and the notif runner materializes a
	// kind-2 mention for Bob, so the `bobMentions == 0` assertion goes red.
	appendMsgEvent(general, map[string]any{"x": "@**Bob Ray**"})
	process()
	notifProcess()
	var bobMentions int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM notification
		WHERE user_id = $1 AND kind = 2`, bobID).Scan(&bobMentions); err != nil {
		t.Fatalf("bob mention count: %v", err)
	}
	if bobMentions != 0 {
		t.Fatalf("bob mention notifications = %d, want 0 (a templated value must not mint mentions)", bobMentions)
	}
	if status, steps := lastRun(rx.ID); status != 5 || !strings.Contains(steps, "template expansion may not alter mentions") {
		t.Fatalf("smuggle run = status %d steps %s, want failed with mention-guard trace", status, steps)
	}
	if n := countInChannel(general, "@**Bob Ray**"); n != 0 {
		t.Fatalf("smuggled message posts = %d, want 0", n)
	}
}
