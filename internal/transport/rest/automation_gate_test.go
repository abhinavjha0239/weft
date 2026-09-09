package rest

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
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
	"github.com/abhinavjha0239/weft/internal/gateway"
)

// TestAutomationPrincipalGate is P-44b's headline: PostToChannelAsAutomation
// resolves send_message for its AUTHOR at the target channel, so a posting
// restriction binds automations exactly as it binds people (ADR-014 AU-2 as
// amended — the scope's authorization governs which RULES may exist, the
// author's verbs govern where they may POST).
//
// One org, so the narrowings compose:
//   - granted → the post lands, authored by the principal, and the event still
//     carries ActorAutomation + the rule id (the loop guard's input, a wire
//     contract this slice must not repoint);
//   - a channel-scope assignment the principal is not in → REFUSED: no message
//     row, run status 5, a trace naming author and channel, and the consumer's
//     cursor still advances (a refusal must never wedge the org);
//   - the same assignment pointed AT role:automations → the automation posts
//     and the org OWNER is refused in that channel (P-44's announcement shape);
//   - the principal's membership row deleted → refused, fail-closed; restored
//     → allowed again, so the gate reads live state rather than latching;
//   - a rule that borrows a HUMAN's identity is gated on THAT human — a
//     deliberate behaviour change for rules that already exist.
func TestAutomationPrincipalGate(t *testing.T) {
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
		"org_slug": "gated", "email": "alice@gated.test", "password": "password123",
		"full_name": "Alice Gate",
	}, &boot)
	bobTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"bob@gated.test", "Bob Ray", "bobgatedtok")
	var bobID int64
	if err := pool.QueryRow(ctx,
		`SELECT id FROM user_account WHERE org_id = $1 AND email = 'bob@gated.test'`,
		boot.OrgID).Scan(&bobID); err != nil {
		t.Fatalf("bob id: %v", err)
	}
	principal := automationPrincipalID(t, ctx, pool, boot.OrgID)

	newChannel := func(name string) int64 {
		t.Helper()
		var c struct {
			ChannelID int64 `json:"channel_id"`
		}
		postJSON(t, ts.URL+"/api/v1/channels", boot.Token, map[string]any{"name": name}, &c)
		return c.ChannelID
	}
	ops := newChannel("ops")
	hr := newChannel("hr")

	process := func() {
		t.Helper()
		drainConsumer(t, ctx, pool, "automations", boot.OrgID, runner.ProcessOrg)
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
	// posted counts the automation's OWN messages by the step's exact content
	// — state, never a status code.
	posted := func(channelID int64, needle string) int {
		t.Helper()
		return countRow(t, ctx, pool, "posts", `
			SELECT count(*) FROM message
			WHERE channel_id = $1 AND source = $2 AND deleted_at IS NULL`, channelID, needle)
	}
	// lastRun returns the newest run for a rule: status and step trace.
	lastRun := func(ruleID int64) (int16, string) {
		t.Helper()
		var status int16
		var steps string
		if err := pool.QueryRow(ctx, `
			SELECT status, steps::text FROM automation_run
			WHERE automation_id = $1 ORDER BY id DESC LIMIT 1`,
			ruleID).Scan(&status, &steps); err != nil {
			t.Fatalf("last run for %d: %v", ruleID, err)
		}
		return status, steps
	}
	// assignChannelSend writes the channel-scope send_message row — the one
	// P-44a's admin surface will write, and the one the resolver has always
	// known how to read.
	assignChannelSend := func(channelID int64, group string) {
		t.Helper()
		if err := db.WithTx(ctx, pool, func(tx pgx.Tx) error {
			gid, err := permsSvc.SystemGroupID(ctx, tx, boot.OrgID, group)
			if err != nil {
				return err
			}
			return permsSvc.Assign(ctx, tx, boot.OrgID, perms.VerbSendMessage,
				perms.ChannelRef(channelID), gid)
		}); err != nil {
			t.Fatalf("assign send_message at channel %d to %s: %v", channelID, group, err)
		}
	}
	clearChannelSend := func(channelID int64) {
		t.Helper()
		if _, err := pool.Exec(ctx, `
			DELETE FROM permission_assignment
			WHERE org_id = $1 AND verb = $2 AND scope_type = 3 AND scope_id = $3`,
			boot.OrgID, perms.VerbSendMessage, channelID); err != nil {
			t.Fatalf("clear channel assignment: %v", err)
		}
	}
	setPrincipalInGroup := func(member bool) {
		t.Helper()
		if err := db.WithTx(ctx, pool, func(tx pgx.Tx) error {
			gid, err := permsSvc.SystemGroupID(ctx, tx, boot.OrgID, perms.GroupAutomations)
			if err != nil {
				return err
			}
			if member {
				return permsSvc.AddUserToGroup(ctx, tx, boot.OrgID, gid, principal)
			}
			return permsSvc.RemoveUserFromGroup(ctx, tx, boot.OrgID, gid, principal)
		}); err != nil {
			t.Fatalf("set principal membership=%v: %v", member, err)
		}
	}
	newRule := func(name, verb string, target int64, content string, actor *int64) int64 {
		t.Helper()
		body := map[string]any{
			"scope_type": 1, "scope_id": boot.OrgID, "name": name,
			"definition": map[string]any{
				"trigger": map[string]any{"verb": verb},
				"steps": []map[string]any{{
					"kind": "post_message", "channel_id": target, "content": content}},
			},
		}
		if actor != nil {
			body["actor_user_id"] = *actor
		}
		var rule automation.Automation
		postJSON(t, ts.URL+"/api/v1/automations", boot.Token, body, &rule)
		if rule.ID == 0 {
			t.Fatalf("create rule %s failed", name)
		}
		return rule.ID
	}
	enable := func(id int64) {
		t.Helper()
		if code := patchJSON(t, fmt.Sprintf("%s/api/v1/automations/%d", ts.URL, id),
			boot.Token, map[string]any{"enabled": true}); code != http.StatusOK {
			t.Fatalf("enable %d = %d", id, code)
		}
	}
	cursor := func() int {
		t.Helper()
		return countRow(t, ctx, pool, "cursor", `
			SELECT COALESCE(last_id, 0) FROM event_consumer_cursor
			WHERE consumer = 'automations' AND org_id = $1`, boot.OrgID)
	}

	process() // drain the bootstrap/channel history before any rule exists.

	// --- 1. GRANTED: the org default carries the principal, so an org-scope
	// rule posts exactly as it did before the gate existed.
	echo := newRule("ops-echo", "message.created", ops, "ops-echo", nil)
	enable(echo)
	send(boot.Token, boot.ChannelID, "first")
	process()
	if n := posted(ops, "ops-echo"); n != 1 {
		t.Fatalf("granted: %d automation posts in #ops, want 1", n)
	}
	if status, steps := lastRun(echo); status != 2 || !strings.Contains(steps, `"status": "ok"`) {
		t.Fatalf("granted run = status %d steps %s, want 2/ok", status, steps)
	}
	// Authored by the principal; the EVENT still carries ActorAutomation + the
	// rule id, which this slice deliberately does not repoint.
	var msgID, authorID int64
	if err := pool.QueryRow(ctx, `
		SELECT id, author_id FROM message
		WHERE channel_id = $1 AND source = 'ops-echo' ORDER BY id DESC LIMIT 1`,
		ops).Scan(&msgID, &authorID); err != nil {
		t.Fatalf("read automation post: %v", err)
	}
	if authorID != principal {
		t.Fatalf("automation post authored by %d, want the principal %d", authorID, principal)
	}
	var actorKind int16
	var actorID *int64
	if err := pool.QueryRow(ctx, `
		SELECT actor_kind, actor_id FROM event_log
		WHERE org_id = $1 AND verb = 'message.created' AND entity_id = $2`,
		boot.OrgID, msgID).Scan(&actorKind, &actorID); err != nil {
		t.Fatalf("read post event: %v", err)
	}
	if actorKind != 3 || actorID == nil || *actorID != echo {
		t.Fatalf("post event = kind %d actor %v, want 3/%d (ActorAutomation + rule id)",
			actorKind, actorID, echo)
	}

	// --- 2. REFUSED: narrow #ops to a group the principal is not in.
	assignChannelSend(ops, perms.GroupOwners)
	cursorBefore := cursor()
	send(boot.Token, boot.ChannelID, "second")
	process()
	if n := posted(ops, "ops-echo"); n != 1 {
		t.Fatalf("refused: %d automation posts in #ops, want still 1", n)
	}
	status, steps := lastRun(echo)
	if status != 5 {
		t.Fatalf("refused run status = %d, want 5 (failed)", status)
	}
	wantReason := fmt.Sprintf("automation author %d lacks send_message in channel %d", principal, ops)
	if !strings.Contains(steps, wantReason) {
		t.Fatalf("refused run trace = %s, want it to name the reason %q", steps, wantReason)
	}
	// The refusal must not wedge the org: the cursor moved past the event.
	if after := cursor(); after <= cursorBefore {
		t.Fatalf("cursor %d did not advance past %d after a refused post", after, cursorBefore)
	}

	// --- 3. The announcement shape: point #ops at role:automations. The
	// automation posts; the org OWNER is refused in the same channel.
	assignChannelSend(ops, perms.GroupAutomations)
	send(boot.Token, boot.ChannelID, "third")
	process()
	if n := posted(ops, "ops-echo"); n != 2 {
		t.Fatalf("re-granted: %d automation posts in #ops, want 2", n)
	}
	if code := postJSONStatus(t, fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, ops),
		boot.Token, map[string]any{"content": "owner speaks"}); code != http.StatusForbidden {
		t.Fatalf("owner posting in an automations-only channel = %d, want 403", code)
	}

	// --- 4. FAIL CLOSED on the membership row. Back to the org default first,
	// so what follows isolates the group membership rather than the channel
	// assignment; then delete the row an admin would delete.
	clearChannelSend(ops)
	send(boot.Token, boot.ChannelID, "fourth")
	process()
	if n := posted(ops, "ops-echo"); n != 3 {
		t.Fatalf("org default restored: %d automation posts in #ops, want 3", n)
	}
	setPrincipalInGroup(false)
	if n := countRow(t, ctx, pool, "membership", `
		SELECT count(*) FROM user_group_member m JOIN user_group g ON g.id = m.group_id
		WHERE g.org_id = $1 AND g.name = 'role:automations' AND m.user_id = $2`,
		boot.OrgID, principal); n != 0 {
		t.Fatal("membership row survived the removal")
	}
	send(boot.Token, boot.ChannelID, "fifth")
	process()
	if n := posted(ops, "ops-echo"); n != 3 {
		t.Fatalf("ungrouped principal: %d automation posts in #ops, want still 3", n)
	}
	if status, steps := lastRun(echo); status != 5 || !strings.Contains(steps, wantReason) {
		t.Fatalf("ungrouped run = status %d steps %s, want 5 + %q", status, steps, wantReason)
	}
	setPrincipalInGroup(true)
	send(boot.Token, boot.ChannelID, "sixth")
	process()
	if n := posted(ops, "ops-echo"); n != 4 {
		t.Fatalf("membership restored: %d automation posts in #ops, want 4", n)
	}

	// --- 5. The rl.ActorUserID branch. A rule that borrows a human's identity
	// posts AS that human, so it is gated on THAT human. Disable the org-scope
	// echo first: an org rule sees every channel and would otherwise mint posts
	// of its own into what follows.
	if code := patchJSON(t, fmt.Sprintf("%s/api/v1/automations/%d", ts.URL, echo),
		boot.Token, map[string]any{"enabled": false}); code != http.StatusOK {
		t.Fatalf("disable echo = %d", code)
	}
	asBob := newRule("as-bob", "reaction.added", hr, "bob-echo", &bobID)
	if code := postJSONStatus(t, fmt.Sprintf("%s/api/v1/automations/%d/consent", ts.URL, asBob),
		bobTok, map[string]any{}); code != http.StatusOK {
		t.Fatalf("bob consent = %d", code)
	}
	enable(asBob)

	// #hr restricted to owners: bob is a plain member, so HIS rule is refused
	// even though the automation principal would have been allowed there.
	assignChannelSend(hr, perms.GroupOwners)
	react := func(emoji string) {
		t.Helper()
		if code := putJSON(t, fmt.Sprintf("%s/api/v1/messages/%d/reactions/%s",
			ts.URL, msgID, url.PathEscape(emoji)), boot.Token, nil); code != http.StatusOK {
			t.Fatalf("react %s = %d", emoji, code)
		}
	}
	react("👍")
	process()
	if n := posted(hr, "bob-echo"); n != 0 {
		t.Fatalf("human-actor rule posted %d times into a channel bob may not post in, want 0", n)
	}
	bobReason := fmt.Sprintf("automation author %d lacks send_message in channel %d", bobID, hr)
	if status, steps := lastRun(asBob); status != 5 || !strings.Contains(steps, bobReason) {
		t.Fatalf("human-actor run = status %d steps %s, want 5 + %q", status, steps, bobReason)
	}
	// Widen #hr to members: bob holds send_message there, so the rule posts —
	// authored by BOB, not by the principal.
	assignChannelSend(hr, perms.GroupMembers)
	react("🎉")
	process()
	if n := posted(hr, "bob-echo"); n != 1 {
		t.Fatalf("human-actor rule posts in #hr = %d, want 1", n)
	}
	var bobAuthored int64
	if err := pool.QueryRow(ctx, `
		SELECT author_id FROM message
		WHERE channel_id = $1 AND source = 'bob-echo' ORDER BY id DESC LIMIT 1`,
		hr).Scan(&bobAuthored); err != nil {
		t.Fatalf("read human-actor post: %v", err)
	}
	if bobAuthored != bobID {
		t.Fatalf("human-actor post authored by %d, want bob %d", bobAuthored, bobID)
	}
}
