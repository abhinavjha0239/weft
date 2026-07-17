package rest

import (
	"context"
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
	"github.com/abhinavjha0239/weft/internal/gateway"
)

// TestAutomationSlash: slash-command triggers end to end. A member's
// invocation fires matching rules with the text templated into the post; an
// org-scope rule fires from any channel while a channel-scope rule fires only
// from its own; and a non-member invoking against a channel they cannot post
// to gets the channel-send gate's denial.
//
// RED/GREEN (the load-bearing access-control line): drop the membership check
// from messaging.RequireChannelSend (delete the `s.requireMember(...)` call),
// and the non-member denial below goes red — the non-member's invocation
// becomes a 202 instead of a 403. Restore to green. Verified manually while
// implementing.
func TestAutomationSlash(t *testing.T) {
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
	autoSvc := automation.New(pool, permsSvc)
	autoSvc.SetMessaging(msgSvc)
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity:    identity.New(pool, permsSvc),
		Messaging:   msgSvc,
		Automations: autoSvc,
	}))
	defer ts.Close()
	runner := automation.NewRunner(pool, msgSvc, permsSvc, notification.New(pool), slog.Default())

	var boot struct {
		OrgID     int64  `json:"org_id"`
		ChannelID int64  `json:"channel_id"` // #general, alice + bob
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "slash", "email": "a@slash.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	bobTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"bob@slash.test", "Bob Ray", "bobslashtok")

	// announce: alice-only; secret: alice-only (bob is a member of neither).
	var announce, secret struct {
		ChannelID int64 `json:"channel_id"`
	}
	postJSON(t, ts.URL+"/api/v1/channels", boot.Token, map[string]any{"name": "announce"}, &announce)
	postJSON(t, ts.URL+"/api/v1/channels", boot.Token,
		map[string]any{"name": "secret", "visibility": "private"}, &secret)

	// Org-scope "deploy" posts to #announce from anywhere; channel-scope "note"
	// posts into #general only when invoked from #general.
	var deploy, note automation.Automation
	postJSON(t, ts.URL+"/api/v1/automations", boot.Token, map[string]any{
		"scope_type": 1, "scope_id": boot.OrgID, "name": "deploy-rule",
		"definition": map[string]any{
			"trigger": map[string]any{"kind": "slash", "command": "deploy"},
			"steps": []any{map[string]any{"kind": "post_message",
				"channel_id": announce.ChannelID, "content": "deploy: {{event.text}}"}},
		}}, &deploy)
	postJSON(t, ts.URL+"/api/v1/automations", boot.Token, map[string]any{
		"scope_type": 3, "scope_id": boot.ChannelID, "name": "note-rule",
		"definition": map[string]any{
			"trigger": map[string]any{"kind": "slash", "command": "note"},
			"steps":   []any{map[string]any{"kind": "post_message", "content": "note: {{event.text}}"}},
		}}, &note)
	for _, id := range []int64{deploy.ID, note.ID} {
		if code := patchJSON(t, autoURL(ts, id), boot.Token,
			map[string]any{"enabled": true}); code != http.StatusOK {
			t.Fatalf("enable %d = %d", id, code)
		}
	}
	drainConsumer(t, ctx, pool, "automations", boot.OrgID, runner.ProcessOrg)

	slash := func(tok, command string, channelID int64, text string) int {
		t.Helper()
		return postJSONStatus(t, ts.URL+"/api/v1/automations/slash", tok, map[string]any{
			"command": command, "channel_id": channelID, "text": text})
	}
	count := func(channelID int64, needle string) int {
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

	// A member fires the org rule; the text is templated into the post.
	if code := slash(bobTok, "deploy", boot.ChannelID, "v2"); code != http.StatusAccepted {
		t.Fatalf("member deploy = %d, want 202", code)
	}
	// The org rule fires from a DIFFERENT channel too (alice, from #announce).
	if code := slash(boot.Token, "deploy", announce.ChannelID, "v3"); code != http.StatusAccepted {
		t.Fatalf("org-scope deploy from announce = %d, want 202", code)
	}
	// The channel-scope rule fires from its OWN channel...
	if code := slash(bobTok, "note", boot.ChannelID, "hi"); code != http.StatusAccepted {
		t.Fatalf("member note = %d, want 202", code)
	}
	// ...but NOT from a different channel (alice invokes it from #announce).
	if code := slash(boot.Token, "note", announce.ChannelID, "nope"); code != http.StatusAccepted {
		t.Fatalf("note from announce = %d, want 202 (invocation accepted, rule just won't match)", code)
	}
	drainConsumer(t, ctx, pool, "automations", boot.OrgID, runner.ProcessOrg)
	if n := count(announce.ChannelID, "deploy: v2"); n != 1 {
		t.Fatalf("deploy v2 in announce = %d, want 1 (member fired org rule, templated)", n)
	}
	if n := count(announce.ChannelID, "deploy: v3"); n != 1 {
		t.Fatalf("deploy v3 in announce = %d, want 1 (org rule fires from any channel)", n)
	}
	if n := count(boot.ChannelID, "note: hi"); n != 1 {
		t.Fatalf("note hi in general = %d, want 1 (channel-scope from its own channel)", n)
	}
	if n := count(boot.ChannelID, "note: nope") + count(announce.ChannelID, "note: nope"); n != 0 {
		t.Fatalf("note nope posted %d times, want 0 (channel-scope must not fire from another channel)", n)
	}

	// A non-member invoking against a PRIVATE channel is denied by the send
	// gate — now the oracle-free 404 (P-34: the slash rides RequireChannelSend →
	// requireMember, so a stranger gets the SAME masked denial a send would; a
	// public channel would still 403). RED/GREEN pin: dropping requireMember
	// makes this a 202.
	if code := slash(bobTok, "deploy", secret.ChannelID, "sneaky"); code != http.StatusNotFound {
		t.Fatalf("non-member slash against a private channel = %d, want 404", code)
	}
}
