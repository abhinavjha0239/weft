package rest

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/domain/automation"
	"github.com/abhinavjha0239/weft/internal/domain/content"
	"github.com/abhinavjha0239/weft/internal/domain/dm"
	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/notification"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/domain/unfurl"
	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/internal/eventlog"
	"github.com/abhinavjha0239/weft/internal/gateway"
	"github.com/abhinavjha0239/weft/internal/platform/egress"
)

// importMessage writes a message the way the IMPORTER writes one, followed by
// a message.created event stamped enum.ActorImporter. It is deliberately not
// a REST send: the whole point is an event whose ONLY difference from a live
// send is its actor kind, so a pin on it cannot pass for some incidental
// reason.
//
// The COLUMN LIST below is copied verbatim from the importer's channel
// message lane (source/ast/rendered/has_link plus origin_system/origin_id)
// and is what makes the resulting event indistinguishable from a real
// backfill; P-27a's IR extraction left it byte for byte unchanged, and so did
// P-27b's Slack loader.
//
// The CONTAINER is chosen here, not copied: this lands in the channel's kind=2
// root thread for test convenience. RE-READ AT P-27b, because half of what
// this note used to say stopped being true — the importer DOES land channel
// messages on the root now, whenever a loader names the flat feed with
// Thread.Root (Slack's unthreaded messages, which are most of them). The half
// that still holds is the one worth keeping: a channel message with NO thread
// is a hard error, never a silent root landing. Nothing here bumps a counter,
// so F-15 holds either way.
func importMessage(t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	orgID, channelID int64, authorID int64, originID, src string) int64 {
	t.Helper()
	var threadID int64
	if err := pool.QueryRow(ctx,
		`SELECT root_thread_id FROM channel WHERE id = $1`, channelID).Scan(&threadID); err != nil {
		t.Fatalf("root thread: %v", err)
	}
	doc := content.Parse(src, func(string) (int64, bool) { return 0, false })
	var msgID int64
	err := pool.QueryRow(ctx, `
		INSERT INTO message (org_id, thread_id, channel_id, author_id,
			source, ast, rendered, render_version, has_link, has_attachment,
			created_at, origin_system, origin_id)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,false,$10,'importtest',$11)
		RETURNING id`,
		orgID, threadID, channelID, authorID, src, doc.JSON(),
		content.RenderHTML(doc), content.RenderVersion, doc.HasLink(),
		time.Now().UTC().Add(-90*24*time.Hour), originID).Scan(&msgID)
	if err != nil {
		t.Fatalf("insert imported message: %v", err)
	}
	if err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		_, err := eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: orgID, ActorKind: enum.ActorImporter,
			EntityType: enum.EntityMessage, EntityID: msgID,
			Verb: "message.created",
			Payload: eventlog.MustPayload(map[string]any{
				"message_id": msgID, "thread_id": threadID, "channel_id": channelID,
			}),
		})
		return err
	}); err != nil {
		t.Fatalf("append importer event: %v", err)
	}
	return msgID
}

// TestImportedMessagesDoNotUnfurl pins half of the backfill invariant
// CLAUDE.md states and the code did not honour: "importer=4 (backfills NEVER
// notify)". unfurl.Runner checked no actor kind at all, so every imported
// message carrying a link cost ONE OUTBOUND FETCH to a third party — a
// privacy and load problem on top of the correctness one, and worst exactly
// when a cell ingests the most links at once.
//
// The pin is a hit COUNTER on the page server, not a preview-row count: a row
// can be absent for many reasons, but a fetch that never happened is the
// property that actually matters. The live send is the positive anchor —
// without it, "zero fetches" would pass on a runner that fetches nothing at
// all, which is the vacuity the P-27 audit caught elsewhere.
//
// RED: delete the enum.ActorImporter skip in unfurl.Runner.ProcessOrg →
// "imported link was fetched 1 time(s), want 0".
func TestImportedMessagesDoNotUnfurl(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx, cancel := context.WithCancel(context.Background())
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		cancel()
		t.Fatalf("connect: %v", err)
	}
	defer func() { cancel(); pool.Close() }()
	resetAndMigrate(t, ctx, pool)

	var liveHits, importedHits atomic.Int64
	page := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/live":
			liveHits.Add(1)
		case "/imported":
			importedHits.Add(1)
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprint(w, `<html><head><meta property="og:title" content="T"></head></html>`)
	}))
	defer page.Close()

	hub := gateway.NewHub(pool, slog.Default())
	go hub.Run(ctx)
	permsSvc := perms.New(pool)
	unfurlSvc := unfurl.New(pool, egress.New(egress.Options{
		UserAgent: "weftbot-test", AllowLoopbackForTests: true,
	}))
	unfurlSvc.SetPerms(permsSvc)
	runner := unfurl.NewRunner(pool, unfurlSvc, slog.Default())
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity:  identity.New(pool, permsSvc),
		Messaging: messaging.New(pool, permsSvc),
		DM:        dm.New(pool),
		Unfurl:    unfurlSvc,
	}))
	defer ts.Close()

	var boot struct {
		OrgID     int64  `json:"org_id"`
		UserID    int64  `json:"user_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "backfill", "email": "a@backfill.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)

	process := func() {
		t.Helper()
		drainConsumer(t, ctx, pool, "unfurl", boot.OrgID, runner.ProcessOrg)
	}

	// POSITIVE ANCHOR — a live human send with a link MUST still be fetched.
	// This is what stops the negative below passing vacuously.
	var sent struct {
		MessageID int64 `json:"message_id"`
	}
	postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, boot.ChannelID),
		boot.Token, map[string]any{"content": fmt.Sprintf("live [x](%s/live)", page.URL)}, &sent)
	if sent.MessageID == 0 {
		t.Fatal("live send failed")
	}
	process()
	if n := liveHits.Load(); n != 1 {
		t.Fatalf("live link was fetched %d time(s), want 1 — the anchor is broken, "+
			"so the imported assert below would prove nothing", n)
	}

	// THE PIN — the same shape of message, differing ONLY in actor kind.
	imported := importMessage(t, ctx, pool, boot.OrgID, boot.ChannelID, boot.UserID,
		"slack-1", fmt.Sprintf("imported [x](%s/imported)", page.URL))
	process()
	if n := importedHits.Load(); n != 0 {
		t.Fatalf("imported link was fetched %d time(s), want 0: backfills must not "+
			"unfurl (ADR-003 E4) — an import would otherwise fan one outbound "+
			"request per imported link", n)
	}
	var previews int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM message_link_preview WHERE message_id = $1`, imported).
		Scan(&previews); err != nil {
		t.Fatalf("count previews: %v", err)
	}
	if previews != 0 {
		t.Fatalf("imported message has %d preview rows, want 0", previews)
	}

	// The event is still CONSUMED, not stalled: the cursor must have advanced
	// past the imported event, or the skip would be a poison pill that blocks
	// the lane forever.
	var lag int64
	if err := pool.QueryRow(ctx, `
		SELECT COALESCE(MAX(e.id), 0) - COALESCE(
			(SELECT last_id FROM event_consumer_cursor
			  WHERE consumer = 'unfurl' AND org_id = $1), 0)
		FROM event_log e WHERE e.org_id = $1`, boot.OrgID).Scan(&lag); err != nil {
		t.Fatalf("lag: %v", err)
	}
	if lag != 0 {
		t.Fatalf("unfurl cursor is %d behind after skipping an imported event; "+
			"a skip must still consume and ack", lag)
	}
}

// TestImportedMessagesDoNotTriggerAutomations pins the other half of the
// backfill invariant. automation.match special-cased ActorAutomation only and
// triggerMatches compares the VERB alone, so every imported message.created
// fired every enabled rule whose trigger verb matched: an automation_run per
// imported message, http_request deliveries to third parties, and —
// transitively — NOTIFICATIONS, because a post_message step mints an
// ActorAutomation message and those are not backfills.
//
// The reply count is asserted alongside the run count precisely because of
// that transitive path: a run with no reply would understate the damage.
//
// RED: delete the enum.ActorImporter guard at the top of automation.match →
// "automation runs = 2, want 1".
func TestImportedMessagesDoNotTriggerAutomations(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx, cancel := context.WithCancel(context.Background())
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
	runner := automation.NewRunner(pool, msgSvc, permsSvc, notification.New(pool), slog.Default())
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity:    identity.New(pool, permsSvc),
		Messaging:   msgSvc,
		DM:          dm.New(pool),
		Automations: automation.New(pool, permsSvc),
	}))
	defer ts.Close()

	var boot struct {
		OrgID     int64  `json:"org_id"`
		UserID    int64  `json:"user_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "backfillau", "email": "a@backfillau.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)

	process := func() {
		t.Helper()
		drainConsumer(t, ctx, pool, "automations", boot.OrgID, runner.ProcessOrg)
	}
	countRuns := func(id int64) int {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM automation_run WHERE automation_id = $1`, id).Scan(&n); err != nil {
			t.Fatalf("count runs: %v", err)
		}
		return n
	}
	countReplies := func() int {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM message
			WHERE channel_id = $1 AND source = 'thanks!' AND deleted_at IS NULL`,
			boot.ChannelID).Scan(&n); err != nil {
			t.Fatalf("count replies: %v", err)
		}
		return n
	}

	// Drain bootstrap history before the rule exists, so nothing below is a
	// leftover from provisioning.
	process()

	var rule automation.Automation
	postJSON(t, ts.URL+"/api/v1/automations", boot.Token, map[string]any{
		"scope_type": 3, "scope_id": boot.ChannelID, "name": "auto-reply",
		"definition": map[string]any{
			"trigger": map[string]any{"verb": "message.created"},
			"steps":   []any{map[string]any{"kind": "post_message", "content": "thanks!"}},
		}}, &rule)
	if code := patchJSON(t, fmt.Sprintf("%s/api/v1/automations/%d", ts.URL, rule.ID),
		boot.Token, map[string]any{"enabled": true}); code != http.StatusOK {
		t.Fatalf("enable = %d", code)
	}

	// POSITIVE ANCHOR — a live human send MUST still fire the rule. Without
	// it, "no new run" below would pass on a rule that never fires at all.
	var sent struct {
		MessageID int64 `json:"message_id"`
	}
	postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, boot.ChannelID),
		boot.Token, map[string]any{"content": "anyone around?"}, &sent)
	if sent.MessageID == 0 {
		t.Fatal("live send failed")
	}
	process()
	if n := countRuns(rule.ID); n != 1 {
		t.Fatalf("automation runs after the live send = %d, want 1 — the anchor is "+
			"broken, so the imported assert below would prove nothing", n)
	}
	if n := countReplies(); n != 1 {
		t.Fatalf("replies after the live send = %d, want 1 (anchor)", n)
	}

	// THE PIN — same shape, differing ONLY in actor kind.
	importMessage(t, ctx, pool, boot.OrgID, boot.ChannelID, boot.UserID,
		"slack-au-1", "imported history, should trigger nothing")
	process()
	if n := countRuns(rule.ID); n != 1 {
		t.Fatalf("automation runs = %d, want 1: an imported message must not "+
			"trigger a rule (ADR-003 E4) — a backfill would otherwise mint one run "+
			"per imported message", n)
	}
	if n := countReplies(); n != 1 {
		t.Fatalf("replies = %d, want 1: a post_message step on an imported trigger "+
			"mints an ActorAutomation message, which DOES notify — this is how "+
			"\"backfills never notify\" fails transitively", n)
	}

	// The skip must still consume: the cursor advances past the imported event.
	var lag int64
	if err := pool.QueryRow(ctx, `
		SELECT COALESCE(MAX(e.id), 0) - COALESCE(
			(SELECT last_id FROM event_consumer_cursor
			  WHERE consumer = 'automations' AND org_id = $1), 0)
		FROM event_log e WHERE e.org_id = $1`, boot.OrgID).Scan(&lag); err != nil {
		t.Fatalf("lag: %v", err)
	}
	if lag != 0 {
		t.Fatalf("automations cursor is %d behind after skipping an imported event; "+
			"a skip must still consume and ack", lag)
	}
}
