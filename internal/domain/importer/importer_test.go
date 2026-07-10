package importer

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
)

// The fixture is a miniature but structurally faithful Zulip export:
// two humans + one bot; #general (collides with the bootstrap channel) and a
// private #core-team (deactivated=false, invite_only); two topics; a DM that
// must be skipped-with-count; a message with edit_history; an @**mention**;
// a reaction; everything dated 2019 to prove backdating.
const fixtureRealm = `{
  "zerver_userprofile": [
    {"id": 11, "delivery_email": "iago@zulip.test", "full_name": "Iago", "is_active": true, "is_bot": false, "role": 200, "date_joined": 1546300800},
    {"id": 12, "delivery_email": "hamlet@zulip.test", "full_name": "Hamlet", "is_active": true, "is_bot": false, "role": 400, "date_joined": 1546300800},
    {"id": 13, "delivery_email": "welcome-bot@zulip.test", "full_name": "Welcome Bot", "is_active": true, "is_bot": true, "role": 400, "date_joined": 1546300800}
  ],
  "zerver_stream": [
    {"id": 21, "name": "general", "description": "imported general", "invite_only": false, "deactivated": false, "date_created": 1546300800},
    {"id": 22, "name": "core-team", "description": "private", "invite_only": true, "deactivated": false, "date_created": 1546300800}
  ],
  "zerver_recipient": [
    {"id": 31, "type": 2, "type_id": 21},
    {"id": 32, "type": 2, "type_id": 22},
    {"id": 33, "type": 1, "type_id": 11}
  ],
  "zerver_subscription": [
    {"id": 41, "user_profile": 11, "recipient": 31, "active": true},
    {"id": 42, "user_profile": 12, "recipient": 31, "active": true},
    {"id": 43, "user_profile": 11, "recipient": 32, "active": true},
    {"id": 44, "user_profile": 12, "recipient": 32, "active": false}
  ]
}`

const fixtureMessages = `{
  "zerver_message": [
    {"id": 101, "sender": 11, "recipient": 31, "subject": "launch plan", "content": "kickoff for **v1**", "date_sent": 1554100000, "edit_history": null},
    {"id": 102, "sender": 12, "recipient": 31, "subject": "launch plan", "content": "ack @**Iago** :rocket:", "date_sent": 1554100600, "edit_history": null},
    {"id": 103, "sender": 12, "recipient": 31, "subject": "random", "content": "edited once", "date_sent": 1554200000, "edit_history": "[{\"prev_content\":\"original\"}]"},
    {"id": 104, "sender": 11, "recipient": 32, "subject": "secrets", "content": "private planning", "date_sent": 1554300000, "edit_history": null},
    {"id": 105, "sender": 11, "recipient": 33, "subject": "", "content": "a DM that must be skipped", "date_sent": 1554400000, "edit_history": null}
  ],
  "zerver_reaction": [
    {"id": 201, "user_profile": 11, "message": 102, "emoji_name": "tada"}
  ]
}`

func writeFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "realm.json"), []byte(fixtureRealm), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "messages-000001.json"), []byte(fixtureMessages), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func testPool(t *testing.T) (*pgxpool.Pool, int64) {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping integration test")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(ctx, `DROP SCHEMA public CASCADE; CREATE SCHEMA public`); err != nil {
		t.Fatalf("reset: %v", err)
	}
	files, _ := filepath.Glob("../../../migrations/0*.sql")
	sort.Strings(files)
	for _, f := range files {
		sqlBytes, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, string(sqlBytes)); err != nil {
			t.Fatalf("apply %s: %v", f, err)
		}
	}
	// A live org with the standard bootstrap shape (#general exists → the
	// import must rename its incoming "general").
	idsvc := identity.New(pool, perms.New(pool))
	res, err := idsvc.Bootstrap(ctx, identity.BootstrapParams{
		OrgSlug: "acme", Email: "owner@acme.test", Password: "password123",
		FullName: "Acme Owner",
	})
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	return pool, res.OrgID
}

func TestZulipImportShowcase(t *testing.T) {
	pool, orgID := testPool(t)
	ctx := context.Background()
	dir := writeFixture(t)
	svc := New(pool)

	// Dry run first: full accounting, zero writes.
	dry, err := svc.Run(ctx, orgID, dir, true)
	if err != nil {
		t.Fatalf("dry run: %v", err)
	}
	if dry.Users != 2 || dry.BotsSkipped != 1 || dry.Channels != 2 ||
		dry.Threads != 3 || dry.Messages != 4 || dry.DMMessagesSkipped != 1 ||
		dry.EditHistoryDropped != 1 || dry.Reactions != 1 {
		t.Fatalf("dry-run report off: %+v", dry)
	}
	var n int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM message WHERE origin_system = 'zulip'`).Scan(&n)
	if n != 0 {
		t.Fatalf("dry run wrote %d messages", n)
	}

	// Real import.
	rep, err := svc.Run(ctx, orgID, dir, false)
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	if rep.Users != 2 || rep.Channels != 2 || rep.Threads != 3 ||
		rep.Messages != 4 || rep.Reactions != 1 || rep.Subscriptions != 3 {
		t.Fatalf("report off: %+v", rep)
	}
	if got := rep.RenamedChannels["general"]; got != "general-zulip1" {
		t.Fatalf("collision rename = %q, want general-zulip1 (visible transform)", got)
	}

	// Topic → titled thread, with backdated message timestamps (E3).
	var title string
	var createdAt time.Time
	err = pool.QueryRow(ctx, `
		SELECT t.title, m.created_at
		FROM message m JOIN thread t ON t.id = m.thread_id
		WHERE m.org_id = $1 AND m.origin_system = 'zulip' AND m.origin_id = '101'`,
		orgID).Scan(&title, &createdAt)
	if err != nil {
		t.Fatalf("imported message: %v", err)
	}
	if title != "launch plan" {
		t.Fatalf("thread title = %q, want the Zulip topic", title)
	}
	if createdAt.Year() != 2019 {
		t.Fatalf("created_at = %v, want backdated 2019", createdAt)
	}

	// Mention re-resolved against the imported directory; render is ours.
	var rendered string
	var ast []byte
	_ = pool.QueryRow(ctx, `
		SELECT rendered, ast FROM message
		WHERE org_id = $1 AND origin_system = 'zulip' AND origin_id = '102'`,
		orgID).Scan(&rendered, &ast)
	var iagoID int64
	_ = pool.QueryRow(ctx, `
		SELECT id FROM user_account WHERE org_id = $1
		 AND origin_system = 'zulip' AND origin_id = '11'`, orgID).Scan(&iagoID)
	if iagoID == 0 || !containsMention(ast, iagoID) {
		t.Fatalf("mention not re-resolved to imported Iago (%d): %s", iagoID, ast)
	}
	if !strings.Contains(rendered, "🚀") {
		t.Fatalf("emoji not rendered by our engine: %s", rendered)
	}

	// Backdated event-log entries with importer attribution (occurred_at 2019,
	// recorded_at now — the E3 keystone).
	var occurred, recorded time.Time
	err = pool.QueryRow(ctx, `
		SELECT occurred_at, recorded_at FROM event_log
		WHERE org_id = $1 AND verb = 'message.created' AND actor_kind = 4
		ORDER BY id LIMIT 1`, orgID).Scan(&occurred, &recorded)
	if err != nil {
		t.Fatalf("importer events: %v", err)
	}
	if occurred.Year() != 2019 || recorded.Year() < 2026 {
		t.Fatalf("E3 violated: occurred=%v recorded=%v", occurred, recorded)
	}

	// Placeholders are claimable deactivated accounts of the placeholder kind.
	var kind int16
	var deact *time.Time
	_ = pool.QueryRow(ctx, `
		SELECT kind, deactivated_at FROM user_account
		WHERE org_id = $1 AND origin_system = 'zulip' AND origin_id = '11'`,
		orgID).Scan(&kind, &deact)
	if kind != 3 {
		t.Fatalf("imported user kind = %d, want 3 (imported_placeholder)", kind)
	}

	// Idempotency (D5): a re-run imports nothing new and duplicates nothing.
	rep2, err := svc.Run(ctx, orgID, dir, false)
	if err != nil {
		t.Fatalf("re-run: %v", err)
	}
	if rep2.Messages != 0 || rep2.Users != 0 || rep2.Channels != 0 || rep2.Threads != 0 {
		t.Fatalf("re-run imported new rows: %+v", rep2)
	}
	if rep2.AlreadyImported == 0 {
		t.Fatalf("re-run should count AlreadyImported, got %+v", rep2)
	}
	var msgs int
	_ = pool.QueryRow(ctx,
		`SELECT count(*) FROM message WHERE org_id = $1 AND origin_system = 'zulip'`,
		orgID).Scan(&msgs)
	if msgs != 4 {
		t.Fatalf("after re-run message count = %d, want 4 (no duplicates)", msgs)
	}
}

func containsMention(ast []byte, userID int64) bool {
	var doc map[string]any
	if json.Unmarshal(ast, &doc) != nil {
		return false
	}
	found := false
	var walk func(n map[string]any)
	walk = func(n map[string]any) {
		if n["type"] == "mention" {
			if attrs, ok := n["attrs"].(map[string]any); ok {
				if id, ok := attrs["user_id"].(float64); ok && int64(id) == userID {
					found = true
				}
			}
		}
		if kids, ok := n["content"].([]any); ok {
			for _, k := range kids {
				if m, ok := k.(map[string]any); ok {
					walk(m)
				}
			}
		}
	}
	walk(doc)
	return found
}
