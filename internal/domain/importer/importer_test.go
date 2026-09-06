package importer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/platform/blob"
	"github.com/abhinavjha0239/weft/migrations"
)

// The fixture is a miniature but structurally faithful Zulip export:
// three humans + one bot; #general (collides with the bootstrap channel) and a
// private #core-team (deactivated=false, invite_only); two topics; a legacy-personal self-DM, a
// 1:1 and an ALL-HUMAN three-way huddle (all imported — the huddle is the
// kind=2 group-DM lane), a bot-tainted huddle (skipped whole, counted); a message with edit_history; an @**mention**;
// reactions in both message chunks; everything dated 2019 to prove backdating. Groups: four system
// role groups (administrators/members map, fullmembers coarsens to members,
// nobody is unmappable) + custom "engineering" with a bot member (skipped)
// and a members⊇engineering nesting edge; the members⊇fullmembers edge must
// collapse to a dropped self-edge.
const fixtureRealm = `{
  "zerver_userprofile": [
    {"id": 11, "delivery_email": "iago@zulip.test", "full_name": "Iago", "is_active": true, "is_bot": false, "role": 200, "date_joined": 1546300800},
    {"id": 12, "delivery_email": "hamlet@zulip.test", "full_name": "Hamlet", "is_active": true, "is_bot": false, "role": 400, "date_joined": 1546300800},
    {"id": 13, "delivery_email": "welcome-bot@zulip.test", "full_name": "Welcome Bot", "is_active": true, "is_bot": true, "role": 400, "date_joined": 1546300800},
    {"id": 14, "delivery_email": "portia@zulip.test", "full_name": "Portia", "is_active": true, "is_bot": false, "role": 400, "date_joined": 1546300800}
  ],
  "zerver_stream": [
    {"id": 21, "name": "general", "description": "imported general", "invite_only": false, "deactivated": false, "date_created": 1546300800},
    {"id": 22, "name": "core-team", "description": "private", "invite_only": true, "deactivated": false, "date_created": 1546300800}
  ],
  "zerver_recipient": [
    {"id": 31, "type": 2, "type_id": 21},
    {"id": 32, "type": 2, "type_id": 22},
    {"id": 33, "type": 1, "type_id": 11},
    {"id": 34, "type": 3, "type_id": 77},
    {"id": 35, "type": 1, "type_id": 12},
    {"id": 36, "type": 3, "type_id": 78}
  ],
  "zerver_subscription": [
    {"id": 41, "user_profile": 11, "recipient": 31, "active": true},
    {"id": 42, "user_profile": 12, "recipient": 31, "active": true},
    {"id": 43, "user_profile": 11, "recipient": 32, "active": true},
    {"id": 44, "user_profile": 12, "recipient": 32, "active": false},
    {"id": 45, "user_profile": 11, "recipient": 34, "active": true},
    {"id": 46, "user_profile": 12, "recipient": 34, "active": true},
    {"id": 47, "user_profile": 13, "recipient": 34, "active": true},
    {"id": 48, "user_profile": 12, "recipient": 36, "active": true},
    {"id": 49, "user_profile": 14, "recipient": 36, "active": true},
    {"id": 50, "user_profile": 11, "recipient": 36, "active": true}
  ],
  "zerver_namedusergroup": [
    {"id": 51, "name": "role:administrators", "description": "", "is_system_group": true, "deactivated": false, "date_created": null},
    {"id": 52, "name": "role:members", "description": "", "is_system_group": true, "deactivated": false, "date_created": null},
    {"id": 53, "name": "role:fullmembers", "description": "", "is_system_group": true, "deactivated": false, "date_created": null},
    {"id": 54, "name": "role:nobody", "description": "", "is_system_group": true, "deactivated": false, "date_created": null},
    {"id": 55, "name": "engineering", "description": "Eng team", "is_system_group": false, "deactivated": false, "date_created": 1546310000}
  ],
  "zerver_usergroupmembership": [
    {"id": 61, "user_profile": 11, "user_group": 51},
    {"id": 62, "user_profile": 12, "user_group": 52},
    {"id": 63, "user_profile": 11, "user_group": 55},
    {"id": 64, "user_profile": 12, "user_group": 55},
    {"id": 65, "user_profile": 13, "user_group": 55}
  ],
  "zerver_groupgroupmembership": [
    {"id": 71, "supergroup": 52, "subgroup": 55},
    {"id": 72, "supergroup": 52, "subgroup": 53}
  ],
  "zerver_attachment": [
    {"id": 401, "file_name": "test.txt", "path_id": "2/ab/test.txt", "owner": 11, "size": 15, "content_type": "text/plain", "create_time": 1554000000}
  ],
  "zerver_attachment_messages": [
    {"id": 501, "attachment": 401, "message": 101}
  ]
}`

// A real export chunks its history across messages-NNNNNN.json files. The two
// chunks below are deliberately UNSORTED, within each file and across the pair
// (103,102,106 then 108,101,105,107,104): the loader's merge must pick up all
// three arrays from BOTH files, and its by-id sort must restore source order —
// that ordering is what keeps the event log ≈ the original history, and it is
// what decides which message a topic takes as its root. Each chunk carries a
// reaction and user-message rows, and each references messages living in the
// OTHER chunk, so dropping either file's arrays moves an assertion.
const fixtureMessagesA = `{
  "zerver_message": [
    {"id": 103, "sender": 12, "recipient": 31, "subject": "random", "content": "edited once", "date_sent": 1554200000, "edit_history": "[{\"prev_content\":\"original\",\"user_id\":12,\"timestamp\":1554210000},{\"prev_content\":\"most original\",\"timestamp\":1554205000}]"},
    {"id": 102, "sender": 12, "recipient": 31, "subject": "launch plan", "content": "ack @**Iago** :rocket:", "date_sent": 1554100600, "edit_history": null},
    {"id": 106, "sender": 12, "recipient": 34, "subject": "", "content": "huddle with the bot — must be skipped whole", "date_sent": 1554500000, "edit_history": null}
  ],
  "zerver_reaction": [
    {"id": 202, "user_profile": 12, "message": 101, "emoji_name": "eyes"}
  ],
  "zerver_usermessage": [
    {"id": 301, "user_profile": 11, "flags_mask": 1, "message": 101},
    {"id": 302, "user_profile": 11, "flags_mask": 1, "message": 102},
    {"id": 303, "user_profile": 12, "flags_mask": 1, "message": 102},
    {"id": 304, "user_profile": 12, "flags_mask": 0, "message": 101}
  ]
}`

const fixtureMessagesB = `{
  "zerver_message": [
    {"id": 108, "sender": 11, "recipient": 36, "subject": "", "content": "three-way sync on the **beta** cut", "date_sent": 1554700000, "edit_history": null},
    {"id": 101, "sender": 11, "recipient": 31, "subject": "launch plan", "content": "kickoff for **v1** [notes](/user_uploads/2/ab/test.txt)", "date_sent": 1554100000, "edit_history": null},
    {"id": 105, "sender": 11, "recipient": 33, "subject": "", "content": "a note to self", "date_sent": 1554400000, "edit_history": null},
    {"id": 107, "sender": 11, "recipient": 35, "subject": "", "content": "ping me when the **beta** branch is cut", "date_sent": 1554600000, "edit_history": null},
    {"id": 104, "sender": 11, "recipient": 32, "subject": "secrets", "content": "private planning", "date_sent": 1554300000, "edit_history": null}
  ],
  "zerver_reaction": [
    {"id": 201, "user_profile": 11, "message": 102, "emoji_name": "tada"}
  ],
  "zerver_usermessage": [
    {"id": 305, "user_profile": 11, "flags_mask": 1, "message": 105},
    {"id": 306, "user_profile": 12, "flags_mask": 3, "message": 103},
    {"id": 307, "user_profile": 12, "flags_mask": 1, "message": 107}
  ]
}`

func writeFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "realm.json"), []byte(fixtureRealm), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "messages-000001.json"), []byte(fixtureMessagesA), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "messages-000002.json"), []byte(fixtureMessagesB), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "uploads", "2", "ab"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "uploads", "2", "ab", "test.txt"),
		[]byte("the roadmap doc"), 0o644); err != nil {
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
	if err := db.Migrate(ctx, pool, migrations.FS); err != nil {
		t.Fatalf("migrate: %v", err)
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
	fsStore, err := blob.Open("fs", t.TempDir())
	if err != nil {
		t.Fatalf("blob: %v", err)
	}
	// The dry run must leave the BLOB seam alone too. A Put is a real side
	// effect on the operator's storage that no SQL count can see, and the
	// attachment lane opens and hashes every file before it writes a row.
	store := &countingStore{Store: fsStore}
	svc := New(pool, store)

	// Dry run first: full accounting, zero writes.
	dry, err := svc.Run(ctx, orgID, dir, true)
	if err != nil {
		t.Fatalf("dry run: %v", err)
	}
	if dry.Users != 3 || dry.BotsSkipped != 1 || dry.Channels != 2 ||
		dry.Threads != 3 || dry.Messages != 4 || dry.Reactions != 2 {
		t.Fatalf("dry-run report off: %+v", dry)
	}
	// Edits: hamlet's attributed entry imports; the null-editor entry is a
	// counted skip. The attachment's bytes are present on disk.
	if dry.MessageEdits != 1 || dry.EditEntriesSkipped != 1 ||
		dry.Attachments != 1 || dry.AttachmentFilesMissing != 0 {
		t.Fatalf("dry-run edit/attachment accounting off: %+v", dry)
	}
	// DMs: the self-DM, the 1:1 and the all-human huddle import (3
	// conversations, 3 messages); the bot-tainted huddle is skipped WHOLE —
	// dropping the bot would shrink the canonical key onto the humans' real 1:1.
	if dry.DMConversations != 3 || dry.DMMessages != 3 || dry.DMMessagesSkipped != 1 {
		t.Fatalf("dry-run dm accounting off: %+v", dry)
	}
	// Groups: 1 custom; 3 mappable system groups (nobody has no counterpart);
	// 2 custom memberships (the bot's is skipped); 1 edge (members⊇fullmembers
	// collapses to a self-edge and is dropped).
	if dry.Groups != 1 || dry.SystemGroupsMapped != 3 ||
		dry.GroupMembers != 2 || dry.GroupEdges != 1 {
		t.Fatalf("dry-run group accounting off: %+v", dry)
	}
	// Watermarks: Iago read both launch-plan messages; Hamlet read the NEWER
	// launch-plan message but not the older (coarsened) and read "random"
	// with a starred bit that must be ignored; DM read flags now land too —
	// Iago's self-DM and Hamlet's 1:1 read each add a pair.
	if dry.Watermarks != 5 || dry.ReadCoarsened != 1 {
		t.Fatalf("dry-run watermark accounting off: %+v", dry)
	}
	var n int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM message WHERE origin_system = 'zulip'`).Scan(&n)
	if n != 0 {
		t.Fatalf("dry run wrote %d messages", n)
	}
	// "Writes nothing" means every table the write path touches, not just
	// the one. A single-table check leaves users, channels, threads, groups,
	// files, DM spaces, watermarks and the event log free to be written by a
	// dry run — and the message table is the LAST thing the write path
	// reaches, so a half-executed write would slip past it entirely.
	for _, tbl := range []struct{ name, q string }{
		{"user_account", `SELECT count(*) FROM user_account WHERE org_id = $1 AND origin_system IS NOT NULL`},
		{"channel", `SELECT count(*) FROM channel WHERE org_id = $1 AND origin_system IS NOT NULL`},
		{"thread", `SELECT count(*) FROM thread WHERE org_id = $1 AND origin_system IS NOT NULL`},
		{"user_group", `SELECT count(*) FROM user_group WHERE org_id = $1 AND origin_system IS NOT NULL`},
		{"file", `SELECT count(*) FROM file WHERE org_id = $1 AND origin_system IS NOT NULL`},
		// dm_space carries no provenance columns, so the fixture's own
		// emptiness is the pin: a bootstrapped org has no conversations.
		{"dm_space", `SELECT count(*) FROM dm_space WHERE org_id = $1`},
		{"thread_read_watermark", `SELECT count(*) FROM thread_read_watermark w
			JOIN thread t ON t.id = w.thread_id WHERE t.org_id = $1`},
		{"event_log(importer)", `SELECT count(*) FROM event_log WHERE org_id = $1 AND actor_kind = 4`},
	} {
		var got int
		if err := pool.QueryRow(ctx, tbl.q, orgID).Scan(&got); err != nil {
			t.Fatalf("dry-run %s census: %v", tbl.name, err)
		}
		if got != 0 {
			t.Fatalf("dry run wrote %d %s row(s)", got, tbl.name)
		}
	}
	if puts := store.puts.Load(); puts != 0 {
		t.Fatalf("dry run put %d blob(s) into the store", puts)
	}

	// Real import.
	rep, err := svc.Run(ctx, orgID, dir, false)
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	// S2: the import ENQUEUES its closure rebuild rather than recomputing
	// in-tx; the worker drives it here exactly as the import CLI does.
	if n, err := perms.NewRebuildWorker(pool, perms.New(pool), slog.Default()).RunOnce(ctx); err != nil || n != 1 {
		t.Fatalf("closure rebuild drain = %d jobs (%v), want 1", n, err)
	}
	if rep.Users != 3 || rep.Channels != 2 || rep.Threads != 3 ||
		rep.Messages != 4 || rep.Reactions != 2 || rep.Subscriptions != 3 {
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

	// The WHOLE importer event surface, per verb. The importer feeds six
	// verbs and the backfill contract (consumers key off actor_kind=4) is
	// only worth anything if every one of them is actually appended — so
	// every Append in the module is pinned by an exact count here, and an
	// unexpected seventh verb fails the same comparison.
	wantEvents := map[string]int{
		"channel.created":   2, // both imported streams
		"thread.created":    3, // launch plan, random, secrets (root threads are silent)
		"usergroup.created": 1, // engineering; system groups MAP, never create
		"dm.opened":         3, // self-DM, 1:1, human huddle
		"file.uploaded":     1,
		"message.created":   7, // 4 stream + 3 dm
	}
	gotEvents := map[string]int{}
	evRows, err := pool.Query(ctx, `
		SELECT verb, count(*) FROM event_log
		WHERE org_id = $1 AND actor_kind = 4 GROUP BY verb`, orgID)
	if err != nil {
		t.Fatalf("importer event census: %v", err)
	}
	for evRows.Next() {
		var verb string
		var n int
		if err := evRows.Scan(&verb, &n); err != nil {
			evRows.Close()
			t.Fatalf("scan event census: %v", err)
		}
		gotEvents[verb] = n
	}
	evRows.Close()
	if err := evRows.Err(); err != nil {
		t.Fatalf("importer event census: %v", err)
	}
	if !reflect.DeepEqual(gotEvents, wantEvents) {
		t.Fatalf("importer events = %v, want %v", gotEvents, wantEvents)
	}
	// E3 holds for EVERY importer event, not just the first message: source
	// time in 2019, ingest time now.
	var badStamp int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM event_log
		WHERE org_id = $1 AND actor_kind = 4
		  AND NOT (extract(year from occurred_at) = 2019
		           AND recorded_at > now() - interval '1 hour')`, orgID).Scan(&badStamp); err != nil {
		t.Fatalf("E3 census: %v", err)
	}
	if badStamp != 0 {
		t.Fatalf("%d importer events violate E3 (2019 occurred_at, fresh recorded_at)", badStamp)
	}

	// Chunk merge + by-id sort. The two message files are unsorted on disk
	// (103,102,106 then 108,101,105,107,104), so imported ids must still
	// ascend with SOURCE ids — without the loader's sort the event log would
	// replay history in file order.
	var misordered int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM (
		  SELECT row_number() OVER (ORDER BY e.id)                 AS by_event,
		         row_number() OVER (ORDER BY m.origin_id::bigint)  AS by_source
		  FROM event_log e
		  JOIN message m ON m.id = e.entity_id AND m.org_id = e.org_id
		  WHERE e.org_id = $1 AND e.actor_kind = 4 AND e.verb = 'message.created'
		) x WHERE by_event <> by_source`, orgID).Scan(&misordered); err != nil {
		t.Fatalf("import order check: %v", err)
	}
	if misordered != 0 {
		t.Fatalf("%d imported messages are out of source order (chunk merge/sort)", misordered)
	}
	// The first message of a topic becomes its root: zulip 101 by source id,
	// but zulip 102 if the chunks were replayed in file order.
	var rootIsFirst bool
	if err := pool.QueryRow(ctx, `
		SELECT t.root_message_id = m.id FROM message m
		JOIN thread t ON t.id = m.thread_id
		WHERE m.org_id = $1 AND m.origin_system = 'zulip' AND m.origin_id = '101'`,
		orgID).Scan(&rootIsFirst); err != nil {
		t.Fatalf("root message check: %v", err)
	}
	if !rootIsFirst {
		t.Fatal("launch-plan root message is not zulip 101 (chunk order leaked through)")
	}
	// Both chunks' reaction arrays merged (201 rides chunk B, 202 chunk A).
	var mergedReactions int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM reaction r
		JOIN message m ON m.id = r.message_id
		WHERE m.org_id = $1 AND m.origin_system = 'zulip'
		  AND (m.origin_id, r.emoji) IN (('102', 'tada'), ('101', 'eyes'))`,
		orgID).Scan(&mergedReactions); err != nil {
		t.Fatalf("merged reactions: %v", err)
	}
	if mergedReactions != 2 {
		t.Fatalf("reactions merged across chunks = %d, want 2", mergedReactions)
	}

	// Placeholders are claimable deactivated accounts of the placeholder
	// kind, with the SOURCE role (Zulip 200 = realm admin → Weft 20).
	var kind, role int16
	var deact *time.Time
	_ = pool.QueryRow(ctx, `
		SELECT kind, role, deactivated_at FROM user_account
		WHERE org_id = $1 AND origin_system = 'zulip' AND origin_id = '11'`,
		orgID).Scan(&kind, &role, &deact)
	if kind != 3 || role != 20 {
		t.Fatalf("imported Iago kind=%d role=%d, want kind 3, role 20 (admin)", kind, role)
	}
	// An ACTIVE source user must not arrive pre-deactivated. The column comes
	// from one `CASE WHEN is_active` and the whole fixture is active, so the
	// polarity is invertible with nothing red here; the other half of the pin
	// (a deactivated source user arriving deactivated) lives in
	// TestImportCountsUnmappableChannelMessagesAndReactions, which owns the
	// only inactive fixture user in the suite.
	if deact != nil {
		t.Fatalf("active source user imported deactivated (deactivated_at = %v)", *deact)
	}

	// Channel visibility carries over: Zulip's invite_only is Weft's
	// visibility 2, everything else is 1. Never asserted before, and the
	// import is the one path that can leak a PRIVATE stream into a public
	// channel — the whole read ACL hangs off this column.
	var visGeneral, visCore int16
	if err := pool.QueryRow(ctx, `
		SELECT
		  (SELECT visibility FROM channel WHERE org_id = $1
		    AND origin_system = 'zulip' AND origin_id = '21'),
		  (SELECT visibility FROM channel WHERE org_id = $1
		    AND origin_system = 'zulip' AND origin_id = '22')`,
		orgID).Scan(&visGeneral, &visCore); err != nil {
		t.Fatalf("channel visibility: %v", err)
	}
	if visGeneral != 1 || visCore != 2 {
		t.Fatalf("visibility: general=%d core-team=%d, want 1 (public) and 2 (private)",
			visGeneral, visCore)
	}

	// F-15: a kind=2 ROOT thread is a container, not a conversation — it
	// carries no denormalized counters, and above all no root_message_id,
	// because messaging/move.go rejects a move for ANY message some thread
	// names as its root (it does not filter by kind), so a root_message_id on
	// a root thread makes that message permanently unmovable. Six roots here:
	// the bootstrap channel, the two imported channels, and the three DM
	// spaces. The importer's counter bump is the only writer in the tree that
	// touches all three columns at once.
	var roots, bumpedRoots int
	if err := pool.QueryRow(ctx, `
		SELECT count(*),
		       count(*) FILTER (WHERE message_count <> 0
		                     OR last_activity_at IS NOT NULL
		                     OR root_message_id IS NOT NULL)
		FROM thread WHERE org_id = $1 AND kind = 2`, orgID).Scan(&roots, &bumpedRoots); err != nil {
		t.Fatalf("root thread fence: %v", err)
	}
	if roots != 6 {
		t.Fatalf("kind=2 root threads = %d, want 6 (bootstrap + 2 imported channels + 3 DM spaces)", roots)
	}
	if bumpedRoots != 0 {
		t.Fatalf("%d root thread(s) carry F-15 counters (message_count / last_activity_at / root_message_id)",
			bumpedRoots)
	}

	// Groups: same accounting as the dry run, plus real rows.
	if rep.Groups != 1 || rep.SystemGroupsMapped != 3 ||
		rep.GroupMembers != 2 || rep.GroupEdges != 1 || len(rep.RenamedGroups) != 0 {
		t.Fatalf("group import report off: %+v", rep)
	}
	var engID int64
	var engSystem bool
	if err := pool.QueryRow(ctx, `
		SELECT id, is_system FROM user_group
		WHERE org_id = $1 AND origin_system = 'zulip' AND origin_id = '55'`,
		orgID).Scan(&engID, &engSystem); err != nil {
		t.Fatalf("engineering group not imported: %v", err)
	}
	if engSystem {
		t.Fatal("imported custom group wrongly marked system")
	}
	var engMembers int
	_ = pool.QueryRow(ctx,
		`SELECT count(*) FROM user_group_member WHERE group_id = $1`, engID).Scan(&engMembers)
	if engMembers != 2 {
		t.Fatalf("engineering members = %d, want 2 (bot skipped)", engMembers)
	}
	// Nesting: seeded role:members now CONTAINS engineering, and the closure
	// resolves imported users through BOTH lanes — Hamlet via his member
	// role, Iago via role-chain nesting AND via engineering.
	var nested bool
	_ = pool.QueryRow(ctx, `
		SELECT EXISTS (
		  SELECT 1 FROM user_group_subgroup s
		  JOIN user_group g ON g.id = s.group_id
		  WHERE g.org_id = $1 AND g.name = 'role:members' AND s.subgroup_id = $2)`,
		orgID, engID).Scan(&nested)
	if !nested {
		t.Fatal("members ⊇ engineering edge not imported")
	}
	var iagoInClosure, hamletInClosure bool
	var hamletID int64
	_ = pool.QueryRow(ctx, `
		SELECT id FROM user_account WHERE org_id = $1
		 AND origin_system = 'zulip' AND origin_id = '12'`, orgID).Scan(&hamletID)
	_ = pool.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM user_group_closure gc
		  JOIN user_group g ON g.id = gc.group_id
		  WHERE g.org_id = $1 AND g.name = 'role:admins' AND gc.user_id = $2)`,
		orgID, iagoID).Scan(&iagoInClosure)
	_ = pool.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM user_group_closure gc
		  WHERE gc.group_id = $1 AND gc.user_id = $2)`,
		engID, hamletID).Scan(&hamletInClosure)
	if !iagoInClosure || !hamletInClosure {
		t.Fatalf("closure not rebuilt: iago-admin=%v hamlet-engineering=%v",
			iagoInClosure, hamletInClosure)
	}

	// Watermarks match the dry-run accounting, and Hamlet's launch-plan
	// watermark sits on the NEWER read message (zulip 102) — the older
	// unread message below it is the counted coarsening.
	if rep.Watermarks != 5 || rep.ReadCoarsened != 1 {
		t.Fatalf("watermark report off: %+v", rep)
	}
	if rep.DMConversations != 3 || rep.DMMessages != 3 || rep.DMMessagesSkipped != 1 {
		t.Fatalf("dm import report off: %+v", rep)
	}
	// The 1:1 landed in a dm_space whose canonical key matches what the
	// native dm module would compute — imported and native history share
	// one conversation.
	var dmKind int16
	var dmKey string
	var dmThread int64
	lo, hi := iagoID, hamletID
	if hi < lo {
		lo, hi = hi, lo
	}
	wantKey := fmt.Sprintf("%d:%d", lo, hi)
	if err := pool.QueryRow(ctx, `
		SELECT ds.kind, ds.dm_key, t.id
		FROM dm_space ds JOIN thread t ON t.dm_space_id = ds.id AND t.kind = 2
		WHERE ds.org_id = $1 AND ds.dm_key = $2`,
		orgID, wantKey).Scan(&dmKind, &dmKey, &dmThread); err != nil {
		t.Fatalf("imported 1:1 dm_space: %v", err)
	}
	if dmKind != 1 {
		t.Fatalf("1:1 dm kind = %d, want 1", dmKind)
	}
	var dmMsgCount int
	var weft107 int64
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM message WHERE thread_id = $1`, dmThread).Scan(&dmMsgCount)
	_ = pool.QueryRow(ctx, `
		SELECT id FROM message WHERE org_id = $1
		 AND origin_system = 'zulip' AND origin_id = '107'`, orgID).Scan(&weft107)
	if dmMsgCount != 1 || weft107 == 0 {
		t.Fatalf("1:1 dm thread has %d messages (weft107=%d), want the imported one", dmMsgCount, weft107)
	}

	// The kind=2 GROUP-DM lane: an all-human huddle keeps every participant,
	// so its canonical key is the three sorted ids — the same derivation the
	// native dm module uses, one id wider than the 1:1 above.
	var portiaID int64
	if err := pool.QueryRow(ctx, `
		SELECT id FROM user_account WHERE org_id = $1
		 AND origin_system = 'zulip' AND origin_id = '14'`, orgID).Scan(&portiaID); err != nil {
		t.Fatalf("imported Portia: %v", err)
	}
	trio := []int64{iagoID, hamletID, portiaID}
	sort.Slice(trio, func(i, j int) bool { return trio[i] < trio[j] })
	wantGroupKey := fmt.Sprintf("%d:%d:%d", trio[0], trio[1], trio[2])
	var groupSpace, groupThread int64
	var groupKind int16
	if err := pool.QueryRow(ctx, `
		SELECT ds.id, ds.kind, t.id
		FROM dm_space ds JOIN thread t ON t.dm_space_id = ds.id AND t.kind = 2
		WHERE ds.org_id = $1 AND ds.dm_key = $2`,
		orgID, wantGroupKey).Scan(&groupSpace, &groupKind, &groupThread); err != nil {
		t.Fatalf("imported huddle dm_space (key %s): %v", wantGroupKey, err)
	}
	if groupKind != 2 {
		t.Fatalf("huddle dm kind = %d, want 2 (group)", groupKind)
	}
	var groupParts int
	_ = pool.QueryRow(ctx,
		`SELECT count(*) FROM dm_participant WHERE dm_space_id = $1`, groupSpace).Scan(&groupParts)
	if groupParts != 3 {
		t.Fatalf("huddle participants = %d, want 3", groupParts)
	}
	// The huddle message landed in that space's root thread, attributed to
	// its imported sender.
	var msg108Thread, msg108Space, msg108Author int64
	if err := pool.QueryRow(ctx, `
		SELECT thread_id, dm_space_id, author_id FROM message
		WHERE org_id = $1 AND origin_system = 'zulip' AND origin_id = '108'`,
		orgID).Scan(&msg108Thread, &msg108Space, &msg108Author); err != nil {
		t.Fatalf("imported huddle message: %v", err)
	}
	if msg108Thread != groupThread || msg108Space != groupSpace || msg108Author != iagoID {
		t.Fatalf("huddle message thread=%d space=%d author=%d, want %d/%d/%d",
			msg108Thread, msg108Space, msg108Author, groupThread, groupSpace, iagoID)
	}
	// dm.opened names all three participants (the payload IS the wire
	// contract the gateway routes on).
	var openedParts int
	if err := pool.QueryRow(ctx, `
		SELECT jsonb_array_length(payload->'user_ids') FROM event_log
		WHERE org_id = $1 AND actor_kind = 4 AND verb = 'dm.opened' AND entity_id = $2`,
		orgID, groupSpace).Scan(&openedParts); err != nil {
		t.Fatalf("huddle dm.opened event: %v", err)
	}
	if openedParts != 3 {
		t.Fatalf("huddle dm.opened user_ids = %d, want 3", openedParts)
	}
	// Hamlet's read flag became a watermark on the DM thread.
	var dmWM int64
	if err := pool.QueryRow(ctx, `
		SELECT last_read_message_id FROM thread_read_watermark
		WHERE user_id = $1 AND thread_id = $2`, hamletID, dmThread).Scan(&dmWM); err != nil {
		t.Fatalf("hamlet dm watermark: %v", err)
	}
	if dmWM != weft107 {
		t.Fatalf("hamlet dm watermark = %d, want %d", dmWM, weft107)
	}
	// S6: the O(1) unread counters were seeded from the imported read state
	// in the import tx (imported events are importer-actor, so the consumer
	// never counts them — without the seed, real imported unread state would
	// be invisible to the counter read). Assert three ways so the check can
	// go red: every counter row equals the live aggregate, at least one row
	// is nonzero (Hamlet's coarsened-unread launch-plan message guarantees
	// one), and no (user, container) with live unread is missing its row.
	var seedMismatch, seedNonzero int
	if err := pool.QueryRow(ctx, `
		SELECT
		  count(*) FILTER (WHERE c.unread_count <> live.n),
		  count(*) FILTER (WHERE c.unread_count > 0)
		FROM container_unread_counter c
		JOIN LATERAL (
		  SELECT count(*) AS n
		  FROM message m
		  LEFT JOIN thread_read_watermark w
		    ON w.user_id = c.user_id AND w.thread_id = m.thread_id
		  WHERE m.deleted_at IS NULL AND m.author_id <> c.user_id
		    AND m.id > COALESCE(w.last_read_message_id, 0)
		    AND ((c.channel_id IS NOT NULL AND m.channel_id = c.channel_id)
		      OR (c.dm_space_id IS NOT NULL AND m.dm_space_id = c.dm_space_id))
		) live ON true
		WHERE c.org_id = $1`, orgID).Scan(&seedMismatch, &seedNonzero); err != nil {
		t.Fatalf("unread counter seed check: %v", err)
	}
	if seedMismatch != 0 || seedNonzero == 0 {
		t.Fatalf("unread counters not seeded from imported read state: mismatched=%d nonzero=%d",
			seedMismatch, seedNonzero)
	}
	var seedMissing int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM (
		  SELECT cm.user_id, cm.channel_id
		  FROM channel_member cm
		  JOIN channel ch ON ch.id = cm.channel_id AND ch.org_id = $1
		  JOIN message m ON m.channel_id = cm.channel_id
		   AND m.author_id <> cm.user_id AND m.deleted_at IS NULL
		  LEFT JOIN thread_read_watermark w
		    ON w.user_id = cm.user_id AND w.thread_id = m.thread_id
		  WHERE cm.unsubscribed_at IS NULL
		    AND m.id > COALESCE(w.last_read_message_id, 0)
		  GROUP BY cm.user_id, cm.channel_id
		) x
		LEFT JOIN container_unread_counter c
		  ON c.user_id = x.user_id AND c.channel_id = x.channel_id
		WHERE c.user_id IS NULL`, orgID).Scan(&seedMissing); err != nil {
		t.Fatalf("unread counter missing-row check: %v", err)
	}
	if seedMissing != 0 {
		t.Fatalf("%d imported (user, channel) unread states have no counter row", seedMissing)
	}
	// The self-DM imported as kind 3; the BOT huddle imported NOTHING (the
	// human one above is the third space).
	var selfCount, spaces int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM dm_space WHERE org_id = $1 AND kind = 3`, orgID).Scan(&selfCount)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM dm_space WHERE org_id = $1`, orgID).Scan(&spaces)
	if selfCount != 1 || spaces != 3 {
		t.Fatalf("dm spaces = %d (self %d), want 3 total (bot huddle skipped)", spaces, selfCount)
	}
	var hamletWM, weft102 int64
	_ = pool.QueryRow(ctx, `
		SELECT id FROM message WHERE org_id = $1
		 AND origin_system = 'zulip' AND origin_id = '102'`, orgID).Scan(&weft102)
	if err := pool.QueryRow(ctx, `
		SELECT w.last_read_message_id
		FROM thread_read_watermark w
		JOIN message m ON m.id = $2
		WHERE w.user_id = $1 AND w.thread_id = m.thread_id`,
		hamletID, weft102).Scan(&hamletWM); err != nil {
		t.Fatalf("hamlet watermark: %v", err)
	}
	if hamletWM != weft102 {
		t.Fatalf("hamlet watermark = %d, want %d (weft id of zulip 102)", hamletWM, weft102)
	}

	// Attachment lane: file row with provenance, blob readable through the
	// store, message content rewritten to the managed URL, reference + flag.
	if rep.Attachments != 1 || rep.AttachmentFilesMissing != 0 {
		t.Fatalf("attachment report off: %+v", rep)
	}
	var fileID int64
	var storageKey string
	if err := pool.QueryRow(ctx, `
		SELECT id, storage_key FROM file
		WHERE org_id = $1 AND origin_system = 'zulip' AND origin_id = '401'`,
		orgID).Scan(&fileID, &storageKey); err != nil {
		t.Fatalf("imported file: %v", err)
	}
	rc, err := store.Open(ctx, storageKey)
	if err != nil {
		t.Fatalf("blob open: %v", err)
	}
	blobBytes, _ := io.ReadAll(rc)
	rc.Close()
	if string(blobBytes) != "the roadmap doc" {
		t.Fatalf("blob content = %q", blobBytes)
	}
	var src101 string
	var attach101 bool
	_ = pool.QueryRow(ctx, `
		SELECT source, has_attachment FROM message
		WHERE org_id = $1 AND origin_system = 'zulip' AND origin_id = '101'`,
		orgID).Scan(&src101, &attach101)
	if !strings.Contains(src101, fmt.Sprintf("/api/v1/files/%d", fileID)) || !attach101 {
		t.Fatalf("rewrite/flag wrong: attach=%v src=%q", attach101, src101)
	}
	var refCount int
	_ = pool.QueryRow(ctx,
		`SELECT count(*) FROM file_reference WHERE file_id = $1`, fileID).Scan(&refCount)
	if refCount != 1 {
		t.Fatalf("file references = %d, want 1", refCount)
	}

	// Edit-history lane: one attributed kind-1 revision on message 103 with
	// backdated edited_at; the null-editor entry was skipped.
	if rep.MessageEdits != 1 || rep.EditEntriesSkipped != 1 {
		t.Fatalf("edit report off: %+v", rep)
	}
	var prevSrc string
	var editedBy int64
	var editedAt, msgEditedAt time.Time
	if err := pool.QueryRow(ctx, `
		SELECT r.prev_source, r.edited_by, r.edited_at, m.edited_at
		FROM message m JOIN message_revision r ON r.message_id = m.id AND r.kind = 1
		WHERE m.org_id = $1 AND m.origin_system = 'zulip' AND m.origin_id = '103'`,
		orgID).Scan(&prevSrc, &editedBy, &editedAt, &msgEditedAt); err != nil {
		t.Fatalf("imported revision: %v", err)
	}
	if prevSrc != "original" || editedBy != hamletID ||
		editedAt.Year() != 2019 || msgEditedAt.Year() != 2019 {
		t.Fatalf("revision wrong: prev=%q by=%d at=%v msg=%v", prevSrc, editedBy, editedAt, msgEditedAt)
	}

	// The REPORT IS THE OPERATOR'S ARTIFACT: `weftd import-zulip` prints
	// exactly this, MarshalIndent'd, and nothing else. Every bucket name and
	// every JSON key is therefore a published contract, and until now not one
	// of them was pinned — the struct tags, the `imported` sub-map built by
	// finalize(), the omitempty on the rename maps and the field order could
	// all change with the suite green. Every number below is derived from the
	// assertions above, not read off a run: 3 users (4 source, 1 bot) · 2
	// channels · 3 topics · 4 channel + 3 DM messages · 2 reactions · 3 of 10
	// subscription rows (active, stream-recipient, mapped both ends) · 1
	// custom group with 2 human members and 1 surviving edge · 5 watermarks ·
	// 3 conversations · 1 attachment · 1 attributed edit; losses: 1 bot, 1
	// bot-tainted DM, 1 null-editor edit, 1 coarsened unread; 3 system groups
	// mapped; the #general collision renamed; nothing pre-existing.
	//
	// P-27a changed exactly two things here and the golden is the proof:
	// `source` was added (a deployment with two adapters cannot otherwise
	// tell whose numbers these are) and Zulip's word "stream" left the
	// skipped-messages key. Every other byte survived the IR extraction.
	const wantJSON = `{
  "source": "zulip",
  "dry_run": false,
  "imported": {
    "attachments": 1,
    "channels": 2,
    "dm_conversations": 3,
    "dm_messages": 3,
    "group_edges": 1,
    "group_members": 2,
    "groups": 1,
    "message_edits": 1,
    "messages": 4,
    "reactions": 2,
    "read_watermarks": 5,
    "subscriptions": 3,
    "threads": 3,
    "users": 3
  },
  "bots_skipped": 1,
  "dm_messages_skipped_unmappable_participants": 1,
  "channel_messages_skipped_unmapped": 0,
  "edit_entries_skipped_unattributable": 1,
  "attachment_files_missing": 0,
  "reactions_unmapped": 0,
  "role_grants_skipped_existing_users": 0,
  "matched_existing_by_email": 0,
  "unread_below_watermark_coarsened": 1,
  "already_imported": 0,
  "renamed_channels": {
    "general": "general-zulip1"
  },
  "system_groups_mapped": 3
}`
	gotJSON, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		t.Fatalf("marshal report: %v", err)
	}
	if string(gotJSON) != wantJSON {
		t.Fatalf("import report JSON drifted.\n--- got ---\n%s\n--- want ---\n%s", gotJSON, wantJSON)
	}

	// Idempotency (D5): a re-run imports nothing new and duplicates nothing.
	rep2, err := svc.Run(ctx, orgID, dir, false)
	if err != nil {
		t.Fatalf("re-run: %v", err)
	}
	// The first job settled, so the re-run minted a FRESH one (no coalescing
	// with done rows) — drain it and the rebuild stays a no-op.
	if n, err := perms.NewRebuildWorker(pool, perms.New(pool), slog.Default()).RunOnce(ctx); err != nil || n != 1 {
		t.Fatalf("re-run closure drain = %d jobs (%v), want 1", n, err)
	}
	if rep2.Messages != 0 || rep2.Users != 0 || rep2.Channels != 0 ||
		rep2.Threads != 0 || rep2.Groups != 0 || rep2.GroupMembers != 0 ||
		rep2.GroupEdges != 0 || rep2.Watermarks != 0 ||
		rep2.DMConversations != 0 || rep2.DMMessages != 0 ||
		rep2.Attachments != 0 || rep2.MessageEdits != 0 {
		t.Fatalf("re-run imported new rows: %+v", rep2)
	}
	if rep2.AlreadyImported == 0 {
		t.Fatalf("re-run should count AlreadyImported, got %+v", rep2)
	}
	var msgs int
	_ = pool.QueryRow(ctx,
		`SELECT count(*) FROM message WHERE org_id = $1 AND origin_system = 'zulip'`,
		orgID).Scan(&msgs)
	if msgs != 7 {
		t.Fatalf("after re-run message count = %d, want 7 (4 stream + 3 dm, no duplicates)", msgs)
	}
	// The event census AFTER the re-run, which is where the importer's
	// idempotency actually lives: all seven eventlog.Append calls sit inside
	// insert-succeeded branches, and the census above only ever ran on a
	// virgin org, so hoisting ANY of them onto the conflict/resolve path
	// would double the whole importer event surface on every re-import with
	// nothing red. Rows do not duplicate (the check above); events must not
	// either, and they are the durable spine every consumer replays.
	if got := importerEventCensus(t, ctx, pool, orgID); !reflect.DeepEqual(got, wantEvents) {
		t.Fatalf("importer events after re-run = %v, want %v (unchanged)", got, wantEvents)
	}
	// A re-run MATCHES its three users by email instead of creating them, and
	// Iago's admin grant is the one role an import refuses to re-apply to an
	// account that already exists. Both buckets exist precisely so that a
	// merge is never invisible, and both were only ever observed as zero.
	if rep2.MatchedExistingByEmail != 3 || rep2.RoleGrantsSkipped != 1 {
		t.Fatalf("re-run matched=%d role-grants-skipped=%d, want 3/1 (the three humans "+
			"re-matched by email; only Iago's role is above plain member)",
			rep2.MatchedExistingByEmail, rep2.RoleGrantsSkipped)
	}
}

// countingStore counts Puts on the way through to a real store, so "the dry
// run writes nothing" can cover the blob seam and not just SQL.
type countingStore struct {
	blob.Store
	puts atomic.Int64
}

func (c *countingStore) Put(ctx context.Context, key string, r io.Reader) error {
	c.puts.Add(1)
	return c.Store.Put(ctx, key, r)
}

// importerEventCensus is the verb→count map over the importer's actor kind.
func importerEventCensus(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID int64) map[string]int {
	t.Helper()
	rows, err := pool.Query(ctx, `
		SELECT verb, count(*) FROM event_log
		WHERE org_id = $1 AND actor_kind = 4 GROUP BY verb`, orgID)
	if err != nil {
		t.Fatalf("event census: %v", err)
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var verb string
		var n int
		if err := rows.Scan(&verb, &n); err != nil {
			t.Fatalf("scan event census: %v", err)
		}
		out[verb] = n
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("event census: %v", err)
	}
	return out
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
