package importer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
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
// two humans + one bot; #general (collides with the bootstrap channel) and a
// private #core-team (deactivated=false, invite_only); two topics; a legacy-personal self-DM and a
// 1:1 (both imported), a bot-tainted huddle (skipped whole, counted); a message with edit_history; an @**mention**;
// a reaction; everything dated 2019 to prove backdating. Groups: four system
// role groups (administrators/members map, fullmembers coarsens to members,
// nobody is unmappable) + custom "engineering" with a bot member (skipped)
// and a members⊇engineering nesting edge; the members⊇fullmembers edge must
// collapse to a dropped self-edge.
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
    {"id": 33, "type": 1, "type_id": 11},
    {"id": 34, "type": 3, "type_id": 77},
    {"id": 35, "type": 1, "type_id": 12}
  ],
  "zerver_subscription": [
    {"id": 41, "user_profile": 11, "recipient": 31, "active": true},
    {"id": 42, "user_profile": 12, "recipient": 31, "active": true},
    {"id": 43, "user_profile": 11, "recipient": 32, "active": true},
    {"id": 44, "user_profile": 12, "recipient": 32, "active": false},
    {"id": 45, "user_profile": 11, "recipient": 34, "active": true},
    {"id": 46, "user_profile": 12, "recipient": 34, "active": true},
    {"id": 47, "user_profile": 13, "recipient": 34, "active": true}
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

const fixtureMessages = `{
  "zerver_message": [
    {"id": 101, "sender": 11, "recipient": 31, "subject": "launch plan", "content": "kickoff for **v1** [notes](/user_uploads/2/ab/test.txt)", "date_sent": 1554100000, "edit_history": null},
    {"id": 102, "sender": 12, "recipient": 31, "subject": "launch plan", "content": "ack @**Iago** :rocket:", "date_sent": 1554100600, "edit_history": null},
    {"id": 103, "sender": 12, "recipient": 31, "subject": "random", "content": "edited once", "date_sent": 1554200000, "edit_history": "[{\"prev_content\":\"original\",\"user_id\":12,\"timestamp\":1554210000},{\"prev_content\":\"most original\",\"timestamp\":1554205000}]"},
    {"id": 104, "sender": 11, "recipient": 32, "subject": "secrets", "content": "private planning", "date_sent": 1554300000, "edit_history": null},
    {"id": 105, "sender": 11, "recipient": 33, "subject": "", "content": "a note to self", "date_sent": 1554400000, "edit_history": null},
    {"id": 106, "sender": 12, "recipient": 34, "subject": "", "content": "huddle with the bot — must be skipped whole", "date_sent": 1554500000, "edit_history": null},
    {"id": 107, "sender": 11, "recipient": 35, "subject": "", "content": "ping me when the **beta** branch is cut", "date_sent": 1554600000, "edit_history": null}
  ],
  "zerver_reaction": [
    {"id": 201, "user_profile": 11, "message": 102, "emoji_name": "tada"}
  ],
  "zerver_usermessage": [
    {"id": 301, "user_profile": 11, "flags_mask": 1, "message": 101},
    {"id": 302, "user_profile": 11, "flags_mask": 1, "message": 102},
    {"id": 303, "user_profile": 12, "flags_mask": 1, "message": 102},
    {"id": 304, "user_profile": 12, "flags_mask": 0, "message": 101},
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
	if err := os.WriteFile(filepath.Join(dir, "messages-000001.json"), []byte(fixtureMessages), 0o644); err != nil {
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
	store, err := blob.Open("fs", t.TempDir())
	if err != nil {
		t.Fatalf("blob: %v", err)
	}
	svc := New(pool, store)

	// Dry run first: full accounting, zero writes.
	dry, err := svc.Run(ctx, orgID, dir, true)
	if err != nil {
		t.Fatalf("dry run: %v", err)
	}
	if dry.Users != 2 || dry.BotsSkipped != 1 || dry.Channels != 2 ||
		dry.Threads != 3 || dry.Messages != 4 || dry.Reactions != 1 {
		t.Fatalf("dry-run report off: %+v", dry)
	}
	// Edits: hamlet's attributed entry imports; the null-editor entry is a
	// counted skip. The attachment's bytes are present on disk.
	if dry.MessageEdits != 1 || dry.EditEntriesSkipped != 1 ||
		dry.Attachments != 1 || dry.AttachmentFilesMissing != 0 {
		t.Fatalf("dry-run edit/attachment accounting off: %+v", dry)
	}
	// DMs: the self-DM and the 1:1 import (2 conversations, 2 messages);
	// the bot-tainted huddle is skipped WHOLE — dropping the bot would
	// shrink the canonical key onto the humans' real 1:1.
	if dry.DMConversations != 2 || dry.DMMessages != 2 || dry.DMMessagesSkipped != 1 {
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
	if rep.DMConversations != 2 || rep.DMMessages != 2 || rep.DMMessagesSkipped != 1 {
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
	// The self-DM imported as kind 3; the bot huddle imported NOTHING.
	var selfCount, spaces int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM dm_space WHERE org_id = $1 AND kind = 3`, orgID).Scan(&selfCount)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM dm_space WHERE org_id = $1`, orgID).Scan(&spaces)
	if selfCount != 1 || spaces != 2 {
		t.Fatalf("dm spaces = %d (self %d), want 2 total (huddle skipped)", spaces, selfCount)
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
	if msgs != 6 {
		t.Fatalf("after re-run message count = %d, want 6 (4 stream + 2 dm, no duplicates)", msgs)
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
