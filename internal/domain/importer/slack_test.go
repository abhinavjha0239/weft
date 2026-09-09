package importer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/domain/notification"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/internal/eventlog"
	"github.com/abhinavjha0239/weft/internal/platform/blob"
)

// A miniature but structurally faithful Slack export. Everything the loader
// has to decide is present exactly once, so no assertion below can be true by
// accident:
//
//   - users.json: an owner, a plain member, a SINGLE-CHANNEL GUEST (P-5
//     ceiling) and a bot; every user carries an email, none collide with the
//     bootstrap owner.
//   - channels.json holds #general, which COLLIDES with the bootstrap channel;
//     groups.json holds a private one, so both visibilities are exercised.
//   - #general's history covers all three threading shapes in one file — an
//     unthreaded message, a parent (thread_ts == ts), a reply (thread_ts != ts)
//     and a thread_broadcast reply — plus a bot-authored message and a
//     channel_join notice, which are the two counted drops.
//   - mpims.json + dms.json give an all-human group DM, a 1:1, and a
//     bot-tainted 1:1 that must be skipped WHOLE.
//   - files: one Slack-hosted upload whose bytes are on disk under a name that
//     does NOT match its JSON name (the sanitization mismatch), one whose
//     bytes are absent, one tombstone (bytes gone at the source), and one
//     external link that is a link and never an attachment.
//   - markup: a user mention, a broadcast mention, two channel references (one
//     resolvable, one not), Slack bold, and an entity-escaped ampersand.
const slackUsers = `[
  {"id": "U-ALICE", "name": "alice", "real_name": "Alice Anderson", "deleted": false,
   "is_owner": true, "profile": {"email": "alice@slack.test", "real_name": "Alice Anderson"}},
  {"id": "U-BOB", "name": "bob", "real_name": "Bob Brown", "deleted": false,
   "profile": {"email": "bob@slack.test", "real_name": "Bob Brown"}},
  {"id": "U-CARA", "name": "cara", "real_name": "Cara Cole", "deleted": true,
   "is_restricted": true, "is_ultra_restricted": true,
   "profile": {"email": "cara@slack.test", "real_name": "Cara Cole"}},
  {"id": "U-BOT", "name": "deploybot", "real_name": "Deploy Bot", "is_bot": true,
   "profile": {}}
]`

const slackChannels = `[
  {"id": "C-GEN", "name": "general", "created": 1550000000, "is_archived": false,
   "purpose": {"value": "everything"}, "members": ["U-ALICE", "U-BOB", "U-CARA"]}
]`

const slackGroups = `[
  {"id": "C-ENG", "name": "eng-private", "created": 1550001000, "is_archived": false,
   "purpose": {"value": "the private one"}, "members": ["U-ALICE", "U-BOB"]}
]`

const slackMPIMs = `[
  {"id": "G-TRIO", "name": "mpdm-alice--bob--cara-1", "created": 1550002000,
   "members": ["U-ALICE", "U-BOB", "U-CARA"]}
]`

const slackDMs = `[
  {"id": "D-AB", "created": 1550003000, "members": ["U-ALICE", "U-BOB"]},
  {"id": "D-BOT", "created": 1550004000, "members": ["U-ALICE", "U-BOT"]}
]`

// Day one of #general. The three threading shapes plus both counted drops.
const slackGeneralDay1 = `[
  {"type": "message", "user": "U-ALICE", "ts": "1554100000.000100",
   "text": "morning <@U-BOB> — <!channel> ship *today*, R&amp;D signed off"},
  {"type": "message", "user": "U-BOB", "ts": "1554100100.000200",
   "thread_ts": "1554100100.000200", "text": "release checklist",
   "reactions": [{"name": "tada", "users": ["U-ALICE", "U-CARA"]},
                 {"name": "clap::skin-tone-3", "users": ["U-ALICE"]},
                 {"name": "eyes", "users": ["U-BOT"]}]},
  {"type": "message", "user": "U-CARA", "ts": "1554100200.000300",
   "thread_ts": "1554100100.000200", "parent_user_id": "U-BOB", "text": "staging is green"},
  {"type": "message", "subtype": "thread_broadcast", "user": "U-BOB",
   "ts": "1554100300.000400", "thread_ts": "1554100100.000200",
   "text": "shipping now, everyone"},
  {"type": "message", "user": "U-BOT", "ts": "1554100400.000500",
   "text": "deploy 42 succeeded"},
  {"type": "message", "subtype": "channel_join", "user": "U-CARA",
   "ts": "1554100500.000600", "text": "<@U-CARA> has joined the channel"}
]`

// Day two of #general: a file_share, whose upload arrives in `file`, not
// `files` — and here in BOTH, which an export is free to do and which must
// still yield one attachment, one reference and one link.
const slackGeneralDay2 = `[
  {"type": "message", "subtype": "file_share", "user": "U-ALICE",
   "ts": "1554200000.000100", "text": "here is the plan",
   "file": {"id": "F-DOC", "name": "roadmap/final.pdf", "title": "Roadmap",
            "mimetype": "application/pdf", "timestamp": 1554199000, "user": "U-ALICE",
            "mode": "hosted", "url_private": "https://files.slack.com/files-pri/T1-F-DOC/roadmap.pdf"},
   "files": [{"id": "F-DOC", "name": "roadmap/final.pdf", "title": "Roadmap",
              "mimetype": "application/pdf", "timestamp": 1554199000, "user": "U-ALICE",
              "mode": "hosted", "url_private": "https://files.slack.com/files-pri/T1-F-DOC/roadmap.pdf"}]}
]`

// The private channel: channel references, and the three non-importable file
// shapes in one message.
const slackEngDay1 = `[
  {"type": "message", "user": "U-ALICE", "ts": "1554300000.000100",
   "text": "see <#C-GEN|general>, and <#C-GONE|archive> is gone"},
  {"type": "message", "user": "U-BOB", "ts": "1554300100.000200", "text": "attachments",
   "files": [
     {"id": "F-LOST", "name": "missing.bin", "mimetype": "application/octet-stream",
      "timestamp": 1554299000, "user": "U-BOB", "mode": "hosted",
      "url_private": "https://files.slack.com/files-pri/T1-F-LOST/missing.bin"},
     {"id": "F-TOMB", "name": "expired.png", "mimetype": "image/png",
      "timestamp": 1554299500, "user": "U-BOB", "mode": "tombstone",
      "url_private": "https://files.slack.com/files-pri/T1-F-TOMB/expired.png"},
     {"id": "F-DRIVE", "name": "budget.gsheet", "mimetype": "application/vnd.google-apps.spreadsheet",
      "timestamp": 1554299800, "user": "U-BOB", "mode": "external",
      "url_private": "https://docs.google.example/d/abc"}
   ]}
]`

const slackTrioDay = `[
  {"type": "message", "user": "U-CARA", "ts": "1554400000.000100", "text": "three of us here"}
]`

const slackPairDay = `[
  {"type": "message", "user": "U-ALICE", "ts": "1554500000.000100", "text": "just us two"}
]`

const slackBotDMDay = `[
  {"type": "message", "user": "U-ALICE", "ts": "1554600000.000100", "text": "hi bot"}
]`

// writeSlackFixture lays the export out on disk. The uploads tree deliberately
// stores F-DOC's bytes under a name that does NOT match its JSON `name`
// ("roadmap/final.pdf" cannot be a filename), which is the whole reason
// resolution is by ID.
func writeSlackFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	write := func(rel, body string) {
		t.Helper()
		full := filepath.Join(dir, rel)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("users.json", slackUsers)
	write("channels.json", slackChannels)
	write("groups.json", slackGroups)
	write("mpims.json", slackMPIMs)
	write("dms.json", slackDMs)
	write(filepath.Join("general", "2019-04-01.json"), slackGeneralDay1)
	write(filepath.Join("general", "2019-04-02.json"), slackGeneralDay2)
	write(filepath.Join("eng-private", "2019-04-01.json"), slackEngDay1)
	write(filepath.Join("mpdm-alice--bob--cara-1", "2019-04-03.json"), slackTrioDay)
	write(filepath.Join("D-AB", "2019-04-03.json"), slackPairDay)
	write(filepath.Join("D-BOT", "2019-04-03.json"), slackBotDMDay)
	write(filepath.Join("__uploads", "F-DOC", "roadmap-final.pdf"), "the slack roadmap")
	return dir
}

func TestSlackImport(t *testing.T) {
	pool, orgID := testPool(t)
	ctx := context.Background()
	dir := writeSlackFixture(t)
	store, err := blob.Open("fs", t.TempDir())
	if err != nil {
		t.Fatalf("blob: %v", err)
	}
	svc := New(pool, store)

	rep, err := svc.RunSlack(ctx, orgID, dir, false)
	if err != nil {
		t.Fatalf("slack import: %v", err)
	}
	if n, err := perms.NewRebuildWorker(pool, perms.New(pool), slog.Default()).RunOnce(ctx); err != nil || n != 1 {
		t.Fatalf("closure rebuild drain = %d jobs (%v), want 1", n, err)
	}

	// --- The whole report, derived from the fixture rather than read off a
	// run. 3 humans of 4 users (1 bot) · 2 channels · 3 + 2 memberships ·
	// exactly ONE thread (the single thread_ts conversation; every other
	// channel message is unthreaded and lands on the flat feed, which creates
	// nothing) · 7 channel messages (4 + 1 of #general's 6, both drops
	// excluded, plus 2 private) · 2 DM conversations and 2 DM messages, the
	// bot-tainted one skipped whole · 3 reaction rows from two merged
	// skin-tone variants and one unmappable reactor · 1 attachment landed,
	// 1 missing, 1 expired at the source · 13 synthetic watermarks.
	want := Report{
		Source: originSlack,
		Users:  3, Channels: 2, Subscriptions: 5, Threads: 1, Messages: 7,
		Reactions: 3, DMConversations: 2, DMMessages: 2, Attachments: 1,
		Watermarks:  13,
		BotsSkipped: 1, ChannelMessagesSkipped: 1, DMMessagesSkipped: 1,
		ReactionsUnmapped: 1, AttachmentFilesMissing: 1,
		AttachmentBytesExpired: 1, BroadcastsFlattened: 1, SystemNoticesDropped: 1,
		RenamedChannels: map[string]string{"general": "general-slack1"},
		RenamedGroups:   map[string]string{},
	}
	want.finalize()
	// Compared by REFLECTION over the whole Report, so a bucket added later is
	// covered here automatically instead of quietly falling out of the claim.
	if diff := reportDiffAs("want", "got", want, rep); len(diff) > 0 {
		t.Fatalf("slack report differs on %d bucket(s): %s", len(diff), strings.Join(diff, "; "))
	}

	// The dry run must predict the same thing on a VIRGIN org and, more
	// importantly, all-zeros with everything already-imported on this one.
	dryAfter, err := svc.RunSlack(ctx, orgID, dir, true)
	if err != nil {
		t.Fatalf("dry run after import: %v", err)
	}
	if dryAfter.Messages != 0 || dryAfter.DMMessages != 0 || dryAfter.Watermarks != 0 ||
		dryAfter.AlreadyImported == 0 || dryAfter.MatchedExistingByEmail != 3 {
		t.Fatalf("dry run on an already-imported org: %+v", dryAfter)
	}

	ids := loadSlackIDs(t, ctx, pool, orgID)

	// --- THREADING (RED 1). thread_ts is on PARENTS TOO: the parent carries
	// thread_ts == ts and the replies carry thread_ts != ts. Treating "has a
	// thread_ts" as "is a reply" would make the parent its own reply; ignoring
	// thread_ts entirely would flatten both replies onto the channel's feed.
	// Both failures are visible here: the three messages must share ONE kind=1
	// thread that is NOT the channel root, and the parent must be its root.
	var parentThread, replyThread, castThread, threadKind, rootMsg int64
	if err := pool.QueryRow(ctx, `
		SELECT p.thread_id, r.thread_id, b.thread_id, t.kind, COALESCE(t.root_message_id, 0)
		FROM message p, message r, message b, thread t
		WHERE p.org_id = $1 AND p.origin_id = 'C-GEN:1554100100.000200'
		  AND r.org_id = $1 AND r.origin_id = 'C-GEN:1554100200.000300'
		  AND b.org_id = $1 AND b.origin_id = 'C-GEN:1554100300.000400'
		  AND t.id = p.thread_id`, orgID).Scan(
		&parentThread, &replyThread, &castThread, &threadKind, &rootMsg); err != nil {
		t.Fatalf("thread shape: %v", err)
	}
	if replyThread != parentThread || castThread != parentThread {
		t.Fatalf("thread_ts was not honoured: parent thread %d, reply %d, broadcast %d "+
			"(all three belong to one thread)", parentThread, replyThread, castThread)
	}
	if parentThread == ids.genRoot {
		t.Fatalf("a threaded conversation landed on the channel's flat feed (thread %d)", parentThread)
	}
	if threadKind != 1 {
		t.Fatalf("imported Slack thread kind = %d, want 1", threadKind)
	}
	// The parent is the thread's root message — the reply is not.
	var parentID int64
	_ = pool.QueryRow(ctx, `SELECT id FROM message WHERE org_id = $1 AND origin_id = $2`,
		orgID, "C-GEN:1554100100.000200").Scan(&parentID)
	if rootMsg != parentID {
		t.Fatalf("thread root_message_id = %d, want the parent (%d)", rootMsg, parentID)
	}
	// Weft threads take thread_ts directly and are UNTITLED (ADR-001 D1's
	// Slack-thread shape). Nothing synthesizes a topic name out of a date and
	// a content snippet, which is Zulip's workaround for having topics.
	var title *string
	var threadOrigin string
	_ = pool.QueryRow(ctx, `SELECT title, origin_id FROM thread WHERE id = $1`,
		parentThread).Scan(&title, &threadOrigin)
	if title != nil && *title != "" {
		t.Fatalf("imported Slack thread carries a synthesized title %q", *title)
	}
	if threadOrigin != "thread:C-GEN:1554100100.000200" {
		t.Fatalf("thread provenance = %q, want thread:<channel id>:<thread_ts>", threadOrigin)
	}
	// The unthreaded messages went to the flat feed, and F-15 still holds
	// there: a kind=2 root takes no counters and no root_message_id.
	var flat, rootCount int
	var rootActivity *time.Time
	var rootRootMsg *int64
	if err := pool.QueryRow(ctx, `
		SELECT (SELECT count(*) FROM message WHERE org_id = $1 AND thread_id = $2),
		       (SELECT message_count FROM thread WHERE id = $2),
		       (SELECT last_activity_at FROM thread WHERE id = $2),
		       (SELECT root_message_id FROM thread WHERE id = $2)`,
		orgID, ids.genRoot).Scan(&flat, &rootCount, &rootActivity, &rootRootMsg); err != nil {
		t.Fatalf("flat feed: %v", err)
	}
	if flat != 2 {
		t.Fatalf("#general's flat feed holds %d messages, want 2 (the unthreaded pair)", flat)
	}
	if rootCount != 0 || rootActivity != nil || rootRootMsg != nil {
		t.Fatalf("F-15 broken on the channel root: count=%d activity=%v root_message=%v",
			rootCount, rootActivity, rootRootMsg)
	}

	// --- MARKUP (RED 2). The broadcast mention is INERT — the assert is on
	// NODE TYPE, because "@channel" as rendered text and "@channel" as a live
	// mention read identically to a human. The user mention beside it is the
	// positive control: the same message proves the lane works, so an inert
	// broadcast cannot be inertness-by-breakage.
	var openerAST, openerRendered, openerSrc string
	if err := pool.QueryRow(ctx, `
		SELECT ast::text, rendered, source FROM message
		WHERE org_id = $1 AND origin_id = 'C-GEN:1554100000.000100'`,
		orgID).Scan(&openerAST, &openerRendered, &openerSrc); err != nil {
		t.Fatalf("opening message: %v", err)
	}
	if !containsMention([]byte(openerAST), ids.bob) {
		t.Fatalf("<@U-BOB> did not resolve to the imported Bob (%d): %s", ids.bob, openerAST)
	}
	labels := mentionLabelsIn(t, openerAST)
	if len(labels) != 1 || labels[0] != "Bob Brown" {
		t.Fatalf("mention nodes = %v, want exactly [Bob Brown] — <!channel> must be inert "+
			"TEXT, and the assert is on NODE TYPE because inert text and a live mention "+
			"read alike: %s", labels, openerAST)
	}
	// It survives as the text the author saw, not as the raw wire token, which
	// would render as visible corruption.
	if !strings.Contains(openerRendered, "@channel") || strings.Contains(openerRendered, "&lt;!channel&gt;") {
		t.Fatalf("broadcast mention did not become inert text: %s", openerRendered)
	}
	// Slack's single-asterisk bold is CommonMark emphasis, so it has to be
	// doubled or every bold word imports italic.
	if !strings.Contains(openerRendered, "<strong>today</strong>") {
		t.Fatalf("Slack *bold* did not convert: %s", openerRendered)
	}
	// Slack entity-escapes & < > in every body; leaving them renders "R&amp;D".
	if !strings.Contains(openerSrc, "R&D") {
		t.Fatalf("Slack entity escapes were not undone: %q", openerSrc)
	}

	// Channel references: resolved by SOURCE ID into an inert node, unresolved
	// when the export has no such channel, and never a link either way.
	var refRendered string
	_ = pool.QueryRow(ctx, `
		SELECT rendered FROM message WHERE org_id = $1 AND origin_id = 'C-ENG:1554300000.000100'`,
		orgID).Scan(&refRendered)
	wantRef := fmt.Sprintf(`<span class="channel-ref" data-channel-id="%d">#general</span>`, ids.general)
	if !strings.Contains(refRendered, wantRef) {
		t.Fatalf("<#C-GEN|general> did not resolve to channel %d: %s", ids.general, refRendered)
	}
	if !strings.Contains(refRendered, `<span class="channel-ref channel-ref-unresolved">#archive</span>`) {
		t.Fatalf("a reference to a channel outside the export must stay unresolved: %s", refRendered)
	}
	if strings.Contains(refRendered, "<a ") {
		t.Fatalf("a channel reference rendered as a link: %s", refRendered)
	}

	// --- DMs. The all-human trio and the 1:1 import with their exact
	// participant sets; the bot-tainted 1:1 is skipped WHOLE, because dropping
	// the bot would shrink the canonical key onto Alice's real conversation.
	var spaces int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM dm_space WHERE org_id = $1`, orgID).Scan(&spaces); err != nil {
		t.Fatalf("dm spaces: %v", err)
	}
	if spaces != 2 {
		t.Fatalf("dm_space rows = %d, want 2 (the bot-tainted conversation is skipped whole)", spaces)
	}
	trio := dmParticipantSet(t, ctx, pool, orgID, 2)
	if len(trio) != 3 || !trio[ids.alice] || !trio[ids.bob] || !trio[ids.cara] {
		t.Fatalf("group DM participants = %v, want alice/bob/cara", trio)
	}
	pair := dmParticipantSet(t, ctx, pool, orgID, 1)
	if len(pair) != 2 || !pair[ids.alice] || !pair[ids.bob] {
		t.Fatalf("1:1 DM participants = %v, want alice/bob", pair)
	}

	// --- Roles. A Slack single-channel guest lands on the guest preset, and a
	// deleted account arrives deactivated.
	var caraRole int16
	var caraDeactivated *time.Time
	_ = pool.QueryRow(ctx, `SELECT role, deactivated_at FROM user_account WHERE id = $1`,
		ids.cara).Scan(&caraRole, &caraDeactivated)
	if caraRole != 50 || caraDeactivated == nil {
		t.Fatalf("single-channel guest imported role=%d deactivated=%v, want 50/deactivated",
			caraRole, caraDeactivated)
	}
	var aliceRole int16
	_ = pool.QueryRow(ctx, `SELECT role FROM user_account WHERE id = $1`, ids.alice).Scan(&aliceRole)
	if aliceRole != 10 {
		t.Fatalf("Slack owner imported role = %d, want 10", aliceRole)
	}

	// --- FILES (RED 4). The upload resolves BY ID out of __uploads/<id>/*.
	// Its bytes sit under "roadmap-final.pdf" while its JSON name is
	// "roadmap/final.pdf", which no filesystem can hold: a loader that matched
	// on the name would count this as a missing file.
	var fileID int64
	var fileName, storageKey string
	if err := pool.QueryRow(ctx, `
		SELECT id, name, storage_key FROM file
		WHERE org_id = $1 AND origin_system = 'slack' AND origin_id = 'F-DOC'`,
		orgID).Scan(&fileID, &fileName, &storageKey); err != nil {
		t.Fatalf("attachment resolved by id: %v", err)
	}
	if fileName != "roadmap/final.pdf" {
		t.Fatalf("file name = %q, want the source's own (the on-disk name is cosmetic)", fileName)
	}
	rc, err := store.Open(ctx, storageKey)
	if err != nil {
		t.Fatalf("blob open: %v", err)
	}
	body := make([]byte, 64)
	n, _ := rc.Read(body)
	rc.Close()
	if string(body[:n]) != "the slack roadmap" {
		t.Fatalf("blob content = %q", body[:n])
	}
	// The message's signed Slack link was rewritten to the managed file, and
	// the m2m reference landed.
	var shareSrc string
	var shareAttach bool
	var refs int
	if err := pool.QueryRow(ctx, `
		SELECT m.source, m.has_attachment,
		       (SELECT count(*) FROM file_reference fr
		         WHERE fr.file_id = $2 AND fr.entity_id = m.id AND fr.entity_type = $3)
		FROM message m WHERE m.org_id = $1 AND m.origin_id = 'C-GEN:1554200000.000100'`,
		orgID, fileID, int16(enum.EntityMessage)).Scan(&shareSrc, &shareAttach, &refs); err != nil {
		t.Fatalf("file_share message: %v", err)
	}
	if !strings.Contains(shareSrc, fmt.Sprintf("/api/v1/files/%d", fileID)) || !shareAttach || refs != 1 {
		t.Fatalf("file_share (upload in `file`, not `files`) did not land: src=%q attach=%v refs=%d",
			shareSrc, shareAttach, refs)
	}
	// The same id in BOTH `file` and `files` is one upload, not two: one
	// reference (asserted above) and one link in the body.
	if n := strings.Count(shareSrc, "/api/v1/files/"); n != 1 {
		t.Fatalf("the body carries %d managed links for one upload: %q", n, shareSrc)
	}
	// The tombstone and the external link never became file rows; the
	// Slack-hosted one with no bytes did not either.
	var strayFiles int
	_ = pool.QueryRow(ctx, `
		SELECT count(*) FROM file WHERE org_id = $1
		  AND origin_system = 'slack' AND origin_id IN ('F-LOST','F-TOMB','F-DRIVE')`,
		orgID).Scan(&strayFiles)
	if strayFiles != 0 {
		t.Fatalf("%d file row(s) for uploads with no importable bytes", strayFiles)
	}

	// --- READ STATE (RED 3). Imported history arrives READ: one synthetic
	// watermark per (member, thread) at the highest message the import landed,
	// and SeedUnreadCounters then finds nothing unread. Skipping the mint does
	// NOT give zero badges — the first live message would create a counter row
	// and the S6 reconcile would recompute it to the FULL imported history.
	var badges int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM container_unread_counter WHERE org_id = $1 AND unread_count > 0`,
		orgID).Scan(&badges); err != nil {
		t.Fatalf("unread counters: %v", err)
	}
	if badges != 0 {
		t.Fatalf("%d unread badge(s) survived the import — history must arrive read", badges)
	}
	// And the watermarks are real rows on the right messages, not an absence
	// of counters: Bob's #general flat-feed watermark sits on the LAST message
	// of that feed, not on the last message of the channel.
	var wmRows int
	_ = pool.QueryRow(ctx, `
		SELECT count(*) FROM thread_read_watermark w
		JOIN thread t ON t.id = w.thread_id WHERE t.org_id = $1`, orgID).Scan(&wmRows)
	if wmRows != 13 {
		t.Fatalf("watermark rows = %d, want 13", wmRows)
	}
	var bobFlatWM, lastFlat int64
	if err := pool.QueryRow(ctx, `
		SELECT w.last_read_message_id,
		       (SELECT id FROM message WHERE org_id = $1 AND origin_id = 'C-GEN:1554200000.000100')
		FROM thread_read_watermark w WHERE w.user_id = $2 AND w.thread_id = $3`,
		orgID, ids.bob, ids.genRoot).Scan(&bobFlatWM, &lastFlat); err != nil {
		t.Fatalf("bob's flat-feed watermark: %v", err)
	}
	if bobFlatWM != lastFlat {
		t.Fatalf("flat-feed watermark = %d, want the feed's last message (%d)", bobFlatWM, lastFlat)
	}

	// --- BACKFILLS NEVER NOTIFY. Every imported event carries actor kind 4,
	// and the notification consumer skips exactly that. The count is only
	// evidence with a REAL consumer wired and a positive anchor, because the
	// importer builds none and an unwired lane answers zero for free.
	var wrongActor int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM event_log e
		JOIN message m ON m.id = e.entity_id AND m.org_id = e.org_id
		WHERE e.org_id = $1 AND e.entity_type = $2 AND m.origin_system = 'slack'
		  AND e.actor_kind <> $3`,
		orgID, int16(enum.EntityMessage), int16(enum.ActorImporter)).Scan(&wrongActor); err != nil {
		t.Fatalf("actor census: %v", err)
	}
	if wrongActor != 0 {
		t.Fatalf("%d imported message event(s) do not carry the importer actor kind", wrongActor)
	}
	// The whole importer event surface, per verb: two channels, ONE thread
	// (the flat feed creates none and roots are silent), two conversations,
	// one file and nine messages. Slack ships no user groups in the four files
	// this loader parses, so usergroup.created must be absent entirely.
	wantEvents := map[string]int{
		"channel.created": 2, "thread.created": 1, "dm.opened": 2,
		"file.uploaded": 1, "message.created": 9,
	}
	if got := importerEventCensus(t, ctx, pool, orgID); !reflect.DeepEqual(got, wantEvents) {
		t.Fatalf("importer events = %v, want %v", got, wantEvents)
	}
	// E3: source time on occurred_at, ingest time on recorded_at, for EVERY
	// importer event.
	var badStamp int
	_ = pool.QueryRow(ctx, `
		SELECT count(*) FROM event_log WHERE org_id = $1 AND actor_kind = $2
		  AND NOT (extract(year from occurred_at) = 2019
		           AND recorded_at > now() - interval '1 hour')`,
		orgID, int16(enum.ActorImporter)).Scan(&badStamp)
	if badStamp != 0 {
		t.Fatalf("%d importer events violate E3 (2019 occurred_at, fresh recorded_at)", badStamp)
	}
	runner := notification.NewRunner(pool, nil, slog.Default())
	drainNotifications(t, ctx, pool, orgID, runner.ProcessOrg)
	var notifs int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM notification WHERE org_id = $1`, orgID).Scan(&notifs)
	if notifs != 0 {
		t.Fatalf("the import minted %d notification(s); backfills never notify", notifs)
	}
	// THE ANCHOR. The same verb, the same payload, the same mention of Bob —
	// only the actor kind differs. If this does not mint a row, the zero above
	// measured a dead consumer rather than the backfill contract.
	var openerID, genThread int64
	_ = pool.QueryRow(ctx,
		`SELECT id, thread_id FROM message WHERE org_id = $1 AND origin_id = 'C-GEN:1554100000.000100'`,
		orgID).Scan(&openerID, &genThread)
	if err := db.WithTx(ctx, pool, func(tx pgx.Tx) error {
		_, err := eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: orgID, ActorKind: enum.ActorHuman, ActorID: &ids.alice,
			EntityType: enum.EntityMessage, EntityID: openerID, Verb: "message.created",
			Payload: eventlog.MustPayload(map[string]any{
				"message_id": openerID, "channel_id": ids.general,
				"thread_id": genThread, "mentions": []int64{ids.bob}}),
		})
		return err
	}); err != nil {
		t.Fatalf("anchor event: %v", err)
	}
	drainNotifications(t, ctx, pool, orgID, runner.ProcessOrg)
	var anchorUser int64
	var anchorKind int16
	if err := pool.QueryRow(ctx,
		`SELECT user_id, kind FROM notification WHERE org_id = $1`, orgID).Scan(&anchorUser, &anchorKind); err != nil {
		t.Fatalf("the same payload under a HUMAN actor minted no notification, so the "+
			"zero above proves nothing: %v", err)
	}
	if anchorUser != ids.bob || anchorKind != notification.KindMention {
		t.Fatalf("anchor notification = user %d kind %d, want bob (%d) mention (%d)",
			anchorUser, anchorKind, ids.bob, notification.KindMention)
	}

	// --- Idempotency (D5). A re-run duplicates nothing, including the
	// synthetic watermarks, which is where a second source is most likely to
	// break it: root threads carry no provenance of their own.
	rep2, err := svc.RunSlack(ctx, orgID, dir, false)
	if err != nil {
		t.Fatalf("re-run: %v", err)
	}
	if rep2.Messages != 0 || rep2.DMMessages != 0 || rep2.Threads != 0 ||
		rep2.Channels != 0 || rep2.Users != 0 || rep2.Watermarks != 0 ||
		rep2.Attachments != 0 {
		t.Fatalf("re-run imported new rows: %+v", rep2)
	}
	var msgs, threads int
	_ = pool.QueryRow(ctx, `
		SELECT (SELECT count(*) FROM message WHERE org_id = $1 AND origin_system = 'slack'),
		       (SELECT count(*) FROM thread  WHERE org_id = $1 AND origin_system = 'slack')`,
		orgID).Scan(&msgs, &threads)
	if msgs != 9 || threads != 1 {
		t.Fatalf("after re-run: %d messages / %d threads, want 9 (7 channel + 2 dm) / 1", msgs, threads)
	}
}

// TestSlackUploadsResolveByID pins the three answers the __uploads convention
// gives, without a database: exactly one file is the bytes, none is a counted
// loss, and two or more is a HARD error — ambiguity must never silently pick.
func TestSlackUploadsResolveByID(t *testing.T) {
	base := func(t *testing.T) string {
		t.Helper()
		dir := t.TempDir()
		for name, body := range map[string]string{
			"users.json": `[]`, "channels.json": `[]`,
		} {
			if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		return dir
	}
	put := func(t *testing.T, dir, id, name string) {
		t.Helper()
		p := filepath.Join(dir, "__uploads", id)
		if err := os.MkdirAll(p, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(p, name), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	dir := base(t)
	put(t, dir, "F-1", "whatever-the-fetcher-called-it.bin")
	ex, err := LoadSlackExport(dir)
	if err != nil {
		t.Fatalf("one file: %v", err)
	}
	probe, open := ex.attachmentBytes("F-1")
	if size, err := probe(); err != nil || size != 1 {
		t.Fatalf("probe = %d, %v; want 1, nil — resolution is by ID, not by name", size, err)
	}
	if f, err := open(); err != nil {
		t.Fatalf("open: %v", err)
	} else {
		f.Close()
	}

	// An id the tree does not carry: a counted loss, never an error.
	probe, _ = ex.attachmentBytes("F-ABSENT")
	if _, err := probe(); err == nil {
		t.Fatal("an absent upload must answer the same 'no bytes' a truncated export does")
	}

	dir2 := base(t)
	put(t, dir2, "F-2", "a.bin")
	put(t, dir2, "F-2", "b.bin")
	if _, err := LoadSlackExport(dir2); err == nil ||
		!strings.Contains(err.Error(), "will not guess which") {
		t.Fatalf("two files under one id must be a hard, named error; got %v", err)
	}
}

// slackRowIDs are the row ids the assertions above compare against.
type slackRowIDs struct {
	alice, bob, cara int64
	general, genRoot int64
}

func loadSlackIDs(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID int64) slackRowIDs {
	t.Helper()
	var out slackRowIDs
	if err := pool.QueryRow(ctx, `
		SELECT (SELECT id FROM user_account WHERE org_id = $1 AND origin_system = 'slack' AND origin_id = 'U-ALICE'),
		       (SELECT id FROM user_account WHERE org_id = $1 AND origin_system = 'slack' AND origin_id = 'U-BOB'),
		       (SELECT id FROM user_account WHERE org_id = $1 AND origin_system = 'slack' AND origin_id = 'U-CARA'),
		       c.id, c.root_thread_id
		FROM channel c
		WHERE c.org_id = $1 AND c.origin_system = 'slack' AND c.origin_id = 'C-GEN'`,
		orgID).Scan(&out.alice, &out.bob, &out.cara, &out.general, &out.genRoot); err != nil {
		t.Fatalf("imported ids: %v", err)
	}
	return out
}

// dmParticipantSet returns the participants of the org's single dm_space of
// the given kind (1 = 1:1, 2 = group).
func dmParticipantSet(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID int64, kind int16) map[int64]bool {
	t.Helper()
	rows, err := pool.Query(ctx, `
		SELECT dp.user_id FROM dm_participant dp
		JOIN dm_space ds ON ds.id = dp.dm_space_id
		WHERE ds.org_id = $1 AND ds.kind = $2`, orgID, kind)
	if err != nil {
		t.Fatalf("dm participants: %v", err)
	}
	defer rows.Close()
	out := map[int64]bool{}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan dm participant: %v", err)
		}
		out[id] = true
	}
	return out
}

// mentionLabelsIn lists every mention node's label in a stored AST.
func mentionLabelsIn(t *testing.T, ast string) []string {
	t.Helper()
	var doc map[string]any
	if err := json.Unmarshal([]byte(ast), &doc); err != nil {
		t.Fatalf("ast: %v", err)
	}
	var out []string
	var walk func(map[string]any)
	walk = func(n map[string]any) {
		if n["type"] == "mention" {
			if attrs, ok := n["attrs"].(map[string]any); ok {
				if label, ok := attrs["label"].(string); ok {
					out = append(out, label)
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
	return out
}

// drainNotifications re-polls until the org's committed backlog is consumed.
// Poll's xmin gate is database-global, so one synchronous pass can legitimately
// come back empty while committed events are still pending — "poll again" is
// the consumer contract, not a workaround.
func drainNotifications(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID int64,
	process func(context.Context, int64) error) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for {
		if err := process(ctx, orgID); err != nil {
			t.Fatalf("drain notifications: %v", err)
		}
		var lag int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM event_log e
			WHERE e.org_id = $1 AND e.id > COALESCE((
			  SELECT last_id FROM event_consumer_cursor
			  WHERE consumer = 'notifications' AND org_id = $1), 0)`, orgID).Scan(&lag); err != nil {
			t.Fatalf("drain lag: %v", err)
		}
		if lag == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the notification consumer never caught up")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestSlackMarkdownDialect is the table for the conversion the loader owns.
// It runs without a database because the dialect is pure, and it covers the
// cases TestSlackImport's fixture cannot hold one of each of: the emphasis
// boundary rules, code spans, and the `<!…>` family beyond a broadcast.
func TestSlackMarkdownDialect(t *testing.T) {
	d := slackDialect{
		userName:    map[string]string{"U1": "Alice Anderson"},
		channelName: map[string]string{"C1": "general"},
	}
	for _, tc := range []struct {
		name, in, want string
		mentions       map[string]string
		chanRefs       map[string]string
	}{
		// Slack's single-delimiter emphasis. CommonMark reads a lone '*' as
		// EMPHASIS, so an unconverted *bold* imports italic.
		{name: "bold", in: "ship *today*", want: "ship **today**"},
		{name: "strike", in: "not ~this~", want: "not ~~this~~"},
		// Left alone on purpose: CommonMark already reads _x_ as emphasis with
		// the same intra-word refusal Slack has.
		{name: "italic untouched", in: "an _aside_ here", want: "an _aside_ here"},
		// Slack has no intra-word formatting, and neither does the conversion.
		{name: "intra-word", in: "a*b*c and x~y~z", want: "a*b*c and x~y~z"},
		// A code span is not prose; rewriting inside it shows the reader
		// literal asterisks.
		{name: "code span", in: "call `a*b*c` now", want: "call `a*b*c` now"},
		{name: "fenced", in: "```\nx = *y*\n```", want: "```\nx = *y*\n```"},

		{name: "mention", in: "hi <@U1>", want: "hi @**Alice Anderson**",
			mentions: map[string]string{"Alice Anderson": "U1"}},
		{name: "mention with handle", in: "hi <@U1|alice>", want: "hi @**Alice Anderson**",
			mentions: map[string]string{"Alice Anderson": "U1"}},
		// Nobody the export knows: inert text, never a person-shaped span for
		// someone who will not exist in the org.
		{name: "unknown mention", in: "hi <@U9|ghost>", want: "hi @ghost"},

		{name: "channel ref", in: "in <#C1|general>", want: "in #**general**",
			chanRefs: map[string]string{"general": "C1"}},
		// The pipe-less form, which the reference implementation ignores.
		{name: "channel ref no pipe", in: "in <#C1>", want: "in #**general**",
			chanRefs: map[string]string{"general": "C1"}},
		// Absent from channels.json: label-only, still rendered, unresolved.
		{name: "unknown channel ref", in: "in <#C9|archive>", want: "in #**archive**"},

		{name: "broadcast", in: "<!channel> ship", want: "@channel ship"},
		{name: "here", in: "<!here|@here> ship", want: "@here ship"},
		{name: "subteam", in: "<!subteam^S1|@eng> ship", want: "@eng ship"},
		// A `<!date…>` is a formatted date, not a mention: its fallback is
		// exactly what the reader saw.
		{name: "date special", in: "on <!date^1554100000^{date_short}|Apr 1, 2019>",
			want: "on Apr 1, 2019"},

		{name: "bare link", in: "see <https://x.example/a>", want: "see https://x.example/a"},
		{name: "labelled link", in: "see <https://x.example/a|the docs>",
			want: "see [the docs](https://x.example/a)"},
		{name: "mailto", in: "mail <mailto:a@x.example|a@x.example>", want: "mail mailto:a@x.example"},

		// Slack escapes & < > in EVERY body; leaving them shows the entities.
		{name: "entities", in: "R&amp;D says 5 &lt; 6", want: "R&D says 5 < 6"},
		// Undone LAST, so an escaped '<' never opens a token and a
		// double-escape yields what the author actually wrote.
		{name: "double escape", in: "type &amp;lt; to escape", want: "type &lt; to escape"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, mentions, chanRefs := d.convertBody(tc.in)
			if got != tc.want {
				t.Fatalf("body = %q, want %q", got, tc.want)
			}
			if len(mentions) != len(tc.mentions) {
				t.Fatalf("mention lane = %v, want %v", mentions, tc.mentions)
			}
			for k, v := range tc.mentions {
				if mentions[k] != v {
					t.Fatalf("mention lane[%q] = %q, want %q", k, mentions[k], v)
				}
			}
			if len(chanRefs) != len(tc.chanRefs) {
				t.Fatalf("channel-ref lane = %v, want %v", chanRefs, tc.chanRefs)
			}
			for k, v := range tc.chanRefs {
				if chanRefs[k] != v {
					t.Fatalf("channel-ref lane[%q] = %q, want %q", k, chanRefs[k], v)
				}
			}
		})
	}
}

// TestSlackTimestampOrdinal pins the ordinal Slack's `ts` becomes. A '>' on the
// strings themselves is meaningless — "1550000000.000200" sorts after
// "1550000001.0" — which is the reason the IR carries an explicit ordinal at
// all, and a float64 cannot hold a microsecond epoch exactly.
func TestSlackTimestampOrdinal(t *testing.T) {
	a, at, ok := slackTS("1550000000.000200")
	if !ok || a != 1550000000000200 {
		t.Fatalf("ordinal = %d (ok=%v), want 1550000000000200", a, ok)
	}
	if at.UTC().Format(time.RFC3339Nano) != "2019-02-12T19:33:20.0002Z" {
		t.Fatalf("sent at = %s", at.UTC().Format(time.RFC3339Nano))
	}
	b, _, _ := slackTS("1550000001.0")
	if !(b > a) {
		t.Fatalf("a later message ordered before an earlier one: %d !> %d", b, a)
	}
	// One microsecond apart must not collide, or two messages swap.
	c, _, _ := slackTS("1550000000.000201")
	if c != a+1 {
		t.Fatalf("microsecond resolution lost: %d vs %d", c, a)
	}
	if _, _, ok := slackTS("not-a-timestamp"); ok {
		t.Fatal("a malformed ts must not parse")
	}
}
