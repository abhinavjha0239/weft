package importer

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/abhinavjha0239/weft/internal/platform/blob"
)

// Two loss buckets of the write path — StreamMessagesSkipped and
// ReactionsUnmapped — appear NOWHERE in the rest of the suite, not even as
// names: the showcase fixture never produces an unmappable channel-message
// author or an unmappable reaction, so both are structurally unobserved and
// either could be deleted with everything still green. This fixture makes
// them non-zero, and pins the ROWS the buckets stand for (the skipped message
// and its thread must not exist; the reactions must not exist) so the
// assertion measures the write, not just the counter.
//
// It also carries the only INACTIVE source user in the suite, which is what
// makes the deactivated_at polarity legible: the write path's
// `CASE WHEN is_active THEN NULL ELSE now() END` is invertible with nothing
// going red as long as every fixture user is active.
const lossyRealm = `{
  "zerver_userprofile": [
    {"id": 11, "delivery_email": "active@zulip.test", "full_name": "Active Human", "is_active": true, "is_bot": false, "role": 400, "date_joined": 1546300800},
    {"id": 12, "delivery_email": "gone@zulip.test", "full_name": "Departed Human", "is_active": false, "is_bot": false, "role": 400, "date_joined": 1546300800},
    {"id": 13, "delivery_email": "notify-bot@zulip.test", "full_name": "Notify Bot", "is_active": true, "is_bot": true, "role": 400, "date_joined": 1546300800}
  ],
  "zerver_stream": [
    {"id": 21, "name": "eng-notes", "description": "no collision with bootstrap", "invite_only": false, "deactivated": false, "date_created": 1546300800}
  ],
  "zerver_recipient": [
    {"id": 31, "type": 2, "type_id": 21}
  ],
  "zerver_subscription": [
    {"id": 41, "user_profile": 11, "recipient": 31, "active": true}
  ]
}`

// 101 is a normal human post. 102 is authored by the BOT, whose account is
// never created, so the channel-message lane cannot map an author and drops
// it — with its own topic, so the drop is visible as a missing thread and not
// merely a missing message. The reactions then have nothing to hang on:
// 201 points at the dropped message, 202 is by the bot.
const lossyMessages = `{
  "zerver_message": [
    {"id": 101, "sender": 11, "recipient": 31, "subject": "kept", "content": "a human wrote this", "date_sent": 1554100000, "edit_history": null},
    {"id": 102, "sender": 13, "recipient": 31, "subject": "bot-only", "content": "automated digest", "date_sent": 1554100100, "edit_history": null}
  ],
  "zerver_reaction": [
    {"id": 201, "user_profile": 11, "message": 102, "emoji_name": "tada"},
    {"id": 202, "user_profile": 13, "message": 101, "emoji_name": "eyes"}
  ],
  "zerver_usermessage": []
}`

func TestImportCountsUnmappableChannelMessagesAndReactions(t *testing.T) {
	pool, orgID := testPool(t)
	ctx := context.Background()

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "realm.json"), []byte(lossyRealm), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "messages-000001.json"), []byte(lossyMessages), 0o644); err != nil {
		t.Fatal(err)
	}
	store, err := blob.Open("fs", t.TempDir())
	if err != nil {
		t.Fatalf("blob: %v", err)
	}

	rep, err := New(pool, store).Run(ctx, orgID, dir, false)
	if err != nil {
		t.Fatalf("import: %v", err)
	}

	// Two humans in, one bot out.
	if rep.Users != 2 || rep.BotsSkipped != 1 {
		t.Fatalf("users = %d, bots skipped = %d, want 2/1 (%+v)", rep.Users, rep.BotsSkipped, rep)
	}
	// The bot's channel message is a COUNTED loss, not a silent one.
	if rep.Messages != 1 || rep.StreamMessagesSkipped != 1 {
		t.Fatalf("messages = %d, channel messages skipped = %d, want 1/1 (%+v)",
			rep.Messages, rep.StreamMessagesSkipped, rep)
	}
	// Both reactions are unmappable: 201's message was dropped, 202's author
	// is the bot. Neither may land, and both must be counted.
	if rep.Reactions != 0 || rep.ReactionsUnmapped != 2 {
		t.Fatalf("reactions = %d, unmapped = %d, want 0/2 (%+v)",
			rep.Reactions, rep.ReactionsUnmapped, rep)
	}

	// The rows the buckets stand for. The dropped message left NO message row
	// and NO thread — the skip happens before the topic is materialized, so a
	// regression that merely mis-counts would still create an empty topic.
	var msgs, threads, reactions int
	if err := pool.QueryRow(ctx, `
		SELECT
		  (SELECT count(*) FROM message WHERE org_id = $1 AND origin_system IS NOT NULL),
		  (SELECT count(*) FROM thread WHERE org_id = $1 AND kind = 1),
		  (SELECT count(*) FROM reaction r JOIN message m ON m.id = r.message_id
		    WHERE m.org_id = $1)`, orgID).Scan(&msgs, &threads, &reactions); err != nil {
		t.Fatalf("row census: %v", err)
	}
	if msgs != 1 || threads != 1 || reactions != 0 {
		t.Fatalf("rows: messages=%d threads=%d reactions=%d, want 1/1/0", msgs, threads, reactions)
	}
	var botTopic int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM thread WHERE org_id = $1 AND title = 'bot-only'`,
		orgID).Scan(&botTopic); err != nil {
		t.Fatalf("bot topic check: %v", err)
	}
	if botTopic != 0 {
		t.Fatalf("the skipped bot message still materialized its topic (%d thread(s))", botTopic)
	}

	// deactivated_at POLARITY. A source user who was ACTIVE arrives live
	// (NULL); one who was DEACTIVATED in the source arrives deactivated. The
	// column is written by a single CASE on is_active, so inverting it is a
	// one-character change — and without a fixture that carries both
	// polarities, nothing in the suite can see it.
	var activeDeact, goneDeact *string
	if err := pool.QueryRow(ctx, `
		SELECT
		  (SELECT deactivated_at::text FROM user_account
		    WHERE org_id = $1 AND origin_system = 'zulip' AND origin_id = '11'),
		  (SELECT deactivated_at::text FROM user_account
		    WHERE org_id = $1 AND origin_system = 'zulip' AND origin_id = '12')`,
		orgID).Scan(&activeDeact, &goneDeact); err != nil {
		t.Fatalf("deactivated_at: %v", err)
	}
	if activeDeact != nil {
		t.Fatalf("an ACTIVE source user imported deactivated (deactivated_at = %q)", *activeDeact)
	}
	if goneDeact == nil {
		t.Fatal("a DEACTIVATED source user imported live (deactivated_at IS NULL)")
	}
}
