package importer

import (
	"context"
	"testing"

	"github.com/abhinavjha0239/weft/internal/platform/blob"
)

// TestReportMatchesTheRowsItLands closes the loop the equality pin cannot: two
// reports can agree with each other and both be wrong. Every "imported" bucket
// is compared against the rows the import actually created, on a virgin org
// where created == total.
//
// It is the executor-side half of the planner contract. The report is now
// projected by the planner BEFORE any row is written; if the executor lands a
// different number of rows than the plan promised, nothing else in the suite
// would notice.
func TestReportMatchesTheRowsItLands(t *testing.T) {
	pool, orgID := testPool(t)
	ctx := context.Background()
	dir := writeFixture(t)
	store, err := blob.Open("fs", t.TempDir())
	if err != nil {
		t.Fatalf("blob: %v", err)
	}
	rep, err := New(pool, store).Run(ctx, orgID, dir, false)
	if err != nil {
		t.Fatalf("import: %v", err)
	}

	for _, c := range []struct {
		bucket string
		got    int
		query  string
	}{
		{"users", rep.Users,
			`SELECT count(*) FROM user_account WHERE org_id = $1 AND origin_system IS NOT NULL`},
		{"channels", rep.Channels,
			`SELECT count(*) FROM channel WHERE org_id = $1 AND origin_system IS NOT NULL`},
		{"threads", rep.Threads,
			`SELECT count(*) FROM thread WHERE org_id = $1 AND origin_system IS NOT NULL`},
		{"messages", rep.Messages,
			`SELECT count(*) FROM message WHERE org_id = $1 AND origin_system IS NOT NULL AND channel_id IS NOT NULL`},
		{"dm_messages", rep.DMMessages,
			`SELECT count(*) FROM message WHERE org_id = $1 AND origin_system IS NOT NULL AND dm_space_id IS NOT NULL`},
		{"dm_conversations", rep.DMConversations,
			`SELECT count(*) FROM dm_space WHERE org_id = $1`},
		// The bootstrap owner is already in role:owners, so the import's own
		// group writes are the rest.
		{"subscriptions", rep.Subscriptions,
			`SELECT count(*) FROM channel_member cm JOIN channel c ON c.id = cm.channel_id
			  WHERE c.org_id = $1 AND c.origin_system IS NOT NULL`},
		{"groups", rep.Groups,
			`SELECT count(*) FROM user_group WHERE org_id = $1 AND origin_system IS NOT NULL`},
		{"group_members", rep.GroupMembers,
			`SELECT count(*) FROM user_group_member m JOIN user_group g ON g.id = m.group_id
			  WHERE g.org_id = $1 AND g.origin_system IS NOT NULL`},
		{"reactions", rep.Reactions,
			`SELECT count(*) FROM reaction r JOIN message m ON m.id = r.message_id WHERE m.org_id = $1`},
		{"attachments", rep.Attachments,
			`SELECT count(*) FROM file WHERE org_id = $1 AND origin_system IS NOT NULL`},
		{"read_watermarks", rep.Watermarks,
			`SELECT count(*) FROM thread_read_watermark w JOIN thread t ON t.id = w.thread_id
			  WHERE t.org_id = $1`},
		{"message_edits", rep.MessageEdits,
			`SELECT count(*) FROM message_revision r JOIN message m ON m.id = r.message_id
			  WHERE m.org_id = $1`},
	} {
		var rows int
		if err := pool.QueryRow(ctx, c.query, orgID).Scan(&rows); err != nil {
			t.Fatalf("%s row count: %v", c.bucket, err)
		}
		if rows != c.got {
			t.Errorf("report says %s = %d, the database holds %d row(s)", c.bucket, c.got, rows)
		}
	}
}
