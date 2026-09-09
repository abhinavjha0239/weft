package importer

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/platform/blob"
)

// memoryBytes is an attachment whose bytes never touch a filesystem. It is
// the point of the test below as much as a convenience: if anything in the
// write path still reached for a path, an export directory or a Zulip
// uploads/ tree, this would not compile, let alone pass.
type memoryBytes struct{ *bytes.Reader }

func (memoryBytes) Close() error { return nil }

// TestWriteDrainsANeutralImport is the only test in the suite that does not
// go through the Zulip loader. It hands write() an Import built by hand and
// asserts the four properties that make the IR a seam rather than a rename:
//
//  1. SOURCE IDS ARE OPAQUE STRINGS. Every id here is non-numeric
//     ("U-ALPHA", "C-GEN", "M-FIRST"). Under the old int64 write path these
//     could not exist; under a `any`-typed one they would land in
//     origin_id as `%!d(string=U-ALPHA)` with go vet silent. Asserting the
//     stored origin_id is what makes that unfakeable.
//  2. THE PROVENANCE TOKEN IS DATA. Nothing in the write path says "zulip":
//     this import calls itself "acme" and every origin_system column AND the
//     operator-visible collision-rename suffix must say so too.
//  3. ORDER COMES FROM Ordinal, NOT FROM EMISSION ORDER. The messages are
//     appended backwards. What lands must still be in ordinal order — which
//     decides the topic's root message and the event log's replay order.
//  4. THE DIALECT SEAMS BELONG TO THE LOADER. Mentions resolve through BOTH
//     lanes (a label paired with a source id, and a plain display name), and
//     the upload-link rewrite is this import's own invented syntax, which
//     the write path has never heard of and only supplies file ids to.
func TestWriteDrainsANeutralImport(t *testing.T) {
	pool, orgID := testPool(t)
	ctx := context.Background()
	store, err := blob.Open("fs", t.TempDir())
	if err != nil {
		t.Fatalf("blob: %v", err)
	}
	svc := New(pool, store)

	joined := time.Date(2020, 3, 4, 5, 6, 7, 0, time.UTC)
	sent := time.Date(2021, 7, 8, 9, 10, 11, 0, time.UTC)
	blobBody := []byte("neutral bytes")
	// A-GONE's opener WOULD work. Its probe says the bytes are not there, and
	// counting this is how the test proves the write path honours the cheap
	// answer instead of paying for the expensive one — which for a remote
	// source is the difference between a stat and a download.
	lostOpens := 0

	ir := &Import{
		Source: "acme",
		Users: []User{
			{SourceID: "U-ALPHA", Email: "alpha@acme.example", DisplayName: "Alpha Person",
				Role: 40, Active: true, JoinedAt: joined},
			{SourceID: "U-BETA", Email: "beta@acme.example", DisplayName: "Beta Person",
				Role: 40, Active: true, JoinedAt: joined},
		},
		// "general" collides with the bootstrap channel, so the rename lane
		// runs and has to spell the suffix out of Source.
		Channels: []Channel{{SourceID: "C-GEN", Name: "general", Description: "neutral",
			Visibility: 1, CreatedAt: joined}},
		Memberships: []Membership{{ChannelID: "C-GEN", UserID: "U-ALPHA"}},
		Conversations: []Conversation{
			{SourceKey: "D-PAIR", MemberIDs: []string{"U-ALPHA", "U-BETA"}}},
		Attachments: []Attachment{
			{SourceID: "A-OK", Name: "notes.txt", MIME: "text/plain",
				OwnerID: "U-ALPHA", CreatedAt: joined,
				Probe: func() (int64, error) { return int64(len(blobBody)), nil },
				Open: func() (io.ReadSeekCloser, error) {
					return memoryBytes{bytes.NewReader(blobBody)}, nil
				}},
			// A source that says it cannot produce bytes is a counted loss,
			// not a failed import — and not an Open either.
			{SourceID: "A-GONE", Name: "lost.bin", CreatedAt: joined,
				Probe: func() (int64, error) { return 0, fmt.Errorf("not there") },
				Open: func() (io.ReadSeekCloser, error) {
					lostOpens++
					return memoryBytes{bytes.NewReader([]byte("should never be read"))}, nil
				}},
		},
		AttachmentRefs: []AttachmentRef{{AttachmentID: "A-OK", MessageID: "M-SECOND"}},
		// Appended BACKWARDS on purpose.
		Messages: []Message{
			{SourceID: "M-SECOND", Ordinal: 2, AuthorID: "U-BETA", SentAt: sent.Add(time.Minute),
				Container: Container{Kind: ContainerChannel, Key: "C-GEN"},
				Thread:    &Thread{Key: "t1", SourceID: "T-LAUNCH", Title: "launch"},
				// This label is nobody's display name; only the source-id
				// lane can resolve it.
				Body:               "[notes]({{file:A-OK}}) cc @**alpha-handle**",
				MentionsBySourceID: map[string]string{"alpha-handle": "U-ALPHA"}},
			{SourceID: "M-FIRST", Ordinal: 1, AuthorID: "U-ALPHA", SentAt: sent,
				Container: Container{Kind: ContainerChannel, Key: "C-GEN"},
				Thread:    &Thread{Key: "t1", SourceID: "T-LAUNCH", Title: "launch"},
				Body:      "kickoff, thanks @**Beta Person**"},
			{SourceID: "M-DM", Ordinal: 3, AuthorID: "U-ALPHA", SentAt: sent.Add(2 * time.Minute),
				Container: Container{Kind: ContainerDirect, Key: "D-PAIR"},
				Body:      "just between us"},
		},
		Reactions: []Reaction{{UserID: "U-BETA", MessageID: "M-FIRST", Emoji: "tada"}},
		ReadState: []ReadState{{UserID: "U-BETA", MessageID: "M-FIRST", Read: true}},
		// An upload dialect the write path has never seen.
		RewriteAttachmentLinks: func(body string, fileIDs map[string]int64) (string, bool) {
			changed := false
			for attachmentID, fileID := range fileIDs {
				token := "{{file:" + attachmentID + "}}"
				if strings.Contains(body, token) {
					body = strings.ReplaceAll(body, token, fmt.Sprintf("/api/v1/files/%d", fileID))
					changed = true
				}
			}
			return body, changed
		},
	}

	var rep Report
	rep.RenamedChannels = map[string]string{}
	rep.RenamedGroups = map[string]string{}
	if err := db.WithTx(ctx, pool, func(tx pgx.Tx) error {
		return svc.write(ctx, tx, orgID, ir, &rep)
	}); err != nil {
		t.Fatalf("write neutral import: %v", err)
	}

	if rep.Users != 2 || rep.Channels != 1 || rep.Subscriptions != 1 ||
		rep.Threads != 1 || rep.Messages != 2 || rep.DMConversations != 1 ||
		rep.DMMessages != 1 || rep.Reactions != 1 || rep.Watermarks != 1 {
		t.Fatalf("neutral import report off: %+v", rep)
	}
	// One attachment landed, one was a counted loss — and the loss was
	// decided by the cheap probe alone, never by opening the bytes.
	if rep.Attachments != 1 || rep.AttachmentFilesMissing != 1 {
		t.Fatalf("attachments = %d, missing = %d, want 1/1 (%+v)",
			rep.Attachments, rep.AttachmentFilesMissing, rep)
	}
	if lostOpens != 0 {
		t.Fatalf("the lane opened an attachment its probe had already refused (%d time(s))", lostOpens)
	}

	// (2) The token is data, including where an operator reads it.
	if got := rep.RenamedChannels["general"]; got != "general-acme1" {
		t.Fatalf("collision rename = %q, want general-acme1 — the suffix must come "+
			"from Import.Source, not from a constant in the write path", got)
	}
	var wrongSystem int
	if err := pool.QueryRow(ctx, `
		SELECT (SELECT count(*) FROM user_account WHERE org_id = $1
		          AND origin_system IS NOT NULL AND origin_system <> 'acme')
		     + (SELECT count(*) FROM channel WHERE org_id = $1
		          AND origin_system IS NOT NULL AND origin_system <> 'acme')
		     + (SELECT count(*) FROM thread WHERE org_id = $1
		          AND origin_system IS NOT NULL AND origin_system <> 'acme')
		     + (SELECT count(*) FROM message WHERE org_id = $1
		          AND origin_system IS NOT NULL AND origin_system <> 'acme')
		     + (SELECT count(*) FROM file WHERE org_id = $1
		          AND origin_system IS NOT NULL AND origin_system <> 'acme')`,
		orgID).Scan(&wrongSystem); err != nil {
		t.Fatalf("origin_system census: %v", err)
	}
	if wrongSystem != 0 {
		t.Fatalf("%d imported row(s) carry an origin_system other than the IR's", wrongSystem)
	}

	// (1) Opaque ids survive verbatim. A stray %d anywhere in the lane would
	// have written %!d(string=U-ALPHA) into these columns.
	var alphaID, betaID, firstID, secondID, fileID, threadID int64
	if err := pool.QueryRow(ctx, `
		SELECT
		  (SELECT id FROM user_account WHERE org_id = $1 AND origin_id = 'U-ALPHA'),
		  (SELECT id FROM user_account WHERE org_id = $1 AND origin_id = 'U-BETA'),
		  (SELECT id FROM message      WHERE org_id = $1 AND origin_id = 'M-FIRST'),
		  (SELECT id FROM message      WHERE org_id = $1 AND origin_id = 'M-SECOND'),
		  (SELECT id FROM file         WHERE org_id = $1 AND origin_id = 'A-OK'),
		  (SELECT id FROM thread       WHERE org_id = $1 AND origin_id = 'T-LAUNCH')`,
		orgID).Scan(&alphaID, &betaID, &firstID, &secondID, &fileID, &threadID); err != nil {
		t.Fatalf("opaque source ids did not round-trip into origin_id: %v", err)
	}
	// The loss carries no row at all.
	var lost int
	_ = pool.QueryRow(ctx,
		`SELECT count(*) FROM file WHERE org_id = $1 AND origin_id = 'A-GONE'`,
		orgID).Scan(&lost)
	if lost != 0 {
		t.Fatalf("an attachment whose bytes were unavailable still wrote %d file row(s)", lost)
	}

	// (3) Ordinal order, not emission order. M-FIRST was appended SECOND, so
	// a write path that trusted the slice would root the thread on M-SECOND
	// and replay history backwards.
	var rootID int64
	var count int
	if err := pool.QueryRow(ctx,
		`SELECT root_message_id, message_count FROM thread WHERE id = $1`,
		threadID).Scan(&rootID, &count); err != nil {
		t.Fatalf("thread counters: %v", err)
	}
	if rootID != firstID || count != 2 {
		t.Fatalf("thread root = %d (want %d, the ordinal-1 message), message_count = %d, want 2",
			rootID, firstID, count)
	}
	var order []string
	rows, err := pool.Query(ctx, `
		SELECT m.origin_id FROM event_log e
		JOIN message m ON m.id = e.entity_id AND m.org_id = e.org_id
		WHERE e.org_id = $1 AND e.actor_kind = 4 AND e.verb = 'message.created'
		ORDER BY e.id`, orgID)
	if err != nil {
		t.Fatalf("event order: %v", err)
	}
	for rows.Next() {
		var oid string
		if err := rows.Scan(&oid); err != nil {
			rows.Close()
			t.Fatalf("scan event order: %v", err)
		}
		order = append(order, oid)
	}
	rows.Close()
	if strings.Join(order, ",") != "M-FIRST,M-SECOND,M-DM" {
		t.Fatalf("event replay order = %v, want ordinal order [M-FIRST M-SECOND M-DM]", order)
	}

	// (4) Both mention lanes, and the loader's own upload dialect.
	var secondSrc, secondAST string
	var secondAttach bool
	if err := pool.QueryRow(ctx,
		`SELECT source, ast::text, has_attachment FROM message WHERE id = $1`,
		secondID).Scan(&secondSrc, &secondAST, &secondAttach); err != nil {
		t.Fatalf("second message: %v", err)
	}
	if !strings.Contains(secondSrc, fmt.Sprintf("/api/v1/files/%d", fileID)) || !secondAttach {
		t.Fatalf("the loader's {{file:A-OK}} dialect was not rewritten: has_attachment=%v src=%q",
			secondAttach, secondSrc)
	}
	if !containsMention([]byte(secondAST), alphaID) {
		t.Fatalf("the by-source-id mention lane did not resolve @**alpha-handle** to %d: %s",
			alphaID, secondAST)
	}
	var firstAST string
	_ = pool.QueryRow(ctx, `SELECT ast::text FROM message WHERE id = $1`, firstID).Scan(&firstAST)
	if !containsMention([]byte(firstAST), betaID) {
		t.Fatalf("the display-name mention lane did not resolve @**Beta Person** to %d: %s",
			betaID, firstAST)
	}

	// The opaque opener really did move bytes through the blob seam.
	var storageKey string
	_ = pool.QueryRow(ctx, `SELECT storage_key FROM file WHERE id = $1`, fileID).Scan(&storageKey)
	rc, err := store.Open(ctx, storageKey)
	if err != nil {
		t.Fatalf("blob open: %v", err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if string(got) != string(blobBody) {
		t.Fatalf("blob content = %q, want %q", got, blobBody)
	}

	// The DM lane is neutral too: participants are source ids, and the
	// canonical key is still the sorted WEFT ids the native dm module uses.
	lo, hi := alphaID, betaID
	if hi < lo {
		lo, hi = hi, lo
	}
	var dmKind int16
	if err := pool.QueryRow(ctx,
		`SELECT kind FROM dm_space WHERE org_id = $1 AND dm_key = $2`,
		orgID, fmt.Sprintf("%d:%d", lo, hi)).Scan(&dmKind); err != nil {
		t.Fatalf("neutral dm_space: %v", err)
	}
	if dmKind != 1 {
		t.Fatalf("dm kind = %d, want 1", dmKind)
	}
}

// TestWriteLandsFlatChannelMessagesOnTheRoot pins the three IR affordances a
// second source needs, over a hand-built import so no loader can be the thing
// under test:
//
//  1. Thread.Root routes a channel message to the container's FLAT FEED — the
//     kind=2 root the channel lane already created — and F-15 still holds
//     there: no message_count, no last_activity_at, and above all no
//     root_message_id, which messaging/move.go reads without filtering by
//     kind and would turn into "permanently unmovable".
//  2. A channel reference resolves through the loader's SOURCE-ID lane into an
//     inert channel_ref node, and an unpaired label stays unresolved.
//  3. Losses the loader observed reach the report verbatim.
//
// The re-run half is where the root lane could quietly break: root threads
// carry no provenance of their own, so a resolution that could not find one
// would re-predict every watermark and stop the import being idempotent.
func TestWriteLandsFlatChannelMessagesOnTheRoot(t *testing.T) {
	pool, orgID := testPool(t)
	ctx := context.Background()
	store, err := blob.Open("fs", t.TempDir())
	if err != nil {
		t.Fatalf("blob: %v", err)
	}
	svc := New(pool, store)
	sent := time.Date(2022, 2, 3, 4, 5, 6, 0, time.UTC)

	build := func() *Import {
		return &Import{
			Source: "acme",
			Users: []User{
				{SourceID: "U-1", DisplayName: "One", Role: 40, Active: true},
				{SourceID: "U-2", DisplayName: "Two", Role: 40, Active: true},
			},
			Channels: []Channel{
				{SourceID: "C-FLAT", Name: "flat-room", Visibility: 1},
				{SourceID: "C-OTHER", Name: "other-room", Visibility: 1},
			},
			Memberships: []Membership{
				{ChannelID: "C-FLAT", UserID: "U-1"}, {ChannelID: "C-FLAT", UserID: "U-2"}},
			Messages: []Message{
				{SourceID: "M-FLAT-1", Ordinal: 1, AuthorID: "U-1", SentAt: sent,
					Container: Container{Kind: ContainerChannel, Key: "C-FLAT"},
					Thread:    &Thread{Key: "root:C-FLAT", Root: true},
					Body:      "flat one, see #**other-room** and #**stranger**",
					ChannelRefsBySourceID: map[string]string{
						"other-room": "C-OTHER",
						// "stranger" is deliberately absent: an unpaired label
						// must stay an UNRESOLVED reference, never resolve by
						// name against whatever the org happens to call things.
					}},
				{SourceID: "M-FLAT-2", Ordinal: 2, AuthorID: "U-2", SentAt: sent.Add(time.Minute),
					Container: Container{Kind: ContainerChannel, Key: "C-FLAT"},
					Thread:    &Thread{Key: "root:C-FLAT", Root: true},
					Body:      "flat two"},
				{SourceID: "M-THREAD", Ordinal: 3, AuthorID: "U-1", SentAt: sent.Add(2 * time.Minute),
					Container: Container{Kind: ContainerChannel, Key: "C-FLAT"},
					Thread:    &Thread{Key: "t-1", SourceID: "T-1", Title: ""},
					Body:      "in a thread of its own"},
			},
			// U-2 has read the whole flat feed; the reducer must land the
			// watermark on the highest message IN THAT THREAD (M-FLAT-2), not
			// on M-THREAD, which is in a different one.
			ReadState: []ReadState{
				{UserID: "U-2", MessageID: "M-FLAT-1", Read: true},
				{UserID: "U-2", MessageID: "M-FLAT-2", Read: true},
			},
			Losses: Losses{AttachmentBytesExpired: 2, BroadcastsFlattened: 1, SystemNoticesDropped: 4},
		}
	}

	run := func() Report {
		t.Helper()
		rep := Report{Source: "acme",
			RenamedChannels: map[string]string{}, RenamedGroups: map[string]string{}}
		if err := db.WithTx(ctx, pool, func(tx pgx.Tx) error {
			return svc.write(ctx, tx, orgID, build(), &rep)
		}); err != nil {
			t.Fatalf("write: %v", err)
		}
		return rep
	}
	rep := run()

	// (3) The loader's own accounting survives into the report untouched.
	if rep.AttachmentBytesExpired != 2 || rep.BroadcastsFlattened != 1 ||
		rep.SystemNoticesDropped != 4 {
		t.Fatalf("loader losses did not reach the report: %+v", rep)
	}
	// ONE thread is created — the message that asked for its own. The two
	// flat-feed messages create none, because the root already exists.
	if rep.Threads != 1 || rep.Messages != 3 {
		t.Fatalf("threads = %d messages = %d, want 1/3 (%+v)", rep.Threads, rep.Messages, rep)
	}

	var rootID, otherID, flatChID int64
	if err := pool.QueryRow(ctx, `
		SELECT c.root_thread_id, c.id,
		       (SELECT id FROM channel WHERE org_id = $1 AND origin_id = 'C-OTHER')
		FROM channel c WHERE c.org_id = $1 AND c.origin_id = 'C-FLAT'`,
		orgID).Scan(&rootID, &flatChID, &otherID); err != nil {
		t.Fatalf("channels: %v", err)
	}
	// (1) Both flat messages sit on the channel's kind=2 root; the threaded
	// one does not.
	var flatOnRoot, rootKind int
	if err := pool.QueryRow(ctx, `
		SELECT (SELECT count(*) FROM message WHERE org_id = $1
		          AND origin_id IN ('M-FLAT-1','M-FLAT-2') AND thread_id = $2),
		       (SELECT kind FROM thread WHERE id = $2)`,
		orgID, rootID).Scan(&flatOnRoot, &rootKind); err != nil {
		t.Fatalf("flat feed: %v", err)
	}
	if flatOnRoot != 2 || rootKind != 2 {
		t.Fatalf("flat messages on the kind=2 root = %d (kind %d), want 2 (kind 2)",
			flatOnRoot, rootKind)
	}
	var threadedElsewhere bool
	_ = pool.QueryRow(ctx, `
		SELECT m.thread_id <> $2 AND t.kind = 1
		FROM message m JOIN thread t ON t.id = m.thread_id
		WHERE m.org_id = $1 AND m.origin_id = 'M-THREAD'`, orgID, rootID).Scan(&threadedElsewhere)
	if !threadedElsewhere {
		t.Fatal("a message that named its own thread landed on the channel root")
	}
	// F-15 on the root the flat feed just filled. root_message_id is the
	// dangerous one — a message some thread names as its root can never be
	// moved again, kind unfiltered.
	var count int
	var lastActivity *time.Time
	var rootMsg *int64
	if err := pool.QueryRow(ctx,
		`SELECT message_count, last_activity_at, root_message_id FROM thread WHERE id = $1`,
		rootID).Scan(&count, &lastActivity, &rootMsg); err != nil {
		t.Fatalf("root counters: %v", err)
	}
	if count != 0 || lastActivity != nil || rootMsg != nil {
		t.Fatalf("the flat feed put F-15 counters on a kind=2 root: count=%d activity=%v root_message=%v",
			count, lastActivity, rootMsg)
	}
	// Roots are silent: two messages landed there and no thread.created fired
	// for it. One thread was created, so exactly one event exists.
	if got := importerEventCensus(t, ctx, pool, orgID)["thread.created"]; got != 1 {
		t.Fatalf("thread.created = %d, want 1 (the root is silent)", got)
	}

	// (2) The channel reference resolved through the SOURCE-ID lane into an
	// inert channel_ref node; the unpaired label stayed unresolved.
	var ast, rendered string
	if err := pool.QueryRow(ctx,
		`SELECT ast::text, rendered FROM message WHERE org_id = $1 AND origin_id = 'M-FLAT-1'`,
		orgID).Scan(&ast, &rendered); err != nil {
		t.Fatalf("flat message: %v", err)
	}
	wantResolved := fmt.Sprintf(`<span class="channel-ref" data-channel-id="%d">#other-room</span>`, otherID)
	if !strings.Contains(rendered, wantResolved) {
		t.Fatalf("channel reference did not resolve to channel %d: %s", otherID, rendered)
	}
	if !strings.Contains(rendered, `<span class="channel-ref channel-ref-unresolved">#stranger</span>`) {
		t.Fatalf("an unpaired label must stay unresolved: %s", rendered)
	}
	if strings.Contains(rendered, "<a ") {
		t.Fatalf("a channel reference rendered as a link: %s", rendered)
	}
	if !strings.Contains(ast, `"channel_ref"`) {
		t.Fatalf("no channel_ref node in the stored AST: %s", ast)
	}

	// The watermark landed on the flat feed's OWN thread, at its last message.
	var wmThread, wmMsg, flat2 int64
	if err := pool.QueryRow(ctx, `
		SELECT w.thread_id, w.last_read_message_id,
		       (SELECT id FROM message WHERE org_id = $1 AND origin_id = 'M-FLAT-2')
		FROM thread_read_watermark w
		JOIN user_account u ON u.id = w.user_id
		WHERE u.org_id = $1 AND u.origin_id = 'U-2'`, orgID).Scan(&wmThread, &wmMsg, &flat2); err != nil {
		t.Fatalf("watermark: %v", err)
	}
	if wmThread != rootID || wmMsg != flat2 {
		t.Fatalf("watermark = (thread %d, message %d), want (%d, %d)",
			wmThread, wmMsg, rootID, flat2)
	}
	if rep.Watermarks != 1 {
		t.Fatalf("watermarks = %d, want 1", rep.Watermarks)
	}

	// The RE-RUN. A root thread has no origin_id, so the plan can only find it
	// through the CHANNEL's provenance; without that it would re-predict the
	// watermark it already wrote and the import would stop being idempotent.
	rep2 := run()
	if rep2.Messages != 0 || rep2.Threads != 0 || rep2.Watermarks != 0 {
		t.Fatalf("re-run was not a no-op: %+v", rep2)
	}
	if rep2.AlreadyImported == 0 {
		t.Fatalf("re-run counted nothing as already imported: %+v", rep2)
	}
	var msgs int
	_ = pool.QueryRow(ctx,
		`SELECT count(*) FROM message WHERE org_id = $1 AND origin_system = 'acme'`,
		orgID).Scan(&msgs)
	if msgs != 3 {
		t.Fatalf("after re-run message count = %d, want 3", msgs)
	}
}

// TestWriteRejectsAChannelMessageWithNoThread pins the one invariant the IR
// cannot express in its types: a channel message must name the thread it
// belongs to. Landing it on the channel's ROOT instead would set
// root_message_id on a kind=2 thread, which messaging/move.go reads without
// filtering by kind — the message would be unmovable forever, silently.
func TestWriteRejectsAChannelMessageWithNoThread(t *testing.T) {
	pool, orgID := testPool(t)
	ctx := context.Background()
	store, err := blob.Open("fs", t.TempDir())
	if err != nil {
		t.Fatalf("blob: %v", err)
	}
	svc := New(pool, store)

	ir := &Import{
		Source:   "acme",
		Users:    []User{{SourceID: "U-1", DisplayName: "One", Role: 40, Active: true}},
		Channels: []Channel{{SourceID: "C-1", Name: "neutral-room", Visibility: 1}},
		Messages: []Message{{SourceID: "M-1", Ordinal: 1, AuthorID: "U-1",
			Container: Container{Kind: ContainerChannel, Key: "C-1"}, Body: "no thread"}},
	}
	var rep Report
	rep.RenamedChannels = map[string]string{}
	rep.RenamedGroups = map[string]string{}
	err = db.WithTx(ctx, pool, func(tx pgx.Tx) error {
		return svc.write(ctx, tx, orgID, ir, &rep)
	})
	if err == nil {
		t.Fatal("a channel message with no thread was accepted")
	}
	if !strings.Contains(err.Error(), "channel message without a thread") {
		t.Fatalf("unexpected error %v", err)
	}
	// All-or-nothing: the transaction rolled back, so not even the channel
	// that was inserted before the bad message survives.
	var rows int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM channel WHERE org_id = $1 AND origin_system IS NOT NULL`,
		orgID).Scan(&rows); err != nil {
		t.Fatalf("rollback census: %v", err)
	}
	if rows != 0 {
		t.Fatalf("%d channel row(s) survived a failed import", rows)
	}
}
