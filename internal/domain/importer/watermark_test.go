package importer

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/platform/blob"
)

// TestWatermarkPredictsTheLandedID pins the read-state reducer across TWO
// imports, which is the only shape in which its two candidate definitions
// disagree.
//
// The watermark is an ID cutoff: everything at or below `last_read_message_id`
// is read. So the faithful choice per (user, thread) is the read message that
// will hold the HIGHEST id — not the one latest in source order. Inside a
// single import those are the same message, because the write path inserts in
// ordinal order and ids ascend with it; that is why the whole existing suite
// cannot tell the two apart. They come apart the moment an import is
// INCREMENTAL: a message imported last time holds a LOW id however late it
// sits in the source's history, and a message imported today holds a high one
// however early it sits.
//
// The fixture is built so that both the report and the row move:
//
//	M-LATE  ordinal 100, imported by run 1 → the LOWEST id in the thread
//	M-EARLY ordinal   1, imported by run 2 → a higher id
//	M-MID   ordinal  50, imported by run 2 → the highest id, and READ
//
// Reducing by ordinal picks M-LATE, whose id already equals the watermark run
// 1 wrote, so the monotone upsert affects nothing: the plan would report
// `read_watermarks: 0` and the row would keep pointing at M-LATE, leaving
// M-MID — which the source says was read — sitting above the line forever.
// Reducing by landed id picks M-MID: one watermark imported, and the row moves.
//
// It runs entirely on a hand-built IR, so nothing here depends on Zulip source
// ids happening to ascend with time. That is the point: Slack's `ts` strings
// have no `>` that means "later", and an importer that leaned on source-id
// order would break silently at the second loader.
func TestWatermarkPredictsTheLandedID(t *testing.T) {
	pool, orgID := testPool(t)
	ctx := context.Background()
	store, err := blob.Open("fs", t.TempDir())
	if err != nil {
		t.Fatalf("blob: %v", err)
	}
	svc := New(pool, store)
	sent := time.Date(2021, 4, 5, 6, 7, 8, 0, time.UTC)

	build := func(msgs []Message, read []ReadState) *Import {
		return &Import{
			Source: "acme",
			Users: []User{{SourceID: "U-1", Email: "one@acme.example",
				DisplayName: "One", Role: 40, Active: true, JoinedAt: sent}},
			Channels: []Channel{{SourceID: "C-1", Name: "wm-room",
				Visibility: 1, CreatedAt: sent}},
			Memberships: []Membership{{ChannelID: "C-1", UserID: "U-1"}},
			Messages:    msgs,
			ReadState:   read,
		}
	}
	msg := func(id string, ordinal int64) Message {
		return Message{SourceID: id, Ordinal: ordinal, AuthorID: "U-1",
			SentAt:    sent.Add(time.Duration(ordinal) * time.Minute),
			Container: Container{Kind: ContainerChannel, Key: "C-1"},
			Thread:    &Thread{Key: "t", SourceID: "T-WM", Title: "watermarks"},
			Body:      id}
	}
	run := func(ir *Import) Report {
		t.Helper()
		var rep Report
		rep.RenamedChannels = map[string]string{}
		rep.RenamedGroups = map[string]string{}
		if err := db.WithTx(ctx, pool, func(tx pgx.Tx) error {
			return svc.write(ctx, tx, orgID, ir, &rep)
		}); err != nil {
			t.Fatalf("write: %v", err)
		}
		return rep
	}
	// predict is what a dry run does: same resolution context, same planner,
	// no writes. Asserting it alongside the write is how this fixture also
	// covers the incremental case for the dry/write equality contract, which
	// the Zulip fixtures cannot reach (they re-import the same export).
	predict := func(ir *Import) Report {
		t.Helper()
		var rep Report
		if err := db.WithTx(ctx, pool, func(tx pgx.Tx) error {
			rc, err := loadResolution(ctx, tx, orgID, ir.Source)
			if err != nil {
				return err
			}
			pl, err := newPlan(ir, rc)
			if err != nil {
				return err
			}
			rep = pl.rep
			return nil
		}); err != nil {
			t.Fatalf("plan: %v", err)
		}
		return rep
	}

	// --- Run 1: only the ordinal-100 message exists, and it is read.
	first := run(build([]Message{msg("M-LATE", 100)},
		[]ReadState{{UserID: "U-1", MessageID: "M-LATE", Read: true}}))
	if first.Watermarks != 1 {
		t.Fatalf("first import watermarks = %d, want 1", first.Watermarks)
	}

	ids := map[string]int64{}
	for _, oid := range []string{"M-LATE"} {
		var id int64
		if err := pool.QueryRow(ctx, `
			SELECT id FROM message WHERE org_id = $1 AND origin_system = 'acme'
			 AND origin_id = $2`, orgID, oid).Scan(&id); err != nil {
			t.Fatalf("message %s: %v", oid, err)
		}
		ids[oid] = id
	}

	// --- Run 2: two EARLIER messages arrive, and one of them is read. It gets
	// the highest id in the thread precisely because it arrived last.
	second := build([]Message{msg("M-EARLY", 1), msg("M-MID", 50), msg("M-LATE", 100)},
		[]ReadState{
			{UserID: "U-1", MessageID: "M-LATE", Read: true},
			{UserID: "U-1", MessageID: "M-MID", Read: true},
			{UserID: "U-1", MessageID: "M-EARLY", Read: false},
		})
	forecast := predict(second)
	rep := run(second)

	for _, oid := range []string{"M-EARLY", "M-MID"} {
		var id int64
		if err := pool.QueryRow(ctx, `
			SELECT id FROM message WHERE org_id = $1 AND origin_system = 'acme'
			 AND origin_id = $2`, orgID, oid).Scan(&id); err != nil {
			t.Fatalf("message %s: %v", oid, err)
		}
		ids[oid] = id
	}
	// The premise the whole test rests on, asserted rather than assumed:
	// source order and id order really are opposed here.
	if !(ids["M-LATE"] < ids["M-EARLY"] && ids["M-EARLY"] < ids["M-MID"]) {
		t.Fatalf("fixture premise broken: ids LATE=%d EARLY=%d MID=%d — the "+
			"ordinal-100 message must hold the LOWEST id for this to measure anything",
			ids["M-LATE"], ids["M-EARLY"], ids["M-MID"])
	}

	if rep.Watermarks != 1 {
		t.Errorf("second import watermarks = %d, want 1: the read message with the "+
			"highest landed id (M-MID) sits above the recorded watermark, so the "+
			"monotone upsert fires", rep.Watermarks)
	}
	if rep.ReadCoarsened != 1 {
		t.Errorf("coarsened = %d, want 1 (M-EARLY is unread below the watermark)",
			rep.ReadCoarsened)
	}
	if forecast.Watermarks != rep.Watermarks || forecast.ReadCoarsened != rep.ReadCoarsened {
		t.Errorf("the dry run mispredicted an INCREMENTAL import: watermarks "+
			"%d/%d coarsened %d/%d (dry/write)",
			forecast.Watermarks, rep.Watermarks, forecast.ReadCoarsened, rep.ReadCoarsened)
	}

	// The row itself. This is the assertion an ordinal reducer cannot pass:
	// it would leave the watermark on M-LATE, and M-MID — flagged read by the
	// source — would read as unread forever.
	var landed int64
	if err := pool.QueryRow(ctx, `
		SELECT w.last_read_message_id FROM thread_read_watermark w
		JOIN thread t ON t.id = w.thread_id
		WHERE t.org_id = $1 AND t.origin_system = 'acme' AND t.origin_id = 'T-WM'`,
		orgID).Scan(&landed); err != nil {
		t.Fatalf("watermark row: %v", err)
	}
	if landed != ids["M-MID"] {
		which := "an id that is none of the three messages"
		for oid, id := range ids {
			if id == landed {
				which = oid
			}
		}
		t.Fatalf("watermark points at %s (id %d), want M-MID (id %d): the cutoff "+
			"must cover every message the source called read", which, landed, ids["M-MID"])
	}
	// And the cutoff really does cover all three, which is the property the
	// id choice exists to protect.
	var stillUnread int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM message m
		JOIN thread_read_watermark w ON w.thread_id = m.thread_id
		WHERE m.org_id = $1 AND m.origin_system = 'acme' AND m.id > w.last_read_message_id`,
		orgID).Scan(&stillUnread); err != nil {
		t.Fatalf("unread census: %v", err)
	}
	if stillUnread != 0 {
		t.Errorf("%d imported message(s) sit above the watermark despite being read", stillUnread)
	}
}
