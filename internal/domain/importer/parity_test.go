package importer

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/abhinavjha0239/weft/internal/platform/blob"
)

// reportDiff lists every Report bucket on which two reports disagree, by JSON
// key where there is one and by field name otherwise. It walks the struct with
// reflection ON PURPOSE: an enumerated list of buckets would silently stop
// covering a bucket added later, and "the two paths agree on EVERY bucket" is
// the whole claim. DryRun is the one field allowed to differ — it is the flag
// that says which path produced the report.
func reportDiff(a, b Report) []string {
	var out []string
	ta := reflect.TypeOf(a)
	va, vb := reflect.ValueOf(a), reflect.ValueOf(b)
	for i := 0; i < ta.NumField(); i++ {
		f := ta.Field(i)
		if f.Name == "DryRun" {
			continue
		}
		x, y := va.Field(i).Interface(), vb.Field(i).Interface()
		if reflect.DeepEqual(x, y) {
			continue
		}
		name := f.Name
		if tag := f.Tag.Get("json"); tag != "" && tag != "-" {
			name = tag
		}
		out = append(out, fmt.Sprintf("%s: dry=%v write=%v", name, x, y))
	}
	sort.Strings(out)
	return out
}

// assertReportsAgree fails with EVERY divergent bucket at once, not the first.
func assertReportsAgree(t *testing.T, phase string, dry, wet Report) {
	t.Helper()
	if diff := reportDiff(dry, wet); len(diff) > 0 {
		t.Errorf("%s: the dry run and the write disagree on %d bucket(s):", phase, len(diff))
		for _, d := range diff {
			t.Errorf("    %s", d)
		}
	}
}

// TestDryRunPredictsTheWrite is the assertion that did not exist before P-27c:
// for the same export and the same org state, the dry-run report and the write
// report are EQUAL over every bucket. Before this slice the two were
// independent accountings that had drifted — `subscriptions` read 10 dry
// against 3 write, a re-run read full counts with `already_imported: 0` dry
// against all-zeros with 17 write — and nothing in the suite compared them.
//
// The three phases are not repetition. A VIRGIN org is where most buckets
// agree by accident (nothing exists, so "what the source contains" and "what
// the write lands" nearly coincide), which is exactly why a virgin-only pin
// would stay green through a substantively wrong change. The RE-RUN phase is
// the one that matters: it is the state an operator is actually in when they
// ask "what will this import do to my org", and it is where the old dry run
// was not merely imprecise but useless. The LOSSY fixture adds the loss
// buckets, which the old dry branch could not reach at all.
func TestDryRunPredictsTheWrite(t *testing.T) {
	pool, orgID := testPool(t)
	ctx := context.Background()
	dir := writeFixture(t)
	store, err := blob.Open("fs", t.TempDir())
	if err != nil {
		t.Fatalf("blob: %v", err)
	}
	svc := New(pool, store)

	// --- Phase 1: virgin org (bootstrap only). The dry run predicts the
	// FIRST import, collision rename included.
	dry1, err := svc.Run(ctx, orgID, dir, true)
	if err != nil {
		t.Fatalf("dry run 1: %v", err)
	}
	wet1, err := svc.Run(ctx, orgID, dir, false)
	if err != nil {
		t.Fatalf("import 1: %v", err)
	}
	assertReportsAgree(t, "virgin org", dry1, wet1)
	// Decision 1 in the flesh: a bucket means what the write LANDS. Three of
	// the fixture's ten zerver_subscription rows become channel_member rows
	// (one is inactive, six are type-3 huddle rows that are DM participation),
	// and the dry run has to say 3 — the number the write side has always
	// asserted — rather than 10.
	if dry1.Subscriptions != 3 {
		t.Errorf("dry subscriptions = %d, want 3: a bucket counts what the write "+
			"would LAND, not what the export contains", dry1.Subscriptions)
	}
	// The resolution context in the flesh: the bootstrap org already owns
	// #general, so the import renames — and the dry run can say so BEFORE the
	// operator commits to it. The old dry branch touched no database and
	// therefore could never report a rename at all.
	if got := dry1.RenamedChannels["general"]; got != "general-zulip1" {
		t.Errorf("dry rename = %q, want general-zulip1: the dry run reads live "+
			"channel names, so it predicts the collision", got)
	}

	// --- Phase 2: NON-VIRGIN org. Everything is already imported, so the
	// honest answer is "this import will land nothing and recognise 17 rows".
	// This is the case the whole slice exists for: on a virgin org an operator
	// does not need to ask.
	dry2, err := svc.Run(ctx, orgID, dir, true)
	if err != nil {
		t.Fatalf("dry run 2: %v", err)
	}
	wet2, err := svc.Run(ctx, orgID, dir, false)
	if err != nil {
		t.Fatalf("import 2: %v", err)
	}
	assertReportsAgree(t, "re-run", dry2, wet2)
	// Derived, not read off a run: 2 channels + 3 topic threads + 1 custom
	// group + 4 channel messages + 3 DM messages + 3 DM conversations (matched
	// by canonical key, the same derivation the native dm module uses) + 1
	// file = 17. The three humans are NOT in this bucket — they match by email
	// first, so matched_existing_by_email carries them instead.
	if dry2.AlreadyImported != 17 {
		t.Errorf("dry already_imported = %d, want 17 (2 channels + 3 threads + 1 group "+
			"+ 7 messages + 3 conversations + 1 file): the dry run must predict the "+
			"re-run, not re-describe the export", dry2.AlreadyImported)
	}
	if dry2.MatchedExistingByEmail != 3 || dry2.RoleGrantsSkipped != 1 {
		t.Errorf("dry matched=%d role-grants-skipped=%d, want 3/1 — the dry run could "+
			"see neither bucket before it read the org",
			dry2.MatchedExistingByEmail, dry2.RoleGrantsSkipped)
	}
	if dry2.Messages != 0 || dry2.Users != 0 || dry2.Channels != 0 ||
		dry2.Threads != 0 || dry2.Subscriptions != 0 || dry2.DMMessages != 0 ||
		dry2.Watermarks != 0 || dry2.Attachments != 0 || dry2.Reactions != 0 {
		t.Errorf("dry run on a fully imported org still promises new rows: %+v", dry2)
	}

	// --- Phase 3: the loss fixture, on its own org. A bot-authored channel
	// message and two unmappable reactions are what made six buckets disagree
	// at once; StreamMessagesSkipped/ReactionsUnmapped were structurally DEAD
	// in the old dry branch (no code path could increment them).
	pool2, orgID2 := testPool(t)
	lossyDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(lossyDir, "realm.json"), []byte(lossyRealm), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(lossyDir, "messages-000001.json"), []byte(lossyMessages), 0o644); err != nil {
		t.Fatal(err)
	}
	store2, err := blob.Open("fs", t.TempDir())
	if err != nil {
		t.Fatalf("blob: %v", err)
	}
	svc2 := New(pool2, store2)
	dry3, err := svc2.Run(ctx, orgID2, lossyDir, true)
	if err != nil {
		t.Fatalf("lossy dry run: %v", err)
	}
	wet3, err := svc2.Run(ctx, orgID2, lossyDir, false)
	if err != nil {
		t.Fatalf("lossy import: %v", err)
	}
	assertReportsAgree(t, "loss fixture", dry3, wet3)
	if dry3.ChannelMessagesSkipped != 1 || dry3.ReactionsUnmapped != 2 {
		t.Errorf("dry loss buckets = %d skipped / %d unmapped, want 1/2: both were "+
			"unreachable in the old dry branch", dry3.ChannelMessagesSkipped, dry3.ReactionsUnmapped)
	}
}
