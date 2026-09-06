package importer

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/abhinavjha0239/weft/internal/platform/blob"
)

// Two source users with NO email, each authoring one message. Zulip always
// supplies emails, which is why the defect below stayed latent here — but the
// write path is shared, and Slack's export routinely omits them.
const emaillessRealm = `{
  "zerver_userprofile": [
    {"id": 11, "delivery_email": "", "email": "", "full_name": "Nomail One", "is_active": true, "is_bot": false, "role": 400, "date_joined": 1546300800},
    {"id": 12, "delivery_email": "", "email": "", "full_name": "Nomail Two", "is_active": true, "is_bot": false, "role": 400, "date_joined": 1546300800}
  ],
  "zerver_stream": [
    {"id": 21, "name": "general", "description": "g", "invite_only": false, "deactivated": false, "date_created": 1546300800}
  ],
  "zerver_recipient": [
    {"id": 31, "type": 2, "type_id": 21}
  ],
  "zerver_subscription": [
    {"id": 41, "user_profile": 11, "recipient": 31, "active": true},
    {"id": 42, "user_profile": 12, "recipient": 31, "active": true}
  ]
}`

const emaillessMessages = `{
  "zerver_message": [
    {"id": 101, "sender": 11, "recipient": 31, "subject": "t", "content": "from one", "date_sent": 1554100000, "edit_history": null},
    {"id": 102, "sender": 12, "recipient": 31, "subject": "t", "content": "from two", "date_sent": 1554100100, "edit_history": null}
  ]
}`

// TestImportEmaillessUsersStayDistinct pins a silent data-corruption defect in
// the SHARED importer write path, found by P-27's pre-flight audit.
//
// The match key was `emailToID[strings.ToLower(u.BestEmail())]` with no guard
// on the empty string. The FIRST emailless user was inserted with `email = ”`
// and cached under `""`; every LATER emailless user then MATCHED it and was
// aliased onto that one account — messages re-attributed, DM canonical keys
// merged — while incrementing NO Report bucket, so the fidelity contract
// ("every source entity lands in exactly one bucket") was violated silently.
// The dry run, which does no matching, counted them individually, so dry and
// write diverged with nothing to show for it.
//
// It survived re-runs too: the existing-account preload selects
// `email IS NOT NULL`, and `”` satisfies that, so the alias was reloaded.
//
// RED (verified): revert the `key != ""` guard and the NULL email →
// "distinct authors = 1, want 2".
func TestImportEmaillessUsersStayDistinct(t *testing.T) {
	pool, orgID := testPool(t)
	ctx := context.Background()

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "realm.json"), []byte(emaillessRealm), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "messages-000001.json"), []byte(emaillessMessages), 0o644); err != nil {
		t.Fatal(err)
	}
	store, err := blob.Open("fs", t.TempDir())
	if err != nil {
		t.Fatalf("blob: %v", err)
	}
	svc := New(pool, store)

	rep, err := svc.Run(ctx, orgID, dir, false)
	if err != nil {
		t.Fatalf("import: %v", err)
	}

	// Two source users → two accounts. Under the defect the second matched
	// the first's '' cache entry and no row was created for it.
	if rep.Users != 2 {
		t.Fatalf("imported users = %d, want 2: two emailless source users must "+
			"not collapse onto one account (%+v)", rep.Users, rep)
	}
	// Neither may be counted as an email match — there was no email to match.
	if rep.MatchedExistingByEmail != 0 {
		t.Fatalf("matched-existing = %d, want 0: an absent email is not a key",
			rep.MatchedExistingByEmail)
	}

	// The load-bearing assertion: the two messages keep DISTINCT authors.
	// This is what "re-attributed" means in practice.
	var authors int
	if err := pool.QueryRow(ctx, `
		SELECT count(DISTINCT author_id) FROM message
		WHERE org_id = $1 AND origin_system IS NOT NULL`, orgID).Scan(&authors); err != nil {
		t.Fatalf("count authors: %v", err)
	}
	if authors != 2 {
		t.Fatalf("distinct authors = %d, want 2: the emailless users were merged, "+
			"so one user's history was re-attributed to the other", authors)
	}

	// Stored as SQL NULL, never '': user_account_email_key is partial on
	// `email IS NOT NULL`, so '' occupies a real unique slot and a second
	// emailless user would collide on it.
	var empties int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM user_account WHERE org_id = $1 AND email = ''`,
		orgID).Scan(&empties); err != nil {
		t.Fatalf("count empty emails: %v", err)
	}
	if empties != 0 {
		t.Fatalf("%d account(s) stored email = '' — must be NULL, or the partial "+
			"unique index turns the second emailless user into a constraint error",
			empties)
	}

	// Re-run is still idempotent: the origin key, not the email, is what
	// makes a second run a no-op.
	rep2, err := svc.Run(ctx, orgID, dir, false)
	if err != nil {
		t.Fatalf("re-import: %v", err)
	}
	if rep2.Users != 0 {
		t.Fatalf("re-run created %d users, want 0 (idempotent by origin id)", rep2.Users)
	}
	var total int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM user_account WHERE org_id = $1 AND origin_system IS NOT NULL`,
		orgID).Scan(&total); err != nil {
		t.Fatalf("count users: %v", err)
	}
	if total != 2 {
		t.Fatalf("imported accounts after re-run = %d, want 2", total)
	}
}
