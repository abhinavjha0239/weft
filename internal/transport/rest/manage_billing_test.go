package rest

import (
	"context"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/migrations"
)

// TestManageBillingUnseeded: P-47's second half. manage_billing was
// registered, seeded to role:owners, and checked in ZERO places — config
// nothing enforces, which the honest-rungs rule forbids. Giving it an
// enforcement site would mean inventing a billing feature, so it is unseeded
// instead, and that is THREE edits, not one:
//
//	(a) no seed for new orgs (perms.defaultAssignments),
//	(b) DELETE the dead grant every existing org already carries (0026 —
//	    without it the unseed reaches only orgs created after the upgrade),
//	(c) the verb STRING survives. PUT /admin/verbs has accepted
//	    "manage_billing" since P-2; making it 400 would be a wire-contract
//	    change, not a cleanup.
//
// Each of the three is pinned separately below, so restoring any one edit
// alone turns exactly its own assertion red.
func TestManageBillingUnseeded(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		cancel()
		t.Fatalf("connect: %v", err)
	}
	defer func() { cancel(); pool.Close() }()
	resetAndMigrate(t, ctx, pool)

	// Anti-drift, as for the backfill: operators must run this exact text.
	raw, err := migrations.FS.ReadFile("0026_manage_org_split.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	if !strings.Contains(string(raw), perms.UnseedManageBillingSQL) {
		t.Fatalf("migration 0026 does not contain perms.UnseedManageBillingSQL verbatim:\n%s", raw)
	}

	ts := permMatrixServer(t, ctx, pool)
	f, ownerTok := newPermFixture(t, ctx, pool, ts.URL, "billing", "alice@billing.test")

	countVerb := func(verb string) int {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM permission_assignment WHERE org_id = $1 AND verb = $2`,
			f.orgID, verb).Scan(&n); err != nil {
			t.Fatalf("count %s: %v", verb, err)
		}
		return n
	}

	// (a) A NEW org gets no manage_billing row at all, while its neighbours in
	// the same seed loop still land.
	if n := countVerb(perms.VerbManageBilling); n != 0 {
		t.Errorf("bootstrapped org has %d manage_billing rows, want 0 (unseeded)", n)
	}
	for _, verb := range []string{perms.VerbSendMessage, perms.VerbAdministerChannel, perms.VerbAddEmoji} {
		if n := countVerb(verb); n != 1 {
			t.Errorf("bootstrapped org has %d %s rows, want 1 (the seed loop still runs)", n, verb)
		}
	}

	// (b) An org that predates the upgrade still carries the dead grant. Plant
	// one, then run the exact statement the migration runs.
	if _, err := pool.Exec(ctx, `
		INSERT INTO permission_assignment (org_id, verb, scope_type, scope_id, group_id)
		SELECT $1, $2, 1, $1, id FROM user_group WHERE org_id = $1 AND name = $3`,
		f.orgID, perms.VerbManageBilling, perms.GroupOwners); err != nil {
		t.Fatalf("plant pre-upgrade grant: %v", err)
	}
	if n := countVerb(perms.VerbManageBilling); n != 1 {
		t.Fatalf("planted grant = %d rows, want 1", n)
	}
	if _, err := pool.Exec(ctx, perms.UnseedManageBillingSQL); err != nil {
		t.Fatalf("unseed: %v", err)
	}
	if n := countVerb(perms.VerbManageBilling); n != 0 {
		t.Errorf("after the migration's DELETE, manage_billing rows = %d, want 0", n)
	}
	// Blast radius: the DELETE is verb-scoped and touched nothing else.
	for _, verb := range []string{perms.VerbSendMessage, perms.VerbAdministerChannel, perms.VerbAddEmoji} {
		if n := countVerb(verb); n != 1 {
			t.Errorf("after the unseed, %s rows = %d, want 1 (the DELETE is verb-scoped)", verb, n)
		}
	}

	// (c) The wire contract holds: the string is still a registry verb, so an
	// operator who assigns it gets the same 200 they got yesterday — and a row
	// back, honestly enforcing nothing until a billing feature lands.
	if code := putJSON(t, ts.URL+"/api/v1/admin/verbs", ownerTok,
		map[string]any{"verb": perms.VerbManageBilling, "group": perms.GroupOwners}); code != http.StatusOK {
		t.Fatalf("PUT /admin/verbs manage_billing = %d, want 200 (unseeding must not 400 a verb we accepted yesterday)", code)
	}
	if n := countVerb(perms.VerbManageBilling); n != 1 {
		t.Errorf("explicit assignment = %d rows, want 1", n)
	}
	if !perms.KnownVerb(perms.VerbManageBilling) {
		t.Error("manage_billing left the registry; that is a wire-contract change")
	}
}
