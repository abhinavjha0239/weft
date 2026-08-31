package rest

import (
	"context"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/migrations"
)

// splitVerbs is the seven verbs manage_org became (P-47), in migration order.
var splitVerbs = []string{
	perms.VerbAddEmoji,
	perms.VerbManageChannelFolders,
	perms.VerbManageStorageQuota,
	perms.VerbManageLinkPreviews,
	perms.VerbManageAutomations,
	perms.VerbManageAuthProviders,
	perms.VerbManagePermissions,
}

// groupID resolves a system role group for an org.
func groupID(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID int64, name string) int64 {
	t.Helper()
	var id int64
	if err := pool.QueryRow(ctx,
		`SELECT id FROM user_group WHERE org_id = $1 AND name = $2`, orgID, name).Scan(&id); err != nil {
		t.Fatalf("group %s in org %d: %v", name, orgID, err)
	}
	return id
}

// assignmentGroups reads the org-scope group each of the seven verbs resolves
// to; a verb with no row at all is reported as 0 (which is DENY, not a
// default — see perms.BackfillManageOrgSplit).
func assignmentGroups(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID int64) map[string]int64 {
	t.Helper()
	out := map[string]int64{}
	for _, verb := range splitVerbs {
		var gid int64
		err := pool.QueryRow(ctx, `
			SELECT group_id FROM permission_assignment
			WHERE org_id = $1 AND verb = $2 AND scope_type = 1 AND scope_id = $1`,
			orgID, verb).Scan(&gid)
		if err != nil && err != pgx.ErrNoRows {
			t.Fatalf("read assignment %s: %v", verb, err)
		}
		out[verb] = gid
	}
	return out
}

// TestManageOrgSplitBackfill is the upgrade pin for P-47.
//
// The backfill cannot be observed through the normal harness: resetAndMigrate
// drops the schema and runs every migration against an EMPTY database, so
// 0026 matches zero rows in every other test, and db.Migrate records applied
// files so it cannot be re-run in process. So this test SYNTHESISES a
// pre-upgrade org — bootstrap, retarget manage_org, then delete the new
// verbs' rows, which is exactly the row shape an org created by the old
// binary has — and executes the SHARED const the migration embeds.
//
// Why the backfill is load-bearing rather than belt-and-braces: SeedOrg
// INSERTs an explicit row for every seeded verb and identity.Bootstrap is the
// only production INSERT INTO org, so no row-less org exists; for an org that
// already exists this statement is the ONLY source of the new verbs' rows.
//
// RED/GREEN: skip the ExecuteBackfill call below and the pre-upgrade org's
// matrix goes red on all 24 endpoints (403 where the org's own administrator
// is entitled to a 2xx) while the default org's two passes stay green.
func TestManageOrgSplitBackfill(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		cancel()
		t.Fatalf("connect: %v", err)
	}
	defer func() { cancel(); pool.Close() }()
	resetAndMigrate(t, ctx, pool)

	// Anti-drift: the shipped migration must embed the exported statement
	// verbatim, or this test would be exercising something operators never run.
	raw, err := migrations.FS.ReadFile("0026_manage_org_split.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	if !strings.Contains(string(raw), perms.BackfillManageOrgSplit) {
		t.Fatalf("migration 0026 does not contain perms.BackfillManageOrgSplit verbatim:\n%s", raw)
	}

	ts := permMatrixServer(t, ctx, pool)

	// --- the control: an org created by the NEW binary, seeded by SeedOrg.
	// Nothing about the backfill touches it, so both its passes stay green
	// whether or not the backfill runs.
	def, defOwnerTok := newPermFixture(t, ctx, pool, ts.URL, "seeded", "alice@seeded.test")
	defMemberTok := addChannelMember(t, ctx, pool, def.orgID, def.channelID,
		"bob@seeded.test", "Bob Member", "bobseededtok")
	runPermMatrix(t, ctx, pool, def, defMemberTok, false)
	runPermMatrix(t, ctx, pool, def, defOwnerTok, true)

	// --- the subject: synthesise an org as the OLD binary left it.
	old, oldOwnerTok := newPermFixture(t, ctx, pool, ts.URL, "upgraded", "alice@upgraded.test")
	oldMemberTok := addChannelMember(t, ctx, pool, old.orgID, old.channelID,
		"bob@upgraded.test", "Bob Member", "bobupgradedtok")

	// An operator who had pointed the umbrella at members — the case a
	// backfill that assumed "everyone is on the seeded default" would lose.
	if code := putJSON(t, ts.URL+"/api/v1/admin/verbs", oldOwnerTok,
		map[string]any{"verb": perms.VerbManageOrg, "group": perms.GroupMembers}); code != http.StatusOK {
		t.Fatalf("retarget manage_org = %d, want 200", code)
	}
	if _, err := pool.Exec(ctx,
		`DELETE FROM permission_assignment WHERE org_id = $1 AND verb = ANY($2)`,
		old.orgID, splitVerbs); err != nil {
		t.Fatalf("synthesise pre-upgrade org: %v", err)
	}
	for verb, gid := range assignmentGroups(t, ctx, pool, old.orgID) {
		if gid != 0 {
			t.Fatalf("pre-upgrade org still has a row for %s (group %d)", verb, gid)
		}
	}

	// FAIL-CLOSED: with neither the new verb nor anything else holding a row,
	// the resolver's `best` CTE is empty, EXISTS is false, and Require answers
	// Forbidden — for the org OWNER as much as for a member. Deny is the
	// default; an un-backfilled org is locked out, never wide open.
	for who, tok := range map[string]string{"owner": oldOwnerTok, "member": oldMemberTok} {
		if code := postEmoji(t, ts.URL, tok, "failclosed", pngBytes("fail-closed", 40)); code != http.StatusForbidden {
			t.Fatalf("no-row %s emoji = %d, want 403 (deny-by-default)", who, code)
		}
	}

	// THE UPGRADE: the exact statement migration 0026 runs, over the whole
	// database, exactly as the migration runs it.
	if _, err := pool.Exec(ctx, perms.BackfillManageOrgSplit); err != nil {
		t.Fatalf("backfill: %v", err)
	}

	// The pre-upgrade org's seven verbs now sit where ITS manage_org sat…
	wantOld := groupID(t, ctx, pool, old.orgID, perms.GroupMembers)
	for verb, gid := range assignmentGroups(t, ctx, pool, old.orgID) {
		if gid != wantOld {
			t.Errorf("upgraded org: %s → group %d, want %d (role:members, where its manage_org pointed)",
				verb, gid, wantOld)
		}
	}
	// …and the already-correct org is untouched: ON CONFLICT DO NOTHING can
	// never clobber a row that is already there.
	wantDef := groupID(t, ctx, pool, def.orgID, perms.GroupAdmins)
	for verb, gid := range assignmentGroups(t, ctx, pool, def.orgID) {
		if gid != wantDef {
			t.Errorf("seeded org: %s → group %d, want %d (role:admins, unclobbered)", verb, gid, wantDef)
		}
	}

	// The whole surface answers for the upgraded org's administrator exactly
	// as it did before the split — which for THIS org means a plain member,
	// because that is where its operator had put the umbrella.
	runPermMatrix(t, ctx, pool, old, oldMemberTok, true)
}

// TestManageOrgSplitNarrowing is the pin the dropped umbrella makes possible.
//
// The first draft of P-47 kept manage_org as an `OR manage_org` fallback at
// every gate. That silently broke the point of the slice: Require resolves
// per-verb, so an operator pointing add_emoji at role:owners to NARROW it
// would change nothing while manage_org still passed — PUT /admin/verbs would
// accept a write with no effect, the honest-rungs violation this slice exists
// to remove. Each gate therefore checks its OWN verb and nothing else, and
// these assertions go red the moment anyone reinstates the fallback.
func TestManageOrgSplitNarrowing(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		cancel()
		t.Fatalf("connect: %v", err)
	}
	defer func() { cancel(); pool.Close() }()
	resetAndMigrate(t, ctx, pool)

	ts := permMatrixServer(t, ctx, pool)
	f, ownerTok := newPermFixture(t, ctx, pool, ts.URL, "narrow", "alice@narrow.test")

	rebuild := func() {
		t.Helper()
		if err := db.WithTx(ctx, pool, func(tx pgx.Tx) error {
			return perms.New(pool).RebuildClosure(ctx, tx, f.orgID)
		}); err != nil {
			t.Fatalf("closure: %v", err)
		}
	}
	addToGroup := func(email, group string) {
		t.Helper()
		if _, err := pool.Exec(ctx, `
			INSERT INTO user_group_member (group_id, user_id)
			SELECT g.id, u.id FROM user_group g, user_account u
			WHERE g.org_id = $1 AND g.name = $2 AND u.email = $3`,
			f.orgID, group, email); err != nil {
			t.Fatalf("add %s to %s: %v", email, group, err)
		}
		rebuild()
	}

	// dora: a PLAIN ADMIN — role:admins but not role:owners, so narrowing a
	// verb to owners must move her and not the owner.
	doraTok := addChannelMember(t, ctx, pool, f.orgID, f.channelID,
		"dora@narrow.test", "Dora Admin", "doranarrowtok")
	addToGroup("dora@narrow.test", perms.GroupAdmins)

	newProvider := func(tok, name string) int {
		return postJSONStatus(t, ts.URL+"/api/v1/admin/auth-providers", tok, map[string]any{
			"name": name, "issuer": permIssuer,
			"client_id": "narrow-client", "client_secret": "narrow-secret"})
	}

	// Baseline: on the seeded defaults a plain admin holds both verbs.
	if code := postEmoji(t, ts.URL, doraTok, "before", pngBytes("narrow", 40)); code != http.StatusCreated {
		t.Fatalf("admin emoji before narrowing = %d, want 201", code)
	}
	if code := newProvider(doraTok, "before-idp"); code != http.StatusCreated {
		t.Fatalf("admin sso before narrowing = %d, want 201", code)
	}

	// NARROW add_emoji to owners only.
	if code := putJSON(t, ts.URL+"/api/v1/admin/verbs", ownerTok,
		map[string]any{"verb": perms.VerbAddEmoji, "group": perms.GroupOwners}); code != http.StatusOK {
		t.Fatalf("narrow add_emoji = %d, want 200", code)
	}
	// The plain admin loses emoji…
	if code := postEmoji(t, ts.URL, doraTok, "after", pngBytes("narrow", 40)); code != http.StatusForbidden {
		t.Fatalf("admin emoji after narrowing = %d, want 403", code)
	}
	// …keeps every sibling verb (the split is what makes this separable)…
	if code := newProvider(doraTok, "after-idp"); code != http.StatusCreated {
		t.Fatalf("admin sso after narrowing = %d, want 201 (only add_emoji narrowed)", code)
	}
	// …and the owner still holds the narrowed verb.
	if code := postEmoji(t, ts.URL, ownerTok, "owneremoji", pngBytes("narrow", 40)); code != http.StatusCreated {
		t.Fatalf("owner emoji after narrowing = %d, want 201", code)
	}
	// State, not just status: the refused call minted no row.
	var names []string
	rows, err := pool.Query(ctx, `SELECT name FROM custom_emoji WHERE org_id = $1 ORDER BY name`, f.orgID)
	if err != nil {
		t.Fatalf("list emoji: %v", err)
	}
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatalf("scan: %v", err)
		}
		names = append(names, n)
	}
	rows.Close()
	if strings.Join(names, ",") != "before,owneremoji" {
		t.Fatalf("custom_emoji = %v, want [before owneremoji] (the narrowed call wrote nothing)", names)
	}

	// The umbrella is GONE, stated positively: manage_org is inert, so
	// pointing it at EVERYONE grants nothing. Reinstating an `OR manage_org`
	// fallback anywhere turns this line red.
	if code := putJSON(t, ts.URL+"/api/v1/admin/verbs", ownerTok,
		map[string]any{"verb": perms.VerbManageOrg, "group": perms.GroupEveryone}); code != http.StatusOK {
		t.Fatalf("retarget manage_org = %d, want 200", code)
	}
	if code := postEmoji(t, ts.URL, doraTok, "umbrella", pngBytes("narrow", 40)); code != http.StatusForbidden {
		t.Fatalf("admin emoji with manage_org at everyone = %d, want 403 (no umbrella fallback)", code)
	}

	// RECORDED GAP, proven rather than asserted: none of these gates checks
	// actor.IsGuest(), and guests sit in role:everyone — so retargeting one of
	// the new verbs to everyone, the very move this slice enables, hands it to
	// guests too.
	var guestID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO user_account (org_id, kind, email, full_name, role)
		VALUES ($1, 1, 'guest@narrow.test', 'Gwen Guest', 15) RETURNING id`,
		f.orgID).Scan(&guestID); err != nil {
		t.Fatalf("guest: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO auth_session (user_id, token_hash, expires_at)
		VALUES ($1, encode(sha256('guestnarrowtok'::bytea), 'hex'), now() + interval '1 day')`,
		guestID); err != nil {
		t.Fatalf("guest session: %v", err)
	}
	addToGroup("guest@narrow.test", perms.GroupEveryone)
	if code := postEmoji(t, ts.URL, "guestnarrowtok", "guesty", pngBytes("narrow", 40)); code != http.StatusForbidden {
		t.Fatalf("guest emoji while add_emoji is at owners = %d, want 403", code)
	}
	if code := putJSON(t, ts.URL+"/api/v1/admin/verbs", ownerTok,
		map[string]any{"verb": perms.VerbAddEmoji, "group": perms.GroupEveryone}); code != http.StatusOK {
		t.Fatalf("widen add_emoji = %d, want 200", code)
	}
	if code := postEmoji(t, ts.URL, "guestnarrowtok", "guesty", pngBytes("narrow", 40)); code != http.StatusCreated {
		t.Fatalf("guest emoji at role:everyone = %d, want 201 — the recorded gap", code)
	}
}
