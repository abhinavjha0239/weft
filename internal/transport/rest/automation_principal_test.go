package rest

import (
	"context"
	"log/slog"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/migrations"
)

// automationPrincipalID reads an org's automation principal by its origin key
// and insists there is EXACTLY ONE. Two rows would mean the Go seed and the
// migration's SQL disagree about the key — the drift that would leave half the
// cell authoring as an account nobody granted anything.
func automationPrincipalID(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID int64) int64 {
	t.Helper()
	rows, err := pool.Query(ctx, `
		SELECT id, kind FROM user_account
		WHERE org_id = $1 AND origin_system = 'system' AND origin_id = $2
		ORDER BY id`, orgID, identity.AutomationPrincipalOriginID)
	if err != nil {
		t.Fatalf("read principal: %v", err)
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		var kind int16
		if err := rows.Scan(&id, &kind); err != nil {
			t.Fatalf("scan principal: %v", err)
		}
		if kind != 2 {
			t.Fatalf("principal %d in org %d has kind %d, want 2 (agent)", id, orgID, kind)
		}
		ids = append(ids, id)
	}
	if len(ids) != 1 {
		t.Fatalf("org %d has %d automation principals, want exactly 1", orgID, len(ids))
	}
	return ids[0]
}

// maySendIn runs the REAL resolver at a channel's scope chain — the identical
// perms.Require call PostToChannelAsAutomation makes — and returns its answer
// (nil = allowed). Never asserts on rows the resolver does not read.
func maySendIn(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID, userID, channelID int64) error {
	t.Helper()
	svc := perms.New(pool)
	var answer error
	if err := db.WithTx(ctx, pool, func(tx pgx.Tx) error {
		chain, err := svc.ChannelScope(ctx, tx, orgID, channelID)
		if err != nil {
			return err
		}
		answer = svc.Require(ctx, tx,
			auth.Identity{UserID: userID, OrgID: orgID}, perms.VerbSendMessage, chain)
		return nil
	}); err != nil {
		t.Fatalf("resolve send_message: %v", err)
	}
	return answer
}

// countRow runs a scalar count query.
func countRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, what, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", what, err)
	}
	return n
}

// TestAutomationPrincipalSeedAndBackfill pins P-44b's two routes to the same
// state: the SEED (perms.SeedOrg's group + nesting, identity.Bootstrap's
// principal + membership) for orgs created from now on, and migration 0027's
// BACKFILL for every org that already exists.
//
// P-47's lesson is why both are needed and why both are tested: SeedOrg writes
// explicit rows, so the seed helps FUTURE orgs only — for an org that already
// exists, the migration is the ONLY source of these rows, and a missing row
// resolves to DENY (perms.Require's secure default), not to "the default". The
// backfill is invisible to the normal harness, which migrates an EMPTY
// database, so this test synthesises the two pre-upgrade shapes an operator
// really has: an org whose old binary had lazily created the principal, and an
// org that never fired a rule and so has none.
func TestAutomationPrincipalSeedAndBackfill(t *testing.T) {
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

	// Anti-drift: the shipped migration must embed both exported statements
	// verbatim, or this test would exercise something operators never run.
	raw, err := migrations.FS.ReadFile("0027_automation_principal_group.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	if !strings.Contains(string(raw), perms.BackfillAutomationsGroupSQL) {
		t.Fatalf("migration 0027 does not contain perms.BackfillAutomationsGroupSQL verbatim:\n%s", raw)
	}
	if !strings.Contains(string(raw), identity.BackfillAutomationPrincipalSQL) {
		t.Fatalf("migration 0027 does not contain identity.BackfillAutomationPrincipalSQL verbatim:\n%s", raw)
	}

	permsSvc := perms.New(pool)
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Log: slog.Default(),
		Identity:  identity.New(pool, permsSvc),
		Messaging: messaging.New(pool, permsSvc),
	}))
	defer ts.Close()

	type org struct {
		OrgID     int64  `json:"org_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	bootstrap := func(slug string) org {
		t.Helper()
		var o org
		postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
			"org_slug": slug, "email": "a@" + slug + ".test",
			"password": "password123", "full_name": "Alice " + slug,
		}, &o)
		return o
	}

	// --- 1. The SEED path: a fresh org is complete at bootstrap.
	seeded := bootstrap("seeded")
	seededPID := automationPrincipalID(t, ctx, pool, seeded.OrgID)

	autoGroup := groupID(t, ctx, pool, seeded.OrgID, perms.GroupAutomations)
	everyone := groupID(t, ctx, pool, seeded.OrgID, perms.GroupEveryone)
	if n := countRow(t, ctx, pool, "is_system",
		`SELECT count(*) FROM user_group WHERE id = $1 AND is_system`, autoGroup); n != 1 {
		t.Fatalf("role:automations is_system rows = %d, want 1", n)
	}
	if n := countRow(t, ctx, pool, "nesting",
		`SELECT count(*) FROM user_group_subgroup WHERE group_id = $1 AND subgroup_id = $2`,
		everyone, autoGroup); n != 1 {
		t.Fatalf("role:automations ⊂ role:everyone rows = %d, want 1", n)
	}
	if n := countRow(t, ctx, pool, "membership",
		`SELECT count(*) FROM user_group_member WHERE group_id = $1 AND user_id = $2`,
		autoGroup, seededPID); n != 1 {
		t.Fatalf("principal membership rows = %d, want 1", n)
	}
	// The closure is what Require actually reads: the principal must reach BOTH
	// its own group and role:everyone (which holds the org-scope send_message
	// assignment), at the org's CURRENT version.
	if n := countRow(t, ctx, pool, "closure", `
		SELECT count(*) FROM user_group_closure c
		WHERE c.user_id = $1 AND c.group_id = ANY($2)
		  AND c.version = (SELECT version FROM closure_current_version WHERE org_id = $3)`,
		seededPID, []int64{autoGroup, everyone}, seeded.OrgID); n != 2 {
		t.Fatalf("principal closure rows = %d, want 2 (own group + role:everyone)", n)
	}
	if err := maySendIn(t, ctx, pool, seeded.OrgID, seededPID, seeded.ChannelID); err != nil {
		t.Fatalf("seeded org: principal may not send: %v", err)
	}

	// --- 2. Two PRE-UPGRADE shapes, synthesised by stripping what the seed
	// wrote. `lazy` is the org whose old binary had already created the
	// principal on a run; `never` is the org that never fired a rule, so the
	// backfill has to mint the account itself.
	lazy := bootstrap("lazyold")
	never := bootstrap("neverold")
	lazyPID := automationPrincipalID(t, ctx, pool, lazy.OrgID)
	stripSeed := func(orgID int64, dropAccount bool) {
		t.Helper()
		stmts := []string{
			`DELETE FROM user_group_closure c USING user_group g
			 WHERE c.group_id = g.id AND g.org_id = $1
			   AND c.user_id IN (SELECT id FROM user_account
			                     WHERE org_id = $1 AND origin_id = 'automation-principal')`,
			`DELETE FROM user_group_member m USING user_group g
			 WHERE m.group_id = g.id AND g.org_id = $1 AND g.name = 'role:automations'`,
			`DELETE FROM user_group_subgroup s USING user_group g
			 WHERE s.subgroup_id = g.id AND g.org_id = $1 AND g.name = 'role:automations'`,
			`DELETE FROM user_group WHERE org_id = $1 AND name = 'role:automations'`,
		}
		if dropAccount {
			stmts = append(stmts,
				`DELETE FROM user_account WHERE org_id = $1 AND origin_id = 'automation-principal'`)
		}
		for _, q := range stmts {
			if _, err := pool.Exec(ctx, q, orgID); err != nil {
				t.Fatalf("synthesise pre-upgrade org %d: %v", orgID, err)
			}
		}
	}
	stripSeed(lazy.OrgID, false)
	stripSeed(never.OrgID, true)

	// The synthesis is real: no group anywhere, and `never` has no account.
	for _, o := range []int64{lazy.OrgID, never.OrgID} {
		if n := countRow(t, ctx, pool, "stripped group",
			`SELECT count(*) FROM user_group WHERE org_id = $1 AND name = 'role:automations'`,
			o); n != 0 {
			t.Fatalf("org %d still has role:automations after stripping", o)
		}
	}
	if n := countRow(t, ctx, pool, "stripped account",
		`SELECT count(*) FROM user_account WHERE org_id = $1 AND origin_id = 'automation-principal'`,
		never.OrgID); n != 0 {
		t.Fatalf("never-ran org still has a principal account after stripping")
	}

	// FAIL CLOSED, stated positively: an account with no group resolves to
	// DENY. This is the state every existing org is in until 0027 runs, and it
	// is why the backfill is load-bearing rather than belt-and-braces.
	if err := maySendIn(t, ctx, pool, lazy.OrgID, lazyPID, lazy.ChannelID); err == nil {
		t.Fatal("pre-upgrade principal may send with no group — deny-by-default is broken")
	}

	// --- 3. THE UPGRADE: the exact statements migration 0027 runs, over the
	// whole database, in the migration's order.
	if _, err := pool.Exec(ctx, perms.BackfillAutomationsGroupSQL); err != nil {
		t.Fatalf("backfill groups: %v", err)
	}
	if _, err := pool.Exec(ctx, identity.BackfillAutomationPrincipalSQL); err != nil {
		t.Fatalf("backfill principals: %v", err)
	}

	// Both pre-upgrade orgs now resolve exactly as a freshly seeded one — and
	// the never-ran org's principal is the account the BACKFILL minted, which
	// identity.AutomationPrincipal finds rather than duplicating.
	for _, o := range []struct {
		name string
		org  org
	}{{"lazyold", lazy}, {"neverold", never}} {
		pid := automationPrincipalID(t, ctx, pool, o.org.OrgID)
		if err := maySendIn(t, ctx, pool, o.org.OrgID, pid, o.org.ChannelID); err != nil {
			t.Fatalf("%s: backfilled principal may not send: %v", o.name, err)
		}
		var lazyID int64
		if err := db.WithTx(ctx, pool, func(tx pgx.Tx) error {
			var err error
			lazyID, err = identity.AutomationPrincipal(ctx, tx, o.org.OrgID)
			return err
		}); err != nil {
			t.Fatalf("%s: lazy lookup: %v", o.name, err)
		}
		if lazyID != pid {
			t.Fatalf("%s: lazy upsert returned %d, want the backfilled %d — the origin keys disagree",
				o.name, lazyID, pid)
		}
	}
	if lazyPID != automationPrincipalID(t, ctx, pool, lazy.OrgID) {
		t.Fatal("lazyold: the backfill minted a SECOND principal instead of placing the existing one")
	}

	// The already-seeded org is untouched: ON CONFLICT DO NOTHING everywhere.
	if seededPID != automationPrincipalID(t, ctx, pool, seeded.OrgID) {
		t.Fatal("seeded org: the backfill replaced its principal")
	}
	if n := countRow(t, ctx, pool, "seeded nesting",
		`SELECT count(*) FROM user_group_subgroup WHERE group_id = $1 AND subgroup_id = $2`,
		everyone, autoGroup); n != 1 {
		t.Fatalf("seeded org nesting rows after backfill = %d, want 1", n)
	}
	if err := maySendIn(t, ctx, pool, seeded.OrgID, seededPID, seeded.ChannelID); err != nil {
		t.Fatalf("seeded org after backfill: principal may not send: %v", err)
	}
}
