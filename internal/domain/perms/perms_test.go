package perms

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
	"github.com/abhinavjha0239/weft/migrations"
)

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping integration test")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(ctx, `DROP SCHEMA public CASCADE; CREATE SCHEMA public`); err != nil {
		t.Fatalf("reset: %v", err)
	}
	if err := db.Migrate(ctx, pool, migrations.FS); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return pool
}

type fixture struct {
	orgID, wsID, channelID int64
	owner, outsider        auth.Identity
	svc                    *Service
}

func setup(t *testing.T, pool *pgxpool.Pool) fixture {
	t.Helper()
	ctx := context.Background()
	f := fixture{svc: New(pool)}
	err := db.WithTx(ctx, pool, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx,
			`INSERT INTO org (name, slug) VALUES ('T', 't') RETURNING id`).Scan(&f.orgID); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx,
			`INSERT INTO workspace (org_id, name, slug) VALUES ($1,'W','w') RETURNING id`,
			f.orgID).Scan(&f.wsID); err != nil {
			return err
		}
		var ownerID, outsiderID int64
		if err := tx.QueryRow(ctx, `
			INSERT INTO user_account (org_id, kind, email, full_name)
			VALUES ($1,1,'o@t.test','Owner') RETURNING id`, f.orgID).Scan(&ownerID); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `
			INSERT INTO user_account (org_id, kind, email, full_name)
			VALUES ($1,1,'x@t.test','Outsider') RETURNING id`, f.orgID).Scan(&outsiderID); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `
			INSERT INTO channel (org_id, workspace_id, name) VALUES ($1,$2,'general')
			RETURNING id`, f.orgID, f.wsID).Scan(&f.channelID); err != nil {
			return err
		}
		f.owner = auth.Identity{UserID: ownerID, OrgID: f.orgID}
		f.outsider = auth.Identity{UserID: outsiderID, OrgID: f.orgID}
		return f.svc.SeedOrg(ctx, tx, f.orgID, ownerID)
	})
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	return f
}

func (f fixture) require(t *testing.T, pool *pgxpool.Pool, actor auth.Identity, verb string) error {
	t.Helper()
	ctx := context.Background()
	var out error
	err := db.WithTx(ctx, pool, func(tx pgx.Tx) error {
		chain, err := f.svc.ChannelScope(ctx, tx, f.orgID, f.channelID)
		if err != nil {
			return err
		}
		out = f.svc.Require(ctx, tx, actor, verb, chain)
		return nil
	})
	if err != nil {
		t.Fatalf("tx: %v", err)
	}
	return out
}

// TestResolverCore: owner allowed transitively (owners⊂…⊂members closure);
// non-member denied; allowed after joining the members group; unknown verb
// denied by default.
func TestResolverCore(t *testing.T) {
	pool := testPool(t)
	f := setup(t, pool)
	ctx := context.Background()

	if err := f.require(t, pool, f.owner, VerbSendMessage); err != nil {
		t.Fatalf("owner should send via nested closure: %v", err)
	}
	if err := f.require(t, pool, f.outsider, VerbSendMessage); apperr.KindOf(err) != apperr.KindForbidden {
		t.Fatalf("outsider must be forbidden, got %v", err)
	}
	// Join members → allowed.
	err := db.WithTx(ctx, pool, func(tx pgx.Tx) error {
		gid, err := f.svc.SystemGroupID(ctx, tx, f.orgID, GroupMembers)
		if err != nil {
			return err
		}
		return f.svc.AddUserToGroup(ctx, tx, f.orgID, gid, f.outsider.UserID)
	})
	if err != nil {
		t.Fatalf("add to members: %v", err)
	}
	if err := f.require(t, pool, f.outsider, VerbSendMessage); err != nil {
		t.Fatalf("member should send: %v", err)
	}
	// Secure default: a verb with no assignment anywhere is denied even for
	// the owner.
	if err := f.require(t, pool, f.owner, "verb_that_does_not_exist"); apperr.KindOf(err) != apperr.KindForbidden {
		t.Fatalf("unassigned verb must deny, got %v", err)
	}
}

// TestMostSpecificWins: a channel-scope assignment overrides the org default
// entirely — members lose send in this channel, admins keep it.
func TestMostSpecificWins(t *testing.T) {
	pool := testPool(t)
	f := setup(t, pool)
	ctx := context.Background()

	// Promote a member.
	member := f.outsider
	err := db.WithTx(ctx, pool, func(tx pgx.Tx) error {
		gid, err := f.svc.SystemGroupID(ctx, tx, f.orgID, GroupMembers)
		if err != nil {
			return err
		}
		return f.svc.AddUserToGroup(ctx, tx, f.orgID, gid, member.UserID)
	})
	if err != nil {
		t.Fatalf("promote: %v", err)
	}
	if err := f.require(t, pool, member, VerbSendMessage); err != nil {
		t.Fatalf("member should send before override: %v", err)
	}

	// Channel override: send_message → admins only (announcement channel).
	err = db.WithTx(ctx, pool, func(tx pgx.Tx) error {
		admins, err := f.svc.SystemGroupID(ctx, tx, f.orgID, GroupAdmins)
		if err != nil {
			return err
		}
		return f.svc.Assign(ctx, tx, f.orgID, VerbSendMessage, ChannelRef(f.channelID), admins)
	})
	if err != nil {
		t.Fatalf("assign override: %v", err)
	}

	if err := f.require(t, pool, member, VerbSendMessage); apperr.KindOf(err) != apperr.KindForbidden {
		t.Fatalf("member must lose send under channel override, got %v", err)
	}
	if err := f.require(t, pool, f.owner, VerbSendMessage); err != nil {
		t.Fatalf("owner (⊂ admins) must keep send: %v", err)
	}
}

// TestHoldersAt: the read-side mirror of Require. The org default for
// manage_org (role:admins — still seeded, though P-47 left it enforcing
// nothing) expands through the closure to admins ∪ owners,
// excluding agent principals (kind 2) and deactivated accounts; a channel
// override of the verb wins over the org default (fewer holders here proves
// it); and a verb with no assignment resolves to no holders (deny-default).
func TestHoldersAt(t *testing.T) {
	pool := testPool(t)
	f := setup(t, pool)
	ctx := context.Background()

	// Populate role:admins with a live human, an agent, and a deactivated
	// human (the owner is already there via owners ⊂ admins).
	var humanAdmin, agentAdmin, deadAdmin int64
	err := db.WithTx(ctx, pool, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `INSERT INTO user_account (org_id, kind, email, full_name)
			VALUES ($1,1,'ha@t.test','Human Admin') RETURNING id`, f.orgID).Scan(&humanAdmin); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `INSERT INTO user_account (org_id, kind, email, full_name)
			VALUES ($1,2,'agent@t.test','Agent') RETURNING id`, f.orgID).Scan(&agentAdmin); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `INSERT INTO user_account (org_id, kind, email, full_name, deactivated_at)
			VALUES ($1,1,'dead@t.test','Dead Admin', now()) RETURNING id`, f.orgID).Scan(&deadAdmin); err != nil {
			return err
		}
		admins, err := f.svc.SystemGroupID(ctx, tx, f.orgID, GroupAdmins)
		if err != nil {
			return err
		}
		for _, uid := range []int64{humanAdmin, agentAdmin, deadAdmin} {
			if _, err := tx.Exec(ctx,
				`INSERT INTO user_group_member (group_id, user_id) VALUES ($1, $2)`, admins, uid); err != nil {
				return err
			}
		}
		return f.svc.RebuildClosure(ctx, tx, f.orgID)
	})
	if err != nil {
		t.Fatalf("seed admins: %v", err)
	}

	holders := func(t *testing.T, verb string, chainFn func(tx pgx.Tx) ([]scopeRef, error)) []int64 {
		t.Helper()
		var out []int64
		if err := db.WithTx(ctx, pool, func(tx pgx.Tx) error {
			chain, err := chainFn(tx)
			if err != nil {
				return err
			}
			out, err = f.svc.HoldersAt(ctx, tx, f.orgID, verb, chain)
			return err
		}); err != nil {
			t.Fatalf("holders: %v", err)
		}
		return out
	}

	orgChain := func(tx pgx.Tx) ([]scopeRef, error) { return OrgScope(f.orgID), nil }
	chanChain := func(tx pgx.Tx) ([]scopeRef, error) { return f.svc.ChannelScope(ctx, tx, f.orgID, f.channelID) }

	// Org default: an admins-seeded verb → admins ∪ owners, live humans only,
	// sorted. Uses manage_auth_providers rather than manage_org, which P-47
	// left unseeded (zero enforcement sites) — that would make this assert
	// pass on an empty set.
	got := holders(t, VerbManageAuthProviders, orgChain)
	want := []int64{f.owner.UserID, humanAdmin}
	if !equalInt64s(got, want) {
		t.Fatalf("manage_auth_providers holders = %v, want %v (owner+admin, agent/deactivated excluded)", got, want)
	}

	// Channel override beats the org default: point administer_channel at
	// owners for this channel. The org default (admins) would return
	// owner+humanAdmin; the override returns owner ALONE — proving the
	// channel-scope assignment won.
	if err := db.WithTx(ctx, pool, func(tx pgx.Tx) error {
		owners, err := f.svc.SystemGroupID(ctx, tx, f.orgID, GroupOwners)
		if err != nil {
			return err
		}
		return f.svc.Assign(ctx, tx, f.orgID, VerbAdministerChannel, ChannelRef(f.channelID), owners)
	}); err != nil {
		t.Fatalf("channel override: %v", err)
	}
	if got := holders(t, VerbAdministerChannel, chanChain); !equalInt64s(got, []int64{f.owner.UserID}) {
		t.Fatalf("administer_channel holders = %v, want [%d] (channel override beat org admins)", got, f.owner.UserID)
	}

	// A verb with no assignment anywhere → no holders (deny-default).
	if got := holders(t, VerbComplianceOfficer, orgChain); len(got) != 0 {
		t.Fatalf("unassigned verb holders = %v, want none", got)
	}
}

func equalInt64s(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
