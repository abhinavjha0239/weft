package perms

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
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
	files, _ := filepath.Glob("../../../migrations/0*.sql")
	sort.Strings(files)
	for _, f := range files {
		sql, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if _, err := pool.Exec(ctx, string(sql)); err != nil {
			t.Fatalf("apply %s: %v", f, err)
		}
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
