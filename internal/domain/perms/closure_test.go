package perms

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
	"github.com/abhinavjha0239/weft/internal/platform/metrics"
)

// closureParity is THE S2 correctness pin: the incrementally maintained
// closure (at the org's CURRENT version) must exactly equal a from-scratch
// recursive recompute over user_group_member ∪ user_group_subgroup —
// exhaustive set equality, both directions.
func closureParity(t *testing.T, pool *pgxpool.Pool, orgID int64) {
	t.Helper()
	var extra, missing int
	err := pool.QueryRow(context.Background(), `
		WITH RECURSIVE reach (group_id, via_group) AS (
		  SELECT id, id FROM user_group WHERE org_id = $1
		  UNION
		  SELECT r.group_id, s.subgroup_id
		  FROM reach r JOIN user_group_subgroup s ON s.group_id = r.via_group
		),
		want (group_id, user_id) AS (
		  SELECT DISTINCT r.group_id, m.user_id
		  FROM reach r JOIN user_group_member m ON m.group_id = r.via_group
		),
		got (group_id, user_id) AS (
		  SELECT gc.group_id, gc.user_id
		  FROM user_group_closure gc
		  JOIN user_group g ON g.id = gc.group_id AND g.org_id = $1
		  WHERE gc.version = (SELECT version FROM closure_current_version WHERE org_id = $1)
		)
		SELECT
		  (SELECT count(*) FROM (SELECT * FROM got EXCEPT SELECT * FROM want) x),
		  (SELECT count(*) FROM (SELECT * FROM want EXCEPT SELECT * FROM got) y)`,
		orgID).Scan(&extra, &missing)
	if err != nil {
		t.Fatalf("parity query: %v", err)
	}
	if extra != 0 || missing != 0 {
		t.Fatalf("closure parity broken: %d rows a from-scratch recompute would not produce, %d rows missing", extra, missing)
	}
}

func makeGroup(t *testing.T, pool *pgxpool.Pool, orgID int64, name string) int64 {
	t.Helper()
	var id int64
	if err := pool.QueryRow(context.Background(),
		`INSERT INTO user_group (org_id, name) VALUES ($1, $2) RETURNING id`,
		orgID, name).Scan(&id); err != nil {
		t.Fatalf("create group %s: %v", name, err)
	}
	return id
}

func makeUser(t *testing.T, pool *pgxpool.Pool, orgID int64, email string) int64 {
	t.Helper()
	var id int64
	if err := pool.QueryRow(context.Background(), `
		INSERT INTO user_account (org_id, kind, email, full_name)
		VALUES ($1, 1, $2, 'U') RETURNING id`, orgID, email).Scan(&id); err != nil {
		t.Fatalf("create user %s: %v", email, err)
	}
	return id
}

func hasClosure(t *testing.T, pool *pgxpool.Pool, orgID, groupID, userID int64) bool {
	t.Helper()
	var ok bool
	if err := pool.QueryRow(context.Background(), `
		SELECT EXISTS (SELECT 1 FROM user_group_closure
		 WHERE group_id = $1 AND user_id = $2
		   AND version = (SELECT version FROM closure_current_version WHERE org_id = $3))`,
		groupID, userID, orgID).Scan(&ok); err != nil {
		t.Fatalf("closure probe: %v", err)
	}
	return ok
}

func inTx(t *testing.T, pool *pgxpool.Pool, fn func(tx pgx.Tx) error) error {
	t.Helper()
	return db.WithTx(context.Background(), pool, fn)
}

// TestClosureIncremental: a sequence of membership and nesting edits, each
// maintained as a delta, stays EXACTLY equal to a from-scratch recursive
// recompute — including diamond nesting, where a user reachable via two paths
// must keep the closure row when one path is severed. This is the
// load-bearing correctness pin of the S2 incremental maintenance.
func TestClosureIncremental(t *testing.T) {
	pool := testPool(t)
	f := setup(t, pool)
	ctx := context.Background()
	svc := f.svc

	// The diamond: u1 sits in BOTH squads and both squads nest under
	// company, so u1 reaches company via two paths.
	company := makeGroup(t, pool, f.orgID, "company")
	squadA := makeGroup(t, pool, f.orgID, "squad-a")
	squadB := makeGroup(t, pool, f.orgID, "squad-b")
	u1 := makeUser(t, pool, f.orgID, "u1@t.test")
	u2 := makeUser(t, pool, f.orgID, "u2@t.test")

	step := func(name string, fn func(tx pgx.Tx) error) {
		t.Helper()
		if err := inTx(t, pool, fn); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		closureParity(t, pool, f.orgID)
	}

	step("add u1 to squad-a", func(tx pgx.Tx) error {
		return svc.AddUserToGroup(ctx, tx, f.orgID, squadA, u1)
	})
	if !hasClosure(t, pool, f.orgID, squadA, u1) {
		t.Fatal("u1 must reach squad-a directly")
	}
	step("nest squad-a under company", func(tx pgx.Tx) error {
		return svc.AddSubgroup(ctx, tx, f.orgID, company, squadA)
	})
	if !hasClosure(t, pool, f.orgID, company, u1) {
		t.Fatal("nesting must lift u1 into company")
	}
	step("nest squad-b under company", func(tx pgx.Tx) error {
		return svc.AddSubgroup(ctx, tx, f.orgID, company, squadB)
	})
	step("add u1 to squad-b (diamond: two paths to company)", func(tx pgx.Tx) error {
		return svc.AddUserToGroup(ctx, tx, f.orgID, squadB, u1)
	})

	// THE diamond pin: removing ONE path must not delete the row the other
	// path still justifies.
	step("remove u1 from squad-a", func(tx pgx.Tx) error {
		return svc.RemoveUserFromGroup(ctx, tx, f.orgID, squadA, u1)
	})
	if hasClosure(t, pool, f.orgID, squadA, u1) {
		t.Fatal("u1 must leave squad-a")
	}
	if !hasClosure(t, pool, f.orgID, company, u1) {
		t.Fatal("diamond: u1 still reaches company via squad-b — the closure row must survive")
	}
	// Severing the second path removes the last route.
	step("remove u1 from squad-b", func(tx pgx.Tx) error {
		return svc.RemoveUserFromGroup(ctx, tx, f.orgID, squadB, u1)
	})
	if hasClosure(t, pool, f.orgID, company, u1) {
		t.Fatal("u1 lost the last path to company — the row must go")
	}

	// The nesting-edge variant of the diamond: u1 back in both squads, then
	// sever an EDGE (not a membership) — the other edge keeps the row.
	step("re-add u1 to both squads", func(tx pgx.Tx) error {
		if err := svc.AddUserToGroup(ctx, tx, f.orgID, squadA, u1); err != nil {
			return err
		}
		return svc.AddUserToGroup(ctx, tx, f.orgID, squadB, u1)
	})
	step("unlink squad-a from company", func(tx pgx.Tx) error {
		return svc.RemoveSubgroup(ctx, tx, f.orgID, company, squadA)
	})
	if !hasClosure(t, pool, f.orgID, company, u1) {
		t.Fatal("edge diamond: u1 still reaches company via squad-b")
	}
	step("unlink squad-b from company", func(tx pgx.Tx) error {
		return svc.RemoveSubgroup(ctx, tx, f.orgID, company, squadB)
	})
	if hasClosure(t, pool, f.orgID, company, u1) {
		t.Fatal("both edges gone — company must lose u1")
	}

	// Deep diamond across levels: company ⊃ squad-a ⊃ fireteam, PLUS the
	// shortcut company ⊃ fireteam. Removing the shortcut keeps u2 in company
	// through the two-hop chain.
	fireteam := makeGroup(t, pool, f.orgID, "fireteam")
	step("build deep diamond", func(tx pgx.Tx) error {
		if err := svc.AddSubgroup(ctx, tx, f.orgID, company, squadA); err != nil {
			return err
		}
		if err := svc.AddSubgroup(ctx, tx, f.orgID, squadA, fireteam); err != nil {
			return err
		}
		if err := svc.AddSubgroup(ctx, tx, f.orgID, company, fireteam); err != nil {
			return err
		}
		return svc.AddUserToGroup(ctx, tx, f.orgID, fireteam, u2)
	})
	if !hasClosure(t, pool, f.orgID, company, u2) {
		t.Fatal("u2 must reach company")
	}
	step("remove the company⊃fireteam shortcut", func(tx pgx.Tx) error {
		return svc.RemoveSubgroup(ctx, tx, f.orgID, company, fireteam)
	})
	if !hasClosure(t, pool, f.orgID, company, u2) {
		t.Fatal("deep diamond: u2 still reaches company via squad-a ⊃ fireteam")
	}

	// A subgroup edit lifts EXISTING transitive members (fireteam's whole
	// subtree), not just direct ones.
	step("nest fireteam under squad-b too", func(tx pgx.Tx) error {
		return svc.AddSubgroup(ctx, tx, f.orgID, squadB, fireteam)
	})
	if !hasClosure(t, pool, f.orgID, squadB, u2) {
		t.Fatal("nesting must lift fireteam's members into squad-b")
	}

	// Idempotence: re-adding and re-removing are no-ops that keep parity.
	step("idempotent re-add", func(tx pgx.Tx) error {
		return svc.AddUserToGroup(ctx, tx, f.orgID, fireteam, u2)
	})
	step("idempotent unlink of an absent edge", func(tx pgx.Tx) error {
		return svc.RemoveSubgroup(ctx, tx, f.orgID, company, fireteam)
	})
	step("idempotent remove of a non-member", func(tx pgx.Tx) error {
		return svc.RemoveUserFromGroup(ctx, tx, f.orgID, company, u2)
	})

	// A FULL rebuild mid-sequence (version flip) and further deltas after it
	// keep parity — deltas always land in the version readers see.
	step("full rebuild between deltas", func(tx pgx.Tx) error {
		return svc.RebuildClosure(ctx, tx, f.orgID)
	})
	step("delta after the flip", func(tx pgx.Tx) error {
		return svc.AddUserToGroup(ctx, tx, f.orgID, squadB, u1)
	})
	var versions int
	if err := pool.QueryRow(ctx, `
		SELECT count(DISTINCT gc.version) FROM user_group_closure gc
		JOIN user_group g ON g.id = gc.group_id WHERE g.org_id = $1`,
		f.orgID).Scan(&versions); err != nil {
		t.Fatalf("version count: %v", err)
	}
	if versions != 1 {
		t.Fatalf("stale closure versions survive the flip: %d, want 1", versions)
	}

	// Cycles are rejected (fireteam ⊂ squad-a already, so squad-a ⊂ fireteam
	// would loop); self-nesting too; a foreign group is an oracle-free 404.
	err := inTx(t, pool, func(tx pgx.Tx) error {
		return svc.AddSubgroup(ctx, tx, f.orgID, fireteam, squadA)
	})
	if apperr.KindOf(err) != apperr.KindInvalid {
		t.Fatalf("cycle must be Invalid, got %v", err)
	}
	err = inTx(t, pool, func(tx pgx.Tx) error {
		return svc.AddSubgroup(ctx, tx, f.orgID, company, company)
	})
	if apperr.KindOf(err) != apperr.KindInvalid {
		t.Fatalf("self-nesting must be Invalid, got %v", err)
	}
	err = inTx(t, pool, func(tx pgx.Tx) error {
		return svc.AddUserToGroup(ctx, tx, f.orgID, 999999, u1)
	})
	if apperr.KindOf(err) != apperr.KindNotFound {
		t.Fatalf("absent group must be NotFound, got %v", err)
	}
	closureParity(t, pool, f.orgID)
}

// countingRegistry observes Gauge.Set — RebuildClosure sets the S0
// closure_rebuild_seconds gauge once per FULL rebuild, so the count proves
// group edits stopped paying the O(org) recompute, and the last value IS the
// measured cost of that recompute (the number the incremental join must
// beat).
type countingRegistry struct {
	mu        sync.Mutex
	sets      int
	lastValue float64
}

func (c *countingRegistry) Counter(name string, l ...string) metrics.Counter {
	return metrics.Nop().Counter(name, l...)
}
func (c *countingRegistry) Gauge(string, ...string) metrics.Gauge { return (*countingGauge)(c) }
func (c *countingRegistry) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sets
}
func (c *countingRegistry) last() float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastValue
}

type countingGauge countingRegistry

func (g *countingGauge) Set(v float64, _ ...string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.sets++
	g.lastValue = v
}

// TestClosureScaleFlat: AddUserToGroup is O(group graph), not O(org) — its
// wall-time stays flat as membership grows 10×, and it never triggers the
// full-rebuild gauge. (Reverting the group-edit path to the DELETE+recompute
// rebuild turns both asserts red: the gauge fires and the time scales with
// membership.)
func TestClosureScaleFlat(t *testing.T) {
	pool := testPool(t)
	f := setup(t, pool)
	ctx := context.Background()
	reg := &countingRegistry{}
	f.svc.SetMetrics(reg)

	var membersGID int64
	if err := inTx(t, pool, func(tx pgx.Tx) error {
		var err error
		membersGID, err = f.svc.SystemGroupID(ctx, tx, f.orgID, GroupMembers)
		return err
	}); err != nil {
		t.Fatalf("members group: %v", err)
	}

	// grow bulk-loads the org to n members OUTSIDE the timed window (raw
	// rows + one full rebuild, the import shape).
	grow := func(n int, prefix string) {
		t.Helper()
		if _, err := pool.Exec(ctx, `
			WITH minted AS (
			  INSERT INTO user_account (org_id, kind, email, full_name)
			  SELECT $1, 1, $2 || g || '@t.test', 'M' FROM generate_series(1, $3) g
			  RETURNING id
			)
			INSERT INTO user_group_member (group_id, user_id)
			SELECT $4, id FROM minted`, f.orgID, prefix, n, membersGID); err != nil {
			t.Fatalf("bulk members: %v", err)
		}
		if err := inTx(t, pool, func(tx pgx.Tx) error {
			return f.svc.RebuildClosure(ctx, tx, f.orgID)
		}); err != nil {
			t.Fatalf("setup rebuild: %v", err)
		}
	}
	// timeAdd measures one incremental join (min of 3 fresh users).
	timeAdd := func(prefix string) time.Duration {
		t.Helper()
		best := time.Duration(1<<62 - 1)
		for i := 0; i < 3; i++ {
			uid := makeUser(t, pool, f.orgID, fmt.Sprintf("%s%d@t.test", prefix, i))
			start := time.Now()
			if err := inTx(t, pool, func(tx pgx.Tx) error {
				return f.svc.AddUserToGroup(ctx, tx, f.orgID, membersGID, uid)
			}); err != nil {
				t.Fatalf("timed add: %v", err)
			}
			if d := time.Since(start); d < best {
				best = d
			}
		}
		return best
	}

	grow(200, "s")
	rebuildsBefore := reg.count() // setup rebuilds don't count against the edits
	tSmall := timeAdd("small")
	grow(2000, "l")
	rebuildsMid := reg.count()
	// grow's own rebuild just measured the O(org) recompute at 2200 members
	// on THIS machine — the S0 gauge value the incremental join must beat.
	fullRebuild := time.Duration(reg.last() * float64(time.Second))
	tLarge := timeAdd("large")

	if got := reg.count() - rebuildsMid; got != 0 {
		t.Fatalf("AddUserToGroup ran %d full closure rebuilds at 2200 members, want 0 (incremental delta only)", got)
	}
	if got := rebuildsMid - rebuildsBefore - 1; got != 0 { // -1: grow()'s own setup rebuild
		t.Fatalf("AddUserToGroup ran %d full closure rebuilds at 200 members, want 0", got)
	}
	// Wall-time flatness, self-calibrated against the S0 gauge: the join must
	// cost a small fraction of the full recompute it replaced — an edit path
	// that still pays the O(org) rebuild lands at ~100% of the gauge and goes
	// red, whatever the machine speed. The floor absorbs scheduler noise when
	// the recompute itself is only a few ms.
	limit := fullRebuild / 2
	if floor := 5 * time.Millisecond; limit < floor {
		limit = floor
	}
	if tLarge > limit {
		t.Fatalf("AddUserToGroup wall-time is not flat: %v at 2200 members vs the %v full recompute (limit %v; %v at 200 members)",
			tLarge, fullRebuild, limit, tSmall)
	}
	closureParity(t, pool, f.orgID)
	t.Logf("join wall-time: %v at 200 members, %v at 2200 (full recompute there: %v)", tSmall, tLarge, fullRebuild)
}

// TestClosureVersionFence: while a full rebuild is IN FLIGHT, concurrent
// readers answer from the OLD version without blocking; the flip is atomic;
// and rows in a non-current version are invisible to Require and HoldersAt —
// which is exactly what makes a half-built closure unreadable.
func TestClosureVersionFence(t *testing.T) {
	pool := testPool(t)
	f := setup(t, pool)
	ctx := context.Background()
	svc := f.svc

	currentVersion := func() int64 {
		var v int64
		if err := pool.QueryRow(ctx,
			`SELECT version FROM closure_current_version WHERE org_id = $1`,
			f.orgID).Scan(&v); err != nil {
			t.Fatalf("read version: %v", err)
		}
		return v
	}
	v0 := currentVersion()

	// Open a rebuild and HOLD it un-committed — the in-flight window.
	rebuildTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer rebuildTx.Rollback(ctx)
	if err := svc.RebuildClosure(ctx, rebuildTx, f.orgID); err != nil {
		t.Fatalf("in-flight rebuild: %v", err)
	}

	// A concurrent reader neither blocks nor changes its answer: the owner
	// resolves through the OLD version (the pointer flip is uncommitted).
	readerCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	start := time.Now()
	var readerErr error
	err = db.WithTx(readerCtx, pool, func(tx pgx.Tx) error {
		chain, err := svc.ChannelScope(readerCtx, tx, f.orgID, f.channelID)
		if err != nil {
			return err
		}
		readerErr = svc.Require(readerCtx, tx, f.owner, VerbSendMessage, chain)
		return nil
	})
	elapsed := time.Since(start)
	cancel()
	if err != nil {
		t.Fatalf("reader tx during rebuild: %v (blocked %v?)", err, elapsed)
	}
	if readerErr != nil {
		t.Fatalf("owner must stay allowed via the OLD version mid-rebuild, got %v", readerErr)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("reader blocked behind the rebuild for %v", elapsed)
	}
	if got := currentVersion(); got != v0 {
		t.Fatalf("version flipped before commit: %d, want %d", got, v0)
	}

	// Commit the rebuild: the flip is atomic, the old version is pruned, the
	// answer is unchanged.
	if err := rebuildTx.Commit(ctx); err != nil {
		t.Fatalf("commit rebuild: %v", err)
	}
	if got := currentVersion(); got != v0+1 {
		t.Fatalf("version after flip = %d, want %d", got, v0+1)
	}
	if err := f.require(t, pool, f.owner, VerbSendMessage); err != nil {
		t.Fatalf("owner allowed after flip: %v", err)
	}
	var versions int
	if err := pool.QueryRow(ctx, `
		SELECT count(DISTINCT gc.version) FROM user_group_closure gc
		JOIN user_group g ON g.id = gc.group_id WHERE g.org_id = $1`,
		f.orgID).Scan(&versions); err != nil {
		t.Fatalf("version count: %v", err)
	}
	if versions != 1 {
		t.Fatalf("old closure version not pruned: %d versions live", versions)
	}

	// The half-built scenario the fence exists for: closure rows committed
	// under a NON-current version (an in-flight rebuild's partial fill) must
	// be invisible. The outsider gains an everyone-group row at a bogus
	// version — Require must keep denying and HoldersAt must not list them.
	var everyoneGID, adminsGID int64
	if err := inTx(t, pool, func(tx pgx.Tx) error {
		var err error
		if everyoneGID, err = svc.SystemGroupID(ctx, tx, f.orgID, GroupEveryone); err != nil {
			return err
		}
		adminsGID, err = svc.SystemGroupID(ctx, tx, f.orgID, GroupAdmins)
		return err
	}); err != nil {
		t.Fatalf("group ids: %v", err)
	}
	half := currentVersion() + 7
	if _, err := pool.Exec(ctx, `
		INSERT INTO user_group_closure (group_id, user_id, version)
		VALUES ($1, $3, $4), ($2, $3, $4)`,
		everyoneGID, adminsGID, f.outsider.UserID, half); err != nil {
		t.Fatalf("plant half-built rows: %v", err)
	}
	if err := f.require(t, pool, f.outsider, VerbSendMessage); apperr.KindOf(err) != apperr.KindForbidden {
		t.Fatalf("half-built closure LEAKED: outsider read a non-current version, got %v", err)
	}
	var holders []int64
	if err := inTx(t, pool, func(tx pgx.Tx) error {
		var err error
		holders, err = svc.HoldersAt(ctx, tx, f.orgID, VerbManageOrg, OrgScope(f.orgID))
		return err
	}); err != nil {
		t.Fatalf("holders: %v", err)
	}
	for _, uid := range holders {
		if uid == f.outsider.UserID {
			t.Fatal("half-built closure LEAKED into HoldersAt")
		}
	}
	if _, err := pool.Exec(ctx,
		`DELETE FROM user_group_closure WHERE version = $1`, half); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	closureParity(t, pool, f.orgID)
}

// TestClosureRebuildWorker: the async lane end-to-end. An importer-shaped
// bulk write (raw membership rows + EnqueueRebuild in one tx) leaves the
// closure honestly stale until the worker drains the queue and flips the
// fence; repeat enqueues coalesce into the pending job; a poisoned org
// records a failed job with its reason while the lane keeps settling other
// orgs; and settled jobs never re-run.
func TestClosureRebuildWorker(t *testing.T) {
	pool := testPool(t)
	f := setup(t, pool)
	ctx := context.Background()
	svc := f.svc
	worker := NewRebuildWorker(pool, svc, slog.Default())

	var membersGID int64
	if err := inTx(t, pool, func(tx pgx.Tx) error {
		var err error
		membersGID, err = svc.SystemGroupID(ctx, tx, f.orgID, GroupMembers)
		return err
	}); err != nil {
		t.Fatalf("members group: %v", err)
	}

	// The importer shape: raw group writes bypassing the service, plus the
	// enqueue, atomic in one tx.
	imported := makeUser(t, pool, f.orgID, "imported@t.test")
	if err := inTx(t, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx,
			`INSERT INTO user_group_member (group_id, user_id) VALUES ($1, $2)`,
			membersGID, imported); err != nil {
			return err
		}
		return svc.EnqueueRebuild(ctx, tx, f.orgID)
	}); err != nil {
		t.Fatalf("bulk write + enqueue: %v", err)
	}
	importedActor := auth.Identity{UserID: imported, OrgID: f.orgID}
	if err := f.require(t, pool, importedActor, VerbSendMessage); apperr.KindOf(err) != apperr.KindForbidden {
		t.Fatalf("pre-drain: the bulk membership must not resolve yet (async gap), got %v", err)
	}

	// Coalescing: a second enqueue while one is pending mints nothing.
	if err := inTx(t, pool, func(tx pgx.Tx) error {
		return svc.EnqueueRebuild(ctx, tx, f.orgID)
	}); err != nil {
		t.Fatalf("re-enqueue: %v", err)
	}
	var pending int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM closure_rebuild_job WHERE org_id = $1 AND status = 1`,
		f.orgID).Scan(&pending); err != nil {
		t.Fatalf("pending count: %v", err)
	}
	if pending != 1 {
		t.Fatalf("pending jobs = %d, want 1 (coalesced)", pending)
	}

	// Drain: the rebuild lands, the fence flips, membership resolves.
	if n, err := worker.RunOnce(ctx); err != nil || n != 1 {
		t.Fatalf("drain = %d jobs (%v), want 1", n, err)
	}
	if err := f.require(t, pool, importedActor, VerbSendMessage); err != nil {
		t.Fatalf("post-drain: imported member must resolve, got %v", err)
	}
	closureParity(t, pool, f.orgID)
	var status int16
	var finished *time.Time
	var jobErr string
	if err := pool.QueryRow(ctx, `
		SELECT status, finished_at, error FROM closure_rebuild_job
		WHERE org_id = $1 ORDER BY id DESC LIMIT 1`,
		f.orgID).Scan(&status, &finished, &jobErr); err != nil {
		t.Fatalf("job row: %v", err)
	}
	if status != 2 || finished == nil || jobErr != "" {
		t.Fatalf("job = status %d finished %v err %q, want done", status, finished, jobErr)
	}
	// Settled queue: nothing claimable.
	if n, err := worker.RunOnce(ctx); err != nil || n != 0 {
		t.Fatalf("idle drain = %d (%v), want 0", n, err)
	}

	// Failure isolation: poison org1's NEXT version with a planted row (the
	// rebuild's insert will hit the PK), and enqueue a healthy second org
	// behind it — the lane records the failure with its reason and still
	// settles the healthy org.
	var org2, owner2 int64
	if err := inTx(t, pool, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx,
			`INSERT INTO org (name, slug) VALUES ('T2', 't2') RETURNING id`).Scan(&org2); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `
			INSERT INTO user_account (org_id, kind, email, full_name)
			VALUES ($1, 1, 'o2@t.test', 'Owner Two') RETURNING id`, org2).Scan(&owner2); err != nil {
			return err
		}
		return svc.SeedOrg(ctx, tx, org2, owner2)
	}); err != nil {
		t.Fatalf("second org: %v", err)
	}
	var v1 int64
	if err := pool.QueryRow(ctx,
		`SELECT version FROM closure_current_version WHERE org_id = $1`,
		f.orgID).Scan(&v1); err != nil {
		t.Fatalf("org1 version: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO user_group_closure (group_id, user_id, version)
		VALUES ($1, $2, $3)`, membersGID, imported, v1+1); err != nil {
		t.Fatalf("poison: %v", err)
	}
	if err := inTx(t, pool, func(tx pgx.Tx) error {
		if err := svc.EnqueueRebuild(ctx, tx, f.orgID); err != nil {
			return err
		}
		return svc.EnqueueRebuild(ctx, tx, org2)
	}); err != nil {
		t.Fatalf("enqueue both: %v", err)
	}
	if n, err := worker.RunOnce(ctx); err != nil || n != 2 {
		t.Fatalf("drain with poison = %d (%v), want 2 settled (1 failed + 1 done)", n, err)
	}
	var failStatus int16
	var failErr string
	if err := pool.QueryRow(ctx, `
		SELECT status, error FROM closure_rebuild_job
		WHERE org_id = $1 ORDER BY id DESC LIMIT 1`,
		f.orgID).Scan(&failStatus, &failErr); err != nil {
		t.Fatalf("failed job row: %v", err)
	}
	if failStatus != 3 || failErr == "" {
		t.Fatalf("poisoned job = status %d err %q, want failed with a recorded reason", failStatus, failErr)
	}
	var okStatus int16
	if err := pool.QueryRow(ctx, `
		SELECT status FROM closure_rebuild_job
		WHERE org_id = $1 ORDER BY id DESC LIMIT 1`, org2).Scan(&okStatus); err != nil {
		t.Fatalf("org2 job row: %v", err)
	}
	if okStatus != 2 {
		t.Fatalf("healthy org's job = status %d, want done despite the neighbor's failure", okStatus)
	}
	// The failed attempt rolled back: org1's live closure is untouched and
	// still correct.
	if err := f.require(t, pool, importedActor, VerbSendMessage); err != nil {
		t.Fatalf("failed rebuild must not damage the live closure: %v", err)
	}

	// Recovery: clear the poison, re-enqueue, drain — done.
	if _, err := pool.Exec(ctx,
		`DELETE FROM user_group_closure WHERE version = $1`, v1+1); err != nil {
		t.Fatalf("heal: %v", err)
	}
	if err := inTx(t, pool, func(tx pgx.Tx) error {
		return svc.EnqueueRebuild(ctx, tx, f.orgID)
	}); err != nil {
		t.Fatalf("re-enqueue after heal: %v", err)
	}
	if n, err := worker.RunOnce(ctx); err != nil || n != 1 {
		t.Fatalf("recovery drain = %d (%v), want 1", n, err)
	}
	closureParity(t, pool, f.orgID)
}
