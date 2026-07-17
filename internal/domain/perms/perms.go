// Package perms is the permission resolver (ADR-006, red-team F-16): every
// permission is (verb, scope) → group; groups nest; membership resolves
// through the flattened user_group_closure so the hot-path check is ONE
// indexed statement, never a recursive query.
//
// Resolution: the MOST SPECIFIC scope with an assignment for the verb wins
// (item > space/channel > workspace > org); if no scope in the chain has an
// assignment, the answer is DENY (secure default — bootstrap seeds org
// defaults so normal orgs never hit it).
//
// Owns tables: user_group, user_group_member, user_group_subgroup,
// user_group_closure, closure_current_version, closure_rebuild_job,
// permission_assignment, permission_profile*.
package perms

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
	"github.com/abhinavjha0239/weft/internal/platform/metrics"
)

type Service struct {
	pool *pgxpool.Pool
	// rebuildSeconds records the wall-time of the last full-org closure rebuild
	// — the O(org group graph) cost every group edit pays today, the signal the
	// scale-tier incremental maintenance will drive down (S0). Optional (default
	// Nop). Set once at wiring.
	rebuildSeconds metrics.Gauge
}

func New(pool *pgxpool.Pool) *Service {
	s := &Service{pool: pool}
	s.SetMetrics(metrics.Nop())
	return s
}

// SetMetrics wires an observability registry (S0). Optional — the default is
// Nop, so an un-instrumented resolver pays nothing. Call once before use.
func (s *Service) SetMetrics(reg metrics.Registry) {
	s.rebuildSeconds = reg.Gauge("closure_rebuild_seconds")
}

// ScopeType mirrors permission_assignment.scope_type.
type ScopeType int16

const (
	ScopeOrg       ScopeType = 1
	ScopeWorkspace ScopeType = 2
	ScopeChannel   ScopeType = 3
	ScopeSpace     ScopeType = 4
	ScopeItem      ScopeType = 5
)

// ChannelScope builds the scope chain for a channel by resolving its
// containers; org-level channels (NULL workspace) skip the workspace hop.
func (s *Service) ChannelScope(ctx context.Context, tx pgx.Tx, orgID, channelID int64) ([]scopeRef, error) {
	var workspaceID *int64
	err := tx.QueryRow(ctx,
		`SELECT workspace_id FROM channel WHERE id = $1 AND org_id = $2`,
		channelID, orgID).Scan(&workspaceID)
	if err != nil {
		return nil, apperr.NotFound("channel not found")
	}
	chain := []scopeRef{{ScopeChannel, channelID}}
	if workspaceID != nil {
		chain = append(chain, scopeRef{ScopeWorkspace, *workspaceID})
	}
	return append(chain, scopeRef{ScopeOrg, orgID}), nil
}

// OrgScope is the trivial chain.
func OrgScope(orgID int64) []scopeRef { return []scopeRef{{ScopeOrg, orgID}} }

type scopeRef struct {
	Type ScopeType
	ID   int64
}

// Require answers "may actor do verb here" inside the caller's transaction
// (same snapshot as the write it guards). CC-6 hook: capability grants for
// agent principals intersect here when the automation milestone lands.
func (s *Service) Require(ctx context.Context, tx pgx.Tx, actor auth.Identity, verb string, chain []scopeRef) error {
	if len(chain) == 0 {
		return apperr.Forbidden("no scope")
	}
	// One statement: find the most specific assignment for the verb in the
	// chain, then test closure membership of exactly that assignment's group.
	types := make([]int16, len(chain))
	ids := make([]int64, len(chain))
	for i, sc := range chain {
		types[i] = int16(sc.Type)
		ids[i] = sc.ID
	}
	// The version predicate is the S2 fence: readers pin the org's CURRENT
	// closure version, so an in-flight rebuild (filling the next version) is
	// invisible until its atomic pointer flip — never a half-built read,
	// never a block. No fence row (impossible after SeedOrg) reads as no
	// closure: deny, the secure default.
	var allowed bool
	err := tx.QueryRow(ctx, `
		WITH chain AS (
		  SELECT unnest($3::smallint[]) AS scope_type, unnest($4::bigint[]) AS scope_id
		),
		best AS (
		  SELECT pa.group_id
		  FROM permission_assignment pa
		  JOIN chain c ON c.scope_type = pa.scope_type AND c.scope_id = pa.scope_id
		  WHERE pa.org_id = $1 AND pa.verb = $2
		  ORDER BY pa.scope_type DESC
		  LIMIT 1
		)
		SELECT EXISTS (
		  SELECT 1 FROM best b
		  JOIN user_group_closure gc ON gc.group_id = b.group_id AND gc.user_id = $5
		   AND gc.version = (SELECT version FROM closure_current_version WHERE org_id = $1)
		)`,
		actor.OrgID, verb, types, ids, actor.UserID).Scan(&allowed)
	if err != nil {
		return apperr.Internal("permission check", err)
	}
	if !allowed {
		return apperr.Forbidden("missing permission: " + verb)
	}
	return nil
}

// HoldersAt answers "who currently holds verb here?" — the read-side mirror
// of Require. It resolves the winning assignment exactly as Require does (the
// most specific scope in the chain with an assignment for the verb), then
// expands that group to its LIVE HUMAN members through the closure. This is
// how a fan-out addresses "whoever may administer this thing" (e.g. P-25
// automation-failure alerts: whoever can edit the rule hears it broke).
//
// No assignment anywhere in the chain → no holders (nil, nil): deny-by-default
// has no one to notify, never an error. Agent principals (kind 2) and
// deactivated accounts are excluded; the result is ordered by user_id.
func (s *Service) HoldersAt(ctx context.Context, tx pgx.Tx, orgID int64, verb string, chain []scopeRef) ([]int64, error) {
	if len(chain) == 0 {
		return nil, nil
	}
	types := make([]int16, len(chain))
	ids := make([]int64, len(chain))
	for i, sc := range chain {
		types[i] = int16(sc.Type)
		ids[i] = sc.ID
	}
	// The winning assignment's group — the same VALUES join + scope_type DESC
	// tiebreak Require uses.
	var groupID int64
	err := tx.QueryRow(ctx, `
		WITH chain AS (
		  SELECT unnest($3::smallint[]) AS scope_type, unnest($4::bigint[]) AS scope_id
		)
		SELECT pa.group_id
		FROM permission_assignment pa
		JOIN chain c ON c.scope_type = pa.scope_type AND c.scope_id = pa.scope_id
		WHERE pa.org_id = $1 AND pa.verb = $2
		ORDER BY pa.scope_type DESC
		LIMIT 1`,
		orgID, verb, types, ids).Scan(&groupID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, apperr.Internal("resolve holders assignment", err)
	}
	// Same S2 fence as Require: expand through the CURRENT closure version
	// only. Liveness (deactivated_at, kind) stays a LIVE user_account join —
	// user state is not versioned, so holders reflect deactivations
	// immediately regardless of closure version.
	rows, err := tx.Query(ctx, `
		SELECT c.user_id
		FROM user_group_closure c
		JOIN user_account u ON u.id = c.user_id
		  AND u.deactivated_at IS NULL AND u.kind = 1
		WHERE c.group_id = $1
		  AND c.version = (SELECT version FROM closure_current_version WHERE org_id = $2)
		ORDER BY c.user_id`, groupID, orgID)
	if err != nil {
		return nil, apperr.Internal("expand holders", err)
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var uid int64
		if err := rows.Scan(&uid); err != nil {
			return nil, apperr.Internal("scan holder", err)
		}
		out = append(out, uid)
	}
	if err := rows.Err(); err != nil {
		return nil, apperr.Internal("expand holders", err)
	}
	return out, nil
}

// SeedOrg creates the system role groups (nested), places the owner, seeds
// default org-scope assignments, and builds the closure. Runs in the
// bootstrap transaction.
func (s *Service) SeedOrg(ctx context.Context, tx pgx.Tx, orgID, ownerUserID int64) error {
	names := []string{GroupEveryone, GroupMembers, GroupModerators, GroupAdmins, GroupOwners}
	ids := map[string]int64{}
	for _, n := range names {
		var id int64
		if err := tx.QueryRow(ctx, `
			INSERT INTO user_group (org_id, name, is_system) VALUES ($1, $2, true)
			RETURNING id`, orgID, n).Scan(&id); err != nil {
			return apperr.Internal("create system group", err)
		}
		ids[n] = id
	}
	// owners ⊂ admins ⊂ moderators ⊂ members ⊂ everyone
	nesting := [][2]string{
		{GroupAdmins, GroupOwners}, {GroupModerators, GroupAdmins},
		{GroupMembers, GroupModerators}, {GroupEveryone, GroupMembers},
	}
	for _, n := range nesting {
		if _, err := tx.Exec(ctx,
			`INSERT INTO user_group_subgroup (group_id, subgroup_id) VALUES ($1, $2)`,
			ids[n[0]], ids[n[1]]); err != nil {
			return apperr.Internal("nest system groups", err)
		}
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO user_group_member (group_id, user_id) VALUES ($1, $2)`,
		ids[GroupOwners], ownerUserID); err != nil {
		return apperr.Internal("add owner", err)
	}
	for verb, group := range defaultAssignments {
		if _, err := tx.Exec(ctx, `
			INSERT INTO permission_assignment (org_id, verb, scope_type, scope_id, group_id)
			VALUES ($1, $2, $3, $4, $5)`,
			orgID, verb, ScopeOrg, orgID, ids[group]); err != nil {
			return apperr.Internal("seed assignment", err)
		}
	}
	return s.RebuildClosure(ctx, tx, orgID)
}

// AddUserToGroup adds membership and maintains the closure INCREMENTALLY
// (S2): the new member's rows for the group and its ancestors — O(the org's
// group graph), never O(org membership). The delta lands in the CURRENT
// closure version under the org closure lock (closure.go), so it can never be
// lost to a concurrent full rebuild's version flip. Foreign and absent groups
// answer an oracle-free 404 (cell invariant).
func (s *Service) AddUserToGroup(ctx context.Context, tx pgx.Tx, orgID, groupID, userID int64) error {
	if err := requireOrgGroup(ctx, tx, orgID, groupID); err != nil {
		return err
	}
	version, err := lockOrgClosure(ctx, tx, orgID)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO user_group_member (group_id, user_id) VALUES ($1, $2)
		ON CONFLICT DO NOTHING`, groupID, userID); err != nil {
		return apperr.Internal("add group member", err)
	}
	return addMemberClosure(ctx, tx, orgID, groupID, userID, version)
}

// RebuildClosure recomputes the org's flattened closure with a recursive CTE
// — the FULL recompute, reserved for bulk graph rewrites (org seeding, the
// import queue's jobs). Group edits do NOT call this anymore; they patch
// incrementally (S2, closure.go).
//
// Version fence: the recompute fills version current+1 while readers keep
// answering from current, then the pointer flip + old-version prune commit
// atomically with the caller's transaction — a reader never sees a half-built
// closure and never blocks, however long the rebuild runs. The org closure
// lock (held to commit) parks concurrent group edits so their deltas apply
// after the flip, to the version that survives. Keeps the exact table and
// call sites the docs/SCHEMA.md contract pins.
func (s *Service) RebuildClosure(ctx context.Context, tx pgx.Tx, orgID int64) error {
	// Record the wall-time (S0): a full-org rebuild is O(org group graph) —
	// the cost the version fence moves OFF the request path.
	start := time.Now()
	defer func() { s.rebuildSeconds.Set(time.Since(start).Seconds()) }()
	current, err := lockOrgClosure(ctx, tx, orgID)
	if err != nil {
		return err
	}
	next := current + 1
	if _, err := tx.Exec(ctx, `
		WITH RECURSIVE reach (group_id, via_group) AS (
		  SELECT id, id FROM user_group WHERE org_id = $1
		  UNION
		  SELECT r.group_id, s.subgroup_id
		  FROM reach r
		  JOIN user_group_subgroup s ON s.group_id = r.via_group
		)
		INSERT INTO user_group_closure (group_id, user_id, version)
		SELECT DISTINCT r.group_id, m.user_id, $2::bigint
		FROM reach r
		JOIN user_group_member m ON m.group_id = r.via_group`, orgID, next); err != nil {
		return apperr.Internal("rebuild closure", err)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE closure_current_version SET version = $2 WHERE org_id = $1`,
		orgID, next); err != nil {
		return apperr.Internal("flip closure version", err)
	}
	if _, err := tx.Exec(ctx, `
		DELETE FROM user_group_closure gc
		USING user_group g
		WHERE gc.group_id = g.id AND g.org_id = $1 AND gc.version <> $2`,
		orgID, next); err != nil {
		return apperr.Internal("prune closure versions", err)
	}
	return nil
}

// SystemGroupID looks up a seeded role group.
func (s *Service) SystemGroupID(ctx context.Context, tx pgx.Tx, orgID int64, name string) (int64, error) {
	var id int64
	if err := tx.QueryRow(ctx,
		`SELECT id FROM user_group WHERE org_id = $1 AND name = $2`,
		orgID, name).Scan(&id); err != nil {
		return 0, apperr.Internal(fmt.Sprintf("lookup group %s", name), err)
	}
	return id, nil
}

// Assign sets (verb, scope) → group, replacing any existing assignment.
func (s *Service) Assign(ctx context.Context, tx pgx.Tx, orgID int64, verb string, scope scopeRef, groupID int64) error {
	if _, err := tx.Exec(ctx, `
		INSERT INTO permission_assignment (org_id, verb, scope_type, scope_id, group_id)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (org_id, verb, scope_type, scope_id)
		DO UPDATE SET group_id = EXCLUDED.group_id`,
		orgID, verb, scope.Type, scope.ID, groupID); err != nil {
		return apperr.Internal("assign permission", err)
	}
	return nil
}

// ChannelRef and OrgRef build scope refs for Assign callers.
func ChannelRef(id int64) scopeRef { return scopeRef{ScopeChannel, id} }
func OrgRef(id int64) scopeRef     { return scopeRef{ScopeOrg, id} }
