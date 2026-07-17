// Closure maintenance (S2, ADR-006 / red-team F-16): the flattened
// user_group_closure is kept correct by INCREMENTAL deltas on every group
// edit — bounded by the org's group GRAPH (nesting depth × branching), never
// by membership — with the full recursive rebuild reserved for the rare bulk
// recompute (RebuildClosure, and the async queue that drives it).
//
// Versioned rows + a per-org current-version pointer form the fence: writers
// fill or patch a version, readers pin WHERE version = current. All closure
// WRITERS first take the org closure lock (the closure_current_version row,
// FOR UPDATE), which linearizes group-graph writes per org, so a delta either
// commits before a full rebuild takes its snapshot (the rebuild sees it) or
// waits and applies to the flipped version — never lost in a version that is
// about to be discarded. Readers never take the lock: an in-flight rebuild
// degrades to reads of the old version, never to blocking.
package perms

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

// closureAncestors is the upward walk shared by every delta: the edited group
// itself plus every group that transitively contains it. Parameter contract:
// $1 = the anchor group id, $2 = org id (the cell pin — a foreign anchor
// yields an empty walk). Cost is bounded by the group graph, not membership.
const closureAncestors = `
	anc (id) AS (
	    SELECT id FROM user_group WHERE id = $1 AND org_id = $2
	  UNION
	    SELECT s.group_id FROM user_group_subgroup s JOIN anc a ON s.subgroup_id = a.id
	)`

// lockOrgClosure takes the org closure lock and returns the current closure
// version. The upsert self-seeds the fence row for an org's first closure
// write (0022 backfills orgs that predate the fence; new orgs land here via
// SeedOrg's rebuild).
func lockOrgClosure(ctx context.Context, tx pgx.Tx, orgID int64) (int64, error) {
	if _, err := tx.Exec(ctx, `
		INSERT INTO closure_current_version (org_id, version) VALUES ($1, 0)
		ON CONFLICT (org_id) DO NOTHING`, orgID); err != nil {
		return 0, apperr.Internal("seed closure version", err)
	}
	var v int64
	if err := tx.QueryRow(ctx,
		`SELECT version FROM closure_current_version WHERE org_id = $1 FOR UPDATE`,
		orgID).Scan(&v); err != nil {
		return 0, apperr.Internal("lock closure version", err)
	}
	return v, nil
}

// requireOrgGroup pins a group id to the caller's org (cell invariant) before
// any graph walk — foreign and absent answer identically (oracle-free 404).
func requireOrgGroup(ctx context.Context, tx pgx.Tx, orgID, groupID int64) error {
	var ok bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM user_group WHERE id = $1 AND org_id = $2)`,
		groupID, orgID).Scan(&ok); err != nil {
		return apperr.Internal("group lookup", err)
	}
	if !ok {
		return apperr.NotFound("group not found")
	}
	return nil
}

// addMemberClosure patches the closure for a new (group, user) membership:
// the user reaches the group and every ancestor — O(ancestors) rows, however
// large the org. ON CONFLICT absorbs diamond overlaps (rows already reachable
// via another path) and re-adds.
func addMemberClosure(ctx context.Context, tx pgx.Tx, orgID, groupID, userID, version int64) error {
	if _, err := tx.Exec(ctx, `
		WITH RECURSIVE `+closureAncestors+`
		INSERT INTO user_group_closure (group_id, user_id, version)
		SELECT a.id, $3::bigint, $4::bigint FROM anc a
		ON CONFLICT DO NOTHING`,
		groupID, orgID, userID, version); err != nil {
		return apperr.Internal("patch closure (add member)", err)
	}
	return nil
}

// removeMemberClosure deletes the user's rows for the group and its ancestors
// ONLY where no other membership still reaches them: `still` recomputes the
// user's reachable set from scratch (their remaining memberships walked
// upward), so a diamond path — the user reachable via a second route — keeps
// its row. Never a blind delete.
func removeMemberClosure(ctx context.Context, tx pgx.Tx, orgID, groupID, userID, version int64) error {
	if _, err := tx.Exec(ctx, `
		WITH RECURSIVE `+closureAncestors+`,
		still (id) AS (
		    SELECT m.group_id FROM user_group_member m
		    JOIN user_group g ON g.id = m.group_id AND g.org_id = $2
		    WHERE m.user_id = $3
		  UNION
		    SELECT s.group_id FROM user_group_subgroup s JOIN still r ON s.subgroup_id = r.id
		)
		DELETE FROM user_group_closure gc
		WHERE gc.user_id = $3 AND gc.version = $4
		  AND gc.group_id IN (SELECT id FROM anc)
		  AND gc.group_id NOT IN (SELECT id FROM still)`,
		groupID, orgID, userID, version); err != nil {
		return apperr.Internal("patch closure (remove member)", err)
	}
	return nil
}

// RemoveUserFromGroup removes direct membership and patches the closure
// incrementally (O(group graph), not O(org)). Removing a user who is not a
// direct member is a no-op; transitive membership via subgroups is untouched
// (remove the nesting edge or the inner membership instead).
func (s *Service) RemoveUserFromGroup(ctx context.Context, tx pgx.Tx, orgID, groupID, userID int64) error {
	if err := requireOrgGroup(ctx, tx, orgID, groupID); err != nil {
		return err
	}
	version, err := lockOrgClosure(ctx, tx, orgID)
	if err != nil {
		return err
	}
	ct, err := tx.Exec(ctx,
		`DELETE FROM user_group_member WHERE group_id = $1 AND user_id = $2`,
		groupID, userID)
	if err != nil {
		return apperr.Internal("remove group member", err)
	}
	if ct.RowsAffected() == 0 {
		return nil
	}
	return removeMemberClosure(ctx, tx, orgID, groupID, userID, version)
}

// AddSubgroup nests subgroup ⊂ group and patches the closure: every user who
// reaches the subgroup (its closure rows — the invariant makes them exactly
// the subtree's members) now also reaches the group and its ancestors. Cost:
// affected subtree members × ancestors, never the whole org. Cycles are
// rejected under the org closure lock, so a concurrent edit cannot race the
// check.
func (s *Service) AddSubgroup(ctx context.Context, tx pgx.Tx, orgID, groupID, subgroupID int64) error {
	if err := requireOrgGroup(ctx, tx, orgID, groupID); err != nil {
		return err
	}
	if err := requireOrgGroup(ctx, tx, orgID, subgroupID); err != nil {
		return err
	}
	if groupID == subgroupID {
		return apperr.Invalid("a group cannot contain itself")
	}
	version, err := lockOrgClosure(ctx, tx, orgID)
	if err != nil {
		return err
	}
	var cyclic bool
	if err := tx.QueryRow(ctx, `
		WITH RECURSIVE down (id) AS (
		    SELECT $1::bigint
		  UNION
		    SELECT s.subgroup_id FROM user_group_subgroup s JOIN down d ON s.group_id = d.id
		)
		SELECT EXISTS (SELECT 1 FROM down WHERE id = $2)`,
		subgroupID, groupID).Scan(&cyclic); err != nil {
		return apperr.Internal("cycle check", err)
	}
	if cyclic {
		return apperr.Invalid("nesting would create a cycle")
	}
	ct, err := tx.Exec(ctx, `
		INSERT INTO user_group_subgroup (group_id, subgroup_id) VALUES ($1, $2)
		ON CONFLICT DO NOTHING`, groupID, subgroupID)
	if err != nil {
		return apperr.Internal("nest subgroup", err)
	}
	if ct.RowsAffected() == 0 {
		return nil
	}
	if _, err := tx.Exec(ctx, `
		WITH RECURSIVE `+closureAncestors+`
		INSERT INTO user_group_closure (group_id, user_id, version)
		SELECT a.id, sub.user_id, $4::bigint
		FROM anc a
		CROSS JOIN (SELECT user_id FROM user_group_closure
		            WHERE group_id = $3 AND version = $4) sub
		ON CONFLICT DO NOTHING`,
		groupID, orgID, subgroupID, version); err != nil {
		return apperr.Internal("patch closure (add subgroup)", err)
	}
	return nil
}

// RemoveSubgroup unlinks subgroup ⊄ group and patches the closure for the
// affected pairs only — the subgroup's reachable users × the group's
// ancestors — deleting a pair ONLY where reachability is really gone: the
// NOT EXISTS arm re-derives it from the remaining graph (`gg` is the
// group-reaches-group relation, bounded by the org's group graph). A user
// reachable via a surviving diamond path keeps every row. Unlinking an edge
// that does not exist is a no-op.
func (s *Service) RemoveSubgroup(ctx context.Context, tx pgx.Tx, orgID, groupID, subgroupID int64) error {
	if err := requireOrgGroup(ctx, tx, orgID, groupID); err != nil {
		return err
	}
	if err := requireOrgGroup(ctx, tx, orgID, subgroupID); err != nil {
		return err
	}
	version, err := lockOrgClosure(ctx, tx, orgID)
	if err != nil {
		return err
	}
	ct, err := tx.Exec(ctx,
		`DELETE FROM user_group_subgroup WHERE group_id = $1 AND subgroup_id = $2`,
		groupID, subgroupID)
	if err != nil {
		return apperr.Internal("unlink subgroup", err)
	}
	if ct.RowsAffected() == 0 {
		return nil
	}
	if _, err := tx.Exec(ctx, `
		WITH RECURSIVE `+closureAncestors+`,
		gg (top, node) AS (
		    SELECT id, id FROM user_group WHERE org_id = $2
		  UNION
		    SELECT gg.top, s.subgroup_id FROM gg JOIN user_group_subgroup s ON s.group_id = gg.node
		)
		DELETE FROM user_group_closure gc
		WHERE gc.version = $4
		  AND gc.group_id IN (SELECT id FROM anc)
		  AND gc.user_id IN (SELECT user_id FROM user_group_closure
		                     WHERE group_id = $3 AND version = $4)
		  AND NOT EXISTS (
		      SELECT 1 FROM gg
		      JOIN user_group_member m ON m.group_id = gg.node AND m.user_id = gc.user_id
		      WHERE gg.top = gc.group_id)`,
		groupID, orgID, subgroupID, version); err != nil {
		return apperr.Internal("patch closure (remove subgroup)", err)
	}
	return nil
}
