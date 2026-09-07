package importer

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// resolution is the target org as the importer needs to see it: the keys an
// incoming entity can already match, and nothing else. It is READ-ONLY — the
// loader never sees it, and neither the dry run nor the write path mutates the
// loaded maps in place (both work on their own copies where a decision has to
// take earlier decisions in the same run into account).
//
// Every map here answers exactly one question the accounting cannot answer
// from the export alone: does this entity already exist, does this name
// already collide, does this row already sit at or above what we would write.
// Without it the only true statement about the report is "this is what a
// virgin org would get", which is the one org nobody needs to ask about.
type resolution struct {
	// emailToID is the D4 match index: lower(email) → account id. '' is never
	// a key (the preload's IS NOT NULL admits it, so the planner guards).
	emailToID map[string]int64
	// liveNames is lower(name) of every NON-archived channel — the collision
	// set the visible rename walks.
	liveNames map[string]bool
	// groupNameToID is lower(name) → id for EVERY group, system or not: it is
	// both the system-group mapping target and the group rename's collision
	// set.
	groupNameToID map[string]int64
}

// loadResolution reads the target org's current shape inside the caller's
// transaction, so every decision downstream is taken against ONE snapshot.
// source scopes the provenance lookups: an org that has ingested two exports
// must not see the other loader's origin keys.
func loadResolution(ctx context.Context, tx pgx.Tx, orgID int64, source string) (*resolution, error) {
	rc := &resolution{
		emailToID:     map[string]int64{},
		liveNames:     map[string]bool{},
		groupNameToID: map[string]int64{},
	}
	if err := scanPairs(ctx, tx, `
		SELECT lower(email), id FROM user_account
		WHERE org_id = $1 AND email IS NOT NULL`,
		[]any{orgID}, rc.emailToID); err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `
		SELECT lower(name) FROM channel
		WHERE org_id = $1 AND archived_at IS NULL`, orgID)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			rows.Close()
			return nil, err
		}
		rc.liveNames[n] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := scanPairs(ctx, tx,
		`SELECT lower(name), id FROM user_group WHERE org_id = $1`,
		[]any{orgID}, rc.groupNameToID); err != nil {
		return nil, err
	}
	return rc, nil
}

// scanPairs drains a two-column (text, bigint) query into dst.
func scanPairs(ctx context.Context, tx pgx.Tx, sql string, args []any, dst map[string]int64) error {
	rows, err := tx.Query(ctx, sql, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var k string
		var v int64
		if err := rows.Scan(&k, &v); err != nil {
			return err
		}
		dst[k] = v
	}
	return rows.Err()
}
