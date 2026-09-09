package identity

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

// The per-org automation principal (ADR-014 AU-2 / F-13): the account the
// automation runner authors as when a rule names no human actor. It is a
// kind-2 agent account — "owned by the scope, not a user" — keyed by an
// origin pair rather than an email so the upsert is race-safe and there is
// exactly ONE per org.
//
// The derivation lives HERE because identity owns user_account writes; the
// automation runner and Bootstrap both call AutomationPrincipal so the origin
// key can never drift into two accounts.
const (
	principalOriginSystem = "system"
	// AutomationPrincipalOriginID is the origin_id half of that key. Exported
	// because migration 0027's backfill writes the same row in SQL and the
	// test asserts the two agree.
	AutomationPrincipalOriginID = "automation-principal"
	// automationPrincipalName is the display name on the account. Role 50
	// (guest preset) is deliberate: the preset pointer is cosmetic — real
	// authority comes from the principal's groups (P-44b) — so the account
	// starts at the LOWEST preset rather than looking like an admin.
	automationPrincipalName = "Automations"
	automationPrincipalRole = 50
)

// AutomationPrincipal returns the org's automation principal account id,
// creating it if absent. Runs in the caller's transaction; the ON CONFLICT on
// the origin key makes concurrent callers race-safe.
//
// Creating the account does NOT grant it anything: authority comes from
// membership in perms.GroupAutomations, which Bootstrap establishes once (and
// migration 0027 backfills). The run path must never place it in the group —
// perms.AddUserToGroup takes the org closure lock, so doing that per run would
// serialize every automation in the org behind an org-wide lock, and a lazy
// re-add would silently undo an admin's deliberate revocation.
func AutomationPrincipal(ctx context.Context, tx pgx.Tx, orgID int64) (int64, error) {
	if _, err := tx.Exec(ctx, `
		INSERT INTO user_account (org_id, kind, full_name, role, origin_system, origin_id)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (org_id, origin_system, origin_id) WHERE origin_system IS NOT NULL
		DO NOTHING`,
		orgID, enum.UserAgentPrincipal, automationPrincipalName, automationPrincipalRole,
		principalOriginSystem, AutomationPrincipalOriginID); err != nil {
		return 0, apperr.Internal("create automation principal", err)
	}
	var id int64
	if err := tx.QueryRow(ctx, `
		SELECT id FROM user_account
		WHERE org_id = $1 AND origin_system = $2 AND origin_id = $3`,
		orgID, principalOriginSystem, AutomationPrincipalOriginID).Scan(&id); err != nil {
		return 0, apperr.Internal(
			fmt.Sprintf("lookup automation principal for org %d", orgID), err)
	}
	return id, nil
}

// BackfillAutomationPrincipalSQL is the identity half of migration 0027
// (P-44b), run AFTER perms.BackfillAutomationsGroupSQL has created the group:
// it mints the principal account for every org that never ran an automation
// (the runner used to create it lazily, so an org with no fired rule has
// none), places every principal in role:automations, and patches the flattened
// closure that perms.Require actually reads.
//
// It is the SQL twin of Bootstrap's AutomationPrincipal + AddUserToGroup, and
// it is what makes the upgrade invisible: the seed covers future orgs only, so
// without these statements a pre-upgrade org's first automation post after the
// gate lands would be refused. Every statement is ON CONFLICT DO NOTHING, so a
// re-run is inert and an operator's hand-made row is never clobbered.
//
// The closure patch mirrors perms' addMemberClosure: rows for the group AND
// every ancestor (role:everyone), at the org's CURRENT version — the version
// readers pin. Writing the current version rather than a fresh one keeps the
// migration out of the version-flip business entirely; a later full rebuild
// re-derives the same rows from the membership row above.
const BackfillAutomationPrincipalSQL = `INSERT INTO user_account (org_id, kind, full_name, role, origin_system, origin_id)
SELECT o.id, 2, 'Automations', 50, 'system', 'automation-principal' FROM org o
ON CONFLICT (org_id, origin_system, origin_id) WHERE origin_system IS NOT NULL
DO NOTHING;

INSERT INTO user_group_member (group_id, user_id)
SELECT g.id, u.id
FROM user_group g
JOIN user_account u ON u.org_id = g.org_id
 AND u.origin_system = 'system' AND u.origin_id = 'automation-principal'
WHERE g.name = 'role:automations'
ON CONFLICT DO NOTHING;

WITH RECURSIVE seed (org_id, group_id, user_id) AS (
    SELECT g.org_id, g.id, u.id
    FROM user_group g
    JOIN user_account u ON u.org_id = g.org_id
     AND u.origin_system = 'system' AND u.origin_id = 'automation-principal'
    WHERE g.name = 'role:automations'
),
anc (org_id, group_id, user_id) AS (
    SELECT org_id, group_id, user_id FROM seed
  UNION
    SELECT a.org_id, s.group_id, a.user_id
    FROM anc a JOIN user_group_subgroup s ON s.subgroup_id = a.group_id
)
INSERT INTO user_group_closure (group_id, user_id, version)
SELECT a.group_id, a.user_id, v.version
FROM anc a
JOIN closure_current_version v ON v.org_id = a.org_id
ON CONFLICT DO NOTHING`
