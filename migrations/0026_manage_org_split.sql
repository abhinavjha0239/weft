-- P-47: manage_org splits into its ADR-006 verbs.
--
-- One verb guarded EIGHTEEN gates that were nowhere near equal in privilege:
-- adding a custom emoji and reconfiguring the org's SSO provider were the
-- same permission, so an org that wanted someone to curate emoji had to let
-- them reconfigure authentication and reassign every permission in the org.
-- Each of those gates now checks its own verb (perms/verbs.go), with NO
-- umbrella fallback — Require resolves per-verb, so an `OR manage_org` would
-- have made narrowing a no-op and made this backfill unobservable.
--
-- WHY THIS BACKFILL IS LOAD-BEARING. perms.SeedOrg INSERTs an EXPLICIT
-- org-scope permission_assignment row for every verb in defaultAssignments,
-- and identity.Bootstrap is the only production INSERT INTO org, so no
-- row-less org exists whose "seeded default" could apply retroactively. For
-- every org that already exists, this statement is the ONLY source of the new
-- verbs' rows — a missing row resolves to DENY (the resolver's secure
-- default), not to the seeded default. Without it, upgrading would strip all
-- eighteen capabilities from every org administrator on the cell.
--
-- scope_type/scope_id are copied VERBATIM rather than assumed to be org
-- scope: no production writer can create a non-org-scope assignment today
-- (SeedOrg hardcodes ScopeOrg, AssignVerb hardcodes OrgRef), but the table
-- permits one and a narrower rung must carry over as itself. ON CONFLICT
-- makes the statement inert on a re-run and unable to clobber a row an
-- operator has already set by hand.
--
-- The statement below is byte-for-byte perms.BackfillManageOrgSplit; the test
-- asserts the containment, so the shipped upgrade and the test that exercises
-- it cannot drift (the backfill is invisible to the normal harness, which
-- migrates an EMPTY database where it matches zero rows).
INSERT INTO permission_assignment (org_id, verb, scope_type, scope_id, group_id)
SELECT pa.org_id, v.verb, pa.scope_type, pa.scope_id, pa.group_id
FROM permission_assignment pa
CROSS JOIN (VALUES
    ('add_emoji'),
    ('manage_channel_folders'),
    ('manage_storage_quota'),
    ('manage_link_previews'),
    ('manage_automations'),
    ('manage_auth_providers'),
    ('manage_permissions')
) AS v (verb)
WHERE pa.verb = 'manage_org'
ON CONFLICT (org_id, verb, scope_type, scope_id) DO NOTHING;
