-- P-44b: give the automation principal a group, so its posts can be gated.
--
-- messaging.PostToChannelAsAutomation has had NO permission gate at all,
-- justified by ADR-014 AU-2 ("the scope's admin authorized the rule at
-- creation"). An org-scope rule sees every channel, so that made a posting
-- restriction a knob automations ignore — precisely the honest-rungs
-- violation this project keeps removing. P-44's decision (b), settled on
-- evidence: automations get NO exemption.
--
-- The obstacle was structural. AU-2 deliberately removed the owning user
-- ("owned by the scope, not a user" — Slack's creator-orphaning footgun
-- designed out), so Zulip's owner-inheritance cannot be copied. The
-- resolution is a principal that is not a user and still holds verbs: the
-- per-org kind-2 account the runner already authors as (created lazily since
-- ADR-014 landed) gains a group, and the group is what a (verb, scope)
-- assignment can point at.
--
-- WHY role:automations IS NESTED UNDER role:everyone RATHER THAN CARRYING ITS
-- OWN ASSIGNMENT. permission_assignment is UNIQUE on
-- (org_id, verb, scope_type, scope_id), and send_message at ORG scope already
-- belongs to role:everyone. A second org-scope row for the verb is therefore
-- impossible, and repointing the existing one at role:automations would strip
-- send_message from every human in the org. Nesting is how the other five
-- role groups inherit their grants (owners ⊂ admins ⊂ … ⊂ everyone), and it
-- produces exactly the intended rung: the principal may post wherever an
-- ordinary member may, and an admin narrows it by assigning send_message at a
-- CHANNEL scope (P-44a's surface) to a group the principal is not in — or by
-- deleting its membership row, which revokes automation posting org-wide.
--
-- WHY THIS BACKFILL IS LOAD-BEARING (P-47's lesson, restated). perms.SeedOrg
-- writes the group and its nesting EXPLICITLY and identity.Bootstrap writes
-- the membership row EXPLICITLY, so the seed helps FUTURE orgs only. For every
-- org that already exists these statements are the ONLY source of those rows,
-- and a missing row resolves to DENY (perms.Require's secure default), not to
-- "the default" — so without them, upgrading would silently stop every
-- automation on the cell. The statements are ON CONFLICT DO NOTHING
-- throughout: a re-run is inert and an operator's hand-made row is never
-- clobbered.
--
-- The two blocks below are byte-for-byte perms.BackfillAutomationsGroupSQL and
-- identity.BackfillAutomationPrincipalSQL; the tests assert containment and
-- then execute the CONSTANTS against a synthesised pre-upgrade org, so the
-- shipped upgrade and the test that exercises it cannot drift (the normal
-- harness migrates an EMPTY database, where all of this matches zero rows).

-- Part one (perms): the group and its nesting under role:everyone.
INSERT INTO user_group (org_id, name, is_system)
SELECT o.id, 'role:automations', true FROM org o
ON CONFLICT (org_id, name) DO NOTHING;

INSERT INTO user_group_subgroup (group_id, subgroup_id)
SELECT e.id, a.id
FROM user_group e
JOIN user_group a ON a.org_id = e.org_id AND a.name = 'role:automations'
WHERE e.name = 'role:everyone'
ON CONFLICT DO NOTHING;

-- Part two (identity): the principal account itself for orgs that never fired
-- an automation (the account was created lazily, so those orgs have none), its
-- membership row, and the flattened closure rows perms.Require actually reads.
--
-- The closure patch mirrors perms' addMemberClosure: the group AND every
-- ancestor of it, at the org's CURRENT version — the version readers pin. It
-- writes the current version rather than minting a new one so the migration
-- stays out of the version-flip business; any later full rebuild re-derives
-- exactly these rows from the membership row above.
INSERT INTO user_account (org_id, kind, full_name, role, origin_system, origin_id)
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
ON CONFLICT DO NOTHING;
