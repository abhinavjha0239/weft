package perms

// Verb registry (ADR-006). Verbs are wire/DB strings — append, never rename.
// The full ~40-verb collapse map lands verb-by-verb as features arrive; a
// verb not in defaultAssignments and not explicitly assigned is DENIED.
const (
	VerbSendMessage       = "send_message"
	VerbCreateThread      = "create_thread"     // Zulip can_create_topic_group
	VerbEditThreadTitle   = "edit_thread_title" // thread-as-container: rename is an UPDATE, not a message move
	VerbResolveThreads    = "resolve_threads"   // Zulip can_resolve_topics_group
	VerbCreateChannel     = "create_channel"
	VerbCreateSpace       = "create_space"
	VerbCreateItems       = "create_items"
	VerbEditItems         = "edit_items" // covers field edits + transitions in the v1 slice
	VerbInviteMembers     = "invite_members"
	VerbAdministerChannel = "administer_channel"
	VerbModerateMessages  = "moderate_messages" // delete others' messages; never edit them
	// manage_org guards NOTHING since P-47 split it (below). It stays in the
	// registry because it is a wire/DB string: existing orgs hold rows for it,
	// migration 0026 reads those rows, and PUT /admin/verbs has accepted it
	// since P-2 — dropping it would 400 a verb that worked yesterday.
	VerbManageOrg         = "manage_org"
	VerbManageBilling     = "manage_billing"
	VerbComplianceOfficer = "compliance_officer" // F-9: NOT in any preset

	// P-47: manage_org's eighteen gates were nowhere near equal in privilege —
	// adding a custom emoji and reconfiguring SSO were literally the same
	// permission — so they split into the ADR-006 verbs they always wanted to
	// be. Each gate checks its OWN verb and nothing else: there is
	// deliberately NO `OR manage_org` umbrella, because Require resolves
	// per-verb, so an umbrella would make NARROWING a no-op (pointing
	// add_emoji at role:owners would change nothing while manage_org still
	// passed, and PUT /admin/verbs would accept a write with no effect) and
	// would make migration 0026's backfill behaviourally invisible.
	// Upgrade-invisibility comes from that backfill instead.
	VerbAddEmoji             = "add_emoji"              // ADR-006:54
	VerbManageChannelFolders = "manage_channel_folders" // folders AND default channels
	VerbManageStorageQuota   = "manage_storage_quota"
	VerbManageLinkPreviews   = "manage_link_previews"
	VerbManageAutomations    = "manage_automations" // also the rule-alert audience
	VerbManageAuthProviders  = "manage_auth_providers"
	// manage_permissions is honestly dangerous and carries no fake guard:
	// its holder can point every other verb, including this one, at any
	// group — so granting it IS granting org administration. AssignVerb has
	// no floor check and this slice does not invent one.
	VerbManagePermissions = "manage_permissions"
)

// knownVerbs gates admin reassignment: only verbs the code actually checks
// may be assigned, so a typo can't mint a dead grant.
var knownVerbs = map[string]bool{
	VerbSendMessage:       true,
	VerbCreateThread:      true,
	VerbEditThreadTitle:   true,
	VerbResolveThreads:    true,
	VerbCreateChannel:     true,
	VerbCreateSpace:       true,
	VerbCreateItems:       true,
	VerbEditItems:         true,
	VerbInviteMembers:     true,
	VerbAdministerChannel: true,
	VerbModerateMessages:  true,
	VerbManageOrg:         true,
	VerbManageBilling:     true,
	VerbComplianceOfficer: true,

	VerbAddEmoji:             true,
	VerbManageChannelFolders: true,
	VerbManageStorageQuota:   true,
	VerbManageLinkPreviews:   true,
	VerbManageAutomations:    true,
	VerbManageAuthProviders:  true,
	VerbManagePermissions:    true,
}

// KnownVerb reports whether verb is in the registry.
func KnownVerb(verb string) bool { return knownVerbs[verb] }

// channelAssignable is the per-verb "assignable scopes" fact (P-44a). The
// table has carried scope_type/scope_id since 0002 and the resolver has
// always walked channel → workspace → org, but nothing could WRITE a
// non-org row, so "which verbs mean anything at channel scope" never had to
// be answered. The write half exists now, and the honest-rungs rule answers
// it: a verb may be assigned only where something CONSULTS it, or the row is
// config nothing enforces — assigning manage_billing or manage_auth_providers
// "for #general" would store a preference no code path can ever read.
//
// The set is exactly the verbs whose Require/HoldersAt call is handed a chain
// built by ChannelScope:
//
//	send_message        messaging.SendMessage, CreateThread, requireChannelSend
//	create_thread       messaging.CreateThread
//	edit_thread_title   messaging.UpdateThread (title arm)
//	resolve_threads     messaging.UpdateThread (resolved arm)
//	moderate_messages   messaging.MoveMessage, DeleteMessage (others' messages)
//	administer_channel  messaging.SetPinned, UpdateChannel,
//	                    automation.requireScopeAdmin, runner alert audience
//
// A verb joins this set in the slice that gives it a channel-scope consumer,
// exactly as a verb joins knownVerbs with its feature.
//
// manage_permissions is the one exclusion that is a SECURITY rule rather than
// a dead-row rule, and it would still be excluded if the whole registry became
// channel-assignable tomorrow: it is the verb that points every OTHER verb, so
// a channel-scope grant of it is a standing delegation of permission
// administration. identity.AssignVerb refuses it with its own named error for
// that reason — two independent guards, so removing either leaves one standing.
var channelAssignable = map[string]bool{
	VerbSendMessage:       true,
	VerbCreateThread:      true,
	VerbEditThreadTitle:   true,
	VerbResolveThreads:    true,
	VerbModerateMessages:  true,
	VerbAdministerChannel: true,
}

// ChannelAssignable reports whether verb may be ASSIGNED at channel scope —
// the write-side counterpart of KnownVerb. Every channel-assignable verb is
// necessarily a known verb; the converse is deliberately false.
func ChannelAssignable(verb string) bool { return channelAssignable[verb] }

// System role groups (ADR-006 P-2: roles are presets over groups), nested
// owners ⊂ admins ⊂ moderators ⊂ members ⊂ everyone.
const (
	GroupEveryone   = "role:everyone"
	GroupMembers    = "role:members"
	GroupModerators = "role:moderators"
	GroupAdmins     = "role:admins"
	GroupOwners     = "role:owners"
	// GroupAutomations holds the org's automation principal — the kind-2
	// account the runner authors as when a rule names no human (ADR-014 AU-2:
	// "owned by the scope, not a user"). P-44b gives that principal REAL
	// verbs so its posts can be gated like anyone else's, and a group is the
	// only thing (verb, scope) assignments can point at.
	//
	// It is nested UNDER role:everyone rather than carrying its own
	// assignment, because permission_assignment is UNIQUE on
	// (org, verb, scope) and send_message at ORG scope already belongs to
	// role:everyone: a second org-scope row is impossible, and repointing the
	// existing one would strip send_message from every human in the org. So
	// the principal inherits the org default exactly the way role:owners
	// inherits it, and an admin narrows automation posting by assigning
	// send_message at a NARROWER scope (P-44a's channel rung) to a group the
	// principal is not in — or by removing it from this group, which revokes
	// automation posting org-wide.
	GroupAutomations = "role:automations"
)

// defaultAssignments seeds org-scope defaults at bootstrap. Deliberately
// conservative; admins can reassign any verb to any group later.
var defaultAssignments = map[string]string{
	// send/thread-create seed to EVERYONE: membership still gates WHERE, so
	// the only principals this adds are guests speaking in their own
	// channels (Slack/Zulip parity). Admins retarget via PUT /admin/verbs
	// (announcement-style orgs, muted guests).
	VerbSendMessage:       GroupEveryone,
	VerbCreateThread:      GroupEveryone,
	VerbEditThreadTitle:   GroupMembers,
	VerbResolveThreads:    GroupMembers,
	VerbCreateChannel:     GroupMembers,
	VerbCreateSpace:       GroupMembers,
	VerbCreateItems:       GroupMembers,
	VerbEditItems:         GroupMembers,
	VerbInviteMembers:     GroupMembers,
	VerbAdministerChannel: GroupAdmins,
	VerbModerateMessages:  GroupModerators,
	// manage_org is NOT seeded either, for the SAME reason and by the same
	// rule — this slice moved all 18 of its gates onto the seven verbs below,
	// so it now has ZERO enforcement sites and seeding it would mint exactly
	// the grant-with-no-lane this slice exists to remove. Its EXISTING rows
	// are deliberately kept: migration 0026 reads them to backfill the seven,
	// and they are the record of what each org had configured. Same residual
	// as manage_billing, accepted the same way: the constant and the
	// knownVerbs entry stay, so PUT /admin/verbs keeps accepting a string it
	// has taken since P-2 rather than starting to 400 it.
	// manage_billing is NOT seeded (P-47): it is checked in ZERO places, so
	// seeding it minted a grant with no lane — the honest-rungs violation.
	// Giving it a lane means inventing a billing feature, which the
	// verbs-land-with-their-features policy above forbids, so it goes the
	// other way: no seed for new orgs, migration 0026 deletes existing rows,
	// and the constant + knownVerbs entry stay because PUT /admin/verbs has
	// accepted the string since P-2 and must not start rejecting it.

	// P-47: the seven verbs manage_org split into, all at its own default so
	// a NEW org's answers are the pre-split answers. This loop is the ONLY
	// reader of this map, so seeding here helps future orgs only — every
	// EXISTING org gets its rows from migration 0026's backfill, and a
	// missing row is DENY (Require's secure default), not "the default".
	VerbAddEmoji:             GroupAdmins,
	VerbManageChannelFolders: GroupAdmins,
	VerbManageStorageQuota:   GroupAdmins,
	VerbManageLinkPreviews:   GroupAdmins,
	VerbManageAutomations:    GroupAdmins,
	VerbManageAuthProviders:  GroupAdmins,
	VerbManagePermissions:    GroupAdmins,
	// compliance_officer is intentionally NOT seeded (F-9): content-touching
	// compliance access is an explicit grant, never a default of adminship.
}

// BackfillManageOrgSplit is THE statement migration 0026 runs to carry every
// pre-split org's manage_org assignment onto the seven verbs that replaced it
// (P-47). It is exported, and the migration file embeds this exact text, so
// the shipped upgrade and the test that exercises it cannot drift:
// TestManageOrgSplitBackfill asserts the containment and then executes THIS
// const against a synthesised pre-upgrade org.
//
// Why a backfill is REQUIRED rather than nice-to-have: SeedOrg INSERTs an
// explicit org-scope row for every verb in defaultAssignments, and
// identity.Bootstrap is the only production INSERT INTO org, so there is no
// row-less org whose "seeded default" could apply retroactively. For every
// org that already exists, this statement is the ONLY source of the new
// verbs' rows, and without it their org administrators would lose all
// eighteen gates on upgrade.
//
// scope_type/scope_id are copied VERBATIM rather than assumed to be org
// scope, so a non-org-scope assignment (none exists today — SeedOrg hardcodes
// ScopeOrg and AssignVerb hardcodes OrgRef — but the table permits one)
// carries over at its own rung. ON CONFLICT makes a re-run inert and can
// never clobber a row an operator has already set.
const BackfillManageOrgSplit = `INSERT INTO permission_assignment (org_id, verb, scope_type, scope_id, group_id)
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
ON CONFLICT (org_id, verb, scope_type, scope_id) DO NOTHING`

// UnseedManageBillingSQL is the second statement migration 0026 runs (P-47).
// manage_billing is registered and was seeded to role:owners, but it is
// checked in ZERO places — config nothing enforces, which the honest-rungs
// rule forbids. Giving it a lane would mean inventing a billing feature, so
// it is unseeded instead: dropped from defaultAssignments for new orgs, and
// deleted here for the orgs that already carry the dead grant.
//
// The verb string itself SURVIVES (the constant and the knownVerbs entry
// stay): PUT /admin/verbs has accepted it since P-2, and making it 400 would
// be a wire-contract change, not a cleanup. Exported for the same
// no-drift reason as the backfill.
const UnseedManageBillingSQL = `DELETE FROM permission_assignment WHERE verb = 'manage_billing'`

// BackfillAutomationsGroupSQL is the perms half of migration 0027 (P-44b):
// the role:automations group and its nesting under role:everyone, for the
// orgs that already exist. It is SeedOrg's upgrade twin — SeedOrg writes both
// rows EXPLICITLY, so (P-47's lesson, restated) the seed helps FUTURE orgs
// only: without this statement every existing org's automation principal
// would resolve to DENY the moment the gate lands, silently stopping every
// rule on the cell.
//
// Exported and embedded byte-for-byte by the migration so the shipped upgrade
// and the test that exercises it cannot drift — the normal harness migrates an
// EMPTY database, where this matches zero orgs and proves nothing.
const BackfillAutomationsGroupSQL = `INSERT INTO user_group (org_id, name, is_system)
SELECT o.id, 'role:automations', true FROM org o
ON CONFLICT (org_id, name) DO NOTHING;

INSERT INTO user_group_subgroup (group_id, subgroup_id)
SELECT e.id, a.id
FROM user_group e
JOIN user_group a ON a.org_id = e.org_id AND a.name = 'role:automations'
WHERE e.name = 'role:everyone'
ON CONFLICT DO NOTHING`
