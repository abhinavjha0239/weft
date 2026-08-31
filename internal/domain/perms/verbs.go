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

// System role groups (ADR-006 P-2: roles are presets over groups), nested
// owners ⊂ admins ⊂ moderators ⊂ members ⊂ everyone.
const (
	GroupEveryone   = "role:everyone"
	GroupMembers    = "role:members"
	GroupModerators = "role:moderators"
	GroupAdmins     = "role:admins"
	GroupOwners     = "role:owners"
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
	VerbManageOrg:         GroupAdmins,
	VerbManageBilling:     GroupOwners,
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
