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
	VerbManageOrg         = "manage_org"
	VerbManageBilling     = "manage_billing"
	VerbComplianceOfficer = "compliance_officer" // F-9: NOT in any preset
)

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
	VerbSendMessage:       GroupMembers,
	VerbCreateThread:      GroupMembers,
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
	// compliance_officer is intentionally NOT seeded (F-9): content-touching
	// compliance access is an explicit grant, never a default of adminship.
}
