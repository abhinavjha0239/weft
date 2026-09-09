package perms

import "testing"

// TestChannelAssignableRegistry pins the per-verb "assignable scopes" fact
// (P-44a) as two independently motivated lists rather than one copied set.
//
// channelEnforced is derived from the CODE, not from channelAssignable: these
// are the verbs some production Require/HoldersAt call resolves through a
// chain that ChannelScope built (see the channelAssignable doc for the call
// sites). Assigning one of them at channel scope changes an answer.
//
// orgOnly is the rest of the registry, and each entry is org-only for one of
// two reasons: nothing consults it through a channel chain (a channel-scope
// row would be config nothing enforces — the honest-rungs rule), or, for
// manage_permissions, because it is the verb that points every other verb and
// a channel-scope grant of it is a standing delegation of permission
// administration.
//
// The two lists must EXHAUST knownVerbs, so a verb added to the registry
// cannot silently default to either answer: whoever registers it has to say
// where it may be assigned.
func TestChannelAssignableRegistry(t *testing.T) {
	channelEnforced := []string{
		VerbSendMessage,
		VerbCreateThread,
		VerbEditThreadTitle,
		VerbResolveThreads,
		VerbModerateMessages,
		VerbAdministerChannel,
	}
	orgOnly := []string{
		VerbCreateChannel,
		VerbCreateSpace,
		VerbCreateItems,
		VerbEditItems,
		VerbInviteMembers,
		VerbManageOrg,
		VerbManageBilling,
		VerbComplianceOfficer,
		VerbAddEmoji,
		VerbManageChannelFolders,
		VerbManageStorageQuota,
		VerbManageLinkPreviews,
		VerbManageAutomations,
		VerbManageAuthProviders,
		VerbManagePermissions,
	}

	for _, verb := range channelEnforced {
		if !KnownVerb(verb) {
			t.Errorf("%s is channel-enforced but not in the registry", verb)
		}
		if !ChannelAssignable(verb) {
			t.Errorf("%s resolves through a channel chain but is not channel-assignable — "+
				"an admin cannot restrict it per channel", verb)
		}
	}
	for _, verb := range orgOnly {
		if !KnownVerb(verb) {
			t.Errorf("%s is listed org-only but is not in the registry", verb)
		}
		if ChannelAssignable(verb) {
			t.Errorf("%s must NOT be channel-assignable: nothing resolves it through a "+
				"channel chain (and for manage_permissions, a channel-scope grant "+
				"delegates permission administration)", verb)
		}
	}

	// manage_permissions is called out separately from the loop: it is the one
	// exclusion whose reason is escalation rather than a dead row, and it is
	// the one an over-eager "just allow everything" change would take first.
	if ChannelAssignable(VerbManagePermissions) {
		t.Fatal("manage_permissions is channel-assignable — its holder can point every " +
			"other verb, so a channel-scope grant hands out permission administration")
	}

	// Exhaustive: every registry verb is classified exactly once.
	seen := map[string]bool{}
	for _, verb := range append(append([]string{}, channelEnforced...), orgOnly...) {
		if seen[verb] {
			t.Errorf("%s classified twice", verb)
		}
		seen[verb] = true
	}
	for verb := range knownVerbs {
		if !seen[verb] {
			t.Errorf("registry verb %s is unclassified: say whether it may be assigned "+
				"at channel scope (channelAssignable) and add it to this test", verb)
		}
	}
	if len(seen) != len(knownVerbs) {
		t.Fatalf("classified %d verbs, registry has %d", len(seen), len(knownVerbs))
	}
}
