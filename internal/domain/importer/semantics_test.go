package importer

import "testing"

// The three functions below ARE the Zulip semantic layer of the importer:
// every imported account's role preset, its seeded role-group membership, and
// every system-group mapping flows through them. None had a direct test — the
// showcase fixture only exercises Zulip roles 200/400 and three of the six
// system-group names, so the owner, moderator and guest rows (and the
// role:everyone mapping) could be wrong, or silently reordered by a refactor,
// with the whole suite still green.

// TestWeftRoleMapsZulipRoleConstants pins the source constants.
// zerver/models/users.py: ROLE_REALM_OWNER = 100, ROLE_REALM_ADMINISTRATOR =
// 200, ROLE_MODERATOR = 300, ROLE_MEMBER = 400, ROLE_GUEST = 600. Weft's
// presets (migrations/0002): 10 owner · 20 admin · 30 moderator · 40 member ·
// 50 guest.
func TestWeftRoleMapsZulipRoleConstants(t *testing.T) {
	cases := []struct {
		zulip int
		want  int16
		why   string
	}{
		{100, 10, "realm owner"},
		{200, 20, "realm administrator"},
		{300, 30, "moderator"},
		{400, 40, "member"},
		{600, 50, "guest"},
		// Anything Zulip has not defined coarsens DOWN to plain member: an
		// import never invents a privilege it cannot ground in a known
		// source constant. 500 is the gap between member and guest.
		{0, 40, "unset"},
		{500, 40, "undefined constant"},
		{700, 40, "above guest"},
		{-1, 40, "negative"},
	}
	for _, c := range cases {
		if got := weftRole(c.zulip); got != c.want {
			t.Errorf("weftRole(%d) = %d, want %d (%s)", c.zulip, got, c.want, c.why)
		}
	}
}

// TestRoleGroupNamesSeededGroups pins which seeded group a role preset joins.
// Guests deliberately hold NO role group (P-5: a guest's reach is its
// channels, never an org-wide group).
func TestRoleGroupNamesSeededGroups(t *testing.T) {
	cases := []struct {
		role int16
		want string
	}{
		{10, "role:owners"},
		{20, "role:admins"},
		{30, "role:moderators"},
		{40, "role:members"},
		{50, ""},
		// Defensive: an unmapped preset must not silently land in a
		// privileged group — the default arm is the members floor.
		{0, "role:members"},
		{99, "role:members"},
	}
	for _, c := range cases {
		if got := roleGroup(c.role); got != c.want {
			t.Errorf("roleGroup(%d) = %q, want %q", c.role, got, c.want)
		}
	}
}

// TestZulipSystemGroupMapping pins the system-group translation, including
// the two coarsenings and the two names with no Weft counterpart. An empty
// result means "no mapping" and the write path then creates NOTHING — so a
// wrong non-empty answer here would duplicate or mis-target a seeded group.
func TestZulipSystemGroupMapping(t *testing.T) {
	cases := []struct {
		zulip string
		want  string
		why   string
	}{
		{"role:owners", "role:owners", "same name both sides"},
		{"role:administrators", "role:admins", "renamed"},
		{"role:moderators", "role:moderators", "same name both sides"},
		{"role:members", "role:members", "same name both sides"},
		{"role:fullmembers", "role:members", "coarsens — no waiting period on this side"},
		{"role:everyone", "role:everyone", "same name both sides"},
		{"role:nobody", "", "no counterpart on this side"},
		{"role:internet", "", "no counterpart on this side"},
		{"engineering", "", "not a system group at all"},
		{"", "", "empty"},
	}
	for _, c := range cases {
		if got := zulipSystemGroup(c.zulip); got != c.want {
			t.Errorf("zulipSystemGroup(%q) = %q, want %q (%s)", c.zulip, got, c.want, c.why)
		}
	}
}
