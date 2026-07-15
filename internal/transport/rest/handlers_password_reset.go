package rest

import (
	"net/http"
)

// Password reset (P-35). Both routes are registered behind the SAME pre-auth
// per-IP limiter as login (they read no Authorization header — the emailed token
// IS the capability). The request endpoint is enumeration-safe: it answers 200
// {"ok":true} for every outcome, logging any internal failure rather than
// surfacing it, so status never reveals whether an email names an account. The
// confirm endpoint maps the taxonomy normally — a bad/expired/used token is one
// indistinguishable 401, the password rules 400.

// handlePasswordResetRequest: POST /api/v1/password-reset/request {org_slug,
// email}. Always 200 {"ok":true}; a returned error is logged (infra failure,
// account-independent) but never changes the response.
func (a *api) handlePasswordResetRequest(w http.ResponseWriter, r *http.Request) {
	type req struct {
		OrgSlug string `json:"org_slug"`
		Email   string `json:"email"`
	}
	in, ok := decode[req](w, r)
	if !ok {
		return
	}
	if err := a.Identity.RequestPasswordReset(r.Context(), in.OrgSlug, in.Email); err != nil {
		a.Log.Error("password reset request", "req", RequestID(r.Context()),
			"path", r.URL.Path, "err", err)
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// handlePasswordResetConfirm: POST /api/v1/password-reset/confirm {token,
// new_password} → 200 {"ok":true}; oracle-free 401 on any token failure.
func (a *api) handlePasswordResetConfirm(w http.ResponseWriter, r *http.Request) {
	type req struct {
		Token       string `json:"token"`
		NewPassword string `json:"new_password"`
	}
	in, ok := decode[req](w, r)
	if !ok {
		return
	}
	if err := a.Identity.ConfirmPasswordReset(r.Context(), in.Token, in.NewPassword); err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}
