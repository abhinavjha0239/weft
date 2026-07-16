package rest

import (
	"net/http"
	"strconv"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/domain/identity"
)

// OIDC login (P-30). The two login routes sit OUTSIDE withAuth behind the
// per-IP authLimit (the login/unsubscribe precedent): no Authorization is read
// because the IdP-issued code+state are the capability. The callback answers
// 200 JSON {token, user_id, org_id} — the API-era austerity, no web page. The
// auth-provider CRUD routes are withAuth + manage_org-gated in the domain.

// handleOIDCStart: GET /api/v1/auth/oidc/{org_slug}/{provider}/start → 302 to
// the IdP authorize URL. An absent/disabled provider or bad org slug is one
// oracle-free 404.
func (a *api) handleOIDCStart(w http.ResponseWriter, r *http.Request) {
	authURL, err := a.Identity.StartOIDC(r.Context(),
		r.PathValue("org_slug"), r.PathValue("provider"))
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	http.Redirect(w, r, authURL, http.StatusFound)
}

// handleOIDCCallback: GET /api/v1/auth/oidc/{org_slug}/{provider}/callback?
// code&state → 200 {token, user_id, org_id}. Replay/expiry/nonce/verify
// failures are one 401; a refused identity is 403; a provider disabled
// mid-flow is 404.
func (a *api) handleOIDCCallback(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	res, err := a.Identity.CallbackOIDC(r.Context(),
		r.PathValue("org_slug"), r.PathValue("provider"),
		q.Get("code"), q.Get("state"), clientIP(r), requestUserAgent(r))
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// handleCreateAuthProvider: POST /api/v1/admin/auth-providers. Creates a
// disabled provider; the response carries has_secret, never the secret.
func (a *api) handleCreateAuthProvider(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	type req struct {
		Name         string `json:"name"`
		Issuer       string `json:"issuer"`
		ClientID     string `json:"client_id"`
		ClientSecret string `json:"client_secret"`
	}
	in, ok := decode[req](w, r)
	if !ok {
		return
	}
	out, err := a.Identity.CreateAuthProvider(r.Context(), id, identity.CreateProviderParams{
		Name: in.Name, Issuer: in.Issuer, ClientID: in.ClientID, ClientSecret: in.ClientSecret,
	})
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, out)
}

// handleListAuthProviders: GET /api/v1/admin/auth-providers.
func (a *api) handleListAuthProviders(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	out, err := a.Identity.ListAuthProviders(r.Context(), id)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

// handleUpdateAuthProvider: PATCH /api/v1/admin/auth-providers/{id}. Fields are
// optional; enabling runs a discovery probe (422 on failure).
func (a *api) handleUpdateAuthProvider(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	providerID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad provider id")
		return
	}
	type req struct {
		Issuer       *string `json:"issuer"`
		ClientID     *string `json:"client_id"`
		ClientSecret *string `json:"client_secret"`
		Enabled      *bool   `json:"enabled"`
	}
	in, ok := decode[req](w, r)
	if !ok {
		return
	}
	out, err := a.Identity.UpdateAuthProvider(r.Context(), id, providerID, identity.UpdateProviderParams{
		Issuer: in.Issuer, ClientID: in.ClientID, ClientSecret: in.ClientSecret, Enabled: in.Enabled,
	})
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

// handleDeleteAuthProvider: DELETE /api/v1/admin/auth-providers/{id}.
func (a *api) handleDeleteAuthProvider(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	providerID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad provider id")
		return
	}
	if err := a.Identity.DeleteAuthProvider(r.Context(), id, providerID); err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}
