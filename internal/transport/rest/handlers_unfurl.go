package rest

import (
	"net/http"

	"github.com/abhinavjha0239/weft/internal/auth"
)

// handleGetLinkPreviews: GET /admin/link-previews → {enabled}
// (manage_org-gated in the domain). Absent setting = enabled.
func (a *api) handleGetLinkPreviews(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	setting, err := a.Unfurl.LinkPreviewsSetting(r.Context(), id)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, setting)
}

// handleSetLinkPreviews: PUT /admin/link-previews {enabled} (manage_org).
func (a *api) handleSetLinkPreviews(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	type req struct {
		Enabled bool `json:"enabled"`
	}
	in, ok := decode[req](w, r)
	if !ok {
		return
	}
	if err := a.Unfurl.SetLinkPreviews(r.Context(), id, in.Enabled); err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"enabled": in.Enabled})
}
