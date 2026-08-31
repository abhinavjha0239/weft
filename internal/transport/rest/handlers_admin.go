package rest

import (
	"net/http"

	"github.com/abhinavjha0239/weft/internal/auth"
)

// handleAssignVerb: PUT /api/v1/admin/verbs — point an org-scope verb at a
// group (manage_permissions gated in the domain layer).
func (a *api) handleAssignVerb(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	type req struct {
		Verb  string `json:"verb"`
		Group string `json:"group"`
	}
	in, ok := decode[req](w, r)
	if !ok {
		return
	}
	if in.Verb == "" || in.Group == "" {
		writeError(w, http.StatusBadRequest, "verb and group required")
		return
	}
	if err := a.Identity.AssignVerb(r.Context(), id, in.Verb, in.Group); err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"verb": in.Verb, "group": in.Group})
}
