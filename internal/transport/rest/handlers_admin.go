package rest

import (
	"net/http"

	"github.com/abhinavjha0239/weft/internal/auth"
)

// handleAssignVerb: PUT /api/v1/admin/verbs — point a verb at a group, at org
// scope or (P-44a) at ONE channel's scope. The scope is chosen by the optional
// channel_id: absent or 0 keeps the org-scope request byte-for-byte what it
// has been since P-2. Gating is the domain layer's (manage_permissions at org
// scope, administer_channel at the channel's), including the channel scope's
// oracle-free 404.
func (a *api) handleAssignVerb(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	type req struct {
		Verb      string `json:"verb"`
		Group     string `json:"group"`
		ChannelID int64  `json:"channel_id"`
	}
	in, ok := decode[req](w, r)
	if !ok {
		return
	}
	if in.Verb == "" || in.Group == "" {
		writeError(w, http.StatusBadRequest, "verb and group required")
		return
	}
	out := map[string]any{"verb": in.Verb, "group": in.Group}
	var err error
	if in.ChannelID != 0 {
		err = a.Identity.AssignVerbAtChannel(r.Context(), id, in.Verb, in.Group, in.ChannelID)
		out["channel_id"] = in.ChannelID
	} else {
		err = a.Identity.AssignVerb(r.Context(), id, in.Verb, in.Group)
	}
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}
