package rest

import (
	"net/http"
	"strconv"

	"github.com/abhinavjha0239/weft/internal/auth"
)

// handleAddReaction / handleRemoveReaction: idempotent ensure-present /
// ensure-absent on PUT/DELETE /messages/{id}/reactions/{emoji} (the emoji
// path segment arrives percent-decoded).
func (a *api) handleAddReaction(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	a.toggleReaction(w, r, id, true)
}

func (a *api) handleRemoveReaction(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	a.toggleReaction(w, r, id, false)
}

func (a *api) toggleReaction(w http.ResponseWriter, r *http.Request, id auth.Identity, add bool) {
	msgID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad message id")
		return
	}
	emoji := r.PathValue("emoji")
	var out any
	if add {
		out, err = a.Messaging.AddReaction(r.Context(), id, msgID, emoji)
	} else {
		out, err = a.Messaging.RemoveReaction(r.Context(), id, msgID, emoji)
	}
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}
