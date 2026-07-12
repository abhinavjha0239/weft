package rest

import (
	"net/http"
	"strconv"

	"github.com/abhinavjha0239/weft/internal/auth"
)

// handleMoveMessage: POST /messages/{id}/move {thread_id} relocates a channel
// message to another thread in the SAME channel (P-04). Gated like delete —
// the author or moderate_messages.
func (a *api) handleMoveMessage(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	msgID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad message id")
		return
	}
	type req struct {
		ThreadID int64 `json:"thread_id"`
	}
	in, ok := decode[req](w, r)
	if !ok {
		return
	}
	if in.ThreadID == 0 {
		writeError(w, http.StatusBadRequest, "thread_id required")
		return
	}
	if err := a.Messaging.MoveMessage(r.Context(), id, msgID, in.ThreadID); err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}
