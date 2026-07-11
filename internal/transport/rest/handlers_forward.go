package rest

import (
	"net/http"
	"strconv"

	"github.com/abhinavjha0239/weft/internal/auth"
)

// handleForwardMessage: POST /messages/{id}/forward {thread_id, comment?}
// copies the source message into another thread the caller may post to
// (P-03). The caller must be able to READ the source and SEND to the target.
func (a *api) handleForwardMessage(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	srcID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad message id")
		return
	}
	type req struct {
		ThreadID int64  `json:"thread_id"`
		Comment  string `json:"comment"`
	}
	in, ok := decode[req](w, r)
	if !ok {
		return
	}
	if in.ThreadID == 0 {
		writeError(w, http.StatusBadRequest, "thread_id required")
		return
	}
	newID, err := a.Messaging.ForwardMessage(r.Context(), id, srcID, in.ThreadID, in.Comment)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]int64{"message_id": newID})
}
