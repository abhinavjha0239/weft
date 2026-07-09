package rest

import (
	"net/http"
	"strconv"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
)

func (a *api) handleMarkRead(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	threadID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad thread id")
		return
	}
	type req struct {
		UpTo int64 `json:"up_to"`
	}
	in, ok := decode[req](w, r)
	if !ok {
		return
	}
	applied, err := a.Messaging.MarkRead(r.Context(), id, threadID, in.UpTo)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int64{"last_read_message_id": applied})
}

func (a *api) handleUnreads(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	unreads, err := a.Messaging.Unreads(r.Context(), id)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	if unreads == nil {
		unreads = []messaging.ChannelUnread{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"channels": unreads})
}
