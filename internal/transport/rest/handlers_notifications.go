package rest

import (
	"net/http"
	"strconv"

	"github.com/abhinavjha0239/weft/internal/auth"
)

func (a *api) handleListNotifications(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	inbox, err := a.Notifications.List(r.Context(), id, limit)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, inbox)
}

func (a *api) handleMarkNotificationsSeen(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	type req struct {
		UpTo int64 `json:"up_to"`
	}
	in, ok := decode[req](w, r)
	if !ok {
		return
	}
	if err := a.Notifications.MarkSeen(r.Context(), id, in.UpTo); err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}
