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

// handleListNotificationPrefs / handleSetNotificationPref: the N-1 step 4
// medium matrix (email only in v1).
func (a *api) handleListNotificationPrefs(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	out, err := a.Notifications.ListMediumPrefs(r.Context(), id)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"prefs": out})
}

func (a *api) handleSetNotificationPref(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	type req struct {
		Kind    int16 `json:"kind"`
		Medium  int16 `json:"medium"`
		Enabled bool  `json:"enabled"`
	}
	in, ok := decode[req](w, r)
	if !ok {
		return
	}
	if err := a.Notifications.SetMediumPref(r.Context(), id, in.Kind, in.Medium, in.Enabled); err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"kind": in.Kind, "medium": in.Medium, "enabled": in.Enabled})
}
