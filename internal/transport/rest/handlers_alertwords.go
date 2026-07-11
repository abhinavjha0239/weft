package rest

import (
	"net/http"

	"github.com/abhinavjha0239/weft/internal/auth"
)

// Alert words (N-1 kind 4): GET the list, PUT the whole list (replace-set).
func (a *api) handleListAlertWords(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	out, err := a.Notifications.ListAlertWords(r.Context(), id)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"words": out})
}

func (a *api) handleSetAlertWords(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	type req struct {
		Words []string `json:"words"`
	}
	in, ok := decode[req](w, r)
	if !ok {
		return
	}
	out, err := a.Notifications.SetAlertWords(r.Context(), id, in.Words)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"words": out})
}
