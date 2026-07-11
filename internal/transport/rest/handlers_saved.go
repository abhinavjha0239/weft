package rest

import (
	"net/http"
	"strconv"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
)

// Saved items (ADR-007 M-6): personal "save for later" on a message.
// PUT/DELETE /messages/{id}/save toggle the caller's own entry; GET /saved
// lists them newest-first with container ids and an excerpt.
func (a *api) handleSaveMessage(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	msgID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad message id")
		return
	}
	if err := a.Messaging.SaveMessage(r.Context(), id, msgID); err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (a *api) handleUnsaveMessage(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	msgID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad message id")
		return
	}
	if err := a.Messaging.UnsaveMessage(r.Context(), id, msgID); err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (a *api) handleListSaved(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	items, err := a.Messaging.ListSaved(r.Context(), id)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	if items == nil {
		items = []messaging.SavedItem{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"saved": items})
}
