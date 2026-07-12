package rest

import (
	"net/http"
	"strconv"

	"github.com/abhinavjha0239/weft/internal/auth"
)

// Channel pins (P-02b): shared per-container curation, gated by
// administer_channel. PUT/DELETE /messages/{id}/pin toggle the pin;
// GET /channels/{id}/pins lists a channel's pinned messages (member-gated).
func (a *api) handlePinMessage(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	msgID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad message id")
		return
	}
	if err := a.Messaging.PinMessage(r.Context(), id, msgID); err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (a *api) handleUnpinMessage(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	msgID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad message id")
		return
	}
	if err := a.Messaging.UnpinMessage(r.Context(), id, msgID); err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (a *api) handleListChannelPins(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	channelID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad channel id")
		return
	}
	pins, err := a.Messaging.ListChannelPins(r.Context(), id, channelID)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"pins": pins})
}
