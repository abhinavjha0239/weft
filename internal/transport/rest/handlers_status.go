package rest

import (
	"net/http"
	"time"

	"github.com/abhinavjha0239/weft/internal/auth"
)

// User status (ADR-011 N-3): PUT sets the caller's own emoji/text/expiry,
// DELETE clears it. The status shows up on directory + batch-profile reads.
func (a *api) handleSetStatus(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	type req struct {
		Emoji      string     `json:"emoji"`
		StatusText string     `json:"status_text"`
		ExpiresAt  *time.Time `json:"expires_at"`
	}
	in, ok := decode[req](w, r)
	if !ok {
		return
	}
	if err := a.Identity.SetStatus(r.Context(), id, in.Emoji, in.StatusText, in.ExpiresAt); err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (a *api) handleClearStatus(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	if err := a.Identity.ClearStatus(r.Context(), id); err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}
