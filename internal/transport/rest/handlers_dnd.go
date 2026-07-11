package rest

import (
	"net/http"
	"time"

	"github.com/abhinavjha0239/weft/internal/auth"
)

// DND snooze (ADR-011 N-2): GET reads the caller's snooze, PUT sets it
// ({snoozed_until} null/absent clears the snooze).
func (a *api) handleGetDND(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	until, err := a.Notifications.GetDND(r.Context(), id)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"snoozed_until": until})
}

func (a *api) handleSetDND(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	type req struct {
		SnoozedUntil *time.Time `json:"snoozed_until"`
	}
	in, ok := decode[req](w, r)
	if !ok {
		return
	}
	if err := a.Notifications.SetDND(r.Context(), id, in.SnoozedUntil); err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// VIP / priority contacts (ADR-011 N-2): GET lists them, PUT replaces the
// whole set. A VIP's messages pierce the caller's DND snooze.
func (a *api) handleListVIPs(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	out, err := a.Notifications.ListVIPs(r.Context(), id)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"user_ids": out})
}

func (a *api) handleSetVIPs(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	type req struct {
		UserIDs []int64 `json:"user_ids"`
	}
	in, ok := decode[req](w, r)
	if !ok {
		return
	}
	out, err := a.Notifications.SetVIPs(r.Context(), id, in.UserIDs)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"user_ids": out})
}
