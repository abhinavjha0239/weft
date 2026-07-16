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

// Push subscriptions (P-21): self-scoped Web Push registrations. Endpoints are
// user-supplied capability URLs, so every send rides the egress guard.
func (a *api) handleCreatePushSubscription(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	type req struct {
		Endpoint string `json:"endpoint"`
		Keys     struct {
			P256dh string `json:"p256dh"`
			Auth   string `json:"auth"`
		} `json:"keys"`
	}
	in, ok := decode[req](w, r)
	if !ok {
		return
	}
	subID, err := a.Notifications.Subscribe(r.Context(), id, in.Endpoint, in.Keys.P256dh, in.Keys.Auth)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]int64{"id": subID})
}

func (a *api) handleListPushSubscriptions(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	out, err := a.Notifications.ListSubscriptions(r.Context(), id)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"subscriptions": out})
}

// handleDeletePushSubscription: DELETE /api/v1/me/push-subscriptions/{id} → 204.
// A foreign or unknown id is an oracle-free 404 (the sessions precedent).
func (a *api) handleDeletePushSubscription(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	subID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad subscription id")
		return
	}
	if err := a.Notifications.DeleteSubscription(r.Context(), id, subID); err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handlePushVAPIDKey: GET /api/v1/push/vapid-key → {key} for clients to
// subscribe, or 404 when push is unconfigured.
func (a *api) handlePushVAPIDKey(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	key, ok := a.Notifications.VAPIDKey()
	if !ok {
		writeError(w, http.StatusNotFound, "push not configured")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"key": key})
}
