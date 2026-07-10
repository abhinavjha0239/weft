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
	dmUnreads, err := a.Messaging.DMUnreads(r.Context(), id)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	if dmUnreads == nil {
		dmUnreads = []messaging.DMUnread{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"channels": unreads, "dms": dmUnreads})
}

// handleThreadSubscription: PUT /threads/{id}/subscription {state} —
// 0 clear · 1 followed · 2 muted · 3 unmuted.
func (a *api) handleThreadSubscription(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	threadID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad thread id")
		return
	}
	type req struct {
		State int16 `json:"state"`
	}
	in, ok := decode[req](w, r)
	if !ok {
		return
	}
	if err := a.Messaging.SetThreadSubscription(r.Context(), id, threadID, in.State); err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// handleChannelNotification: PUT /channels/{id}/notification — the caller's
// own level (0 inherit · 1 all · 2 mentions · 3 nothing) and mute flag.
func (a *api) handleChannelNotification(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	channelID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad channel id")
		return
	}
	type req struct {
		Level *int16 `json:"level"`
		Muted *bool  `json:"muted"`
	}
	in, ok := decode[req](w, r)
	if !ok {
		return
	}
	if err := a.Messaging.SetChannelNotification(r.Context(), id, channelID,
		messaging.ChannelNotificationParams{Level: in.Level, Muted: in.Muted}); err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}
