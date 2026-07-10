package rest

import (
	"net/http"
	"strconv"
	"time"

	"github.com/abhinavjha0239/weft/internal/auth"
)

func (a *api) handleScheduleMessage(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	type req struct {
		ThreadID     int64     `json:"thread_id"`
		Content      string    `json:"content"`
		ScheduledFor time.Time `json:"scheduled_for"`
	}
	in, ok := decode[req](w, r)
	if !ok {
		return
	}
	out, err := a.Messaging.ScheduleMessage(r.Context(), id, in.ThreadID, in.Content, in.ScheduledFor)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, out)
}

func (a *api) handleListScheduled(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	out, err := a.Messaging.ListScheduled(r.Context(), id)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"scheduled": out})
}

func (a *api) handleUpdateScheduled(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	schedID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad scheduled id")
		return
	}
	type req struct {
		Content      *string    `json:"content"`
		ScheduledFor *time.Time `json:"scheduled_for"`
	}
	in, ok := decode[req](w, r)
	if !ok {
		return
	}
	if err := a.Messaging.UpdateScheduled(r.Context(), id, schedID, in.Content, in.ScheduledFor); err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"scheduled_id": schedID})
}

func (a *api) handleCancelScheduled(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	schedID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad scheduled id")
		return
	}
	if err := a.Messaging.CancelScheduled(r.Context(), id, schedID); err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"scheduled_id": schedID, "cancelled": true})
}
