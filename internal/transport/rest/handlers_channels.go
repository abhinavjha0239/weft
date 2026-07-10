package rest

import (
	"net/http"
	"strconv"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
)

func (a *api) handleCreateChannel(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	type req struct {
		Name        string `json:"name"`
		Description string `json:"description"`
		Private     bool   `json:"private"`
		WorkspaceID int64  `json:"workspace_id"`
	}
	in, ok := decode[req](w, r)
	if !ok {
		return
	}
	out, err := a.Messaging.CreateChannel(r.Context(), id, messaging.CreateChannelParams{
		Name: in.Name, Description: in.Description,
		Private: in.Private, WorkspaceID: in.WorkspaceID,
	})
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, out)
}

func (a *api) handleListChannels(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	channels, err := a.Messaging.ListChannels(r.Context(), id)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	if channels == nil {
		channels = []messaging.ChannelSummary{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"channels": channels})
}

func (a *api) handleJoinChannel(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	channelID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad channel id")
		return
	}
	if err := a.Messaging.JoinChannel(r.Context(), id, channelID); err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (a *api) handleLeaveChannel(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	channelID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad channel id")
		return
	}
	if err := a.Messaging.LeaveChannel(r.Context(), id, channelID); err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// handleUpdateChannel is the lifecycle endpoint: rename (F-22 alias
// reservation), description, archive/unarchive.
func (a *api) handleUpdateChannel(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	channelID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad channel id")
		return
	}
	type req struct {
		Name        *string `json:"name"`
		Description *string `json:"description"`
		Archived    *bool   `json:"archived"`
	}
	in, ok := decode[req](w, r)
	if !ok {
		return
	}
	if err := a.Messaging.UpdateChannel(r.Context(), id, channelID, messaging.UpdateChannelParams{
		Name: in.Name, Description: in.Description, Archived: in.Archived,
	}); err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}
