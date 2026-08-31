package rest

import (
	"net/http"
	"strconv"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
)

// P-09 channel folders + default channels: workspace-admin
// (manage_channel_folders)
// surfaces. The workspace is resolved server-side (the org's bootstrap
// workspace), so these endpoints are workspace-implicit.

func (a *api) handleCreateFolder(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	type req struct {
		Name string `json:"name"`
	}
	in, ok := decode[req](w, r)
	if !ok {
		return
	}
	out, err := a.Messaging.CreateFolder(r.Context(), id, in.Name)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, out)
}

func (a *api) handleListFolders(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	folders, err := a.Messaging.ListFolders(r.Context(), id)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	if folders == nil {
		folders = []messaging.Folder{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"folders": folders})
}

func (a *api) handleUpdateFolder(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	folderID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad folder id")
		return
	}
	type req struct {
		Name string `json:"name"`
	}
	in, ok := decode[req](w, r)
	if !ok {
		return
	}
	if err := a.Messaging.UpdateFolder(r.Context(), id, folderID, in.Name); err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (a *api) handleDeleteFolder(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	folderID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad folder id")
		return
	}
	if err := a.Messaging.DeleteFolder(r.Context(), id, folderID); err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (a *api) handleSetDefaultChannels(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	type req struct {
		ChannelIDs []int64 `json:"channel_ids"`
	}
	in, ok := decode[req](w, r)
	if !ok {
		return
	}
	if err := a.Messaging.SetDefaultChannels(r.Context(), id, in.ChannelIDs); err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (a *api) handleListDefaultChannels(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	ids, err := a.Messaging.DefaultChannelIDs(r.Context(), id)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	if ids == nil {
		ids = []int64{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"channel_ids": ids})
}
