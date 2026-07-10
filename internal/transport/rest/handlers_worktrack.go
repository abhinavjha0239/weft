package rest

import (
	"net/http"
	"strconv"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/domain/worktrack"
)

func (a *api) handleCreateSpace(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	type req struct {
		Key  string `json:"key"`
		Name string `json:"name"`
	}
	in, ok := decode[req](w, r)
	if !ok {
		return
	}
	out, err := a.Worktrack.CreateSpace(r.Context(), id, worktrack.CreateSpaceParams{
		Key: in.Key, Name: in.Name,
	})
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, out)
}

func (a *api) handleListSpaces(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	spaces, err := a.Worktrack.ListSpaces(r.Context(), id)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	if spaces == nil {
		spaces = []worktrack.SpaceSummary{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"spaces": spaces})
}

func (a *api) handleCreateItem(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	spaceID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad space id")
		return
	}
	type req struct {
		Title       string `json:"title"`
		Description string `json:"description"`
		Type        string `json:"type"`
	}
	in, ok := decode[req](w, r)
	if !ok {
		return
	}
	out, err := a.Worktrack.CreateItem(r.Context(), id, worktrack.CreateItemParams{
		SpaceID: spaceID, Title: in.Title, Description: in.Description, Type: in.Type,
	})
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, out)
}

func (a *api) handleListItems(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	spaceID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad space id")
		return
	}
	items, err := a.Worktrack.ListItems(r.Context(), id, spaceID)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	if items == nil {
		items = []worktrack.Item{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (a *api) handleSpaceStatuses(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	spaceID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad space id")
		return
	}
	sts, err := a.Worktrack.Statuses(r.Context(), id, spaceID)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"statuses": sts})
}

func (a *api) handleUpdateItem(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	itemID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad item id")
		return
	}
	type req struct {
		Title      *string `json:"title"`
		StatusID   *int64  `json:"status_id"`
		AssigneeID *int64  `json:"assignee_id"`
	}
	in, ok := decode[req](w, r)
	if !ok {
		return
	}
	if err := a.Worktrack.UpdateItem(r.Context(), id, itemID, worktrack.UpdateItemParams{
		Title: in.Title, StatusID: in.StatusID, AssigneeID: in.AssigneeID,
	}); err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (a *api) handlePromoteThread(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	threadID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad thread id")
		return
	}
	type req struct {
		SpaceID int64  `json:"space_id"`
		Type    string `json:"type"`
	}
	in, ok := decode[req](w, r)
	if !ok {
		return
	}
	out, err := a.Worktrack.PromoteThread(r.Context(), id, threadID, in.SpaceID, in.Type)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, out)
}

func (a *api) handleSendToThread(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	threadID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad thread id")
		return
	}
	type req struct {
		Content string `json:"content"`
	}
	in, ok := decode[req](w, r)
	if !ok {
		return
	}
	msgID, err := a.Messaging.SendToThread(r.Context(), id, threadID, in.Content)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]int64{"message_id": msgID})
}
