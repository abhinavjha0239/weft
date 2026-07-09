package rest

import (
	"net/http"
	"strconv"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
)

func (a *api) handleCreateThread(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	channelID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad channel id")
		return
	}
	type req struct {
		Title   string `json:"title"`
		Content string `json:"content"`
	}
	in, ok := decode[req](w, r)
	if !ok {
		return
	}
	out, err := a.Messaging.CreateThread(r.Context(), id, messaging.CreateThreadParams{
		ChannelID: channelID, Title: in.Title, Content: in.Content,
	})
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, out)
}

func (a *api) handleUpdateThread(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	threadID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad thread id")
		return
	}
	type req struct {
		Title    *string `json:"title"`
		Resolved *bool   `json:"resolved"`
	}
	in, ok := decode[req](w, r)
	if !ok {
		return
	}
	if err := a.Messaging.UpdateThread(r.Context(), id, threadID, messaging.UpdateThreadParams{
		Title: in.Title, Resolved: in.Resolved,
	}); err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (a *api) handleListThreads(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	channelID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad channel id")
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	page, err := a.Messaging.ListThreads(r.Context(), id, channelID,
		r.URL.Query().Get("cursor"), limit)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func (a *api) handleListThreadMessages(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	threadID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad thread id")
		return
	}
	before, _ := strconv.ParseInt(r.URL.Query().Get("before"), 10, 64)
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	page, err := a.Messaging.ListMessages(r.Context(), id, threadID, before, limit)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, page)
}
