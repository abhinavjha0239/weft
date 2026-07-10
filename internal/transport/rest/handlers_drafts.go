package rest

import (
	"net/http"
	"strconv"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
)

type draftReq struct {
	ChannelID *int64 `json:"channel_id"`
	ThreadID  *int64 `json:"thread_id"`
	DMSpaceID *int64 `json:"dm_space_id"`
	Source    string `json:"source"`
}

func (r draftReq) params() messaging.DraftParams {
	return messaging.DraftParams{ChannelID: r.ChannelID, ThreadID: r.ThreadID,
		DMSpaceID: r.DMSpaceID, Source: r.Source}
}

func (a *api) handleCreateDraft(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	in, ok := decode[draftReq](w, r)
	if !ok {
		return
	}
	d, err := a.Messaging.CreateDraft(r.Context(), id, in.params())
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, d)
}

func (a *api) handleUpdateDraft(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	draftID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad draft id")
		return
	}
	in, ok := decode[draftReq](w, r)
	if !ok {
		return
	}
	if err := a.Messaging.UpdateDraft(r.Context(), id, draftID, in.params()); err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"draft_id": draftID})
}

func (a *api) handleDeleteDraft(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	draftID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad draft id")
		return
	}
	if err := a.Messaging.DeleteDraft(r.Context(), id, draftID); err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"draft_id": draftID, "deleted": true})
}

func (a *api) handleListDrafts(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	out, err := a.Messaging.ListDrafts(r.Context(), id)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"drafts": out})
}
