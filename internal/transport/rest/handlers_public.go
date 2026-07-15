package rest

import (
	"net/http"
	"strconv"
)

// The P-16 anonymous read allowlist under /api/v1/public/. These handlers run
// with NO identity — no token is read — so they must call ONLY the Public*
// domain methods, which pin `visibility = 3 AND archived_at IS NULL` in SQL.
// Everything else on the API (posting, joining, search, files, gateway)
// stays behind withAuth and is therefore closed to anonymous callers.

func (a *api) handlePublicChannel(w http.ResponseWriter, r *http.Request) {
	channelID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad channel id")
		return
	}
	out, err := a.Messaging.PublicChannel(r.Context(), channelID)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (a *api) handlePublicChannelThreads(w http.ResponseWriter, r *http.Request) {
	channelID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad channel id")
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	page, err := a.Messaging.PublicChannelThreads(r.Context(), channelID,
		r.URL.Query().Get("cursor"), limit)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func (a *api) handlePublicThreadMessages(w http.ResponseWriter, r *http.Request) {
	threadID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad thread id")
		return
	}
	before, _ := strconv.ParseInt(r.URL.Query().Get("before"), 10, 64)
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	page, err := a.Messaging.PublicThreadMessages(r.Context(), threadID, before, limit)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, page)
}
