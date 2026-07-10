package rest

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/domain/identity"
)

func (a *api) handleMe(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	me, err := a.Identity.Me(r.Context(), id)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, me)
}

// handleListUsers serves both profile forms: GET /users?ids=1,2,3 is the
// batch lookup; without ids it is the active-member directory (pickers).
func (a *api) handleListUsers(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	raw := r.URL.Query().Get("ids")
	if raw == "" {
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		users, err := a.Identity.Directory(r.Context(), id, limit)
		if err != nil {
			writeDomainError(w, a.Log, r, err)
			return
		}
		if users == nil {
			users = []identity.Profile{}
		}
		writeJSON(w, http.StatusOK, map[string]any{"users": users})
		return
	}
	var ids []int64
	for _, part := range strings.Split(raw, ",") {
		n, err := strconv.ParseInt(strings.TrimSpace(part), 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, "bad user id "+part)
			return
		}
		ids = append(ids, n)
	}
	users, err := a.Identity.Profiles(r.Context(), id, ids)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	if users == nil {
		users = []identity.Profile{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"users": users})
}

// handlePresence is the bootstrap snapshot for the ephemeral presence plane:
// clients read it once, then apply presence.changed signals.
func (a *api) handlePresence(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	online := a.Hub.OnlineUsers(id.OrgID)
	if online == nil {
		online = []int64{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"online": online})
}
