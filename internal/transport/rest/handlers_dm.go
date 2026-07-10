package rest

import (
	"net/http"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/domain/dm"
)

// handleOpenDM is create-or-get: POST /dms {user_ids:[...]} returns THE
// conversation for that participant set (the actor is always included).
func (a *api) handleOpenDM(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	type req struct {
		UserIDs []int64 `json:"user_ids"`
	}
	in, ok := decode[req](w, r)
	if !ok {
		return
	}
	out, err := a.DM.Open(r.Context(), id, in.UserIDs)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (a *api) handleListDMs(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	dms, err := a.DM.List(r.Context(), id)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	if dms == nil {
		dms = []dm.Summary{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"dms": dms})
}
