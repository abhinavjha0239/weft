package rest

import (
	"net/http"
	"strconv"

	"github.com/abhinavjha0239/weft/internal/auth"
)

// Session management (P-29). Every handler derives the CURRENT session from
// the presented bearer token's hash — the raw token is hashed and compared,
// never stored or echoed.

// handleListSessions: GET /api/v1/me/sessions — the caller's live sessions,
// newest first, with the presenting one flagged `current`.
func (a *api) handleListSessions(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	out, err := a.Identity.Sessions(r.Context(), id, auth.TokenHash(auth.BearerToken(r)))
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"sessions": out})
}

// handleRevokeSession: DELETE /api/v1/me/sessions/{id} → 204. Revoking the
// current session is logout; a foreign or unknown id is an oracle-free 404.
func (a *api) handleRevokeSession(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	sessionID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad session id")
		return
	}
	if err := a.Identity.RevokeSession(r.Context(), id, sessionID); err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleRevokeOtherSessions: DELETE /api/v1/me/sessions — sign out everywhere
// else; the presenting session survives.
func (a *api) handleRevokeOtherSessions(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	n, err := a.Identity.RevokeOtherSessions(r.Context(), id, auth.TokenHash(auth.BearerToken(r)))
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int64{"revoked": n})
}

// handleChangePassword: POST /api/v1/me/password — verify the current
// password, set the new one, and revoke every OTHER live session in the same
// transaction (the presenting session survives).
func (a *api) handleChangePassword(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	type req struct {
		CurrentPassword string `json:"current_password"`
		NewPassword     string `json:"new_password"`
	}
	in, ok := decode[req](w, r)
	if !ok {
		return
	}
	n, err := auth.ChangePassword(r.Context(), a.Pool, id.UserID,
		auth.TokenHash(auth.BearerToken(r)), in.CurrentPassword, in.NewPassword)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int64{"revoked_sessions": n})
}
