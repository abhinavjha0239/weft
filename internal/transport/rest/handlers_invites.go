package rest

import (
	"net/http"
	"strconv"
	"time"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/domain/identity"
)

func (a *api) handleCreateInvite(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	type req struct {
		Email      *string `json:"email"`
		Role       int16   `json:"role"`
		ChannelIDs []int64 `json:"channel_ids"`
		ExpiresIn  int     `json:"expires_in_hours"`
		MaxUses    int     `json:"max_uses"`
	}
	in, ok := decode[req](w, r)
	if !ok {
		return
	}
	out, err := a.Identity.CreateInvite(r.Context(), id, identity.CreateInviteParams{
		Email: in.Email, Role: in.Role, ChannelIDs: in.ChannelIDs,
		ExpiresIn: time.Duration(in.ExpiresIn) * time.Hour, MaxUses: in.MaxUses,
	})
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, out)
}

func (a *api) handleListInvites(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	out, err := a.Identity.ListInvites(r.Context(), id)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"invites": out})
}

func (a *api) handleRevokeInvite(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	inviteID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad invite id")
		return
	}
	if err := a.Identity.RevokeInvite(r.Context(), id, inviteID); err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"invite_id": inviteID, "revoked": true})
}

// handleAcceptInvite is PRE-AUTH (IP rate-limited like login/bootstrap):
// the token is the credential.
func (a *api) handleAcceptInvite(w http.ResponseWriter, r *http.Request) {
	type req struct {
		Token    string `json:"token"`
		Email    string `json:"email"`
		Password string `json:"password"`
		FullName string `json:"full_name"`
	}
	in, ok := decode[req](w, r)
	if !ok {
		return
	}
	out, err := a.Identity.AcceptInvite(r.Context(), identity.AcceptInviteParams{
		Token: in.Token, Email: in.Email, Password: in.Password, FullName: in.FullName,
		IP: clientIP(r), UserAgent: requestUserAgent(r),
	})
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, out)
}
