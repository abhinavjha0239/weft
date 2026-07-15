package rest

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
)

func itoa(v int64) string { return strconv.FormatInt(v, 10) }

func decode[T any](w http.ResponseWriter, r *http.Request) (T, bool) {
	var v T
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // 1 MiB request cap
	if err := json.NewDecoder(r.Body).Decode(&v); err != nil {
		writeError(w, http.StatusBadRequest, "malformed JSON body")
		return v, false
	}
	return v, true
}

func (a *api) handleBootstrap(w http.ResponseWriter, r *http.Request) {
	type req struct {
		OrgName  string `json:"org_name"`
		OrgSlug  string `json:"org_slug"`
		Email    string `json:"email"`
		Password string `json:"password"`
		FullName string `json:"full_name"`
	}
	in, ok := decode[req](w, r)
	if !ok {
		return
	}
	out, err := a.Identity.Bootstrap(r.Context(), identity.BootstrapParams{
		OrgName: in.OrgName, OrgSlug: in.OrgSlug, Email: in.Email,
		Password: in.Password, FullName: in.FullName,
		IP: clientIP(r), UserAgent: requestUserAgent(r),
	})
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, out)
}

func (a *api) handleLogin(w http.ResponseWriter, r *http.Request) {
	type req struct {
		OrgSlug  string `json:"org_slug"`
		Email    string `json:"email"`
		Password string `json:"password"`
	}
	in, ok := decode[req](w, r)
	if !ok {
		return
	}
	token, err := a.Identity.Login(r.Context(), in.OrgSlug, in.Email, in.Password,
		clientIP(r), requestUserAgent(r))
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"token": token})
}

func (a *api) handleSendMessage(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	channelID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad channel id")
		return
	}
	type req struct {
		ThreadID int64  `json:"thread_id,omitempty"`
		Content  string `json:"content"`
	}
	in, ok := decode[req](w, r)
	if !ok {
		return
	}
	msgID, err := a.Messaging.Send(r.Context(), id, messaging.SendParams{
		ChannelID: channelID, ThreadID: in.ThreadID, Content: in.Content,
	})
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]int64{"message_id": msgID})
}

func (a *api) handleGetMessage(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	msgID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad message id")
		return
	}
	msg, err := a.Messaging.Get(r.Context(), id, msgID)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, msg)
}

func (a *api) handleGateway(w http.ResponseWriter, r *http.Request) {
	id, err := auth.FromToken(r.Context(), a.Pool, auth.BearerToken(r))
	if err != nil {
		writeError(w, http.StatusUnauthorized, "invalid or missing token")
		return
	}
	a.Hub.Serve(w, r, id)
}

func (a *api) handleEditMessage(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	msgID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad message id")
		return
	}
	type req struct {
		Content string `json:"content"`
	}
	in, ok := decode[req](w, r)
	if !ok {
		return
	}
	if err := a.Messaging.EditMessage(r.Context(), id, msgID, in.Content); err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (a *api) handleDeleteMessage(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	msgID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad message id")
		return
	}
	if err := a.Messaging.DeleteMessage(r.Context(), id, msgID); err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}
