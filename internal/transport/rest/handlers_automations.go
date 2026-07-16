package rest

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/domain/automation"
)

// maxWebhookBody bounds an inbound webhook body (checked only after auth).
const maxWebhookBody = 64 * 1024

func (a *api) handleCreateAutomation(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	type req struct {
		ScopeType   int16           `json:"scope_type"`
		ScopeID     int64           `json:"scope_id"`
		Name        string          `json:"name"`
		Definition  json.RawMessage `json:"definition"`
		ActorUserID *int64          `json:"actor_user_id"`
	}
	in, ok := decode[req](w, r)
	if !ok {
		return
	}
	out, err := a.Automations.Create(r.Context(), id, automation.CreateParams{
		ScopeType: in.ScopeType, ScopeID: in.ScopeID, Name: in.Name,
		Definition: in.Definition, ActorUserID: in.ActorUserID,
	})
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, out)
}

func (a *api) handleListAutomations(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	scopeType, _ := strconv.ParseInt(r.URL.Query().Get("scope_type"), 10, 16)
	scopeID, _ := strconv.ParseInt(r.URL.Query().Get("scope_id"), 10, 64)
	out, err := a.Automations.List(r.Context(), id, int16(scopeType), scopeID)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"automations": out})
}

func (a *api) handleUpdateAutomation(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	autoID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad automation id")
		return
	}
	type req struct {
		Name             *string         `json:"name"`
		Definition       json.RawMessage `json:"definition"`
		Enabled          *bool           `json:"enabled"`
		AllowRuleTrigger *bool           `json:"allow_rule_trigger"`
	}
	in, ok := decode[req](w, r)
	if !ok {
		return
	}
	if err := a.Automations.Update(r.Context(), id, autoID, automation.UpdateParams{
		Name: in.Name, Definition: in.Definition,
		Enabled: in.Enabled, AllowRuleTrigger: in.AllowRuleTrigger,
	}); err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"automation_id": autoID})
}

func (a *api) handleDeleteAutomation(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	autoID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad automation id")
		return
	}
	if err := a.Automations.Delete(r.Context(), id, autoID); err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"automation_id": autoID, "deleted": true})
}

func (a *api) handleConsentAutomation(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	autoID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad automation id")
		return
	}
	if err := a.Automations.Consent(r.Context(), id, autoID); err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"automation_id": autoID, "consented": true})
}

func (a *api) handleListAutomationRuns(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	autoID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad automation id")
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	out, err := a.Automations.ListRuns(r.Context(), id, autoID, limit)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"runs": out})
}

func (a *api) handleListAutomationDeliveries(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	autoID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad automation id")
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	out, err := a.Automations.ListDeliveries(r.Context(), id, autoID, limit)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deliveries": out})
}

// handleSlash records a slash-command invocation (authed); the channel-send
// gate in the domain authorizes it and multiple rules may fire it.
func (a *api) handleSlash(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	type req struct {
		Command   string `json:"command"`
		ChannelID int64  `json:"channel_id"`
		Text      string `json:"text"`
	}
	in, ok := decode[req](w, r)
	if !ok {
		return
	}
	if err := a.Automations.Slash(r.Context(), id, in.Command, in.ChannelID, in.Text); err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]bool{"ok": true})
}

// handleRotateWebhookToken mints a fresh capability token for a webhook rule
// (scope-admin gated) and returns it — the only surface, besides List, that
// reveals the token.
func (a *api) handleRotateWebhookToken(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	autoID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad automation id")
		return
	}
	token, err := a.Automations.RotateWebhookToken(r.Context(), id, autoID)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"automation_id": autoID, "webhook_token": token})
}

// handleWebhook is the UNAUTHENTICATED inbound-webhook endpoint. It is
// oracle-free: every authentication failure — bad id, wrong token, disabled,
// or non-webhook rule — returns the identical 404. Body validation runs only
// AFTER authentication passes, and a per-rule limiter (past the per-IP one)
// bounds a rule's ingest and any external-service echo loop.
func (a *api) handleWebhook(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	orgID, err := a.Automations.AuthenticateWebhook(r.Context(), id, r.PathValue("token"))
	if err != nil {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	// Per-rule limiter, after auth so only authenticated senders consume it.
	if !a.hookLimit.Allow("h:" + itoa(id)) {
		writeError(w, http.StatusTooManyRequests, "rate limit exceeded")
		return
	}
	body, tooBig, err := readLimited(r.Body, maxWebhookBody)
	if err != nil {
		writeError(w, http.StatusBadRequest, "could not read body")
		return
	}
	if tooBig {
		writeError(w, http.StatusRequestEntityTooLarge, "body exceeds 64KB")
		return
	}
	if !json.Valid(body) {
		writeError(w, http.StatusBadRequest, "body must be valid JSON")
		return
	}
	if err := a.Automations.RecordWebhook(r.Context(), orgID, id, body); err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]bool{"ok": true})
}

// readLimited reads up to max bytes; tooBig reports that the reader carried
// more (so the caller can 413 without buffering an unbounded body).
func readLimited(r io.Reader, max int64) (body []byte, tooBig bool, err error) {
	b, err := io.ReadAll(io.LimitReader(r, max+1))
	if err != nil {
		return nil, false, err
	}
	if int64(len(b)) > max {
		return nil, true, nil
	}
	return b, false, nil
}
