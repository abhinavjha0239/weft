package rest

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/domain/automation"
)

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
