package rest

import (
	"net/http"
	"strconv"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/domain/compliance"
)

// handleSetRetentionPolicy: PUT /api/v1/admin/retention-policies (upsert per
// scope; compliance_officer gated in the domain layer).
func (a *api) handleSetRetentionPolicy(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	type req struct {
		ScopeType    int16 `json:"scope_type"`
		ScopeID      int64 `json:"scope_id"`
		DurationDays int32 `json:"duration_days"`
		KeepEdits    *bool `json:"keep_edits"`
	}
	in, ok := decode[req](w, r)
	if !ok {
		return
	}
	p := compliance.RetentionPolicy{
		ScopeType: in.ScopeType, ScopeID: in.ScopeID,
		DurationDays: in.DurationDays,
		// Omitted keep_edits defaults to keeping history — the safe reading.
		KeepEdits: in.KeepEdits == nil || *in.KeepEdits,
	}
	if err := a.Compliance.SetRetentionPolicy(r.Context(), id, p); err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, p)
}

func (a *api) handleListRetentionPolicies(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	out, err := a.Compliance.ListRetentionPolicies(r.Context(), id)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"policies": out})
}

func (a *api) handleCreateLegalHold(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	type req struct {
		Name            string `json:"name"`
		CustodianUserID *int64 `json:"custodian_user_id"`
		ChannelID       *int64 `json:"channel_id"`
	}
	in, ok := decode[req](w, r)
	if !ok {
		return
	}
	h, err := a.Compliance.CreateLegalHold(r.Context(), id, in.Name, in.CustodianUserID, in.ChannelID)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, h)
}

func (a *api) handleListLegalHolds(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	out, err := a.Compliance.ListLegalHolds(r.Context(), id)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"holds": out})
}

func (a *api) handleReleaseLegalHold(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	holdID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad hold id")
		return
	}
	if err := a.Compliance.ReleaseLegalHold(r.Context(), id, holdID); err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"hold_id": holdID, "released": true})
}
