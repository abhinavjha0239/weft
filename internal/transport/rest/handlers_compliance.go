package rest

import (
	"net/http"
	"strconv"
	"time"

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

// handleRequestExport: POST /api/v1/admin/exports (AD-5; the result file
// downloads through the normal files endpoint).
func (a *api) handleRequestExport(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	in, ok := decode[compliance.ExportScope](w, r)
	if !ok {
		return
	}
	job, err := a.Compliance.RequestExport(r.Context(), id, in)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, job)
}

func (a *api) handleListExports(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	out, err := a.Compliance.ListExports(r.Context(), id)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"exports": out})
}

// handleAuditEvents: GET /api/v1/audit/events — the compliance_officer read
// over the raw event log (P-31), keyset-paginated newest first. All filters
// are optional query params; malformed since/until are the only 400s here (the
// verb-length bound and the officer gate live in the domain).
func (a *api) handleAuditEvents(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	q := r.URL.Query()
	f := compliance.AuditFilter{Verb: q.Get("verb")}
	if v, err := strconv.ParseInt(q.Get("entity_type"), 10, 16); err == nil {
		f.EntityType = int16(v)
	}
	f.ActorID, _ = strconv.ParseInt(q.Get("actor_id"), 10, 64)
	f.EntityID, _ = strconv.ParseInt(q.Get("entity_id"), 10, 64)
	f.Cursor, _ = strconv.ParseInt(q.Get("cursor"), 10, 64)
	f.Limit, _ = strconv.Atoi(q.Get("limit"))
	if s := q.Get("since"); s != "" {
		ts, err := time.Parse(time.RFC3339, s)
		if err != nil {
			writeError(w, http.StatusBadRequest, "since must be an RFC3339 timestamp")
			return
		}
		f.Since = &ts
	}
	if s := q.Get("until"); s != "" {
		ts, err := time.Parse(time.RFC3339, s)
		if err != nil {
			writeError(w, http.StatusBadRequest, "until must be an RFC3339 timestamp")
			return
		}
		f.Until = &ts
	}
	page, err := a.Compliance.AuditEvents(r.Context(), id, f)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, page)
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
