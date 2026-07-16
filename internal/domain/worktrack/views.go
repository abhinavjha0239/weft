package worktrack

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

// Saved views (ADR-008 C-1 / ADR-010 S-4): a board is a saved query + layout.
// v1 is PERSONAL — owner_id is the creator and every read/write is
// owner-scoped, so a foreign or other-owner view is an oracle-free 404.
// Shared/team views and server-side view EXECUTION (running the filter to page
// items) are recorded gaps: a view is a saved query the client applies, and
// ListItems is never auto-filtered by one.
//
// view_def (migration 0005) has no space_id column — a view may span spaces —
// so the validated, org-local space_id is folded into the query JSON.

// viewFilterFields are the item attributes a v1 filter may target (S-4 subset).
var viewFilterFields = map[string]bool{
	"status_id": true, "type_id": true, "assignee_id": true,
	"sprint_id": true, "label": true, "flagged": true,
}

type ViewSummary struct {
	ID        int64           `json:"id"`
	Name      string          `json:"name"`
	Layout    int16           `json:"layout"`
	SpaceID   int64           `json:"space_id"`
	Query     json.RawMessage `json:"query"`
	Config    json.RawMessage `json:"config"`
	OwnerID   int64           `json:"owner_id"`
	CreatedAt time.Time       `json:"created_at"`
}

type viewFilter struct {
	Field string          `json:"field"`
	Op    string          `json:"op"`
	Value json.RawMessage `json:"value"`
}

// storedQuery is the JSONB persisted in view_def.query: the validated space_id
// folded in beside the filter AST (which has no dedicated column).
type storedQuery struct {
	SpaceID int64        `json:"space_id"`
	Filters []viewFilter `json:"filters"`
}

// validateViewQuery parses the client's {"filters":[...]} document, rejecting
// unknown fields/ops (and any stray keys) at write time, and returns the
// normalized, never-nil filter list.
func validateViewQuery(raw json.RawMessage) ([]viewFilter, error) {
	var doc struct {
		Filters []viewFilter `json:"filters"`
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&doc); err != nil {
		return nil, apperr.Invalid("query: " + err.Error())
	}
	for i, f := range doc.Filters {
		if !viewFilterFields[f.Field] {
			return nil, apperr.Invalid(fmt.Sprintf("query: filter %d: unknown field %q", i, f.Field))
		}
		if f.Op != "eq" && f.Op != "in" {
			return nil, apperr.Invalid(fmt.Sprintf("query: filter %d: unknown op %q (want eq or in)", i, f.Op))
		}
	}
	if doc.Filters == nil {
		doc.Filters = []viewFilter{}
	}
	return doc.Filters, nil
}

// buildStoredQuery folds the space_id into the validated filters for storage.
func buildStoredQuery(spaceID int64, filters []viewFilter) (json.RawMessage, error) {
	b, err := json.Marshal(storedQuery{SpaceID: spaceID, Filters: filters})
	if err != nil {
		return nil, apperr.Internal("marshal query", err)
	}
	return b, nil
}

// spaceIDFromQuery reads back the folded space_id from a stored query.
func spaceIDFromQuery(raw json.RawMessage) int64 {
	var q struct {
		SpaceID int64 `json:"space_id"`
	}
	_ = json.Unmarshal(raw, &q)
	return q.SpaceID
}

// normalizeConfig defaults an omitted config to {} so a nil Go slice never
// overrides the column DEFAULT with a SQL NULL.
func normalizeConfig(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return json.RawMessage("{}")
	}
	return raw
}

type CreateViewParams struct {
	Name    string
	Layout  int16
	SpaceID int64
	Query   json.RawMessage
	Config  json.RawMessage
}

// CreateView stores a personal view. The space_id must be org-local (404) and
// the query must validate (400); the space is folded into the stored query.
func (s *Service) CreateView(ctx context.Context, actor auth.Identity, p CreateViewParams) (ViewSummary, error) {
	if p.Name == "" || len(p.Name) > 100 {
		return ViewSummary{}, apperr.Invalid("name 1..100 chars required")
	}
	if p.Layout < 1 || p.Layout > 4 {
		return ViewSummary{}, apperr.Invalid("layout must be 1..4 (list/kanban/timeline/saved-search)")
	}
	filters, err := validateViewQuery(p.Query)
	if err != nil {
		return ViewSummary{}, err
	}
	config := normalizeConfig(p.Config)
	var out ViewSummary
	err = db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := loadSpace(ctx, tx, actor.OrgID, p.SpaceID); err != nil {
			return err
		}
		stored, err := buildStoredQuery(p.SpaceID, filters)
		if err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `
			INSERT INTO view_def (org_id, owner_id, name, layout, query, config)
			VALUES ($1, $2, $3, $4, $5, $6)
			RETURNING id, created_at`,
			actor.OrgID, actor.UserID, p.Name, p.Layout, stored, config).
			Scan(&out.ID, &out.CreatedAt); err != nil {
			return apperr.Internal("create view", err)
		}
		out.Name, out.Layout, out.SpaceID = p.Name, p.Layout, p.SpaceID
		out.Query, out.Config, out.OwnerID = stored, config, actor.UserID
		return nil
	})
	if err != nil {
		return ViewSummary{}, err
	}
	return out, nil
}

// GetView returns one of the caller's own views; anyone else's is a 404.
func (s *Service) GetView(ctx context.Context, actor auth.Identity, id int64) (ViewSummary, error) {
	var v ViewSummary
	err := s.pool.QueryRow(ctx, `
		SELECT id, name, layout, query, config, owner_id, created_at
		FROM view_def
		WHERE id = $1 AND org_id = $2 AND owner_id = $3`,
		id, actor.OrgID, actor.UserID).
		Scan(&v.ID, &v.Name, &v.Layout, &v.Query, &v.Config, &v.OwnerID, &v.CreatedAt)
	if err != nil {
		return ViewSummary{}, apperr.NotFound("view not found")
	}
	v.SpaceID = spaceIDFromQuery(v.Query)
	return v, nil
}

// ListViews returns the caller's own views, oldest first.
func (s *Service) ListViews(ctx context.Context, actor auth.Identity) ([]ViewSummary, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, name, layout, query, config, owner_id, created_at
		FROM view_def
		WHERE org_id = $1 AND owner_id = $2
		ORDER BY id`, actor.OrgID, actor.UserID)
	if err != nil {
		return nil, apperr.Internal("list views", err)
	}
	defer rows.Close()
	var out []ViewSummary
	for rows.Next() {
		var v ViewSummary
		if err := rows.Scan(&v.ID, &v.Name, &v.Layout, &v.Query, &v.Config,
			&v.OwnerID, &v.CreatedAt); err != nil {
			return nil, apperr.Internal("scan view", err)
		}
		v.SpaceID = spaceIDFromQuery(v.Query)
		out = append(out, v)
	}
	return out, rows.Err()
}

type UpdateViewParams struct {
	Name   *string
	Layout *int16
	Query  json.RawMessage
	Config json.RawMessage
}

// UpdateView patches an owned view (name/layout/query/config), re-validating a
// changed query. space_id is not changeable here, so a new query keeps the
// existing folded space. Anyone else's view is a 404.
func (s *Service) UpdateView(ctx context.Context, actor auth.Identity, id int64, p UpdateViewParams) error {
	if p.Name == nil && p.Layout == nil && p.Query == nil && p.Config == nil {
		return apperr.Invalid("nothing to update")
	}
	return db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		var storedQ json.RawMessage
		err := tx.QueryRow(ctx, `
			SELECT query FROM view_def
			WHERE id = $1 AND org_id = $2 AND owner_id = $3 FOR UPDATE`,
			id, actor.OrgID, actor.UserID).Scan(&storedQ)
		if err != nil {
			return apperr.NotFound("view not found")
		}
		if p.Name != nil {
			if *p.Name == "" || len(*p.Name) > 100 {
				return apperr.Invalid("name 1..100 chars required")
			}
			if _, err := tx.Exec(ctx,
				`UPDATE view_def SET name = $2 WHERE id = $1`, id, *p.Name); err != nil {
				return apperr.Internal("rename view", err)
			}
		}
		if p.Layout != nil {
			if *p.Layout < 1 || *p.Layout > 4 {
				return apperr.Invalid("layout must be 1..4 (list/kanban/timeline/saved-search)")
			}
			if _, err := tx.Exec(ctx,
				`UPDATE view_def SET layout = $2 WHERE id = $1`, id, *p.Layout); err != nil {
				return apperr.Internal("relayout view", err)
			}
		}
		if p.Query != nil {
			filters, err := validateViewQuery(p.Query)
			if err != nil {
				return err
			}
			newStored, err := buildStoredQuery(spaceIDFromQuery(storedQ), filters)
			if err != nil {
				return err
			}
			if _, err := tx.Exec(ctx,
				`UPDATE view_def SET query = $2 WHERE id = $1`, id, newStored); err != nil {
				return apperr.Internal("update query", err)
			}
		}
		if p.Config != nil {
			if _, err := tx.Exec(ctx,
				`UPDATE view_def SET config = $2 WHERE id = $1`, id, normalizeConfig(p.Config)); err != nil {
				return apperr.Internal("update config", err)
			}
		}
		return nil
	})
}

// DeleteView removes an owned view; anyone else's is an oracle-free 404.
func (s *Service) DeleteView(ctx context.Context, actor auth.Identity, id int64) error {
	ct, err := s.pool.Exec(ctx,
		`DELETE FROM view_def WHERE id = $1 AND org_id = $2 AND owner_id = $3`,
		id, actor.OrgID, actor.UserID)
	if err != nil {
		return apperr.Internal("delete view", err)
	}
	if ct.RowsAffected() == 0 {
		return apperr.NotFound("view not found")
	}
	return nil
}
