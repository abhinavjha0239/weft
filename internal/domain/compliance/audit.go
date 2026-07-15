package compliance

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

// Audit read API (P-31). A compliance officer reads the org's raw event log —
// the F-4 immutable record — as keyset-paginated pages, newest first. Gated on
// compliance_officer at org scope (F-9: never seeded, so even owners are
// refused without the explicit grant). The read is deliberately NOT a
// consumer: it carries no txid commit-order gate, so a late-committing row may
// appear on a later page, which is fine for a historical read.

const (
	auditDefaultLimit = 50
	auditMaxLimit     = 200
	auditMaxVerbLen   = 64
)

// AuditFilter narrows the read. All filters AND-compose; the zero value of
// each numeric filter (and an empty verb) means "no filter on that field".
type AuditFilter struct {
	EntityType int16
	Verb       string
	ActorID    int64
	EntityID   int64
	Since      *time.Time // occurred_at >= Since
	Until      *time.Time // occurred_at <  Until
	Cursor     int64      // id < Cursor (keyset page boundary)
	Limit      int        // clamped to 1..200 (default 50)
}

// AuditEvent is one event-log row as the officer sees it. hint is deliberately
// excluded (delivery-routing noise, not audit data); payload and origin ride
// through verbatim as raw JSON (F-4 guarantees structural deltas + revision
// references, never the only copy of content).
type AuditEvent struct {
	ID          int64           `json:"id"`
	OccurredAt  time.Time       `json:"occurred_at"`
	RecordedAt  time.Time       `json:"recorded_at"`
	ActorKind   int16           `json:"actor_kind"`
	ActorID     *int64          `json:"actor_id,omitempty"`
	EntityType  int16           `json:"entity_type"`
	EntityID    int64           `json:"entity_id"`
	Verb        string          `json:"verb"`
	WorkspaceID *int64          `json:"workspace_id,omitempty"`
	Payload     json.RawMessage `json:"payload"`
	Origin      json.RawMessage `json:"origin"`
}

// AuditPage is one page plus the keyset cursor for the next one (0 when the
// page was not full — there is nothing more to fetch).
type AuditPage struct {
	Events     []AuditEvent `json:"events"`
	NextCursor int64        `json:"next_cursor"`
}

// AuditEvents returns one officer-gated page of the org's event log, newest
// first. The dynamic WHERE is built with parameterized placeholders (the
// search add() precedent) — filter VALUES are never interpolated into SQL.
func (s *Service) AuditEvents(ctx context.Context, actor auth.Identity, f AuditFilter) (AuditPage, error) {
	if len(f.Verb) > auditMaxVerbLen {
		return AuditPage{}, apperr.Invalid("verb filter too long (max 64 characters)")
	}
	limit := f.Limit
	if limit <= 0 {
		limit = auditDefaultLimit
	}
	if limit > auditMaxLimit {
		limit = auditMaxLimit
	}
	out := AuditPage{Events: []AuditEvent{}}
	err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		// F-9: compliance standing is an explicit grant, never adminship.
		if err := s.perms.Require(ctx, tx, actor, perms.VerbComplianceOfficer,
			perms.OrgScope(actor.OrgID)); err != nil {
			return err
		}
		var args []any
		add := func(v any) string {
			args = append(args, v)
			return "$" + strconv.Itoa(len(args))
		}
		var sb strings.Builder
		sb.WriteString(`SELECT id, occurred_at, recorded_at, actor_kind, actor_id,
			entity_type, entity_id, verb, workspace_id, payload, origin
			FROM event_log WHERE org_id = ` + add(actor.OrgID))
		if f.EntityType > 0 {
			sb.WriteString(" AND entity_type = " + add(f.EntityType))
		}
		if f.Verb != "" {
			sb.WriteString(" AND verb = " + add(f.Verb))
		}
		if f.ActorID > 0 {
			sb.WriteString(" AND actor_id = " + add(f.ActorID))
		}
		if f.EntityID > 0 {
			sb.WriteString(" AND entity_id = " + add(f.EntityID))
		}
		if f.Since != nil {
			sb.WriteString(" AND occurred_at >= " + add(*f.Since))
		}
		if f.Until != nil {
			sb.WriteString(" AND occurred_at < " + add(*f.Until))
		}
		if f.Cursor > 0 {
			sb.WriteString(" AND id < " + add(f.Cursor))
		}
		// Newest first, riding (org_id, id). No txid gate — a historical read,
		// not a consumer, so a late-committing row may surface on a later page.
		sb.WriteString(" ORDER BY id DESC LIMIT " + add(limit))
		rows, err := tx.Query(ctx, sb.String(), args...)
		if err != nil {
			return apperr.Internal("audit query", err)
		}
		defer rows.Close()
		for rows.Next() {
			var e AuditEvent
			var payload, origin []byte
			if err := rows.Scan(&e.ID, &e.OccurredAt, &e.RecordedAt, &e.ActorKind,
				&e.ActorID, &e.EntityType, &e.EntityID, &e.Verb, &e.WorkspaceID,
				&payload, &origin); err != nil {
				return apperr.Internal("scan audit event", err)
			}
			e.Payload = payload
			e.Origin = origin
			out.Events = append(out.Events, e)
		}
		return rows.Err()
	})
	if err != nil {
		return AuditPage{}, err
	}
	// A full page means there may be more; the cursor is its last (smallest) id.
	if len(out.Events) == limit {
		out.NextCursor = out.Events[len(out.Events)-1].ID
	}
	return out, nil
}
