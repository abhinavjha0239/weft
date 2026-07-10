// Package compliance wakes the ADR-013 subsystem: retention policies (AD-3),
// legal holds (AD-4), and the retention-enforcement lane that acts on them.
// Every operation here is gated on the compliance_officer verb — never
// seeded (F-9), so even org owners must be explicitly granted the standing,
// and every grant/act is event-logged.
//
// Module-ownership note (ARCHITECTURE.md): compliance owns retention_policy
// and legal_hold, and it ALSO owns lifecycle-enforcement writes over other
// domains' rows (file purge, revision scrub) — deletion policy lives in one
// module, by design. Interactive writes stay with their owning domains.
package compliance

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/internal/eventlog"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

type Service struct {
	pool  *pgxpool.Pool
	perms *perms.Service
	// files backs the export lane (SetFiles); nil until wired.
	files      ExportStore
	exportWake chan struct{}
	logger     *slog.Logger
}

func New(pool *pgxpool.Pool, p *perms.Service) *Service {
	return &Service{pool: pool, perms: p, exportWake: make(chan struct{}, 1)}
}

// SetLogger overrides the default logger (weftd wires its own).
func (s *Service) SetLogger(l *slog.Logger) { s.logger = l }

func (s *Service) log() *slog.Logger {
	if s.logger != nil {
		return s.logger
	}
	return slog.Default()
}

// Retention scope types (retention_policy.scope_type). The AD-3 ladder is
// org → workspace → channel/space/dm; v1 accepts the rungs the enforcement
// lane resolves (org default, channel override) and rejects the rest rather
// than storing rows nothing honors.
const (
	ScopeOrg     int16 = 1
	ScopeChannel int16 = 3
)

// DurationForever keeps content indefinitely (duration_days = -1).
const DurationForever = -1

type RetentionPolicy struct {
	ScopeType    int16 `json:"scope_type"`
	ScopeID      int64 `json:"scope_id"`
	DurationDays int32 `json:"duration_days"`
	KeepEdits    bool  `json:"keep_edits"`
}

// SetRetentionPolicy upserts the policy for one scope. duration_days is -1
// (forever) or a positive day count; keep_edits=false marks prior message
// versions for the scrub lane.
func (s *Service) SetRetentionPolicy(ctx context.Context, actor auth.Identity, p RetentionPolicy) error {
	if p.DurationDays != DurationForever && (p.DurationDays < 1 || p.DurationDays > 36500) {
		return apperr.Invalid("duration_days must be -1 (forever) or 1..36500")
	}
	return db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.perms.Require(ctx, tx, actor, perms.VerbComplianceOfficer,
			perms.OrgScope(actor.OrgID)); err != nil {
			return err
		}
		entityType := enum.EntityOrg
		entityID := actor.OrgID
		switch p.ScopeType {
		case ScopeOrg:
			if p.ScopeID != actor.OrgID {
				return apperr.Invalid("org-scope policy must target your org")
			}
		case ScopeChannel:
			var ok bool
			if err := tx.QueryRow(ctx, `
				SELECT EXISTS (SELECT 1 FROM channel WHERE id = $1 AND org_id = $2)`,
				p.ScopeID, actor.OrgID).Scan(&ok); err != nil {
				return apperr.Internal("channel lookup", err)
			}
			if !ok {
				return apperr.NotFound("channel not found")
			}
			entityType, entityID = enum.EntityChannel, p.ScopeID
		default:
			return apperr.Invalid("scope_type must be 1 (org) or 3 (channel) — workspace/space/dm scopes arrive with those policy consumers")
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO retention_policy (org_id, scope_type, scope_id, duration_days, keep_edits)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (org_id, scope_type, scope_id)
			DO UPDATE SET duration_days = EXCLUDED.duration_days,
			              keep_edits = EXCLUDED.keep_edits`,
			actor.OrgID, p.ScopeType, p.ScopeID, p.DurationDays, p.KeepEdits); err != nil {
			return apperr.Internal("set retention policy", err)
		}
		_, err := eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
			EntityType: entityType, EntityID: entityID, Verb: "retention.policy_set",
			Payload: eventlog.MustPayload(map[string]any{
				"scope_type": p.ScopeType, "scope_id": p.ScopeID,
				"duration_days": p.DurationDays, "keep_edits": p.KeepEdits}),
		})
		return err
	})
}

// ListRetentionPolicies returns the org's policies, org default first.
func (s *Service) ListRetentionPolicies(ctx context.Context, actor auth.Identity) ([]RetentionPolicy, error) {
	out := []RetentionPolicy{}
	err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.perms.Require(ctx, tx, actor, perms.VerbComplianceOfficer,
			perms.OrgScope(actor.OrgID)); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `
			SELECT scope_type, scope_id, duration_days, keep_edits
			FROM retention_policy WHERE org_id = $1
			ORDER BY scope_type, scope_id`, actor.OrgID)
		if err != nil {
			return apperr.Internal("list retention policies", err)
		}
		defer rows.Close()
		for rows.Next() {
			var p RetentionPolicy
			if err := rows.Scan(&p.ScopeType, &p.ScopeID, &p.DurationDays, &p.KeepEdits); err != nil {
				return apperr.Internal("scan retention policy", err)
			}
			out = append(out, p)
		}
		return rows.Err()
	})
	return out, err
}

type LegalHold struct {
	ID              int64      `json:"hold_id"`
	Name            string     `json:"name"`
	CustodianUserID *int64     `json:"custodian_user_id,omitempty"`
	ChannelID       *int64     `json:"channel_id,omitempty"`
	CreatedBy       int64      `json:"created_by"`
	CreatedAt       time.Time  `json:"created_at"`
	ReleasedAt      *time.Time `json:"released_at,omitempty"`
}

// CreateLegalHold freezes matching content against retention and GC (AD-4).
// v1 scopes: per-custodian (the primary model) and/or per-channel; space and
// query scopes arrive with their enforcement.
func (s *Service) CreateLegalHold(ctx context.Context, actor auth.Identity, name string, custodianUserID, channelID *int64) (LegalHold, error) {
	if name == "" {
		return LegalHold{}, apperr.Invalid("name required")
	}
	if custodianUserID == nil && channelID == nil {
		return LegalHold{}, apperr.Invalid("a custodian_user_id or channel_id scope is required")
	}
	var h LegalHold
	err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.perms.Require(ctx, tx, actor, perms.VerbComplianceOfficer,
			perms.OrgScope(actor.OrgID)); err != nil {
			return err
		}
		if custodianUserID != nil {
			var ok bool
			if err := tx.QueryRow(ctx, `
				SELECT EXISTS (SELECT 1 FROM user_account WHERE id = $1 AND org_id = $2)`,
				*custodianUserID, actor.OrgID).Scan(&ok); err != nil {
				return apperr.Internal("custodian lookup", err)
			}
			if !ok {
				return apperr.NotFound("custodian not found")
			}
		}
		if channelID != nil {
			var ok bool
			if err := tx.QueryRow(ctx, `
				SELECT EXISTS (SELECT 1 FROM channel WHERE id = $1 AND org_id = $2)`,
				*channelID, actor.OrgID).Scan(&ok); err != nil {
				return apperr.Internal("channel lookup", err)
			}
			if !ok {
				return apperr.NotFound("channel not found")
			}
		}
		if err := tx.QueryRow(ctx, `
			INSERT INTO legal_hold (org_id, name, custodian_user_id, channel_id, created_by)
			VALUES ($1, $2, $3, $4, $5)
			RETURNING id, created_at`,
			actor.OrgID, name, custodianUserID, channelID, actor.UserID).Scan(&h.ID, &h.CreatedAt); err != nil {
			return apperr.Internal("create hold", err)
		}
		h.Name, h.CustodianUserID, h.ChannelID, h.CreatedBy = name, custodianUserID, channelID, actor.UserID
		_, err := eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
			EntityType: enum.EntityLegalHold, EntityID: h.ID, Verb: "legalhold.created",
			Payload: eventlog.MustPayload(map[string]any{
				"hold_id": h.ID, "name": name,
				"custodian_user_id": custodianUserID, "channel_id": channelID}),
		})
		return err
	})
	if err != nil {
		return LegalHold{}, err
	}
	return h, nil
}

// ReleaseLegalHold ends a hold. Holds are never deleted — release is an
// audited state change, and releasing twice is a conflict, not a no-op.
func (s *Service) ReleaseLegalHold(ctx context.Context, actor auth.Identity, holdID int64) error {
	return db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.perms.Require(ctx, tx, actor, perms.VerbComplianceOfficer,
			perms.OrgScope(actor.OrgID)); err != nil {
			return err
		}
		ct, err := tx.Exec(ctx, `
			UPDATE legal_hold SET released_by = $3, released_at = now()
			WHERE id = $1 AND org_id = $2 AND released_at IS NULL`,
			holdID, actor.OrgID, actor.UserID)
		if err != nil {
			return apperr.Internal("release hold", err)
		}
		if ct.RowsAffected() == 0 {
			var exists bool
			if err := tx.QueryRow(ctx, `
				SELECT EXISTS (SELECT 1 FROM legal_hold WHERE id = $1 AND org_id = $2)`,
				holdID, actor.OrgID).Scan(&exists); err != nil {
				return apperr.Internal("hold lookup", err)
			}
			if !exists {
				return apperr.NotFound("hold not found")
			}
			return apperr.Conflict("hold already released")
		}
		_, err = eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
			EntityType: enum.EntityLegalHold, EntityID: holdID, Verb: "legalhold.released",
			Payload: eventlog.MustPayload(map[string]any{"hold_id": holdID}),
		})
		return err
	})
}

// ListLegalHolds returns the org's holds, newest first, released included
// (they are the audit record).
func (s *Service) ListLegalHolds(ctx context.Context, actor auth.Identity) ([]LegalHold, error) {
	out := []LegalHold{}
	err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.perms.Require(ctx, tx, actor, perms.VerbComplianceOfficer,
			perms.OrgScope(actor.OrgID)); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `
			SELECT id, name, custodian_user_id, channel_id, created_by, created_at, released_at
			FROM legal_hold WHERE org_id = $1 ORDER BY id DESC`, actor.OrgID)
		if err != nil {
			return apperr.Internal("list holds", err)
		}
		defer rows.Close()
		for rows.Next() {
			var h LegalHold
			if err := rows.Scan(&h.ID, &h.Name, &h.CustodianUserID, &h.ChannelID,
				&h.CreatedBy, &h.CreatedAt, &h.ReleasedAt); err != nil {
				return apperr.Internal("scan hold", err)
			}
			out = append(out, h)
		}
		return rows.Err()
	})
	return out, err
}
