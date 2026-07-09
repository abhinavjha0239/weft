// Package identity: orgs, workspaces, users, sessions (ARCHITECTURE.md
// module map). Owns: org, workspace, user_account, user_credential,
// membership, auth_session (mechanics in platform auth).
package identity

import (
	"context"

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
}

func New(pool *pgxpool.Pool, p *perms.Service) *Service {
	return &Service{pool: pool, perms: p}
}

type BootstrapParams struct {
	OrgName  string
	OrgSlug  string
	Email    string
	Password string
	FullName string
}

type BootstrapResult struct {
	OrgID       int64  `json:"org_id"`
	WorkspaceID int64  `json:"workspace_id"`
	UserID      int64  `json:"user_id"`
	ChannelID   int64  `json:"channel_id"`
	Token       string `json:"token"`
}

// Bootstrap creates org + workspace + owner + #general (with its channel-root
// thread, F-15) in one transaction and mints a session.
func (s *Service) Bootstrap(ctx context.Context, p BootstrapParams) (BootstrapResult, error) {
	if p.OrgSlug == "" || p.Email == "" || len(p.Password) < 8 {
		return BootstrapResult{}, apperr.Invalid("org_slug, email, password (min 8) required")
	}
	pwHash, err := auth.HashPassword(p.Password)
	if err != nil {
		return BootstrapResult{}, apperr.Internal("hash password", err)
	}
	if p.OrgName == "" {
		p.OrgName = p.OrgSlug
	}
	if p.FullName == "" {
		p.FullName = p.Email
	}

	var out BootstrapResult
	err = db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx,
			`INSERT INTO org (name, slug) VALUES ($1, $2) RETURNING id`,
			p.OrgName, p.OrgSlug).Scan(&out.OrgID); err != nil {
			return apperr.Conflict("org slug unavailable")
		}
		if err := tx.QueryRow(ctx,
			`INSERT INTO workspace (org_id, name, slug) VALUES ($1, 'General', 'general') RETURNING id`,
			out.OrgID).Scan(&out.WorkspaceID); err != nil {
			return apperr.Internal("create workspace", err)
		}
		if err := tx.QueryRow(ctx, `
			INSERT INTO user_account (org_id, kind, email, full_name, role)
			VALUES ($1, $2, $3, $4, 10) RETURNING id`,
			out.OrgID, enum.UserHuman, p.Email, p.FullName).Scan(&out.UserID); err != nil {
			return apperr.Internal("create user", err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO user_credential (user_id, password_hash) VALUES ($1, $2)`,
			out.UserID, pwHash); err != nil {
			return apperr.Internal("store credential", err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO membership (user_id, workspace_id, role) VALUES ($1, $2, 10)`,
			out.UserID, out.WorkspaceID); err != nil {
			return apperr.Internal("create membership", err)
		}
		// System role groups + default verb assignments + closure (ADR-006).
		if err := s.perms.SeedOrg(ctx, tx, out.OrgID, out.UserID); err != nil {
			return err
		}
		var rootThreadID int64
		if err := tx.QueryRow(ctx, `
			INSERT INTO channel (org_id, workspace_id, name, creator_id)
			VALUES ($1, $2, 'general', $3) RETURNING id`,
			out.OrgID, out.WorkspaceID, out.UserID).Scan(&out.ChannelID); err != nil {
			return apperr.Internal("create channel", err)
		}
		if err := tx.QueryRow(ctx,
			`INSERT INTO thread (org_id, channel_id, kind) VALUES ($1, $2, 2) RETURNING id`,
			out.OrgID, out.ChannelID).Scan(&rootThreadID); err != nil {
			return apperr.Internal("create root thread", err)
		}
		if _, err := tx.Exec(ctx,
			`UPDATE channel SET root_thread_id = $1 WHERE id = $2`,
			rootThreadID, out.ChannelID); err != nil {
			return apperr.Internal("bind root thread", err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO channel_member (channel_id, user_id) VALUES ($1, $2)`,
			out.ChannelID, out.UserID); err != nil {
			return apperr.Internal("join channel", err)
		}
		if _, err := eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: out.OrgID, WorkspaceID: &out.WorkspaceID,
			ActorKind: enum.ActorHuman, ActorID: &out.UserID,
			EntityType: enum.EntityChannel, EntityID: out.ChannelID,
			Verb:    "channel.created",
			Payload: eventlog.MustPayload(map[string]any{"channel_id": out.ChannelID, "name": "general"}),
		}); err != nil {
			return apperr.Internal("append event", err)
		}
		token, err := auth.CreateSession(ctx, tx, out.UserID)
		if err != nil {
			return apperr.Internal("create session", err)
		}
		out.Token = token
		return nil
	})
	if err != nil {
		return BootstrapResult{}, err
	}
	return out, nil
}

// Login verifies credentials and mints a session.
func (s *Service) Login(ctx context.Context, orgSlug, email, password string) (string, error) {
	token, err := auth.Login(ctx, s.pool, orgSlug, email, password)
	if err != nil {
		return "", apperr.Unauthorized("invalid credentials")
	}
	return token, nil
}
