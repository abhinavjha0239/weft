// Package identity: orgs, workspaces, users, sessions (ARCHITECTURE.md
// module map). Owns: org, workspace, user_account, user_credential,
// membership, auth_session (mechanics in platform auth).
package identity

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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
			// Only an actual duplicate slug is the client's problem;
			// anything else (e.g. an unmigrated database) is ours.
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23505" { // unique_violation
				return apperr.Conflict("org slug unavailable")
			}
			return apperr.Internal("create org", err)
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

type Profile struct {
	ID          int64  `json:"id"`
	FullName    string `json:"full_name"`
	Kind        int16  `json:"kind"`
	Deactivated bool   `json:"deactivated,omitempty"`
	// User status (ADR-011 N-3): the current unexpired manual status, empty
	// when unset or lapsed. A LEFT JOIN with the expiry filter supplies it.
	StatusEmoji string `json:"emoji,omitempty"`
	StatusText  string `json:"status_text,omitempty"`
	// AvatarFileID is non-null when the user has an avatar (P-06); clients
	// build the URL /api/v1/users/{id}/avatar from its presence.
	AvatarFileID *int64 `json:"avatar_file_id,omitempty"`
}

type MyProfile struct {
	UserID   int64  `json:"user_id"`
	OrgID    int64  `json:"org_id"`
	FullName string `json:"full_name"`
	Email    string `json:"email,omitempty"`
	Role     int16  `json:"role"`
	Kind     int16  `json:"kind"`
}

// Me returns the actor's own profile — the client's boot identity.
func (s *Service) Me(ctx context.Context, actor auth.Identity) (MyProfile, error) {
	var p MyProfile
	err := s.pool.QueryRow(ctx, `
		SELECT id, org_id, full_name, COALESCE(email, ''), role, kind
		FROM user_account WHERE id = $1 AND org_id = $2`,
		actor.UserID, actor.OrgID).Scan(&p.UserID, &p.OrgID, &p.FullName,
		&p.Email, &p.Role, &p.Kind)
	if err != nil {
		return MyProfile{}, apperr.Internal("me", err)
	}
	return p, nil
}

// guestVisibleClause is the P-5 boundary on people-read surfaces: guests
// resolve only themselves and users sharing a live channel membership.
// $3 = viewer id, $4 = viewer-is-guest; non-guests pass everything.
const guestVisibleClause = `(NOT $4 OR u.id = $3 OR EXISTS (
	SELECT 1 FROM channel_member me
	JOIN channel_member them ON them.channel_id = me.channel_id
	WHERE me.user_id = $3 AND me.unsubscribed_at IS NULL
	  AND them.user_id = u.id AND them.unsubscribed_at IS NULL))`

const maxProfileIDs = 100

// Profiles batch-resolves user ids to display data, pinned to the actor's
// org. Deactivated users are included and flagged — they authored history
// that still renders. Unknown or foreign ids are silently absent from the
// result, never an error.
func (s *Service) Profiles(ctx context.Context, actor auth.Identity, ids []int64) ([]Profile, error) {
	if len(ids) == 0 {
		return nil, apperr.Invalid("ids required")
	}
	if len(ids) > maxProfileIDs {
		return nil, apperr.Invalid("too many ids (max 100)")
	}
	rows, err := s.pool.Query(ctx, `
		SELECT u.id, u.full_name, u.kind, u.deactivated_at IS NOT NULL,
		       COALESCE(st.emoji, ''), COALESCE(st.status_text, ''),
		       u.avatar_file_id
		FROM user_account u
		LEFT JOIN user_status st ON st.user_id = u.id
		  AND (st.expires_at IS NULL OR st.expires_at > now())
		WHERE u.org_id = $1 AND u.id = ANY($2)
		  AND `+guestVisibleClause+`
		ORDER BY u.id`, actor.OrgID, ids, actor.UserID, actor.IsGuest())
	if err != nil {
		return nil, apperr.Internal("profiles", err)
	}
	defer rows.Close()
	var out []Profile
	for rows.Next() {
		var p Profile
		if err := rows.Scan(&p.ID, &p.FullName, &p.Kind, &p.Deactivated,
			&p.StatusEmoji, &p.StatusText, &p.AvatarFileID); err != nil {
			return nil, apperr.Internal("scan profile", err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// Directory lists the org's live members (humans and agents) for pickers,
// ordered by name. Capped at 200 — paging arrives when an org that size
// consumes it (the batch ids= form stays the bulk-resolution path).
func (s *Service) Directory(ctx context.Context, actor auth.Identity, limit int) ([]Profile, error) {
	if limit <= 0 || limit > 200 {
		limit = 200
	}
	rows, err := s.pool.Query(ctx, `
		SELECT u.id, u.full_name, u.kind, false,
		       COALESCE(st.emoji, ''), COALESCE(st.status_text, ''),
		       u.avatar_file_id
		FROM user_account u
		LEFT JOIN user_status st ON st.user_id = u.id
		  AND (st.expires_at IS NULL OR st.expires_at > now())
		WHERE u.org_id = $1 AND u.deactivated_at IS NULL AND u.kind IN (1, 2)
		  AND `+guestVisibleClause+`
		ORDER BY lower(u.full_name), u.id
		LIMIT $2`, actor.OrgID, limit, actor.UserID, actor.IsGuest())
	if err != nil {
		return nil, apperr.Internal("directory", err)
	}
	defer rows.Close()
	var out []Profile
	for rows.Next() {
		var p Profile
		if err := rows.Scan(&p.ID, &p.FullName, &p.Kind, &p.Deactivated,
			&p.StatusEmoji, &p.StatusText, &p.AvatarFileID); err != nil {
			return nil, apperr.Internal("scan directory", err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}
