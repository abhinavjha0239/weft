package messaging

import (
	"context"
	"errors"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/internal/eventlog"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

// P-09: channel folders (a workspace-admin grouping for the channel sidebar)
// and default channels (the set a new member auto-joins on invite accept).
// Both are WORKSPACE-scoped in the schema; v1 has no workspace-selection API,
// so "the workspace" is resolved server-side as the org's bootstrap workspace
// (resolveWorkspace) — the API stays workspace-implicit until the org-hierarchy
// UX threads workspace_id through. The default_channel `bundle` column (C-3
// DefaultChannelGroup) stays NULL/dormant: v1 has only the "always" bundle.

const (
	maxFolderNameLen   = 60
	maxDefaultChannels = 20
)

// resolveWorkspace picks the org's bootstrap (first) workspace. This is the one
// documented v1 reduction: a single implicit workspace per org (org-hierarchy
// UX is Tier-2). Every folder/default operation scopes to it.
func (s *Service) resolveWorkspace(ctx context.Context, tx pgx.Tx, orgID int64) (int64, error) {
	var wsID int64
	err := tx.QueryRow(ctx,
		`SELECT id FROM workspace WHERE org_id = $1 ORDER BY id LIMIT 1`, orgID).Scan(&wsID)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, apperr.Internal("resolve workspace", errors.New("org has no workspace"))
	}
	if err != nil {
		return 0, apperr.Internal("resolve workspace", err)
	}
	return wsID, nil
}

type Folder struct {
	ID       int64  `json:"id"`
	Name     string `json:"name"`
	Position int    `json:"position"`
}

func validateFolderName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" || len(name) > maxFolderNameLen {
		return "", apperr.Invalid("folder name must be 1-60 characters")
	}
	return name, nil
}

// CreateFolder appends a folder to the resolved workspace (position = append
// order). manage_channel_folders-gated.
func (s *Service) CreateFolder(ctx context.Context, actor auth.Identity, name string) (Folder, error) {
	name, err := validateFolderName(name)
	if err != nil {
		return Folder{}, err
	}
	out := Folder{Name: name}
	err = db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.perms.Require(ctx, tx, actor, perms.VerbManageChannelFolders,
			perms.OrgScope(actor.OrgID)); err != nil {
			return err
		}
		wsID, err := s.resolveWorkspace(ctx, tx, actor.OrgID)
		if err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `
			INSERT INTO channel_folder (org_id, workspace_id, name, position)
			VALUES ($1, $2, $3,
				COALESCE((SELECT max(position) + 1 FROM channel_folder
				          WHERE org_id = $1 AND workspace_id = $2), 0))
			RETURNING id, position`,
			actor.OrgID, wsID, name).Scan(&out.ID, &out.Position); err != nil {
			return apperr.Internal("create folder", err)
		}
		return appendFolderEvent(ctx, tx, actor, wsID, out.ID, "folder.created",
			map[string]any{"folder_id": out.ID, "name": name})
	})
	if err != nil {
		return Folder{}, err
	}
	return out, nil
}

// ListFolders returns the resolved workspace's folders, position then id.
func (s *Service) ListFolders(ctx context.Context, actor auth.Identity) ([]Folder, error) {
	var out []Folder
	err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.perms.Require(ctx, tx, actor, perms.VerbManageChannelFolders,
			perms.OrgScope(actor.OrgID)); err != nil {
			return err
		}
		wsID, err := s.resolveWorkspace(ctx, tx, actor.OrgID)
		if err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `
			SELECT id, name, position FROM channel_folder
			WHERE org_id = $1 AND workspace_id = $2
			ORDER BY position, id`, actor.OrgID, wsID)
		if err != nil {
			return apperr.Internal("list folders", err)
		}
		defer rows.Close()
		for rows.Next() {
			var f Folder
			if err := rows.Scan(&f.ID, &f.Name, &f.Position); err != nil {
				return apperr.Internal("scan folder", err)
			}
			out = append(out, f)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// UpdateFolder renames a folder in the resolved workspace.
// manage_channel_folders-gated.
func (s *Service) UpdateFolder(ctx context.Context, actor auth.Identity, folderID int64, name string) error {
	name, err := validateFolderName(name)
	if err != nil {
		return err
	}
	return db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.perms.Require(ctx, tx, actor, perms.VerbManageChannelFolders,
			perms.OrgScope(actor.OrgID)); err != nil {
			return err
		}
		wsID, err := s.resolveWorkspace(ctx, tx, actor.OrgID)
		if err != nil {
			return err
		}
		ct, err := tx.Exec(ctx, `
			UPDATE channel_folder SET name = $1
			WHERE id = $2 AND org_id = $3 AND workspace_id = $4`,
			name, folderID, actor.OrgID, wsID)
		if err != nil {
			return apperr.Internal("update folder", err)
		}
		if ct.RowsAffected() == 0 {
			return apperr.NotFound("folder not found")
		}
		return appendFolderEvent(ctx, tx, actor, wsID, folderID, "folder.updated",
			map[string]any{"folder_id": folderID, "name": name})
	})
}

// DeleteFolder hard-deletes a folder and clears folder_id on its member
// channels in the same transaction (the schema FK forbids orphan references,
// and folders carry no lifecycle beyond existence). manage_channel_folders-gated.
func (s *Service) DeleteFolder(ctx context.Context, actor auth.Identity, folderID int64) error {
	return db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.perms.Require(ctx, tx, actor, perms.VerbManageChannelFolders,
			perms.OrgScope(actor.OrgID)); err != nil {
			return err
		}
		wsID, err := s.resolveWorkspace(ctx, tx, actor.OrgID)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`UPDATE channel SET folder_id = NULL WHERE folder_id = $1 AND org_id = $2`,
			folderID, actor.OrgID); err != nil {
			return apperr.Internal("clear folder members", err)
		}
		ct, err := tx.Exec(ctx,
			`DELETE FROM channel_folder WHERE id = $1 AND org_id = $2 AND workspace_id = $3`,
			folderID, actor.OrgID, wsID)
		if err != nil {
			return apperr.Internal("delete folder", err)
		}
		if ct.RowsAffected() == 0 {
			return apperr.NotFound("folder not found")
		}
		return appendFolderEvent(ctx, tx, actor, wsID, folderID, "folder.deleted",
			map[string]any{"folder_id": folderID})
	})
}

func appendFolderEvent(ctx context.Context, tx pgx.Tx, actor auth.Identity, wsID, folderID int64, verb string, payload map[string]any) error {
	if _, err := eventlog.Append(ctx, tx, eventlog.Event{
		OrgID: actor.OrgID, WorkspaceID: &wsID,
		ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
		EntityType: enum.EntityChannelFolder, EntityID: folderID, Verb: verb,
		Payload: eventlog.MustPayload(payload),
	}); err != nil {
		return apperr.Internal("append event", err)
	}
	return nil
}

// SetDefaultChannels replaces the resolved workspace's default-channel set (the
// always-bundle, bundle IS NULL). Each channel must be a PUBLIC, live channel
// in that workspace — a private, archived, or foreign channel is rejected. A
// new MEMBER auto-joins this set on invite accept (identity.AcceptInvite).
// manage_channel_folders-gated (the folder verb covers the whole sidebar
// surface: both are workspace channel-organisation config).
func (s *Service) SetDefaultChannels(ctx context.Context, actor auth.Identity, channelIDs []int64) error {
	distinct := dedupInt64(channelIDs)
	if len(distinct) > maxDefaultChannels {
		return apperr.Invalid("too many default channels (max 20)")
	}
	return db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.perms.Require(ctx, tx, actor, perms.VerbManageChannelFolders,
			perms.OrgScope(actor.OrgID)); err != nil {
			return err
		}
		wsID, err := s.resolveWorkspace(ctx, tx, actor.OrgID)
		if err != nil {
			return err
		}
		if len(distinct) > 0 {
			var eligible int
			if err := tx.QueryRow(ctx, `
				SELECT count(*) FROM channel
				WHERE org_id = $1 AND workspace_id = $2 AND visibility = $3
				  AND archived_at IS NULL AND id = ANY($4)`,
				actor.OrgID, wsID, visibilityPublic, distinct).Scan(&eligible); err != nil {
				return apperr.Internal("validate default channels", err)
			}
			if eligible != len(distinct) {
				return apperr.Invalid("default channels must be public, live channels in this workspace")
			}
		}
		if _, err := tx.Exec(ctx,
			`DELETE FROM default_channel WHERE workspace_id = $1 AND bundle IS NULL`,
			wsID); err != nil {
			return apperr.Internal("clear default channels", err)
		}
		for _, cid := range distinct {
			if _, err := tx.Exec(ctx, `
				INSERT INTO default_channel (workspace_id, channel_id)
				VALUES ($1, $2) ON CONFLICT DO NOTHING`, wsID, cid); err != nil {
				return apperr.Internal("set default channel", err)
			}
		}
		return nil
	})
}

// DefaultChannelIDs lists the resolved workspace's always-bundle default
// channels. manage_channel_folders-gated (an admin-config surface).
func (s *Service) DefaultChannelIDs(ctx context.Context, actor auth.Identity) ([]int64, error) {
	var out []int64
	err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.perms.Require(ctx, tx, actor, perms.VerbManageChannelFolders,
			perms.OrgScope(actor.OrgID)); err != nil {
			return err
		}
		wsID, err := s.resolveWorkspace(ctx, tx, actor.OrgID)
		if err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `
			SELECT channel_id FROM default_channel
			WHERE workspace_id = $1 AND bundle IS NULL
			ORDER BY channel_id`, wsID)
		if err != nil {
			return apperr.Internal("list default channels", err)
		}
		defer rows.Close()
		for rows.Next() {
			var cid int64
			if err := rows.Scan(&cid); err != nil {
				return apperr.Internal("scan default channel", err)
			}
			out = append(out, cid)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func dedupInt64(in []int64) []int64 {
	seen := make(map[int64]bool, len(in))
	out := make([]int64, 0, len(in))
	for _, v := range in {
		if !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	return out
}
