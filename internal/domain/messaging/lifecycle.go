package messaging

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/internal/eventlog"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

// Channel lifecycle (ADR-008 + F-22): rename reserves the old name in
// channel_name_alias so links and habits never break — a reserved name can
// only be taken back by the channel it belonged to; release is a deliberate
// admin action (a later slice). Archive freezes writes and hides the
// channel from lists while history stays readable to members; the live-name
// index frees an archived channel's name by design, so unarchive re-checks
// availability.

type UpdateChannelParams struct {
	Name        *string
	Description *string
	Archived    *bool
	// FolderIDSet distinguishes an absent folder_id (leave as-is) from an
	// explicit one; when set, a nil FolderID clears the assignment (P-09).
	FolderIDSet bool
	FolderID    *int64
}

func (s *Service) UpdateChannel(ctx context.Context, actor auth.Identity, channelID int64, p UpdateChannelParams) error {
	if p.Name == nil && p.Description == nil && p.Archived == nil && !p.FolderIDSet {
		return apperr.Invalid("nothing to update")
	}
	if p.Name != nil {
		n := strings.TrimSpace(strings.TrimPrefix(*p.Name, "#"))
		if n == "" || len(n) > 80 {
			return apperr.Invalid("channel name must be 1-80 characters")
		}
		p.Name = &n
	}
	return db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		var wsID *int64
		var curName string
		var archived bool
		err := tx.QueryRow(ctx, `
			SELECT workspace_id, name, archived_at IS NOT NULL FROM channel
			WHERE id = $1 AND org_id = $2`, channelID, actor.OrgID).
			Scan(&wsID, &curName, &archived)
		if err != nil {
			return apperr.NotFound("channel not found")
		}
		chain, err := s.perms.ChannelScope(ctx, tx, actor.OrgID, channelID)
		if err != nil {
			return err
		}
		if err := s.perms.Require(ctx, tx, actor, perms.VerbAdministerChannel, chain); err != nil {
			return err
		}
		ws := int64(0)
		if wsID != nil {
			ws = *wsID
		}

		if p.Name != nil && !strings.EqualFold(*p.Name, curName) {
			if err := s.renameChannel(ctx, tx, actor, channelID, ws, wsID, curName, *p.Name); err != nil {
				return err
			}
		} else if p.Name != nil && *p.Name != curName {
			// Pure case change: same lowered identity, no reservation needed.
			if _, err := tx.Exec(ctx,
				`UPDATE channel SET name = $1 WHERE id = $2`, *p.Name, channelID); err != nil {
				return apperr.Internal("rename", err)
			}
		}

		if p.Description != nil {
			if _, err := tx.Exec(ctx,
				`UPDATE channel SET description = $1 WHERE id = $2`,
				strings.TrimSpace(*p.Description), channelID); err != nil {
				return apperr.Internal("describe", err)
			}
			if _, err := eventlog.Append(ctx, tx, eventlog.Event{
				OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
				EntityType: enum.EntityChannel, EntityID: channelID, Verb: "channel.updated",
				Payload: eventlog.MustPayload(map[string]any{"channel_id": channelID}),
			}); err != nil {
				return apperr.Internal("append event", err)
			}
		}

		if p.Archived != nil && *p.Archived != archived {
			if *p.Archived {
				if _, err := tx.Exec(ctx,
					`UPDATE channel SET archived_at = now() WHERE id = $1`, channelID); err != nil {
					return apperr.Internal("archive", err)
				}
				if _, err := eventlog.Append(ctx, tx, eventlog.Event{
					OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
					EntityType: enum.EntityChannel, EntityID: channelID, Verb: "channel.archived",
					Payload: eventlog.MustPayload(map[string]any{"channel_id": channelID}),
				}); err != nil {
					return apperr.Internal("append event", err)
				}
			} else {
				// The live-name index freed this name at archive time; someone
				// may hold it now.
				name := curName
				if p.Name != nil {
					name = *p.Name
				}
				var taken bool
				if err := tx.QueryRow(ctx, `
					SELECT EXISTS (SELECT 1 FROM channel
					 WHERE org_id = $1 AND COALESCE(workspace_id, 0) = $2
					   AND lower(name) = lower($3) AND archived_at IS NULL AND id <> $4)`,
					actor.OrgID, ws, name, channelID).Scan(&taken); err != nil {
					return apperr.Internal("name check", err)
				}
				if taken {
					return apperr.Conflict("the channel's name is now in use — rename it in the same request to unarchive")
				}
				if _, err := tx.Exec(ctx,
					`UPDATE channel SET archived_at = NULL WHERE id = $1`, channelID); err != nil {
					return apperr.Internal("unarchive", err)
				}
				if _, err := eventlog.Append(ctx, tx, eventlog.Event{
					OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
					EntityType: enum.EntityChannel, EntityID: channelID, Verb: "channel.unarchived",
					Payload: eventlog.MustPayload(map[string]any{"channel_id": channelID}),
				}); err != nil {
					return apperr.Internal("append event", err)
				}
			}
		}

		if p.FolderIDSet {
			if p.FolderID != nil {
				// The folder must be a live folder in the same (resolved)
				// workspace — a foreign or cross-workspace folder is rejected.
				resolvedWS, err := s.resolveWorkspace(ctx, tx, actor.OrgID)
				if err != nil {
					return err
				}
				var ok bool
				if err := tx.QueryRow(ctx, `
					SELECT EXISTS (SELECT 1 FROM channel_folder
					 WHERE id = $1 AND org_id = $2 AND workspace_id = $3)`,
					*p.FolderID, actor.OrgID, resolvedWS).Scan(&ok); err != nil {
					return apperr.Internal("folder check", err)
				}
				if !ok {
					return apperr.Invalid("folder not found in this workspace")
				}
			}
			if _, err := tx.Exec(ctx,
				`UPDATE channel SET folder_id = $1 WHERE id = $2`,
				p.FolderID, channelID); err != nil {
				return apperr.Internal("assign folder", err)
			}
			if _, err := eventlog.Append(ctx, tx, eventlog.Event{
				OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
				EntityType: enum.EntityChannel, EntityID: channelID, Verb: "channel.updated",
				Payload: eventlog.MustPayload(map[string]any{"channel_id": channelID}),
			}); err != nil {
				return apperr.Internal("append event", err)
			}
		}
		return nil
	})
}

// renameChannel enforces F-22 inside the transaction: the target name must
// be free among live channels AND not reserved for another channel; the old
// name is reserved for this one on the way out.
func (s *Service) renameChannel(ctx context.Context, tx pgx.Tx, actor auth.Identity, channelID, ws int64, wsID *int64, oldName, newName string) error {
	var taken bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM channel
		 WHERE org_id = $1 AND COALESCE(workspace_id, 0) = $2
		   AND lower(name) = lower($3) AND archived_at IS NULL AND id <> $4)`,
		actor.OrgID, ws, newName, channelID).Scan(&taken); err != nil {
		return apperr.Internal("name check", err)
	}
	if taken {
		return apperr.Conflict("channel name already in use")
	}
	var aliasOwner int64
	err := tx.QueryRow(ctx,
		`SELECT channel_id FROM channel_name_alias WHERE org_id = $1 AND name = $2`,
		actor.OrgID, strings.ToLower(newName)).Scan(&aliasOwner)
	switch {
	case err == pgx.ErrNoRows:
		// free
	case err != nil:
		return apperr.Internal("alias check", err)
	case aliasOwner != channelID:
		return apperr.Conflict("channel name is reserved by a renamed channel")
	default:
		// Taking one of its own old names back: the reservation dissolves.
		if _, err := tx.Exec(ctx,
			`DELETE FROM channel_name_alias WHERE org_id = $1 AND name = $2`,
			actor.OrgID, strings.ToLower(newName)); err != nil {
			return apperr.Internal("alias release", err)
		}
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO channel_name_alias (org_id, workspace_id, name, channel_id)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (org_id, name)
		DO UPDATE SET channel_id = EXCLUDED.channel_id, renamed_at = now()`,
		actor.OrgID, wsID, strings.ToLower(oldName), channelID); err != nil {
		return apperr.Internal("reserve old name", err)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE channel SET name = $1 WHERE id = $2`, newName, channelID); err != nil {
		return apperr.Internal("rename", err)
	}
	if _, err := eventlog.Append(ctx, tx, eventlog.Event{
		OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
		EntityType: enum.EntityChannel, EntityID: channelID, Verb: "channel.renamed",
		Payload: eventlog.MustPayload(map[string]any{
			"channel_id": channelID, "old_name": oldName, "name": newName}),
	}); err != nil {
		return apperr.Internal("append event", err)
	}
	return nil
}

// requireLiveChannel rejects writes into archived channels (history stays
// readable; only the write plane freezes).
func (s *Service) requireLiveChannel(ctx context.Context, tx pgx.Tx, orgID, channelID int64) error {
	var archived bool
	if err := tx.QueryRow(ctx,
		`SELECT archived_at IS NOT NULL FROM channel WHERE id = $1 AND org_id = $2`,
		channelID, orgID).Scan(&archived); err != nil {
		return apperr.NotFound("channel not found")
	}
	if archived {
		return apperr.Invalid("channel is archived")
	}
	return nil
}
