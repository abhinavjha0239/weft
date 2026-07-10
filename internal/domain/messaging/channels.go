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

const (
	visibilityPublic  = 1
	visibilityPrivate = 2
)

type CreateChannelParams struct {
	Name        string
	Description string
	Private     bool
	WorkspaceID int64 // 0 = the org's sole workspace (M1 orgs have one)
}

type CreateChannelResult struct {
	ChannelID    int64 `json:"channel_id"`
	RootThreadID int64 `json:"root_thread_id"`
}

// CreateChannel makes a channel with its root thread (F-15), the creator as
// first member, and channel.created — one transaction.
//
// Verb gap (REALITY.md): one create_channel verb covers public and private;
// the ADR-006 public/private/web-public verb split lands with the full
// registry.
func (s *Service) CreateChannel(ctx context.Context, actor auth.Identity, p CreateChannelParams) (CreateChannelResult, error) {
	name := strings.TrimSpace(strings.TrimPrefix(p.Name, "#"))
	if name == "" || len(name) > 80 {
		return CreateChannelResult{}, apperr.Invalid("channel name must be 1-80 characters")
	}
	var out CreateChannelResult
	err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.perms.Require(ctx, tx, actor, perms.VerbCreateChannel,
			perms.OrgScope(actor.OrgID)); err != nil {
			return err
		}
		wsID := p.WorkspaceID
		if wsID == 0 {
			rows, err := tx.Query(ctx,
				`SELECT id FROM workspace WHERE org_id = $1 AND archived_at IS NULL
				 ORDER BY id LIMIT 2`, actor.OrgID)
			if err != nil {
				return apperr.Internal("workspaces", err)
			}
			var ids []int64
			for rows.Next() {
				var id int64
				if err := rows.Scan(&id); err != nil {
					rows.Close()
					return apperr.Internal("scan workspace", err)
				}
				ids = append(ids, id)
			}
			rows.Close()
			if err := rows.Err(); err != nil {
				return apperr.Internal("workspaces", err)
			}
			switch len(ids) {
			case 1:
				wsID = ids[0]
			case 0:
				return apperr.Invalid("org has no workspace")
			default:
				return apperr.Invalid("workspace_id required (org has several workspaces)")
			}
		}
		// Pre-check the live-name index (a unique-violation error would abort
		// the transaction — same rule as the importer).
		var taken bool
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS (SELECT 1 FROM channel
			 WHERE org_id = $1 AND COALESCE(workspace_id, 0) = $2
			   AND lower(name) = lower($3) AND archived_at IS NULL)`,
			actor.OrgID, wsID, name).Scan(&taken); err != nil {
			return apperr.Internal("name check", err)
		}
		if taken {
			return apperr.Conflict("channel name already in use")
		}
		// F-22: renamed-away names stay reserved for their channel.
		var reserved bool
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS (SELECT 1 FROM channel_name_alias
			 WHERE org_id = $1 AND name = lower($2))`,
			actor.OrgID, name).Scan(&reserved); err != nil {
			return apperr.Internal("alias check", err)
		}
		if reserved {
			return apperr.Conflict("channel name is reserved by a renamed channel")
		}

		visibility := visibilityPublic
		if p.Private {
			visibility = visibilityPrivate
		}
		if err := tx.QueryRow(ctx, `
			INSERT INTO channel (org_id, workspace_id, name, visibility,
				description, creator_id)
			VALUES ($1, $2, $3, $4, $5, $6) RETURNING id`,
			actor.OrgID, wsID, name, visibility, strings.TrimSpace(p.Description),
			actor.UserID).Scan(&out.ChannelID); err != nil {
			return apperr.Internal("create channel", err)
		}
		if err := tx.QueryRow(ctx,
			`INSERT INTO thread (org_id, channel_id, kind) VALUES ($1, $2, 2) RETURNING id`,
			actor.OrgID, out.ChannelID).Scan(&out.RootThreadID); err != nil {
			return apperr.Internal("root thread", err)
		}
		if _, err := tx.Exec(ctx,
			`UPDATE channel SET root_thread_id = $1 WHERE id = $2`,
			out.RootThreadID, out.ChannelID); err != nil {
			return apperr.Internal("bind root", err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO channel_member (channel_id, user_id) VALUES ($1, $2)`,
			out.ChannelID, actor.UserID); err != nil {
			return apperr.Internal("join creator", err)
		}
		if _, err := eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: actor.OrgID, WorkspaceID: &wsID,
			ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
			EntityType: enum.EntityChannel, EntityID: out.ChannelID, Verb: "channel.created",
			Payload: eventlog.MustPayload(map[string]any{
				"channel_id": out.ChannelID, "name": name, "private": p.Private}),
		}); err != nil {
			return apperr.Internal("append event", err)
		}
		return nil
	})
	if err != nil {
		return CreateChannelResult{}, err
	}
	return out, nil
}

type ChannelSummary struct {
	ID           int64  `json:"id"`
	Name         string `json:"name"`
	Description  string `json:"description"`
	Private      bool   `json:"private"`
	Member       bool   `json:"member"`
	RootThreadID int64  `json:"root_thread_id"`
}

// ListChannels: the ADR-008 C-2 read-model slice — public channels are
// discoverable org-wide; private channels appear only to members.
func (s *Service) ListChannels(ctx context.Context, actor auth.Identity) ([]ChannelSummary, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT c.id, c.name, c.description, c.visibility = 2,
		       cm.user_id IS NOT NULL, COALESCE(c.root_thread_id, 0)
		FROM channel c
		LEFT JOIN channel_member cm
		  ON cm.channel_id = c.id AND cm.user_id = $2 AND cm.unsubscribed_at IS NULL
		WHERE c.org_id = $1 AND c.archived_at IS NULL
		  AND (c.visibility = 1 OR cm.user_id IS NOT NULL)
		ORDER BY c.name`,
		actor.OrgID, actor.UserID)
	if err != nil {
		return nil, apperr.Internal("list channels", err)
	}
	defer rows.Close()
	var out []ChannelSummary
	for rows.Next() {
		var c ChannelSummary
		if err := rows.Scan(&c.ID, &c.Name, &c.Description, &c.Private, &c.Member, &c.RootThreadID); err != nil {
			return nil, apperr.Internal("scan channel", err)
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// JoinChannel: self-join for PUBLIC channels (private = invitation, a later
// endpoint; Zulip's can_subscribe_group refines this when the verb registry
// grows). Re-joining reactivates the membership row so history_from survives.
func (s *Service) JoinChannel(ctx context.Context, actor auth.Identity, channelID int64) error {
	return db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		var visibility int16
		err := tx.QueryRow(ctx,
			`SELECT visibility FROM channel
			 WHERE id = $1 AND org_id = $2 AND archived_at IS NULL`,
			channelID, actor.OrgID).Scan(&visibility)
		if err != nil {
			return apperr.NotFound("channel not found")
		}
		if visibility != visibilityPublic {
			return apperr.Forbidden("private channels are joined by invitation")
		}
		ct, err := tx.Exec(ctx, `
			INSERT INTO channel_member (channel_id, user_id)
			VALUES ($1, $2)
			ON CONFLICT (channel_id, user_id)
			DO UPDATE SET unsubscribed_at = NULL
			WHERE channel_member.unsubscribed_at IS NOT NULL`,
			channelID, actor.UserID)
		if err != nil {
			return apperr.Internal("join", err)
		}
		if ct.RowsAffected() == 0 {
			return nil // already a member — idempotent
		}
		if _, err := eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
			EntityType: enum.EntityChannel, EntityID: channelID, Verb: "member.joined",
			Payload: eventlog.MustPayload(map[string]any{
				"channel_id": channelID, "user_id": actor.UserID}),
		}); err != nil {
			return apperr.Internal("append event", err)
		}
		return nil
	})
}

// LeaveChannel marks the membership unsubscribed (the row survives so
// history_from is never lost — ADR-008 C-4).
func (s *Service) LeaveChannel(ctx context.Context, actor auth.Identity, channelID int64) error {
	return db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		ct, err := tx.Exec(ctx, `
			UPDATE channel_member SET unsubscribed_at = now()
			WHERE channel_id = $1 AND user_id = $2 AND unsubscribed_at IS NULL
			  AND EXISTS (SELECT 1 FROM channel
			              WHERE id = $1 AND org_id = $3)`,
			channelID, actor.UserID, actor.OrgID)
		if err != nil {
			return apperr.Internal("leave", err)
		}
		if ct.RowsAffected() == 0 {
			return apperr.NotFound("not a member of this channel")
		}
		if _, err := eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
			EntityType: enum.EntityChannel, EntityID: channelID, Verb: "member.left",
			Payload: eventlog.MustPayload(map[string]any{
				"channel_id": channelID, "user_id": actor.UserID}),
		}); err != nil {
			return apperr.Internal("append event", err)
		}
		return nil
	})
}
