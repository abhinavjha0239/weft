package identity

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/internal/eventlog"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

// Invites are capability tokens: the secret is minted once, stored hashed
// (the auth_session pattern), and never listed again. The ROLE CEILING is
// structural — an invite provisions a member (40) or guest (50), never
// anything above — and a guest invite must enumerate its channels (P-5:
// membership scope is the inviter's decision, not the guest's).

const (
	RoleMember int16 = 40
	RoleGuest  int16 = 50

	maxInviteChannels = 20
	defaultInviteTTL  = 7 * 24 * time.Hour
	maxInviteTTL      = 30 * 24 * time.Hour
	maxInviteUses     = 100
)

type Invite struct {
	ID         int64     `json:"invite_id"`
	Token      string    `json:"token,omitempty"` // present ONLY at creation
	Email      *string   `json:"email,omitempty"`
	Role       int16     `json:"role"`
	ChannelIDs []int64   `json:"channel_ids"`
	CreatedBy  int64     `json:"created_by"`
	ExpiresAt  time.Time `json:"expires_at"`
	MaxUses    int       `json:"max_uses"`
	UsedCount  int       `json:"used_count"`
}

type CreateInviteParams struct {
	Email      *string
	Role       int16
	ChannelIDs []int64
	ExpiresIn  time.Duration // 0 = the 7-day default
	MaxUses    int           // 0 = 1
}

func hashInviteToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// CreateInvite mints a link (invite_members gated).
func (s *Service) CreateInvite(ctx context.Context, actor auth.Identity, p CreateInviteParams) (Invite, error) {
	if p.Role == 0 {
		p.Role = RoleMember
	}
	if p.Role != RoleMember && p.Role != RoleGuest {
		return Invite{}, apperr.Invalid("role must be 40 (member) or 50 (guest)")
	}
	if p.Role == RoleGuest && len(p.ChannelIDs) == 0 {
		return Invite{}, apperr.Invalid("a guest invite must enumerate its channels")
	}
	if len(p.ChannelIDs) > maxInviteChannels {
		return Invite{}, apperr.Invalid("too many channels (max 20)")
	}
	if p.ExpiresIn <= 0 {
		p.ExpiresIn = defaultInviteTTL
	}
	if p.ExpiresIn > maxInviteTTL {
		return Invite{}, apperr.Invalid("expiry must be within 30 days")
	}
	if p.MaxUses <= 0 {
		p.MaxUses = 1
	}
	if p.MaxUses > maxInviteUses {
		return Invite{}, apperr.Invalid("max_uses must be within 100")
	}
	if p.Email != nil {
		e := strings.TrimSpace(strings.ToLower(*p.Email))
		if e == "" || !strings.Contains(e, "@") {
			return Invite{}, apperr.Invalid("bad email pin")
		}
		p.Email = &e
	}

	if p.ChannelIDs == nil {
		p.ChannelIDs = []int64{} // an explicit NULL would override the column default
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return Invite{}, apperr.Internal("token entropy", err)
	}
	token := hex.EncodeToString(raw)

	var out Invite
	err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.perms.Require(ctx, tx, actor, perms.VerbInviteMembers,
			perms.OrgScope(actor.OrgID)); err != nil {
			return err
		}
		if len(p.ChannelIDs) > 0 {
			var n int
			if err := tx.QueryRow(ctx, `
				SELECT count(*) FROM channel
				WHERE id = ANY($1) AND org_id = $2 AND archived_at IS NULL`,
				p.ChannelIDs, actor.OrgID).Scan(&n); err != nil {
				return apperr.Internal("channel check", err)
			}
			if n != len(p.ChannelIDs) {
				return apperr.NotFound("channel not found")
			}
		}
		expires := time.Now().Add(p.ExpiresIn)
		if err := tx.QueryRow(ctx, `
			INSERT INTO invite (org_id, token_hash, email, role, channel_ids,
				created_by, expires_at, max_uses)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id`,
			actor.OrgID, hashInviteToken(token), p.Email, p.Role, p.ChannelIDs,
			actor.UserID, expires, p.MaxUses).Scan(&out.ID); err != nil {
			return apperr.Internal("create invite", err)
		}
		out.Token, out.Email, out.Role = token, p.Email, p.Role
		out.ChannelIDs, out.CreatedBy = p.ChannelIDs, actor.UserID
		out.ExpiresAt, out.MaxUses = expires, p.MaxUses
		_, err := eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
			EntityType: enum.EntityOrg, EntityID: actor.OrgID, Verb: "invite.created",
			Payload: eventlog.MustPayload(map[string]any{
				"invite_id": out.ID, "role": p.Role, "max_uses": p.MaxUses}),
		})
		return err
	})
	if err != nil {
		return Invite{}, err
	}
	return out, nil
}

// ListInvites returns the org's usable invites — never their tokens.
func (s *Service) ListInvites(ctx context.Context, actor auth.Identity) ([]Invite, error) {
	out := []Invite{}
	err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.perms.Require(ctx, tx, actor, perms.VerbInviteMembers,
			perms.OrgScope(actor.OrgID)); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `
			SELECT id, email, role, channel_ids, created_by, expires_at, max_uses, used_count
			FROM invite
			WHERE org_id = $1 AND revoked_at IS NULL
			  AND expires_at > now() AND used_count < max_uses
			ORDER BY id DESC LIMIT 200`, actor.OrgID)
		if err != nil {
			return apperr.Internal("list invites", err)
		}
		defer rows.Close()
		for rows.Next() {
			var iv Invite
			if err := rows.Scan(&iv.ID, &iv.Email, &iv.Role, &iv.ChannelIDs,
				&iv.CreatedBy, &iv.ExpiresAt, &iv.MaxUses, &iv.UsedCount); err != nil {
				return apperr.Internal("scan invite", err)
			}
			out = append(out, iv)
		}
		return rows.Err()
	})
	return out, err
}

// RevokeInvite kills a link (audited; the row stays as the record).
func (s *Service) RevokeInvite(ctx context.Context, actor auth.Identity, inviteID int64) error {
	return db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.perms.Require(ctx, tx, actor, perms.VerbInviteMembers,
			perms.OrgScope(actor.OrgID)); err != nil {
			return err
		}
		ct, err := tx.Exec(ctx, `
			UPDATE invite SET revoked_at = now()
			WHERE id = $1 AND org_id = $2 AND revoked_at IS NULL`,
			inviteID, actor.OrgID)
		if err != nil {
			return apperr.Internal("revoke invite", err)
		}
		if ct.RowsAffected() == 0 {
			return apperr.NotFound("invite not found")
		}
		_, err = eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
			EntityType: enum.EntityOrg, EntityID: actor.OrgID, Verb: "invite.revoked",
			Payload: eventlog.MustPayload(map[string]any{"invite_id": inviteID}),
		})
		return err
	})
}

type AcceptInviteParams struct {
	Token    string
	Email    string
	Password string
	FullName string
	// IP and UserAgent are recorded on the minted session (P-29 metadata);
	// empty values are allowed.
	IP        string
	UserAgent string
}

type AcceptInviteResult struct {
	OrgID      int64   `json:"org_id"`
	UserID     int64   `json:"user_id"`
	Role       int16   `json:"role"`
	ChannelIDs []int64 `json:"channel_ids"`
	Token      string  `json:"token"` // session
}

// joinChannelOnAccept adds a newly-provisioned user to a channel and emits the
// member.joined event — the shared shape for both invite-explicit joins and
// P-09 default-channel auto-joins.
func joinChannelOnAccept(ctx context.Context, tx pgx.Tx, orgID, userID, channelID int64) error {
	// Guarded join: only a LIVE channel in this org takes the new member.
	// Channels were validated when the invite (or default set) was created,
	// but archiving can happen in between — an archived channel is read-only
	// and must not gain members, and a stale default_channel row must not
	// act. Skipping silently is deliberate: account provisioning must not
	// fail over a channel that archived since; the join just doesn't happen
	// (and no member.joined is emitted for it).
	ct, err := tx.Exec(ctx, `
		INSERT INTO channel_member (channel_id, user_id)
		SELECT c.id, $2 FROM channel c
		WHERE c.id = $1 AND c.org_id = $3 AND c.archived_at IS NULL
		ON CONFLICT DO NOTHING`, channelID, userID, orgID)
	if err != nil {
		return apperr.Internal("join channel", err)
	}
	if ct.RowsAffected() == 0 {
		return nil
	}
	if _, err := eventlog.Append(ctx, tx, eventlog.Event{
		OrgID: orgID, ActorKind: enum.ActorHuman, ActorID: &userID,
		EntityType: enum.EntityChannel, EntityID: channelID, Verb: "member.joined",
		Payload: eventlog.MustPayload(map[string]any{
			"channel_id": channelID, "user_id": userID}),
	}); err != nil {
		return apperr.Internal("append event", err)
	}
	return nil
}

// AcceptInvite redeems a token into a new account + session. The token IS
// the authorization: pre-join channels (private included) come from the
// invite, and the new principal lands in role:members or, for guests,
// role:everyone only — the verb defaults do the rest. Unknown tokens 404;
// a known-but-dead one says why (the holder already has the secret).
func (s *Service) AcceptInvite(ctx context.Context, p AcceptInviteParams) (AcceptInviteResult, error) {
	p.Email = strings.TrimSpace(strings.ToLower(p.Email))
	if p.Token == "" || p.Email == "" || !strings.Contains(p.Email, "@") || len(p.Password) < 8 {
		return AcceptInviteResult{}, apperr.Invalid("token, email, password (min 8) required")
	}
	if p.FullName == "" {
		p.FullName = p.Email
	}
	pwHash, err := auth.HashPassword(p.Password)
	if err != nil {
		return AcceptInviteResult{}, apperr.Internal("hash password", err)
	}
	var out AcceptInviteResult
	err = db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		var inviteID, orgID int64
		var pinned *string
		var role int16
		var channelIDs []int64
		var expiresAt time.Time
		var maxUses, usedCount int
		var revoked bool
		err := tx.QueryRow(ctx, `
			SELECT id, org_id, email, role, channel_ids, expires_at,
			       max_uses, used_count, revoked_at IS NOT NULL
			FROM invite WHERE token_hash = $1 FOR UPDATE`,
			hashInviteToken(p.Token)).Scan(&inviteID, &orgID, &pinned, &role,
			&channelIDs, &expiresAt, &maxUses, &usedCount, &revoked)
		if err != nil {
			return apperr.NotFound("invite not found")
		}
		switch {
		case revoked:
			return apperr.Conflict("invite revoked")
		case time.Now().After(expiresAt):
			return apperr.Conflict("invite expired")
		case usedCount >= maxUses:
			return apperr.Conflict("invite exhausted")
		case pinned != nil && *pinned != p.Email:
			return apperr.Forbidden("this invite is pinned to another email")
		}

		if err := tx.QueryRow(ctx, `
			INSERT INTO user_account (org_id, kind, email, full_name, role)
			VALUES ($1, $2, $3, $4, $5) RETURNING id`,
			orgID, enum.UserHuman, p.Email, p.FullName, role).Scan(&out.UserID); err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23505" {
				return apperr.Conflict("email already registered")
			}
			return apperr.Internal("create user", err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO user_credential (user_id, password_hash) VALUES ($1, $2)`,
			out.UserID, pwHash); err != nil {
			return apperr.Internal("store credential", err)
		}
		group := perms.GroupMembers
		if role == RoleGuest {
			group = perms.GroupEveryone
		}
		groupID, err := s.perms.SystemGroupID(ctx, tx, orgID, group)
		if err != nil {
			return err
		}
		if err := s.perms.AddUserToGroup(ctx, tx, orgID, groupID, out.UserID); err != nil {
			return err
		}
		if err := s.perms.RebuildClosure(ctx, tx, orgID); err != nil {
			return err
		}
		// Pre-join the invite's channels — the invite IS the authorization
		// (private channels included, that's the point of inviting).
		explicit := make(map[int64]bool, len(channelIDs))
		for _, chID := range channelIDs {
			explicit[chID] = true
			if err := joinChannelOnAccept(ctx, tx, orgID, out.UserID, chID); err != nil {
				return err
			}
		}
		// P-09: a new MEMBER also auto-joins the workspace's default channels
		// (the always-bundle, bundle IS NULL), deduped against the explicit
		// list. A GUEST gets ONLY the invite's explicit channels (P-5). The
		// workspace is the org's bootstrap workspace — the documented v1
		// reduction shared with the messaging folder/default surface.
		if role != RoleGuest {
			drows, err := tx.Query(ctx, `
				SELECT channel_id FROM default_channel
				WHERE bundle IS NULL AND workspace_id = (
					SELECT id FROM workspace WHERE org_id = $1 ORDER BY id LIMIT 1)
				ORDER BY channel_id`, orgID)
			if err != nil {
				return apperr.Internal("default channels", err)
			}
			var defaults []int64
			for drows.Next() {
				var cid int64
				if err := drows.Scan(&cid); err != nil {
					drows.Close()
					return apperr.Internal("scan default channel", err)
				}
				defaults = append(defaults, cid)
			}
			drows.Close()
			if err := drows.Err(); err != nil {
				return apperr.Internal("default channels", err)
			}
			for _, cid := range defaults {
				if explicit[cid] {
					continue
				}
				if err := joinChannelOnAccept(ctx, tx, orgID, out.UserID, cid); err != nil {
					return err
				}
			}
		}
		if _, err := tx.Exec(ctx,
			`UPDATE invite SET used_count = used_count + 1 WHERE id = $1`, inviteID); err != nil {
			return apperr.Internal("consume invite", err)
		}
		if _, err := eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: orgID, ActorKind: enum.ActorHuman, ActorID: &out.UserID,
			EntityType: enum.EntityUser, EntityID: out.UserID, Verb: "user.joined",
			Payload: eventlog.MustPayload(map[string]any{
				"user_id": out.UserID, "role": role, "invite_id": inviteID}),
		}); err != nil {
			return apperr.Internal("append event", err)
		}
		token, err := auth.CreateSession(ctx, tx, out.UserID, p.IP, p.UserAgent)
		if err != nil {
			return apperr.Internal("create session", err)
		}
		out.OrgID, out.Role, out.ChannelIDs, out.Token = orgID, role, channelIDs, token
		if out.ChannelIDs == nil {
			out.ChannelIDs = []int64{}
		}
		return nil
	})
	if err != nil {
		return AcceptInviteResult{}, err
	}
	return out, nil
}
