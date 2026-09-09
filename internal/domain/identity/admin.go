package identity

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/internal/eventlog"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

// AssignVerb points an org-scope verb at a group — the ADR-006 "admins can
// reassign any verb to any group" surface, and the only way to grant
// compliance_officer (F-9: never seeded, always an explicit act). The
// assignment REPLACES the previous one for that verb, and the act itself is
// event-logged so the audit trail shows who opened which door.
func (s *Service) AssignVerb(ctx context.Context, actor auth.Identity, verb, groupName string) error {
	return s.assignVerb(ctx, actor, verb, groupName, nil)
}

// AssignVerbAtChannel points a verb at a group FOR ONE CHANNEL (P-44a) —
// ADR-006's "announcement channels = send_message restricted at channel
// scope", finally writable. The resolver has always READ this rung
// (perms.ChannelScope walks channel → workspace → org and the most specific
// assignment wins) and permission_assignment has been UNIQUE on
// (org_id, verb, scope_type, scope_id) since migration 0002, so this needs no
// migration and no new resolution rule: it writes the row the resolver already
// knows how to prefer, and every OTHER channel keeps answering from the org
// default.
//
// The gate is at the TARGET scope, not at the org: administer_channel resolved
// through perms.ChannelScope. That chain ENDS at the org, where
// administer_channel seeds to role:admins, so org admins can retarget any
// channel out of the box (their behaviour is unchanged) while a channel's own
// admins gain the rung for their channel alone.
//
// administer_channel — not manage_permissions — because manage_permissions can
// never be written at channel scope (see below), so resolving IT through the
// chain would always land on the org row and "a channel admin who is not an
// org permission administrator" would be an unreachable state. It is also the
// verb every other channel-owned config already gates on (messaging.SetPinned,
// messaging.UpdateChannel, automation.requireScopeAdmin), and it matches
// Zulip, where per-stream permission settings — can_send_message_group
// included — are gated on can_administer_channel_group with realm admins
// implicitly included (zerver/lib/streams.py,
// check_stream_access_for_delete_or_update_requiring_metadata_access).
//
// Oracle-free: absent, foreign-org and un-administered channels all answer the
// IDENTICAL NotFound that ChannelScope already produces, so this surface
// cannot enumerate channel ids or reveal who administers what. It is
// deliberately BLUNTER than the P-34 masking gates (which 403 a non-member of
// a PUBLIC channel, keeping the join affordance honest): there is no
// affordance to keep honest here, and the nuance would cost a second
// visibility lookup to leak strictly more. Only a Forbidden is masked — an
// Internal from the resolver rides through as itself.
func (s *Service) AssignVerbAtChannel(ctx context.Context, actor auth.Identity, verb, groupName string, channelID int64) error {
	return s.assignVerb(ctx, actor, verb, groupName, &channelID)
}

// assignVerb is the one write path behind both surfaces. A nil channelID means
// org scope — the shape that has existed since P-2, unchanged down to its gate
// verb, its event verb and its payload keys (the scope is ADDED to the
// payload, never repurposed onto an existing key).
func (s *Service) assignVerb(ctx context.Context, actor auth.Identity, verb, groupName string, channelID *int64) error {
	if !perms.KnownVerb(verb) {
		return apperr.Invalid("unknown verb")
	}
	if channelID != nil {
		// manage_permissions is refused BEFORE any lookup, so the refusal is
		// neither reachable around nor usable to probe channel ids. Its holder
		// can point every other verb — including this one — at any group, so a
		// channel-scope grant would be a standing delegation of permission
		// administration that the org's own manage_permissions holder never
		// approved. It is also absent from perms.ChannelAssignable; the
		// duplication is deliberate, so deleting either guard leaves one.
		if verb == perms.VerbManagePermissions {
			return apperr.Invalid("manage_permissions may not be assigned at channel scope: " +
				"its holder can point every other verb, so a channel-scope grant hands out " +
				"permission administration")
		}
		// Honest rungs: a verb no channel-scope gate consults would be stored
		// config nothing enforces (perms.ChannelAssignable carries the
		// reasoning and the call sites).
		if !perms.ChannelAssignable(verb) {
			return apperr.Invalid(verb + " is not consulted at channel scope; assign it at org scope")
		}
	}
	return db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		// The gate, resolved AT THE SCOPE BEING WRITTEN. One verb per scope, no
		// umbrella, for the P-47 reason: Require resolves per-verb, so an
		// `OR org manage_permissions` fallback would make narrowing a
		// channel's admin rung a no-op.
		scope := perms.OrgRef(actor.OrgID)
		payload := map[string]any{
			"verb": verb, "group": groupName,
			"scope_type": int16(perms.ScopeOrg), "scope_id": actor.OrgID,
		}
		ev := eventlog.Event{
			OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
			EntityType: enum.EntityOrg, EntityID: actor.OrgID, Verb: "org.verb_assigned",
		}
		if channelID == nil {
			if err := s.perms.Require(ctx, tx, actor, perms.VerbManagePermissions,
				perms.OrgScope(actor.OrgID)); err != nil {
				return err
			}
		} else {
			chain, err := s.perms.ChannelScope(ctx, tx, actor.OrgID, *channelID)
			if err != nil {
				return err // NotFound: absent, or another org's channel
			}
			if err := s.perms.Require(ctx, tx, actor, perms.VerbAdministerChannel, chain); err != nil {
				if apperr.KindOf(err) == apperr.KindForbidden {
					return apperr.NotFound("channel not found")
				}
				return err
			}
			scope = perms.ChannelRef(*channelID)
			payload["scope_type"] = int16(perms.ScopeChannel)
			payload["scope_id"] = *channelID
			// channel_id is the gateway read-ACL's key (gateway client.filter),
			// not a duplicate of scope_id for its own sake: naming the channel
			// is what keeps this event off the org-wide fan and inside the
			// channel it is about.
			payload["channel_id"] = *channelID
			// Its OWN verb rather than org.verb_assigned with a channel
			// entity: verbs are what consumers filter on (the compliance audit
			// read takes verb=), and an org-named verb carrying entity_type 3
			// would force every consumer to sniff the payload to learn what
			// the event is about. Adding a verb is append-only; overloading
			// one is the wire-contract change the verb rule forbids.
			ev.EntityType, ev.EntityID, ev.Verb = enum.EntityChannel, *channelID, "channel.verb_assigned"
		}
		// The group lookup runs AFTER the gate at both scopes: a principal who
		// may not assign here must not learn which group names exist.
		var groupID int64
		if err := tx.QueryRow(ctx, `
			SELECT id FROM user_group WHERE org_id = $1 AND name = $2`,
			actor.OrgID, groupName).Scan(&groupID); err != nil {
			return apperr.NotFound("group not found")
		}
		if err := s.perms.Assign(ctx, tx, actor.OrgID, verb, scope, groupID); err != nil {
			return err
		}
		ev.Payload = eventlog.MustPayload(payload)
		_, err := eventlog.Append(ctx, tx, ev)
		return err
	})
}
