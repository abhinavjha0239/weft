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
	if !perms.KnownVerb(verb) {
		return apperr.Invalid("unknown verb")
	}
	return db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.perms.Require(ctx, tx, actor, perms.VerbManageOrg,
			perms.OrgScope(actor.OrgID)); err != nil {
			return err
		}
		var groupID int64
		err := tx.QueryRow(ctx, `
			SELECT id FROM user_group WHERE org_id = $1 AND name = $2`,
			actor.OrgID, groupName).Scan(&groupID)
		if err != nil {
			return apperr.NotFound("group not found")
		}
		if err := s.perms.Assign(ctx, tx, actor.OrgID, verb,
			perms.OrgRef(actor.OrgID), groupID); err != nil {
			return err
		}
		_, err = eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
			EntityType: enum.EntityOrg, EntityID: actor.OrgID, Verb: "org.verb_assigned",
			Payload: eventlog.MustPayload(map[string]any{
				"verb": verb, "group": groupName}),
		})
		return err
	})
}
