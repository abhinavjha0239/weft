// Item links (ADR-008 C-6): typed relationships between work items, keyed by
// stable INTERNAL ids so a link survives a move/rename/rank change on either
// item. A relationship is ONE row (work_item_link) rendered both ways through
// its link_type's inward/outward phrases — never two rows. System link types
// (blocks / is blocked by; relates to) are seeded per org alongside a space's
// workflow (see CreateSpace). This file owns link create/list/delete; all of
// it rides VerbEditItems org-wide in v1 (cross-space consent is a recorded
// gap — the actor already needs edit rights on both spaces, org-wide today).
package worktrack

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

// Link is the created-link response.
type Link struct {
	ID         int64 `json:"id"`
	FromItemID int64 `json:"from_item_id"`
	ToItemID   int64 `json:"to_item_id"`
	LinkTypeID int64 `json:"link_type_id"`
}

// LinkView renders one link from a given item's perspective: the resolved
// phrase (outward when the item is the source, inward when it is the target)
// and the OTHER item's summary.
type LinkView struct {
	ID     int64      `json:"id"`
	Phrase string     `json:"phrase"`
	Item   LinkedItem `json:"item"`
}

type LinkedItem struct {
	ID     int64  `json:"id"`
	Key    string `json:"key"`
	Title  string `json:"title"`
	Status string `json:"status"`
}

// CreateLink links fromItemID → toItemID with a link type. Both items must be
// org-local and live, the link type org-local; self-links 400 and the
// UNIQUE(from,to,type) duplicate 409s. The inverse is implicit — one row.
func (s *Service) CreateLink(ctx context.Context, actor auth.Identity, fromItemID, toItemID, linkTypeID int64) (Link, error) {
	if fromItemID == toItemID {
		return Link{}, apperr.Invalid("an item cannot link to itself")
	}
	out := Link{FromItemID: fromItemID, ToItemID: toItemID, LinkTypeID: linkTypeID}
	err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.perms.Require(ctx, tx, actor, perms.VerbEditItems,
			perms.OrgScope(actor.OrgID)); err != nil {
			return err
		}
		if err := requireLiveItem(ctx, tx, actor.OrgID, fromItemID); err != nil {
			return err
		}
		if err := requireLiveItem(ctx, tx, actor.OrgID, toItemID); err != nil {
			return err
		}
		var ltExists bool
		if err := tx.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM link_type WHERE id = $1 AND org_id = $2)`,
			linkTypeID, actor.OrgID).Scan(&ltExists); err != nil {
			return apperr.Internal("link type check", err)
		}
		if !ltExists {
			return apperr.NotFound("link type not found")
		}
		// Pre-check UNIQUE(from,to,type) (a violation would abort the tx).
		var dup bool
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS (SELECT 1 FROM work_item_link
			 WHERE from_item_id = $1 AND to_item_id = $2 AND link_type_id = $3)`,
			fromItemID, toItemID, linkTypeID).Scan(&dup); err != nil {
			return apperr.Internal("link dup check", err)
		}
		if dup {
			return apperr.Conflict("these items are already linked with this type")
		}
		if err := tx.QueryRow(ctx, `
			INSERT INTO work_item_link (org_id, from_item_id, to_item_id, link_type_id, created_by)
			VALUES ($1,$2,$3,$4,$5) RETURNING id`,
			actor.OrgID, fromItemID, toItemID, linkTypeID, actor.UserID).Scan(&out.ID); err != nil {
			return apperr.Internal("create link", err)
		}
		if _, err := eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
			EntityType: enum.EntityWorkItem, EntityID: fromItemID, Verb: "workitem.linked",
			Payload: eventlog.MustPayload(map[string]any{
				"link_id": out.ID, "from_item_id": fromItemID,
				"to_item_id": toItemID, "link_type_id": linkTypeID}),
		}); err != nil {
			return apperr.Internal("append event", err)
		}
		return nil
	})
	if err != nil {
		return Link{}, err
	}
	return out, nil
}

// ListLinks returns an item's links in BOTH directions, each with the resolved
// phrase and the other item's summary. An org-scoped read (the write endpoints
// carry the VerbEditItems gate). A foreign/absent/trashed anchor 404s.
func (s *Service) ListLinks(ctx context.Context, actor auth.Identity, itemID int64) ([]LinkView, error) {
	var live bool
	if err := s.pool.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM work_item
		 WHERE id = $1 AND org_id = $2 AND trashed_at IS NULL)`,
		itemID, actor.OrgID).Scan(&live); err != nil {
		return nil, apperr.Internal("item check", err)
	}
	if !live {
		return nil, apperr.NotFound("item not found")
	}
	// Outbound rows render the outward phrase toward to_item; inbound rows
	// render the inward phrase toward from_item. Trashed counterparts are
	// omitted. The link's org_id pins the query; the counterpart shares it.
	rows, err := s.pool.Query(ctx, `
		SELECT l.id, lt.outward, o.id, sp.key || '-' || o.key_no, o.title, st.name
		FROM work_item_link l
		JOIN link_type lt ON lt.id = l.link_type_id
		JOIN work_item o ON o.id = l.to_item_id AND o.org_id = l.org_id
		JOIN space sp ON sp.id = o.space_id
		JOIN status st ON st.id = o.status_id
		WHERE l.from_item_id = $1 AND l.org_id = $2 AND o.trashed_at IS NULL
		UNION ALL
		SELECT l.id, lt.inward, o.id, sp.key || '-' || o.key_no, o.title, st.name
		FROM work_item_link l
		JOIN link_type lt ON lt.id = l.link_type_id
		JOIN work_item o ON o.id = l.from_item_id AND o.org_id = l.org_id
		JOIN space sp ON sp.id = o.space_id
		JOIN status st ON st.id = o.status_id
		WHERE l.to_item_id = $1 AND l.org_id = $2 AND o.trashed_at IS NULL
		ORDER BY 1`, itemID, actor.OrgID)
	if err != nil {
		return nil, apperr.Internal("list links", err)
	}
	defer rows.Close()
	var out []LinkView
	for rows.Next() {
		var lv LinkView
		if err := rows.Scan(&lv.ID, &lv.Phrase, &lv.Item.ID, &lv.Item.Key,
			&lv.Item.Title, &lv.Item.Status); err != nil {
			return nil, apperr.Internal("scan link", err)
		}
		out = append(out, lv)
	}
	return out, rows.Err()
}

// DeleteLink removes a link addressed from EITHER endpoint: the link must be
// org-local and involve itemID (as source OR target). A foreign/absent link,
// or one not touching this item, is one indistinguishable 404.
func (s *Service) DeleteLink(ctx context.Context, actor auth.Identity, itemID, linkID int64) error {
	return db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.perms.Require(ctx, tx, actor, perms.VerbEditItems,
			perms.OrgScope(actor.OrgID)); err != nil {
			return err
		}
		var fromID, toID, ltID int64
		if err := tx.QueryRow(ctx, `
			SELECT from_item_id, to_item_id, link_type_id FROM work_item_link
			WHERE id = $1 AND org_id = $2 AND ($3 IN (from_item_id, to_item_id))`,
			linkID, actor.OrgID, itemID).Scan(&fromID, &toID, &ltID); err != nil {
			return apperr.NotFound("link not found")
		}
		if _, err := tx.Exec(ctx, `DELETE FROM work_item_link WHERE id = $1`, linkID); err != nil {
			return apperr.Internal("delete link", err)
		}
		if _, err := eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
			EntityType: enum.EntityWorkItem, EntityID: itemID, Verb: "workitem.unlinked",
			Payload: eventlog.MustPayload(map[string]any{
				"link_id": linkID, "from_item_id": fromID,
				"to_item_id": toID, "link_type_id": ltID}),
		}); err != nil {
			return apperr.Internal("append event", err)
		}
		return nil
	})
}

// requireLiveItem asserts an item is org-local and not trashed, else 404
// (oracle-free: foreign-org and nonexistent are indistinguishable).
func requireLiveItem(ctx context.Context, tx pgx.Tx, orgID, itemID int64) error {
	var live bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM work_item
		 WHERE id = $1 AND org_id = $2 AND trashed_at IS NULL)`,
		itemID, orgID).Scan(&live); err != nil {
		return apperr.Internal("item check", err)
	}
	if !live {
		return apperr.NotFound("item not found")
	}
	return nil
}
