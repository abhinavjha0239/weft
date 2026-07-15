// Package worktrack is the work-item module (ADR-009 tier 1 + the ADR-001 D2
// fusion): a Space holds items; every item OWNS a thread (its discussion —
// same messages, same renderer, same search); any channel thread can be
// PROMOTED into an item without re-parenting its ACL (red-team F-5: the
// channel keeps governing the discussion). Resolution is DERIVED from status
// category (W-3) — the Jira resolution trap is structurally impossible.
//
// v1 visibility slice (REALITY.md): spaces and their items are visible to the
// whole org; VisibilityScope and space-level permission profiles refine this
// later. Owns tables: space, status_set, status, item_type, rank_context,
// work_item (+ links/sprints/etc. as they wake).
package worktrack

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/internal/eventlog"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

const (
	catTodo       = 1
	catInProgress = 2
	catDone       = 3
)

type Service struct {
	pool      *pgxpool.Pool
	perms     *perms.Service
	messaging *messaging.Service
}

func New(pool *pgxpool.Pool, p *perms.Service, m *messaging.Service) *Service {
	return &Service{pool: pool, perms: p, messaging: m}
}

type CreateSpaceParams struct {
	Key  string // e.g. "WEFT" → items WEFT-1, WEFT-2…
	Name string
}

type SpaceSummary struct {
	ID   int64  `json:"id"`
	Key  string `json:"key"`
	Name string `json:"name"`
}

// CreateSpace seeds the ADR-009 defaults: a StatusSet (To Do / In Progress /
// Done with categories), Task+Bug+Epic types (typed levels, W-1), and the
// space's rank context (F-21).
func (s *Service) CreateSpace(ctx context.Context, actor auth.Identity, p CreateSpaceParams) (SpaceSummary, error) {
	key := strings.ToUpper(strings.TrimSpace(p.Key))
	if len(key) < 1 || len(key) > 10 || !isKey(key) {
		return SpaceSummary{}, apperr.Invalid("key must be 1-10 chars, A-Z then A-Z0-9")
	}
	if strings.TrimSpace(p.Name) == "" {
		return SpaceSummary{}, apperr.Invalid("name required")
	}
	var out SpaceSummary
	err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.perms.Require(ctx, tx, actor, perms.VerbCreateSpace,
			perms.OrgScope(actor.OrgID)); err != nil {
			return err
		}
		var wsID int64
		if err := tx.QueryRow(ctx, `
			SELECT id FROM workspace WHERE org_id = $1 AND archived_at IS NULL
			ORDER BY id LIMIT 1`, actor.OrgID).Scan(&wsID); err != nil {
			return apperr.Invalid("org has no workspace")
		}
		var taken bool
		if err := tx.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM space WHERE org_id = $1 AND key = $2)`,
			actor.OrgID, key).Scan(&taken); err != nil {
			return apperr.Internal("key check", err)
		}
		if taken {
			return apperr.Conflict("space key already in use (keys are never reused)")
		}

		var setID int64
		if err := tx.QueryRow(ctx,
			`INSERT INTO status_set (org_id, name) VALUES ($1, $2 || ' workflow') RETURNING id`,
			actor.OrgID, p.Name).Scan(&setID); err != nil {
			return apperr.Internal("status set", err)
		}
		for i, st := range []struct {
			name string
			cat  int
		}{{"To Do", catTodo}, {"In Progress", catInProgress}, {"Done", catDone}} {
			if _, err := tx.Exec(ctx, `
				INSERT INTO status (status_set_id, name, category, position)
				VALUES ($1, $2, $3, $4)`, setID, st.name, st.cat, i); err != nil {
				return apperr.Internal("seed status", err)
			}
		}
		if err := tx.QueryRow(ctx, `
			INSERT INTO space (org_id, workspace_id, key, name, status_set_id, lead_user_id)
			VALUES ($1, $2, $3, $4, $5, $6) RETURNING id`,
			actor.OrgID, wsID, key, strings.TrimSpace(p.Name), setID,
			actor.UserID).Scan(&out.ID); err != nil {
			return apperr.Internal("create space", err)
		}
		for _, ty := range []struct {
			name  string
			level int
		}{{"Task", 0}, {"Bug", 0}, {"Epic", 1}} {
			if _, err := tx.Exec(ctx, `
				INSERT INTO item_type (space_id, name, level) VALUES ($1, $2, $3)`,
				out.ID, ty.name, ty.level); err != nil {
				return apperr.Internal("seed type", err)
			}
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO rank_context (org_id, name) VALUES ($1, $2)`,
			actor.OrgID, key); err != nil {
			return apperr.Internal("rank context", err)
		}
		out.Key, out.Name = key, strings.TrimSpace(p.Name)
		if _, err := eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
			EntityType: enum.EntitySpace, EntityID: out.ID, Verb: "space.created",
			Payload: eventlog.MustPayload(map[string]any{
				"space_id": out.ID, "key": key, "name": out.Name}),
		}); err != nil {
			return apperr.Internal("append event", err)
		}
		return nil
	})
	if err != nil {
		return SpaceSummary{}, err
	}
	return out, nil
}

func (s *Service) ListSpaces(ctx context.Context, actor auth.Identity) ([]SpaceSummary, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, key, name FROM space
		WHERE org_id = $1 AND archived_at IS NULL AND trashed_at IS NULL
		ORDER BY key`, actor.OrgID)
	if err != nil {
		return nil, apperr.Internal("list spaces", err)
	}
	defer rows.Close()
	var out []SpaceSummary
	for rows.Next() {
		var sp SpaceSummary
		if err := rows.Scan(&sp.ID, &sp.Key, &sp.Name); err != nil {
			return nil, apperr.Internal("scan space", err)
		}
		out = append(out, sp)
	}
	return out, rows.Err()
}

type CreateItemParams struct {
	SpaceID     int64
	Title       string
	Description string // becomes the root message of the item's thread
	Type        string // "", "Task", "Bug", "Epic"
}

type Item struct {
	ID             int64  `json:"id"`
	Key            string `json:"key"`
	Title          string `json:"title"`
	Type           string `json:"type"`
	Status         string `json:"status"`
	StatusID       int64  `json:"status_id"`
	StatusCategory int    `json:"status_category"`
	AssigneeID     *int64 `json:"assignee_id,omitempty"`
	ThreadID       int64  `json:"thread_id"`
	Resolved       bool   `json:"resolved"`
}

// CreateItem: the D2 fusion forward direction — the item is born owning a
// space-governed Thread; the description (if any) is that thread's root
// message, so item discussion and description share one content system.
func (s *Service) CreateItem(ctx context.Context, actor auth.Identity, p CreateItemParams) (Item, error) {
	if strings.TrimSpace(p.Title) == "" {
		return Item{}, apperr.Invalid("title required")
	}
	if p.Type == "" {
		p.Type = "Task"
	}
	var out Item
	err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.perms.Require(ctx, tx, actor, perms.VerbCreateItems,
			perms.OrgScope(actor.OrgID)); err != nil {
			return err
		}
		sp, err := loadSpace(ctx, tx, actor.OrgID, p.SpaceID)
		if err != nil {
			return err
		}
		var typeID int64
		if err := tx.QueryRow(ctx,
			`SELECT id FROM item_type WHERE space_id = $1 AND lower(name) = lower($2)`,
			p.SpaceID, p.Type).Scan(&typeID); err != nil {
			return apperr.Invalid("unknown item type " + p.Type)
		}
		var statusID int64
		if err := tx.QueryRow(ctx, `
			SELECT id FROM status WHERE status_set_id = $1 AND category = $2
			ORDER BY position LIMIT 1`, sp.statusSetID, catTodo).Scan(&statusID); err != nil {
			return apperr.Internal("initial status", err)
		}

		// Per-space key_no assignment is serialized with an advisory lock —
		// MAX+1 would race under concurrent creates (unique violation aborts
		// the tx). Per-space serialization of item creation is the accepted
		// cost (Jira behaves the same); cell invariant unaffected.
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, p.SpaceID); err != nil {
			return apperr.Internal("lock space", err)
		}
		var keyNo int
		if err := tx.QueryRow(ctx,
			`SELECT COALESCE(MAX(key_no), 0) + 1 FROM work_item WHERE space_id = $1`,
			p.SpaceID).Scan(&keyNo); err != nil {
			return apperr.Internal("key_no", err)
		}

		var threadID int64
		if err := tx.QueryRow(ctx, `
			INSERT INTO thread (org_id, space_id, kind, title, last_activity_at)
			VALUES ($1, $2, 1, $3, now()) RETURNING id`,
			actor.OrgID, p.SpaceID, p.Title).Scan(&threadID); err != nil {
			return apperr.Internal("item thread", err)
		}
		// v1 rank: sparse sortable string from key_no (LexoRank arrives with
		// board reordering).
		rank := fmt.Sprintf("m%08d", keyNo)
		if err := tx.QueryRow(ctx, `
			INSERT INTO work_item (org_id, space_id, key_no, type_id, status_id,
				title, thread_id, reporter_id, rank, rank_context_id)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,
			        (SELECT id FROM rank_context WHERE org_id = $1 AND name = $10))
			RETURNING id`,
			actor.OrgID, p.SpaceID, keyNo, typeID, statusID,
			strings.TrimSpace(p.Title), threadID, actor.UserID, rank,
			sp.key).Scan(&out.ID); err != nil {
			return apperr.Internal("create item", err)
		}
		if strings.TrimSpace(p.Description) != "" {
			if _, err := s.messaging.InsertThreadMessage(ctx, tx, actor,
				threadID, nil, nil, p.Description); err != nil {
				return err
			}
		}
		out.Key = fmt.Sprintf("%s-%d", sp.key, keyNo)
		out.Title, out.Type, out.Status = strings.TrimSpace(p.Title), p.Type, "To Do"
		out.StatusID, out.StatusCategory, out.ThreadID = statusID, catTodo, threadID
		if _, err := eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
			EntityType: enum.EntityWorkItem, EntityID: out.ID, Verb: "workitem.created",
			Payload: eventlog.MustPayload(map[string]any{
				"item_id": out.ID, "space_id": p.SpaceID, "key": out.Key,
				"thread_id": threadID}),
		}); err != nil {
			return apperr.Internal("append event", err)
		}
		return nil
	})
	if err != nil {
		return Item{}, err
	}
	return out, nil
}

// PromoteThread: the D2 fusion reverse direction — an existing CHANNEL thread
// becomes a work item's discussion. The thread's ACL does not change (F-5):
// the channel keeps governing it; the item points at it.
func (s *Service) PromoteThread(ctx context.Context, actor auth.Identity, threadID, spaceID int64, itemType string) (Item, error) {
	if itemType == "" {
		itemType = "Task"
	}
	var out Item
	err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.perms.Require(ctx, tx, actor, perms.VerbCreateItems,
			perms.OrgScope(actor.OrgID)); err != nil {
			return err
		}
		var channelID *int64
		var kind int16
		var title *string
		err := tx.QueryRow(ctx, `
			SELECT channel_id, kind, title FROM thread
			WHERE id = $1 AND org_id = $2`, threadID, actor.OrgID).
			Scan(&channelID, &kind, &title)
		if err != nil {
			return apperr.NotFound("thread not found")
		}
		if channelID == nil {
			return apperr.Invalid("this thread already belongs to a space")
		}
		if kind == 2 {
			return apperr.Invalid("the channel root thread cannot be promoted")
		}
		// Promoter must be able to SEE the discussion they are promoting.
		var member bool
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS (SELECT 1 FROM channel_member
			 WHERE channel_id = $1 AND user_id = $2 AND unsubscribed_at IS NULL)`,
			*channelID, actor.UserID).Scan(&member); err != nil {
			return apperr.Internal("membership check", err)
		}
		if !member {
			return apperr.Forbidden("not a member of the thread's channel")
		}
		var already bool
		if err := tx.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM work_item WHERE thread_id = $1)`,
			threadID).Scan(&already); err != nil {
			return apperr.Internal("promotion check", err)
		}
		if already {
			return apperr.Conflict("this thread is already a work item's discussion")
		}
		sp, err := loadSpace(ctx, tx, actor.OrgID, spaceID)
		if err != nil {
			return err
		}
		var typeID int64
		if err := tx.QueryRow(ctx,
			`SELECT id FROM item_type WHERE space_id = $1 AND lower(name) = lower($2)`,
			spaceID, itemType).Scan(&typeID); err != nil {
			return apperr.Invalid("unknown item type " + itemType)
		}
		var statusID int64
		if err := tx.QueryRow(ctx, `
			SELECT id FROM status WHERE status_set_id = $1 AND category = $2
			ORDER BY position LIMIT 1`, sp.statusSetID, catTodo).Scan(&statusID); err != nil {
			return apperr.Internal("initial status", err)
		}
		itemTitle := "Promoted thread"
		if title != nil && *title != "" {
			itemTitle = *title
		}
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, spaceID); err != nil {
			return apperr.Internal("lock space", err)
		}
		var keyNo int
		if err := tx.QueryRow(ctx,
			`SELECT COALESCE(MAX(key_no), 0) + 1 FROM work_item WHERE space_id = $1`,
			spaceID).Scan(&keyNo); err != nil {
			return apperr.Internal("key_no", err)
		}
		if err := tx.QueryRow(ctx, `
			INSERT INTO work_item (org_id, space_id, key_no, type_id, status_id,
				title, thread_id, reporter_id, rank, rank_context_id)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,
			        (SELECT id FROM rank_context WHERE org_id = $1 AND name = $10))
			RETURNING id`,
			actor.OrgID, spaceID, keyNo, typeID, statusID, itemTitle,
			threadID, actor.UserID, fmt.Sprintf("m%08d", keyNo),
			sp.key).Scan(&out.ID); err != nil {
			return apperr.Internal("promote", err)
		}
		out.Key = fmt.Sprintf("%s-%d", sp.key, keyNo)
		out.Title, out.Type, out.Status = itemTitle, itemType, "To Do"
		out.StatusID, out.StatusCategory, out.ThreadID = statusID, catTodo, threadID
		if _, err := eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
			EntityType: enum.EntityWorkItem, EntityID: out.ID,
			Verb: "workitem.promoted_from_thread",
			Payload: eventlog.MustPayload(map[string]any{
				"item_id": out.ID, "space_id": spaceID, "key": out.Key,
				"thread_id": threadID, "channel_id": *channelID}),
		}); err != nil {
			return apperr.Internal("append event", err)
		}
		return nil
	})
	if err != nil {
		return Item{}, err
	}
	return out, nil
}

type UpdateItemParams struct {
	Title      *string
	StatusID   *int64
	AssigneeID *int64 // 0 clears
	SprintID   *int64 // 0 clears (the AssigneeID precedent)
}

// UpdateItem edits fields and transitions status. Resolution is DERIVED:
// entering a done-category status sets resolved_at; leaving clears it (W-3).
func (s *Service) UpdateItem(ctx context.Context, actor auth.Identity, itemID int64, p UpdateItemParams) error {
	if p.Title == nil && p.StatusID == nil && p.AssigneeID == nil && p.SprintID == nil {
		return apperr.Invalid("nothing to update")
	}
	return db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.perms.Require(ctx, tx, actor, perms.VerbEditItems,
			perms.OrgScope(actor.OrgID)); err != nil {
			return err
		}
		var spaceID, statusSetID, curStatus int64
		err := tx.QueryRow(ctx, `
			SELECT w.space_id, sp.status_set_id, w.status_id
			FROM work_item w JOIN space sp ON sp.id = w.space_id
			WHERE w.id = $1 AND w.org_id = $2 AND w.trashed_at IS NULL`,
			itemID, actor.OrgID).Scan(&spaceID, &statusSetID, &curStatus)
		if err != nil {
			return apperr.NotFound("item not found")
		}
		if p.Title != nil {
			t := strings.TrimSpace(*p.Title)
			if t == "" {
				return apperr.Invalid("title cannot be empty")
			}
			if _, err := tx.Exec(ctx,
				`UPDATE work_item SET title = $1, updated_at = now() WHERE id = $2`,
				t, itemID); err != nil {
				return apperr.Internal("retitle", err)
			}
		}
		if p.AssigneeID != nil {
			var v *int64
			if *p.AssigneeID != 0 {
				v = p.AssigneeID
			}
			if _, err := tx.Exec(ctx,
				`UPDATE work_item SET assignee_id = $1, updated_at = now() WHERE id = $2`,
				v, itemID); err != nil {
				return apperr.Internal("assign", err)
			}
		}
		if p.SprintID != nil {
			var v *int64
			if *p.SprintID != 0 {
				// Same loadItem tx: the sprint must exist, belong to THIS item's
				// space, and not be closed. The space pin makes a cross-space or
				// foreign-org sprint one uniform 400; clearing (0) is always allowed.
				var sState int
				if err := tx.QueryRow(ctx, `
					SELECT state FROM sprint
					WHERE id = $1 AND org_id = $2 AND space_id = $3`,
					*p.SprintID, actor.OrgID, spaceID).Scan(&sState); err != nil {
					return apperr.Invalid("sprint not found in this item's space")
				}
				if sState == 3 {
					return apperr.Invalid("cannot assign an item to a closed sprint")
				}
				v = p.SprintID
			}
			if _, err := tx.Exec(ctx,
				`UPDATE work_item SET sprint_id = $1, updated_at = now() WHERE id = $2`,
				v, itemID); err != nil {
				return apperr.Internal("assign sprint", err)
			}
		}
		if p.StatusID != nil && *p.StatusID != curStatus {
			var cat int
			if err := tx.QueryRow(ctx,
				`SELECT category FROM status WHERE id = $1 AND status_set_id = $2`,
				*p.StatusID, statusSetID).Scan(&cat); err != nil {
				return apperr.Invalid("status does not belong to this space's workflow")
			}
			// W-3: resolved_at follows the category, never a free field.
			if _, err := tx.Exec(ctx, `
				UPDATE work_item SET status_id = $1, updated_at = now(),
				  resolved_at = CASE WHEN $2 = 3 THEN COALESCE(resolved_at, now()) ELSE NULL END
				WHERE id = $3`, *p.StatusID, cat, itemID); err != nil {
				return apperr.Internal("transition", err)
			}
			if _, err := eventlog.Append(ctx, tx, eventlog.Event{
				OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
				EntityType: enum.EntityWorkItem, EntityID: itemID, Verb: "workitem.status_changed",
				Payload: eventlog.MustPayload(map[string]any{
					"item_id": itemID, "space_id": spaceID,
					"status_id": *p.StatusID, "resolved": cat == catDone}),
			}); err != nil {
				return apperr.Internal("append event", err)
			}
		}
		if p.Title != nil || p.AssigneeID != nil || p.SprintID != nil {
			if _, err := eventlog.Append(ctx, tx, eventlog.Event{
				OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
				EntityType: enum.EntityWorkItem, EntityID: itemID, Verb: "workitem.updated",
				Payload: eventlog.MustPayload(map[string]any{
					"item_id": itemID, "space_id": spaceID}),
			}); err != nil {
				return apperr.Internal("append event", err)
			}
		}
		return nil
	})
}

// ListItems returns a space's live items in rank order with status metadata
// (the client groups by status_category to draw a board).
func (s *Service) ListItems(ctx context.Context, actor auth.Identity, spaceID int64) ([]Item, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT w.id, sp.key || '-' || w.key_no, w.title, it.name,
		       st.name, st.id, st.category, w.assignee_id, w.thread_id,
		       w.resolved_at IS NOT NULL
		FROM work_item w
		JOIN space sp ON sp.id = w.space_id
		JOIN item_type it ON it.id = w.type_id
		JOIN status st ON st.id = w.status_id
		WHERE w.space_id = $1 AND w.org_id = $2 AND w.trashed_at IS NULL
		ORDER BY w.rank COLLATE "C"`, spaceID, actor.OrgID)
	if err != nil {
		return nil, apperr.Internal("list items", err)
	}
	defer rows.Close()
	var out []Item
	for rows.Next() {
		var it Item
		if err := rows.Scan(&it.ID, &it.Key, &it.Title, &it.Type, &it.Status,
			&it.StatusID, &it.StatusCategory, &it.AssigneeID, &it.ThreadID,
			&it.Resolved); err != nil {
			return nil, apperr.Internal("scan item", err)
		}
		out = append(out, it)
	}
	return out, rows.Err()
}

// Statuses lists a space's workflow (for transition pickers).
type Status struct {
	ID       int64  `json:"id"`
	Name     string `json:"name"`
	Category int    `json:"category"`
}

func (s *Service) Statuses(ctx context.Context, actor auth.Identity, spaceID int64) ([]Status, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT st.id, st.name, st.category
		FROM status st JOIN space sp ON sp.status_set_id = st.status_set_id
		WHERE sp.id = $1 AND sp.org_id = $2
		ORDER BY st.position`, spaceID, actor.OrgID)
	if err != nil {
		return nil, apperr.Internal("statuses", err)
	}
	defer rows.Close()
	var out []Status
	for rows.Next() {
		var st Status
		if err := rows.Scan(&st.ID, &st.Name, &st.Category); err != nil {
			return nil, apperr.Internal("scan status", err)
		}
		out = append(out, st)
	}
	return out, rows.Err()
}

type spaceRow struct {
	key         string
	statusSetID int64
}

func loadSpace(ctx context.Context, tx pgx.Tx, orgID, spaceID int64) (spaceRow, error) {
	var sp spaceRow
	err := tx.QueryRow(ctx, `
		SELECT key, status_set_id FROM space
		WHERE id = $1 AND org_id = $2 AND archived_at IS NULL AND trashed_at IS NULL`,
		spaceID, orgID).Scan(&sp.key, &sp.statusSetID)
	if err != nil {
		return sp, apperr.NotFound("space not found")
	}
	return sp, nil
}

func isKey(s string) bool {
	for i, r := range s {
		if i == 0 && !(r >= 'A' && r <= 'Z') {
			return false
		}
		if !(r >= 'A' && r <= 'Z' || r >= '0' && r <= '9') {
			return false
		}
	}
	return true
}
