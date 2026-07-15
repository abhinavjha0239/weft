package worktrack

import (
	"context"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/internal/eventlog"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

// Sprints (P-11): a sprint is a COLUMN on work_item (work_item.sprint_id),
// Jira's active-sprint model — one sprint per item. The `sprint` table and
// the FK/partial index have existed since migration 0005 with no writers;
// this file is those writers. All mutations gate on the org-scoped
// VerbEditItems (a per-space sprint-admin verb is a recorded module gap).
const (
	sprintFuture = 1
	sprintActive = 2
	sprintClosed = 3
)

type CreateSprintParams struct {
	SpaceID  int64
	Name     string
	Goal     string
	StartsAt *time.Time
	EndsAt   *time.Time
}

// SprintSummary is the read/return shape. item_count is populated only by
// ListSprints (a freshly created sprint has none).
type SprintSummary struct {
	ID          int64      `json:"id"`
	Name        string     `json:"name"`
	Goal        string     `json:"goal"`
	State       int        `json:"state"`
	StartsAt    *time.Time `json:"starts_at"`
	EndsAt      *time.Time `json:"ends_at"`
	CompletedAt *time.Time `json:"completed_at"`
	ItemCount   int        `json:"item_count"`
}

// CreateSprint mints a state=1 (future) sprint in a live, org-local space.
func (s *Service) CreateSprint(ctx context.Context, actor auth.Identity, p CreateSprintParams) (SprintSummary, error) {
	name := strings.TrimSpace(p.Name)
	if n := utf8.RuneCountInString(name); n < 1 || n > 100 {
		return SprintSummary{}, apperr.Invalid("name must be 1-100 characters")
	}
	if utf8.RuneCountInString(p.Goal) > 2000 {
		return SprintSummary{}, apperr.Invalid("goal must be at most 2000 characters")
	}
	// Cross-date validity only when both are supplied (a one-sided plan is fine).
	if p.StartsAt != nil && p.EndsAt != nil && !p.EndsAt.After(*p.StartsAt) {
		return SprintSummary{}, apperr.Invalid("ends_at must be after starts_at")
	}
	out := SprintSummary{
		Name: name, Goal: p.Goal, State: sprintFuture,
		StartsAt: p.StartsAt, EndsAt: p.EndsAt,
	}
	err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.perms.Require(ctx, tx, actor, perms.VerbEditItems,
			perms.OrgScope(actor.OrgID)); err != nil {
			return err
		}
		// loadSpace is the oracle-free gate: a foreign-org, archived, or trashed
		// space is a 404, never confirmed to exist.
		if _, err := loadSpace(ctx, tx, actor.OrgID, p.SpaceID); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `
			INSERT INTO sprint (org_id, space_id, name, goal, state, starts_at, ends_at)
			VALUES ($1, $2, $3, $4, 1, $5, $6) RETURNING id`,
			actor.OrgID, p.SpaceID, name, p.Goal, p.StartsAt, p.EndsAt).Scan(&out.ID); err != nil {
			return apperr.Internal("create sprint", err)
		}
		if _, err := eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
			EntityType: enum.EntitySprint, EntityID: out.ID, Verb: "sprint.created",
			Payload: eventlog.MustPayload(map[string]any{
				"sprint_id": out.ID, "space_id": p.SpaceID, "name": name}),
		}); err != nil {
			return apperr.Internal("append event", err)
		}
		return nil
	})
	if err != nil {
		return SprintSummary{}, err
	}
	return out, nil
}

// ListSprints returns a space's sprints ordered state ASC, id DESC. item_count
// comes from ONE grouped, live-only count join (no N+1). Org-pinned like the
// sibling ListItems read: a foreign/nonexistent space yields the empty set,
// which is itself oracle-free.
func (s *Service) ListSprints(ctx context.Context, actor auth.Identity, spaceID int64) ([]SprintSummary, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT sp.id, sp.name, sp.goal, sp.state,
		       sp.starts_at, sp.ends_at, sp.completed_at,
		       count(w.id) FILTER (WHERE w.trashed_at IS NULL)
		FROM sprint sp
		LEFT JOIN work_item w ON w.sprint_id = sp.id AND w.org_id = sp.org_id
		WHERE sp.space_id = $1 AND sp.org_id = $2
		GROUP BY sp.id
		ORDER BY sp.state ASC, sp.id DESC`, spaceID, actor.OrgID)
	if err != nil {
		return nil, apperr.Internal("list sprints", err)
	}
	defer rows.Close()
	var out []SprintSummary
	for rows.Next() {
		var sp SprintSummary
		if err := rows.Scan(&sp.ID, &sp.Name, &sp.Goal, &sp.State,
			&sp.StartsAt, &sp.EndsAt, &sp.CompletedAt, &sp.ItemCount); err != nil {
			return nil, apperr.Internal("scan sprint", err)
		}
		out = append(out, sp)
	}
	return out, rows.Err()
}

type StartSprintParams struct {
	StartsAt *time.Time
	EndsAt   *time.Time
}

// StartSprint transitions a future sprint to active (1→2 only). It stamps
// starts_at (body value or now) and ends_at when the body supplies one. Jira
// parity: at most one active sprint per space — a second start is a 409.
func (s *Service) StartSprint(ctx context.Context, actor auth.Identity, sprintID int64, p StartSprintParams) error {
	return db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.perms.Require(ctx, tx, actor, perms.VerbEditItems,
			perms.OrgScope(actor.OrgID)); err != nil {
			return err
		}
		spr, err := loadSprint(ctx, tx, actor.OrgID, sprintID)
		if err != nil {
			return err
		}
		if spr.state != sprintFuture {
			return apperr.Invalid("only a future sprint can be started")
		}
		var active bool
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS (SELECT 1 FROM sprint
			 WHERE space_id = $1 AND org_id = $2 AND state = 2)`,
			spr.spaceID, actor.OrgID).Scan(&active); err != nil {
			return apperr.Internal("active check", err)
		}
		if active {
			return apperr.Conflict("this space already has an active sprint")
		}
		if _, err := tx.Exec(ctx, `
			UPDATE sprint SET state = 2,
			  starts_at = COALESCE($2, now()),
			  ends_at = COALESCE($3, ends_at)
			WHERE id = $1`, sprintID, p.StartsAt, p.EndsAt); err != nil {
			return apperr.Internal("start sprint", err)
		}
		if _, err := eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
			EntityType: enum.EntitySprint, EntityID: sprintID, Verb: "sprint.started",
			Payload: eventlog.MustPayload(map[string]any{
				"sprint_id": sprintID, "space_id": spr.spaceID}),
		}); err != nil {
			return apperr.Internal("append event", err)
		}
		return nil
	})
}

type CloseSprintParams struct {
	MoveToSprintID *int64
}

// CloseSprint transitions an active sprint to closed (2→3 only) and stamps
// completed_at. UNFINISHED items on this sprint carry over to
// move_to_sprint_id (which must be same-space and not itself closed) or to the
// backlog (sprint_id = NULL) when none is given. FINISHED items keep their
// sprint_id — sprint history is the report.
func (s *Service) CloseSprint(ctx context.Context, actor auth.Identity, sprintID int64, p CloseSprintParams) error {
	return db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.perms.Require(ctx, tx, actor, perms.VerbEditItems,
			perms.OrgScope(actor.OrgID)); err != nil {
			return err
		}
		spr, err := loadSprint(ctx, tx, actor.OrgID, sprintID)
		if err != nil {
			return err
		}
		if spr.state != sprintActive {
			return apperr.Invalid("only an active sprint can be closed")
		}
		// Resolve the carry-over target. Absent (nil / 0 sentinel) → backlog.
		var moveTo *int64
		if p.MoveToSprintID != nil && *p.MoveToSprintID != 0 {
			target := *p.MoveToSprintID
			if target == sprintID {
				return apperr.Invalid("cannot move items into the sprint being closed")
			}
			// Same query pins org + space, so a foreign-org, cross-space, or
			// nonexistent target is one uniform 400 (never an existence oracle).
			var tstate int
			if err := tx.QueryRow(ctx, `
				SELECT state FROM sprint
				WHERE id = $1 AND org_id = $2 AND space_id = $3`,
				target, actor.OrgID, spr.spaceID).Scan(&tstate); err != nil {
				return apperr.Invalid("move target must be a sprint in the same space")
			}
			if tstate == sprintClosed {
				return apperr.Invalid("cannot move items into a closed sprint")
			}
			moveTo = &target
		}
		if _, err := tx.Exec(ctx, `
			UPDATE sprint SET state = 3, completed_at = now() WHERE id = $1`,
			sprintID); err != nil {
			return apperr.Internal("close sprint", err)
		}
		// CARRY-OVER, one UPDATE. The `resolved_at IS NULL` guard is
		// load-bearing: TestSprints pins that FINISHED items stay on the closed
		// sprint (drop the guard and they move too — the "finished stay" assert
		// fails). trashed items never carry over.
		tag, err := tx.Exec(ctx, `
			UPDATE work_item SET sprint_id = $1, updated_at = now()
			WHERE org_id = $2 AND sprint_id = $3
			  AND resolved_at IS NULL AND trashed_at IS NULL`,
			moveTo, actor.OrgID, sprintID)
		if err != nil {
			return apperr.Internal("carry over", err)
		}
		if _, err := eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
			EntityType: enum.EntitySprint, EntityID: sprintID, Verb: "sprint.closed",
			Payload: eventlog.MustPayload(map[string]any{
				"sprint_id": sprintID, "moved": tag.RowsAffected(), "moved_to": moveTo}),
		}); err != nil {
			return apperr.Internal("append event", err)
		}
		return nil
	})
}

type sprintRow struct {
	spaceID int64
	state   int
}

// loadSprint is the oracle-free sprint gate: a foreign-org or nonexistent
// sprint is a 404, indistinguishable from one another.
func loadSprint(ctx context.Context, tx pgx.Tx, orgID, sprintID int64) (sprintRow, error) {
	var sp sprintRow
	err := tx.QueryRow(ctx, `
		SELECT space_id, state FROM sprint WHERE id = $1 AND org_id = $2`,
		sprintID, orgID).Scan(&sp.spaceID, &sp.state)
	if err != nil {
		return sp, apperr.NotFound("sprint not found")
	}
	return sp, nil
}
