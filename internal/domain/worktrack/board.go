package worktrack

import (
	"cmp"
	"context"
	"slices"

	"github.com/jackc/pgx/v5"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/internal/eventlog"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

// MoveItem reorders one item on its board by minting a rank strictly between
// its new neighbours (LexoRank, F-21). afterID / beforeID name the items the
// moved item is dropped between; at least one is required (a nil after means
// "to the top", a nil before means "to the bottom"). Both named items must be
// live, org-local, and in the moved item's rank_context — cross-space moves are
// a recorded gap, not this slice.
//
// The whole context is locked FOR UPDATE in board order so concurrent moves
// serialize under one snapshot; pre-P-12 NULL-rank rows are backfilled with
// evenly spaced ranks first, and an exhausted gap respaces the context and
// retries once (after an even rebalance the neighbours are far apart, so it
// always fits).
func (s *Service) MoveItem(ctx context.Context, actor auth.Identity, itemID int64, afterID, beforeID *int64) error {
	if afterID == nil && beforeID == nil {
		return apperr.Invalid("after_item_id or before_item_id required")
	}
	if afterID != nil && beforeID != nil && *afterID == *beforeID {
		return apperr.Invalid("after_item_id and before_item_id must differ")
	}
	return db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.perms.Require(ctx, tx, actor, perms.VerbEditItems,
			perms.OrgScope(actor.OrgID)); err != nil {
			return err
		}
		var ctxID int64
		if err := tx.QueryRow(ctx, `
			SELECT rank_context_id FROM work_item
			WHERE id = $1 AND org_id = $2 AND trashed_at IS NULL`,
			itemID, actor.OrgID).Scan(&ctxID); err != nil {
			return apperr.NotFound("item not found")
		}
		order, ranks, hasNull, err := lockContextRanks(ctx, tx, actor.OrgID, ctxID)
		if err != nil {
			return err
		}
		if _, ok := ranks[itemID]; !ok {
			// Trashed between the unlocked read and the lock.
			return apperr.NotFound("item not found")
		}
		if err := checkNeighbor(ctx, tx, actor.OrgID, afterID, ranks); err != nil {
			return err
		}
		if err := checkNeighbor(ctx, tx, actor.OrgID, beforeID, ranks); err != nil {
			return err
		}
		if hasNull {
			ids := slices.Clone(order)
			slices.Sort(ids)
			if err := applyRanks(ctx, tx, ranks, ids); err != nil {
				return err
			}
		}
		lo, hi := neighborBounds(afterID, beforeID, ranks)
		newRank, ok := rankBetween(lo, hi)
		if !ok {
			ids := slices.Clone(order)
			slices.SortFunc(ids, func(a, b int64) int {
				if c := cmp.Compare(ranks[a], ranks[b]); c != 0 {
					return c
				}
				return cmp.Compare(a, b)
			})
			if err := applyRanks(ctx, tx, ranks, ids); err != nil {
				return err
			}
			lo, hi = neighborBounds(afterID, beforeID, ranks)
			if newRank, ok = rankBetween(lo, hi); !ok {
				return apperr.Internal("rank exhausted after rebalance", nil)
			}
		}
		if _, err := tx.Exec(ctx,
			`UPDATE work_item SET rank = $1, updated_at = now() WHERE id = $2`,
			newRank, itemID); err != nil {
			return apperr.Internal("update rank", err)
		}
		_, err = eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
			EntityType: enum.EntityWorkItem, EntityID: itemID, Verb: "workitem.reordered",
			Payload: eventlog.MustPayload(map[string]any{
				"item_id": itemID, "after": afterID, "before": beforeID}),
		})
		return err
	})
}

// lockContextRanks locks every live item in a rank_context FOR UPDATE and
// returns them in board order (rank NULLS LAST, id) alongside a rank lookup.
// A NULL DB rank maps to "" in ranks and flips hasNull — "" is never a stored
// rank (CreateItem/backfill/rebalance always mint non-empty), so it is an
// unambiguous NULL marker for the backfill decision.
func lockContextRanks(ctx context.Context, tx pgx.Tx, orgID, ctxID int64) (order []int64, ranks map[int64]string, hasNull bool, err error) {
	rows, err := tx.Query(ctx, `
		SELECT id, rank FROM work_item
		WHERE rank_context_id = $1 AND org_id = $2 AND trashed_at IS NULL
		ORDER BY rank NULLS LAST, id
		FOR UPDATE`, ctxID, orgID)
	if err != nil {
		return nil, nil, false, apperr.Internal("lock context", err)
	}
	defer rows.Close()
	ranks = map[int64]string{}
	for rows.Next() {
		var id int64
		var r *string
		if err := rows.Scan(&id, &r); err != nil {
			return nil, nil, false, apperr.Internal("scan rank", err)
		}
		order = append(order, id)
		if r == nil {
			hasNull = true
			ranks[id] = ""
		} else {
			ranks[id] = *r
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, false, apperr.Internal("read ranks", err)
	}
	return order, ranks, hasNull, nil
}

// checkNeighbor validates a named neighbour. An item in the locked context set
// is fine; otherwise a same-org live item is a cross-context move (400) while
// anything else is an oracle-free 404 (nonexistent, trashed, or another org).
func checkNeighbor(ctx context.Context, tx pgx.Tx, orgID int64, neighbor *int64, ranks map[int64]string) error {
	if neighbor == nil {
		return nil
	}
	if _, ok := ranks[*neighbor]; ok {
		return nil
	}
	var otherCtx int64
	if err := tx.QueryRow(ctx, `
		SELECT rank_context_id FROM work_item
		WHERE id = $1 AND org_id = $2 AND trashed_at IS NULL`,
		*neighbor, orgID).Scan(&otherCtx); err != nil {
		return apperr.NotFound("neighbor item not found")
	}
	return apperr.Invalid("neighbor item is on a different board (cross-context moves are not supported)")
}

// neighborBounds turns the two optional neighbours into rankBetween bounds:
// a nil neighbour becomes the "" sentinel (start for lo, end for hi).
func neighborBounds(afterID, beforeID *int64, ranks map[int64]string) (lo, hi string) {
	if afterID != nil {
		lo = ranks[*afterID]
	}
	if beforeID != nil {
		hi = ranks[*beforeID]
	}
	return lo, hi
}

// applyRanks respaces the given ids (already in the desired final order) with
// evenly spread 3-char ranks and updates the in-memory map to match. It powers
// both the NULL-rank backfill (ids sorted by id) and the gap rebalance (ids
// sorted by current rank). It touches rank only, not updated_at: a respace is
// an internal reindex, not a user-facing edit of every item.
func applyRanks(ctx context.Context, tx pgx.Tx, ranks map[int64]string, ordered []int64) error {
	fresh := evenRanks(len(ordered))
	for i, id := range ordered {
		if _, err := tx.Exec(ctx,
			`UPDATE work_item SET rank = $1 WHERE id = $2`, fresh[i], id); err != nil {
			return apperr.Internal("respace rank", err)
		}
		ranks[id] = fresh[i]
	}
	return nil
}
