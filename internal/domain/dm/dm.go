// Package dm implements direct-message spaces (ADR-008): a DM is its own
// container — not a channel — with a canonical sorted-participant key
// (create-or-get needs no locks, just the unique index), one root thread,
// and participant-only visibility enforced everywhere content flows
// (fetch, list, search, gateway fan-out, read state).
package dm

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/internal/eventlog"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

const maxParticipants = 25

type Service struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Service { return &Service{pool: pool} }

type Summary struct {
	ID             int64   `json:"id"`
	Kind           int16   `json:"kind"` // 1 one-to-one · 2 group · 3 self
	RootThreadID   int64   `json:"root_thread_id"`
	ParticipantIDs []int64 `json:"participant_ids"`
	LastMessageID  int64   `json:"last_message_id,omitempty"`
}

// Open creates or returns THE DM space for a participant set (the actor is
// always included). The canonical dm_key makes this idempotent under
// concurrency via the unique index alone — no locks.
func (s *Service) Open(ctx context.Context, actor auth.Identity, userIDs []int64) (Summary, error) {
	set := map[int64]bool{actor.UserID: true}
	for _, id := range userIDs {
		if id <= 0 {
			return Summary{}, apperr.Invalid("bad user id")
		}
		set[id] = true
	}
	if len(set) > maxParticipants {
		return Summary{}, apperr.Invalid(fmt.Sprintf("too many participants (max %d)", maxParticipants))
	}
	ids := make([]int64, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	var kind int16
	switch {
	case len(ids) == 1:
		kind = 3 // self
	case len(ids) == 2:
		kind = 1 // one-to-one
	default:
		kind = 2 // group
	}
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = fmt.Sprint(id)
	}
	key := strings.Join(parts, ":")

	out := Summary{Kind: kind, ParticipantIDs: ids}
	err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		// Every participant must be a live account in the actor's org.
		var live int
		if err := tx.QueryRow(ctx, `
			SELECT count(*) FROM user_account
			WHERE org_id = $1 AND id = ANY($2) AND deactivated_at IS NULL`,
			actor.OrgID, ids).Scan(&live); err != nil {
			return apperr.Internal("participant check", err)
		}
		if live != len(ids) {
			return apperr.Invalid("unknown or deactivated participant")
		}
		err := tx.QueryRow(ctx, `
			INSERT INTO dm_space (org_id, kind, dm_key) VALUES ($1, $2, $3)
			ON CONFLICT (org_id, dm_key) DO NOTHING
			RETURNING id`, actor.OrgID, kind, key).Scan(&out.ID)
		if err == pgx.ErrNoRows { // already exists → get
			if err := tx.QueryRow(ctx,
				`SELECT id FROM dm_space WHERE org_id = $1 AND dm_key = $2`,
				actor.OrgID, key).Scan(&out.ID); err != nil {
				return apperr.Internal("resolve dm", err)
			}
			if err := tx.QueryRow(ctx,
				`SELECT id FROM thread WHERE dm_space_id = $1 AND kind = 2`,
				out.ID).Scan(&out.RootThreadID); err != nil {
				return apperr.Internal("resolve dm thread", err)
			}
			return nil
		}
		if err != nil {
			return apperr.Internal("create dm", err)
		}
		for _, uid := range ids {
			if _, err := tx.Exec(ctx,
				`INSERT INTO dm_participant (dm_space_id, user_id) VALUES ($1, $2)`,
				out.ID, uid); err != nil {
				return apperr.Internal("add participant", err)
			}
		}
		if err := tx.QueryRow(ctx, `
			INSERT INTO thread (org_id, dm_space_id, kind) VALUES ($1, $2, 2)
			RETURNING id`, actor.OrgID, out.ID).Scan(&out.RootThreadID); err != nil {
			return apperr.Internal("dm root thread", err)
		}
		// user_ids in the payload lets the gateway deliver this to the
		// invited participants BEFORE their membership views refresh.
		if _, err := eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
			EntityType: enum.EntityDM, EntityID: out.ID, Verb: "dm.opened",
			Payload: eventlog.MustPayload(map[string]any{
				"dm_space_id": out.ID, "root_thread_id": out.RootThreadID,
				"user_ids": ids}),
		}); err != nil {
			return apperr.Internal("append event", err)
		}
		return nil
	})
	if err != nil {
		return Summary{}, err
	}
	return out, nil
}

// List returns the actor's DM spaces, most recently active first.
func (s *Service) List(ctx context.Context, actor auth.Identity) ([]Summary, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT ds.id, ds.kind, t.id, COALESCE(lm.id, 0)
		FROM dm_participant p
		JOIN dm_space ds ON ds.id = p.dm_space_id
		JOIN thread t ON t.dm_space_id = ds.id AND t.kind = 2
		LEFT JOIN LATERAL (
			SELECT id FROM message
			WHERE dm_space_id = ds.id AND deleted_at IS NULL
			ORDER BY id DESC LIMIT 1
		) lm ON true
		WHERE p.user_id = $1 AND ds.org_id = $2
		ORDER BY COALESCE(lm.id, 0) DESC, ds.id DESC`,
		actor.UserID, actor.OrgID)
	if err != nil {
		return nil, apperr.Internal("list dms", err)
	}
	defer rows.Close()
	var out []Summary
	byID := map[int64]*Summary{}
	for rows.Next() {
		var d Summary
		if err := rows.Scan(&d.ID, &d.Kind, &d.RootThreadID, &d.LastMessageID); err != nil {
			return nil, apperr.Internal("scan dm", err)
		}
		out = append(out, d)
		byID[d.ID] = &out[len(out)-1]
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return out, nil
	}
	spaceIDs := make([]int64, len(out))
	for i, d := range out {
		spaceIDs[i] = d.ID
	}
	prows, err := s.pool.Query(ctx, `
		SELECT dm_space_id, user_id FROM dm_participant
		WHERE dm_space_id = ANY($1) ORDER BY user_id`, spaceIDs)
	if err != nil {
		return nil, apperr.Internal("dm participants", err)
	}
	defer prows.Close()
	for prows.Next() {
		var sid, uid int64
		if err := prows.Scan(&sid, &uid); err != nil {
			return nil, apperr.Internal("scan participant", err)
		}
		byID[sid].ParticipantIDs = append(byID[sid].ParticipantIDs, uid)
	}
	return out, prows.Err()
}
