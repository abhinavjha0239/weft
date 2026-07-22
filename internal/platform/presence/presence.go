// Package presence is the cross-node shared presence plane seam (the
// platform/blob.Store and platform/mail.Sender pattern): the gateway speaks
// ONLY the Plane interface, and which backend carries presence across a cell's
// gateway nodes is an operator choice, not a code change.
//
// Presence is DERIVED, EPHEMERAL state (ADR-002 P5, docs/SCHEMA.md): a user is
// active/idle while they hold a live gateway connection on ANY node, and
// offline only once their last connection across ALL nodes drops. It is NEVER
// event-logged — the plane is a side-channel, not the event log — and it is
// rebuildable from live connections, so a node re-publishes on (re)connect and
// a crash-truncated store simply refills as connections re-announce.
//
// Two drivers ship, both per-CELL and per-ORG scoped — the plane NEVER leaks
// presence across orgs on a cell (the cell model, docs/SCHEMA.md):
//
//   - local (default): an in-process map, i.e. the pre-S5 single-node status
//     quo — each process sees only its own connections. Kept as the seam's
//     zero-dependency default (an un-wired hub and every single-node test keep
//     their semantics) and as the red/green control for the multi-node work.
//   - pg: the dormant UNLOGGED presence table as the shared store +
//     LISTEN/NOTIFY as the delta bus — NO new dependency, NO migration. Every
//     node reads the org-wide view from the table and fans deltas off the bus
//     to ITS OWN local connections.
package presence

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/db"
)

// The derived-state vocabulary the gateway already fans as presence.changed —
// the plane carries these wire values verbatim, never re-mapping them.
const (
	stateActive  = "active"
	stateIdle    = "idle"
	stateOffline = "offline"
)

// Delta is one node-local presence transition to put on the plane: user U in
// org O moved to State. An "offline" delta means THIS node dropped U's last
// LOCAL connection — whether U is truly offline is decided cell-wide (a node
// that still holds U re-asserts its live state), never by one node alone.
type Delta struct {
	OrgID  int64
	UserID int64
	State  string
}

// Plane is the shared presence plane. Implementations must be safe for
// concurrent use, and every method is ORG-SCOPED — the plane never leaks
// presence across orgs within a cell.
type Plane interface {
	// Publish records U's derived state in the shared store and broadcasts the
	// delta to every node subscribed on the cell (the publisher included).
	Publish(ctx context.Context, d Delta) error
	// Snapshot returns the org-wide view: every currently active/idle user in
	// the org mapped to their state (offline users absent). This is the REST
	// bootstrap, sourced cell-wide instead of from one process's memory.
	Snapshot(ctx context.Context, orgID int64) (map[int64]string, error)
	// Subscribe blocks until ctx ends, delivering every delta published on the
	// cell to fn (one call per delta). The caller fans each delta to its OWN
	// local connections. A dropped bus connection is transparently re-established.
	Subscribe(ctx context.Context, fn func(context.Context, Delta))
}

// Open selects the driver by name — the blob/mail/metrics seam factory shape,
// so an operator swaps backends by config alone:
//
//	"" / "local": the in-process plane (single-node status quo, no DB writes).
//	"pg":         the UNLOGGED presence table + LISTEN/NOTIFY (multi-node,
//	              no new dependency, no migration).
//
// An unknown driver is a config error, never a silent fallback.
func Open(driver string, pool *pgxpool.Pool, log *slog.Logger) (Plane, error) {
	switch driver {
	case "", "local":
		return Local(), nil
	case "pg":
		if pool == nil {
			return nil, fmt.Errorf("presence: pg driver needs a database pool")
		}
		if log == nil {
			log = slog.Default()
		}
		return &pgPlane{pool: pool, log: log}, nil
	default:
		return nil, fmt.Errorf("presence: unknown driver %q (implement Plane and register it here)", driver)
	}
}

// localBusBuffer bounds the in-process delta queue. A connect/idle/disconnect
// storm that overruns it drops deltas (best-effort, like the event bus) —
// presence is rebuildable, so a dropped delta self-heals on the next
// transition or snapshot.
const localBusBuffer = 4096

// Local is the in-process plane: presence lives in this process's memory only,
// exactly the pre-S5 single-node behavior. It is the NewHub default and the
// red/green control — two hubs each on their own Local plane never see each
// other's users, which is precisely the fragmentation the pg plane fixes.
func Local() Plane {
	return &localPlane{
		byOrg: map[int64]map[int64]string{},
		bus:   make(chan Delta, localBusBuffer),
	}
}

type localPlane struct {
	mu    sync.Mutex
	byOrg map[int64]map[int64]string // orgID → userID → state (offline absent)
	bus   chan Delta
}

func (p *localPlane) Publish(_ context.Context, d Delta) error {
	p.mu.Lock()
	users := p.byOrg[d.OrgID]
	if users == nil {
		users = map[int64]string{}
		p.byOrg[d.OrgID] = users
	}
	if d.State == stateOffline {
		delete(users, d.UserID)
	} else {
		users[d.UserID] = d.State
	}
	p.mu.Unlock()
	select {
	case p.bus <- d:
	default: // bus backed up: drop (best-effort; presence is rebuildable)
	}
	return nil
}

func (p *localPlane) Snapshot(_ context.Context, orgID int64) (map[int64]string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[int64]string, len(p.byOrg[orgID]))
	for uid, st := range p.byOrg[orgID] {
		out[uid] = st
	}
	return out, nil
}

func (p *localPlane) Subscribe(ctx context.Context, fn func(context.Context, Delta)) {
	for {
		select {
		case <-ctx.Done():
			return
		case d := <-p.bus:
			fn(ctx, d)
		}
	}
}

// notifyChannel carries presence deltas — the side-channel delta bus, kept
// SEPARATE from the event_log channel because presence is never event-logged
// (ADR-002 P5).
const notifyChannel = "presence_delta"

type pgPlane struct {
	pool *pgxpool.Pool
	log  *slog.Logger
}

func (p *pgPlane) Publish(ctx context.Context, d Delta) error {
	// The UNLOGGED presence table is the shared store — the row is the org-wide
	// truth any node reads. Upsert + NOTIFY ride ONE transaction (the
	// eventlog.Append idiom) so the delta is delivered only after the row it
	// describes is committed. Offline keeps the row (status=3) as a last-seen
	// marker; Snapshot filters it out.
	return db.WithTx(ctx, p.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			INSERT INTO presence (user_id, status, last_active_at)
			VALUES ($1, $2, now())
			ON CONFLICT (user_id) DO UPDATE
			  SET status = EXCLUDED.status, last_active_at = EXCLUDED.last_active_at`,
			d.UserID, statusCode(d.State)); err != nil {
			return fmt.Errorf("presence: upsert: %w", err)
		}
		// The delta fans to every node LISTENing on the cell (payload
		// org:user:state — no content; presence is metadata).
		if _, err := tx.Exec(ctx, `SELECT pg_notify($1, $2)`, notifyChannel, encode(d)); err != nil {
			return fmt.Errorf("presence: notify: %w", err)
		}
		return nil
	})
}

func (p *pgPlane) Snapshot(ctx context.Context, orgID int64) (map[int64]string, error) {
	// Org-scope via the user_account join — the presence table carries no
	// org_id and this slice adds NO migration, so the join is what confines the
	// read to one org (and drops rows for deleted users). The result is bounded
	// by the org's currently-connected users, the same shape as the snapshot it
	// replaces.
	rows, err := p.pool.Query(ctx, `
		SELECT p.user_id, p.status
		FROM presence p
		JOIN user_account u ON u.id = p.user_id
		WHERE u.org_id = $1 AND p.status <> 3`, orgID)
	if err != nil {
		return nil, fmt.Errorf("presence: snapshot: %w", err)
	}
	defer rows.Close()
	out := map[int64]string{}
	for rows.Next() {
		var uid int64
		var status int16
		if err := rows.Scan(&uid, &status); err != nil {
			return nil, fmt.Errorf("presence: snapshot scan: %w", err)
		}
		out[uid] = stateFromCode(status)
	}
	return out, rows.Err()
}

// Subscribe mirrors the gateway's dispatcher listenLoop: LISTEN on the presence
// channel and hand each decoded delta to fn, transparently re-establishing a
// dropped connection until ctx ends.
func (p *pgPlane) Subscribe(ctx context.Context, fn func(context.Context, Delta)) {
	for ctx.Err() == nil {
		if err := p.listen(ctx, fn); err != nil && ctx.Err() == nil {
			p.log.Warn("presence: listen loop restarting", "err", err)
			time.Sleep(time.Second)
		}
	}
}

func (p *pgPlane) listen(ctx context.Context, fn func(context.Context, Delta)) error {
	conn, err := p.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, "LISTEN "+notifyChannel); err != nil {
		return err
	}
	for {
		n, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			return err
		}
		if d, ok := decode(n.Payload); ok {
			fn(ctx, d)
		}
	}
}

// statusCode maps a wire state to the presence.status code (0006: 1 active ·
// 2 idle · 3 offline). An unknown state is treated as offline — the safe
// default (a user is never wrongly shown present).
func statusCode(state string) int16 {
	switch state {
	case stateActive:
		return 1
	case stateIdle:
		return 2
	default:
		return 3
	}
}

func stateFromCode(code int16) string {
	switch code {
	case 1:
		return stateActive
	case 2:
		return stateIdle
	default:
		return stateOffline
	}
}

// encode/decode render a Delta as the pg_notify text payload "org:user:state".
// State is a fixed small word with no colon, so a 3-way split is unambiguous
// and cheap — no JSON allocation on the 100k-connect hot path.
func encode(d Delta) string {
	return strconv.FormatInt(d.OrgID, 10) + ":" +
		strconv.FormatInt(d.UserID, 10) + ":" + d.State
}

func decode(payload string) (Delta, bool) {
	parts := strings.SplitN(payload, ":", 3)
	if len(parts) != 3 || parts[2] == "" {
		return Delta{}, false
	}
	org, err1 := strconv.ParseInt(parts[0], 10, 64)
	user, err2 := strconv.ParseInt(parts[1], 10, 64)
	if err1 != nil || err2 != nil {
		return Delta{}, false
	}
	return Delta{OrgID: org, UserID: user, State: parts[2]}, true
}
