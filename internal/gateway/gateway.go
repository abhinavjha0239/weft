// Package gateway is the server→client event stream (ADR-002, F-2 contract):
// seq IS the per-org event-log id; sequences are gappy by construction (ACL
// filtering), and the guarantee is "no undetectable loss" via checkpoint
// heartbeats. Resume = reconnect with last_id; the gap replays from the log.
//
// Scale shape (docs/SCHEMA.md): ONE dispatcher goroutine LISTENs for event
// notifications and wakes only the orgs that have connections with pending
// signals — idle orgs cost zero. Each connection pumps independently with the
// txid-gated read (same F-1 rule as consumers).
package gateway

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/time/rate"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/platform/metrics"
)

const (
	checkpointInterval = 30 * time.Second
	sweepInterval      = 5 * time.Second // poll fallback if a NOTIFY is missed
	batchLimit         = 200
)

// Envelope is the wire format (ADR-002 P1, org_id per CC-1).
type Envelope struct {
	Seq     int64           `json:"seq"`
	Type    string          `json:"type"`
	OrgID   int64           `json:"org_id"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

type client struct {
	conn   *websocket.Conn
	id     auth.Identity
	shard  *orgShard // the org slice this connection belongs to (set at register)
	lastID int64
	wake   chan struct{}
	// Membership views for ACL filtering (channels + DM participation);
	// refreshed when a membership event for this user arrives (F-2 replay
	// rule, M0 slice).
	channels map[int64]bool
	dms      map[int64]bool
	// Inbound signal-frame budget (typing storms, abuse).
	frameLimit *rate.Limiter
	// Serializes all writes to conn (coder/websocket forbids concurrent Write).
	writeMu sync.Mutex
}

// orgShard is one org's slice of the hub — its live connections and its
// per-user derived-presence registry — under a single per-org lock. The hub's
// top-level mu only guards the orgs map; all per-org work takes the shard lock,
// so a 100k-connection org's traffic never contends the whole hub. Connections
// and presence drain together: the last connection to leave empties both maps.
type orgShard struct {
	orgID int64
	mu    sync.Mutex
	conns map[*client]struct{}
	// Per-user derived presence (connection count + last activity + idle flag);
	// per-process, never stored (the UNLOGGED presence table is unused).
	userConns map[int64]*userPresence // userID → presence
}

type Hub struct {
	pool *pgxpool.Pool
	log  *slog.Logger

	// Durable read-state dependency for the read_marker signal; optional
	// (nil = signal ignored), set via SetMarkReader at wiring time.
	markReader MarkReader

	// IdleAfter is how long a connected user may be silent before presence
	// demotes active→idle (P-05). Exported so tests shrink it; default 10min.
	IdleAfter time.Duration

	// mu guards the orgs map only (shard lookup/create/delete). Each org's
	// connection set and derived-presence registry live under that shard's OWN
	// lock, so a high-fan-out org never serializes the whole hub — the S3 lock
	// split (docs/PERF.md). S5's multi-node presence reuses these shards.
	mu   sync.Mutex
	orgs map[int64]*orgShard

	// Metrics (S0), optional (default Nop). pumpQueries is THE S3 signal — one
	// catch-up query PER connection PER wake, so it scales with connection
	// count until per-org multicast lands (docs/PERF.md). Set once at wiring.
	pumpQueries metrics.Counter
	deliveries  metrics.Counter
	connections metrics.Gauge
}

func NewHub(pool *pgxpool.Pool, log *slog.Logger) *Hub {
	h := &Hub{pool: pool, log: log, IdleAfter: 10 * time.Minute,
		orgs: map[int64]*orgShard{}}
	h.SetMetrics(metrics.Nop())
	return h
}

// shard returns the org's shard, or nil when the org has no connections.
func (h *Hub) shard(orgID int64) *orgShard {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.orgs[orgID]
}

// shards snapshots the live shard set for the sweeps: copied under the map
// lock, iterated (and locked individually) without it.
func (h *Hub) shards() []*orgShard {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]*orgShard, 0, len(h.orgs))
	for _, sh := range h.orgs {
		out = append(out, sh)
	}
	return out
}

// register adds c to its org's shard (creating the shard on first use) and
// records the connection for presence, reporting whether an "active" presence
// broadcast is owed. Registration nests h.mu→sh.mu so a shard can never be
// deleted by a concurrent deregister between lookup and insert (which would
// strand the connection in an orphaned shard, invisible to wake/fan-out).
func (h *Hub) register(c *client, now time.Time) (announce bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	sh := h.orgs[c.id.OrgID]
	if sh == nil {
		sh = &orgShard{orgID: c.id.OrgID, conns: map[*client]struct{}{},
			userConns: map[int64]*userPresence{}}
		h.orgs[c.id.OrgID] = sh
	}
	c.shard = sh
	sh.mu.Lock()
	defer sh.mu.Unlock()
	sh.conns[c] = struct{}{}
	h.connections.Set(float64(len(sh.conns)), strconv.FormatInt(c.id.OrgID, 10))
	return sh.trackConnect(c.id.UserID, now)
}

// deregister removes c from its shard, dropping the shard from the orgs map
// once its last connection leaves, and reports whether the user went offline.
// Nested h.mu→sh.mu matches register so the empty-shard delete is atomic.
func (h *Hub) deregister(c *client) (wentOffline bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	sh := c.shard
	sh.mu.Lock()
	delete(sh.conns, c)
	h.connections.Set(float64(len(sh.conns)), strconv.FormatInt(c.id.OrgID, 10))
	wentOffline = sh.trackDisconnect(c.id.UserID)
	empty := len(sh.conns) == 0
	sh.mu.Unlock()
	if empty {
		delete(h.orgs, sh.orgID)
	}
	return wentOffline
}

// SetMetrics wires an observability registry (S0). Optional — the default is
// Nop, so an un-instrumented hub pays nothing. Call once before Run/Serve.
func (h *Hub) SetMetrics(reg metrics.Registry) {
	h.pumpQueries = reg.Counter("gateway_pump_queries_total")
	h.deliveries = reg.Counter("fanout_deliveries_total")
	h.connections = reg.Gauge("gateway_connections", "org")
}

// SetMarkReader wires the durable read-state service (rest layer adapts
// auth.Identity ↔ Actor).
func (h *Hub) SetMarkReader(m MarkReader) { h.markReader = m }

// Run is the dispatcher: LISTEN on the event-log channel and wake that org's
// connections; a slow sweep covers missed notifications. Blocks until ctx ends.
func (h *Hub) Run(ctx context.Context) {
	go h.sweep(ctx)
	go h.presenceSweep(ctx)
	for ctx.Err() == nil {
		if err := h.listenLoop(ctx); err != nil && ctx.Err() == nil {
			h.log.Warn("gateway: listen loop restarting", "err", err)
			time.Sleep(time.Second)
		}
	}
}

func (h *Hub) listenLoop(ctx context.Context) error {
	conn, err := h.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `LISTEN event_log`); err != nil {
		return err
	}
	for {
		n, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			return err
		}
		if orgID, err := strconv.ParseInt(n.Payload, 10, 64); err == nil {
			h.wakeOrg(orgID)
		}
	}
}

func (h *Hub) sweep(ctx context.Context) {
	t := time.NewTicker(sweepInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			for _, sh := range h.shards() {
				sh.wakeAll()
			}
		}
	}
}

func (h *Hub) wakeOrg(orgID int64) {
	if sh := h.shard(orgID); sh != nil {
		sh.wakeAll()
	}
}

// wakeAll signals every connection in the shard to run its catch-up pump.
// (Per-connection catch-up; the S3 per-org multicast reader replaces this.)
func (sh *orgShard) wakeAll() {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	for c := range sh.conns {
		select {
		case c.wake <- struct{}{}:
		default: // already signaled
		}
	}
}

// Serve upgrades the request and streams events. ?last_id=N resumes.
func (h *Hub) Serve(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	lastID, _ := strconv.ParseInt(r.URL.Query().Get("last_id"), 10, 64)
	// last_id=-1: tail mode — start from the org's current head instead of
	// replaying history (fresh clients render state from REST, then follow).
	if lastID < 0 {
		if err := h.pool.QueryRow(r.Context(),
			`SELECT COALESCE(MAX(id), 0) FROM event_log WHERE org_id = $1`,
			id.OrgID).Scan(&lastID); err != nil {
			http.Error(w, "head lookup failed", http.StatusInternalServerError)
			return
		}
	}
	ws, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	c := &client{conn: ws, id: id, lastID: lastID,
		wake: make(chan struct{}, 1), frameLimit: newFrameLimiter()}

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	// Setup order matters (fixes a startup race): load the membership view and
	// register the connection BEFORE reading any client frame, so the sender's
	// own ACL view is populated and the connection is discoverable as a
	// fan-out target. Only then start the reader and announce `ready`.
	if err := c.loadChannels(ctx, h.pool); err != nil {
		ws.Close(websocket.StatusInternalError, "membership load failed")
		return
	}
	announce := h.register(c, time.Now())
	if announce {
		h.broadcastPresence(ctx, id.OrgID, id.UserID, "active")
	}
	defer func() {
		wentOffline := h.deregister(c)
		if wentOffline {
			// The request context is gone; presence still fans out.
			h.broadcastPresence(context.Background(), id.OrgID, id.UserID, "offline")
		}
		ws.CloseNow()
	}()

	// The reader goroutine serves two jobs: prompt disconnect detection, and
	// the ephemeral signal plane (typing, read_marker — ADR-002 P5). Durable
	// writes remain REST (P3).
	go func() {
		for {
			_, data, err := ws.Read(ctx)
			if err != nil {
				cancel()
				return
			}
			if len(data) > 0 {
				h.handleClientFrame(ctx, c, data)
			}
		}
	}()

	// `ready` (ADR-002 P4 hello): the connection is registered and its ACL
	// view is loaded — clients wait for this before sending signal frames.
	if err := h.send(ctx, c, Envelope{Type: "ready", OrgID: id.OrgID}); err != nil {
		return
	}

	// Immediate pump serves the resume gap before any live traffic.
	c.wake <- struct{}{}

	checkpoint := time.NewTicker(checkpointInterval)
	defer checkpoint.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.wake:
			if err := h.pump(ctx, c); err != nil {
				return
			}
		case <-checkpoint.C:
			// F-2: "no UNDETECTABLE loss" — clients compare applied ids
			// against checkpoints and resync when behind.
			if err := h.send(ctx, c, Envelope{
				Seq: c.lastID, Type: "checkpoint", OrgID: c.id.OrgID}); err != nil {
				return
			}
		}
	}
}

// pump drains rows after the client's cursor, ACL-filtered, until caught up.
func (h *Hub) pump(ctx context.Context, c *client) error {
	for {
		// One catch-up query PER connection PER wake: this counter is THE S3
		// blowup signal — it scales with connection count until per-org
		// multicast replaces it (docs/PERF.md), and S0 makes that measurable.
		h.pumpQueries.Add(1)
		rows, err := h.pool.Query(ctx, `
			SELECT e.id, e.verb, e.payload
			FROM event_log e
			WHERE e.org_id = $1 AND e.id > $2
			  AND e.txid < pg_snapshot_xmin(pg_current_snapshot())
			ORDER BY e.id
			LIMIT $3`,
			c.id.OrgID, c.lastID, batchLimit)
		if err != nil {
			return err
		}
		type row struct {
			id      int64
			verb    string
			payload json.RawMessage
		}
		var batch []row
		for rows.Next() {
			var r row
			if err := rows.Scan(&r.id, &r.verb, &r.payload); err != nil {
				rows.Close()
				return err
			}
			batch = append(batch, r)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		if len(batch) == 0 {
			return nil
		}
		for _, r := range batch {
			deliver, refresh := c.filter(r.verb, r.payload)
			if refresh {
				if err := c.loadChannels(ctx, h.pool); err != nil {
					return err
				}
			}
			if deliver {
				if err := h.send(ctx, c, Envelope{
					Seq: r.id, Type: r.verb, OrgID: c.id.OrgID, Payload: r.payload,
				}); err != nil {
					return err
				}
				h.deliveries.Add(1)
			}
			// The cursor advances past filtered events too — gaps in seq are
			// expected by the protocol (F-2).
			c.lastID = r.id
		}
		if len(batch) < batchLimit {
			return nil
		}
	}
}

// filter applies the read ACL: channel-scoped events require membership,
// DM-scoped events require participation, container-less events (spaces)
// deliver org-wide. Events about this user's own membership trigger a view
// refresh. dm.opened and dm.participants_changed decide from their user_ids
// list rather than current participation: dm.opened reaches the invited side
// whose view predates the new conversation, and dm.participants_changed
// reaches the leaver whose view must now drop it (their dm_participant row is
// already gone, so a participation check would wrongly withhold it).
func (c *client) filter(verb string, payload json.RawMessage) (deliver, refresh bool) {
	var p struct {
		ChannelID int64   `json:"channel_id"`
		DMSpaceID int64   `json:"dm_space_id"`
		UserID    int64   `json:"user_id"`
		UserIDs   []int64 `json:"user_ids"`
	}
	_ = json.Unmarshal(payload, &p)
	if (verb == "member.joined" || verb == "member.left") && p.UserID == c.id.UserID {
		refresh = true
	}
	if verb == "dm.opened" || verb == "dm.participants_changed" {
		for _, uid := range p.UserIDs {
			if uid == c.id.UserID {
				return true, true
			}
		}
		return false, refresh
	}
	if p.ChannelID != 0 {
		return c.channels[p.ChannelID], refresh
	}
	if p.DMSpaceID != 0 {
		return c.dms[p.DMSpaceID], refresh
	}
	return true, refresh
}

func (c *client) loadChannels(ctx context.Context, pool *pgxpool.Pool) error {
	rows, err := pool.Query(ctx, `
		SELECT channel_id FROM channel_member
		WHERE user_id = $1 AND unsubscribed_at IS NULL`, c.id.UserID)
	if err != nil {
		return err
	}
	defer rows.Close()
	set := map[int64]bool{}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return err
		}
		set[id] = true
	}
	c.channels = set
	if err := rows.Err(); err != nil {
		return err
	}
	drows, err := pool.Query(ctx,
		`SELECT dm_space_id FROM dm_participant WHERE user_id = $1`, c.id.UserID)
	if err != nil {
		return err
	}
	defer drows.Close()
	dset := map[int64]bool{}
	for drows.Next() {
		var id int64
		if err := drows.Scan(&id); err != nil {
			return err
		}
		dset[id] = true
	}
	c.dms = dset
	return drows.Err()
}

// send serializes writes per connection: coder/websocket forbids concurrent
// Write on one conn, and a client is written to from several goroutines (its
// pump, its reader's signal replies, and other connections' ephemeral
// fan-out). The write uses a fresh timeout (not the caller's context) so
// cross-connection fan-out is never cancelled by the sender disconnecting.
func (h *Hub) send(_ context.Context, c *client, e Envelope) error {
	data, err := json.Marshal(e)
	if err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	wctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return c.conn.Write(wctx, websocket.MessageText, data)
}
