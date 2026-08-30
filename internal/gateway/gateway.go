// Package gateway is the server→client event stream (ADR-002, F-2 contract):
// seq IS the per-org event-log id; sequences are gappy by construction (ACL
// filtering), and the guarantee is "no undetectable loss" via checkpoint
// heartbeats. Resume = reconnect with last_id; the gap replays from the log.
//
// Scale shape (docs/SCHEMA.md, docs/PERF.md): ONE dispatcher goroutine LISTENs
// for event notifications and wakes only the orgs that have connections — idle
// orgs cost zero. Within an org, ONE per-org multicast reader (S3) runs a
// SINGLE txid-gated event-log read per event-batch and fans the shared rows to
// that org's live connections in memory, each applying its own O(1) ACL filter
// — so per-message DB cost is O(1) per org, independent of connection count.
// A connection that reconnects behind the org head runs its OWN bounded
// catch-up (the per-connection pump) only until it reaches the head, then joins
// the live lane; the per-connection query survives for the resume gap alone.
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
	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/internal/platform/metrics"
	"github.com/abhinavjha0239/weft/internal/platform/presence"
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
	cancel context.CancelFunc
	lastID int64
	// live is the lane flag (guarded by shard.mu): a live connection is caught
	// up to the org head and receives the shared multicast batch via feed; a
	// non-live connection is behind the head and runs its own resume pump until
	// it catches up. lastID/channels/dms are otherwise owned by this
	// connection's own goroutine (the pump, deliverShared, and checkpoint).
	live bool
	// feed carries the per-org reader's shared batch to this connection's live
	// lane. Bounded: a connection that cannot keep up fills the buffer and is
	// dropped (backpressure) rather than stalling the org (F-2 resync on reconnect).
	feed chan []eventRow
	wake chan struct{}
	// Membership views for ACL filtering (channels + DM participation);
	// refreshed when a membership event for this user arrives (F-2 replay
	// rule, M0 slice).
	channels map[int64]bool
	dms      map[int64]bool
	// historyFloor is the ADR-008 C-2 protected-history boundary per channel:
	// channel_member.history_from for the channels that stamped one. Absent
	// key = full history (a shared channel, or the protected channel's
	// creator). Loaded and refreshed with `channels` by loadChannels, so the
	// same membership events that widen the view also carry the floor.
	historyFloor map[int64]time.Time
	// spaces is the connection's space-visibility set — the third container
	// view, loaded and refreshed exactly like channels/dms. Space-scoped
	// events (space, work item, sprint, field def) have no channel or DM to
	// gate on and used to fan ORG-WIDE; they now resolve their space against
	// this set. Empty for a guest (P-5: a guest sees nothing beyond their own
	// channels), which is what lets guests ride the SAME predicate with no
	// role branch in filter.
	spaces map[int64]bool
	// itemSecurityActive reports whether this org defines ANY visibility_scope
	// (P-4 item security). work_item.security_scope_id can only reference such
	// a row, so while the org defines none, NO item is security-scoped and the
	// space set alone is exact. The moment one exists the gateway cannot tell
	// which items it covers without a per-event query — and it has no
	// evaluator for visibility_scope.rule at all — so every work-item event is
	// withheld: an unresolvable scope must never fall through to org-wide
	// delivery. Defaults to true (withhold) so a partial load fails closed.
	itemSecurityActive bool
	// Inbound signal-frame budget (typing storms, abuse).
	frameLimit *rate.Limiter
	// Serializes all writes to conn (coder/websocket forbids concurrent Write).
	writeMu sync.Mutex
}

// feedBuffer bounds how many multicast batches may queue for one connection
// before the per-org reader gives up on it (drops it to resync). Small: live
// connections drain immediately; a stalled one is shed fast, not indulged.
const feedBuffer = 16

// orgShard is one org's slice of the hub — its live connections and its
// per-user derived-presence registry — under a single per-org lock. The hub's
// top-level mu only guards the orgs map; all per-org work takes the shard lock,
// so a 100k-connection org's traffic never contends the whole hub. Connections
// and presence drain together: the last connection to leave empties both maps.
type orgShard struct {
	orgID int64
	mu    sync.Mutex
	conns map[*client]struct{}
	// head is the org's live event cursor: the highest event-log id the
	// multicast reader has read for the live lane (guarded by mu). A resuming
	// connection joins the live lane only once its own cursor reaches head, and
	// the reader advances head under the same lock — so the resume→live hand-off
	// is a single consistent point and no event slips through it.
	head int64
	// wake signals the per-org multicast reader; buffered 1 so many event
	// notifications for one org coalesce into a single catch-up pass.
	wake chan struct{}
	// stop ends the reader goroutine when the shard is dropped (last connection
	// leaves) or the hub shuts down.
	stop context.CancelFunc
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

	// plane is the cross-node shared presence plane (the platform/presence
	// seam). Default = the in-process Local plane (single-node semantics); a
	// multi-node cell wires the shared pg plane via SetPresencePlane. Presence
	// transitions publish here, every node subscribes and fans deltas to its
	// OWN local connections, and PresenceSnapshot reads the org-wide view.
	plane presence.Plane

	// IdleAfter is how long a connected user may be silent before presence
	// demotes active→idle (P-05). Exported so tests shrink it; default 10min.
	IdleAfter time.Duration

	// mu guards the orgs map only (shard lookup/create/delete). Each org's
	// connection set and derived-presence registry live under that shard's OWN
	// lock, so a high-fan-out org never serializes the whole hub — the S3 lock
	// split (docs/PERF.md). S5's multi-node presence reuses these shards.
	mu   sync.Mutex
	orgs map[int64]*orgShard

	// runCtx is the hub's lifetime, set by Run under mu; per-org reader
	// goroutines derive their context from it so they all stop on shutdown.
	// Default context.Background() (a hub without Run has no gateway
	// connections, hence no readers).
	runCtx context.Context

	// Metrics (S0), optional (default Nop). pumpQueries counts event-log
	// catch-up reads: after S3 the per-org multicast reader runs ONE per
	// event-batch (O(1) per org), plus one per resume-lane pump (rare, bounded
	// by the replay gap) — so it no longer scales with connection count. Set
	// once at wiring (docs/PERF.md).
	pumpQueries metrics.Counter
	deliveries  metrics.Counter
	connections metrics.Gauge
	// encoded counts Envelope JSON marshals — the marshal-once invariant: the
	// live multicast lane encodes each event ONCE for the whole org, so this
	// rises O(events), not O(connections). A regression to per-connection
	// marshaling makes it rise ~O(connections) (the red/green pin).
	encoded metrics.Counter
}

func NewHub(pool *pgxpool.Pool, log *slog.Logger) *Hub {
	h := &Hub{pool: pool, log: log, IdleAfter: 10 * time.Minute,
		orgs: map[int64]*orgShard{}, runCtx: context.Background(),
		plane: presence.Local()}
	h.SetMetrics(metrics.Nop())
	return h
}

// SetPresencePlane wires the cross-node presence plane (the platform/presence
// seam). Optional — the default is the in-process Local plane, i.e. single-node
// presence (an un-wired hub and every single-node test keep their semantics).
// A multi-node cell wires the shared pg plane so any node sees org-wide
// presence. Call once before Run.
func (h *Hub) SetPresencePlane(p presence.Plane) { h.plane = p }

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

// register adds c to its org's shard (creating the shard, and its per-org
// multicast reader, on first use) and records the connection for presence.
// A fresh shard's head seeds from orgHead — the org's event cursor at connect
// time — so the reader only ever reads NEW events; c joins the LIVE lane when
// its own cursor already reaches head, else the resume lane (its pump replays
// the gap). Reports whether an "active" presence broadcast is owed and whether
// c starts live. Registration nests h.mu→sh.mu so a shard is never deleted by
// a concurrent deregister between lookup and insert (which would strand the
// connection in an orphaned shard, invisible to fan-out).
func (h *Hub) register(c *client, orgHead int64, now time.Time) (announce, live bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	sh := h.orgs[c.id.OrgID]
	if sh == nil {
		sh = h.newShard(c.id.OrgID, orgHead)
		h.orgs[c.id.OrgID] = sh
	}
	c.shard = sh
	sh.mu.Lock()
	defer sh.mu.Unlock()
	sh.conns[c] = struct{}{}
	c.live = c.lastID >= sh.head
	h.connections.Set(float64(len(sh.conns)), strconv.FormatInt(c.id.OrgID, 10))
	announce = sh.trackConnect(c.id.UserID, now)
	return announce, c.live
}

// newShard builds an org's shard and starts its multicast reader. Caller holds
// h.mu. The reader's context derives from the hub lifetime and a per-shard
// cancel, so it stops on hub shutdown OR the moment the org empties.
func (h *Hub) newShard(orgID, head int64) *orgShard {
	ctx, cancel := context.WithCancel(h.runCtx)
	sh := &orgShard{
		orgID:     orgID,
		conns:     map[*client]struct{}{},
		userConns: map[int64]*userPresence{},
		head:      head,
		wake:      make(chan struct{}, 1),
		stop:      cancel,
	}
	go h.runReader(ctx, sh)
	return sh
}

// deregister removes c from its shard, dropping the shard (and stopping its
// reader) once its last connection leaves, and reports whether the user went
// offline. Nested h.mu→sh.mu matches register so the empty-shard delete is
// atomic.
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
		sh.stop() // end the per-org multicast reader
	}
	return wentOffline
}

// SetMetrics wires an observability registry (S0). Optional — the default is
// Nop, so an un-instrumented hub pays nothing. Call once before Run/Serve.
func (h *Hub) SetMetrics(reg metrics.Registry) {
	h.pumpQueries = reg.Counter("gateway_pump_queries_total")
	h.deliveries = reg.Counter("fanout_deliveries_total")
	h.connections = reg.Gauge("gateway_connections", "org")
	h.encoded = reg.Counter("gateway_envelopes_encoded_total")
}

// encodeEnvelope is the SINGLE Envelope-marshal choke point, so the encoded
// counter measures exactly how many times an event was JSON-encoded. The live
// multicast lane calls this once per event for the whole org (marshal-once);
// the resume-lane fallback calls it once per delivered row. Default Nop
// registry makes the count a no-op when metrics are off.
func (h *Hub) encodeEnvelope(e Envelope) ([]byte, error) {
	h.encoded.Add(1)
	return json.Marshal(e)
}

// SetMarkReader wires the durable read-state service (rest layer adapts
// auth.Identity ↔ Actor).
func (h *Hub) SetMarkReader(m MarkReader) { h.markReader = m }

// Run is the dispatcher: LISTEN on the event-log channel and wake that org's
// multicast reader; a slow sweep covers missed notifications. Blocks until ctx
// ends. ctx is also the parent of every per-org reader, so they stop on shutdown.
func (h *Hub) Run(ctx context.Context) {
	h.mu.Lock()
	h.runCtx = ctx
	h.mu.Unlock()
	go h.sweep(ctx)
	go h.presenceSweep(ctx)
	// Subscribe to the shared presence plane: every delta published on the cell
	// (by this node or any other) is fanned to this node's own local connections.
	go h.plane.Subscribe(ctx, h.onPresenceDelta)
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
				sh.wakeReader()
			}
		}
	}
}

func (h *Hub) wakeOrg(orgID int64) {
	if sh := h.shard(orgID); sh != nil {
		sh.wakeReader()
	}
}

// wakeReader nudges the per-org multicast reader to run a catch-up pass. The
// buffered-1 wake coalesces a burst of notifications into a single read.
func (sh *orgShard) wakeReader() {
	select {
	case sh.wake <- struct{}{}:
	default: // a pass is already pending
	}
}

// Serve upgrades the request and streams events. ?last_id=N resumes.
func (h *Hub) Serve(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	lastID, _ := strconv.ParseInt(r.URL.Query().Get("last_id"), 10, 64)
	// The org's current event cursor: it seeds a NEW shard's live head (so the
	// per-org reader only ever reads fresh events) AND resolves tail mode
	// (last_id<0 → start at head, no history replay). One query serves both — a
	// fresh client renders state from REST, then follows the live stream.
	var orgHead int64
	if err := h.pool.QueryRow(r.Context(),
		`SELECT COALESCE(MAX(id), 0) FROM event_log WHERE org_id = $1`,
		id.OrgID).Scan(&orgHead); err != nil {
		http.Error(w, "head lookup failed", http.StatusInternalServerError)
		return
	}
	if lastID < 0 {
		lastID = orgHead
	}
	ws, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	c := &client{conn: ws, id: id, cancel: cancel, lastID: lastID,
		feed: make(chan []eventRow, feedBuffer),
		wake: make(chan struct{}, 1), frameLimit: newFrameLimiter(),
		// Seeded WITHHOLDING: nil container views deny by lookup, and item
		// security starts active, so a connection whose ACL load never ran
		// (or failed) can only ever under-deliver. loadChannels below is the
		// only thing that relaxes any of it.
		itemSecurityActive: true}

	// Setup order matters (fixes a startup race): load the membership view and
	// register the connection BEFORE reading any client frame, so the sender's
	// own ACL view is populated and the connection is discoverable as a
	// fan-out target. Only then start the reader and announce `ready`.
	if err := c.loadChannels(ctx, h.pool); err != nil {
		ws.Close(websocket.StatusInternalError, "membership load failed")
		return
	}
	announce, live := h.register(c, orgHead, time.Now())
	if announce {
		h.publishPresence(ctx, id.OrgID, id.UserID, "active")
	}
	defer func() {
		wentOffline := h.deregister(c)
		if wentOffline {
			// The request context is gone; presence still publishes to the plane.
			h.publishPresence(context.Background(), id.OrgID, id.UserID, "offline")
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

	// Catch up anything committed since the head query: a resume-lane
	// connection pumps its own gap; a live one nudges the per-org reader.
	if live {
		c.shard.wakeReader()
	} else {
		c.wake <- struct{}{}
	}

	checkpoint := time.NewTicker(checkpointInterval)
	defer checkpoint.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case batch := <-c.feed:
			// Live lane: the per-org reader handed us the SHARED rows; we apply
			// our own ACL filter and deliver — no per-connection query (S3).
			if err := h.deliverShared(ctx, c, batch); err != nil {
				return
			}
		case <-c.wake:
			// Resume lane: drain our own catch-up gap, then join the live lane.
			if err := h.resume(ctx, c); err != nil {
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

// runReader is the per-org multicast dispatcher (S3): on each wake it runs ONE
// event-log read for the whole org and fans the shared rows to that org's live
// connections in memory — instead of every connection querying independently.
// This is the connections-axis O(1) invariant: per-message DB cost is one read
// per org per event-batch, independent of connection count (docs/PERF.md).
func (h *Hub) runReader(ctx context.Context, sh *orgShard) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-sh.wake:
			for {
				more, err := h.multicast(ctx, sh)
				if err != nil {
					if ctx.Err() == nil {
						h.log.Warn("gateway: multicast read failed",
							"org", sh.orgID, "err", err)
					}
					break
				}
				if !more {
					break
				}
			}
		}
	}
}

// multicast runs the single hoisted catch-up read for the org from its live
// head, then fans the shared batch to every LIVE connection's lane. It
// snapshots the live set and advances head under sh.mu, so a resuming
// connection's go-live check sees one consistent hand-off point (risk #2); the
// fan itself is OUTSIDE the lock, so a slow connection never stalls the org
// (risk #1) — it overruns its bounded lane and is dropped (risk #3). Returns
// whether the batch was full, so the reader drains a backlog to completion.
func (h *Hub) multicast(ctx context.Context, sh *orgShard) (more bool, err error) {
	sh.mu.Lock()
	head := sh.head
	sh.mu.Unlock()

	// THE S3 read: one hoisted org-scope catch-up query per event-batch. This
	// counter used to rise once PER connection PER wake (the blowup); now it
	// rises once per org per batch (plus rare resume pumps) — docs/PERF.md.
	h.pumpQueries.Add(1)
	batch, err := h.readEvents(ctx, sh.orgID, head, batchLimit)
	if err != nil {
		return false, err
	}
	if len(batch) == 0 {
		return false, nil
	}
	maxID := batch[len(batch)-1].id

	// Marshal each row's Envelope ONCE here, in the single reader goroutine,
	// before fanning: the bytes are identical for every connection in the org,
	// so the per-connection deliver reuses them (marshal-once — the CPU twin of
	// the O(1) read above). The channel send in feed establishes happens-before,
	// so the connection goroutines read enc safely.
	for i := range batch {
		batch[i].enc, _ = h.encodeEnvelope(Envelope{
			Seq: batch[i].id, Type: batch[i].verb,
			OrgID: sh.orgID, Payload: batch[i].payload,
		})
	}

	sh.mu.Lock()
	live := make([]*client, 0, len(sh.conns))
	for c := range sh.conns {
		if c.live {
			live = append(live, c)
		}
	}
	sh.head = maxID
	sh.mu.Unlock()

	for _, c := range live {
		h.feed(c, batch)
	}
	return len(batch) == batchLimit, nil
}

// feed hands the shared batch to one live connection's lane without blocking
// the org. A full buffer means the connection has fallen too far behind, so it
// is dropped: its own goroutine sees the cancel, cleans up, and the client
// resyncs on reconnect (F-2 tolerates detectable loss) — one slow connection
// never stalls the rest of the org.
func (h *Hub) feed(c *client, batch []eventRow) {
	select {
	case c.feed <- batch:
	default:
		c.cancel()
	}
}

// eventRow is one event-log row as the gateway fans it: id (the wire seq), verb
// (the event type), and the opaque payload. The per-org reader shares ONE
// []eventRow across all of an org's live connections — read-only, so the fan is
// a slice-header pass, not a per-connection re-query.
type eventRow struct {
	id      int64
	verb    string
	payload json.RawMessage
	// occurredAt is the event's DOMAIN time (event_log.occurred_at) — the same
	// clock message.created_at runs on, and the one importers backdate (E3).
	// The read ACL's protected-history floor compares against it: the floor is
	// a TIMESTAMPTZ (channel_member.history_from) while the stream is ordered
	// by id, so there is no id-space equivalent to compare with.
	occurredAt time.Time
	// entityType is event_log.entity_type — the event's own NOT NULL statement
	// of what it is ABOUT. The read ACL classifies with it rather than by
	// sniffing payload keys: a work-item event whose payload happens to carry
	// no space_id is still a work-item event and must fail closed, where key
	// sniffing would silently fall through to org-wide delivery.
	entityType enum.EntityType
	// enc is the row's Envelope pre-marshaled ONCE by the multicast reader
	// (the Envelope is identical for every connection in the org — same seq,
	// verb, payload, org id), so the fan reuses these bytes instead of
	// re-encoding per connection: O(1) marshal per event, not O(connections).
	// nil on the resume lane (per-connection pump), where deliverShared
	// marshals its own fallback — rare, bounded by the replay gap.
	enc []byte
}

// readEvents reads an org's committed event-log rows after afterID (ascending,
// up to limit), applying the SAME txid<xmin visibility gate the durable
// consumers use (F-1) so an in-flight transaction's rows never surface early.
func (h *Hub) readEvents(ctx context.Context, orgID, afterID int64, limit int) ([]eventRow, error) {
	rows, err := h.pool.Query(ctx, `
		SELECT e.id, e.verb, e.payload, e.occurred_at, e.entity_type
		FROM event_log e
		WHERE e.org_id = $1 AND e.id > $2
		  AND e.txid < pg_snapshot_xmin(pg_current_snapshot())
		ORDER BY e.id
		LIMIT $3`,
		orgID, afterID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var batch []eventRow
	for rows.Next() {
		var r eventRow
		if err := rows.Scan(&r.id, &r.verb, &r.payload, &r.occurredAt,
			&r.entityType); err != nil {
			return nil, err
		}
		batch = append(batch, r)
	}
	return batch, rows.Err()
}

// pump is the RESUME lane: a connection behind the org head runs its OWN
// bounded catch-up read, draining its gap batch by batch. It survives only for
// the reconnect gap (F-2's replay window); steady-state live traffic flows
// through the per-org multicast reader, never here.
func (h *Hub) pump(ctx context.Context, c *client) error {
	for {
		// One catch-up query for THIS connection's resume gap. The reader's
		// hoisted org-scope read is the steady-state path; both increment
		// gateway_pump_queries_total, and the S3 proof tells them apart by
		// count — O(1) per org vs the old O(connections) (docs/PERF.md).
		h.pumpQueries.Add(1)
		batch, err := h.readEvents(ctx, c.id.OrgID, c.lastID, batchLimit)
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			return nil
		}
		if err := h.deliverShared(ctx, c, batch); err != nil {
			return err
		}
		if len(batch) < batchLimit {
			return nil
		}
	}
}

// deliverShared applies one connection's ACL filter to a batch of rows — the
// resume pump's own batch OR the per-org reader's SHARED batch — writing the
// events it may see and advancing its cursor past every row (gaps in seq are
// expected, F-2). Rows at or below the cursor are skipped, so a live connection
// that tailed past the org head silently ignores anything it already holds.
func (h *Hub) deliverShared(ctx context.Context, c *client, batch []eventRow) error {
	for _, r := range batch {
		if r.id <= c.lastID {
			continue
		}
		deliver, refresh := c.filter(r)
		if refresh {
			if err := c.loadChannels(ctx, h.pool); err != nil {
				return err
			}
		}
		if deliver {
			// Live lane: reuse the reader's marshal-once bytes. Resume lane
			// (enc nil) marshals its own — rare, bounded by the replay gap.
			var err error
			if r.enc != nil {
				err = h.sendRaw(c, r.enc)
			} else {
				err = h.send(ctx, c, Envelope{
					Seq: r.id, Type: r.verb, OrgID: c.id.OrgID, Payload: r.payload,
				})
			}
			if err != nil {
				return err
			}
			h.deliveries.Add(1)
		}
		// The cursor advances past filtered events too — gaps in seq are
		// expected by the protocol (F-2).
		c.lastID = r.id
	}
	return nil
}

// resume drains this connection's catch-up gap (the per-connection pump) and,
// once its cursor reaches the org head under the shard lock, promotes it to the
// live lane — from then on the per-org reader feeds it and it stops querying.
// The go-live check and the reader's head advance share sh.mu, so the hand-off
// never drops or duplicates an event (risk #2). If the head moved past our
// drain, we re-arm the pump instead of joining, so the gap is never skipped.
func (h *Hub) resume(ctx context.Context, c *client) error {
	sh := c.shard
	sh.mu.Lock()
	already := c.live
	sh.mu.Unlock()
	if already {
		return nil
	}
	if err := h.pump(ctx, c); err != nil {
		return err
	}
	sh.mu.Lock()
	if c.lastID >= sh.head {
		c.live = true
	} else {
		select {
		case c.wake <- struct{}{}: // head advanced mid-drain; pump again
		default:
		}
	}
	sh.mu.Unlock()
	return nil
}

// filter applies the read ACL: channel-scoped events require membership AND
// clear the channel's protected-history floor, DM-scoped events require
// participation, SPACE-scoped events require space visibility, and only what
// is genuinely org-wide fans org-wide. Events about this user's own membership
// trigger a view refresh. dm.opened and dm.participants_changed decide from
// their user_ids list rather than current participation: dm.opened reaches the
// invited side whose view predates the new conversation, and
// dm.participants_changed reaches the leaver whose view must now drop it
// (their dm_participant row is already gone, so a participation check would
// wrongly withhold it).
//
// The floor (ADR-008 C-2, F-16b) is the half REST already enforced and the
// stream did not: a member of a PROTECTED channel resuming with a last_id from
// before their join used to replay events that predate their access. The
// comparison is deliberately TIME-based — history_from is a TIMESTAMPTZ and
// the stream is ordered by id, so there is no id-space floor to compare with —
// and its boundary is INCLUSIVE of history_from: an event whose occurred_at
// EQUALS the stamp is delivered. That is exactly messaging.ListMessages'
// `created_at >= history_from` and the negation of messaging.Get's
// `created_at < history_from` hide-rule, so the realtime plane and the REST
// read answer identically at the boundary instant.
func (c *client) filter(r eventRow) (deliver, refresh bool) {
	var p struct {
		ChannelID int64   `json:"channel_id"`
		DMSpaceID int64   `json:"dm_space_id"`
		SpaceID   int64   `json:"space_id"`
		UserID    int64   `json:"user_id"`
		UserIDs   []int64 `json:"user_ids"`
	}
	_ = json.Unmarshal(r.payload, &p)
	if (r.verb == "member.joined" || r.verb == "member.left") && p.UserID == c.id.UserID {
		refresh = true
	}
	// A new Space widens (or, for a guest, does not widen) every connection's
	// space set, so it drives the same event-driven refresh membership does.
	// The event itself is decided against the PRE-refresh set and so is
	// withheld — the member.joined precedent: the view catches up, the
	// envelope that caused it does not arrive, and the client's next REST read
	// closes the gap (F-2 tolerates a detectable gap, not a leak).
	if r.verb == "space.created" {
		refresh = true
	}
	if r.verb == "dm.opened" || r.verb == "dm.participants_changed" {
		for _, uid := range p.UserIDs {
			if uid == c.id.UserID {
				return true, true
			}
		}
		return false, refresh
	}
	if p.ChannelID != 0 {
		if !c.channels[p.ChannelID] {
			return false, refresh
		}
		// Protected-history floor. Absent key = no boundary (shared channel or
		// the protected channel's creator) — the common case, one map miss.
		if floor, bounded := c.historyFloor[p.ChannelID]; bounded &&
			r.occurredAt.Before(floor) {
			return false, refresh
		}
		return true, refresh
	}
	if p.DMSpaceID != 0 {
		return c.dms[p.DMSpaceID], refresh
	}
	if spaceScoped(r.entityType) {
		// Container-less but NOT org-wide: these events belong to a Space, so
		// they resolve one instead of fanning. Everything here fails CLOSED —
		// an unresolvable scope is withheld, never dropped through to the
		// org-wide return below, which is exactly the hole this closes.
		//   · space_id missing from the payload  → withhold (unresolvable)
		//   · space not in this connection's set → withhold (includes every
		//     guest, whose set is empty — the same predicate, no role branch)
		//   · a work item while the org defines any visibility_scope →
		//     withhold (no evaluator exists for the scope's rule)
		if p.SpaceID == 0 || !c.spaces[p.SpaceID] {
			return false, refresh
		}
		if r.entityType == enum.EntityWorkItem && c.itemSecurityActive {
			return false, refresh
		}
		return true, refresh
	}
	// Genuinely org-wide: org settings, emoji, user directory, and the
	// space-governed THREAD traffic (message.*/thread.*) that the v1
	// visibility slice makes org-visible over REST too — withholding it here
	// would over-withhold against messaging.Get, not close a hole.
	return true, refresh
}

// spaceScoped reports whether an event's entity belongs to a Space, i.e. has
// no channel or DM container but is NOT org-wide. These four are the only
// entity types worktrack appends under, and only work_item carries a
// security_scope_id.
func spaceScoped(t enum.EntityType) bool {
	switch t {
	case enum.EntitySpace, enum.EntityWorkItem, enum.EntitySprint, enum.EntityFieldDef:
		return true
	}
	return false
}

func (c *client) loadChannels(ctx context.Context, pool *pgxpool.Pool) error {
	// The membership set and its protected-history floors load together, so a
	// refresh can never widen the view without carrying the boundary that
	// bounds it. history_from is stamped ONLY on a join to a history_mode=2
	// channel and is never cleared (ADR-008 C-4 keeps the row across
	// unsubscribe precisely so the boundary survives leave→rejoin), so a
	// non-NULL stamp IS the protected boundary and is honoured unconditionally:
	// the failure direction is a withheld event the client refetches over REST,
	// never a delivered one REST would hide. The channel join adds the org pin
	// the bare channel_member read lacked (cell isolation, defence in depth —
	// a user's memberships are all in their own org today).
	rows, err := pool.Query(ctx, `
		SELECT cm.channel_id, cm.history_from
		FROM channel_member cm
		JOIN channel c ON c.id = cm.channel_id AND c.org_id = $2
		WHERE cm.user_id = $1 AND cm.unsubscribed_at IS NULL`,
		c.id.UserID, c.id.OrgID)
	if err != nil {
		return err
	}
	defer rows.Close()
	set := map[int64]bool{}
	floors := map[int64]time.Time{}
	for rows.Next() {
		var id int64
		var from *time.Time
		if err := rows.Scan(&id, &from); err != nil {
			return err
		}
		set[id] = true
		if from != nil {
			floors[id] = *from
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	c.channels, c.historyFloor = set, floors
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
	if err := drows.Err(); err != nil {
		return err
	}
	return c.loadSpaces(ctx, pool)
}

// loadSpaces builds the space-visibility set — the container view for events
// that have neither a channel nor a DM. Spaces are org-visible in the v1
// worktrack slice (see its package doc), and a GUEST sees none (P-5, the same
// `NOT $2` shape messaging.ListChannels uses), so the guest restriction lives
// in the SQL and filter keeps ONE predicate for everyone. Archival is
// deliberately not filtered: it is a lifecycle state, not an ACL state, and
// REST still reads an archived space's items — withholding here would
// over-withhold against the read path rather than close a hole.
//
// The uncorrelated EXISTS is Postgres's InitPlan (evaluated once, not per row)
// and rides the same round trip as the set.
func (c *client) loadSpaces(ctx context.Context, pool *pgxpool.Pool) error {
	rows, err := pool.Query(ctx, `
		SELECT sp.id,
		       EXISTS (SELECT 1 FROM visibility_scope vs
		               JOIN space s2 ON s2.id = vs.space_id
		               WHERE s2.org_id = $1)
		FROM space sp
		WHERE sp.org_id = $1 AND NOT $2`, c.id.OrgID, c.id.IsGuest())
	if err != nil {
		return err
	}
	defer rows.Close()
	// Fail closed on the way in: until a row proves otherwise the connection
	// behaves as if item security were active. Zero rows leaves it true, which
	// is moot — an empty space set already withholds every space-scoped event.
	set, scoped := map[int64]bool{}, true
	for rows.Next() {
		var id int64
		var active bool
		if err := rows.Scan(&id, &active); err != nil {
			return err
		}
		set[id] = true
		scoped = active
	}
	if err := rows.Err(); err != nil {
		return err
	}
	c.spaces, c.itemSecurityActive = set, scoped
	return nil
}

// send serializes writes per connection: coder/websocket forbids concurrent
// Write on one conn, and a client is written to from several goroutines (its
// pump, its reader's signal replies, and other connections' ephemeral
// fan-out). The write uses a fresh timeout (not the caller's context) so
// cross-connection fan-out is never cancelled by the sender disconnecting.
func (h *Hub) send(_ context.Context, c *client, e Envelope) error {
	data, err := h.encodeEnvelope(e)
	if err != nil {
		return err
	}
	return h.sendRaw(c, data)
}

// sendRaw writes already-marshaled Envelope bytes to one connection. The live
// multicast lane passes the reader's marshal-once bytes here so an event is
// encoded ONCE per org, not once per connection (the CPU side of S3).
func (h *Hub) sendRaw(c *client, data []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	wctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return c.conn.Write(wctx, websocket.MessageText, data)
}
