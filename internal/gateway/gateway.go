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
// A connection that reconnects behind the org head runs a bounded catch-up (the
// resume pump) only until it reaches the head, then joins the live lane. That
// read is shared too: connections that reconnect together resume from cursors
// the F-2 checkpoint has already converged, so a RECONNECT COHORT — a node
// restart's whole population — issues about one read between them rather than
// one each (catchup.go), while a genuinely far-behind resumer reads alone.
//
// The ACL STATE those filters read is placed by the same rule. What is
// per-USER — channel membership, DM participation, the protected-history floor
// — is held per connection and refreshed by events about that user. What is
// per-ORG — the space-visibility set and the item-security flag — is held ONCE
// on the org shard (see spaceView) and shared by that org's connections, so a
// new Space costs the org ONE query rather than one per connection, and
// resident state is O(spaces) rather than O(connections x spaces).
//
// Both lanes read through the event-feed seam (eventlog.Tail, P-45), so the
// operator's driver choice reaches live fan-out too. Under the commit-ordered
// logical driver an event with a LOWER id can legitimately arrive AFTER a
// higher one — that is the point of that feed — so `id <= lastID` stops
// meaning "already delivered" and a connection tracks what it has ACTUALLY
// sent. The protocol cost is stated in ADR-002 and repeated here because it is
// a WIRE contract: seq is still the event id and still replayable with
// `WHERE id > seq`, sequences are still gappy (ACL filtering), and clients must
// now also tolerate a rare DUPLICATE seq, bounded to the live/resume hand-off.
// Duplicates are absorbable by any client; the alternative was silent LOSS.
package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/time/rate"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/internal/eventlog"
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
	// recent is the bounded window of event ids this connection has ACTUALLY
	// handled — the only sound "already delivered" test under a COMMIT-ordered
	// feed, where the cursor cannot serve as one (see client.passed). nil
	// under an id-monotone feed (the default xmin driver), which needs no
	// window at all, and nil until the first row is passed, so the 100k-live-
	// connection case pays for it only on the connections that resume.
	recent *recentIDs
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
	// spaces is this connection's handle on the ORG's shared space-visibility
	// view (orgShard.spaces) — the third container view. Space-scoped events
	// (space, work item, sprint, field def) have no channel or DM to gate on
	// and used to fan ORG-WIDE; they resolve their space against this view.
	//
	// It is a POINTER to per-org state, not a per-connection set, because the
	// answer is per-ORG: see spaceView. nil means "this connection may resolve
	// no Space at all" and covers BOTH withholding cases with one predicate —
	// a GUEST, who is never handed the shared view (P-5: a guest sees nothing
	// beyond their own channels), and a connection whose view has not loaded
	// yet or failed to load. That is what keeps filter free of a role branch
	// and fail-closed by construction (see spaceView's nil-receiver methods).
	//
	// Written only by this connection's own goroutine (Serve's attach and
	// deliverShared's refresh), exactly like channels/dms; the view it points
	// at is immutable once published, so sharing it across the org's
	// connections needs no lock on the read path.
	spaces *spaceView
	// arrivedAt is when this connection's request reached Serve — the freshness
	// anchor for the org's shared catch-up block (see blockServes). It is
	// sampled BEFORE the head lookup, so it precedes every read this connection
	// could itself have issued, and it is stable for the connection's whole
	// life because the resume lane is a CONNECT-time lane: register decides the
	// lane and resume only ever promotes to live, never back.
	arrivedAt time.Time
	// Inbound signal-frame budget (typing storms, abuse).
	frameLimit *rate.Limiter
	// Serializes all writes to conn (coder/websocket forbids concurrent Write).
	writeMu sync.Mutex
}

// spaceView is ONE org's space-visibility answer, shared by every non-guest
// connection in that org and owned by the org shard.
//
// It is per-ORG rather than per-connection because the query that produces it
// has exactly two inputs — the org id and whether the reader is a guest — and
// so has exactly two possible answers in an org: the full set (non-guest) or
// nothing at all (guest, P-5). Spaces are org-visible in the v1 worktrack
// slice (see its package doc), so there is no per-user narrowing to represent.
// Holding it once per org instead of once per connection is what turns one
// space.created in an org with N live connections from N space queries into
// ONE, and resident state from O(connections x spaces) into O(spaces).
//
// A view is IMMUTABLE once published: a change builds a whole new view and
// swaps the shard's pointer, so a connection reading it needs no lock and can
// never observe a half-updated set. Archival is deliberately not filtered — it
// is a lifecycle state, not an ACL state, and REST still reads an archived
// space's items.
//
// NIL IS THE WITHHOLDING ANSWER, and the methods below are written on a nil
// receiver so that every way of not having a view — a guest, a view that has
// not loaded yet, a view whose load failed — resolves through the SAME
// predicate in filter, with no role branch and no fall-through to the org-wide
// fan. That fall-through is the hole #132 closed, so it is expressed in the
// type rather than left to a caller to remember.
type spaceView struct {
	// spaces is the org's Space ids. Read-only after publication.
	spaces map[int64]bool
	// itemSecurityActive reports whether this org defines ANY visibility_scope
	// (P-4 item security). work_item.security_scope_id can only reference such
	// a row, so while the org defines none, NO item is security-scoped and the
	// space set alone is exact. The moment one exists the gateway cannot tell
	// which items it covers without a per-event query — and it has no
	// evaluator for visibility_scope.rule at all — so every work-item event is
	// withheld: an unresolvable scope must never fall through to org-wide
	// delivery.
	//
	// Unlike the space set this flag has NO refresh verb — nothing in the
	// product writes visibility_scope yet (P-4 owns that, and must emit one) —
	// so the only way a connection learns it flipped is a fresh read. That is
	// what the connect-time freshness rule in Hub.attachSpaceView exists for:
	// reconnecting is the documented recovery, and sharing a cached view across
	// connects would have quietly removed it.
	itemSecurityActive bool
	// startedAt is when the load that produced this view BEGAN. It is the
	// connect-time freshness test (see Hub.attachSpaceView): a registering
	// connection may reuse a shared view only if that view's read started after
	// the connection did, so it can never inherit an answer sampled before a
	// change it should see. Set by Hub.orgSpaceView, never read by the ACL.
	startedAt time.Time
}

// visible reports whether the holder of this view may resolve spaceID. A nil
// view resolves nothing: a guest, and a connection registered before its org's
// view loaded, both withhold every space-scoped event.
func (v *spaceView) visible(spaceID int64) bool {
	return v != nil && v.spaces[spaceID]
}

// itemSecurity reports whether work-item events must be withheld wholesale. A
// nil view answers YES — the same seeding the per-connection flag had, now
// structural: a partial or missing load can only ever under-deliver.
func (v *spaceView) itemSecurity() bool {
	return v == nil || v.itemSecurityActive
}

// feedBuffer bounds how many multicast batches may queue for one connection
// before the per-org reader gives up on it (drops it to resync). Small: live
// connections drain immediately; a stalled one is shed fast, not indulged.
const feedBuffer = 16

// recentWindow bounds the per-connection recently-delivered id window. It has
// to cover the only span where a connection can be handed a row it already
// sent: the resume→live hand-off, where the shared batch starts at the org's
// reader position and the connection's own pump may already have run past it
// (events committed DURING the drain). That span is normally a handful of
// events; 64 covers it with 512 bytes per connection, which is what makes the
// window affordable at the 100k-connection target.
//
// Overflowing it costs a DUPLICATE seq — allowed by the protocol since P-45
// and absorbable by any client — never a drop. That asymmetry is the whole
// reason the window may be bounded at all: the failure mode of "too small" is
// chatter, not loss.
const recentWindow = 64

// recentIDs is a fixed ring of the ids a connection has handled most recently.
// A ring rather than a map: 64 int64s are one linear scan of half a cache
// line's worth of lines, it allocates once, and it is only ever SCANNED on the
// rare path (a row at or below the cursor — see client.passed), while the
// common forward row answers from the cursor alone.
type recentIDs struct {
	ring [recentWindow]int64
	next int
}

// add records id, evicting the oldest. Event ids start at 1, so the zero value
// can never collide with a real id.
func (r *recentIDs) add(id int64) {
	r.ring[r.next] = id
	r.next++
	if r.next == len(r.ring) {
		r.next = 0
	}
}

func (r *recentIDs) has(id int64) bool {
	for _, v := range r.ring {
		if v == id {
			return true
		}
	}
	return false
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
	// head is the org's live event cursor in ID space: the highest event-log id
	// the multicast reader has fanned (guarded by mu). A resuming connection
	// joins the live lane only once its own cursor reaches head, and the reader
	// advances head under the same lock — so the resume→live hand-off is a
	// single consistent point and no event slips through it. It is compared
	// against a connection's last_id, which is an event id (ADR-002 F-2), so
	// it must stay in id space and it must be MONOTONE: under a commit-ordered
	// feed a batch can carry a LOWER id than one already fanned, and a head
	// that fell back would send a still-behind connection live.
	head int64
	// pos is the same cursor in the FEED DRIVER's ordering domain (an event id
	// under xmin, a commit LSN under logical) and is what the reader actually
	// reads from. It is separate from head because commit order is not id
	// order: under the logical driver head can stand still while pos advances
	// past a late-committing lower id.
	pos eventlog.Position
	// wake signals the per-org multicast reader; buffered 1 so many event
	// notifications for one org coalesce into a single catch-up pass.
	wake chan struct{}
	// stop ends the reader goroutine when the shard is dropped (last connection
	// leaves) or the hub shuts down.
	stop context.CancelFunc
	// spaces is the ORG's shared space-visibility view (see spaceView), held
	// once here instead of once per connection. Atomic rather than under mu
	// because it is read on the delivery path by every connection in the org
	// and mu is the fan lock: a shard lock taken per space-scoped event would
	// put 100k connections back in each other's way, which is exactly what the
	// S3 lock split removed. nil until the org's first connection loads it, and
	// nil again if a load fails — both WITHHOLD (spaceView's nil methods).
	//
	// It lives on the SHARD, not the hub, so it dies with the org's last
	// connection: an idle org costs nothing, a live org costs O(spaces), and
	// there is no hub-wide map in which one org's set could be handed to
	// another's connection.
	spaces atomic.Pointer[spaceView]
	// spaceMu single-flights loads of `spaces`. Deliberately NOT mu: the load
	// is a pool round trip, and holding the fan lock across it would stall the
	// org's whole multicast (S3's "never do I/O under the shard lock"). Its
	// only job is that N connections reacting to the same space.created issue
	// ONE query between them — each waiter re-checks `spaces` under the lock
	// and takes the winner's result.
	spaceMu sync.Mutex
	// catchup is the org's shared RESUME read — the last catch-up block a
	// resuming connection published, held here so a reconnect cohort issues one
	// read between them instead of one each (see catchup.go). Atomic and
	// immutable-once-published for the same reason `spaces` is: it is read on
	// the resume path without a lock, and mu is the fan lock.
	//
	// It lives on the SHARD, so it dies with the org's last connection and no
	// hub-wide map can hand one org's event rows to another's connection; the
	// 5s sweep drops it earlier (dropStaleCatchup) once it can serve nobody.
	catchup atomic.Pointer[catchupBlock]
	// catchupMu single-flights the read that fills `catchup`, exactly as
	// spaceMu does for the space view, and deliberately NOT mu: it is held
	// across a pool round trip, and the fan lock must never be.
	catchupMu sync.Mutex
	// Per-user derived presence (connection count + last activity + idle flag);
	// per-process, never stored (the UNLOGGED presence table is unused).
	userConns map[int64]*userPresence // userID → presence
}

type Hub struct {
	pool *pgxpool.Pool
	log  *slog.Logger

	// tail is the event source for BOTH lanes (the S4 driver seam, P-45): the
	// per-org multicast reader's hoisted live read and the per-connection
	// resume replay. Default = the xmin-gated tail this hub has always used,
	// so a hub that never calls SetSource behaves exactly as before;
	// SetSource swaps in the operator's driver.
	tail eventlog.Tail
	// ordered caches tail.Ordered() — read once per delivered row, and the
	// difference between "already delivered" being a cursor comparison and
	// being a window lookup (client.passed). Cached, not called, because it is
	// a fixed property of the wired driver and this is the hot loop.
	ordered bool

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
	// catch-up reads on BOTH lanes: the per-org multicast reader runs ONE per
	// event-batch (S3, O(1) per org) and the resume lane one per COHORT rather
	// than per resuming connection (see catchup.go) — so neither term scales
	// with connection count. Set once at wiring (docs/PERF.md).
	pumpQueries metrics.Counter
	deliveries  metrics.Counter
	connections metrics.Gauge
	// encoded counts Envelope JSON marshals — the marshal-once invariant: the
	// live multicast lane encodes each event ONCE for the whole org, so this
	// rises O(events), not O(connections). A regression to per-connection
	// marshaling makes it rise ~O(connections) (the red/green pin).
	encoded metrics.Counter
	// resumeReads counts the RESUME lane's catch-up reads alone — the subset of
	// pumpQueries a reconnect storm produces. It is its own series because the
	// two lanes now have different shapes: the live read is one per org per
	// event-batch, and this one is one per org per COHORT, so a regression to
	// one per resuming connection is visible here and invisible in the sum.
	resumeReads metrics.Counter
	// spaceQueries counts reads of the space-visibility set. Since the set is
	// held per ORG it rises once per org's first connection plus once per
	// space.created — CONSTANT in the number of live connections, which is the
	// pin: reverting to a per-connection load makes it rise with N.
	spaceQueries metrics.Counter
}

func NewHub(pool *pgxpool.Pool, log *slog.Logger) *Hub {
	h := &Hub{pool: pool, log: log, IdleAfter: 10 * time.Minute,
		orgs: map[int64]*orgShard{}, runCtx: context.Background(),
		plane: presence.Local(), tail: eventlog.NewTail(pool), ordered: true}
	h.SetMetrics(metrics.Nop())
	return h
}

// SetSource swaps the event-feed driver (S4/P-45) — the runner SetSource
// pattern, for the one consumer that reads through the CURSOR-FREE half of the
// seam. Optional: the default is the xmin-gated tail this hub has always used,
// which is also what keeps the default driver byte-for-byte unchanged.
//
// Under a commit-ordered driver the gateway also changes what it treats as
// "already delivered" — see client.passed — so this must be called ONCE at
// wiring, before Run/Serve, and never mid-flight: connections registered under
// one rule must not be delivered to under the other.
func (h *Hub) SetSource(src eventlog.Source) {
	h.tail = src.Tail()
	h.ordered = h.tail.Ordered()
	// Take the driver's PUSH wake as well as the LISTEN one. Under a streaming
	// driver the two fire at different instants and only this one is useful:
	// Append's NOTIFY arrives at COMMIT, before the WAL reader has decoded the
	// commit, so the woken read finds nothing and live delivery silently falls
	// back to the 5s sweep. wakeOrg is a map lookup plus a non-blocking send,
	// which is what the OnWake contract requires. The LISTEN path stays: it is
	// the whole wake mechanism for the default driver.
	h.tail.OnWake(h.wakeOrg)
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
func (h *Hub) register(c *client, pos eventlog.Position, orgHead int64, now time.Time) (announce, live bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	sh := h.orgs[c.id.OrgID]
	if sh == nil {
		sh = h.newShard(c.id.OrgID, pos, orgHead)
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
func (h *Hub) newShard(orgID int64, pos eventlog.Position, head int64) *orgShard {
	ctx, cancel := context.WithCancel(h.runCtx)
	sh := &orgShard{
		orgID:     orgID,
		conns:     map[*client]struct{}{},
		userConns: map[int64]*userPresence{},
		head:      head,
		pos:       pos,
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
	h.resumeReads = reg.Counter("gateway_resume_reads_total")
	h.deliveries = reg.Counter("fanout_deliveries_total")
	h.connections = reg.Gauge("gateway_connections", "org")
	h.encoded = reg.Counter("gateway_envelopes_encoded_total")
	h.spaceQueries = reg.Counter("gateway_space_queries_total")
}

// encodeEnvelope is the SINGLE Envelope-marshal choke point, so the encoded
// counter measures exactly how many times an event was JSON-encoded. Both
// shared lanes call it once per event for the whole org (marshal-once) — the
// live multicast batch and the shared catch-up block; only a row neither
// published is encoded per delivery. Default Nop registry makes the count a
// no-op when metrics are off.
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
			h.sweepOnce(time.Now())
		}
	}
}

// sweepOnce is one pass of the poll fallback, with the clock injected so a test
// can drive it (the worker convention): wake every live org's reader in case a
// NOTIFY was missed, and release any shared catch-up block the cohort that
// produced it has long since drained (see dropStaleCatchup).
func (h *Hub) sweepOnce(now time.Time) {
	for _, sh := range h.shards() {
		sh.wakeReader()
		sh.dropStaleCatchup(now)
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
	// Stamped FIRST, before the head lookup below: it is the freshness anchor
	// for the org's shared catch-up block (blockServes), and anchoring it ahead
	// of every read this connection could itself have issued is what makes a
	// shared read provably no staler than a solo one.
	arrivedAt := time.Now()
	lastID, _ := strconv.ParseInt(r.URL.Query().Get("last_id"), 10, 64)
	// The org's current event cursor, from the feed driver: the POSITION seeds
	// a NEW shard's live reader (so it only ever reads fresh events) and the id
	// resolves tail mode (last_id<0 → start at head, no history replay). One
	// call serves both, and it samples them in the order that cannot lose an
	// event — see eventlog.Tail.Head. A fresh client renders state from REST,
	// then follows the live stream.
	pos, orgHead, err := h.tail.Head(r.Context(), id.OrgID)
	if err != nil {
		// Includes ErrFeedNotReady on a logical cell whose WAL reader has not
		// started yet: the client retries, which is the same answer every
		// other consumer of that driver gets. Never a silent fallback to the
		// gated poller, which would hide a misconfigured cell.
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
	// Seeded WITHHOLDING: every container view starts at its zero value and
	// every zero value denies — nil channel/dm maps deny by lookup, and a nil
	// spaces view resolves no Space AND reports item security active
	// (spaceView's nil methods). So a connection whose ACL load never ran, or
	// failed, can only ever under-deliver. loadChannels and syncSpaceView below
	// are the only things that relax any of it.
	c := &client{conn: ws, id: id, cancel: cancel, lastID: lastID,
		arrivedAt: arrivedAt, feed: make(chan []eventRow, feedBuffer),
		wake: make(chan struct{}, 1), frameLimit: newFrameLimiter()}

	// Setup order matters (fixes a startup race): load the membership view and
	// register the connection BEFORE reading any client frame, so the sender's
	// own ACL view is populated and the connection is discoverable as a
	// fan-out target. Only then start the reader and announce `ready`.
	if err := c.loadChannels(ctx, h.pool); err != nil {
		ws.Close(websocket.StatusInternalError, "membership load failed")
		return
	}
	announce, live := h.register(c, pos, orgHead, time.Now())
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

	// Take the ORG's shared space view. Two orderings are load-bearing here.
	// It comes AFTER register because it hangs off the shard, which is safe in
	// the only direction that matters: until it lands c.spaces is nil and every
	// space-scoped event is WITHHELD, so a connection that is already a fan
	// target can under-deliver for an instant but can never fall through to the
	// org-wide branch. And it comes AFTER the tail.Head sample above, which is
	// what makes the two halves of the view's freshness meet: a Space this read
	// misses committed after lastID was taken, so its space.created is above
	// this connection's cursor and will drive a refresh rather than be skipped.
	if err := h.attachSpaceView(ctx, c); err != nil {
		ws.Close(websocket.StatusInternalError, "space view load failed")
		return
	}

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
	pos := sh.pos
	sh.mu.Unlock()

	// THE S3 read: one hoisted org-scope catch-up read per event-batch. This
	// counter used to rise once PER connection PER wake (the blowup); now it
	// rises once per org per batch (plus rare resume pumps) — docs/PERF.md.
	h.pumpQueries.Add(1)
	rows, next, err := h.tail.Next(ctx, sh.orgID, pos, batchLimit)
	if err != nil {
		if errors.Is(err, eventlog.ErrFeedNotReady) {
			// The driver's WAL reader is not streaming (starting up, or another
			// node holds the single per-cell slot). Nothing to do; the sweep
			// retries. Not a warning — it is a documented normal condition.
			return false, nil
		}
		if errors.Is(err, eventlog.ErrCursorTooOld) {
			h.resync(sh)
			return false, nil
		}
		return false, err
	}
	if len(rows) == 0 {
		// Still persist the position: a driver may advance past a window whose
		// bodies are gone (a retention partition dropped between decode and
		// read), and a reader that did not record that would re-read the dead
		// span on every wake forever. head does not move, so the resume→live
		// hand-off is untouched.
		sh.mu.Lock()
		sh.pos = next
		sh.mu.Unlock()
		return false, nil
	}
	// Marshal each row's Envelope ONCE here, in the single reader goroutine,
	// before fanning: the bytes are identical for every connection in the org,
	// so the per-connection deliver reuses them (marshal-once — the CPU twin of
	// the O(1) read above). The channel send in feed establishes happens-before,
	// so the connection goroutines read enc safely.
	batch := fanRows(rows)
	var maxID int64
	for i, r := range rows {
		batch[i].enc, _ = h.encodeEnvelope(Envelope{
			Seq: r.ID, Type: r.Verb, OrgID: sh.orgID, Payload: r.Payload,
		})
		if r.ID > maxID {
			maxID = r.ID
		}
	}

	sh.mu.Lock()
	live := make([]*client, 0, len(sh.conns))
	for c := range sh.conns {
		if c.live {
			live = append(live, c)
		}
	}
	sh.pos = next
	// head only ever RISES: a commit-ordered batch may carry a lower id than
	// one already fanned, and letting the go-live boundary fall back would
	// promote a connection that has not covered the head yet.
	if maxID > sh.head {
		sh.head = maxID
	}
	sh.mu.Unlock()

	for _, c := range live {
		h.feed(c, batch)
	}
	// The logical driver cuts on commit boundaries and may overshoot the limit,
	// so a full batch is >=, not ==.
	return len(rows) >= batchLimit, nil
}

// resync drops every connection in the shard so its clients reconnect and
// replay the gap from the log (F-2). It answers eventlog.ErrCursorTooOld: the
// org's live position fell further behind than the feed driver's bounded
// commit-order window, so the shared reader can no longer be served from it.
// Reconnecting replays through the RESUME lane, an id-ordered read of the
// table, so nothing is actually lost — while leaving the shard in place would
// wedge the whole org's live lane on the same error forever, which is exactly
// the undetectable stall this series exists to remove.
func (h *Hub) resync(sh *orgShard) {
	sh.mu.Lock()
	conns := make([]*client, 0, len(sh.conns))
	for c := range sh.conns {
		conns = append(conns, c)
	}
	sh.mu.Unlock()
	h.log.Warn("gateway: org fell out of the event-feed window; dropping connections to resync",
		"org", sh.orgID, "connections", len(conns))
	for _, c := range conns {
		c.cancel()
	}
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
	// boundaryAt is the timestamp the read ACL judges this event at for the
	// protected-history floor — the earlier of occurred_at and recorded_at,
	// applied in fanRows (P-46 computed it in SQL; the feed seam removed that
	// query, so the rule moved into Go over eventlog.Row, which carries both
	// columns). The floor is a TIMESTAMPTZ (channel_member.history_from) while the
	// stream is ordered by id, so there is no id-space equivalent; the
	// question is only WHICH timestamp, and neither column is right alone:
	//
	//   · occurred_at is DOMAIN time, but the APP clock writes it
	//     (eventlog.Append's time.Now()), while history_from is the DB clock
	//     (now() in the join transaction). App-ahead-of-DB skew would put an
	//     event that raced the join on the DELIVERED side of a boundary REST
	//     hides — the one leak-direction residual this closes.
	//   · recorded_at is the DB clock (DEFAULT now() = the appending tx's
	//     start), so for a live message — appended in the SAME transaction as
	//     the INSERT — it is the identical value message.created_at got, i.e.
	//     exactly what messaging.Get compares. But an IMPORT backdates
	//     occurred_at and message.created_at together (E3) while recorded_at
	//     stays at INGEST time, so recorded_at alone would stream a member the
	//     pre-join history a backfill just wrote.
	//
	// LEAST is exact in both cases and errs toward WITHHOLDING in every other:
	// a read ACL never wants the later of two candidate times.
	boundaryAt time.Time
	// entityType is event_log.entity_type — the event's own NOT NULL statement
	// of what it is ABOUT. The read ACL classifies with it rather than by
	// sniffing payload keys: a work-item event whose payload happens to carry
	// no space_id is still a work-item event and must fail closed, where key
	// sniffing would silently fall through to org-wide delivery.
	// fanRows below is the SINGLE construction site (P-45) precisely so a
	// column the ACL reads cannot be populated on one lane and left zero on
	// the other — a zero entityType is not space-scoped, so scoped events
	// would fan org-wide; a zero boundaryAt precedes every floor.
	entityType enum.EntityType
	// enc is the row's Envelope pre-marshaled ONCE for the whole org (the
	// Envelope is identical for every connection in it — same seq, verb,
	// payload, org id), so the fan reuses these bytes instead of re-encoding per
	// connection: O(1) marshal per event, not O(connections). Both shared lanes
	// fill it — the multicast reader for a live batch, catchUp for a shared
	// resume block — and it stays nil only on rows nothing published, where
	// deliverShared marshals its own.
	enc []byte
}

// fanRows is the ONE place an eventRow is built, from the feed driver's row.
// Both lanes go through it — the shared multicast batch and the per-connection
// resume batch — so a column the read ACL depends on cannot be populated on one
// lane and left at its zero value on the other.
//
// boundaryAt is LEAST(occurred_at, recorded_at) — see the field doc for WHY
// neither clock is correct alone. P-46 computed that in SQL, in a query the
// feed seam removed, so the SAME rule is applied here in Go over eventlog.Row,
// which carries BOTH columns. Taking OccurredAt alone silently reinstates the
// app-vs-DB clock-skew leak (gateway_acl_test.go break 17).
func fanRows(rows []eventlog.Row) []eventRow {
	out := make([]eventRow, len(rows))
	for i, r := range rows {
		boundary := r.OccurredAt
		if r.RecordedAt.Before(boundary) {
			boundary = r.RecordedAt
		}
		out[i] = eventRow{
			id: r.ID, verb: r.Verb, payload: r.Payload,
			boundaryAt: boundary, entityType: r.EntityType,
		}
	}
	return out
}

// pump is the RESUME lane: a connection behind the org head drains its gap
// batch by batch. It survives only for the reconnect gap (F-2's replay window);
// steady-state live traffic flows through the per-org multicast reader, never
// here.
//
// The batch comes from catchUp, which reads HISTORY — id-ordered, after the
// client's last_id, because a resume cursor is an event id, not a driver
// position — and SHARES that read with the rest of a reconnect cohort when it
// can (catchup.go). Whether the read is visibility-gated is the driver's
// business (eventlog.Tail.History), not this lane's.
//
// The loop condition is the READ's `more`, not the length of this connection's
// own suffix: a shared block that hit batchLimit means there is more behind it
// even for a connection only a few of its rows were new to.
func (h *Hub) pump(ctx context.Context, c *client) error {
	for {
		batch, more, err := h.catchUp(ctx, c)
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			return nil
		}
		if err := h.deliverShared(ctx, c, batch); err != nil {
			return err
		}
		if !more {
			return nil
		}
	}
}

// deliverShared applies one connection's ACL filter to a batch of rows — the
// resume pump's own batch OR the per-org reader's SHARED batch — writing the
// events it may see and advancing its cursor past every row (gaps in seq are
// expected, F-2). Rows this connection has already handled are skipped; what
// "already handled" MEANS is the driver's ordering guarantee, not a constant —
// see client.passed.
func (h *Hub) deliverShared(ctx context.Context, c *client, batch []eventRow) error {
	for _, r := range batch {
		if c.passed(r.id, h.ordered) {
			continue
		}
		out := c.filter(r)
		if out.membership {
			if err := c.loadChannels(ctx, h.pool); err != nil {
				return err
			}
		}
		if out.newSpace != 0 {
			// Applied AFTER the verdict above, so the space.created envelope
			// itself is still decided against the PRE-refresh view and withheld
			// (the member.joined precedent) while every LATER row in this batch
			// sees the new Space. Costs ONE query for the whole org, or none
			// when the shared view already resolves that Space — which is the
			// whole point of hoisting it: this line used to run a per-connection
			// three-query reload on each of the org's delivery goroutines.
			if err := h.refreshSpaceView(ctx, c, out.newSpace); err != nil {
				return err
			}
		}
		if out.deliver {
			// Reuse the shared marshal-once bytes (the live batch's or the
			// shared catch-up block's); a row nothing published marshals here.
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
		c.pass(r.id, h.ordered)
	}
	return nil
}

// passed reports whether this connection has ALREADY handled id — the skip
// that decides, per row, between a silent DROP and a duplicate.
//
// `ordered` is the feed driver's own answer to "is delivery order event-id
// monotone?" (eventlog.Tail.Ordered), and it is the whole distinction:
//
//   - ORDERED (the default xmin driver): the scan is ORDER BY id behind a
//     visibility gate, so nothing at or below a delivered id can still arrive.
//     The cursor IS the exact, unbounded answer. This is the pre-P-45 rule,
//     unchanged, which is what keeps the default driver byte-for-byte as it
//     was.
//   - COMMIT-ORDERED (logical): the same test is a silent drop. txid is
//     stamped at a transaction's first write and the id at append, so a
//     transaction can hold the LOWER id and commit LAST; its event then
//     arrives BELOW the cursor, legitimately, and "id <= lastID" would throw
//     it away — the exact undetectable loss S4 removed from the durable
//     consumers, reintroduced at the connection. So the connection consults
//     what it has ACTUALLY sent (a bounded window) and delivers anything else.
//
// A row above the cursor is new under EITHER rule, which is the overwhelmingly
// common case and answers without touching the window; only a row at or below
// the cursor — a crossing, or the resume→live overlap — pays the scan.
//
// A missing window (nil, or an id evicted from it) resolves to "not handled",
// i.e. DELIVER: the failure mode of forgetting is a duplicate seq, which the
// protocol allows and a client absorbs, never a drop.
func (c *client) passed(id int64, ordered bool) bool {
	if id > c.lastID {
		return false
	}
	if ordered {
		return true
	}
	return c.recent != nil && c.recent.has(id)
}

// pass records that this connection has handled id: it advances the resume
// cursor (checkpoint seq and the pump's floor, so MONOTONE — a client's
// last_id must never go backwards) and, under a commit-ordered feed, remembers
// the id itself, because there the cursor alone cannot answer passed().
func (c *client) pass(id int64, ordered bool) {
	if !ordered {
		if c.recent == nil {
			c.recent = &recentIDs{}
		}
		c.recent.add(id)
	}
	if id > c.lastID {
		c.lastID = id
	}
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

// aclOutcome is one row's verdict from client.filter: whether the connection
// may see the row, and which of its container views the row says are now
// stale. The two are separate because they are applied at DIFFERENT times —
// the verdict decides THIS row (against the pre-refresh views, the
// member.joined precedent) while the refreshes take effect from the NEXT row —
// and because they cost wildly different things.
type aclOutcome struct {
	deliver bool
	// membership marks an event about THIS user's own channel/DM membership:
	// the per-connection channel and DM views must be re-read. Genuinely
	// per-connection (it is that user's membership), so it stays a
	// per-connection load — and it is scoped to one user's connections, never
	// the org's.
	membership bool
	// newSpace is the Space id a space.created announced, or 0. An ID and not
	// a flag on purpose: the shared per-org view is reloaded only when it does
	// not ALREADY know that Space (spaceView.covers), which is what collapses
	// one space.created in an org with N live connections to ONE query instead
	// of N, and what keeps a resume lane replaying historical space.created
	// events — whose Spaces the current view already has — from querying at all.
	newSpace int64
}

// filter applies the read ACL: channel-scoped events require membership AND
// clear the channel's protected-history floor, DM-scoped events require
// participation, SPACE-scoped events require space visibility, and only what
// is genuinely org-wide fans org-wide. An event that names several containers
// must clear ALL of their gates. Events about this user's own membership
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
// it judges floorAt (the earlier of eventRow.boundaryAt, see there for WHY
// that is LEAST(occurred_at, recorded_at) and not either column alone, and the
// referenced message's created_at when the payload names one), and its
// boundary is INCLUSIVE of history_from: an event AT the stamp is delivered.
// That is exactly messaging.ListMessages' `created_at >= history_from` and the
// negation of messaging.Get's `created_at < history_from` hide-rule, so the
// realtime plane and the REST read answer identically at the boundary instant.
func (c *client) filter(r eventRow) (out aclOutcome) {
	var p struct {
		ChannelID int64   `json:"channel_id"`
		DMSpaceID int64   `json:"dm_space_id"`
		SpaceID   int64   `json:"space_id"`
		UserID    int64   `json:"user_id"`
		UserIDs   []int64 `json:"user_ids"`
		// MessageCreatedAt is eventlog.MessageCreatedAtKey — the created_at of
		// the message this event is ABOUT (absent when it is about no existing
		// message). Deliberately RAW rather than a time.Time, for two reasons.
		// (1) Isolation: a time.Time field whose value fails to parse ABORTS
		// encoding/json's object decode, leaving every field spelled after it
		// in the payload at its ZERO value — verified. Today's key order
		// (Go marshals map keys alphabetically) happens to put channel_id and
		// dm_space_id BEFORE it, so the reachable damage is withhold-direction,
		// but a read ACL must not rest on a key-sort accident; raw bytes never
		// fail to decode, so no gate can be disturbed by the value of this one
		// field. (2) Cost: floorAt parses it only on the rare row that actually
		// faces a floor, so the common event pays nothing.
		MessageCreatedAt json.RawMessage `json:"message_created_at"`
	}
	_ = json.Unmarshal(r.payload, &p)
	if (r.verb == "member.joined" || r.verb == "member.left") && p.UserID == c.id.UserID {
		out.membership = true
	}
	// A new Space widens (or, for a guest, does not widen) the ORG's space
	// view, so it drives the same event-driven refresh membership does — but
	// against per-org state, so the org pays for it once however many
	// connections observe it. The event itself is decided against the
	// PRE-refresh view and so is withheld — the member.joined precedent: the
	// view catches up, the envelope that caused it does not arrive, and the
	// client's next REST read closes the gap (F-2 tolerates a detectable gap,
	// not a leak).
	if r.verb == "space.created" {
		out.newSpace = p.SpaceID
	}
	if r.verb == "dm.opened" || r.verb == "dm.participants_changed" {
		for _, uid := range p.UserIDs {
			if uid == c.id.UserID {
				out.deliver, out.membership = true, true
				return out
			}
		}
		return out
	}
	// The container gates below are CONJUNCTIVE, not a first-match dispatch: an
	// event may name MORE than one container and must clear every gate it
	// names. workitem.promoted_from_thread is the case — the D2 fusion's
	// reverse direction leaves the discussion governed by its CHANNEL (F-5)
	// while the item it now backs lives in a SPACE, so its payload carries
	// channel_id AND space_id. A first-match dispatch let the channel gate
	// PASS and return, skipping the space and item-security gates entirely;
	// conjunction is the fail-closed direction (it can only ever withhold
	// more) and it is why neither gate may return true early.
	if p.ChannelID != 0 {
		if !c.channels[p.ChannelID] {
			return out
		}
		// Protected-history floor. Absent key = no boundary (shared channel or
		// the protected channel's creator) — the common case, one map miss.
		if floor, bounded := c.historyFloor[p.ChannelID]; bounded &&
			floorAt(r.boundaryAt, p.MessageCreatedAt).Before(floor) {
			return out
		}
	}
	if p.DMSpaceID != 0 && !c.dms[p.DMSpaceID] {
		return out
	}
	if spaceScoped(r.entityType) {
		// These events belong to a Space, so they resolve one instead of
		// fanning. Everything here fails CLOSED — an unresolvable scope is
		// withheld, never dropped through to the org-wide return below, which
		// is exactly the hole this closes.
		//   · space_id missing from the payload   → withhold (unresolvable)
		//   · space not resolvable by this view   → withhold. ONE predicate
		//     covers three cases with no role branch, because all three hold a
		//     view that resolves nothing: a GUEST (never handed the org's
		//     shared view — see Hub.syncSpaceView, which is where the P-5
		//     restriction lives now that it is no longer a SQL parameter), a
		//     connection whose view has not loaded yet, and one whose load
		//     failed. See spaceView's nil-receiver methods.
		//   · a work item while the org defines any visibility_scope →
		//     withhold (no evaluator exists for the scope's rule), and a view
		//     that cannot answer at all reports it ACTIVE.
		if p.SpaceID == 0 || !c.spaces.visible(p.SpaceID) {
			return out
		}
		if r.entityType == enum.EntityWorkItem && c.spaces.itemSecurity() {
			return out
		}
	}
	// Cleared every gate it named. An event that named NO container and is not
	// space-scoped is genuinely org-wide: org settings, emoji, user directory,
	// and the space-governed THREAD traffic (message.*/thread.*) that the v1
	// visibility slice makes org-visible over REST too — withholding it here
	// would over-withhold against messaging.Get, not close a hole.
	out.deliver = true
	return out
}

// floorAt is the timestamp the protected-history floor judges ONE event at:
// the EARLIER of the event's own boundaryAt and the created_at of the message
// the event is ABOUT, when its payload names one
// (eventlog.MessageCreatedAtKey).
//
// It extends the LEAST discipline boundaryAt already is rather than inventing a
// second rule, and that is what makes it fail-closed BY CONSTRUCTION: the
// earlier of two times always wins, so every outcome can only withhold MORE
// than judging boundaryAt alone, never less.
//
//   - key ABSENT (len 0) → boundaryAt, i.e. exactly the pre-existing rule. The
//     field's presence is never made load-bearing in the opening direction: an
//     event about no existing message (message.created, a channel rename) must
//     keep flowing, and a producer that has not been taught the key yet must
//     not be handed a wider gate than it had.
//   - key present and PARSEABLE → the earlier of the two. The message's own
//     time is what REST compares (messaging.Get's `m.created_at <
//     cm2.history_from`), so this is the value that makes the two planes agree;
//     boundaryAt still caps it, so a producer cannot widen its own audience by
//     naming a LATER created_at than the event.
//   - key present and UNPARSEABLE → the zero time, which precedes every floor,
//     so the event is withheld. A value we cannot read is a value we cannot
//     authorize; over-withholding costs a client a REST refetch, under-
//     withholding is the oracle this closes. JSON `null` lands here too, by a
//     different route: time.Time accepts it without error and yields the zero
//     time, which withholds for the same reason.
//
// The parse is paid only on rows that actually face a floor (a protected
// channel this connection joined late), so the common event stays the same
// single Unmarshal it always was — the O(1)-per-event fan-out invariant the
// scale slices established is untouched, and no lookup leaves the process.
func floorAt(boundaryAt time.Time, messageCreatedAt json.RawMessage) time.Time {
	if len(messageCreatedAt) == 0 {
		return boundaryAt
	}
	var refAt time.Time
	if err := json.Unmarshal(messageCreatedAt, &refAt); err != nil {
		return time.Time{}
	}
	if refAt.Before(boundaryAt) {
		return refAt
	}
	return boundaryAt
}

// spaceScoped reports whether an event's entity belongs to a Space — a gate
// most of these events have INSTEAD of a channel or DM, and one
// (workitem.promoted_from_thread) has IN ADDITION to a channel. These four are
// the only entity types worktrack appends under, and only work_item carries a
// security_scope_id.
func spaceScoped(t enum.EntityType) bool {
	switch t {
	case enum.EntitySpace, enum.EntityWorkItem, enum.EntitySprint, enum.EntityFieldDef:
		return true
	}
	return false
}

// loadChannels reloads the two GENUINELY per-connection container views:
// channel membership (with its protected-history floors) and DM participation.
// Both are this user's own membership, so they cannot be shared with the org;
// they are also only ever refreshed by an event about THIS user, so the cost is
// bounded to that user's connections. The space view is deliberately NOT
// chained here any more — it is per-ORG (see spaceView) and refreshed through
// Hub.syncSpaceView, which is what stopped one space.created from costing three
// pool queries per live connection.
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
	return nil
}

// attachSpaceView is the CONNECT-time half of the refresh protocol: it gives a
// registering connection its org's space view, requiring a view whose read
// BEGAN after this connection did.
//
// That freshness rule is not paranoia, it is the item-security contract.
// itemSecurityActive has no refresh verb (nothing writes visibility_scope yet;
// P-4 must emit one), so reconnecting is the ONLY way a connection learns the
// org's first scope appeared — a recovery `gateway_acl_test.go` pins directly.
// Reusing an arbitrarily old shared view here would have deleted it silently,
// in the DELIVERING direction. So a connect costs the same one space query it
// always did; what this slice removes is the per-connection query on the FAN
// path, which is where the herd was.
//
// It is still shared where sharing is free: connections that arrive WHILE a
// load is running all take that load's result, because it started after they
// did. A thundering herd of registrations therefore collapses to roughly one
// query per load latency instead of one per connection, and a steady trickle
// pays exactly what it paid before.
func (h *Hub) attachSpaceView(ctx context.Context, c *client) error {
	arrived := time.Now()
	return h.syncSpaceView(ctx, c, func(v *spaceView) bool {
		return v != nil && v.startedAt.After(arrived)
	})
}

// refreshSpaceView is the EVENT-driven half: a space.created announced spaceID,
// so the org's view must resolve it. Any view that already does is accepted
// as-is, which is what turns one space.created into ONE query for the whole org
// however many connections observe it — and what keeps a resume lane replaying
// historical space.created events, whose Spaces the current view already holds,
// from querying at all.
func (h *Hub) refreshSpaceView(ctx context.Context, c *client, spaceID int64) error {
	return h.syncSpaceView(ctx, c, func(v *spaceView) bool { return v.visible(spaceID) })
}

// syncSpaceView points c at its org's shared space view, loading one if the
// shard holds nothing `fresh` accepts.
//
// THIS IS WHERE THE GUEST RESTRICTION LIVES. It used to be a SQL parameter
// (`AND NOT $2`), which only worked while the query was per-connection: a view
// loaded once for the whole org cannot carry a per-reader role. So a guest is
// never HANDED the view at all — it keeps the nil view it was born with, which
// resolves no Space and reports item security active. The restriction therefore
// stays out of filter (one predicate for everyone, no role branch, exactly as
// #132 built it), it is stated once here, and a guest costs the org nothing: it
// does not even trigger a load.
//
// A load failure leaves c.spaces at whatever it was — nil for a fresh
// connection (withhold), or its last good view for an established one, which
// can only be MISSING Spaces, never carrying extra — and propagates, so the
// caller drops the connection exactly as a failed membership load does.
func (h *Hub) syncSpaceView(ctx context.Context, c *client, fresh func(*spaceView) bool) error {
	if c.id.IsGuest() {
		return nil
	}
	// Shared state is where cross-org bugs live, so the org pin is asserted at
	// the hand-off as well as carried in the query below: a view built for
	// another org must never reach this connection.
	if c.shard.orgID != c.id.OrgID {
		return fmt.Errorf("gateway: shard org %d serving connection org %d",
			c.shard.orgID, c.id.OrgID)
	}
	v, err := h.orgSpaceView(ctx, c.shard, fresh)
	if err != nil {
		return err
	}
	c.spaces = v
	return nil
}

// orgSpaceView returns the org's shared space view, loading one when the shard
// holds nothing the caller accepts — and loading it AT MOST ONCE however many
// connections ask at the same instant.
//
// The refresh protocol in full:
//   - WHO loads: a connection, never the multicast reader. The reader could not
//     be the only trigger anyway — a registering connection needs a coherent
//     view before its first batch and Serve must not block on another
//     goroutine to get one — and a pool round trip inside the reader would
//     stall the org's ENTIRE fan on a rare event.
//   - WHEN: on register (attachSpaceView's freshness rule) and on a
//     space.created naming a Space the shared view does not hold
//     (refreshSpaceView). Nothing else changes the set — space.created is the
//     only verb worktrack appends that adds a Space — and a guest never asks.
//   - HOW ONCE: the double check around spaceMu. N connections handed the same
//     space.created all fail `fresh`, all reach for spaceMu, and the winner
//     publishes; every loser re-checks INSIDE the lock, accepts the winner's
//     view and returns without querying. spaceMu is not the shard's fan lock,
//     so the org's multicast is never held up by this (S3's rule: no I/O under
//     the lock that fans).
//   - MID-REFRESH REGISTRATION: coherent by construction. A view is immutable
//     and published by ONE atomic Store, so a connection registering during a
//     reload takes the old view or the new one, never a torn one — and its
//     freshness rule then rejects any view whose read began before it did.
//   - A SPACE COMMITTED BUT NOT YET FANNED: also covered, by ORDER in Serve.
//     The connection samples the org head BEFORE this load runs, so a Space
//     that commits after the load has an event id ABOVE the connection's
//     cursor; that space.created is therefore never skipped as already-handled
//     and drives refreshSpaceView normally. A Space that commits before the
//     load is simply in it.
//   - ON FAILURE: WITHHOLD, and never cache the failure. The shard's view is
//     reset to nil so the next asker retries with a real query instead of
//     inheriting a half-built set, and the error propagates to the caller,
//     which drops the connection exactly as a failed membership load does.
func (h *Hub) orgSpaceView(ctx context.Context, sh *orgShard, fresh func(*spaceView) bool) (*spaceView, error) {
	if v := sh.spaces.Load(); fresh(v) {
		return v, nil
	}
	sh.spaceMu.Lock()
	defer sh.spaceMu.Unlock()
	if v := sh.spaces.Load(); fresh(v) {
		return v, nil // another connection loaded it while we waited
	}
	// The one place the space set is read from the database. Counted so the
	// per-ORG claim is measurable: this rises once per space.created for the
	// whole org, NOT once per connection (docs/PERF.md).
	h.spaceQueries.Add(1)
	startedAt := time.Now()
	v, err := loadSpaceView(ctx, h.pool, sh.orgID)
	if err != nil {
		sh.spaces.Store(nil) // a failed load is never cached; nil withholds
		return nil, err
	}
	v.startedAt = startedAt
	sh.spaces.Store(v)
	return v, nil
}

// loadSpaceView reads ONE org's space-visibility answer — the container view
// for events that have neither a channel nor a DM. Spaces are org-visible in
// the v1 worktrack slice (see its package doc), so the org id is the ONLY input
// and the answer is shared by every non-guest connection in the org; the guest
// case is not a narrower set but NO set, and lives at the hand-off
// (Hub.syncSpaceView) rather than in this SQL. Archival is deliberately not
// filtered: it is a lifecycle state, not an ACL state, and REST still reads an
// archived space's items — withholding here would over-withhold against the
// read path rather than close a hole.
//
// The uncorrelated EXISTS is Postgres's InitPlan (evaluated once, not per row)
// and rides the same round trip as the set.
//
// It returns a view only on complete success: a partial set published under an
// error would be a set that silently withholds real Spaces, and worse, one
// whose itemSecurityActive could be read off a truncated scan.
func loadSpaceView(ctx context.Context, pool *pgxpool.Pool, orgID int64) (*spaceView, error) {
	rows, err := pool.Query(ctx, `
		SELECT sp.id,
		       EXISTS (SELECT 1 FROM visibility_scope vs
		               JOIN space s2 ON s2.id = vs.space_id
		               WHERE s2.org_id = $1)
		FROM space sp
		WHERE sp.org_id = $1`, orgID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	// Fail closed on the way in: until a row proves otherwise the org behaves as
	// if item security were active. Zero rows leaves it true, which is moot — an
	// empty space set already withholds every space-scoped event.
	set, scoped := map[int64]bool{}, true
	for rows.Next() {
		var id int64
		var active bool
		if err := rows.Scan(&id, &active); err != nil {
			return nil, err
		}
		set[id] = true
		scoped = active
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return &spaceView{spaces: set, itemSecurityActive: scoped}, nil
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
