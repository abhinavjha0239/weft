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

	"github.com/abhinavjha0239/weft/internal/auth"
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
	lastID int64
	wake   chan struct{}
	// Channel-membership view for ACL filtering; refreshed when a membership
	// event for this user arrives (F-2 replay rule, M0 slice).
	channels map[int64]bool
}

type Hub struct {
	pool *pgxpool.Pool
	log  *slog.Logger

	mu    sync.Mutex
	conns map[int64]map[*client]struct{} // orgID → clients
}

func NewHub(pool *pgxpool.Pool, log *slog.Logger) *Hub {
	return &Hub{pool: pool, log: log, conns: map[int64]map[*client]struct{}{}}
}

// Run is the dispatcher: LISTEN on the event-log channel and wake that org's
// connections; a slow sweep covers missed notifications. Blocks until ctx ends.
func (h *Hub) Run(ctx context.Context) {
	go h.sweep(ctx)
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
			h.mu.Lock()
			for orgID := range h.conns {
				h.wakeOrgLocked(orgID)
			}
			h.mu.Unlock()
		}
	}
}

func (h *Hub) wakeOrg(orgID int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.wakeOrgLocked(orgID)
}

func (h *Hub) wakeOrgLocked(orgID int64) {
	for c := range h.conns[orgID] {
		select {
		case c.wake <- struct{}{}:
		default: // already signaled
		}
	}
}

// Serve upgrades the request and streams events. ?last_id=N resumes.
func (h *Hub) Serve(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	lastID, _ := strconv.ParseInt(r.URL.Query().Get("last_id"), 10, 64)
	ws, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	c := &client{conn: ws, id: id, lastID: lastID, wake: make(chan struct{}, 1)}

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	// The client never sends application frames (writes are REST, ADR-002 P3),
	// but reading is how we learn about disconnects promptly instead of at the
	// next checkpoint write.
	go func() {
		for {
			if _, _, err := ws.Read(ctx); err != nil {
				cancel()
				return
			}
		}
	}()
	if err := c.loadChannels(ctx, h.pool); err != nil {
		ws.Close(websocket.StatusInternalError, "membership load failed")
		return
	}

	h.mu.Lock()
	if h.conns[id.OrgID] == nil {
		h.conns[id.OrgID] = map[*client]struct{}{}
	}
	h.conns[id.OrgID][c] = struct{}{}
	h.mu.Unlock()
	defer func() {
		h.mu.Lock()
		delete(h.conns[id.OrgID], c)
		if len(h.conns[id.OrgID]) == 0 {
			delete(h.conns, id.OrgID)
		}
		h.mu.Unlock()
		ws.CloseNow()
	}()

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

// filter applies the M0 ACL slice: channel-scoped events require membership;
// events about this user's own membership trigger a view refresh.
func (c *client) filter(verb string, payload json.RawMessage) (deliver, refresh bool) {
	var p struct {
		ChannelID int64 `json:"channel_id"`
		UserID    int64 `json:"user_id"`
	}
	_ = json.Unmarshal(payload, &p)
	if (verb == "member.joined" || verb == "member.left") && p.UserID == c.id.UserID {
		refresh = true
	}
	if p.ChannelID != 0 {
		return c.channels[p.ChannelID], refresh
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
	return rows.Err()
}

func (h *Hub) send(ctx context.Context, c *client, e Envelope) error {
	data, err := json.Marshal(e)
	if err != nil {
		return err
	}
	wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return c.conn.Write(wctx, websocket.MessageText, data)
}
