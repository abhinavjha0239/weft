package gateway

import (
	"context"
	"encoding/json"
	"time"

	"github.com/abhinavjha0239/weft/internal/platform/presence"
)

// Presence is DERIVED state, not stored state: a user is "active" or "idle"
// (P-05) while they hold at least one live gateway connection on ANY node — by
// how recently they last signalled — and "offline" once their last connection
// across ALL nodes drops. Each node keeps a per-process registry of its OWN
// connections; transitions publish to the shared presence plane
// (platform/presence, S5), and every node subscribes and fans presence.changed
// on the ephemeral plane (seq=0, never event-logged — ADR-002 P5) to its own
// connections. Clients bootstrap from the REST snapshot — now the org-WIDE view
// read from the plane, not just this process — and then apply signals.

// userPresence is a connected user's derived state ON THIS NODE: how many live
// connections they hold here, when they last signalled (any inbound frame or a
// fresh connection), and whether they have been demoted to idle. This per-node
// registry is the liveness source for this node's own connections; the shared
// presence plane aggregates it across the cell (the UNLOGGED presence table
// backs the pg driver).
type userPresence struct {
	conns      int
	lastActive time.Time
	idle       bool
}

// trackConnect registers a new connection for the user and reports whether a
// presence.changed "active" should be broadcast — true when this is their
// FIRST connection, or a reconnect that finds them idle (a reconnect resets
// the idle timer). Caller must hold sh.mu.
func (sh *orgShard) trackConnect(userID int64, now time.Time) (announce bool) {
	p := sh.userConns[userID]
	if p == nil {
		sh.userConns[userID] = &userPresence{conns: 1, lastActive: now}
		return true
	}
	p.conns++
	wasIdle := p.idle
	p.idle = false
	p.lastActive = now
	return wasIdle
}

// trackDisconnect decrements and reports whether this was the user's LAST live
// connection (→ offline). Caller must hold sh.mu. The shard itself is dropped
// from the orgs map by deregister once its last connection leaves.
func (sh *orgShard) trackDisconnect(userID int64) bool {
	p := sh.userConns[userID]
	if p == nil {
		return false
	}
	p.conns--
	if p.conns > 0 {
		return false
	}
	delete(sh.userConns, userID)
	return true
}

// markActiveLocked records activity for the user and reports whether it
// promoted them from idle back to active (the only transition worth a
// broadcast; a user already active just has their timer refreshed). Multi-
// device: activity on ANY connection makes the user active. Caller holds sh.mu.
func (sh *orgShard) markActiveLocked(userID int64, now time.Time) (promoted bool) {
	p := sh.userConns[userID]
	if p == nil {
		return false
	}
	p.lastActive = now
	if p.idle {
		p.idle = false
		return true
	}
	return false
}

// recordActivity is called for EVERY inbound client frame (typing, read
// marker, the explicit "active" ping, or anything else): it refreshes the
// user's last-activity time and, if they were idle, fans the promotion back
// to active. The lock is released before broadcasting — the fan never holds
// h.mu.
func (h *Hub) recordActivity(ctx context.Context, c *client) {
	sh := c.shard
	sh.mu.Lock()
	promoted := sh.markActiveLocked(c.id.UserID, time.Now())
	sh.mu.Unlock()
	if promoted {
		h.publishPresence(ctx, c.id.OrgID, c.id.UserID, "active")
	}
}

// presenceSweep demotes silent-but-connected users active→idle once they pass
// IdleAfter. One ticker per hub; the transition list is copied UNDER the lock
// and fanned AFTER releasing it, so the sweep never broadcasts while holding
// sh.mu. The interval tracks IdleAfter (so a tiny test threshold is observed
// promptly) but is capped at a minute for the 10-minute production default.
func (h *Hub) presenceSweep(ctx context.Context) {
	interval := h.IdleAfter / 4
	if interval > time.Minute {
		interval = time.Minute
	}
	if interval <= 0 {
		interval = time.Millisecond
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	type transition struct{ orgID, userID int64 }
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			now := time.Now()
			var demoted []transition
			// Per-shard so the sweep never holds a global lock across every
			// org; the transition list is copied under each shard lock and
			// fanned only after releasing it (the fan never holds sh.mu).
			for _, sh := range h.shards() {
				sh.mu.Lock()
				for userID, p := range sh.userConns {
					if !p.idle && now.Sub(p.lastActive) > h.IdleAfter {
						p.idle = true
						demoted = append(demoted, transition{sh.orgID, userID})
					}
				}
				sh.mu.Unlock()
			}
			for _, d := range demoted {
				h.publishPresence(ctx, d.orgID, d.userID, "idle")
			}
		}
	}
}

// publishPresence puts a derived-state transition on the shared plane instead
// of fanning it directly: every node (this one included) then receives the
// delta via Subscribe and fans it to ITS OWN local connections. This is what
// makes presence correct across nodes — a transition on one node reaches
// observers on every node, and PresenceSnapshot reads the org-wide store rather
// than one process's memory.
func (h *Hub) publishPresence(ctx context.Context, orgID, userID int64, state string) {
	if err := h.plane.Publish(ctx, presence.Delta{OrgID: orgID, UserID: userID, State: state}); err != nil {
		h.log.Warn("gateway: presence publish failed",
			"org", orgID, "user", userID, "state", state, "err", err)
	}
}

// onPresenceDelta is the single subscriber the hub registers on the plane
// (Run): it fans one delta to THIS node's local connections. Cross-node
// last-writer-wins: an "offline" delta means SOME node dropped a user's last
// LOCAL connection, but a user with a live connection on ANY node is still
// present — so if this node still holds a connection for the user, the offline
// is premature and we re-publish our live state (which, arriving after the
// offline, wins). Offline only sticks when NO node re-asserts, mirroring
// presence.go's "offline at zero connections", now distributed across the cell.
func (h *Hub) onPresenceDelta(ctx context.Context, d presence.Delta) {
	if d.State == "offline" {
		if state, ok := h.localState(d.OrgID, d.UserID); ok {
			h.publishPresence(ctx, d.OrgID, d.UserID, state)
			return
		}
	}
	h.broadcastPresence(ctx, d.OrgID, d.UserID, d.State)
}

// localState reports a user's derived state on THIS node (active/idle) and
// whether the node holds any live connection for them — the local-liveness
// check behind onPresenceDelta's cross-node last-writer-wins rule.
func (h *Hub) localState(orgID, userID int64) (string, bool) {
	sh := h.shard(orgID)
	if sh == nil {
		return "", false
	}
	sh.mu.Lock()
	defer sh.mu.Unlock()
	p := sh.userConns[userID]
	if p == nil {
		return "", false
	}
	if p.idle {
		return "idle", true
	}
	return "active", true
}

// broadcastPresence fans a presence transition to THIS node's org connections —
// presence is member-visible metadata, never content. Called only from
// onPresenceDelta, so every fan (local or from another node) flows through the
// plane: the fan touches O(local conns) on this node, never the cell-wide set.
func (h *Hub) broadcastPresence(ctx context.Context, orgID, userID int64, state string) {
	payload, _ := json.Marshal(map[string]any{"user_id": userID, "state": state})
	h.fanEphemeral(ctx, orgID, Envelope{Type: "presence.changed", OrgID: orgID, Payload: payload},
		func(*client) bool { return true })
}

// PresenceSnapshot is the REST bootstrap: every active/idle user in the ORG
// mapped to their state (offline absent). Sourced from the shared plane, so it
// is the org-WIDE view across every gateway node on the cell — not just the
// users connected to this process. Clients read it once, then apply
// presence.changed signals.
func (h *Hub) PresenceSnapshot(orgID int64) map[int64]string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	snap, err := h.plane.Snapshot(ctx, orgID)
	if err != nil {
		h.log.Warn("gateway: presence snapshot failed", "org", orgID, "err", err)
		return map[int64]string{}
	}
	return snap
}

// NotifyUser fans an ephemeral notification ping to one user's connections
// (the notification materializer's live path; the row is the durable truth).
func (h *Hub) NotifyUser(ctx context.Context, orgID, userID int64, payload json.RawMessage) {
	h.fanEphemeral(ctx, orgID, Envelope{Type: "notification.created", OrgID: orgID, Payload: payload},
		func(peer *client) bool { return peer.id.UserID == userID })
}
