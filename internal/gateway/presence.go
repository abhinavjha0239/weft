package gateway

import (
	"context"
	"encoding/json"
	"time"
)

// Presence is DERIVED state, not stored state: a user is connected while they
// have at least one live gateway connection in this process, and within that
// they are "active" or "idle" (P-05) by how recently they last signalled.
// Transitions broadcast presence.changed on the ephemeral plane (seq=0, never
// event-logged) with state "active"|"idle"|"offline"; clients bootstrap from
// the REST snapshot and then apply signals. Scope note (REALITY.md): the
// registry is per gateway process — the single-node v1 slice; a shared
// presence plane arrives with multi-node cells.

// userPresence is a connected user's derived state: how many live connections
// they hold, when they last signalled (any inbound frame or a fresh
// connection), and whether they have been demoted to idle. Presence stays in
// process memory — the UNLOGGED presence table is deliberately unused.
type userPresence struct {
	conns      int
	lastActive time.Time
	idle       bool
}

// trackConnect registers a new connection for the user and reports whether a
// presence.changed "active" should be broadcast — true when this is their
// FIRST connection, or a reconnect that finds them idle (a reconnect resets
// the idle timer). Caller must hold h.mu.
func (h *Hub) trackConnect(orgID, userID int64, now time.Time) (announce bool) {
	if h.userConns == nil {
		h.userConns = map[int64]map[int64]*userPresence{}
	}
	if h.userConns[orgID] == nil {
		h.userConns[orgID] = map[int64]*userPresence{}
	}
	p := h.userConns[orgID][userID]
	if p == nil {
		h.userConns[orgID][userID] = &userPresence{conns: 1, lastActive: now}
		return true
	}
	p.conns++
	wasIdle := p.idle
	p.idle = false
	p.lastActive = now
	return wasIdle
}

// trackDisconnect decrements and reports whether this was the user's LAST live
// connection (→ offline). Caller must hold h.mu.
func (h *Hub) trackDisconnect(orgID, userID int64) bool {
	m := h.userConns[orgID]
	if m == nil {
		return false
	}
	p := m[userID]
	if p == nil {
		return false
	}
	p.conns--
	if p.conns > 0 {
		return false
	}
	delete(m, userID)
	if len(m) == 0 {
		delete(h.userConns, orgID)
	}
	return true
}

// markActiveLocked records activity for the user and reports whether it
// promoted them from idle back to active (the only transition worth a
// broadcast; a user already active just has their timer refreshed). Multi-
// device: activity on ANY connection makes the user active. Caller holds h.mu.
func (h *Hub) markActiveLocked(orgID, userID int64, now time.Time) (promoted bool) {
	m := h.userConns[orgID]
	if m == nil {
		return false
	}
	p := m[userID]
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
	h.mu.Lock()
	promoted := h.markActiveLocked(c.id.OrgID, c.id.UserID, time.Now())
	h.mu.Unlock()
	if promoted {
		h.broadcastPresence(ctx, c.id.OrgID, c.id.UserID, "active")
	}
}

// presenceSweep demotes silent-but-connected users active→idle once they pass
// IdleAfter. One ticker per hub; the transition list is copied UNDER the lock
// and fanned AFTER releasing it, so the sweep never broadcasts while holding
// h.mu. The interval tracks IdleAfter (so a tiny test threshold is observed
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
			h.mu.Lock()
			for orgID, users := range h.userConns {
				for userID, p := range users {
					if !p.idle && now.Sub(p.lastActive) > h.IdleAfter {
						p.idle = true
						demoted = append(demoted, transition{orgID, userID})
					}
				}
			}
			h.mu.Unlock()
			for _, d := range demoted {
				h.broadcastPresence(ctx, d.orgID, d.userID, "idle")
			}
		}
	}
}

// broadcastPresence fans a presence transition to the whole org — presence is
// member-visible metadata, never content.
func (h *Hub) broadcastPresence(ctx context.Context, orgID, userID int64, state string) {
	payload, _ := json.Marshal(map[string]any{"user_id": userID, "state": state})
	h.fanEphemeral(ctx, orgID, Envelope{Type: "presence.changed", OrgID: orgID, Payload: payload},
		func(*client) bool { return true })
}

// PresenceSnapshot is the REST bootstrap: every connected user in the org
// mapped to their current state ("active" or "idle"); offline users are
// absent. Clients read it once, then apply presence.changed signals.
func (h *Hub) PresenceSnapshot(orgID int64) map[int64]string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make(map[int64]string, len(h.userConns[orgID]))
	for uid, p := range h.userConns[orgID] {
		state := "active"
		if p.idle {
			state = "idle"
		}
		out[uid] = state
	}
	return out
}

// NotifyUser fans an ephemeral notification ping to one user's connections
// (the notification materializer's live path; the row is the durable truth).
func (h *Hub) NotifyUser(ctx context.Context, orgID, userID int64, payload json.RawMessage) {
	h.fanEphemeral(ctx, orgID, Envelope{Type: "notification.created", OrgID: orgID, Payload: payload},
		func(peer *client) bool { return peer.id.UserID == userID })
}
