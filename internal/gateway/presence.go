package gateway

import (
	"context"
	"encoding/json"
	"sort"
)

// Presence is DERIVED state, not stored state: a user is online while they
// have at least one live gateway connection in this process. Transitions
// broadcast presence.changed on the ephemeral plane (seq=0, never
// event-logged); clients bootstrap from the REST snapshot and then apply
// signals. Scope note (REALITY.md): the registry is per gateway process —
// the single-node v1 slice; a shared presence plane arrives with multi-node
// cells.

// trackConnect increments the user's connection count and reports whether
// this is their FIRST live connection. Caller must hold h.mu.
func (h *Hub) trackConnect(orgID, userID int64) bool {
	if h.userConns == nil {
		h.userConns = map[int64]map[int64]int{}
	}
	if h.userConns[orgID] == nil {
		h.userConns[orgID] = map[int64]int{}
	}
	h.userConns[orgID][userID]++
	return h.userConns[orgID][userID] == 1
}

// trackDisconnect decrements and reports whether this was the user's LAST
// live connection. Caller must hold h.mu.
func (h *Hub) trackDisconnect(orgID, userID int64) bool {
	m := h.userConns[orgID]
	if m == nil {
		return false
	}
	m[userID]--
	if m[userID] > 0 {
		return false
	}
	delete(m, userID)
	if len(m) == 0 {
		delete(h.userConns, orgID)
	}
	return true
}

// broadcastPresence fans a presence transition to the whole org — presence
// is member-visible metadata, never content.
func (h *Hub) broadcastPresence(ctx context.Context, orgID, userID int64, state string) {
	payload, _ := json.Marshal(map[string]any{"user_id": userID, "state": state})
	h.fanEphemeral(ctx, orgID, Envelope{Type: "presence.changed", OrgID: orgID, Payload: payload},
		func(*client) bool { return true })
}

// OnlineUsers is the REST snapshot: user ids with at least one live
// connection in this org, sorted for stable output.
func (h *Hub) OnlineUsers(orgID int64) []int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	ids := make([]int64, 0, len(h.userConns[orgID]))
	for uid := range h.userConns[orgID] {
		ids = append(ids, uid)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// NotifyUser fans an ephemeral notification ping to one user's connections
// (the notification materializer's live path; the row is the durable truth).
func (h *Hub) NotifyUser(ctx context.Context, orgID, userID int64, payload json.RawMessage) {
	h.fanEphemeral(ctx, orgID, Envelope{Type: "notification.created", OrgID: orgID, Payload: payload},
		func(peer *client) bool { return peer.id.UserID == userID })
}
