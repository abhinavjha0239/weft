package gateway

import (
	"context"
	"encoding/json"

	"golang.org/x/time/rate"
)

// The ephemeral signal plane (ADR-002 P5): client→server frames on the same
// socket, fanned in memory to the right connections. Nothing here touches the
// event log or durable tables (read_marker persists via the messaging service,
// but its *sync* to other sessions is ephemeral). Ephemeral envelopes carry
// seq=0 — they are not resumable and clients must not advance last_id on them.
//
// Frames:
//   {"type":"typing","channel_id":N,"state":"start"|"stop"}
//   {"type":"typing","dm_space_id":N,"state":"start"|"stop"}
//   {"type":"read_marker","thread_id":N,"up_to":N}
//
// Fan-out:
//   typing      → org connections whose membership view contains the scope
//                 (channel members / DM participants), excluding every one
//                 of the sender's own connections
//   read_marker → durable MarkRead, then readstate.synced to the SAME USER's
//                 other connections (multi-device sync)
//
// Presence (online/offline) also lives on this plane but is server-derived
// from the connection registry — see presence.go.

// MarkReader is the durable read-state dependency (implemented by
// messaging.Service); an interface keeps gateway free of a domain import.
type MarkReader interface {
	MarkRead(ctx context.Context, actor Actor, threadID, upTo int64) (int64, error)
}

// Actor mirrors auth.Identity's fields to avoid the import knot; the rest
// layer adapts between them.
type Actor struct {
	UserID int64
	OrgID  int64
}

type clientFrame struct {
	Type      string `json:"type"`
	ChannelID int64  `json:"channel_id,omitempty"`
	DMSpaceID int64  `json:"dm_space_id,omitempty"`
	ThreadID  int64  `json:"thread_id,omitempty"`
	State     string `json:"state,omitempty"`
	UpTo      int64  `json:"up_to,omitempty"`
}

// handleClientFrame processes one inbound frame from c's socket. Errors are
// reported to the sender as an ephemeral error envelope, never a disconnect.
func (h *Hub) handleClientFrame(ctx context.Context, c *client, data []byte) {
	if !c.frameLimit.Allow() {
		h.sendEphemeral(ctx, c, "error", map[string]any{"message": "signal rate limit exceeded"})
		return
	}
	var f clientFrame
	if err := json.Unmarshal(data, &f); err != nil {
		h.sendEphemeral(ctx, c, "error", map[string]any{"message": "malformed frame"})
		return
	}
	switch f.Type {
	case "typing":
		h.handleTyping(ctx, c, f)
	case "read_marker":
		h.handleReadMarker(ctx, c, f)
	default:
		h.sendEphemeral(ctx, c, "error", map[string]any{"message": "unknown frame type"})
	}
}

func (h *Hub) handleTyping(ctx context.Context, c *client, f clientFrame) {
	verb := "typing.started"
	if f.State == "stop" {
		verb = "typing.stopped"
	}
	// Senders may only signal into scopes their own membership view contains
	// — the same ACL surface the read side uses; targets are gated by THEIR
	// view. Exclude ALL of the sender's own connections: a user never sees
	// their own typing indicator on another device.
	switch {
	case f.ChannelID != 0:
		if !c.channels[f.ChannelID] {
			return
		}
		payload, _ := json.Marshal(map[string]any{
			"channel_id": f.ChannelID, "user_id": c.id.UserID,
		})
		h.fanEphemeral(ctx, c.id.OrgID, Envelope{Type: verb, OrgID: c.id.OrgID, Payload: payload},
			func(peer *client) bool {
				return peer.id.UserID != c.id.UserID && peer.channels[f.ChannelID]
			})
	case f.DMSpaceID != 0:
		if !c.dms[f.DMSpaceID] {
			return
		}
		payload, _ := json.Marshal(map[string]any{
			"dm_space_id": f.DMSpaceID, "user_id": c.id.UserID,
		})
		h.fanEphemeral(ctx, c.id.OrgID, Envelope{Type: verb, OrgID: c.id.OrgID, Payload: payload},
			func(peer *client) bool {
				return peer.id.UserID != c.id.UserID && peer.dms[f.DMSpaceID]
			})
	}
}

func (h *Hub) handleReadMarker(ctx context.Context, c *client, f clientFrame) {
	if h.markReader == nil || f.ThreadID == 0 {
		return
	}
	applied, err := h.markReader.MarkRead(ctx, Actor{UserID: c.id.UserID, OrgID: c.id.OrgID},
		f.ThreadID, f.UpTo)
	if err != nil {
		h.sendEphemeral(ctx, c, "error", map[string]any{"message": "mark read failed"})
		return
	}
	payload, _ := json.Marshal(map[string]any{
		"thread_id": f.ThreadID, "last_read_message_id": applied,
	})
	// Multi-device sync: the same user's OTHER connections only.
	h.fanEphemeral(ctx, c.id.OrgID, Envelope{Type: "readstate.synced", OrgID: c.id.OrgID, Payload: payload},
		func(peer *client) bool {
			return peer != c && peer.id.UserID == c.id.UserID
		})
}

// fanEphemeral delivers an envelope to the org's connections passing filter.
// coder/websocket serializes concurrent writers, so writing from the sender's
// reader goroutine is safe alongside the pump goroutine.
func (h *Hub) fanEphemeral(ctx context.Context, orgID int64, e Envelope, filter func(*client) bool) {
	h.mu.Lock()
	targets := make([]*client, 0, len(h.conns[orgID]))
	for peer := range h.conns[orgID] {
		if filter(peer) {
			targets = append(targets, peer)
		}
	}
	h.mu.Unlock()
	for _, peer := range targets {
		// Best effort: a slow/dead peer fails its own connection, not the fan.
		_ = h.send(ctx, peer, e)
	}
}

func (h *Hub) sendEphemeral(ctx context.Context, c *client, typ string, payload map[string]any) {
	p, _ := json.Marshal(payload)
	_ = h.send(ctx, c, Envelope{Type: typ, OrgID: c.id.OrgID, Payload: p})
}

// signalRate bounds inbound frames per connection (typing storms, abuse).
func newFrameLimiter() *rate.Limiter { return rate.NewLimiter(10, 20) }
