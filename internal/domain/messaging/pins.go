package messaging

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/internal/eventlog"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

// Channel pins (P-02b). Re-specced to MATCH the 0004 schema: the pin table is
// channel-scoped (channel_id NOT NULL), so pins are CHANNEL messages only —
// DM/space-thread pins would need a schema change, deferred until the fusion
// UX needs them. Unlike saved items (personal), a pin is SHARED per-container
// curation: it is event-logged (message.pinned/message.unpinned, entity =
// message, channel_id in the payload so the gateway routes it to members) and
// gated like moderation-lite — the actor must pass the three-way read ACL AND
// hold administer_channel on the message's channel. Cap: 50 per channel.

const (
	// pinCap bounds a channel's pins (Slack is 100; we choose 50, cheap to
	// raise). The insert-then-count-under-lock path keeps it exact.
	pinCap = 50
	// pinExcerptLen matches the saved-items preview length (reuses excerpt()).
	pinExcerptLen = 160
)

// PinnedMessage is one entry in a channel's pin list, newest-pinned first.
type PinnedMessage struct {
	MessageID int64     `json:"message_id"`
	AuthorID  int64     `json:"author_id"`
	Excerpt   string    `json:"excerpt"`
	PinnedBy  int64     `json:"pinned_by"`
	PinnedAt  time.Time `json:"pinned_at"`
}

// PinMessage pins a channel message. Idempotent: a second pin of an already
// pinned message changes nothing and emits nothing (RowsAffected gate, even
// at the cap). A message the actor cannot READ is an oracle-free 404; a
// DM/space message is a 400; lacking administer_channel is a 403; a channel
// already at the cap is a 409.
func (s *Service) PinMessage(ctx context.Context, actor auth.Identity, msgID int64) error {
	return s.togglePin(ctx, actor, msgID, true)
}

// UnpinMessage removes a channel message's pin. Idempotent ensure-absent twin:
// unpinning something not pinned succeeds and emits nothing.
func (s *Service) UnpinMessage(ctx context.Context, actor auth.Identity, msgID int64) error {
	return s.togglePin(ctx, actor, msgID, false)
}

func (s *Service) togglePin(ctx context.Context, actor auth.Identity, msgID int64, want bool) error {
	return db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		// Read gate first (oracle-free): the same three-way container rule as a
		// fetch/reaction. A deleted or unreadable message is 404, so pinning
		// never confirms a private message exists.
		_, channelID, _, err := s.loadReactable(ctx, tx, actor, msgID)
		if err != nil {
			return err
		}
		// The actor CAN see it; only now is a shape mismatch safe to reveal.
		if channelID == nil {
			return apperr.Invalid("pins are for channel messages")
		}
		// Pinning is a channel-admin action, beyond read.
		chain, err := s.perms.ChannelScope(ctx, tx, actor.OrgID, *channelID)
		if err != nil {
			return err
		}
		if err := s.perms.Require(ctx, tx, actor, perms.VerbAdministerChannel, chain); err != nil {
			return err
		}

		var ct pgconn.CommandTag
		if want {
			// Serialize pins on this channel so the cap is exact under
			// concurrency: lock the row first, then insert + re-count (the
			// documented cap+lock discipline). Admin action — contention is
			// negligible.
			if _, err := tx.Exec(ctx,
				`SELECT 1 FROM channel WHERE id = $1 FOR UPDATE`, *channelID); err != nil {
				return apperr.Internal("lock channel", err)
			}
			ct, err = tx.Exec(ctx, `
				INSERT INTO pin (channel_id, message_id, pinned_by)
				VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`,
				*channelID, msgID, actor.UserID)
			if err != nil {
				return apperr.Internal("insert pin", err)
			}
			// The cap binds only a NEW pin (a re-pin stays idempotent even at
			// the cap); the count includes the row just inserted.
			if ct.RowsAffected() > 0 {
				var count int
				if err := tx.QueryRow(ctx,
					`SELECT count(*) FROM pin WHERE channel_id = $1`, *channelID).Scan(&count); err != nil {
					return apperr.Internal("count pins", err)
				}
				if count > pinCap {
					return apperr.Conflict("pin limit reached (50 per channel)")
				}
			}
		} else {
			ct, err = tx.Exec(ctx, `
				DELETE FROM pin WHERE channel_id = $1 AND message_id = $2`,
				*channelID, msgID)
			if err != nil {
				return apperr.Internal("delete pin", err)
			}
		}

		// Only a real state change is event-logged (idempotent no-ops stay
		// silent — the reactions pattern).
		if ct.RowsAffected() > 0 {
			verb := "message.pinned"
			if !want {
				verb = "message.unpinned"
			}
			if _, err := eventlog.Append(ctx, tx, eventlog.Event{
				OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
				EntityType: enum.EntityMessage, EntityID: msgID, Verb: verb,
				Payload: eventlog.MustPayload(map[string]any{
					"message_id": msgID, "channel_id": *channelID}),
			}); err != nil {
				return apperr.Internal("append event", err)
			}
		}
		return nil
	})
}

// ListChannelPins lists a channel's pinned messages, newest-pinned first, with
// a ≤160-rune excerpt. Member-gated (requireMember): a guest sees pins only in
// their own channels, since the same membership gate governs — no role branch.
// Deleted messages are auto-dropped (the JOIN filters deleted_at); DeleteMessage
// also clears the pin row in the same tx, so the list and the cap stay honest.
func (s *Service) ListChannelPins(ctx context.Context, actor auth.Identity, channelID int64) ([]PinnedMessage, error) {
	out := []PinnedMessage{}
	err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.requireMember(ctx, tx, channelID, actor.UserID); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `
			SELECT p.message_id, m.author_id, m.source, p.pinned_by, p.pinned_at
			FROM pin p
			JOIN message m ON m.id = p.message_id AND m.org_id = $2
			                  AND m.deleted_at IS NULL
			WHERE p.channel_id = $1
			ORDER BY p.pinned_at DESC, p.message_id DESC`,
			channelID, actor.OrgID)
		if err != nil {
			return apperr.Internal("list pins", err)
		}
		defer rows.Close()
		for rows.Next() {
			var it PinnedMessage
			var source string
			if err := rows.Scan(&it.MessageID, &it.AuthorID, &source,
				&it.PinnedBy, &it.PinnedAt); err != nil {
				return apperr.Internal("scan pin", err)
			}
			it.Excerpt = excerpt(source, pinExcerptLen)
			out = append(out, it)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
