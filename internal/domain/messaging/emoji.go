package messaging

import (
	"context"
	"errors"
	"regexp"

	"github.com/jackc/pgx/v5"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/internal/eventlog"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

// Custom emoji (P-06). Org-scoped registry backing reaction tokens: reactions
// already store arbitrary emoji tokens, so this list is how a client resolves
// a custom name to its image file. Create/delete are add_emoji-gated org
// config (event-logged); the image bytes ride files.Upload, and the
// custom_emoji.file_id FK guards them from GC (avatar-style). Delete is SOFT
// (deactivated_at), and the UNIQUE(org_id, name) index means a deactivated
// name STAYS reserved — re-creating it 409s (Zulip parity).

var emojiNameRE = regexp.MustCompile(`^[a-z0-9_]{2,32}$`)

// ValidEmojiName reports whether name is a legal custom-emoji token — lowercase
// alphanumerics and underscore, 2..32 runes. Exported so the transport can
// reject a bad name before spooling an upload it would only discard.
func ValidEmojiName(name string) bool { return emojiNameRE.MatchString(name) }

// Emoji is one live custom emoji: its name and the file backing its image.
type Emoji struct {
	Name   string `json:"name"`
	FileID int64  `json:"file_id"`
}

// CreateEmoji registers a custom emoji (add_emoji). fileID is a file already
// uploaded through the org-scoped image path. The name must match
// [a-z0-9_]{2,32} and be free across the org's WHOLE registry — a deactivated
// name is still taken, so a collision is a 409.
func (s *Service) CreateEmoji(ctx context.Context, actor auth.Identity, name string, fileID int64) (Emoji, error) {
	if !ValidEmojiName(name) {
		return Emoji{}, apperr.Invalid("emoji name must be 2-32 chars of a-z, 0-9, or _")
	}
	err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.perms.Require(ctx, tx, actor, perms.VerbAddEmoji, perms.OrgScope(actor.OrgID)); err != nil {
			return err
		}
		// The name is reserved across live AND deactivated rows (the unique
		// index); pre-check so a duplicate is a clean 409, not a tx abort.
		var taken bool
		if err := tx.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM custom_emoji WHERE org_id = $1 AND name = $2)`,
			actor.OrgID, name).Scan(&taken); err != nil {
			return apperr.Internal("emoji name check", err)
		}
		if taken {
			return apperr.Conflict("emoji name already in use")
		}
		var emojiID int64
		if err := tx.QueryRow(ctx, `
			INSERT INTO custom_emoji (org_id, name, file_id, author_id)
			VALUES ($1, $2, $3, $4) RETURNING id`,
			actor.OrgID, name, fileID, actor.UserID).Scan(&emojiID); err != nil {
			return apperr.Internal("create emoji", err)
		}
		if _, err := eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
			EntityType: enum.EntityEmoji, EntityID: emojiID, Verb: "emoji.created",
			Payload: eventlog.MustPayload(map[string]any{
				"emoji_id": emojiID, "name": name, "file_id": fileID}),
		}); err != nil {
			return apperr.Internal("append event", err)
		}
		return nil
	})
	if err != nil {
		return Emoji{}, err
	}
	return Emoji{Name: name, FileID: fileID}, nil
}

// ListEmoji returns the org's LIVE custom emoji (deactivated excluded), ordered
// by name. Any org member may read the registry — it is how reactions render.
func (s *Service) ListEmoji(ctx context.Context, actor auth.Identity) ([]Emoji, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT name, COALESCE(file_id, 0) FROM custom_emoji
		WHERE org_id = $1 AND deactivated_at IS NULL
		ORDER BY name`, actor.OrgID)
	if err != nil {
		return nil, apperr.Internal("list emoji", err)
	}
	defer rows.Close()
	out := []Emoji{}
	for rows.Next() {
		var e Emoji
		if err := rows.Scan(&e.Name, &e.FileID); err != nil {
			return nil, apperr.Internal("scan emoji", err)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// DeleteEmoji soft-deletes a live custom emoji (add_emoji): deactivated_at is
// set while the row and its file_id FK stay, so the name remains reserved and
// the image survives for historical reaction rendering. An unknown or
// already-deactivated name is a 404.
func (s *Service) DeleteEmoji(ctx context.Context, actor auth.Identity, name string) error {
	return db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.perms.Require(ctx, tx, actor, perms.VerbAddEmoji, perms.OrgScope(actor.OrgID)); err != nil {
			return err
		}
		var emojiID int64
		err := tx.QueryRow(ctx, `
			UPDATE custom_emoji SET deactivated_at = now()
			WHERE org_id = $1 AND name = $2 AND deactivated_at IS NULL
			RETURNING id`, actor.OrgID, name).Scan(&emojiID)
		if errors.Is(err, pgx.ErrNoRows) {
			return apperr.NotFound("emoji not found")
		}
		if err != nil {
			return apperr.Internal("delete emoji", err)
		}
		if _, err := eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
			EntityType: enum.EntityEmoji, EntityID: emojiID, Verb: "emoji.deleted",
			Payload: eventlog.MustPayload(map[string]any{"emoji_id": emojiID, "name": name}),
		}); err != nil {
			return apperr.Internal("append event", err)
		}
		return nil
	})
}
