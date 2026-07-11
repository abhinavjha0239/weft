package identity

import (
	"context"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

// User status (ADR-011 N-3) is a durable object SEPARATE from presence: an
// emoji + text + optional "clear after" expiry the user sets by hand.
// Presence is the derived active/idle signal on the ephemeral plane; status
// is profile-adjacent state the identity module owns. This slice writes only
// kind=1 (manual). Kinds 2-5 (in-call/focus/ooo/after-hours) are the auto
// kinds the system sets once those lanes exist — the API never exposes kind,
// so a manual set always lands as kind 1. Status changes are NOT event-logged
// (per-user ephemera, the read-state precedent).

const (
	maxStatusEmojiLen = 32
	maxStatusTextLen  = 120
)

// SetStatus upserts the actor's own manual status. At least one of emoji or
// text must be present; an emoji is a short whitespace/control-free token;
// text is length-capped and control-free; an expiry, if given, must be in the
// future. Any prior auto-status is overwritten as a manual (kind 1) row.
func (s *Service) SetStatus(ctx context.Context, actor auth.Identity, emoji, text string, expiresAt *time.Time) error {
	emoji = strings.TrimSpace(emoji)
	text = strings.TrimSpace(text)
	if emoji == "" && text == "" {
		return apperr.Invalid("at least one of emoji or status_text is required")
	}
	if emoji != "" {
		if utf8.RuneCountInString(emoji) > maxStatusEmojiLen {
			return apperr.Invalid("emoji must be at most 32 characters")
		}
		for _, r := range emoji {
			if unicode.IsSpace(r) || unicode.IsControl(r) {
				return apperr.Invalid("emoji must not contain whitespace or control characters")
			}
		}
	}
	if utf8.RuneCountInString(text) > maxStatusTextLen {
		return apperr.Invalid("status_text must be at most 120 characters")
	}
	for _, r := range text {
		if unicode.IsControl(r) {
			return apperr.Invalid("status_text must not contain control characters")
		}
	}
	if expiresAt != nil && !expiresAt.After(time.Now()) {
		return apperr.Invalid("expires_at must be in the future")
	}
	var emojiArg *string
	if emoji != "" {
		emojiArg = &emoji
	}
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO user_status (user_id, emoji, status_text, kind, expires_at)
		VALUES ($1, $2, $3, 1, $4)
		ON CONFLICT (user_id) DO UPDATE
		  SET emoji = EXCLUDED.emoji, status_text = EXCLUDED.status_text,
		      kind = 1, expires_at = EXCLUDED.expires_at`,
		actor.UserID, emojiArg, text, expiresAt); err != nil {
		return apperr.Internal("set status", err)
	}
	return nil
}

// ClearStatus removes the actor's own status row.
func (s *Service) ClearStatus(ctx context.Context, actor auth.Identity) error {
	if _, err := s.pool.Exec(ctx,
		`DELETE FROM user_status WHERE user_id = $1`, actor.UserID); err != nil {
		return apperr.Internal("clear status", err)
	}
	return nil
}
