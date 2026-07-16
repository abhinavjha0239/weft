package automation

// Slash commands let a user fire rules from a channel. The invocation records
// an automation.slash_invoked event; the runner then delivers it to every
// enabled slash rule whose command matches and whose scope covers the channel
// (org scope: any channel; channel scope: only its own). Multiple rules may
// match one command — OQ-AU12 stays open (recorded).

import (
	"context"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/internal/eventlog"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

const maxSlashTextRunes = 2000

// Slash authorizes and records a slash-command invocation. The gate is
// messaging's exported channel-send gate (send_message + membership + a live
// channel) — the EXACT authorization a normal post into that channel requires,
// never reimplemented here — so a non-member gets the same oracle-free denial
// a send would. The command's text is length-bounded and control-character
// stripped before it becomes attacker-influenceable {{event.text}} data.
func (s *Service) Slash(ctx context.Context, actor auth.Identity, command string, channelID int64, text string) error {
	if !slashCommandRe.MatchString(command) {
		return apperr.Invalid("command must match ^[a-z0-9][a-z0-9_-]{0,31}$")
	}
	if utf8.RuneCountInString(text) > maxSlashTextRunes {
		return apperr.Invalid("text must be at most 2000 characters")
	}
	text = stripControl(text)
	return db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.msg.RequireChannelSend(ctx, tx, actor, channelID); err != nil {
			return err
		}
		_, err := eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
			EntityType: enum.EntityChannel, EntityID: channelID, Verb: verbSlashInvoked,
			Payload: eventlog.MustPayload(map[string]any{
				"command": command, "channel_id": channelID,
				"user_id": actor.UserID, "text": text}),
		})
		return err
	})
}

// stripControl drops control characters (the P-29 sanitizer precedent),
// including newlines and tabs — slash text is a single free-text argument that
// flows into rendered posts, so control bytes are removed before storage.
func stripControl(s string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return -1
		}
		return r
	}, s)
}
