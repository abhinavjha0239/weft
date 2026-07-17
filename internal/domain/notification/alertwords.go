package notification

import (
	"context"
	"strings"
	"unicode"

	"github.com/jackc/pgx/v5"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

// Alert words (N-1 kind 4): per-user, stored LOWERCASE (matching is
// case-insensitive; the runner's SQL prefilter relies on stored-lower),
// replace-set semantics — the client sends the whole list, idempotently.

const (
	maxAlertWords   = 50
	maxAlertWordLen = 50
)

// SetAlertWords replaces the actor's word list. Words are trimmed and
// lowercased; duplicates collapse; empties, one-char words, and control
// characters are rejected (one bad word fails the whole set — the client
// sent a list, it gets a clear answer about it).
func (s *Service) SetAlertWords(ctx context.Context, actor auth.Identity, words []string) ([]string, error) {
	if len(words) > maxAlertWords {
		return nil, apperr.Invalid("too many alert words (max 50)")
	}
	seen := map[string]bool{}
	clean := make([]string, 0, len(words))
	for _, w := range words {
		w = strings.ToLower(strings.TrimSpace(w))
		if len(w) < 2 || len(w) > maxAlertWordLen {
			return nil, apperr.Invalid("alert words must be 2..50 characters")
		}
		for _, r := range w {
			if unicode.IsControl(r) {
				return nil, apperr.Invalid("alert words must not contain control characters")
			}
		}
		if !seen[w] {
			seen[w] = true
			clean = append(clean, w)
		}
	}
	err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx,
			`DELETE FROM alert_word WHERE user_id = $1`, actor.UserID); err != nil {
			return apperr.Internal("clear alert words", err)
		}
		for _, w := range clean {
			if _, err := tx.Exec(ctx,
				`INSERT INTO alert_word (user_id, word) VALUES ($1, $2)`,
				actor.UserID, w); err != nil {
				return apperr.Internal("store alert word", err)
			}
		}
		// F-17: the word list defines the alert-word reason (3) across every
		// channel the user is in — resync in the same tx.
		if err := s.deliv.PatchAlertWords(ctx, tx, actor.OrgID, actor.UserID); err != nil {
			return apperr.Internal("patch deliverability", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return clean, nil
}

// ListAlertWords returns the actor's words, alphabetical.
func (s *Service) ListAlertWords(ctx context.Context, actor auth.Identity) ([]string, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT word FROM alert_word WHERE user_id = $1 ORDER BY word`, actor.UserID)
	if err != nil {
		return nil, apperr.Internal("list alert words", err)
	}
	defer rows.Close()
	out := []string{}
	for rows.Next() {
		var w string
		if err := rows.Scan(&w); err != nil {
			return nil, apperr.Internal("scan alert word", err)
		}
		out = append(out, w)
	}
	return out, rows.Err()
}
