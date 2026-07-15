package notification

import (
	"context"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

// Mediums (notification_medium_pref.medium). In-app is structural — the
// row IS the badge and always lands (N-4: the badge accrues even when
// delivery is suppressed); prefs govern the mediums BEYOND it. Push is
// reserved until a push lane exists (the honest-rungs rule).
const (
	MediumInApp int16 = 1
	MediumEmail int16 = 2
)

// defaultEmailEnabled is the zero-rows matrix: email for DMs, mentions,
// and keywords (setting an alert word IS the opt-in — the user asked for
// that signal); quiet for follows and channel activity.
func defaultEmailEnabled(kind int16) bool {
	return kind == KindDM || kind == KindMention || kind == KindKeyword
}

// prefKinds are the settable reason classes.
var prefKinds = []int16{KindDM, KindMention, KindFollowedThread, KindKeyword, KindChannelActivity, KindAutomationFailure}

type MediumPref struct {
	Kind    int16 `json:"kind"`
	Medium  int16 `json:"medium"`
	Enabled bool  `json:"enabled"`
}

// ListMediumPrefs returns the actor's EFFECTIVE email matrix — stored
// overrides where they exist, defaults elsewhere.
func (s *Service) ListMediumPrefs(ctx context.Context, actor auth.Identity) ([]MediumPref, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT kind, enabled FROM notification_medium_pref
		WHERE user_id = $1 AND medium = $2`, actor.UserID, MediumEmail)
	if err != nil {
		return nil, apperr.Internal("list prefs", err)
	}
	defer rows.Close()
	stored := map[int16]bool{}
	for rows.Next() {
		var kind int16
		var enabled bool
		if err := rows.Scan(&kind, &enabled); err != nil {
			return nil, apperr.Internal("scan pref", err)
		}
		stored[kind] = enabled
	}
	if err := rows.Err(); err != nil {
		return nil, apperr.Internal("list prefs", err)
	}
	out := make([]MediumPref, 0, len(prefKinds))
	for _, kind := range prefKinds {
		enabled, ok := stored[kind]
		if !ok {
			enabled = defaultEmailEnabled(kind)
		}
		out = append(out, MediumPref{Kind: kind, Medium: MediumEmail, Enabled: enabled})
	}
	return out, nil
}

// SetMediumPref upserts one (kind, medium) knob for the actor. Only the
// email medium is settable in v1.
func (s *Service) SetMediumPref(ctx context.Context, actor auth.Identity, kind, medium int16, enabled bool) error {
	if medium != MediumEmail {
		return apperr.Invalid("only the email medium (2) is settable — in-app is structural, push is reserved")
	}
	valid := false
	for _, k := range prefKinds {
		if k == kind {
			valid = true
		}
	}
	if !valid {
		return apperr.Invalid("kind must be 1 (dm), 2 (mention), 3 (followed), 4 (keyword), 5 (channel activity), or 6 (automation failure)")
	}
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO notification_medium_pref (user_id, kind, medium, enabled)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (user_id, kind, medium) DO UPDATE SET enabled = EXCLUDED.enabled`,
		actor.UserID, kind, medium, enabled); err != nil {
		return apperr.Internal("set pref", err)
	}
	return nil
}
