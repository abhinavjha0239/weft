package messaging

import (
	"context"
	"regexp"
	"strings"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

// Sidebar presentation prefs (P-14) wake the dormant channel_member.pinned /
// color columns (the C-4 quartet, 0003). They are PERSONAL, per-membership
// presentation — a SEPARATE concern from the /notification delivery knobs — and
// like read state are never event-logged and never shared: nothing reacts to
// them, clients read them back through ListChannels.

// sidebarColorRE is the accepted color form: a 6-digit hex triple, matched
// case-insensitively and stored lowercase. "" (handled before this) clears it.
var sidebarColorRE = regexp.MustCompile(`^#[0-9a-fA-F]{6}$`)

// SetSidebarPrefs replaces BOTH of the caller's own sidebar flags on a channel
// (PUT semantics — no partial merge; clients send current values). The
// membership gate lives INSIDE the UPDATE's WHERE (the "gates in the statement"
// invariant): the org-pinned channel join plus the caller's own live membership
// row (unsubscribed_at IS NULL). Zero rows affected → an oracle-free 404, so a
// non-member, an unsubscribed former member, and a foreign-org channel id are
// one indistinguishable "channel not found" — a sidebar PUT never confirms a
// channel exists. Archived channels are NOT excluded: flags ride along through
// rename/archive (a member still lists an archived channel).
func (s *Service) SetSidebarPrefs(ctx context.Context, actor auth.Identity, channelID int64, pinned bool, color string) error {
	var storedColor *string
	if color != "" {
		if !sidebarColorRE.MatchString(color) {
			return apperr.Invalid("color must be #rrggbb")
		}
		lc := strings.ToLower(color)
		storedColor = &lc
	}
	ct, err := s.pool.Exec(ctx, `
		UPDATE channel_member cm
		SET pinned = $1, color = $2
		FROM channel c
		WHERE cm.channel_id = c.id
		  AND cm.channel_id = $3
		  AND cm.user_id = $4
		  AND c.org_id = $5
		  AND cm.unsubscribed_at IS NULL`,
		pinned, storedColor, channelID, actor.UserID, actor.OrgID)
	if err != nil {
		return apperr.Internal("set sidebar prefs", err)
	}
	if ct.RowsAffected() == 0 {
		return apperr.NotFound("channel not found")
	}
	return nil
}
