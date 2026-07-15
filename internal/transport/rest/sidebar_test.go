package rest

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/domain/messaging"
)

// TestSidebarPrefs: P-14 wakes the dormant channel_member.pinned/color columns.
// PUT /channels/{id}/sidebar replaces the caller's own pair (PUT-replace, not
// merge), surfaced back through ListChannels; the membership gate lives in the
// UPDATE's WHERE so a non-member, an unsubscribed former member, and a
// foreign-org channel are one oracle-free 404. Flags are personal (per-user
// independent, never shared) and guests may pin their own channels.
func TestSidebarPrefs(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		cancel()
		t.Fatalf("connect: %v", err)
	}
	defer func() { cancel(); pool.Close() }()
	resetAndMigrate(t, ctx, pool)
	ts := newIdentityServer(t, ctx, pool)

	var boot struct {
		OrgID     int64  `json:"org_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "sb", "email": "alice@sb.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)

	// A foreign org + channel for the cross-org 404.
	var other struct {
		ChannelID int64 `json:"channel_id"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "sbother", "email": "z@sbother.test", "password": "password123",
	}, &other)

	// A second channel alice owns; bob will NOT join it (same-org non-member).
	var priv struct {
		ChannelID int64 `json:"channel_id"`
	}
	postJSON(t, ts.URL+"/api/v1/channels", boot.Token,
		map[string]any{"name": "private2", "private": true}, &priv)

	setSidebar := func(token string, chID int64, pinned bool, color string) int {
		t.Helper()
		return putJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/sidebar", ts.URL, chID), token,
			map[string]any{"pinned": pinned, "color": color})
	}
	channelFromList := func(token string, chID int64) messaging.ChannelSummary {
		t.Helper()
		var out struct {
			Channels []messaging.ChannelSummary `json:"channels"`
		}
		if code := getJSON(t, ts.URL+"/api/v1/channels", token, &out); code != http.StatusOK {
			t.Fatalf("list channels = %d", code)
		}
		for _, c := range out.Channels {
			if c.ID == chID {
				return c
			}
		}
		t.Fatalf("channel %d absent from the caller's channel list", chID)
		return messaging.ChannelSummary{}
	}

	// Set pin + color; ListChannels reflects both, color normalized lowercase.
	if code := setSidebar(boot.Token, boot.ChannelID, true, "#AABBCC"); code != http.StatusOK {
		t.Fatalf("set sidebar = %d, want 200", code)
	}
	if c := channelFromList(boot.Token, boot.ChannelID); !c.Pinned || c.Color != "#aabbcc" {
		t.Fatalf("after set: pinned=%v color=%q, want true #aabbcc (lowercased)", c.Pinned, c.Color)
	}

	// Clear color with "" (keep pinned); list shows "".
	if code := setSidebar(boot.Token, boot.ChannelID, true, ""); code != http.StatusOK {
		t.Fatalf("clear color = %d, want 200", code)
	}
	if c := channelFromList(boot.Token, boot.ChannelID); !c.Pinned || c.Color != "" {
		t.Fatalf("after clear: pinned=%v color=%q, want true and empty", c.Pinned, c.Color)
	}

	// Bad colors 400.
	for _, bad := range []string{"red", "#12345", "#gggggg"} {
		if code := setSidebar(boot.Token, boot.ChannelID, true, bad); code != http.StatusBadRequest {
			t.Fatalf("bad color %q = %d, want 400", bad, code)
		}
	}

	// bob joins #general; his flags are INDEPENDENT of alice's (personal state).
	bobTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"bob@sb.test", "Bob Ray", "bobsbtok")
	if code := setSidebar(bobTok, boot.ChannelID, true, "#112233"); code != http.StatusOK {
		t.Fatalf("bob set = %d, want 200", code)
	}
	if c := channelFromList(bobTok, boot.ChannelID); !c.Pinned || c.Color != "#112233" {
		t.Fatalf("bob's #general = pinned %v color %q, want true #112233", c.Pinned, c.Color)
	}
	if c := channelFromList(boot.Token, boot.ChannelID); !c.Pinned || c.Color != "" {
		t.Fatalf("alice's #general after bob's set = pinned %v color %q, want true+empty (independent)", c.Pinned, c.Color)
	}

	// Same-org non-member (bob not in #private2) and foreign-org channel are the
	// same oracle-free 404.
	if code := setSidebar(bobTok, priv.ChannelID, true, ""); code != http.StatusNotFound {
		t.Fatalf("non-member sidebar = %d, want 404", code)
	}
	if code := setSidebar(boot.Token, other.ChannelID, true, ""); code != http.StatusNotFound {
		t.Fatalf("foreign-org sidebar = %d, want 404", code)
	}

	// Unsubscribed member → 404. THE load-bearing assertion: the UPDATE's
	// `unsubscribed_at IS NULL` predicate. Red/green proven by dropping it —
	// the departed member's row then writes (200) and this case catches it.
	if _, err := pool.Exec(ctx, `
		UPDATE channel_member SET unsubscribed_at = now()
		WHERE channel_id = $1
		  AND user_id = (SELECT id FROM user_account WHERE email = 'bob@sb.test')`,
		boot.ChannelID); err != nil {
		t.Fatalf("unsubscribe bob: %v", err)
	}
	if code := setSidebar(bobTok, boot.ChannelID, false, "#445566"); code != http.StatusNotFound {
		t.Fatalf("unsubscribed-member sidebar = %d, want 404", code)
	}

	// A guest (role 50) that is a member may pin their OWN channel.
	var guestID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO user_account (org_id, kind, email, full_name, role)
		VALUES ($1, 1, 'guest@sb.test', 'Guest Gwen', 50) RETURNING id`, boot.OrgID).Scan(&guestID); err != nil {
		t.Fatalf("guest: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO channel_member (channel_id, user_id) VALUES ($1, $2)`, boot.ChannelID, guestID); err != nil {
		t.Fatalf("guest join: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO auth_session (user_id, token_hash, expires_at)
		VALUES ($1, encode(sha256($2::bytea), 'hex'), now() + interval '1 day')`,
		guestID, "guestsbtok"); err != nil {
		t.Fatalf("guest session: %v", err)
	}
	if code := setSidebar("guestsbtok", boot.ChannelID, true, "#778899"); code != http.StatusOK {
		t.Fatalf("guest sidebar on own channel = %d, want 200", code)
	}
	if c := channelFromList("guestsbtok", boot.ChannelID); !c.Pinned || c.Color != "#778899" {
		t.Fatalf("guest's #general = pinned %v color %q, want true #778899", c.Pinned, c.Color)
	}
}
