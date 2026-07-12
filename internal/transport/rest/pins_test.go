package rest

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/domain/dm"
	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/gateway"
)

func pinReq(t *testing.T, base, token string, msgID int64, method string) int {
	t.Helper()
	req, _ := http.NewRequest(method, fmt.Sprintf("%s/api/v1/messages/%d/pin", base, msgID), nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("pin %s: %v", method, err)
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

// addGuest inserts a role-50 guest who is a channel member with a session —
// the pins list gate is requireMember (membership), so a guest sees pins only
// in their own channels exactly as any member does; there is no role branch.
func addGuest(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID, channelID int64, email, name, token string) {
	t.Helper()
	var uid int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO user_account (org_id, kind, email, full_name, role)
		VALUES ($1, 1, $2, $3, 50) RETURNING id`, orgID, email, name).Scan(&uid); err != nil {
		t.Fatalf("guest: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO channel_member (channel_id, user_id) VALUES ($1, $2)`, channelID, uid); err != nil {
		t.Fatalf("guest join: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO auth_session (user_id, token_hash, expires_at)
		VALUES ($1, encode(sha256($2::bytea), 'hex'), now() + interval '1 day')`,
		uid, token); err != nil {
		t.Fatalf("guest session: %v", err)
	}
}

// TestChannelPins: P-02b — channel-only pins as shared, event-logged,
// administer_channel-gated curation with an exact per-channel cap; the
// three-way read ACL makes an unpinnable message indistinguishable from a
// missing one; delete clears pins in the same tx.
func TestChannelPins(t *testing.T) {
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

	hub := gateway.NewHub(pool, slog.Default())
	go hub.Run(ctx)
	permsSvc := perms.New(pool)
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity:  identity.New(pool, permsSvc),
		Messaging: messaging.New(pool, permsSvc),
		DM:        dm.New(pool),
	}))
	defer ts.Close()

	var boot struct {
		OrgID     int64  `json:"org_id"`
		UserID    int64  `json:"user_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "pin", "email": "a@pin.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	// bob is a plain member of #general (can read, no administer_channel).
	bobTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"bob@pin.test", "Bob Ray", "bobpintok")
	// charlie is an org member but NOT a member of #general (a second channel).
	var other struct {
		ChannelID int64 `json:"channel_id"`
	}
	postJSON(t, ts.URL+"/api/v1/channels", boot.Token, map[string]any{"name": "other"}, &other)
	charlieTok := addChannelMember(t, ctx, pool, boot.OrgID, other.ChannelID,
		"charlie@pin.test", "Charlie Kim", "charliepintok")

	send := func(token, content string) int64 {
		var m struct {
			MessageID int64 `json:"message_id"`
		}
		postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, boot.ChannelID),
			token, map[string]any{"content": content}, &m)
		return m.MessageID
	}
	msg1 := send(boot.Token, "pin me")

	// Permission matrix. A plain member can READ but not pin (403); a
	// non-member cannot even see the message (oracle-free 404); an admin
	// (the owner) pins (200).
	if code := pinReq(t, ts.URL, bobTok, msg1, "PUT"); code != http.StatusForbidden {
		t.Fatalf("member pin = %d, want 403", code)
	}
	if code := pinReq(t, ts.URL, charlieTok, msg1, "PUT"); code != http.StatusNotFound {
		t.Fatalf("non-member pin = %d, want 404 (oracle-free)", code)
	}
	if code := pinReq(t, ts.URL, boot.Token, msg1, "PUT"); code != http.StatusOK {
		t.Fatalf("admin pin = %d, want 200", code)
	}

	// Exactly one event, and it carries the channel for gateway routing.
	pinnedCount := func() int {
		var n int
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM event_log WHERE org_id = $1 AND verb = 'message.pinned'`,
			boot.OrgID).Scan(&n); err != nil {
			t.Fatalf("count pinned events: %v", err)
		}
		return n
	}
	if pinnedCount() != 1 {
		t.Fatalf("message.pinned events = %d, want 1", pinnedCount())
	}
	var chanInPayload int64
	if err := pool.QueryRow(ctx, `
		SELECT (payload->>'channel_id')::bigint FROM event_log
		WHERE verb = 'message.pinned' ORDER BY id LIMIT 1`).Scan(&chanInPayload); err != nil || chanInPayload != boot.ChannelID {
		t.Fatalf("payload channel = %d (%v), want %d", chanInPayload, err, boot.ChannelID)
	}
	// Idempotent re-pin: still 200, still exactly one event.
	if code := pinReq(t, ts.URL, boot.Token, msg1, "PUT"); code != http.StatusOK {
		t.Fatalf("re-pin = %d, want 200", code)
	}
	if pinnedCount() != 1 {
		t.Fatalf("re-pin emitted a second event: %d", pinnedCount())
	}

	// List shape + ordering: newest-pinned first, excerpt + metadata present.
	msg2 := send(boot.Token, "second pin")
	if code := pinReq(t, ts.URL, boot.Token, msg2, "PUT"); code != http.StatusOK {
		t.Fatalf("pin msg2 = %d", code)
	}
	listPins := func(token string, channelID int64) (int, []messaging.PinnedMessage) {
		var resp struct {
			Pins []messaging.PinnedMessage `json:"pins"`
		}
		code := getJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/pins", ts.URL, channelID), token, &resp)
		return code, resp.Pins
	}
	code, pins := listPins(boot.Token, boot.ChannelID)
	if code != 200 || len(pins) != 2 {
		t.Fatalf("list = %d, %d pins, want 2", code, len(pins))
	}
	if pins[0].MessageID != msg2 || pins[1].MessageID != msg1 {
		t.Fatalf("order = [%d %d], want newest-pinned first [%d %d]",
			pins[0].MessageID, pins[1].MessageID, msg2, msg1)
	}
	if pins[1].Excerpt != "pin me" || pins[1].AuthorID != boot.UserID ||
		pins[1].PinnedBy != boot.UserID || pins[1].PinnedAt.IsZero() {
		t.Fatalf("preview shape wrong: %+v", pins[1])
	}

	// Unpin: own event once, idempotent repeat stays quiet, list shrinks.
	if code := pinReq(t, ts.URL, boot.Token, msg1, "DELETE"); code != http.StatusOK {
		t.Fatalf("unpin = %d", code)
	}
	if code := pinReq(t, ts.URL, boot.Token, msg1, "DELETE"); code != http.StatusOK {
		t.Fatalf("re-unpin = %d, want idempotent 200", code)
	}
	var unpinned int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM event_log WHERE org_id = $1 AND verb = 'message.unpinned'`,
		boot.OrgID).Scan(&unpinned); err != nil || unpinned != 1 {
		t.Fatalf("message.unpinned events = %d (%v), want 1", unpinned, err)
	}
	if _, pins := listPins(boot.Token, boot.ChannelID); len(pins) != 1 || pins[0].MessageID != msg2 {
		t.Fatalf("after unpin, pins = %+v, want [msg2]", pins)
	}

	// Delete clears the pin row in the same tx (keeps the cap honest).
	if code := deleteReq(t, fmt.Sprintf("%s/api/v1/messages/%d", ts.URL, msg2), boot.Token); code != 200 {
		t.Fatalf("delete msg2 = %d", code)
	}
	var pinRows int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM pin WHERE message_id = $1`, msg2).Scan(&pinRows); err != nil || pinRows != 0 {
		t.Fatalf("delete left pin rows = %d (%v), want 0", pinRows, err)
	}
	if _, pins := listPins(boot.Token, boot.ChannelID); len(pins) != 0 {
		t.Fatalf("after delete, pins = %+v, want empty", pins)
	}

	// DM message: readable by the participant but not a channel message → 400.
	var conv struct {
		RootThreadID int64 `json:"root_thread_id"`
	}
	var bobID int64
	if err := pool.QueryRow(ctx, `SELECT id FROM user_account WHERE email = 'bob@pin.test'`).Scan(&bobID); err != nil {
		t.Fatalf("bob id: %v", err)
	}
	postJSON(t, ts.URL+"/api/v1/dms", boot.Token, map[string]any{"user_ids": []int64{bobID}}, &conv)
	var dmMsg struct {
		MessageID int64 `json:"message_id"`
	}
	postJSON(t, fmt.Sprintf("%s/api/v1/threads/%d/messages", ts.URL, conv.RootThreadID),
		boot.Token, map[string]any{"content": "dm hi"}, &dmMsg)
	if code := pinReq(t, ts.URL, boot.Token, dmMsg.MessageID, "PUT"); code != http.StatusBadRequest {
		t.Fatalf("pin DM message = %d, want 400", code)
	}

	// Cap: 50 pins per channel is exact. Fill a fresh channel to the cap via
	// SQL, then the 51st pin is a 409 and does not land.
	var capCh struct {
		ChannelID int64 `json:"channel_id"`
	}
	postJSON(t, ts.URL+"/api/v1/channels", boot.Token, map[string]any{"name": "capchan"}, &capCh)
	var capRoot int64
	if err := pool.QueryRow(ctx,
		`SELECT root_thread_id FROM channel WHERE id = $1`, capCh.ChannelID).Scan(&capRoot); err != nil {
		t.Fatalf("cap root: %v", err)
	}
	var ids []int64
	rows, err := pool.Query(ctx, `
		INSERT INTO message (org_id, thread_id, channel_id, author_id,
			source, ast, rendered, render_version)
		SELECT $1, $2, $3, $4, 'cap-' || g, '{}', '', 1
		FROM generate_series(1, 51) g RETURNING id`,
		boot.OrgID, capRoot, capCh.ChannelID, boot.UserID)
	if err != nil {
		t.Fatalf("seed cap messages: %v", err)
	}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan cap id: %v", err)
		}
		ids = append(ids, id)
	}
	rows.Close()
	if len(ids) != 51 {
		t.Fatalf("seeded %d cap messages, want 51", len(ids))
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO pin (channel_id, message_id, pinned_by)
		SELECT $1, m, $3 FROM unnest($2::bigint[]) m`,
		capCh.ChannelID, ids[:50], boot.UserID); err != nil {
		t.Fatalf("seed 50 pins: %v", err)
	}
	if code := pinReq(t, ts.URL, boot.Token, ids[50], "PUT"); code != http.StatusConflict {
		t.Fatalf("51st pin = %d, want 409", code)
	}
	var capPins int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM pin WHERE channel_id = $1`, capCh.ChannelID).Scan(&capPins); err != nil || capPins != 50 {
		t.Fatalf("cap pins = %d (%v), want 50 (409 must not land)", capPins, err)
	}

	// Guest visibility: a guest member of #general sees its pins (member gate),
	// but is blocked from a channel they are not in (403) — same as any member.
	msg3 := send(boot.Token, "for the guest")
	if code := pinReq(t, ts.URL, boot.Token, msg3, "PUT"); code != http.StatusOK {
		t.Fatalf("pin msg3 = %d", code)
	}
	addGuest(t, ctx, pool, boot.OrgID, boot.ChannelID, "gina@pin.test", "Gina Guest", "ginapintok")
	if code, pins := listPins("ginapintok", boot.ChannelID); code != 200 || len(pins) != 1 || pins[0].MessageID != msg3 {
		t.Fatalf("guest list of own channel = %d %+v, want [msg3]", code, pins)
	}
	if code, _ := listPins("ginapintok", other.ChannelID); code != http.StatusForbidden {
		t.Fatalf("guest list of foreign channel = %d, want 403", code)
	}
}
