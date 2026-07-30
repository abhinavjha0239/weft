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

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/notification"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/gateway"
)

// addNonMemberUser provisions an org member with a live session who is NOT in
// any channel — addChannelMember's counterpart for the one case it cannot
// serve: a principal who must JOIN a channel over the API. The default member
// role (40) plus the role group grant carry the ordinary verbs; the closure
// rebuild is what makes them resolve.
func addNonMemberUser(t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	orgID int64, email, name, token string) int64 {
	t.Helper()
	var uid int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO user_account (org_id, kind, email, full_name, role)
		VALUES ($1, 1, $2, $3, 40) RETURNING id`, orgID, email, name).Scan(&uid); err != nil {
		t.Fatalf("user: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO user_group_member (group_id, user_id)
		SELECT id, $2 FROM user_group WHERE org_id = $1 AND name = 'role:members'`,
		orgID, uid); err != nil {
		t.Fatalf("grant role: %v", err)
	}
	if err := db.WithTx(ctx, pool, func(tx pgx.Tx) error {
		return perms.New(pool).RebuildClosure(ctx, tx, orgID)
	}); err != nil {
		t.Fatalf("closure: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO auth_session (user_id, token_hash, expires_at)
		VALUES ($1, encode(sha256($2::bytea), 'hex'), now() + interval '1 day')`,
		uid, token); err != nil {
		t.Fatalf("session: %v", err)
	}
	return uid
}

// TestDeliverabilityMembershipPatch pins the F-17 invalidation leg that can
// cause a MISSED notification. The settings legs (PatchChannelUser off
// messaging's DeliverabilityPatcher seam, PatchAlertWords in-package) are
// pinned end-to-end elsewhere; PatchMembership — driven from member.joined /
// member.left off the event log — was the only one with no test, and it is
// the dangerous one, because a set row that never appears is a notification
// the recipient never gets, for up to an hour per message, with only the
// reconcile Warn ever betraying it.
//
// The bug only exists against a BUILT channel (an unbuilt one skips the patch
// under the lock and derives from committed truth on its lazy first build),
// so the channel is built FIRST and every phase drains the consumer rather
// than sleeping. Two miss-capable shapes are covered:
//
//   - JOIN with a standing reason: an alert-word holder joins a channel they
//     were never in. The word list predates the join, so PatchAlertWords had
//     no channel to touch — only the joined event can add the reason-3 row,
//     and without it the new member silently never keyword-pings here.
//   - LEAVE then REJOIN with the level PRESERVED: leaving unsubscribes the
//     membership row but keeps level=1 on it (ADR-008 C-4 keeps history_from),
//     so the rejoin must re-derive reason 2 from that surviving setting. This
//     is the reviewer-flagged scenario: the user reads as level=all in their
//     own settings while the materializer can no longer see them.
//
// The LEAVE direction is asserted too, but it is the safe direction: the
// candidate passes re-verify membership live, so a stale-extra row costs one
// wasted scan and can never mint a wrong notification. Only the missing-row
// direction drops work.
//
// RED/GREEN (the load-bearing pin): make Deliverability.PatchMembership a
// no-op (`return nil` before the WithTx). The join-leg assert goes red first
// — dana holds no reason-3 row after joining. Neutering ONLY the rejoin (skip
// the third call) leaves every earlier phase green and takes the rejoin
// activity assert red instead: dana keeps level=all and receives nothing.
func TestDeliverabilityMembershipPatch(t *testing.T) {
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
	runner := notification.NewRunner(pool, hub, slog.Default())
	permsSvc := perms.New(pool)
	msgSvc := messaging.New(pool, permsSvc)
	// The production wiring: settings writes patch the set in their own tx.
	msgSvc.SetDeliverability(notification.NewDeliverability(pool, slog.Default()))
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity:      identity.New(pool, permsSvc),
		Messaging:     msgSvc,
		Notifications: notification.New(pool),
	}))
	defer ts.Close()

	var boot struct {
		OrgID     int64  `json:"org_id"`
		UserID    int64  `json:"user_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "dmb", "email": "a@dmb.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	const danaTok = "danadmbtok"
	danaID := addNonMemberUser(t, ctx, pool, boot.OrgID, "dana@dmb.test", "Dana Vo", danaTok)

	drain := func() {
		t.Helper()
		drainConsumer(t, ctx, pool, "notifications", boot.OrgID, runner.ProcessOrg)
	}
	send := func(content string) int64 {
		t.Helper()
		var sent struct {
			MessageID int64 `json:"message_id"`
		}
		postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, boot.ChannelID),
			boot.Token, map[string]any{"content": content}, &sent)
		return sent.MessageID
	}
	// reasons lists dana's live delivery reasons in the channel — the cache
	// state the whole invalidation contract is about.
	reasons := func() []int16 {
		t.Helper()
		rows, err := pool.Query(ctx, `
			SELECT reason FROM channel_deliverability
			WHERE channel_id = $1 AND user_id = $2 AND medium = 1 ORDER BY reason`,
			boot.ChannelID, danaID)
		if err != nil {
			t.Fatalf("reasons: %v", err)
		}
		defer rows.Close()
		var out []int16
		for rows.Next() {
			var r int16
			if err := rows.Scan(&r); err != nil {
				t.Fatalf("scan reason: %v", err)
			}
			out = append(out, r)
		}
		return out
	}
	kindsFor := func(msgID int64) []int16 {
		t.Helper()
		rows, err := pool.Query(ctx, `
			SELECT kind FROM notification
			WHERE org_id = $1 AND user_id = $2 AND entity_type = 1 AND entity_id = $3
			ORDER BY kind`, boot.OrgID, danaID, msgID)
		if err != nil {
			t.Fatalf("kinds: %v", err)
		}
		defer rows.Close()
		var out []int16
		for rows.Next() {
			var k int16
			if err := rows.Scan(&k); err != nil {
				t.Fatalf("scan kind: %v", err)
			}
			out = append(out, k)
		}
		return out
	}
	joinChannel := func(what string) {
		t.Helper()
		if code := postJSONStatus(t, fmt.Sprintf("%s/api/v1/channels/%d/join", ts.URL, boot.ChannelID),
			danaTok, map[string]any{}); code != http.StatusOK {
			t.Fatalf("%s = %d, want 200", what, code)
		}
		drain()
	}

	// Precondition: the channel must be BUILT, or every patch legitimately
	// skips under the lock and this test would prove nothing.
	send("kickoff")
	drain()
	var builtAt *time.Time
	if err := pool.QueryRow(ctx, `SELECT deliverability_built_at FROM channel WHERE id = $1`,
		boot.ChannelID).Scan(&builtAt); err != nil || builtAt == nil {
		t.Fatalf("channel not built (%v, %v): the membership legs below would be vacuous", builtAt, err)
	}

	// (1) A standing alert word held by a NON-member touches nothing: the
	// alert-word patch only walks channels the user is a live member of.
	if code := putJSON(t, ts.URL+"/api/v1/alert-words", danaTok,
		map[string]any{"words": []string{"postgres"}}); code != http.StatusOK {
		t.Fatalf("dana alert words = %d, want 200", code)
	}
	if got := reasons(); len(got) != 0 {
		t.Fatalf("non-member holds reasons %v, want none", got)
	}

	// (2) JOIN: the joined event is the ONLY thing that can add dana's
	// reason-3 row, and without it she never keyword-pings in this channel.
	joinChannel("dana join")
	if got := reasons(); len(got) != 1 || got[0] != 3 {
		t.Fatalf("after join dana holds reasons %v, want [3] (alert-word holder) — member.joined did not patch the set", got)
	}
	keywordMsg := send("postgres is down again")
	drain()
	if got := kindsFor(keywordMsg); len(got) != 1 || got[0] != notification.KindKeyword {
		t.Fatalf("new member's keyword ping = %v, want [%d]", got, notification.KindKeyword)
	}

	// (3) level=all adds reason 2 through the settings seam (the already-
	// pinned leg) — the setting the leave/rejoin below must preserve.
	if code := putJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/notification", ts.URL, boot.ChannelID),
		danaTok, map[string]any{"level": 1}); code != http.StatusOK {
		t.Fatalf("dana level=all = %d, want 200", code)
	}
	if got := reasons(); len(got) != 2 || got[0] != 2 || got[1] != 3 {
		t.Fatalf("after level=all dana holds reasons %v, want [2 3]", got)
	}

	// (4) LEAVE: the membership row SURVIVES with level=1 intact (ADR-008
	// C-4), and the left event must clear every derived reason.
	if code := postJSONStatus(t, fmt.Sprintf("%s/api/v1/channels/%d/leave", ts.URL, boot.ChannelID),
		danaTok, map[string]any{}); code != http.StatusOK {
		t.Fatalf("dana leave = %d, want 200", code)
	}
	drain()
	var level int16
	var unsubscribed *time.Time
	if err := pool.QueryRow(ctx, `
		SELECT level, unsubscribed_at FROM channel_member
		WHERE channel_id = $1 AND user_id = $2`,
		boot.ChannelID, danaID).Scan(&level, &unsubscribed); err != nil {
		t.Fatalf("membership after leave: %v", err)
	}
	if level != 1 || unsubscribed == nil {
		t.Fatalf("after leave: level=%d unsubscribed=%v, want the row preserved with level=all intact",
			level, unsubscribed)
	}
	if got := reasons(); len(got) != 0 {
		t.Fatalf("after leave dana holds reasons %v, want none", got)
	}
	silentMsg := send("routine standup notes")
	drain()
	if got := kindsFor(silentMsg); len(got) != 0 {
		t.Fatalf("a departed member was notified %v, want nothing", got)
	}

	// (5) REJOIN with the level preserved — the reviewer-flagged miss. Dana
	// never re-touched her settings, so ONLY the joined event can re-derive
	// reason 2 from the surviving level=1; a regression here leaves her
	// reading as level=all in her own settings while the materializer can no
	// longer see her at all.
	joinChannel("dana rejoin")
	if got := reasons(); len(got) != 2 || got[0] != 2 || got[1] != 3 {
		t.Fatalf("after rejoin dana holds reasons %v, want [2 3] re-derived from the PRESERVED level=all", got)
	}
	activityMsg := send("deploy window moved to friday")
	drain()
	if got := kindsFor(activityMsg); len(got) != 1 || got[0] != notification.KindChannelActivity {
		t.Fatalf("rejoined level=all member's activity ping = %v, want [%d] — the missed-notification shape",
			got, notification.KindChannelActivity)
	}
}
