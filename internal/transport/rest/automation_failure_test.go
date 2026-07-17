package rest

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/domain/automation"
	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/notification"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/gateway"
)

// TestAutomationFailureNotifies: P-25. A run that enters the failing state
// alerts whoever may administer the rule (the write-gate holders) with a
// kind-6 in-app row; the alert is throttled to at most hourly while the rule
// keeps failing and re-arms on any success; a channel-scope rule alerts its
// channel admins, not the org admins; the live ping is DND-gated; and the
// kind-6 email medium is off by default but sendable after an opt-in.
func TestAutomationFailureNotifies(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		cancel()
		t.Fatalf("connect: %v", err)
	}
	defer func() { cancel(); pool.Close() }()
	resetAndMigrate(t, ctx, pool)

	hub := gateway.NewHub(pool, slog.Default())
	permsSvc := perms.New(pool)
	msgSvc := messaging.New(pool, permsSvc)
	notifSvc := notification.New(pool)
	capture := &captureFan{}
	notifSvc.SetFanout(capture)
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity:      identity.New(pool, permsSvc),
		Messaging:     msgSvc,
		Notifications: notifSvc,
		Automations:   automation.New(pool, permsSvc),
	}))
	defer ts.Close()
	runner := automation.NewRunner(pool, msgSvc, permsSvc, notifSvc, slog.Default())

	var boot struct {
		OrgID     int64  `json:"org_id"`
		UserID    int64  `json:"user_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "fail", "email": "alice@fail.test", "password": "password123",
		"full_name": "Alice Owner",
	}, &boot)
	aliceID := boot.UserID

	userID := func(email string) int64 {
		t.Helper()
		var id int64
		if err := pool.QueryRow(ctx, `SELECT id FROM user_account WHERE email = $1`, email).Scan(&id); err != nil {
			t.Fatalf("user %s: %v", email, err)
		}
		return id
	}
	rebuildClosure := func() {
		t.Helper()
		if err := db.WithTx(ctx, pool, func(tx pgx.Tx) error {
			return permsSvc.RebuildClosure(ctx, tx, boot.OrgID)
		}); err != nil {
			t.Fatalf("closure: %v", err)
		}
	}
	addToGroup := func(uid int64, group string) {
		t.Helper()
		if _, err := pool.Exec(ctx, `
			INSERT INTO user_group_member (group_id, user_id)
			SELECT id, $2 FROM user_group WHERE org_id = $1 AND name = $3`,
			boot.OrgID, uid, group); err != nil {
			t.Fatalf("add %d to %s: %v", uid, group, err)
		}
		rebuildClosure()
	}

	// bob: a second org admin (in role:members via addChannelMember, then
	// role:admins). carol: a plain member. dan: a guest (role:everyone only).
	addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID, "bob@fail.test", "Bob Admin", "bobfailtok")
	bobID := userID("bob@fail.test")
	addToGroup(bobID, perms.GroupAdmins)
	addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID, "carol@fail.test", "Carol Member", "carolfailtok")
	carolID := userID("carol@fail.test")
	var danID int64
	if err := pool.QueryRow(ctx, `INSERT INTO user_account (org_id, kind, email, full_name, role)
		VALUES ($1, 1, 'dan@fail.test', 'Dan Guest', 15) RETURNING id`, boot.OrgID).Scan(&danID); err != nil {
		t.Fatalf("dan: %v", err)
	}
	addToGroup(danID, perms.GroupEveryone)

	// bob is snoozed from the outset: his in-app row must still land (the badge
	// is structural), but his LIVE ping is suppressed (system actor, no VIP
	// pierce). alice stays active.
	if _, err := pool.Exec(ctx, `INSERT INTO dnd_setting (user_id, snoozed_until)
		VALUES ($1, now() + interval '1 hour')`, bobID); err != nil {
		t.Fatalf("snooze bob: %v", err)
	}

	// A separate target channel the failer posts into; archiving it (via SQL,
	// leaving the trigger channel live) is how we force step failures on demand.
	var target struct {
		ChannelID int64 `json:"channel_id"`
	}
	postJSON(t, ts.URL+"/api/v1/channels", boot.Token, map[string]any{"name": "target"}, &target)
	setArchived := func(channelID int64, archived bool) {
		t.Helper()
		q := `UPDATE channel SET archived_at = now() WHERE id = $1`
		if !archived {
			q = `UPDATE channel SET archived_at = NULL WHERE id = $1`
		}
		if _, err := pool.Exec(ctx, q, channelID); err != nil {
			t.Fatalf("archive %d=%v: %v", channelID, archived, err)
		}
	}

	process := func() {
		t.Helper()
		drainConsumer(t, ctx, pool, "automations", boot.OrgID, runner.ProcessOrg)
	}
	fire := func(content string) {
		t.Helper()
		var sent struct {
			MessageID int64 `json:"message_id"`
		}
		postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, boot.ChannelID),
			boot.Token, map[string]any{"content": content}, &sent)
		if sent.MessageID == 0 {
			t.Fatal("fire send failed")
		}
	}
	latestRun := func(automationID int64) (int64, int16) {
		t.Helper()
		var id int64
		var status int16
		if err := pool.QueryRow(ctx, `SELECT id, status FROM automation_run
			WHERE automation_id = $1 ORDER BY id DESC LIMIT 1`, automationID).Scan(&id, &status); err != nil {
			t.Fatalf("latest run: %v", err)
		}
		return id, status
	}
	kind6ForRun := func(uid, runID int64) int {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM notification
			WHERE user_id = $1 AND kind = 6 AND entity_type = 18 AND entity_id = $2`,
			uid, runID).Scan(&n); err != nil {
			t.Fatalf("kind6 count: %v", err)
		}
		return n
	}
	totalKind6 := func(uid int64) int {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM notification
			WHERE user_id = $1 AND kind = 6`, uid).Scan(&n); err != nil {
			t.Fatalf("total kind6: %v", err)
		}
		return n
	}

	// Drain bootstrap/setup history before any rule exists.
	process()

	// The failer: an org-scope rule posting into target on every message.
	var failer automation.Automation
	postJSON(t, ts.URL+"/api/v1/automations", boot.Token, map[string]any{
		"scope_type": 1, "scope_id": boot.OrgID, "name": "failer",
		"definition": map[string]any{
			"trigger": map[string]any{"verb": "message.created"},
			"steps": []any{map[string]any{
				"kind": "post_message", "channel_id": target.ChannelID, "content": "auto"}},
		}}, &failer)
	if code := patchJSON(t, fmt.Sprintf("%s/api/v1/automations/%d", ts.URL, failer.ID),
		boot.Token, map[string]any{"enabled": true}); code != http.StatusOK {
		t.Fatalf("enable failer")
	}

	// Phase A — first failure notifies both org admins, nobody else, and the
	// live ping is DND-gated.
	setArchived(target.ChannelID, true)
	fire("one")
	process()
	run1, st1 := latestRun(failer.ID)
	if st1 != 5 {
		t.Fatalf("run1 status = %d, want 5 (failed)", st1)
	}
	if kind6ForRun(aliceID, run1) != 1 || kind6ForRun(bobID, run1) != 1 {
		t.Fatalf("run1: admins should each get one kind-6 row (alice=%d bob=%d)",
			kind6ForRun(aliceID, run1), kind6ForRun(bobID, run1))
	}
	// Load-bearing: a member and a guest are NOT holders of manage_org.
	// Resolving recipients WITHOUT the closure/verb path (e.g. "all org users")
	// would give them rows and fail here.
	if kind6ForRun(carolID, run1) != 0 || kind6ForRun(danID, run1) != 0 {
		t.Fatalf("run1: non-admins must get nothing (carol=%d dan=%d)",
			kind6ForRun(carolID, run1), kind6ForRun(danID, run1))
	}
	// Ping: alice (active) captured; bob (snoozed, system actor → no VIP
	// pierce) suppressed. Both rows landed regardless (asserted above).
	pinged := map[int64]bool{}
	for _, c := range capture.take() {
		pinged[c.userID] = true
	}
	if !pinged[aliceID] || pinged[bobID] {
		t.Fatalf("ping set = %v, want alice pinged and bob (snoozed) suppressed", pinged)
	}

	// Phase B — an immediate second failure is throttled: no new rows.
	fire("two")
	process()
	run2, st2 := latestRun(failer.ID)
	if st2 != 5 || run2 == run1 {
		t.Fatalf("run2 = %d status %d, want a new failed run", run2, st2)
	}
	if kind6ForRun(aliceID, run2) != 0 || kind6ForRun(bobID, run2) != 0 {
		t.Fatalf("run2 must be throttled (no new rows): alice=%d bob=%d",
			kind6ForRun(aliceID, run2), kind6ForRun(bobID, run2))
	}
	if totalKind6(aliceID) != 1 {
		t.Fatalf("alice total kind-6 = %d, want still 1 after throttle", totalKind6(aliceID))
	}

	// Phase C — backdate all prior finished runs >1h: the next failure alerts
	// again (entry into the failing state has re-armed by age).
	if _, err := pool.Exec(ctx, `UPDATE automation_run SET finished_at = now() - interval '2 hours'
		WHERE automation_id = $1`, failer.ID); err != nil {
		t.Fatalf("backdate: %v", err)
	}
	fire("three")
	process()
	run3, st3 := latestRun(failer.ID)
	if st3 != 5 {
		t.Fatalf("run3 status = %d, want 5", st3)
	}
	if kind6ForRun(aliceID, run3) != 1 || kind6ForRun(bobID, run3) != 1 {
		t.Fatalf("run3 should re-notify after backdate: alice=%d bob=%d",
			kind6ForRun(aliceID, run3), kind6ForRun(bobID, run3))
	}

	// Phase D — a success between failures re-arms: unarchive → success (no
	// alert), re-archive → the next failure alerts again.
	setArchived(target.ChannelID, false)
	fire("four")
	process()
	run4, st4 := latestRun(failer.ID)
	if st4 != 2 {
		t.Fatalf("run4 status = %d, want 2 (success)", st4)
	}
	if kind6ForRun(aliceID, run4) != 0 {
		t.Fatalf("run4 succeeded; must not notify")
	}
	setArchived(target.ChannelID, true)
	fire("five")
	process()
	run5, st5 := latestRun(failer.ID)
	if st5 != 5 {
		t.Fatalf("run5 status = %d, want 5", st5)
	}
	if kind6ForRun(aliceID, run5) != 1 {
		t.Fatalf("run5 should notify (a success re-armed): alice=%d", kind6ForRun(aliceID, run5))
	}

	// Phase E — a channel-scope rule alerts its channel admins, NOT the org
	// admins. administer_channel on #ops points at a custom group holding only
	// erin. Disable the org failer first for isolation.
	if code := patchJSON(t, fmt.Sprintf("%s/api/v1/automations/%d", ts.URL, failer.ID),
		boot.Token, map[string]any{"enabled": false}); code != http.StatusOK {
		t.Fatalf("disable failer")
	}
	var ops struct {
		ChannelID int64 `json:"channel_id"`
	}
	postJSON(t, ts.URL+"/api/v1/channels", boot.Token, map[string]any{"name": "ops"}, &ops)

	// alice creates + enables the channel rule and fires its trigger while she
	// still holds administer_channel on #ops (the org default) and it is live.
	var chanRule automation.Automation
	postJSON(t, ts.URL+"/api/v1/automations", boot.Token, map[string]any{
		"scope_type": 3, "scope_id": ops.ChannelID, "name": "ops-rule",
		"definition": map[string]any{
			"trigger": map[string]any{"verb": "message.created"},
			"steps":   []any{map[string]any{"kind": "post_message", "content": "ack"}},
		}}, &chanRule)
	if code := patchJSON(t, fmt.Sprintf("%s/api/v1/automations/%d", ts.URL, chanRule.ID),
		boot.Token, map[string]any{"enabled": true}); code != http.StatusOK {
		t.Fatalf("enable ops-rule")
	}
	var opsMsg struct {
		MessageID int64 `json:"message_id"`
	}
	postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, ops.ChannelID),
		boot.Token, map[string]any{"content": "in ops"}, &opsMsg)

	// NOW point administer_channel on #ops at a custom group holding only erin
	// (excluding the org admins), so the failure's holder resolution differs
	// from the org-admin set. The rule already exists and its trigger is
	// already logged, so alice losing the verb here does not block anything.
	var erinID int64
	if err := pool.QueryRow(ctx, `INSERT INTO user_account (org_id, kind, email, full_name, role)
		VALUES ($1, 1, 'erin@fail.test', 'Erin ChanAdmin', 40) RETURNING id`, boot.OrgID).Scan(&erinID); err != nil {
		t.Fatalf("erin: %v", err)
	}
	if err := db.WithTx(ctx, pool, func(tx pgx.Tx) error {
		var gid int64
		if err := tx.QueryRow(ctx, `INSERT INTO user_group (org_id, name, is_system)
			VALUES ($1, 'ops-admins', false) RETURNING id`, boot.OrgID).Scan(&gid); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO user_group_member (group_id, user_id) VALUES ($1, $2)`,
			gid, erinID); err != nil {
			return err
		}
		if err := permsSvc.Assign(ctx, tx, boot.OrgID, perms.VerbAdministerChannel,
			perms.ChannelRef(ops.ChannelID), gid); err != nil {
			return err
		}
		return permsSvc.RebuildClosure(ctx, tx, boot.OrgID)
	}); err != nil {
		t.Fatalf("ops-admins setup: %v", err)
	}

	// Archive #ops, THEN process: the durable message.created event drives the
	// rule, whose step now posts into an archived channel and fails.
	setArchived(ops.ChannelID, true)
	process()
	chanRun, chanStatus := latestRun(chanRule.ID)
	if chanStatus != 5 {
		t.Fatalf("ops-rule run status = %d, want 5", chanStatus)
	}
	if kind6ForRun(erinID, chanRun) != 1 {
		t.Fatalf("channel-rule failure must notify erin (the channel admin): %d", kind6ForRun(erinID, chanRun))
	}
	if kind6ForRun(aliceID, chanRun) != 0 || kind6ForRun(bobID, chanRun) != 0 {
		t.Fatalf("channel-rule failure must NOT notify org admins (alice=%d bob=%d)",
			kind6ForRun(aliceID, chanRun), kind6ForRun(bobID, chanRun))
	}

	// Phase F — prefs PUT accepts kind 6; the email worker skips kind-6 rows by
	// default and sends them after the opt-in. Uses alice (never snoozed —
	// bob's snooze would correctly hold his email back, a delay not a drop).
	mailCapture := &captureSender{}
	worker := notification.NewEmailWorker(pool, mailCapture, slog.Default())
	due := time.Now().Add(time.Minute)
	if n, err := worker.RunOnce(ctx, due); err != nil || n != 0 {
		t.Fatalf("default sweep = %d (%v), want 0 (kind-6 email off by default)", n, err)
	}
	if code := putJSON(t, ts.URL+"/api/v1/notification-prefs", boot.Token,
		map[string]any{"kind": 6, "medium": 2, "enabled": true}); code != http.StatusOK {
		t.Fatalf("prefs PUT kind 6 = %d, want 200", code)
	}
	if n, err := worker.RunOnce(ctx, due); err != nil || n != 1 {
		t.Fatalf("post-opt-in sweep = %d (%v), want 1 email to alice", n, err)
	}
	mails := mailCapture.take()
	if len(mails) != 1 || mails[0].to != "alice@fail.test" ||
		!strings.Contains(mails[0].text, "An automation run failed") {
		t.Fatalf("kind-6 email = %+v", mails)
	}
}
