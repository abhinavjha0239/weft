package rest

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/domain/compliance"
	"github.com/abhinavjha0239/weft/internal/domain/dm"
	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/gateway"
	"github.com/abhinavjha0239/weft/internal/platform/blob"
)

// TestRetentionVacuum: P-17 commit 1. The vacuum lane removes messages from
// the product once their age passes the EFFECTIVE retention policy (nearest
// scope wins, channel over org; -1 = forever never vacuums); a vacuumed
// message is invisible on the P-16 read paths but its content stays in place
// for the restore window; and an active legal hold freezes an otherwise
// eligible message (the load-bearing guard).
func TestRetentionVacuum(t *testing.T) {
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

	store, err := blob.Open("fs", t.TempDir())
	if err != nil {
		t.Fatalf("blob: %v", err)
	}
	hub := gateway.NewHub(pool, slog.Default())
	go hub.Run(ctx)
	permsSvc := perms.New(pool)
	msgSvc := messaging.New(pool, permsSvc)
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity:   identity.New(pool, permsSvc),
		Messaging:  msgSvc,
		DM:         dm.New(pool),
		Compliance: compliance.New(pool, permsSvc),
	}))
	defer ts.Close()
	janitor := compliance.NewJanitor(pool, store, slog.Default())

	var boot struct {
		OrgID     int64  `json:"org_id"`
		UserID    int64  `json:"user_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "ret", "email": "a@ret.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	bobID := userID(t, ctx, pool, "bob@ret.test", boot.OrgID, "Bob Ray")
	if code := putJSON(t, ts.URL+"/api/v1/admin/verbs", boot.Token,
		map[string]any{"verb": "compliance_officer", "group": "role:admins"}); code != http.StatusOK {
		t.Fatalf("grant = %d", code)
	}

	mkChannel := func(name string) int64 {
		t.Helper()
		var c struct {
			ChannelID int64 `json:"channel_id"`
		}
		postJSON(t, ts.URL+"/api/v1/channels", boot.Token, map[string]any{"name": name}, &c)
		if c.ChannelID == 0 {
			t.Fatalf("create channel %q failed", name)
		}
		return c.ChannelID
	}
	hot := mkChannel("hot")
	cold := mkChannel("cold")
	kept := mkChannel("kept")
	send := func(channelID int64, content string) int64 {
		t.Helper()
		var sent struct {
			MessageID int64 `json:"message_id"`
		}
		postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, channelID),
			boot.Token, map[string]any{"content": content}, &sent)
		if sent.MessageID == 0 {
			t.Fatal("send failed")
		}
		return sent.MessageID
	}
	setPolicy := func(scopeType int16, scopeID int64, days int32) {
		t.Helper()
		if code := putJSON(t, ts.URL+"/api/v1/admin/retention-policies", boot.Token,
			map[string]any{"scope_type": scopeType, "scope_id": scopeID,
				"duration_days": days, "keep_edits": true}); code != http.StatusOK {
			t.Fatalf("policy scope=%d/%d days=%d = %d", scopeType, scopeID, days, code)
		}
	}
	visible := func(msgID int64) bool {
		t.Helper()
		code := getJSON(t, fmt.Sprintf("%s/api/v1/messages/%d", ts.URL, msgID), boot.Token, nil)
		return code == http.StatusOK
	}
	// vacuumState reports (deleted_at set, retention_vacuumed_at set, content
	// still present in the row).
	vacuumState := func(msgID int64) (deleted, vacuumed, hasContent bool) {
		t.Helper()
		if err := pool.QueryRow(ctx, `
			SELECT deleted_at IS NOT NULL, retention_vacuumed_at IS NOT NULL, source <> ''
			FROM message WHERE id = $1`, msgID).Scan(&deleted, &vacuumed, &hasContent); err != nil {
			t.Fatalf("vacuum state %d: %v", msgID, err)
		}
		return
	}
	rowExists := func(msgID int64) bool {
		t.Helper()
		var exists bool
		if err := pool.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM message WHERE id = $1)`, msgID).Scan(&exists); err != nil {
			t.Fatalf("row exists %d: %v", msgID, err)
		}
		return exists
	}

	gMsg := send(boot.ChannelID, "general note")
	// Give gMsg associations so the purge lane's FK cleanup is exercised.
	if code := putJSON(t, fmt.Sprintf("%s/api/v1/messages/%d/reactions/%s",
		ts.URL, gMsg, url.PathEscape("👍")), boot.Token, nil); code != http.StatusOK {
		t.Fatalf("react gMsg = %d", code)
	}
	if code := putJSON(t, fmt.Sprintf("%s/api/v1/messages/%d/save", ts.URL, gMsg),
		boot.Token, nil); code != http.StatusOK {
		t.Fatalf("save gMsg = %d", code)
	}
	if code := patchJSON(t, fmt.Sprintf("%s/api/v1/messages/%d", ts.URL, gMsg),
		boot.Token, map[string]any{"content": "general note v2"}); code != http.StatusOK {
		t.Fatalf("edit gMsg = %d", code)
	}
	hotMsg := send(hot, "ephemeral")
	coldMsg := send(cold, "archival")
	keptMsg := send(kept, "permanent")
	var conv struct {
		RootThreadID int64 `json:"root_thread_id"`
	}
	postJSON(t, ts.URL+"/api/v1/dms", boot.Token, map[string]any{"user_ids": []int64{bobID}}, &conv)
	var dmSent struct {
		MessageID int64 `json:"message_id"`
	}
	postJSON(t, fmt.Sprintf("%s/api/v1/threads/%d/messages", ts.URL, conv.RootThreadID),
		boot.Token, map[string]any{"content": "dm note"}, &dmSent)

	// The sweep clock is 48h ahead of real send time; a message is eligible
	// only when its effective duration is under 2 days.
	future := time.Now().Add(48 * time.Hour)

	// Phase A — no policy anywhere: the COALESCE default is forever, nothing
	// vacuums even far in the future.
	if rep, err := janitor.SweepOnce(ctx, future); err != nil || rep.MessagesVacuumed != 0 {
		t.Fatalf("no-policy sweep = %+v (%v), want zero vacuumed", rep, err)
	}

	// Phase B — a 1-day channel policy on #hot: hotMsg ages out, nothing else
	// has a policy.
	setPolicy(3, hot, 1)
	if rep, err := janitor.SweepOnce(ctx, future); err != nil || rep.MessagesVacuumed != 1 {
		t.Fatalf("hot sweep = %+v (%v), want 1 vacuumed", rep, err)
	}
	if d, v, c := vacuumState(hotMsg); !d || !v || !c {
		t.Fatalf("hotMsg state deleted=%v vacuumed=%v hasContent=%v, want all true (tombstoned, content kept)", d, v, c)
	}
	if visible(hotMsg) {
		t.Fatal("vacuumed message must be invisible on the authed read path")
	}
	var vEvents int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM event_log
		WHERE verb = 'retention.message_vacuumed' AND entity_id = $1`, hotMsg).Scan(&vEvents); err != nil || vEvents != 1 {
		t.Fatalf("vacuum events = %d (%v), want 1", vEvents, err)
	}
	for _, id := range []int64{gMsg, coldMsg, keptMsg, dmSent.MessageID} {
		if !visible(id) {
			t.Fatalf("message %d must survive (no applicable policy yet)", id)
		}
	}

	// Phase C — a 3650-day policy on #cold: coldMsg's 48h age is far under it,
	// so nothing new vacuums (the age boundary).
	setPolicy(3, cold, 3650)
	if rep, err := janitor.SweepOnce(ctx, future); err != nil || rep.MessagesVacuumed != 0 {
		t.Fatalf("cold sweep = %+v (%v), want zero (within duration)", rep, err)
	}
	if !visible(coldMsg) {
		t.Fatal("coldMsg within its long duration must survive")
	}

	// Phase D — the ladder: a 1-day org default reaches #general and the DM;
	// a -1 (forever) channel policy on #kept and the 3650-day #cold override
	// it (nearest scope wins).
	setPolicy(1, boot.OrgID, 1)
	setPolicy(3, kept, -1)
	if rep, err := janitor.SweepOnce(ctx, future); err != nil || rep.MessagesVacuumed != 2 {
		t.Fatalf("ladder sweep = %+v (%v), want 2 (general + dm)", rep, err)
	}
	if visible(gMsg) || visible(dmSent.MessageID) {
		t.Fatal("general and dm messages must vacuum under the org default")
	}
	if !visible(keptMsg) || !visible(coldMsg) {
		t.Fatal("channel overrides (-1 and 3650d) must outrank the org default")
	}

	// Phase E — legal hold (the load-bearing guard). A fresh #hot message
	// under a custodian hold on its author is NOT vacuumed even though its age
	// passes the 1-day policy.
	heldMsg := send(hot, "under hold")
	var hold compliance.LegalHold
	postJSON(t, ts.URL+"/api/v1/admin/legal-holds", boot.Token,
		map[string]any{"name": "Alice custodian", "custodian_user_id": boot.UserID}, &hold)
	if rep, err := janitor.SweepOnce(ctx, future); err != nil || rep.MessagesVacuumed != 0 {
		t.Fatalf("held sweep = %+v (%v), want zero (hold freezes)", rep, err)
	}
	if d, _, _ := vacuumState(heldMsg); d {
		t.Fatal("a held message must not be vacuumed")
	}
	if !visible(heldMsg) {
		t.Fatal("a held message must stay readable")
	}

	// Releasing the hold lets the next sweep take it.
	if code := postJSONStatus(t, fmt.Sprintf("%s/api/v1/admin/legal-holds/%d/release", ts.URL, hold.ID),
		boot.Token, map[string]any{}); code != http.StatusOK {
		t.Fatalf("release hold = %d", code)
	}
	if rep, err := janitor.SweepOnce(ctx, future); err != nil || rep.MessagesVacuumed != 1 {
		t.Fatalf("post-release sweep = %+v (%v), want the freed message", rep, err)
	}
	if visible(heldMsg) {
		t.Fatal("released-then-aged message must vacuum")
	}

	// By now four messages are vacuumed (hotMsg, gMsg, dmMsg, heldMsg), all
	// stamped at `future`. Phase F — a sweep still inside the 30-day restore
	// window purges none of them.
	withinWindow := future.Add(29 * 24 * time.Hour)
	if rep, err := janitor.SweepOnce(ctx, withinWindow); err != nil || rep.MessagesPurged != 0 {
		t.Fatalf("within-window sweep = %+v (%v), want zero purged", rep, err)
	}
	if !rowExists(gMsg) {
		t.Fatal("a vacuumed message inside its restore window must still exist")
	}

	// A legal hold placed during the window freezes permanent removal. Hold
	// #hot (covers hotMsg + heldMsg), then sweep past the window: the unheld
	// #general and DM messages purge; the held #hot ones stay.
	if _, err := pool.Exec(ctx, `
		INSERT INTO legal_hold (org_id, name, channel_id, created_by)
		VALUES ($1, 'freeze hot', $2, $3)`, boot.OrgID, hot, boot.UserID); err != nil {
		t.Fatalf("insert channel hold: %v", err)
	}
	pastWindow := future.Add(31 * 24 * time.Hour)
	if rep, err := janitor.SweepOnce(ctx, pastWindow); err != nil || rep.MessagesPurged != 2 {
		t.Fatalf("past-window sweep = %+v (%v), want 2 purged (general + dm)", rep, err)
	}
	if rowExists(gMsg) || rowExists(dmSent.MessageID) {
		t.Fatal("past-window unheld messages must be permanently removed")
	}
	if !rowExists(hotMsg) || !rowExists(heldMsg) {
		t.Fatal("held vacuumed messages must NOT be purged")
	}

	// gMsg's child rows are gone; its audit trail survives (event_log has no
	// FK to message, so created + vacuumed + purged all remain).
	for _, tbl := range []string{"reaction", "saved_item", "message_revision", "message_link_preview"} {
		var n int
		if err := pool.QueryRow(ctx,
			fmt.Sprintf(`SELECT count(*) FROM %s WHERE message_id = $1`, tbl), gMsg).Scan(&n); err != nil {
			t.Fatalf("count %s: %v", tbl, err)
		}
		if n != 0 {
			t.Fatalf("%s rows for purged gMsg = %d, want 0", tbl, n)
		}
	}
	var vac, purg, created int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE verb = 'retention.message_vacuumed'),
		       count(*) FILTER (WHERE verb = 'retention.message_purged'),
		       count(*) FILTER (WHERE verb = 'message.created')
		FROM event_log WHERE entity_type = 1 AND entity_id = $1`, gMsg).Scan(&vac, &purg, &created); err != nil {
		t.Fatalf("audit trail query: %v", err)
	}
	if vac != 1 || purg != 1 || created != 1 {
		t.Fatalf("gMsg audit trail vac=%d purg=%d created=%d, want 1/1/1 (spine survives purge)", vac, purg, created)
	}

	// Phase G — release the hold; the next past-window sweep takes the rest.
	if _, err := pool.Exec(ctx,
		`UPDATE legal_hold SET released_at = now() WHERE name = 'freeze hot'`); err != nil {
		t.Fatalf("release channel hold: %v", err)
	}
	if rep, err := janitor.SweepOnce(ctx, pastWindow); err != nil || rep.MessagesPurged != 2 {
		t.Fatalf("post-release purge = %+v (%v), want hotMsg + heldMsg", rep, err)
	}
	if rowExists(hotMsg) || rowExists(heldMsg) {
		t.Fatal("released vacuumed messages must purge")
	}
}

// userID provisions a channel-less org member by direct insert and returns
// its id (the shared helper adds channel members; this one just needs a user
// to author a DM and stand as a hold custodian).
func userID(t *testing.T, ctx context.Context, pool *pgxpool.Pool, email string, orgID int64, name string) int64 {
	t.Helper()
	var id int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO user_account (org_id, email, full_name, role)
		VALUES ($1, $2, $3, 40) RETURNING id`,
		orgID, email, name).Scan(&id); err != nil {
		t.Fatalf("provision %s: %v", email, err)
	}
	return id
}
