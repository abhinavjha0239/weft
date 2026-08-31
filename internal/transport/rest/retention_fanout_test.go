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

	"github.com/abhinavjha0239/weft/internal/domain/compliance"
	"github.com/abhinavjha0239/weft/internal/domain/dm"
	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/eventlog"
	"github.com/abhinavjha0239/weft/internal/gateway"
	"github.com/abhinavjha0239/weft/internal/platform/blob"
)

// TestRetentionEventFanout pins the AUDIENCE of the two retention verbs, which
// the P-17 lane shipped without: `retention.message_vacuumed` added channel_id
// only when the message had one — so a DM message's event named no container
// and the gateway fanned a DM thread id to every connection in the org — and
// `retention.message_purged` carried `{message_id}` alone, for public, private
// and DM messages alike. The gateway needs no change for either: it already
// gates on channel_id / dm_space_id, and on the created_at of the message an
// event is about; the producer simply has to say them.
//
// Carol is the container probe and she is a LIVE fan target throughout: she is
// a member of #general only, and every drain terminates on a #general message
// logged AFTER the retention events. The stream is id-ordered, so those events
// were provably offered to her connection and dropped by her own filter — and
// her #general message's OWN retention event, which she must receive, is the
// positive anchor that stops the negatives from passing vacuously.
//
// Dana is the FLOOR probe: a protected-history member of #ledger. Retention
// events are the sharpest case for the ADR-008 C-2 floor, because their own
// timestamp is the SWEEP clock and the message they name is by definition old
// enough to have aged out — so judging the event's stamp clears every floor,
// every time. She must hear about the #ledger message created after her join
// and never about the one created before it.
func TestRetentionEventFanout(t *testing.T) {
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
		"org_slug": "retfan", "email": "alice@retfan.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	if code := putJSON(t, ts.URL+"/api/v1/admin/verbs", boot.Token,
		map[string]any{"verb": "compliance_officer", "group": "role:admins"}); code != http.StatusOK {
		t.Fatalf("grant compliance_officer = %d", code)
	}
	// Carol: an ordinary member of #general and of NOTHING else. Bob exists
	// only to give the DM a second participant.
	carolTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"carol@retfan.test", "Carol Diaz", "retfan-carol-tok")
	bobID := userID(t, ctx, pool, "bob@retfan.test", boot.OrgID, "Bob Ray")

	var vault struct {
		ChannelID int64 `json:"channel_id"`
	}
	postJSON(t, ts.URL+"/api/v1/channels", boot.Token,
		map[string]any{"name": "vault", "visibility": "private"}, &vault)
	var conv struct {
		ID           int64 `json:"id"`
		RootThreadID int64 `json:"root_thread_id"`
	}
	postJSON(t, ts.URL+"/api/v1/dms", boot.Token,
		map[string]any{"user_ids": []int64{bobID}}, &conv)
	if conv.ID == 0 || conv.RootThreadID == 0 {
		t.Fatalf("open dm = %+v", conv)
	}

	// #ledger is private AND protected: dana's view starts at her join stamp,
	// which is what the retention events' created_at has to respect.
	var ledger struct {
		ChannelID int64 `json:"channel_id"`
	}
	postJSON(t, ts.URL+"/api/v1/channels", boot.Token, map[string]any{
		"name": "ledger", "visibility": "private", "protected": true}, &ledger)
	ledgerPre := sendChannel(t, ts.URL, boot.Token, ledger.ChannelID, "before dana")
	var inv identity.Invite
	postJSON(t, ts.URL+"/api/v1/invites", boot.Token, map[string]any{
		"role": 40, "channel_ids": []int64{ledger.ChannelID}}, &inv)
	var dana identity.AcceptInviteResult
	postJSON(t, ts.URL+"/api/v1/invites/accept", "", map[string]any{
		"token": inv.Token, "email": "dana@retfan.test", "password": "password123",
		"full_name": "Dana Late"}, &dana)
	var stamp *time.Time
	if err := pool.QueryRow(ctx, `
		SELECT history_from FROM channel_member
		WHERE channel_id = $1 AND user_id = $2`,
		ledger.ChannelID, dana.UserID).Scan(&stamp); err != nil || stamp == nil {
		t.Fatalf("dana history_from = %v (%v), want stamped", stamp, err)
	}
	ledgerPost := sendChannel(t, ts.URL, boot.Token, ledger.ChannelID, "after dana")

	genMsg := sendChannel(t, ts.URL, boot.Token, boot.ChannelID, "general note")
	vaultMsg := sendChannel(t, ts.URL, boot.Token, vault.ChannelID, "vault note")
	var dmSent struct {
		MessageID int64 `json:"message_id"`
	}
	postJSON(t, fmt.Sprintf("%s/api/v1/threads/%d/messages", ts.URL, conv.RootThreadID),
		boot.Token, map[string]any{"content": "dm note"}, &dmSent)
	if dmSent.MessageID == 0 {
		t.Fatal("dm send failed")
	}
	// Carol must genuinely lack access to the two she is not allowed to hear
	// about, or the negatives below would be about nothing.
	for _, id := range []int64{vaultMsg, dmSent.MessageID} {
		if code := getJSON(t, fmt.Sprintf("%s/api/v1/messages/%d", ts.URL, id),
			carolTok, nil); code != http.StatusNotFound {
			t.Fatalf("carol reading message %d = %d, want 404 (she must not have access)", id, code)
		}
	}
	if code := getJSON(t, fmt.Sprintf("%s/api/v1/messages/%d", ts.URL, genMsg),
		carolTok, nil); code != http.StatusOK {
		t.Fatalf("carol reading the #general message = %d, want 200 (the anchor)", code)
	}
	// Capture the created_at of all three before the purge lane removes the
	// rows: the payload assertions compare against the ROW, never against what
	// the producer reported.
	createdAt := map[int64]time.Time{}
	for _, id := range []int64{genMsg, vaultMsg, dmSent.MessageID, ledgerPre, ledgerPost} {
		var at time.Time
		if err := pool.QueryRow(ctx,
			`SELECT created_at FROM message WHERE id = $1`, id).Scan(&at); err != nil {
			t.Fatalf("created_at %d: %v", id, err)
		}
		createdAt[id] = at
	}
	// The two #ledger messages must actually straddle dana's stamp, or both
	// halves of her assertions would be about the same side of the floor.
	if !createdAt[ledgerPre].Before(*stamp) || createdAt[ledgerPost].Before(*stamp) {
		t.Fatalf("fixture does not straddle dana's stamp: pre=%v post=%v stamp=%v",
			createdAt[ledgerPre], createdAt[ledgerPost], *stamp)
	}

	// One org-wide 1-day policy reaches all three containers.
	if code := putJSON(t, ts.URL+"/api/v1/admin/retention-policies", boot.Token,
		map[string]any{"scope_type": 1, "scope_id": boot.OrgID,
			"duration_days": 1, "keep_edits": true}); code != http.StatusOK {
		t.Fatalf("org retention policy = %d", code)
	}

	carol := dialClientLast(t, ctx, ts.URL, carolTok, "-1")
	defer carol.conn.CloseNow()
	carol.waitFor(t, "ready")
	danaC := dialClientLast(t, ctx, ts.URL, dana.Token, "-1")
	defer danaC.conn.CloseNow()
	danaC.waitFor(t, "ready")

	future := time.Now().Add(48 * time.Hour)
	if rep, err := janitor.SweepOnce(ctx, future); err != nil || rep.MessagesVacuumed != 5 {
		t.Fatalf("vacuum sweep = %+v (%v), want 5 vacuumed (general + vault + dm + both ledger)",
			rep, err)
	}
	vacTail := sendChannel(t, ts.URL, boot.Token, boot.ChannelID, "after the vacuum")
	ledgerVacTail := sendChannel(t, ts.URL, boot.Token, ledger.ChannelID, "dana drain")

	sawVac := drainUntil(t, carol, vacTail)
	if !hasMessageEvent(sawVac, "retention.message_vacuumed", genMsg) {
		t.Fatalf("a member of #general did not receive the vacuum of message %d; without this anchor every negative below is vacuous. saw %v",
			genMsg, envTypes(sawVac))
	}
	if hasMessageEvent(sawVac, "retention.message_vacuumed", vaultMsg) {
		t.Fatalf("a non-member received the vacuum of PRIVATE-channel message %d; the event must carry channel_id so the membership gate applies. saw %v",
			vaultMsg, envTypes(sawVac))
	}
	if hasMessageEvent(sawVac, "retention.message_vacuumed", dmSent.MessageID) {
		t.Fatalf("a non-participant received the vacuum of DM message %d; the event must carry dm_space_id so the participation gate applies. saw %v",
			dmSent.MessageID, envTypes(sawVac))
	}

	sawDanaVac := drainUntil(t, danaC, ledgerVacTail)
	if !hasMessageEvent(sawDanaVac, "retention.message_vacuumed", ledgerPost) {
		t.Fatalf("a protected-history member did not receive the vacuum of post-join message %d; the floor must not blanket-withhold retention events. saw %v",
			ledgerPost, envTypes(sawDanaVac))
	}
	if hasMessageEvent(sawDanaVac, "retention.message_vacuumed", ledgerPre) {
		t.Fatalf("a protected-history member received the vacuum of PRE-join message %d; the sweep clock is far above her stamp, so the event must carry the message's created_at. saw %v",
			ledgerPre, envTypes(sawDanaVac))
	}

	// The payloads themselves, independent of who happened to be connected.
	assertContainer(t, ctx, pool, "retention.message_vacuumed", genMsg,
		"channel_id", boot.ChannelID, createdAt[genMsg])
	assertContainer(t, ctx, pool, "retention.message_vacuumed", vaultMsg,
		"channel_id", vault.ChannelID, createdAt[vaultMsg])
	assertContainer(t, ctx, pool, "retention.message_vacuumed", dmSent.MessageID,
		"dm_space_id", conv.ID, createdAt[dmSent.MessageID])

	// Past the restore window the purge lane removes all five rows (and vacuums
	// the terminators, which are now over a day old on the sweep clock).
	pastWindow := future.Add(31 * 24 * time.Hour)
	if rep, err := janitor.SweepOnce(ctx, pastWindow); err != nil || rep.MessagesPurged != 5 {
		t.Fatalf("purge sweep = %+v (%v), want 5 purged", rep, err)
	}
	purgeTail := sendChannel(t, ts.URL, boot.Token, boot.ChannelID, "after the purge")
	ledgerPurgeTail := sendChannel(t, ts.URL, boot.Token, ledger.ChannelID, "dana purge drain")

	sawPurge := drainUntil(t, carol, purgeTail)
	if !hasMessageEvent(sawPurge, "retention.message_purged", genMsg) {
		t.Fatalf("a member of #general did not receive the purge of message %d; the anchor for the two negatives below. saw %v",
			genMsg, envTypes(sawPurge))
	}
	if hasMessageEvent(sawPurge, "retention.message_purged", vaultMsg) {
		t.Fatalf("a non-member received the purge of PRIVATE-channel message %d; `{message_id}` alone fans org-wide. saw %v",
			vaultMsg, envTypes(sawPurge))
	}
	if hasMessageEvent(sawPurge, "retention.message_purged", dmSent.MessageID) {
		t.Fatalf("a non-participant received the purge of DM message %d; `{message_id}` alone fans org-wide. saw %v",
			dmSent.MessageID, envTypes(sawPurge))
	}
	sawDanaPurge := drainUntil(t, danaC, ledgerPurgeTail)
	if !hasMessageEvent(sawDanaPurge, "retention.message_purged", ledgerPost) {
		t.Fatalf("a protected-history member did not receive the purge of post-join message %d. saw %v",
			ledgerPost, envTypes(sawDanaPurge))
	}
	if hasMessageEvent(sawDanaPurge, "retention.message_purged", ledgerPre) {
		t.Fatalf("a protected-history member received the purge of PRE-join message %d; the purge lane must carry created_at too, read under the lock before the DELETE. saw %v",
			ledgerPre, envTypes(sawDanaPurge))
	}

	assertContainer(t, ctx, pool, "retention.message_purged", genMsg,
		"channel_id", boot.ChannelID, createdAt[genMsg])
	assertContainer(t, ctx, pool, "retention.message_purged", vaultMsg,
		"channel_id", vault.ChannelID, createdAt[vaultMsg])
	assertContainer(t, ctx, pool, "retention.message_purged", dmSent.MessageID,
		"dm_space_id", conv.ID, createdAt[dmSent.MessageID])

}

// assertContainer checks one event's payload directly: exactly ONE container
// key, holding wantID, plus the referenced message's created_at. The other
// container key must be ABSENT — a channel event that also claimed a
// dm_space_id (or the reverse) would face a gate for a container it does not
// belong to, which is a different bug from the one being fixed.
func assertContainer(t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	verb string, msgID int64, wantKey string, wantID int64, wantAt time.Time) {
	t.Helper()
	otherKey := "dm_space_id"
	if wantKey == "dm_space_id" {
		otherKey = "channel_id"
	}
	var gotID *int64
	var hasOther bool
	var gotAt *time.Time
	if err := pool.QueryRow(ctx, `
		SELECT (payload->>$3)::bigint, payload ? $4, (payload->>$5)::timestamptz
		FROM event_log WHERE verb = $1 AND entity_id = $2`,
		verb, msgID, wantKey, otherKey, eventlog.MessageCreatedAtKey).
		Scan(&gotID, &hasOther, &gotAt); err != nil {
		t.Fatalf("%s payload for message %d: %v", verb, msgID, err)
	}
	if gotID == nil || *gotID != wantID {
		t.Fatalf("%s for message %d: %s = %v, want %d — without it the gateway has no container to gate on",
			verb, msgID, wantKey, gotID, wantID)
	}
	if hasOther {
		t.Fatalf("%s for message %d carries %s as well as %s; it belongs to one container",
			verb, msgID, otherKey, wantKey)
	}
	if gotAt == nil || !gotAt.Equal(wantAt) {
		t.Fatalf("%s for message %d: %s = %v, want the message row's %v — the sweep clock is far above the message's age, so without this every protected-history floor is cleared",
			verb, msgID, eventlog.MessageCreatedAtKey, gotAt, wantAt)
	}
}
