package rest

import (
	"context"
	"expvar"
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
	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/internal/eventlog"
	"github.com/abhinavjha0239/weft/internal/gateway"
	"github.com/abhinavjha0239/weft/internal/platform/metrics"
)

// deliverabilityMembers sizes the big channel: large enough that an
// O(members) candidate scan is unmistakable against the O(reasons) bound,
// small enough to provision in one set-based INSERT.
const deliverabilityMembers = 5000

// scannedCandidates reads the materializer's candidate-scan counter (the
// expvar driver publishes a label-less instrument as a bare Float).
// The counter is process-global and monotone, so assertions use deltas.
func scannedCandidates(t *testing.T) float64 {
	t.Helper()
	v := expvar.Get("notification_candidates_scanned_total")
	if v == nil {
		return 0
	}
	f, ok := v.(*expvar.Float)
	if !ok {
		t.Fatal("candidate counter is not an expvar.Float")
	}
	return f.Value()
}

// TestDeliverabilitySet: the F-17 O(1) invariant — per-message notification
// cost is O(actual reasons on the message), independent of channel
// membership. (1) One mention in a big channel where nobody holds a
// non-default reason yields exactly ONE notification row and an O(reasons)
// candidate scan, NOT O(members). (2) level=all is one set row: setting it
// gains the row and the next message notifies; unsetting removes both.
// Plus the pre-mortem safety net: the reconcile sweep repairs (and would
// log) a diverged set in both directions.
func TestDeliverabilitySet(t *testing.T) {
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
	runner.SetMetrics(metrics.NewExpvar())
	go runner.Run(ctx)
	permsSvc := perms.New(pool)
	msgSvc := messaging.New(pool, permsSvc)
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
		"org_slug": "dlv", "email": "a@dlv.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"bob@dlv.test", "Bob Ray", "bobdlvtok")
	charlieTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"charlie@dlv.test", "Charlie Kim", "charliedlvtok")
	var bobID, charlieID int64
	_ = pool.QueryRow(ctx, `SELECT id FROM user_account WHERE org_id=$1 AND email='bob@dlv.test'`,
		boot.OrgID).Scan(&bobID)
	_ = pool.QueryRow(ctx, `SELECT id FROM user_account WHERE org_id=$1 AND email='charlie@dlv.test'`,
		boot.OrgID).Scan(&charlieID)

	// Bulk-provision the big membership set-based (these users only ever
	// receive, so no sessions or role groups are needed). All on default
	// settings: none of them belongs in the deliverability set.
	if _, err := pool.Exec(ctx, `
		INSERT INTO user_account (org_id, kind, email, full_name, role)
		SELECT $1, 1, 'u' || i || '@dlv.test', 'User ' || i, 40
		FROM generate_series(1, $2) i`, boot.OrgID, deliverabilityMembers); err != nil {
		t.Fatalf("bulk users: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO channel_member (channel_id, user_id)
		SELECT $1, id FROM user_account
		WHERE org_id = $2 AND email LIKE 'u%@dlv.test'`,
		boot.ChannelID, boot.OrgID); err != nil {
		t.Fatalf("bulk members: %v", err)
	}

	waitForConsumer := func() {
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
	rowsFor := func(msgID int64) (n int, kind int16, uid int64) {
		t.Helper()
		if err := pool.QueryRow(ctx, `
			SELECT count(*), COALESCE(min(kind), 0), COALESCE(min(user_id), 0)
			FROM notification
			WHERE org_id = $1 AND entity_type = 1 AND entity_id = $2`,
			boot.OrgID, msgID).Scan(&n, &kind, &uid); err != nil {
			t.Fatalf("rows for %d: %v", msgID, err)
		}
		return n, kind, uid
	}
	setRows := func(userID int64, reason int16) int {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM channel_deliverability
			WHERE channel_id = $1 AND user_id = $2 AND reason = $3 AND medium = 1`,
			boot.ChannelID, userID, reason).Scan(&n); err != nil {
			t.Fatalf("set rows: %v", err)
		}
		return n
	}

	// (1) One mention among deliverabilityMembers defaults: exactly ONE
	// notification row, and the candidate scan is O(reasons) — the set is
	// empty, so the per-message work cannot depend on membership. (The
	// first message also pays the one-time lazy build; the counter
	// measures only the per-message candidate path.)
	waitForConsumer()
	before := scannedCandidates(t)
	mentionMsg := send("welcome aboard @**Bob Ray**")
	waitForConsumer()
	if n, kind, uid := rowsFor(mentionMsg); n != 1 || kind != notification.KindMention || uid != bobID {
		t.Fatalf("mention in big channel: rows=%d kind=%d user=%d, want exactly bob's one mention row", n, kind, uid)
	}
	if delta := scannedCandidates(t) - before; delta > 20 {
		t.Fatalf("candidate scan is O(members): delta=%.0f for one message in a %d-member channel, want O(reasons) <= 20",
			delta, deliverabilityMembers)
	}
	var builtAt *time.Time
	if err := pool.QueryRow(ctx, `SELECT deliverability_built_at FROM channel WHERE id = $1`,
		boot.ChannelID).Scan(&builtAt); err != nil || builtAt == nil {
		t.Fatalf("lazy build did not stamp deliverability_built_at (%v, %v)", builtAt, err)
	}

	// (2) level=all is exactly one set row, added and removed by the
	// settings patch — and resolution follows it.
	if code := putJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/notification", ts.URL, boot.ChannelID),
		charlieTok, map[string]any{"level": 1}); code != http.StatusOK {
		t.Fatalf("charlie level=all = %d", code)
	}
	if n := setRows(charlieID, 2); n != 1 {
		t.Fatalf("level=all set rows = %d, want 1", n)
	}
	before = scannedCandidates(t)
	activityMsg := send("routine update, no mentions")
	waitForConsumer()
	if n, kind, uid := rowsFor(activityMsg); n != 1 || kind != notification.KindChannelActivity || uid != charlieID {
		t.Fatalf("level=all message: rows=%d kind=%d user=%d, want charlie's one activity row", n, kind, uid)
	}
	if delta := scannedCandidates(t) - before; delta < 1 || delta > 20 {
		t.Fatalf("candidate scan delta=%.0f, want 1..20 (charlie is the only candidate)", delta)
	}
	if code := putJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/notification", ts.URL, boot.ChannelID),
		charlieTok, map[string]any{"level": 0}); code != http.StatusOK {
		t.Fatalf("charlie level reset = %d", code)
	}
	if n := setRows(charlieID, 2); n != 0 {
		t.Fatalf("set rows after level reset = %d, want 0 (row gone)", n)
	}
	quietMsg := send("back to defaults")
	waitForConsumer()
	if n, _, _ := rowsFor(quietMsg); n != 0 {
		t.Fatalf("post-reset message minted %d rows, want 0", n)
	}

	// Reconciliation (the pre-mortem drift net): a lost row is restored
	// and a bogus row is removed by one sweep.
	if code := putJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/notification", ts.URL, boot.ChannelID),
		charlieTok, map[string]any{"level": 1}); code != http.StatusOK {
		t.Fatalf("charlie level=all again = %d", code)
	}
	if _, err := pool.Exec(ctx, `
		DELETE FROM channel_deliverability
		WHERE channel_id = $1 AND user_id = $2 AND reason = 2`,
		boot.ChannelID, charlieID); err != nil {
		t.Fatalf("simulate missing row: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO channel_deliverability (channel_id, user_id, reason, medium, org_id)
		VALUES ($1, $2, 2, 1, $3)`, boot.ChannelID, bobID, boot.OrgID); err != nil {
		t.Fatalf("simulate extra row: %v", err)
	}
	if err := notification.NewDeliverability(pool, slog.Default()).ReconcileOnce(ctx); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if n := setRows(charlieID, 2); n != 1 {
		t.Fatalf("reconcile did not restore charlie's row (rows=%d)", n)
	}
	if n := setRows(bobID, 2); n != 0 {
		t.Fatalf("reconcile did not remove bob's bogus row (rows=%d)", n)
	}

	// (3) Batch coalescing (F-17b): a synthetic bulk producer emits N item
	// events (message payloads with distinct entity ids) all stamped with
	// ONE batch_id in the hint — each recipient gets ONE digest row per
	// reason class, never N. Charlie still holds level=all from the
	// reconcile step, so the batch exercises the mention lane (bob) and
	// the activity lane (charlie) at once.
	waitForConsumer()
	const batchItems = 5
	const bulkBatchID = 777
	rootThread := channelRootThread(t, ctx, pool, boot.ChannelID)
	err = db.WithTx(ctx, pool, func(tx pgx.Tx) error {
		for i := 0; i < batchItems; i++ {
			if _, err := eventlog.Append(ctx, tx, eventlog.Event{
				OrgID: boot.OrgID, ActorKind: enum.ActorHuman, ActorID: &boot.UserID,
				EntityType: enum.EntityMessage, EntityID: int64(900000 + i),
				Verb: "message.created",
				Payload: eventlog.MustPayload(map[string]any{
					"message_id": 900000 + i, "thread_id": rootThread,
					"channel_id": boot.ChannelID, "mentions": []int64{bobID}}),
				Hint: eventlog.MustPayload(map[string]any{"batch_id": bulkBatchID}),
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("append bulk batch: %v", err)
	}
	waitForConsumer()
	batchRows := func(userID int64) (n int, kind int16, entity int64, gotBatch *int64) {
		t.Helper()
		if err := pool.QueryRow(ctx, `
			SELECT count(*), COALESCE(min(kind), 0), COALESCE(min(entity_id), 0), min(batch_id)
			FROM notification
			WHERE org_id = $1 AND user_id = $2 AND entity_type = 1
			  AND entity_id BETWEEN 900000 AND 900004`,
			boot.OrgID, userID).Scan(&n, &kind, &entity, &gotBatch); err != nil {
			t.Fatalf("batch rows: %v", err)
		}
		return n, kind, entity, gotBatch
	}
	if n, kind, entity, gotBatch := batchRows(bobID); n != 1 || kind != notification.KindMention ||
		entity != 900000 || gotBatch == nil || *gotBatch != bulkBatchID {
		t.Fatalf("bulk event minted %d rows for bob (kind=%d entity=%d batch=%v), want ONE digest row anchored on the first item",
			n, kind, entity, gotBatch)
	}
	if n, kind, _, gotBatch := batchRows(charlieID); n != 1 || kind != notification.KindChannelActivity ||
		gotBatch == nil || *gotBatch != bulkBatchID {
		t.Fatalf("bulk event minted %d activity rows for charlie (kind=%d batch=%v), want ONE digest row",
			n, kind, gotBatch)
	}
	var totalBatch int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM notification
		WHERE org_id = $1 AND entity_type = 1 AND entity_id BETWEEN 900000 AND 900004`,
		boot.OrgID).Scan(&totalBatch); err != nil {
		t.Fatalf("total batch rows: %v", err)
	}
	if totalBatch != 2 {
		t.Fatalf("bulk event minted %d rows total for %d items, want 2 (one digest per recipient)",
			totalBatch, batchItems)
	}

	// (4) The batch-id FRESHNESS contract (notification.BatchHintForEvent).
	// notification_batch_dedupe_key (user_id, kind, batch_id) is unique
	// FOREVER with no time component, and the digest insert conflicts with an
	// open DO NOTHING — so the two directions below are the whole contract,
	// and a future producer that gets it wrong fails HERE instead of silently
	// in production:
	//   · successive bulk operations mint FRESH ids and BOTH deliver;
	//   · the SAME id restamped coalesces into the one row already recorded.
	// RED/GREEN: make BatchHintForEvent ignore its argument and stamp a
	// constant — the stable-entity-id foot-gun — and the per-operation
	// digest count drops to 1: the second operation delivers NOTHING, with
	// no error anywhere, which is exactly the silent permanent suppression
	// the helper exists to prevent.
	//
	// bulkOp emits one bulk operation the way a real producer must: the
	// operation's OWN event first, then its item events, every one stamped
	// with the hint minted from that trigger id. A nonzero reuse deliberately
	// restamps an ALREADY-SPENT batch id instead (the coalescing direction).
	bulkOp := func(sprintID, firstEntity int64, items int, reuse int64) int64 {
		t.Helper()
		batch := reuse
		if err := db.WithTx(ctx, pool, func(tx pgx.Tx) error {
			if batch == 0 {
				id, err := eventlog.Append(ctx, tx, eventlog.Event{
					OrgID: boot.OrgID, ActorKind: enum.ActorHuman, ActorID: &boot.UserID,
					EntityType: enum.EntitySprint, EntityID: sprintID,
					Verb: "sprint.closed",
				})
				if err != nil {
					return err
				}
				batch = id
			}
			hint := notification.BatchHintForEvent(batch)
			for i := 0; i < items; i++ {
				entity := firstEntity + int64(i)
				if _, err := eventlog.Append(ctx, tx, eventlog.Event{
					OrgID: boot.OrgID, ActorKind: enum.ActorHuman, ActorID: &boot.UserID,
					EntityType: enum.EntityMessage, EntityID: entity,
					Verb: "message.created",
					Payload: eventlog.MustPayload(map[string]any{
						"message_id": entity, "thread_id": rootThread,
						"channel_id": boot.ChannelID, "mentions": []int64{bobID}}),
					Hint: hint,
				}); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatalf("bulk op: %v", err)
		}
		return batch
	}
	rowsForBatch := func(userID, batch int64) (n int, entity int64) {
		t.Helper()
		if err := pool.QueryRow(ctx, `
			SELECT count(*), COALESCE(min(entity_id), 0) FROM notification
			WHERE org_id = $1 AND user_id = $2 AND batch_id = $3`,
			boot.OrgID, userID, batch).Scan(&n, &entity); err != nil {
			t.Fatalf("rows for batch %d: %v", batch, err)
		}
		return n, entity
	}

	firstOp := bulkOp(9001, 910000, 3, 0)
	waitForConsumer()
	secondOp := bulkOp(9002, 920000, 3, 0)
	waitForConsumer()
	if firstOp == secondOp || firstOp <= 0 {
		t.Fatalf("successive operations minted batch ids %d and %d, want two distinct positive ids",
			firstOp, secondOp)
	}
	// THE finding, stated without reference to the minted ids: each
	// operation must leave the recipient exactly one digest. A producer
	// that reused a stable id leaves ONE row total — the second sweep is
	// suppressed permanently, silently, for this (user, kind) forever.
	var opDigests int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM notification
		WHERE org_id = $1 AND user_id = $2 AND entity_type = 1
		  AND entity_id BETWEEN 910000 AND 920002`, boot.OrgID, bobID).Scan(&opDigests); err != nil {
		t.Fatalf("per-operation digests: %v", err)
	}
	if opDigests != 2 {
		t.Fatalf("two successive bulk operations left bob %d digest rows, want 2 (one per operation) — a batch id that is not minted fresh silently drops every later sweep",
			opDigests)
	}
	if n, entity := rowsForBatch(bobID, firstOp); n != 1 || entity != 910000 {
		t.Fatalf("first operation: %d rows for batch %d (anchor entity %d), want ONE digest anchored on 910000",
			n, firstOp, entity)
	}
	if n, entity := rowsForBatch(bobID, secondOp); n != 1 || entity != 920000 {
		t.Fatalf("second operation: %d rows for batch %d (anchor entity %d), want ONE digest anchored on 920000 — a fresh batch id must deliver again",
			n, secondOp, entity)
	}

	// The coalescing direction: restamping a SPENT batch id records nothing
	// new — that is the feature, and it is what makes reuse fatal.
	bulkOp(0, 930000, 2, firstOp)
	waitForConsumer()
	if n, _ := rowsForBatch(bobID, firstOp); n != 1 {
		t.Fatalf("restamping batch %d left %d rows, want 1 (the same batch coalesces)", firstOp, n)
	}
	var restamped int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM notification
		WHERE org_id = $1 AND entity_type = 1 AND entity_id BETWEEN 930000 AND 930001`,
		boot.OrgID).Scan(&restamped); err != nil {
		t.Fatalf("restamped rows: %v", err)
	}
	if restamped != 0 {
		t.Fatalf("restamping a spent batch minted %d new rows, want 0", restamped)
	}
}
