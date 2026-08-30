package rest

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/domain/worktrack"
	"github.com/abhinavjha0239/weft/internal/gateway"
)

// TestGatewayReadACL pins the realtime read ACL against real Postgres and real
// WebSockets (P-46). The load-bearing shape is the NEGATIVE, written the
// P-33/P-34 way: every connection that must NOT receive an event is a LIVE fan
// target that demonstrably receives a LATER event on the same stream. Because
// the stream is ordered by event id, seeing the later event proves the earlier
// one was already offered to that connection and dropped by its own filter —
// so no assertion here can pass vacuously by "nothing was ever sent".
//
// Subtests:
//   - "protected history floor": ADR-008 C-2 / F-16b. A member of a PROTECTED
//     channel who resumes from before their join must not replay pre-join
//     events, while the SAME user's shared-history channel replays fully, and
//     the boundary survives leave→rejoin (the preserved channel_member row).
//   - "membership refresh mid-connection": a live connection that joins a
//     channel starts receiving it without reconnecting, and received nothing
//     from it before.
//   - "space visibility": space-scoped events (space/work item/sprint/field
//     def) have no channel or DM to gate on and used to fan ORG-WIDE. They now
//     resolve a Space against the connection's space set — a member gets them
//     (after a mid-connection refresh driven by space.created), a GUEST gets
//     none, an event whose space_id cannot be resolved gets withheld rather
//     than fanned, and a work-item event is withheld entirely while the org
//     defines a visibility_scope the gateway has no evaluator for, without
//     blacking out the space events that cannot carry one.
func TestGatewayReadACL(t *testing.T) {
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
	go hub.Run(ctx)
	permsSvc := perms.New(pool)
	msgSvc := messaging.New(pool, permsSvc)
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity:  identity.New(pool, permsSvc),
		Messaging: msgSvc,
		Worktrack: worktrack.New(pool, permsSvc, msgSvc),
	}))
	defer ts.Close()

	var boot struct {
		OrgID     int64  `json:"org_id"`
		UserID    int64  `json:"user_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "acl", "email": "alice@acl.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)

	t.Run("protected history floor", func(t *testing.T) {
		// #ledger is private+protected (history_mode 2): each invited member's
		// view starts at their join stamp. #general (bootstrap) is shared.
		var ledger struct {
			ChannelID int64 `json:"channel_id"`
		}
		postJSON(t, ts.URL+"/api/v1/channels", boot.Token, map[string]any{
			"name": "ledger", "visibility": "private", "protected": true}, &ledger)

		// Two messages BEFORE mia exists: one in the protected channel (which
		// her floor must hide) and one in the shared channel (which it must
		// NOT — the contrast that proves the floor is per-channel, not "mia
		// sees no history at all").
		genPre := sendChannel(t, ts.URL, boot.Token, boot.ChannelID, "shared pre-join")
		protPre := sendChannel(t, ts.URL, boot.Token, ledger.ChannelID, "protected pre-join")

		var inv identity.Invite
		postJSON(t, ts.URL+"/api/v1/invites", boot.Token, map[string]any{
			"role": 40, "channel_ids": []int64{ledger.ChannelID}}, &inv)
		var mia identity.AcceptInviteResult
		postJSON(t, ts.URL+"/api/v1/invites/accept", "", map[string]any{
			"token": inv.Token, "email": "mia@acl.test", "password": "password123",
			"full_name": "Mia New"}, &mia)
		// The stamp is the boundary under test; assert it exists so a schema
		// change that stops stamping cannot silently make this test vacuous.
		var stamp *time.Time
		if err := pool.QueryRow(ctx, `
			SELECT history_from FROM channel_member
			WHERE channel_id = $1 AND user_id = $2`,
			ledger.ChannelID, mia.UserID).Scan(&stamp); err != nil || stamp == nil {
			t.Fatalf("mia history_from = %v (%v), want stamped", stamp, err)
		}

		protPost := sendChannel(t, ts.URL, boot.Token, ledger.ChannelID, "protected post-join")

		// Full replay from the very beginning of the org's log.
		mc := dialClientLast(t, ctx, ts.URL, mia.Token, "0")
		defer mc.conn.CloseNow()
		seen := drainMessagesUntil(t, mc, protPost)
		if slices.Contains(seen, protPre) {
			t.Fatalf("protected pre-join message %d replayed to a member who joined after it; got %v",
				protPre, seen)
		}
		if !slices.Contains(seen, genPre) {
			t.Fatalf("shared-history pre-join message %d was withheld; the floor must be per-channel. got %v",
				genPre, seen)
		}

		// The boundary INSTANT itself. history_from is a timestamp and the
		// stream is ordered by id, so the only way to construct an event at
		// exactly the stamp is to backdate its domain time — which is also the
		// only way to pin inclusive-vs-exclusive, the off-by-one that is a
		// silent leak one way and a silent gap the other. REST's rule is
		// `created_at >= history_from` (messaging.ListMessages), so an event AT
		// the stamp is VISIBLE; the gateway must answer the same.
		atBoundary := sendChannel(t, ts.URL, boot.Token, ledger.ChannelID, "exactly at the stamp")
		if ct, err := pool.Exec(ctx, `
			UPDATE event_log SET occurred_at = $2
			WHERE verb = 'message.created' AND (payload->>'message_id')::bigint = $1`,
			atBoundary, *stamp); err != nil || ct.RowsAffected() != 1 {
			t.Fatalf("backdate to the boundary: %v rows (%v)", ct.RowsAffected(), err)
		}
		after := sendChannel(t, ts.URL, boot.Token, ledger.ChannelID, "strictly after")
		bc := dialClientLast(t, ctx, ts.URL, mia.Token, "0")
		defer bc.conn.CloseNow()
		seenB := drainMessagesUntil(t, bc, after)
		if !slices.Contains(seenB, atBoundary) {
			t.Fatalf("message %d at exactly history_from was withheld; the floor must be INCLUSIVE (REST shows it). got %v",
				atBoundary, seenB)
		}

		// Leave and rejoin the SAME preserved membership row (#123 shape): the
		// original stamp survives, so the pre-join message stays hidden even
		// though the rejoin is brand new.
		if code := postJSONStatus(t, fmt.Sprintf("%s/api/v1/channels/%d/leave", ts.URL, ledger.ChannelID),
			mia.Token, map[string]any{}); code != http.StatusOK {
			t.Fatalf("mia leave = %d, want 200", code)
		}
		if _, err := pool.Exec(ctx, `
			UPDATE channel_member SET unsubscribed_at = NULL
			WHERE channel_id = $1 AND user_id = $2 AND unsubscribed_at IS NOT NULL`,
			ledger.ChannelID, mia.UserID); err != nil {
			t.Fatalf("reactivate: %v", err)
		}
		var afterRejoin *time.Time
		if err := pool.QueryRow(ctx, `
			SELECT history_from FROM channel_member
			WHERE channel_id = $1 AND user_id = $2`,
			ledger.ChannelID, mia.UserID).Scan(&afterRejoin); err != nil ||
			afterRejoin == nil || !afterRejoin.Equal(*stamp) {
			t.Fatalf("history_from after rejoin = %v (%v), want the preserved %v",
				afterRejoin, err, stamp)
		}
		rejoinPost := sendChannel(t, ts.URL, boot.Token, ledger.ChannelID, "after rejoin")

		mc2 := dialClientLast(t, ctx, ts.URL, mia.Token, "0")
		defer mc2.conn.CloseNow()
		seen2 := drainMessagesUntil(t, mc2, rejoinPost)
		if slices.Contains(seen2, protPre) {
			t.Fatalf("rejoin lost the preserved floor: pre-join message %d replayed; got %v",
				protPre, seen2)
		}
		if !slices.Contains(seen2, protPost) {
			t.Fatalf("post-join message %d missing after rejoin; got %v", protPost, seen2)
		}
	})

	t.Run("membership refresh mid-connection", func(t *testing.T) {
		bobTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
			"bob@acl.test", "Bob Ray", "acl-bob-tok")
		var ops struct {
			ChannelID int64 `json:"channel_id"`
		}
		postJSON(t, ts.URL+"/api/v1/channels", boot.Token,
			map[string]any{"name": "ops"}, &ops)

		bob := dialClientLast(t, ctx, ts.URL, bobTok, "-1")
		defer bob.conn.CloseNow()
		bob.waitFor(t, "ready")

		// Before joining: the #ops message must not reach bob. He is a LIVE fan
		// target — the #general message that follows it in the log DOES reach
		// him, so the shared batch demonstrably passed through his filter.
		outside := sendChannel(t, ts.URL, boot.Token, ops.ChannelID, "not for bob")
		inside := sendChannel(t, ts.URL, boot.Token, boot.ChannelID, "for bob")
		seen := drainMessagesUntil(t, bob, inside)
		if slices.Contains(seen, outside) {
			t.Fatalf("non-member received message %d from a channel he is not in; got %v",
				outside, seen)
		}

		// Joining refreshes the view on the LIVE connection (no reconnect).
		if code := postJSONStatus(t, fmt.Sprintf("%s/api/v1/channels/%d/join", ts.URL, ops.ChannelID),
			bobTok, map[string]any{}); code != http.StatusOK {
			t.Fatalf("bob join #ops = %d, want 200", code)
		}
		afterJoin := sendChannel(t, ts.URL, boot.Token, ops.ChannelID, "now for bob")
		got := drainMessagesUntil(t, bob, afterJoin)
		if slices.Contains(got, outside) {
			t.Fatalf("the refresh replayed pre-join traffic %d; got %v", outside, got)
		}
	})

	// Space-scoped events have no channel or DM to gate on and used to fan
	// ORG-WIDE regardless of space access or guest status. They now resolve a
	// Space against the connection's own space-visibility set, and every way
	// of failing to resolve one WITHHOLDS.
	t.Run("space visibility", func(t *testing.T) {
		carolTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
			"carol@acl.test", "Carol Diaz", "acl-carol-tok")
		// A guest lives inside one private channel and nothing else (P-5).
		var support struct {
			ChannelID int64 `json:"channel_id"`
		}
		postJSON(t, ts.URL+"/api/v1/channels", boot.Token,
			map[string]any{"name": "support", "private": true}, &support)
		var ginv identity.Invite
		postJSON(t, ts.URL+"/api/v1/invites", boot.Token, map[string]any{
			"role": 50, "channel_ids": []int64{support.ChannelID}}, &ginv)
		var gina identity.AcceptInviteResult
		postJSON(t, ts.URL+"/api/v1/invites/accept", "", map[string]any{
			"token": ginv.Token, "email": "gina@acl.test", "password": "password123",
			"full_name": "Gina Guest"}, &gina)
		var ginaRole int16
		if err := pool.QueryRow(ctx, `SELECT role FROM user_account WHERE id = $1`,
			gina.UserID).Scan(&ginaRole); err != nil || ginaRole < 50 {
			t.Fatalf("gina role = %d (%v), want the guest ceiling; the guest half of this test would be vacuous otherwise",
				ginaRole, err)
		}

		carol := dialClientLast(t, ctx, ts.URL, carolTok, "-1")
		defer carol.conn.CloseNow()
		ginaC := dialClientLast(t, ctx, ts.URL, gina.Token, "-1")
		defer ginaC.conn.CloseNow()
		carol.waitFor(t, "ready")
		ginaC.waitFor(t, "ready")

		// The Space is created AFTER both connections exist, so carol only
		// receives its items if space.created refreshed her set MID-connection
		// — the "a scope change must be driven by an event" requirement.
		var space struct {
			ID int64 `json:"id"`
		}
		postJSON(t, ts.URL+"/api/v1/spaces", boot.Token,
			map[string]any{"key": "ops", "name": "Operations"}, &space)
		var first, second struct {
			ID int64 `json:"id"`
		}
		postJSON(t, fmt.Sprintf("%s/api/v1/spaces/%d/items", ts.URL, space.ID),
			boot.Token, map[string]any{"title": "first"}, &first)
		postJSON(t, fmt.Sprintf("%s/api/v1/spaces/%d/items", ts.URL, space.ID),
			boot.Token, map[string]any{"title": "second"}, &second)

		// Both connections are LIVE fan targets: each drain terminates on a
		// message in a channel that connection IS in, which is logged AFTER
		// the work-item events — so the work-item events were offered to both.
		ginaPing := sendChannel(t, ts.URL, boot.Token, support.ChannelID, "guest ping")
		carolPing := sendChannel(t, ts.URL, boot.Token, boot.ChannelID, "member ping")

		carolSaw := drainUntil(t, carol, carolPing)
		if !hasItemEvent(carolSaw, "workitem.created", second.ID) {
			t.Fatalf("a member did not receive workitem.created for item %d; the space set must refresh on space.created. saw %v",
				second.ID, envTypes(carolSaw))
		}
		ginaSaw := drainUntil(t, ginaC, ginaPing)
		for _, e := range ginaSaw {
			if strings.HasPrefix(e.Type, "workitem.") || strings.HasPrefix(e.Type, "space.") {
				t.Fatalf("a guest received the space-scoped event %q; guests hold an empty space set. saw %v",
					e.Type, envTypes(ginaSaw))
			}
		}

		// An event whose Space cannot be RESOLVED is withheld, not fanned:
		// workitem.reordered carries no space_id, so even a full member — who
		// receives every other work-item event for this space — gets nothing.
		if code := postJSONStatus(t, fmt.Sprintf("%s/api/v1/items/%d/move", ts.URL, second.ID),
			boot.Token, map[string]any{"before_item_id": first.ID}); code != http.StatusOK {
			t.Fatalf("move item = %d, want 200", code)
		}
		movePing := sendChannel(t, ts.URL, boot.Token, boot.ChannelID, "after move")
		afterMove := drainUntil(t, carol, movePing)
		for _, e := range afterMove {
			if e.Type == "workitem.reordered" {
				t.Fatalf("an event with no resolvable space_id was fanned org-wide; saw %v",
					envTypes(afterMove))
			}
		}

		// workitem.promoted_from_thread is the ONE work-item event that names
		// TWO containers: the promoted thread keeps its CHANNEL's ACL (the D2
		// fusion's F-5 rule) while the item it now backs lives in a SPACE, so
		// the payload carries channel_id AND space_id. It must clear BOTH
		// gates, which splits three live connections three ways —
		//   alice: in #support AND sees the Space       -> receives it
		//   gina:  in #support, empty space set (guest) -> SPACE gate withholds
		//   carol: sees the Space, NOT in #support      -> CHANNEL gate withholds
		// and carol's line is the one that goes red if entity class is made to
		// WIN over the payload's channel_id rather than be conjunctive with it:
		// that ordering leaks a private channel's thread_id and channel_id to
		// everyone who can see the Space.
		alice := dialClientLast(t, ctx, ts.URL, boot.Token, "-1")
		defer alice.conn.CloseNow()
		alice.waitFor(t, "ready")

		var escalation struct {
			ThreadID int64 `json:"thread_id"`
		}
		postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/threads", ts.URL, support.ChannelID),
			boot.Token, map[string]any{"title": "escalation", "content": "needs an item"},
			&escalation)
		var promoted struct {
			ID int64 `json:"id"`
		}
		postJSON(t, fmt.Sprintf("%s/api/v1/threads/%d/promote", ts.URL, escalation.ThreadID),
			boot.Token, map[string]any{"space_id": space.ID}, &promoted)
		promotedPingSupport := sendChannel(t, ts.URL, boot.Token, support.ChannelID, "after promote")
		promotedPingGeneral := sendChannel(t, ts.URL, boot.Token, boot.ChannelID, "after promote")

		aliceSaw := drainUntil(t, alice, promotedPingGeneral)
		if !hasItemEvent(aliceSaw, "workitem.promoted_from_thread", promoted.ID) {
			t.Fatalf("the promoter, who is in the channel AND sees the Space, did not receive workitem.promoted_from_thread for item %d; saw %v",
				promoted.ID, envTypes(aliceSaw))
		}
		ginaPromote := drainUntil(t, ginaC, promotedPingSupport)
		if hasItemEvent(ginaPromote, "workitem.promoted_from_thread", promoted.ID) {
			t.Fatalf("a guest in the channel received a work-item event the SPACE gate must withhold; the gates must be conjunctive. saw %v",
				envTypes(ginaPromote))
		}
		carolPromote := drainUntil(t, carol, promotedPingGeneral)
		if hasItemEvent(carolPromote, "workitem.promoted_from_thread", promoted.ID) {
			t.Fatalf("a non-member of the promoted thread's channel received its channel_id and thread_id; the CHANNEL gate must still apply to space-scoped events. saw %v",
				envTypes(carolPromote))
		}

		// P-4 item security: the org defines a visibility_scope, so no
		// work-item event can be resolved any more (there is no evaluator for
		// the rule) and every one of them is withheld — including the
		// two-container promotion, for a connection that clears both of ITS
		// gates — while space-scoped events that CANNOT carry a security scope
		// keep flowing, so the withholding is as narrow as the hook it fails
		// closed on.
		if _, err := pool.Exec(ctx, `
			INSERT INTO visibility_scope (space_id, name, rule)
			VALUES ($1, 'restricted', '{"roles":["reporter"]}'::jsonb)`,
			space.ID); err != nil {
			t.Fatalf("define visibility scope: %v", err)
		}
		if _, err := pool.Exec(ctx,
			`UPDATE work_item SET security_scope_id = (SELECT id FROM visibility_scope
			 WHERE space_id = $1) WHERE id = $2`, space.ID, first.ID); err != nil {
			t.Fatalf("scope the item: %v", err)
		}
		scoped := dialClientLast(t, ctx, ts.URL, carolTok, "-1")
		defer scoped.conn.CloseNow()
		scoped.waitFor(t, "ready")
		// alice re-dials so her connection picks the scope up; she is the only
		// principal who passes BOTH gates on a #support promotion, so anything
		// she is denied below is denied by item security and nothing else.
		scopedAlice := dialClientLast(t, ctx, ts.URL, boot.Token, "-1")
		defer scopedAlice.conn.CloseNow()
		scopedAlice.waitFor(t, "ready")

		if code := patchJSON(t, fmt.Sprintf("%s/api/v1/items/%d", ts.URL, first.ID),
			boot.Token, map[string]any{"title": "retitled"}); code != http.StatusOK {
			t.Fatalf("retitle item = %d, want 200", code)
		}
		var sprint struct {
			ID int64 `json:"id"`
		}
		postJSON(t, fmt.Sprintf("%s/api/v1/spaces/%d/sprints", ts.URL, space.ID),
			boot.Token, map[string]any{"name": "Sprint 1"}, &sprint)
		var second2 struct {
			ThreadID int64 `json:"thread_id"`
		}
		postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/threads", ts.URL, support.ChannelID),
			boot.Token, map[string]any{"title": "second escalation", "content": "another"},
			&second2)
		var promoted2 struct {
			ID int64 `json:"id"`
		}
		postJSON(t, fmt.Sprintf("%s/api/v1/threads/%d/promote", ts.URL, second2.ThreadID),
			boot.Token, map[string]any{"space_id": space.ID}, &promoted2)
		scopedPing := sendChannel(t, ts.URL, boot.Token, boot.ChannelID, "after scoping")

		afterScope := drainUntil(t, scoped, scopedPing)
		for _, e := range afterScope {
			if strings.HasPrefix(e.Type, "workitem.") {
				t.Fatalf("a work-item event (%q) was delivered while the org defines a visibility_scope the gateway cannot evaluate; saw %v",
					e.Type, envTypes(afterScope))
			}
		}
		if !hasEvent(afterScope, "sprint.created") {
			t.Fatalf("item security blacked out a NON-work-item space event; sprint.created must still flow. saw %v",
				envTypes(afterScope))
		}
		aliceScope := drainUntil(t, scopedAlice, scopedPing)
		if hasItemEvent(aliceScope, "workitem.promoted_from_thread", promoted2.ID) {
			t.Fatalf("the promotion event escaped the item-security blackout for a connection that clears both container gates; every work-item event must be withheld while a visibility_scope exists. saw %v",
				envTypes(aliceScope))
		}
		if !hasEvent(aliceScope, "sprint.created") {
			t.Fatalf("the same connection must still receive sprint.created; a blanket space blackout is too wide. saw %v",
				envTypes(aliceScope))
		}
	})
}

// drainMessagesUntil reads envelopes from c until the message.created carrying
// wantID arrives, returning every message.created id seen on the way (in
// stream order, wantID last). This is what makes the negatives non-vacuous:
// the gateway delivers in event-id order, so an event logged BEFORE wantID has
// already been offered to this connection by the time wantID lands — if it is
// absent from the result it was dropped by that connection's ACL filter, not
// merely late.
func drainMessagesUntil(t *testing.T, c *wsClient, wantID int64) []int64 {
	t.Helper()
	deadline := time.After(8 * time.Second)
	var seen []int64
	for {
		select {
		case e, ok := <-c.events:
			if !ok {
				t.Fatalf("connection closed while waiting for message %d (saw %v)", wantID, seen)
			}
			if e.Type != "message.created" {
				continue
			}
			var p struct {
				MessageID int64 `json:"message_id"`
			}
			if err := json.Unmarshal(e.Payload, &p); err != nil {
				t.Fatalf("decode message.created payload: %v", err)
			}
			seen = append(seen, p.MessageID)
			if p.MessageID == wantID {
				return seen
			}
		case <-deadline:
			t.Fatalf("timed out waiting for message %d (saw %v)", wantID, seen)
		}
	}
}

// drainUntil is drainMessagesUntil for assertions about NON-message events: it
// returns every envelope seen up to the message.created carrying wantID, so a
// "must not arrive" claim is checked against the whole window the connection
// was actually offered — never against silence.
func drainUntil(t *testing.T, c *wsClient, wantID int64) []gateway.Envelope {
	t.Helper()
	deadline := time.After(8 * time.Second)
	var seen []gateway.Envelope
	for {
		select {
		case e, ok := <-c.events:
			if !ok {
				t.Fatalf("connection closed while waiting for message %d (saw %v)",
					wantID, envTypes(seen))
			}
			seen = append(seen, e)
			if e.Type != "message.created" {
				continue
			}
			var p struct {
				MessageID int64 `json:"message_id"`
			}
			if err := json.Unmarshal(e.Payload, &p); err != nil {
				t.Fatalf("decode message.created payload: %v", err)
			}
			if p.MessageID == wantID {
				return seen
			}
		case <-deadline:
			t.Fatalf("timed out waiting for message %d (saw %v)", wantID, envTypes(seen))
		}
	}
}

func envTypes(evs []gateway.Envelope) []string {
	out := make([]string, 0, len(evs))
	for _, e := range evs {
		out = append(out, e.Type)
	}
	return out
}

func hasEvent(evs []gateway.Envelope, want string) bool {
	for _, e := range evs {
		if e.Type == want {
			return true
		}
	}
	return false
}

// hasItemEvent matches a work-item envelope by verb AND item id, so a stray
// event for a different item cannot satisfy the assertion.
func hasItemEvent(evs []gateway.Envelope, want string, itemID int64) bool {
	for _, e := range evs {
		if e.Type != want {
			continue
		}
		var p struct {
			ItemID int64 `json:"item_id"`
		}
		if json.Unmarshal(e.Payload, &p) == nil && p.ItemID == itemID {
			return true
		}
	}
	return false
}
