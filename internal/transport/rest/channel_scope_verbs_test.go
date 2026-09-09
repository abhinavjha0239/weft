package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/gateway"
)

// putJSONBody is putJSON plus the response BODY: the oracle-free assertions
// below compare error bodies byte-for-byte, not just their status codes.
func putJSONBody(t *testing.T, url, token string, body any) (int, []byte) {
	t.Helper()
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest("PUT", url, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT %s: %v", url, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s: %v", url, err)
	}
	return resp.StatusCode, raw
}

// TestChannelScopeVerbAssignment: P-44a, the WRITE half of a scope the
// resolver has always read.
//
// The load-bearing claim is PRECEDENCE — a channel-scope assignment beats the
// org default for that channel and leaves every other channel untouched — and
// it is asserted through real sends, not through the row alone: after
// send_message is pointed at role:admins for channel A, the member who could
// post everywhere a moment ago is refused in A, still posts in B, and the org
// default row is proven unchanged.
//
// Two guards carry their own pins:
//
//   - The gate is resolved AT THE TARGET SCOPE (administer_channel through
//     ChannelScope), so a channel admin retargets their OWN channel while
//     holding no org-wide permission reassignment. Requiring org
//     manage_permissions instead turns bob's assign into a refusal.
//   - manage_permissions is NEVER channel-assignable. Allowing it writes the
//     row this asserts is absent.
//
// And the oracle: absent, foreign-org and un-administered channels answer ONE
// byte-identical 404, so the surface cannot enumerate channels or reveal who
// administers what.
func TestChannelScopeVerbAssignment(t *testing.T) {
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
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity:  identity.New(pool, permsSvc),
		Messaging: messaging.New(pool, permsSvc),
	}))
	defer ts.Close()

	type boot struct {
		OrgID     int64  `json:"org_id"`
		UserID    int64  `json:"user_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	var acme, other boot
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "acme", "email": "alice@acme.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &acme)
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "other", "email": "olga@other.test", "password": "password123",
		"full_name": "Olga Vega",
	}, &other)

	// Channel A is bootstrap's; alice mints channel B so "every other channel
	// is untouched" has something to be true of.
	chanA := acme.ChannelID
	var made struct {
		ChannelID int64 `json:"channel_id"`
	}
	postJSON(t, ts.URL+"/api/v1/channels", acme.Token,
		map[string]any{"name": "sidebar"}, &made)
	chanB := made.ChannelID

	// Bob: a plain member (role:members) of A, joined to B as well.
	bobTok := addChannelMember(t, ctx, pool, acme.OrgID, chanA,
		"bob@acme.test", "Bob Ray", "bobscopetok")
	if code := postJSONStatus(t, fmt.Sprintf("%s/api/v1/channels/%d/join", ts.URL, chanB),
		bobTok, map[string]any{}); code != http.StatusOK {
		t.Fatalf("bob join B = %d, want 200", code)
	}

	verbsURL := ts.URL + "/api/v1/admin/verbs"
	send := func(t *testing.T, token string, channelID int64, text string) int {
		t.Helper()
		return postJSONStatus(t, fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, channelID),
			token, map[string]any{"content": text})
	}
	// assignedGroup reads the group NAME a (verb, scope) row points at, or ""
	// when no row exists — the state every assertion below is really about.
	assignedGroup := func(t *testing.T, verb string, scopeType int16, scopeID int64) string {
		t.Helper()
		var name string
		err := pool.QueryRow(ctx, `
			SELECT g.name FROM permission_assignment pa
			JOIN user_group g ON g.id = pa.group_id
			WHERE pa.org_id = $1 AND pa.verb = $2 AND pa.scope_type = $3 AND pa.scope_id = $4`,
			acme.OrgID, verb, scopeType, scopeID).Scan(&name)
		if err != nil {
			return ""
		}
		return name
	}
	countEvents := func(t *testing.T, verb string) int {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM event_log WHERE org_id = $1 AND verb = $2`,
			acme.OrgID, verb).Scan(&n); err != nil {
			t.Fatalf("count events: %v", err)
		}
		return n
	}

	// --- baseline: the seeded org default lets a member post in both channels.
	if code := send(t, bobTok, chanA, "hello A"); code != http.StatusCreated {
		t.Fatalf("member send A baseline = %d, want 201", code)
	}
	if code := send(t, bobTok, chanB, "hello B"); code != http.StatusCreated {
		t.Fatalf("member send B baseline = %d, want 201", code)
	}
	if got := assignedGroup(t, "send_message", 1, acme.OrgID); got != "role:everyone" {
		t.Fatalf("seeded org send_message = %q, want role:everyone", got)
	}

	// --- the ORG-scope surface is untouched by this slice.
	// A member still cannot reassign org-wide…
	if code := putJSON(t, verbsURL, bobTok,
		map[string]any{"verb": "send_message", "group": "role:admins"}); code != http.StatusForbidden {
		t.Fatalf("member org-scope reassign = %d, want 403", code)
	}
	if got := assignedGroup(t, "send_message", 1, acme.OrgID); got != "role:everyone" {
		t.Fatalf("refused org-scope reassign still wrote: send_message = %q", got)
	}
	// …and an org admin's org-scope assign is exactly what it was, with the
	// scope ADDED to the payload rather than any key repurposed.
	if code := putJSON(t, verbsURL, acme.Token,
		map[string]any{"verb": "resolve_threads", "group": "role:admins"}); code != http.StatusOK {
		t.Fatalf("org-scope assign = %d, want 200", code)
	}
	var orgEv struct {
		EntityType int16 `json:"entity_type"`
		EntityID   int64 `json:"entity_id"`
		Payload    struct {
			Verb      string `json:"verb"`
			Group     string `json:"group"`
			ScopeType int16  `json:"scope_type"`
			ScopeID   int64  `json:"scope_id"`
			ChannelID int64  `json:"channel_id"`
		} `json:"payload"`
	}
	lastEvent(t, ctx, pool, acme.OrgID, "org.verb_assigned", &orgEv)
	if orgEv.EntityType != 8 || orgEv.EntityID != acme.OrgID ||
		orgEv.Payload.Verb != "resolve_threads" || orgEv.Payload.Group != "role:admins" ||
		orgEv.Payload.ScopeType != 1 || orgEv.Payload.ScopeID != acme.OrgID ||
		orgEv.Payload.ChannelID != 0 {
		t.Fatalf("org.verb_assigned = %+v, want entity 8/%d and payload "+
			"{resolve_threads role:admins scope 1/%d, no channel_id}",
			orgEv, acme.OrgID, acme.OrgID)
	}

	// --- manage_permissions is NEVER channel-assignable (the escalation pin).
	// Even the OWNER is refused, and nothing is written.
	code, body := putJSONBody(t, verbsURL, acme.Token, map[string]any{
		"verb": "manage_permissions", "group": "role:members", "channel_id": chanA})
	// Errorf, not Fatalf, for the wire checks: when this guard is removed the
	// ROW is the failure that matters, and stopping at the status code would
	// hide it.
	if code != http.StatusBadRequest {
		t.Errorf("manage_permissions at channel scope = %d, want 400", code)
	}
	if !bytes.Contains(body, []byte("may not be assigned at channel scope")) {
		t.Errorf("manage_permissions refusal body = %s, want the named refusal", body)
	}
	if got := assignedGroup(t, "manage_permissions", 3, chanA); got != "" {
		t.Fatalf("manage_permissions rows at channel scope = 1 (group %q), want 0 — "+
			"a channel-scope grant of it delegates permission administration", got)
	}
	if got := assignedGroup(t, "manage_permissions", 1, acme.OrgID); got != "role:admins" {
		t.Fatalf("org manage_permissions = %q, want role:admins (untouched)", got)
	}

	// --- honest rungs: a verb no channel gate consults is refused too.
	code, body = putJSONBody(t, verbsURL, acme.Token, map[string]any{
		"verb": "add_emoji", "group": "role:members", "channel_id": chanA})
	if code != http.StatusBadRequest {
		t.Fatalf("add_emoji at channel scope = %d, want 400", code)
	}
	if !bytes.Contains(body, []byte("not consulted at channel scope")) {
		t.Fatalf("add_emoji refusal body = %s, want the honest-rungs refusal", body)
	}
	if got := assignedGroup(t, "add_emoji", 3, chanA); got != "" {
		t.Fatalf("add_emoji rows at channel scope = 1 (group %q), want 0", got)
	}
	if n := countEvents(t, "channel.verb_assigned"); n != 0 {
		t.Fatalf("channel.verb_assigned events after two refusals = %d, want 0", n)
	}

	// --- an org admin retargets a channel's admin rung: today's org admins
	// keep everything they had, because the channel chain ENDS at the org.
	if code := putJSON(t, verbsURL, acme.Token, map[string]any{
		"verb": "administer_channel", "group": "role:members", "channel_id": chanA,
	}); code != http.StatusOK {
		t.Fatalf("owner assign administer_channel at A = %d, want 200", code)
	}
	if got := assignedGroup(t, "administer_channel", 3, chanA); got != "role:members" {
		t.Fatalf("administer_channel at A = %q, want role:members", got)
	}
	if got := assignedGroup(t, "administer_channel", 1, acme.OrgID); got != "role:admins" {
		t.Fatalf("org administer_channel = %q, want role:admins (other channels untouched)", got)
	}
	var chanEv struct {
		EntityType int16 `json:"entity_type"`
		EntityID   int64 `json:"entity_id"`
		ActorID    int64 `json:"actor_id"`
		Payload    struct {
			Verb      string `json:"verb"`
			Group     string `json:"group"`
			ScopeType int16  `json:"scope_type"`
			ScopeID   int64  `json:"scope_id"`
			ChannelID int64  `json:"channel_id"`
		} `json:"payload"`
	}
	lastEvent(t, ctx, pool, acme.OrgID, "channel.verb_assigned", &chanEv)
	if chanEv.EntityType != 3 || chanEv.EntityID != chanA || chanEv.ActorID != acme.UserID ||
		chanEv.Payload.Verb != "administer_channel" || chanEv.Payload.Group != "role:members" ||
		chanEv.Payload.ScopeType != 3 || chanEv.Payload.ScopeID != chanA ||
		chanEv.Payload.ChannelID != chanA {
		t.Fatalf("channel.verb_assigned = %+v, want entity 3/%d by %d and payload "+
			"{administer_channel role:members scope 3/%d channel_id %d}",
			chanEv, chanA, acme.UserID, chanA, chanA)
	}

	// --- PIN 1: bob now administers A (and only A), so he may retarget A's
	// verbs while holding no org-wide permission reassignment at all.
	if code := putJSON(t, verbsURL, bobTok, map[string]any{
		"verb": "send_message", "group": "role:admins", "channel_id": chanA,
	}); code != http.StatusOK {
		t.Fatalf("channel admin assign at own channel = %d, want 200 — the gate must "+
			"resolve at the TARGET scope, not at the org", code)
	}
	if got := assignedGroup(t, "send_message", 3, chanA); got != "role:admins" {
		t.Fatalf("send_message at A = %q, want role:admins", got)
	}

	// --- PRECEDENCE, through real sends: the channel row beats the org
	// default for A and leaves B alone.
	if code := send(t, bobTok, chanA, "still here?"); code != http.StatusForbidden {
		t.Fatalf("member send A after channel override = %d, want 403", code)
	}
	if code := send(t, bobTok, chanB, "but B is fine"); code != http.StatusCreated {
		t.Fatalf("member send B after A's override = %d, want 201 — the override must "+
			"not leak to other channels", code)
	}
	if code := send(t, acme.Token, chanA, "admins may still post"); code != http.StatusCreated {
		t.Fatalf("admin send A after channel override = %d, want 201", code)
	}
	if got := assignedGroup(t, "send_message", 1, acme.OrgID); got != "role:everyone" {
		t.Fatalf("org send_message after channel override = %q, want role:everyone", got)
	}
	if got := assignedGroup(t, "send_message", 3, chanB); got != "" {
		t.Fatalf("channel B grew a send_message row (%q) it was never given", got)
	}

	// --- the oracle: three refusals a channel admin must not tell apart.
	// (1) a channel he does not administer, (2) one that does not exist,
	// (3) another org's channel — plus an ORG ADMIN reaching across the cell
	// boundary, which must answer the same way.
	unadministered, bodyB := putJSONBody(t, verbsURL, bobTok, map[string]any{
		"verb": "send_message", "group": "role:admins", "channel_id": chanB})
	absent, bodyAbsent := putJSONBody(t, verbsURL, bobTok, map[string]any{
		"verb": "send_message", "group": "role:admins", "channel_id": 999999})
	foreign, bodyForeign := putJSONBody(t, verbsURL, bobTok, map[string]any{
		"verb": "send_message", "group": "role:admins", "channel_id": other.ChannelID})
	crossCell, bodyCross := putJSONBody(t, verbsURL, acme.Token, map[string]any{
		"verb": "send_message", "group": "role:admins", "channel_id": other.ChannelID})
	for _, c := range []struct {
		name string
		code int
		body []byte
	}{
		{"un-administered channel", unadministered, bodyB},
		{"absent channel", absent, bodyAbsent},
		{"another org's channel", foreign, bodyForeign},
		{"another org's channel, as an org admin", crossCell, bodyCross},
	} {
		if c.code != http.StatusNotFound {
			t.Fatalf("%s = %d, want 404", c.name, c.code)
		}
		if !bytes.Equal(c.body, bodyAbsent) {
			t.Fatalf("%s body = %s, want byte-identical to the absent-channel body %s",
				c.name, c.body, bodyAbsent)
		}
	}
	if got := assignedGroup(t, "send_message", 3, chanB); got != "" {
		t.Fatalf("refused assign at B still wrote a row (%q)", got)
	}
	var foreignRows int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM permission_assignment WHERE org_id = $1`, other.OrgID).Scan(&foreignRows); err != nil {
		t.Fatalf("count foreign rows: %v", err)
	}
	var seeded int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM permission_assignment WHERE org_id = $1 AND scope_type = 1`,
		other.OrgID).Scan(&seeded); err != nil {
		t.Fatalf("count foreign seeded rows: %v", err)
	}
	if foreignRows != seeded {
		t.Fatalf("the other org gained %d non-org-scope assignment rows, want 0",
			foreignRows-seeded)
	}

	// --- the whole run wrote exactly the two channel-scope events it should.
	if n := countEvents(t, "channel.verb_assigned"); n != 2 {
		t.Fatalf("channel.verb_assigned events = %d, want 2 (administer_channel, send_message)", n)
	}
}

// lastEvent decodes the newest event_log row with verb into out — the
// event-log assertions above read the row the write actually appended.
func lastEvent(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID int64, verb string, out any) {
	t.Helper()
	var entityType int16
	var entityID, actorID int64
	var payload []byte
	if err := pool.QueryRow(ctx, `
		SELECT entity_type, entity_id, actor_id, payload FROM event_log
		WHERE org_id = $1 AND verb = $2 ORDER BY id DESC LIMIT 1`,
		orgID, verb).Scan(&entityType, &entityID, &actorID, &payload); err != nil {
		t.Fatalf("no %s event: %v", verb, err)
	}
	row, err := json.Marshal(map[string]any{
		"entity_type": entityType, "entity_id": entityID, "actor_id": actorID,
		"payload": json.RawMessage(payload),
	})
	if err != nil {
		t.Fatalf("marshal %s event: %v", verb, err)
	}
	if err := json.Unmarshal(row, out); err != nil {
		t.Fatalf("decode %s event: %v", verb, err)
	}
}
