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

	"github.com/abhinavjha0239/weft/internal/domain/automation"
	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/gateway"
)

// TestAutomationCRUD: ADR-014 governance — rules are owned by the scope
// (org rules need manage_org, channel rules administer_channel), created
// DISABLED, validated against the v1 definition vocabulary, and the F-13
// consent arc holds: acting as another human requires their consent, a
// definition edit clears consent AND disables, and only re-consent
// re-opens enabling.
func TestAutomationCRUD(t *testing.T) {
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
		Identity:    identity.New(pool, permsSvc),
		Messaging:   messaging.New(pool, permsSvc),
		Automations: automation.New(pool, permsSvc),
	}))
	defer ts.Close()

	var boot struct {
		OrgID     int64  `json:"org_id"`
		UserID    int64  `json:"user_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "aut", "email": "a@aut.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	bobTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"bob@aut.test", "Bob Ray", "bobauttok")
	var bobID int64
	if err := pool.QueryRow(ctx, `SELECT id FROM user_account WHERE email = 'bob@aut.test'`).Scan(&bobID); err != nil {
		t.Fatalf("bob id: %v", err)
	}

	def := func(verb string, channelID int64, content string) map[string]any {
		step := map[string]any{"kind": "post_message", "content": content}
		if channelID != 0 {
			step["channel_id"] = channelID
		}
		return map[string]any{
			"trigger": map[string]any{"verb": verb},
			"steps":   []any{step},
		}
	}

	// Scope gates: a plain member can manage neither scope rung.
	if code := postJSONStatus(t, ts.URL+"/api/v1/automations", bobTok, map[string]any{
		"scope_type": 1, "scope_id": boot.OrgID, "name": "nope",
		"definition": def("message.created", boot.ChannelID, "hi")}); code != http.StatusForbidden {
		t.Fatalf("member org rule = %d, want 403", code)
	}
	if code := postJSONStatus(t, ts.URL+"/api/v1/automations", bobTok, map[string]any{
		"scope_type": 3, "scope_id": boot.ChannelID, "name": "nope",
		"definition": def("message.created", 0, "hi")}); code != http.StatusForbidden {
		t.Fatalf("member channel rule = %d, want 403", code)
	}

	// Definition validation.
	for name, body := range map[string]map[string]any{
		"unknown verb":        {"scope_type": 1, "scope_id": boot.OrgID, "name": "x", "definition": def("meteor.landed", boot.ChannelID, "hi")},
		"no steps":            {"scope_type": 1, "scope_id": boot.OrgID, "name": "x", "definition": map[string]any{"trigger": map[string]any{"verb": "message.created"}, "steps": []any{}}},
		"org step no channel": {"scope_type": 1, "scope_id": boot.OrgID, "name": "x", "definition": def("message.created", 0, "hi")},
		"unknown scope rung":  {"scope_type": 2, "scope_id": 1, "name": "x", "definition": def("message.created", boot.ChannelID, "hi")},
	} {
		if code := postJSONStatus(t, ts.URL+"/api/v1/automations", boot.Token, body); code != http.StatusBadRequest {
			t.Fatalf("%s = %d, want 400", name, code)
		}
	}
	// A channel-scope rule may not post into a different channel.
	var other struct {
		ChannelID int64 `json:"channel_id"`
	}
	postJSON(t, ts.URL+"/api/v1/channels", boot.Token, map[string]any{"name": "other"}, &other)
	if code := postJSONStatus(t, ts.URL+"/api/v1/automations", boot.Token, map[string]any{
		"scope_type": 3, "scope_id": boot.ChannelID, "name": "escape",
		"definition": def("message.created", other.ChannelID, "hi")}); code != http.StatusBadRequest {
		t.Fatalf("scope escape = %d, want 400", code)
	}

	// Create (disabled by default) → enable → list shows it.
	var rule automation.Automation
	postJSON(t, ts.URL+"/api/v1/automations", boot.Token, map[string]any{
		"scope_type": 3, "scope_id": boot.ChannelID, "name": "auto-reply",
		"definition": def("message.created", 0, "thanks!")}, &rule)
	if rule.ID == 0 || rule.Enabled {
		t.Fatalf("create = %+v, want disabled rule", rule)
	}
	if code := patchJSON(t, fmt.Sprintf("%s/api/v1/automations/%d", ts.URL, rule.ID),
		boot.Token, map[string]any{"enabled": true}); code != http.StatusOK {
		t.Fatalf("enable = %d", code)
	}
	var list struct {
		Automations []automation.Automation `json:"automations"`
	}
	if code := getJSON(t, fmt.Sprintf("%s/api/v1/automations?scope_type=3&scope_id=%d", ts.URL, boot.ChannelID),
		boot.Token, &list); code != http.StatusOK || len(list.Automations) != 1 || !list.Automations[0].Enabled {
		t.Fatalf("list = %d %+v", code, list.Automations)
	}

	// Consent arc: alice names BOB as the acting human → pending, enable 409,
	// alice can't consent for him, bob consents, enable works.
	var asBob automation.Automation
	postJSON(t, ts.URL+"/api/v1/automations", boot.Token, map[string]any{
		"scope_type": 1, "scope_id": boot.OrgID, "name": "as-bob",
		"definition":    def("workitem.created", boot.ChannelID, "an item appeared"),
		"actor_user_id": bobID}, &asBob)
	if asBob.ID == 0 || asBob.ActorConsented {
		t.Fatalf("as-bob create = %+v, want unconsented", asBob)
	}
	if code := patchJSON(t, fmt.Sprintf("%s/api/v1/automations/%d", ts.URL, asBob.ID),
		boot.Token, map[string]any{"enabled": true}); code != http.StatusConflict {
		t.Fatalf("enable unconsented = %d, want 409", code)
	}
	if code := postJSONStatus(t, fmt.Sprintf("%s/api/v1/automations/%d/consent", ts.URL, asBob.ID),
		boot.Token, map[string]any{}); code != http.StatusForbidden {
		t.Fatalf("alice consenting for bob = %d, want 403", code)
	}
	if code := postJSONStatus(t, fmt.Sprintf("%s/api/v1/automations/%d/consent", ts.URL, asBob.ID),
		bobTok, map[string]any{}); code != http.StatusOK {
		t.Fatalf("bob consent = %d", code)
	}
	if code := patchJSON(t, fmt.Sprintf("%s/api/v1/automations/%d", ts.URL, asBob.ID),
		boot.Token, map[string]any{"enabled": true}); code != http.StatusOK {
		t.Fatalf("enable consented = %d", code)
	}

	// The consent-clearing edit rule: a definition change bumps the version,
	// clears bob's consent, AND disables the rule.
	if code := patchJSON(t, fmt.Sprintf("%s/api/v1/automations/%d", ts.URL, asBob.ID),
		boot.Token, map[string]any{
			"definition": def("workitem.created", boot.ChannelID, "REVISED text")}); code != http.StatusOK {
		t.Fatalf("edit definition = %d", code)
	}
	var orgList struct {
		Automations []automation.Automation `json:"automations"`
	}
	getJSON(t, fmt.Sprintf("%s/api/v1/automations?scope_type=1&scope_id=%d", ts.URL, boot.OrgID),
		boot.Token, &orgList)
	if len(orgList.Automations) != 1 {
		t.Fatalf("org list = %+v", orgList.Automations)
	}
	got := orgList.Automations[0]
	if got.Version != 2 || got.ActorConsented || got.Enabled {
		t.Fatalf("after edit = v%d consented=%v enabled=%v, want v2 false false",
			got.Version, got.ActorConsented, got.Enabled)
	}
	if code := patchJSON(t, fmt.Sprintf("%s/api/v1/automations/%d", ts.URL, asBob.ID),
		boot.Token, map[string]any{"enabled": true}); code != http.StatusConflict {
		t.Fatalf("re-enable after edit = %d, want 409 until re-consent", code)
	}
	if code := postJSONStatus(t, fmt.Sprintf("%s/api/v1/automations/%d/consent", ts.URL, asBob.ID),
		bobTok, map[string]any{}); code != http.StatusOK {
		t.Fatalf("re-consent = %d", code)
	}
	if code := patchJSON(t, fmt.Sprintf("%s/api/v1/automations/%d", ts.URL, asBob.ID),
		boot.Token, map[string]any{"enabled": true}); code != http.StatusOK {
		t.Fatalf("re-enable = %d", code)
	}

	// Soft delete removes it from lists; runs stay queryable-by-history
	// design, but the rule itself 404s.
	if code := deleteReq(t, fmt.Sprintf("%s/api/v1/automations/%d", ts.URL, asBob.ID), boot.Token); code != http.StatusOK {
		t.Fatalf("delete = %d", code)
	}
	getJSON(t, fmt.Sprintf("%s/api/v1/automations?scope_type=1&scope_id=%d", ts.URL, boot.OrgID),
		boot.Token, &orgList)
	if len(orgList.Automations) != 0 {
		t.Fatalf("post-delete list = %+v, want empty", orgList.Automations)
	}
	if code := patchJSON(t, fmt.Sprintf("%s/api/v1/automations/%d", ts.URL, asBob.ID),
		boot.Token, map[string]any{"enabled": true}); code != http.StatusNotFound {
		t.Fatalf("patch deleted = %d, want 404", code)
	}
}
