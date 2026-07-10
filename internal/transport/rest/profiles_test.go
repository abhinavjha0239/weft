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

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/gateway"
)

// TestUserProfiles: /me boot identity + /users?ids= batch resolution —
// org-pinned, deactivated flagged, foreign ids silently absent, caps.
func TestUserProfiles(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		cancel()
		t.Fatalf("connect: %v", err)
	}
	defer func() { cancel(); pool.Close() }()
	resetAndMigrate(t, ctx, pool)

	hub := gateway.NewHub(pool, slog.Default())
	go hub.Run(ctx)
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity:  identity.New(pool, perms.New(pool)),
		Messaging: messaging.New(pool, perms.New(pool)),
	}))
	defer ts.Close()

	var boot struct {
		OrgID     int64  `json:"org_id"`
		UserID    int64  `json:"user_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "prof", "email": "alice@p.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	_ = addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"bob@p.test", "Bob Ray", "bobproftok")
	var bobID int64
	if err := pool.QueryRow(ctx,
		`SELECT id FROM user_account WHERE org_id=$1 AND email='bob@p.test'`,
		boot.OrgID).Scan(&bobID); err != nil {
		t.Fatalf("bob id: %v", err)
	}

	// A second org whose users must be invisible to the first.
	var other struct {
		UserID int64 `json:"user_id"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "prof2", "email": "carol@p2.test", "password": "password123",
		"full_name": "Carol Cross",
	}, &other)

	// /me: the boot identity.
	var me struct {
		UserID   int64  `json:"user_id"`
		OrgID    int64  `json:"org_id"`
		FullName string `json:"full_name"`
		Email    string `json:"email"`
	}
	if code := getJSON(t, ts.URL+"/api/v1/me", boot.Token, &me); code != http.StatusOK {
		t.Fatalf("me: %d, want 200", code)
	}
	if me.UserID != boot.UserID || me.OrgID != boot.OrgID ||
		me.FullName != "Alice Chen" || me.Email != "alice@p.test" {
		t.Fatalf("me wrong: %+v", me)
	}

	// Batch lookup: own org's users resolve; the foreign id is silently
	// absent (org pin), not an error.
	var got struct {
		Users []struct {
			ID          int64  `json:"id"`
			FullName    string `json:"full_name"`
			Deactivated bool   `json:"deactivated"`
		} `json:"users"`
	}
	url := fmt.Sprintf("%s/api/v1/users?ids=%d,%d,%d",
		ts.URL, boot.UserID, bobID, other.UserID)
	if code := getJSON(t, url, boot.Token, &got); code != http.StatusOK {
		t.Fatalf("users: %d, want 200", code)
	}
	if len(got.Users) != 2 {
		t.Fatalf("users = %d results, want 2 (foreign id must be absent): %+v",
			len(got.Users), got.Users)
	}
	names := map[int64]string{}
	for _, u := range got.Users {
		names[u.ID] = u.FullName
		if u.Deactivated {
			t.Fatalf("user %d wrongly flagged deactivated", u.ID)
		}
	}
	if names[boot.UserID] != "Alice Chen" || names[bobID] != "Bob Ray" {
		t.Fatalf("names wrong: %v", names)
	}

	// Deactivated users still resolve (their history renders), flagged.
	if _, err := pool.Exec(ctx,
		`UPDATE user_account SET deactivated_at = now() WHERE id = $1`, bobID); err != nil {
		t.Fatalf("deactivate: %v", err)
	}
	if code := getJSON(t, fmt.Sprintf("%s/api/v1/users?ids=%d", ts.URL, bobID),
		boot.Token, &got); code != http.StatusOK {
		t.Fatalf("deactivated lookup: %d, want 200", code)
	}
	if len(got.Users) != 1 || !got.Users[0].Deactivated || got.Users[0].FullName != "Bob Ray" {
		t.Fatalf("deactivated profile wrong: %+v", got.Users)
	}

	// Guards: missing ids, malformed id, over-cap list.
	for name, q := range map[string]string{
		"missing": "", "malformed": "?ids=1,zap",
		"overcap": "?ids=" + strings.TrimSuffix(strings.Repeat("1,", 101), ","),
	} {
		u := ts.URL + "/api/v1/users" + q
		req, _ := http.NewRequestWithContext(ctx, "GET", u, nil)
		req.Header.Set("Authorization", "Bearer "+boot.Token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("%s query = %d, want 400", name, resp.StatusCode)
		}
	}
}
