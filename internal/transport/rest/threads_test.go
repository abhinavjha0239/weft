package rest

import (
	"bytes"
	"context"
	"encoding/json"
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
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/gateway"
)

func patchJSON(t *testing.T, url, token string, body any) int {
	t.Helper()
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest("PATCH", url, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PATCH %s: %v", url, err)
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

func getJSON(t *testing.T, url, token string, out any) int {
	t.Helper()
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	if out != nil {
		_ = json.NewDecoder(resp.Body).Decode(out)
	}
	return resp.StatusCode
}

func TestThreadLifecycle(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	pool, err := pgxpool.New(ctx, url)
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
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "threads", "email": "t@t.test", "password": "password123",
		"full_name": "Thread Owner",
	}, &boot)
	chURL := fmt.Sprintf("%s/api/v1/channels/%d", ts.URL, boot.ChannelID)

	// Create two titled threads and one untitled.
	var th1, th2, th3 struct {
		ThreadID      int64 `json:"thread_id"`
		RootMessageID int64 `json:"root_message_id"`
	}
	postJSON(t, chURL+"/threads", boot.Token,
		map[string]any{"title": "Deploy plan", "content": "kickoff **notes**"}, &th1)
	postJSON(t, chURL+"/threads", boot.Token,
		map[string]any{"title": "Bug triage", "content": "list incoming"}, &th2)
	postJSON(t, chURL+"/threads", boot.Token,
		map[string]any{"content": "untitled side thread"}, &th3)
	if th1.ThreadID == 0 || th2.ThreadID == 0 || th3.ThreadID == 0 {
		t.Fatalf("thread creation incomplete: %+v %+v %+v", th1, th2, th3)
	}

	// Reply into thread 1 → its activity/count bumps.
	postJSON(t, chURL+"/messages", boot.Token,
		map[string]any{"thread_id": th1.ThreadID, "content": "follow-up"}, nil)

	// List: newest activity first → th1 first (just replied); root excluded.
	var page struct {
		Threads []struct {
			ID           int64  `json:"id"`
			Title        string `json:"title"`
			MessageCount int    `json:"message_count"`
			Resolved     bool   `json:"resolved"`
		} `json:"threads"`
		NextCursor string `json:"next_cursor"`
	}
	if code := getJSON(t, chURL+"/threads?limit=10", boot.Token, &page); code != 200 {
		t.Fatalf("list threads: %d", code)
	}
	if len(page.Threads) != 3 {
		t.Fatalf("thread count = %d, want 3 (root must be excluded)", len(page.Threads))
	}
	if page.Threads[0].ID != th1.ThreadID || page.Threads[0].MessageCount != 2 {
		t.Fatalf("ordering/count wrong: first=%+v", page.Threads[0])
	}

	// Keyset pagination: limit 2 → cursor → remaining 1, no overlap.
	var p1, p2 struct {
		Threads []struct {
			ID int64 `json:"id"`
		} `json:"threads"`
		NextCursor string `json:"next_cursor"`
	}
	getJSON(t, chURL+"/threads?limit=2", boot.Token, &p1)
	if len(p1.Threads) != 2 || p1.NextCursor == "" {
		t.Fatalf("page1 = %+v", p1)
	}
	getJSON(t, chURL+"/threads?limit=2&cursor="+p1.NextCursor, boot.Token, &p2)
	if len(p2.Threads) != 1 || p2.Threads[0].ID == p1.Threads[0].ID || p2.Threads[0].ID == p1.Threads[1].ID {
		t.Fatalf("page2 = %+v", p2)
	}

	// Resolve + retitle thread 2; idempotent resolve.
	thURL := fmt.Sprintf("%s/api/v1/threads/%d", ts.URL, th2.ThreadID)
	if code := patchJSON(t, thURL, boot.Token, map[string]any{
		"resolved": true, "title": "Bug triage (done)"}); code != 200 {
		t.Fatalf("resolve+retitle: %d", code)
	}
	if code := patchJSON(t, thURL, boot.Token, map[string]any{"resolved": true}); code != 200 {
		t.Fatalf("idempotent resolve: %d", code)
	}
	getJSON(t, chURL+"/threads?limit=10", boot.Token, &page)
	for _, th := range page.Threads {
		if th.ID == th2.ThreadID && (!th.Resolved || th.Title != "Bug triage (done)") {
			t.Fatalf("resolve/retitle not reflected: %+v", th)
		}
	}

	// F-15: the channel root thread rejects mutation.
	var rootID int64
	if err := pool.QueryRow(ctx,
		`SELECT root_thread_id FROM channel WHERE id = $1`, boot.ChannelID).Scan(&rootID); err != nil {
		t.Fatalf("root lookup: %v", err)
	}
	if code := patchJSON(t, fmt.Sprintf("%s/api/v1/threads/%d", ts.URL, rootID),
		boot.Token, map[string]any{"resolved": true}); code != http.StatusBadRequest {
		t.Fatalf("root mutation must 400, got %d", code)
	}

	// Thread messages: newest-first with before-cursor.
	var msgs struct {
		Messages []struct {
			ID     int64  `json:"id"`
			Source string `json:"source"`
		} `json:"messages"`
	}
	getJSON(t, fmt.Sprintf("%s/api/v1/threads/%d/messages?limit=10", ts.URL, th1.ThreadID),
		boot.Token, &msgs)
	if len(msgs.Messages) != 2 || msgs.Messages[0].Source != "follow-up" {
		t.Fatalf("thread messages = %+v", msgs.Messages)
	}

	// Verb wiring: restrict create_thread to admins at channel scope → a
	// plain member is denied thread creation but can still send.
	var memberTok string
	{
		var uid int64
		err := pool.QueryRow(ctx, `
			INSERT INTO user_account (org_id, kind, email, full_name, role)
			VALUES ($1, 1, 'm@t.test', 'Plain Member', 40) RETURNING id`,
			boot.OrgID).Scan(&uid)
		if err != nil {
			t.Fatalf("member: %v", err)
		}
		_, _ = pool.Exec(ctx,
			`INSERT INTO channel_member (channel_id, user_id) VALUES ($1, $2)`,
			boot.ChannelID, uid)
		_, _ = pool.Exec(ctx, `
			INSERT INTO user_group_member (group_id, user_id)
			SELECT id, $2 FROM user_group WHERE org_id = $1 AND name = 'role:members'`,
			boot.OrgID, uid)
		// Rebuild closure via the service layer.
		psvc := perms.New(pool)
		if err := db.WithTx(ctx, pool, func(tx pgx.Tx) error {
			return psvc.RebuildClosure(ctx, tx, boot.OrgID)
		}); err != nil {
			t.Fatalf("closure: %v", err)
		}
		if err := pool.QueryRow(ctx, `
			INSERT INTO auth_session (user_id, token_hash, expires_at)
			VALUES ($1, encode(sha256('membertoken'::bytea), 'hex'), now() + interval '1 day')
			RETURNING 'membertoken'`, uid).Scan(&memberTok); err != nil {
			t.Fatalf("session: %v", err)
		}
		_, _ = pool.Exec(ctx, `
			INSERT INTO permission_assignment (org_id, verb, scope_type, scope_id, group_id)
			SELECT $1, 'create_thread', 3, $2, id FROM user_group
			WHERE org_id = $1 AND name = 'role:admins'`, boot.OrgID, boot.ChannelID)
	}
	code := postJSONStatus(t, chURL+"/threads", memberTok,
		map[string]any{"title": "nope", "content": "denied?"})
	if code != http.StatusForbidden {
		t.Fatalf("member thread creation must 403 under channel override, got %d", code)
	}
	code = postJSONStatus(t, chURL+"/messages", memberTok, map[string]any{"content": "still fine"})
	if code != http.StatusCreated {
		t.Fatalf("member send must remain allowed, got %d", code)
	}
}

func postJSONStatus(t *testing.T, url, token string, body any) int {
	t.Helper()
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", url, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	return resp.StatusCode
}
