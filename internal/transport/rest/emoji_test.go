package rest

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/domain/files"
	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/gateway"
	"github.com/abhinavjha0239/weft/internal/platform/blob"
)

func postEmoji(t *testing.T, base, token, name string, data []byte) int {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	fw, _ := mw.CreateFormFile("file", "emoji.bin")
	_, _ = fw.Write(data)
	mw.Close()
	u := fmt.Sprintf("%s/api/v1/emoji?name=%s", base, url.QueryEscape(name))
	req, _ := http.NewRequest("POST", u, &buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post emoji: %v", err)
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

// TestCustomEmoji: P-06 emoji — add_emoji gating (P-47), the name grammar, the
// reserved-even-when-deactivated uniqueness, soft delete with a live-only
// list, and a reaction with a custom name surfacing in the aggregates.
func TestCustomEmoji(t *testing.T) {
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
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity:  identity.New(pool, permsSvc),
		Messaging: messaging.New(pool, permsSvc),
		Files:     files.New(pool, store),
	}))
	defer ts.Close()

	var boot struct {
		OrgID     int64  `json:"org_id"`
		UserID    int64  `json:"user_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "emo", "email": "a@emo.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	bobTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"bob@emo.test", "Bob Ray", "bobemotok")

	png := pngBytes("emoji-image", 40)

	// add_emoji gate: a plain member cannot create.
	if code := postEmoji(t, ts.URL, bobTok, "partyparrot", png); code != http.StatusForbidden {
		t.Fatalf("member create emoji = %d, want 403", code)
	}
	// Name grammar: uppercase/short/symbols are 400 (rejected before upload).
	if code := postEmoji(t, ts.URL, boot.Token, "Party!", png); code != http.StatusBadRequest {
		t.Fatalf("bad emoji name = %d, want 400", code)
	}
	// Admin creates; duplicate is 409.
	if code := postEmoji(t, ts.URL, boot.Token, "partyparrot", png); code != http.StatusCreated {
		t.Fatalf("admin create emoji = %d, want 201", code)
	}
	if code := postEmoji(t, ts.URL, boot.Token, "partyparrot", png); code != http.StatusConflict {
		t.Fatalf("duplicate emoji = %d, want 409", code)
	}

	// List shows the live emoji with its file id.
	listEmoji := func() []messaging.Emoji {
		var resp struct {
			Emoji []messaging.Emoji `json:"emoji"`
		}
		getJSON(t, ts.URL+"/api/v1/emoji", boot.Token, &resp)
		return resp.Emoji
	}
	emos := listEmoji()
	if len(emos) != 1 || emos[0].Name != "partyparrot" || emos[0].FileID == 0 {
		t.Fatalf("emoji list = %+v, want [partyparrot]", emos)
	}

	// A reaction with the custom name surfaces in the message aggregates
	// (reactions store tokens; the registry only resolves the image).
	var sent struct {
		MessageID int64 `json:"message_id"`
	}
	postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, boot.ChannelID),
		boot.Token, map[string]any{"content": "party time"}, &sent)
	if code, st := react(t, ts.URL, boot.Token, sent.MessageID, "partyparrot", "PUT"); code != 200 ||
		st.Emoji != "partyparrot" || st.Count != 1 || !st.Me {
		t.Fatalf("react with custom emoji = %d %+v", code, st)
	}
	var msg messaging.Message
	getJSON(t, fmt.Sprintf("%s/api/v1/messages/%d", ts.URL, sent.MessageID), boot.Token, &msg)
	if len(msg.Reactions) != 1 || msg.Reactions[0].Emoji != "partyparrot" || msg.Reactions[0].Count != 1 {
		t.Fatalf("custom-emoji aggregate = %+v", msg.Reactions)
	}

	// Soft delete: gone from the live list, but the name stays RESERVED (a
	// re-create 409s), and deleting again is a 404.
	if code := deleteReq(t, ts.URL+"/api/v1/emoji/partyparrot", boot.Token); code != http.StatusOK {
		t.Fatalf("delete emoji = %d, want 200", code)
	}
	if emos := listEmoji(); len(emos) != 0 {
		t.Fatalf("after delete, live emoji = %+v, want empty", emos)
	}
	if code := postEmoji(t, ts.URL, boot.Token, "partyparrot", png); code != http.StatusConflict {
		t.Fatalf("re-create deactivated name = %d, want 409 (reserved)", code)
	}
	if code := deleteReq(t, ts.URL+"/api/v1/emoji/partyparrot", boot.Token); code != http.StatusNotFound {
		t.Fatalf("re-delete = %d, want 404", code)
	}
	// A plain member cannot delete either.
	if code := deleteReq(t, ts.URL+"/api/v1/emoji/partyparrot", bobTok); code != http.StatusForbidden {
		t.Fatalf("member delete = %d, want 403", code)
	}

	// Exactly one create and one delete event landed.
	for verb, want := range map[string]int{"emoji.created": 1, "emoji.deleted": 1} {
		var n int
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM event_log WHERE org_id = $1 AND verb = $2`,
			boot.OrgID, verb).Scan(&n); err != nil || n != want {
			t.Fatalf("%s events = %d (%v), want %d", verb, n, err, want)
		}
	}
}
