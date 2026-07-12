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

	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/gateway"
)

// TestChannelFolders proves P-09's channel-folder + default-channel admin
// surfaces: CRUD, the folder assignment on channels (surfaced on the list and
// cleared by folder delete), the manage_org gate on every endpoint, and the
// replace-set validation for default channels (public + live + same-org only,
// atomic on failure).
func TestChannelFolders(t *testing.T) {
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
		Identity:  identity.New(pool, permsSvc),
		Messaging: messaging.New(pool, permsSvc),
	}))
	defer ts.Close()

	var boot struct {
		OrgID     int64  `json:"org_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "fold", "email": "alice@f.test", "password": "password123",
		"full_name": "Alice Admin",
	}, &boot)
	// A plain member (role:members, no manage_org) for the gate tests.
	bobTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"bob@f.test", "Bob Member", "bobfoldtok")

	folderURL := ts.URL + "/api/v1/channel-folders"

	// --- Folder CRUD ---
	var eng, ops messaging.Folder
	postJSON(t, folderURL, boot.Token, map[string]any{"name": "  Eng  "}, &eng)
	if eng.ID == 0 || eng.Name != "Eng" || eng.Position != 0 {
		t.Fatalf("create folder = %+v, want trimmed name Eng at position 0", eng)
	}
	postJSON(t, folderURL, boot.Token, map[string]any{"name": "Ops"}, &ops)
	if ops.Position != 1 {
		t.Fatalf("second folder position = %d, want 1 (append order)", ops.Position)
	}
	var fl struct {
		Folders []messaging.Folder `json:"folders"`
	}
	getJSON(t, folderURL, boot.Token, &fl)
	if len(fl.Folders) != 2 || fl.Folders[0].ID != eng.ID || fl.Folders[1].ID != ops.ID {
		t.Fatalf("list folders = %+v, want [Eng, Ops] by position", fl.Folders)
	}
	if code := patchJSON(t, fmt.Sprintf("%s/%d", folderURL, eng.ID), boot.Token,
		map[string]any{"name": "Engineering"}); code != http.StatusOK {
		t.Fatalf("rename folder = %d, want 200", code)
	}
	getJSON(t, folderURL, boot.Token, &fl)
	if fl.Folders[0].Name != "Engineering" {
		t.Fatalf("after rename, folder[0] = %q, want Engineering", fl.Folders[0].Name)
	}
	if code := postJSONStatus(t, folderURL, boot.Token, map[string]any{"name": "   "}); code != http.StatusBadRequest {
		t.Fatalf("blank folder name = %d, want 400", code)
	}
	if code := patchJSON(t, fmt.Sprintf("%s/999999", folderURL), boot.Token,
		map[string]any{"name": "Ghost"}); code != http.StatusNotFound {
		t.Fatalf("rename nonexistent folder = %d, want 404", code)
	}

	// --- Channel folder assignment ---
	var pub struct {
		ChannelID int64 `json:"channel_id"`
	}
	postJSON(t, ts.URL+"/api/v1/channels", boot.Token, map[string]any{"name": "random"}, &pub)
	chanURL := fmt.Sprintf("%s/api/v1/channels/%d", ts.URL, pub.ChannelID)
	if code := patchJSON(t, chanURL, boot.Token, map[string]any{"folder_id": eng.ID}); code != http.StatusOK {
		t.Fatalf("assign folder = %d, want 200", code)
	}
	if fid := channelFolderID(t, ts.URL, boot.Token, pub.ChannelID); fid == nil || *fid != eng.ID {
		t.Fatalf("channel folder_id after assign = %v, want %d", fid, eng.ID)
	}

	// A folder from another org must not be assignable (org isolation).
	var boot2 struct {
		OrgID int64  `json:"org_id"`
		Token string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "fold2", "email": "carl@f2.test", "password": "password123",
		"full_name": "Carl Other",
	}, &boot2)
	var foreign messaging.Folder
	postJSON(t, folderURL, boot2.Token, map[string]any{"name": "Foreign"}, &foreign)
	if code := patchJSON(t, chanURL, boot.Token, map[string]any{"folder_id": foreign.ID}); code != http.StatusBadRequest {
		t.Fatalf("assign foreign folder = %d, want 400", code)
	}
	// The rejected assignment did not change the channel's folder.
	if fid := channelFolderID(t, ts.URL, boot.Token, pub.ChannelID); fid == nil || *fid != eng.ID {
		t.Fatalf("channel folder_id after rejected assign = %v, want %d", fid, eng.ID)
	}

	// Clear with an explicit null.
	if code := patchJSON(t, chanURL, boot.Token, map[string]any{"folder_id": nil}); code != http.StatusOK {
		t.Fatalf("clear folder = %d, want 200", code)
	}
	if fid := channelFolderID(t, ts.URL, boot.Token, pub.ChannelID); fid != nil {
		t.Fatalf("channel folder_id after clear = %v, want nil", fid)
	}

	// Reassign to Ops, then delete Ops: the member channel's folder_id nulls.
	if code := patchJSON(t, chanURL, boot.Token, map[string]any{"folder_id": ops.ID}); code != http.StatusOK {
		t.Fatalf("reassign folder = %d, want 200", code)
	}
	if code := deleteReq(t, fmt.Sprintf("%s/%d", folderURL, ops.ID), boot.Token); code != http.StatusOK {
		t.Fatalf("delete folder = %d, want 200", code)
	}
	if fid := channelFolderID(t, ts.URL, boot.Token, pub.ChannelID); fid != nil {
		t.Fatalf("channel folder_id after folder delete = %v, want nil (hard delete nulls members)", fid)
	}
	getJSON(t, folderURL, boot.Token, &fl)
	if len(fl.Folders) != 1 || fl.Folders[0].ID != eng.ID {
		t.Fatalf("after deleting Ops, folders = %+v, want [Engineering]", fl.Folders)
	}
	if code := deleteReq(t, fmt.Sprintf("%s/999999", folderURL), boot.Token); code != http.StatusNotFound {
		t.Fatalf("delete nonexistent folder = %d, want 404", code)
	}

	// --- Default channels (replace-set) ---
	defURL := ts.URL + "/api/v1/default-channels"
	var priv struct {
		ChannelID int64 `json:"channel_id"`
	}
	postJSON(t, ts.URL+"/api/v1/channels", boot.Token,
		map[string]any{"name": "secret", "private": true}, &priv)
	var temp struct {
		ChannelID int64 `json:"channel_id"`
	}
	postJSON(t, ts.URL+"/api/v1/channels", boot.Token, map[string]any{"name": "temp"}, &temp)
	if code := patchJSON(t, fmt.Sprintf("%s/api/v1/channels/%d", ts.URL, temp.ChannelID),
		boot.Token, map[string]any{"archived": true}); code != http.StatusOK {
		t.Fatalf("archive temp = %d, want 200", code)
	}

	if code := putJSON(t, defURL, boot.Token, map[string]any{"channel_ids": []int64{pub.ChannelID}}); code != http.StatusOK {
		t.Fatalf("set defaults = %d, want 200", code)
	}
	var dc struct {
		ChannelIDs []int64 `json:"channel_ids"`
	}
	getJSON(t, defURL, boot.Token, &dc)
	if len(dc.ChannelIDs) != 1 || dc.ChannelIDs[0] != pub.ChannelID {
		t.Fatalf("get defaults = %+v, want [%d]", dc.ChannelIDs, pub.ChannelID)
	}
	if code := putJSON(t, defURL, boot.Token, map[string]any{"channel_ids": []int64{priv.ChannelID}}); code != http.StatusBadRequest {
		t.Fatalf("private default = %d, want 400", code)
	}
	if code := putJSON(t, defURL, boot.Token, map[string]any{"channel_ids": []int64{temp.ChannelID}}); code != http.StatusBadRequest {
		t.Fatalf("archived default = %d, want 400", code)
	}
	if code := putJSON(t, defURL, boot.Token, map[string]any{"channel_ids": []int64{999999}}); code != http.StatusBadRequest {
		t.Fatalf("foreign default = %d, want 400", code)
	}
	many := make([]int64, 21)
	for i := range many {
		many[i] = int64(i + 1)
	}
	if code := putJSON(t, defURL, boot.Token, map[string]any{"channel_ids": many}); code != http.StatusBadRequest {
		t.Fatalf(">20 defaults = %d, want 400", code)
	}
	// Every rejected replace-set rolled back: the valid set survives.
	getJSON(t, defURL, boot.Token, &dc)
	if len(dc.ChannelIDs) != 1 || dc.ChannelIDs[0] != pub.ChannelID {
		t.Fatalf("defaults after rejected sets = %+v, want unchanged [%d]", dc.ChannelIDs, pub.ChannelID)
	}

	// --- manage_org gate: a plain member is refused everywhere ---
	if code := postJSONStatus(t, folderURL, bobTok, map[string]any{"name": "sneaky"}); code != http.StatusForbidden {
		t.Fatalf("member create folder = %d, want 403", code)
	}
	if code := getJSON(t, folderURL, bobTok, nil); code != http.StatusForbidden {
		t.Fatalf("member list folders = %d, want 403", code)
	}
	if code := putJSON(t, defURL, bobTok, map[string]any{"channel_ids": []int64{pub.ChannelID}}); code != http.StatusForbidden {
		t.Fatalf("member set defaults = %d, want 403", code)
	}
	if code := getJSON(t, defURL, bobTok, nil); code != http.StatusForbidden {
		t.Fatalf("member list defaults = %d, want 403", code)
	}
}

// channelFolderID reads the folder_id the channel list surfaces for one channel.
func channelFolderID(t *testing.T, baseURL, token string, channelID int64) *int64 {
	t.Helper()
	var cl struct {
		Channels []messaging.ChannelSummary `json:"channels"`
	}
	getJSON(t, baseURL+"/api/v1/channels", token, &cl)
	for _, c := range cl.Channels {
		if c.ID == channelID {
			return c.FolderID
		}
	}
	t.Fatalf("channel %d not in list", channelID)
	return nil
}
