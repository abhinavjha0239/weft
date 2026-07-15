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
	"github.com/abhinavjha0239/weft/internal/enum"
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

// TestDefaultChannelsOnInvite proves P-09's consumer: bootstrap seeds #general
// as a default channel, a new MEMBER auto-joins the workspace's default
// channels (deduped against the invite's explicit list) on accept, and a GUEST
// gets ONLY the invite's explicit channels — never the defaults (P-5).
func TestDefaultChannelsOnInvite(t *testing.T) {
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
		"org_slug": "cons", "email": "alice@c.test", "password": "password123",
		"full_name": "Alice Admin",
	}, &boot)

	// Bootstrap seeds #general as the workspace's default channel.
	defURL := ts.URL + "/api/v1/default-channels"
	var dc struct {
		ChannelIDs []int64 `json:"channel_ids"`
	}
	getJSON(t, defURL, boot.Token, &dc)
	if len(dc.ChannelIDs) != 1 || dc.ChannelIDs[0] != boot.ChannelID {
		t.Fatalf("bootstrap default channels = %+v, want [#general %d]", dc.ChannelIDs, boot.ChannelID)
	}

	// Two more public channels; make {#general, eng} the defaults.
	var eng, random struct {
		ChannelID int64 `json:"channel_id"`
	}
	postJSON(t, ts.URL+"/api/v1/channels", boot.Token, map[string]any{"name": "eng"}, &eng)
	postJSON(t, ts.URL+"/api/v1/channels", boot.Token, map[string]any{"name": "random"}, &random)
	if code := putJSON(t, defURL, boot.Token,
		map[string]any{"channel_ids": []int64{boot.ChannelID, eng.ChannelID}}); code != http.StatusOK {
		t.Fatalf("set defaults = %d, want 200", code)
	}

	// A member invite whose explicit channels OVERLAP the defaults (eng) and
	// add a non-default (random). Accept lands in the union, deduped.
	var inv identity.Invite
	postJSON(t, ts.URL+"/api/v1/invites", boot.Token, map[string]any{
		"channel_ids": []int64{eng.ChannelID, random.ChannelID}}, &inv)
	var carol identity.AcceptInviteResult
	postJSON(t, ts.URL+"/api/v1/invites/accept", "", map[string]any{
		"token": inv.Token, "email": "carol@c.test", "password": "password123",
		"full_name": "Carol Member"}, &carol)
	// The result payload surfaces only the invite's explicit channels.
	if len(carol.ChannelIDs) != 2 {
		t.Fatalf("member result channels = %+v, want the 2 explicit", carol.ChannelIDs)
	}
	// Actual membership is the union {#general, eng, random}: #general came from
	// the defaults (not in the invite), random from the invite (not a default),
	// eng from both.
	got := memberChannels(t, ctx, pool, carol.UserID)
	want := map[int64]bool{boot.ChannelID: true, eng.ChannelID: true, random.ChannelID: true}
	if !sameInt64Set(got, want) {
		t.Fatalf("member channels = %v, want {general, eng, random} %v", got, want)
	}
	// Dedup: eng (explicit AND default) produced exactly one member.joined.
	if n := memberJoinedCount(t, ctx, pool, carol.UserID, eng.ChannelID); n != 1 {
		t.Fatalf("member.joined for the overlapping channel = %d, want exactly 1 (deduped)", n)
	}

	// A guest invite (guests must enumerate channels) gets ONLY its explicit
	// channel — never the defaults (P-5).
	var ginv identity.Invite
	postJSON(t, ts.URL+"/api/v1/invites", boot.Token, map[string]any{
		"role": 50, "channel_ids": []int64{random.ChannelID}}, &ginv)
	var gina identity.AcceptInviteResult
	postJSON(t, ts.URL+"/api/v1/invites/accept", "", map[string]any{
		"token": ginv.Token, "email": "gina@c.test", "password": "password123",
		"full_name": "Gina Guest"}, &gina)
	if g := memberChannels(t, ctx, pool, gina.UserID); !sameInt64Set(g, map[int64]bool{random.ChannelID: true}) {
		t.Fatalf("guest channels = %v, want {random} only (no defaults)", g)
	}
}

func memberChannels(t *testing.T, ctx context.Context, pool *pgxpool.Pool, userID int64) map[int64]bool {
	t.Helper()
	rows, err := pool.Query(ctx,
		`SELECT channel_id FROM channel_member WHERE user_id = $1 AND unsubscribed_at IS NULL`, userID)
	if err != nil {
		t.Fatalf("member channels: %v", err)
	}
	defer rows.Close()
	out := map[int64]bool{}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan channel: %v", err)
		}
		out[id] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	return out
}

func memberJoinedCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, userID, channelID int64) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM event_log
		WHERE verb = 'member.joined' AND actor_id = $1
		  AND entity_type = $2 AND entity_id = $3`,
		userID, int16(enum.EntityChannel), channelID).Scan(&n); err != nil {
		t.Fatalf("event count: %v", err)
	}
	return n
}

func sameInt64Set(a, b map[int64]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if !b[k] {
			return false
		}
	}
	return true
}

// TestArchivedChannelsSkippedOnAccept: the review-batch hardening — a channel
// archived AFTER being listed (as an org default OR on an invite's explicit
// list) must not gain members at accept time. The join is guarded at the one
// shared site (joinChannelOnAccept: INSERT … SELECT WHERE archived_at IS
// NULL, org-pinned), so a stale default_channel row or a stale invite cannot
// put anyone into a read-only channel; account provisioning itself still
// succeeds (a since-archived channel silently doesn't join — signup must not
// fail over it), and no member.joined is emitted for the skipped channel.
func TestArchivedChannelsSkippedOnAccept(t *testing.T) {
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
		"org_slug": "arc", "email": "alice@arc.test", "password": "password123",
		"full_name": "Alice Admin",
	}, &boot)

	memberOf := func(email string, channelID int64) bool {
		t.Helper()
		var ok bool
		if err := pool.QueryRow(ctx, `
			SELECT EXISTS (
			  SELECT 1 FROM channel_member cm
			  JOIN user_account u ON u.id = cm.user_id
			  WHERE u.email = $1 AND cm.channel_id = $2
			    AND cm.unsubscribed_at IS NULL)`, email, channelID).Scan(&ok); err != nil {
			t.Fatalf("memberOf: %v", err)
		}
		return ok
	}

	// Defaults path: {#general, doomed} are the defaults; doomed archives
	// AFTER being set (the stale default_channel row stays behind).
	var doomed struct {
		ChannelID int64 `json:"channel_id"`
	}
	postJSON(t, ts.URL+"/api/v1/channels", boot.Token, map[string]any{"name": "doomed"}, &doomed)
	if code := putJSON(t, ts.URL+"/api/v1/default-channels", boot.Token,
		map[string]any{"channel_ids": []int64{boot.ChannelID, doomed.ChannelID}}); code != http.StatusOK {
		t.Fatalf("set defaults = %d", code)
	}
	if code := patchJSON(t, fmt.Sprintf("%s/api/v1/channels/%d", ts.URL, doomed.ChannelID),
		boot.Token, map[string]any{"archived": true}); code != http.StatusOK {
		t.Fatalf("archive doomed = %d", code)
	}

	var inv identity.Invite
	postJSON(t, ts.URL+"/api/v1/invites", boot.Token, map[string]any{}, &inv)
	var carol identity.AcceptInviteResult
	postJSON(t, ts.URL+"/api/v1/invites/accept", "", map[string]any{
		"token": inv.Token, "email": "carol@arc.test", "password": "password123",
		"full_name": "Carol"}, &carol)
	if carol.UserID == 0 {
		t.Fatal("accept must still provision the account")
	}
	if !memberOf("carol@arc.test", boot.ChannelID) {
		t.Fatal("carol must join the live default #general")
	}
	if memberOf("carol@arc.test", doomed.ChannelID) {
		t.Fatal("carol must NOT join the archived default")
	}
	var doomedJoins int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM event_log
		WHERE org_id = $1 AND verb = 'member.joined'
		  AND entity_id = $2 AND actor_id = $3`,
		boot.OrgID, doomed.ChannelID, carol.UserID).Scan(&doomedJoins); err != nil || doomedJoins != 0 {
		t.Fatalf("member.joined for archived channel = %d (%v), want 0", doomedJoins, err)
	}

	// Explicit path: an invite names a channel that archives before accept.
	var doomed2 struct {
		ChannelID int64 `json:"channel_id"`
	}
	postJSON(t, ts.URL+"/api/v1/channels", boot.Token, map[string]any{"name": "doomed2"}, &doomed2)
	var inv2 identity.Invite
	postJSON(t, ts.URL+"/api/v1/invites", boot.Token,
		map[string]any{"channel_ids": []int64{doomed2.ChannelID}}, &inv2)
	if code := patchJSON(t, fmt.Sprintf("%s/api/v1/channels/%d", ts.URL, doomed2.ChannelID),
		boot.Token, map[string]any{"archived": true}); code != http.StatusOK {
		t.Fatalf("archive doomed2 = %d", code)
	}
	var dave identity.AcceptInviteResult
	postJSON(t, ts.URL+"/api/v1/invites/accept", "", map[string]any{
		"token": inv2.Token, "email": "dave@arc.test", "password": "password123",
		"full_name": "Dave"}, &dave)
	if dave.UserID == 0 {
		t.Fatal("accept must still provision dave")
	}
	if memberOf("dave@arc.test", doomed2.ChannelID) {
		t.Fatal("dave must NOT join the channel archived after his invite was minted")
	}
}
