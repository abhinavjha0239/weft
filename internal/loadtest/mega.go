package loadtest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/time/rate"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
)

// MegaConfig drives the single-mega-org mode (S0): ONE org with a single
// channel of Members, Connections gateway connections onto it, and a bounded
// number of measured Sends. It exists to make the gateway's per-connection
// fan-out cost (the S3 blowup) and the closure-rebuild cost MEASURABLE against
// real numbers, so those slices attach a red/green pin instead of a claim.
type MegaConfig struct {
	BaseURL       string
	Members       int     // channel members provisioned (default 100000)
	Connections   int     // gateway connections opened (default 100000)
	Sends         int     // messages sent in the measured window (default 1)
	SendRate      float64 // sends/sec pacing (keep under the per-user 50/s cap)
	ProvisionConc int     // parallel session-token minting
}

func (c *MegaConfig) applyDefaults() {
	if c.Members <= 0 {
		c.Members = 100_000
	}
	if c.Connections <= 0 {
		c.Connections = 100_000
	}
	if c.Connections > c.Members {
		c.Connections = c.Members
	}
	if c.Sends <= 0 {
		c.Sends = 1
	}
	if c.SendRate <= 0 {
		c.SendRate = 40
	}
	if c.ProvisionConc <= 0 {
		c.ProvisionConc = 32
	}
}

// MegaResult is what the run recorded. PerMsgPumpQueries is THE headline: the
// gateway_pump_queries_total rise divided by messages sent — how many catch-up
// queries ONE message costs, which today scales with connection count (the
// blowup) and after S3's per-org multicast should be O(1).
type MegaResult struct {
	Members, Connections int
	Sent, Delivered      int
	PumpQueriesDelta     int64
	PerMsgPumpQueries    float64
	DelP50, DelP99       time.Duration
	DelMax               time.Duration
	ClosureRebuildSec    float64
	ProvisionElapsed     time.Duration
	SendElapsed          time.Duration
}

type megaOrg struct {
	orgID        int64
	channelID    int64
	ownerUserID  int64
	ownerToken   string
	memberTokens []string
}

// RunMega provisions the mega-org (outside the timing window, per PERF.md),
// opens the connections, drives the measured sends while correlating delivery
// latency by message id, reads the server's pump-query counter from
// /debug/vars before and after, and injects one group edit to time a
// closure rebuild against the mega-org.
func RunMega(ctx context.Context, pool *pgxpool.Pool, cfg MegaConfig) (MegaResult, error) {
	cfg.applyDefaults()

	provStart := time.Now()
	org, err := provisionMega(ctx, pool, cfg)
	if err != nil {
		return MegaResult{}, fmt.Errorf("provision: %w", err)
	}
	provElapsed := time.Since(provStart)

	var sentAt sync.Map // messageID → send time, for id-correlated latency
	delHist := &histogram{}
	var delivered int64

	subCtx, stopSubs := context.WithCancel(ctx)
	defer stopSubs()
	var subWG sync.WaitGroup
	ready := make(chan struct{}, cfg.Connections)
	for i := 0; i < cfg.Connections; i++ {
		u := megaWsURL(cfg.BaseURL, org.memberTokens[i])
		subWG.Add(1)
		go func(u string) {
			defer subWG.Done()
			subscribe(subCtx, u, &sentAt, delHist, &delivered, ready)
		}(u)
	}
	// Barrier: every connection dialed before the first send, so a tail-mode
	// (last_id=-1) subscriber cannot start past the message it must receive.
	deadline := time.After(60 * time.Second)
	for i := 0; i < cfg.Connections; i++ {
		select {
		case <-ready:
		case <-deadline:
			i = cfg.Connections
		case <-ctx.Done():
			stopSubs()
			return MegaResult{}, ctx.Err()
		}
	}
	// Settle: let the server register every connection and drain its immediate
	// (empty, tail-mode) catch-up pump BEFORE the baseline count is taken.
	time.Sleep(time.Second)

	client := &http.Client{
		Timeout: 15 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        cfg.Connections + 8,
			MaxIdleConnsPerHost: cfg.Connections + 8,
			IdleConnTimeout:     60 * time.Second,
		},
	}

	q0, err := fetchVar(client, cfg.BaseURL, "gateway_pump_queries_total")
	if err != nil {
		stopSubs()
		return MegaResult{}, fmt.Errorf("read baseline pump queries: %w", err)
	}

	sendStart := time.Now()
	sent, err := driveMegaSends(ctx, client, cfg, org, &sentAt)
	if err != nil {
		stopSubs()
		return MegaResult{}, fmt.Errorf("send: %w", err)
	}
	// Wait for the fan-out to land (or a bounded deadline): every delivery is a
	// pump query that ran after the baseline, so this makes the delta stable.
	want := int64(sent * cfg.Connections)
	waitDeadline := time.Now().Add(30 * time.Second)
	for atomic.LoadInt64(&delivered) < want && time.Now().Before(waitDeadline) {
		time.Sleep(20 * time.Millisecond)
	}
	sendElapsed := time.Since(sendStart)

	q1, err := fetchVar(client, cfg.BaseURL, "gateway_pump_queries_total")
	if err != nil {
		stopSubs()
		return MegaResult{}, fmt.Errorf("read final pump queries: %w", err)
	}

	closureSec, err := injectClosureRebuild(ctx, pool, org)
	if err != nil {
		stopSubs()
		return MegaResult{}, fmt.Errorf("closure rebuild: %w", err)
	}

	stopSubs()
	subWG.Wait()

	delta := int64(q1 - q0)
	perMsg := 0.0
	if sent > 0 {
		perMsg = float64(delta) / float64(sent)
	}
	return MegaResult{
		Members:           cfg.Members,
		Connections:       cfg.Connections,
		Sent:              sent,
		Delivered:         int(atomic.LoadInt64(&delivered)),
		PumpQueriesDelta:  delta,
		PerMsgPumpQueries: perMsg,
		DelP50:            delHist.percentile(0.50),
		DelP99:            delHist.percentile(0.99),
		DelMax:            delHist.max(),
		ClosureRebuildSec: closureSec,
		ProvisionElapsed:  provElapsed,
		SendElapsed:       sendElapsed,
	}, nil
}

// provisionMega builds the org + owner + #general via the service layer, then
// bulk-adds Members-1 more members (account + channel membership + the members
// role group) and mints session tokens for the first Connections of them. All
// of this is setup, deliberately outside the timing window (PERF.md).
func provisionMega(ctx context.Context, pool *pgxpool.Pool, cfg MegaConfig) (*megaOrg, error) {
	permsSvc := perms.New(pool)
	idsvc := identity.New(pool, permsSvc)
	runID := time.Now().UnixNano() % 1_000_000
	slug := fmt.Sprintf("mega%d", runID)
	boot, err := idsvc.Bootstrap(ctx, identity.BootstrapParams{
		OrgSlug:  slug,
		Email:    fmt.Sprintf("owner@%s.test", slug),
		Password: "megaorgpassword",
		FullName: "Mega Owner",
	})
	if err != nil {
		return nil, err
	}
	org := &megaOrg{
		orgID:       boot.OrgID,
		channelID:   boot.ChannelID,
		ownerUserID: boot.UserID,
		ownerToken:  boot.Token,
	}

	extra := cfg.Members - 1 // the owner already counts as a member
	if extra > 0 {
		memberIDs, err := bulkMembers(ctx, pool, boot.OrgID, boot.ChannelID, slug+"-m", extra)
		if err != nil {
			return nil, err
		}
		// A full-org closure with every member in it, so the injected rebuild
		// later times a REPRESENTATIVE mega-org, not a two-row toy.
		if err := db.WithTx(ctx, pool, func(tx pgx.Tx) error {
			return permsSvc.RebuildClosure(ctx, tx, boot.OrgID)
		}); err != nil {
			return nil, err
		}
		tokens, err := mintTokens(ctx, pool, memberIDs, min(cfg.Connections, len(memberIDs)), cfg.ProvisionConc)
		if err != nil {
			return nil, err
		}
		org.memberTokens = tokens
	}
	// Connections cannot exceed distinct member tokens; the owner backfills a
	// tiny remainder (e.g. Members==1) so the connection count is still honoured.
	for len(org.memberTokens) < cfg.Connections {
		org.memberTokens = append(org.memberTokens, org.ownerToken)
	}
	return org, nil
}

// bulkMembers inserts `n` member accounts, joins them to the channel, and adds
// them to the members role group, in three set-based statements. emailPrefix is
// a text-typed run-unique prefix (a bare int param in a `||` concat makes pgx
// try to encode it as text and fail — the anchor must already be text).
func bulkMembers(ctx context.Context, pool *pgxpool.Pool, orgID, channelID int64, emailPrefix string, n int) ([]int64, error) {
	rows, err := pool.Query(ctx, `
		INSERT INTO user_account (org_id, kind, email, full_name, role)
		SELECT $1, 1, $2 || g || '@mega.test', 'Member ' || g, 40
		FROM generate_series(1, $3) g
		RETURNING id`, orgID, emailPrefix, n)
	if err != nil {
		return nil, err
	}
	ids := make([]int64, 0, n)
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO channel_member (channel_id, user_id) SELECT $1, unnest($2::bigint[])`,
		channelID, ids); err != nil {
		return nil, err
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO user_group_member (group_id, user_id)
		SELECT (SELECT id FROM user_group WHERE org_id = $1 AND name = $2), u
		FROM unnest($3::bigint[]) u`, orgID, perms.GroupMembers, ids); err != nil {
		return nil, err
	}
	return ids, nil
}

// mintTokens creates one live session per member (each connection needs a
// bearer token) with bounded concurrency. auth.CreateSession mints the raw
// token and stores only its hash — the raw value it returns is what the WS
// dial presents.
func mintTokens(ctx context.Context, pool *pgxpool.Pool, memberIDs []int64, n, conc int) ([]string, error) {
	tokens := make([]string, n)
	sem := make(chan struct{}, max(1, conc))
	var wg sync.WaitGroup
	var firstErr atomic.Value
	for i := 0; i < n; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			token, err := auth.CreateSession(ctx, pool, memberIDs[i], "", "")
			if err != nil {
				firstErr.CompareAndSwap(nil, err)
				return
			}
			tokens[i] = token
		}(i)
	}
	wg.Wait()
	if e := firstErr.Load(); e != nil {
		return nil, e.(error)
	}
	return tokens, nil
}

func megaWsURL(base, token string) string {
	u := strings.Replace(base, "http://", "ws://", 1)
	u = strings.Replace(u, "https://", "wss://", 1)
	return u + "/api/v1/gateway?last_id=-1&token=" + token
}

// fetchVar reads one published expvar scalar from the server's /debug/vars.
// A metric not yet published reads as 0 (the counter simply has not moved).
func fetchVar(client *http.Client, baseURL, name string) (float64, error) {
	resp, err := client.Get(baseURL + "/debug/vars")
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("/debug/vars: status %d", resp.StatusCode)
	}
	var vars map[string]json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&vars); err != nil {
		return 0, err
	}
	raw, ok := vars[name]
	if !ok {
		return 0, nil
	}
	var v float64
	if err := json.Unmarshal(raw, &v); err != nil {
		return 0, fmt.Errorf("metric %q is not a scalar: %w", name, err)
	}
	return v, nil
}

// driveMegaSends posts Sends messages as the owner, paced under the per-user
// API cap, recording each send time by message id for latency correlation.
func driveMegaSends(ctx context.Context, client *http.Client, cfg MegaConfig, org *megaOrg, sentAt *sync.Map) (int, error) {
	lim := rate.NewLimiter(rate.Limit(cfg.SendRate), 1)
	url := fmt.Sprintf("%s/api/v1/channels/%d/messages", cfg.BaseURL, org.channelID)
	body := []byte(`{"content":"mega-org fan-out probe"}`)
	sent := 0
	for sent < cfg.Sends {
		if err := lim.Wait(ctx); err != nil {
			return sent, err
		}
		t0 := time.Now()
		req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
		if err != nil {
			return sent, err
		}
		req.Header.Set("Authorization", "Bearer "+org.ownerToken)
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			return sent, err
		}
		if resp.StatusCode >= 300 {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			return sent, fmt.Errorf("send status %d", resp.StatusCode)
		}
		var out struct {
			MessageID int64 `json:"message_id"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&out)
		resp.Body.Close()
		if out.MessageID != 0 {
			sentAt.Store(out.MessageID, t0)
		}
		sent++
	}
	return sent, nil
}

// injectClosureRebuild adds the owner to the moderators group — a real group
// edit — and times the full operation, which is dominated by the O(org) closure
// rebuild it triggers. The wall-time is the cost the scale-tier incremental
// rebuild must beat.
func injectClosureRebuild(ctx context.Context, pool *pgxpool.Pool, org *megaOrg) (float64, error) {
	permsSvc := perms.New(pool)
	start := time.Now()
	err := db.WithTx(ctx, pool, func(tx pgx.Tx) error {
		gid, err := permsSvc.SystemGroupID(ctx, tx, org.orgID, perms.GroupModerators)
		if err != nil {
			return err
		}
		return permsSvc.AddUserToGroup(ctx, tx, org.orgID, gid, org.ownerUserID)
	})
	if err != nil {
		return 0, err
	}
	return time.Since(start).Seconds(), nil
}
