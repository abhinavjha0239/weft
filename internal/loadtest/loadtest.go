// Package loadtest is Weft's load generator: it provisions many tenants,
// drives real HTTP message sends and real WebSocket fan-out against a running
// server, and reports throughput + correlated end-to-end latency.
//
// What is measured vs setup: provisioning uses the identity service directly
// against the DB (fast setup, NOT the measured path). The measured hot path —
// send → one transaction → event log → gateway fan-out → subscriber receipt —
// is 100% real HTTP + WebSocket + Postgres. Delivery latency is correlated by
// message id between the sending goroutine and every subscriber.
//
// Scale framing (docs/SCHEMA.md cell contract): these are PER-NODE / PER-CELL
// numbers. Fleet capacity = per-cell × cell count; there is no cross-org
// coordination to serialize, which is what makes that multiplication valid.
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

	"github.com/coder/websocket"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/time/rate"

	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
)

type Config struct {
	BaseURL           string
	Orgs              int
	SubscribersPerOrg int
	SendRatePerOrg    float64 // keep under the per-user API limit (50/s)
	Duration          time.Duration
	ProvisionConc     int
}

type Result struct {
	Orgs, Subscribers                 int
	Sent, SendErr, Send429, Delivered int64
	SendP50, SendP99, SendMax         time.Duration
	DelP50, DelP99, DelMax            time.Duration
	Elapsed                           time.Duration
}

func (r Result) SendThroughput() float64 {
	if r.Elapsed == 0 {
		return 0
	}
	return float64(r.Sent) / r.Elapsed.Seconds()
}

func (r Result) DeliveryThroughput() float64 {
	if r.Elapsed == 0 {
		return 0
	}
	return float64(r.Delivered) / r.Elapsed.Seconds()
}

type tenant struct {
	slug      string
	token     string
	channelID int64
}

// Run executes the full load test: provision → connect subscribers → drive
// sends for Duration → drain → report.
func Run(ctx context.Context, pool *pgxpool.Pool, cfg Config) (Result, error) {
	tenants, err := provision(ctx, pool, cfg)
	if err != nil {
		return Result{}, fmt.Errorf("provision: %w", err)
	}

	var sentAt sync.Map // messageID(int64) → time.Time
	sendHist := &histogram{}
	delHist := &histogram{}
	var sent, sendErr, send429, delivered int64

	// Prune sentAt so memory stays bounded to a few seconds of in-flight
	// correlation (deliveries complete in ms; anything older is done).
	pruneCtx, stopPrune := context.WithCancel(ctx)
	defer stopPrune()
	go prune(pruneCtx, &sentAt)

	client := &http.Client{
		Timeout: 15 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        cfg.Orgs * 2,
			MaxIdleConnsPerHost: cfg.Orgs * 2,
			MaxConnsPerHost:     0,
			IdleConnTimeout:     60 * time.Second,
		},
	}

	// Subscribers connect and read until the run context ends.
	subCtx, stopSubs := context.WithCancel(ctx)
	var subWG sync.WaitGroup
	subReady := make(chan struct{}, cfg.Orgs*cfg.SubscribersPerOrg)
	for _, tn := range tenants {
		wsURL := wsURL(cfg.BaseURL, tn.token)
		for s := 0; s < cfg.SubscribersPerOrg; s++ {
			subWG.Add(1)
			go func(u string) {
				defer subWG.Done()
				subscribe(subCtx, u, &sentAt, delHist, &delivered, subReady)
			}(wsURL)
		}
	}
	// Wait (bounded) for subscribers to be connected before sending.
	wantReady := cfg.Orgs * cfg.SubscribersPerOrg
	deadline := time.After(30 * time.Second)
	for i := 0; i < wantReady; i++ {
		select {
		case <-subReady:
		case <-deadline:
			i = wantReady
		case <-ctx.Done():
			stopSubs()
			return Result{}, ctx.Err()
		}
	}

	// Drive sends for the measured window.
	sendCtx, stopSend := context.WithTimeout(ctx, cfg.Duration)
	defer stopSend()
	start := time.Now()
	var sendWG sync.WaitGroup
	for _, tn := range tenants {
		sendWG.Add(1)
		go func(tn tenant) {
			defer sendWG.Done()
			driveSends(sendCtx, client, cfg, tn, &sentAt, sendHist,
				&sent, &sendErr, &send429)
		}(tn)
	}
	sendWG.Wait()
	elapsed := time.Since(start)

	// Grace for in-flight deliveries, then stop subscribers.
	time.Sleep(500 * time.Millisecond)
	stopSubs()
	subWG.Wait()

	return Result{
		Orgs:        cfg.Orgs,
		Subscribers: wantReady,
		Sent:        atomic.LoadInt64(&sent),
		SendErr:     atomic.LoadInt64(&sendErr),
		Send429:     atomic.LoadInt64(&send429),
		Delivered:   atomic.LoadInt64(&delivered),
		SendP50:     sendHist.percentile(0.50),
		SendP99:     sendHist.percentile(0.99),
		SendMax:     sendHist.max(),
		DelP50:      delHist.percentile(0.50),
		DelP99:      delHist.percentile(0.99),
		DelMax:      delHist.max(),
		Elapsed:     elapsed,
	}, nil
}

func provision(ctx context.Context, pool *pgxpool.Pool, cfg Config) ([]tenant, error) {
	idsvc := identity.New(pool, perms.New(pool))
	tenants := make([]tenant, cfg.Orgs)
	sem := make(chan struct{}, max(1, cfg.ProvisionConc))
	var wg sync.WaitGroup
	var firstErr atomic.Value
	runID := time.Now().UnixNano() % 1_000_000
	for i := 0; i < cfg.Orgs; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			slug := fmt.Sprintf("lt%d-%d", runID, i)
			res, err := idsvc.Bootstrap(ctx, identity.BootstrapParams{
				OrgSlug:  slug,
				Email:    fmt.Sprintf("owner@%s.test", slug),
				Password: "loadtestpassword",
				FullName: "Load Owner",
			})
			if err != nil {
				firstErr.CompareAndSwap(nil, err)
				return
			}
			tenants[i] = tenant{slug: slug, token: res.Token, channelID: res.ChannelID}
		}(i)
	}
	wg.Wait()
	if e := firstErr.Load(); e != nil {
		return nil, e.(error)
	}
	return tenants, nil
}

func driveSends(ctx context.Context, client *http.Client, cfg Config, tn tenant,
	sentAt *sync.Map, hist *histogram, sent, sendErr, send429 *int64) {
	lim := rate.NewLimiter(rate.Limit(cfg.SendRatePerOrg), 1)
	url := fmt.Sprintf("%s/api/v1/channels/%d/messages", cfg.BaseURL, tn.channelID)
	body := []byte(`{"content":"load test message with **bold** and a :rocket:"}`)
	for ctx.Err() == nil {
		if err := lim.Wait(ctx); err != nil {
			return
		}
		t0 := time.Now()
		req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
		if err != nil {
			return
		}
		req.Header.Set("Authorization", "Bearer "+tn.token)
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			// A send in flight when the window closes is cancelled, not a
			// real failure — don't count the shutdown edge.
			if ctx.Err() == nil {
				atomic.AddInt64(sendErr, 1)
			}
			continue
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			atomic.AddInt64(send429, 1)
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			continue
		}
		if resp.StatusCode >= 300 {
			atomic.AddInt64(sendErr, 1)
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			continue
		}
		var out struct {
			MessageID int64 `json:"message_id"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&out)
		resp.Body.Close()
		hist.record(time.Since(t0))
		if out.MessageID != 0 {
			sentAt.Store(out.MessageID, t0)
		}
		atomic.AddInt64(sent, 1)
	}
}

func subscribe(ctx context.Context, wsURL string, sentAt *sync.Map,
	delHist *histogram, delivered *int64, ready chan<- struct{}) {
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		select {
		case ready <- struct{}{}: // unblock the barrier even on failure
		default:
		}
		return
	}
	defer conn.CloseNow()
	conn.SetReadLimit(1 << 20)
	select {
	case ready <- struct{}{}:
	default:
	}
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return
		}
		var e struct {
			Type    string `json:"type"`
			Payload struct {
				MessageID int64 `json:"message_id"`
			} `json:"payload"`
		}
		if json.Unmarshal(data, &e) != nil || e.Type != "message.created" {
			continue
		}
		if v, ok := sentAt.Load(e.Payload.MessageID); ok {
			delHist.record(time.Since(v.(time.Time)))
			atomic.AddInt64(delivered, 1)
		}
	}
}

// prune bounds sentAt memory: deliveries are sub-second, so entries older than
// a few seconds are complete and safe to drop.
func prune(ctx context.Context, sentAt *sync.Map) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			sentAt.Range(func(k, v any) bool {
				if now.Sub(v.(time.Time)) > 10*time.Second {
					sentAt.Delete(k)
				}
				return true
			})
		}
	}
}

func wsURL(base, token string) string {
	u := strings.Replace(base, "http://", "ws://", 1)
	u = strings.Replace(u, "https://", "wss://", 1)
	return u + "/api/v1/gateway?last_id=0&token=" + token
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
