package rest

import (
	"context"
	"expvar"
	"fmt"
	"log/slog"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/gateway"
	"github.com/abhinavjha0239/weft/internal/platform/metrics"
)

// TestGatewayReconnectStorm is the end-to-end half of the reconnect-cohort
// resume: real Postgres, real WebSockets, real reconnects, and the read counted
// off the same expvar series an operator scrapes.
//
// A gateway node restart drops every connection it held and they all come back
// at once, resuming from cursors the F-2 checkpoint has converged onto the org
// head (measured — see the Gateway reconnect-cohort resume row in
// docs/REALITY.md). The cohort must cost O(cohort reads), not one catch-up read
// per connection, and it must cost that WITHOUT weakening the resume contract:
// every connection still receives every event after its own last_id, in order,
// exactly once, including one deliberately far-behind resumer in the same
// storm.
//
// The count is asserted at TWO cohort sizes and compared, because "fewer than
// five reads" passes on a small fixture whatever the code does; what must hold
// is that tripling the cohort does not triple the reads. The deterministic
// same-numbers-every-run version of that comparison is
// TestCatchupSharesOneReadPerCohort in the gateway package; this one proves the
// mechanism engages against a real storm, where the sharing depends on real
// arrival overlap.
//
// RED: restore the per-connection read (delete the shared-block lookups from
// gateway.catchUp) — the read count tracks the cohort size and the comparison
// below names both numbers.
func TestGatewayReconnectStorm(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		cancel()
		t.Fatalf("connect: %v", err)
	}
	defer func() { cancel(); pool.Close() }()
	resetAndMigrate(t, ctx, pool)

	hub := gateway.NewHub(pool, slog.Default())
	hub.SetMetrics(metrics.NewExpvar())
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
		"org_slug": "storm", "email": "a@st.test", "password": "password123",
		"full_name": "Alice",
	}, &boot)

	// One storm: `n` members of the channel reconnect AT ONCE from the cursor
	// they shared when the node went down, plus one connection that was offline
	// far longer. Returns the catch-up reads the whole storm cost.
	storm := func(label string, n int, gap int) float64 {
		t.Helper()
		// The cursor the cohort died on, and a much older one for the outlier.
		farBehind := orgHead(t, ctx, pool, boot.OrgID)
		for i := 0; i < 7; i++ {
			sendChannel(t, ts.URL, boot.Token, boot.ChannelID, fmt.Sprintf("%s-pre-%d", label, i))
		}
		resumeAt := orgHead(t, ctx, pool, boot.OrgID)
		// Sent while the whole cohort is away: this is the gap every one of
		// them must replay.
		for i := 0; i < gap; i++ {
			sendChannel(t, ts.URL, boot.Token, boot.ChannelID, fmt.Sprintf("%s-gap-%d", label, i))
		}
		// One container-less event at the END of the gap. It is org-visible by
		// design (gateway.filter), so it reaches the outsider below and PROVES
		// that connection is a real recipient of the shared block — without it,
		// "the outsider heard no channel event" would be satisfied by an
		// outsider that was simply never served at all.
		commitEvent(t, ctx, pool, boot.OrgID, orgWideVerb, nil)

		// Independently derived expectations: read out of event_log, not out of
		// what the gateway happened to send.
		wantCohort := messageEvents(t, ctx, pool, boot.OrgID, resumeAt)
		wantFar := messageEvents(t, ctx, pool, boot.OrgID, farBehind)
		if len(wantCohort) != gap {
			t.Fatalf("%s: fixture produced %d gap events, want %d", label, len(wantCohort), gap)
		}
		if len(wantFar) <= len(wantCohort) {
			t.Fatalf("%s: the far-behind resumer is not actually further behind (%d vs %d)",
				label, len(wantFar), len(wantCohort))
		}

		tokens := bulkChannelMembers(t, ctx, pool, boot.OrgID, boot.ChannelID, label, n+1)
		// The ACL negative, IN THE SAME STORM and on the SAME cursor: an org
		// member who is not in the channel. A shared read means shared ROWS, so
		// the only thing standing between this connection and the cohort's
		// channel traffic is its own filter — the property a careless
		// implementation of this slice breaks.
		outsiderTok := bareOrgMember(t, ctx, pool, boot.OrgID,
			label+"-outsider@st.test", "Outsider", label+"-outsider-tok")
		before := readResumeReads(t)

		// THE STORM: every connection dials at the same instant, which is what
		// a node restart looks like from the server's side.
		type result struct {
			seqs []int64
			err  error
		}
		results := make([]result, n+1)
		var outsiderErr error
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := dialClientLast(t, ctx, ts.URL, outsiderTok, fmt.Sprintf("%d", resumeAt))
			defer c.conn.CloseNow()
			outsiderErr = expectOnlyOrgWide(c)
		}()
		for i := range tokens {
			from, want := resumeAt, wantCohort
			if i == n { // the outlier, in the same storm
				from, want = farBehind, wantFar
			}
			wg.Add(1)
			go func(i int, tok string, from int64, want []int64) {
				defer wg.Done()
				c := dialClientLast(t, ctx, ts.URL, tok, fmt.Sprintf("%d", from))
				defer c.conn.CloseNow()
				seqs, err := collectMessageSeqs(c, len(want))
				results[i] = result{seqs: seqs, err: err}
			}(i, tokens[i], from, want)
		}
		wg.Wait()
		after := readResumeReads(t)

		if outsiderErr != nil {
			t.Fatalf("%s outsider: %v", label, outsiderErr)
		}
		for i, got := range results {
			want := wantCohort
			who := fmt.Sprintf("%s cohort connection %d", label, i)
			if i == n {
				want, who = wantFar, label+" far-behind resumer"
			}
			if got.err != nil {
				t.Fatalf("%s: %v (want the %d events after its last_id)", who, got.err, len(want))
			}
			if len(got.seqs) != len(want) {
				t.Fatalf("%s replayed %d events, want %d", who, len(got.seqs), len(want))
			}
			for k := range want {
				if got.seqs[k] != want[k] {
					t.Fatalf("%s replayed %v, want %v — a shared read must deliver each "+
						"connection its OWN gap, in order, with no hole and no duplicate",
						who, got.seqs, want)
				}
			}
		}
		return after - before
	}

	const small, large = 8, 24
	smallReads := storm("stormA", small, 6)
	largeReads := storm("stormB", large, 6)

	t.Logf("reconnect storm: %d connections → %g catch-up reads, %d connections → %g",
		small, smallReads, large, largeReads)
	if smallReads < 1 || largeReads < 1 {
		t.Fatalf("a storm issued no catch-up read at all (%g, %g); the gap would never replay",
			smallReads, largeReads)
	}
	// Constancy: tripling the cohort may cost a few more reads (sharing depends
	// on real arrival overlap, and the far-behind resumer always reads alone),
	// but it must not cost three times as many.
	if largeReads > smallReads+4 {
		t.Fatalf("catch-up reads scaled with the cohort: %d connections cost %g reads, "+
			"%d cost %g — a reconnect storm must cost O(cohort reads), not O(connections)",
			small, smallReads, large, largeReads)
	}
	if largeReads > float64(large)/3 {
		t.Fatalf("a %d-connection storm cost %g catch-up reads; want substantially fewer than %d",
			large, largeReads, large)
	}
}

// orgHead is the org's current event-log head — the cursor a connection holds
// when the node it was on goes down (the F-2 checkpoint hands every connection
// exactly this value, ACL gaps included).
func orgHead(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID int64) int64 {
	t.Helper()
	var id int64
	if err := pool.QueryRow(ctx,
		`SELECT COALESCE(MAX(id), 0) FROM event_log WHERE org_id = $1`, orgID).Scan(&id); err != nil {
		t.Fatalf("org head: %v", err)
	}
	return id
}

// messageEvents is the INDEPENDENT expectation: the message events a connection
// resuming at afterID is owed, read straight out of event_log rather than taken
// from what the gateway sent.
func messageEvents(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID, afterID int64) []int64 {
	t.Helper()
	rows, err := pool.Query(ctx, `
		SELECT id FROM event_log
		WHERE org_id = $1 AND id > $2 AND verb = 'message.created'
		ORDER BY id`, orgID, afterID)
	if err != nil {
		t.Fatalf("expected events: %v", err)
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan expected event: %v", err)
		}
		out = append(out, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("expected events: %v", err)
	}
	return out
}

// collectMessageSeqs drains one resuming connection until it has `want` message
// events, returning their seqs in arrival order. It is error-returning rather
// than t.Fatal-ing because it runs on the storm's own goroutines.
func collectMessageSeqs(c *wsClient, want int) ([]int64, error) {
	out := make([]int64, 0, want)
	deadline := time.After(30 * time.Second)
	for len(out) < want {
		select {
		case e, ok := <-c.events:
			if !ok {
				return out, fmt.Errorf("connection closed after %d events", len(out))
			}
			if e.Type == "message.created" {
				out = append(out, e.Seq)
			}
		case <-deadline:
			return out, fmt.Errorf("timed out after %d events", len(out))
		}
	}
	return out, nil
}

// orgWideVerb is the container-less event that bookends each storm's gap: it
// names no channel and no DM, so gateway.filter delivers it to every connection
// in the org, member or not.
const orgWideVerb = "emoji.created"

// expectOnlyOrgWide drains a NON-member resuming on the cohort's cursor and
// checks the two halves of the ACL claim at once: it must receive the
// container-less event that ends the gap (so it is PROVED to have been served
// the shared block, not merely ignored), and it must receive none of the
// channel traffic that precedes it in the very same block. Ordering makes the
// negative deterministic without a sleep: the resume lane replays in id order,
// so a wrongly-delivered channel event would arrive BEFORE the bookend.
func expectOnlyOrgWide(c *wsClient) error {
	deadline := time.After(30 * time.Second)
	for {
		select {
		case e, ok := <-c.events:
			if !ok {
				return fmt.Errorf("connection closed before the org-wide event arrived")
			}
			switch e.Type {
			case "message.created":
				return fmt.Errorf("a NON-member was replayed channel event seq %d out of the "+
					"cohort's shared block: a shared read is only safe because every "+
					"connection still applies its OWN filter to it", e.Seq)
			case orgWideVerb:
				return nil
			}
		case <-deadline:
			return fmt.Errorf("timed out waiting for the org-wide event; this connection was " +
				"never served the shared block, so the silence above proves nothing")
		}
	}
}

// readResumeReads reads the process-global gateway_resume_reads_total counter —
// the RESUME lane's catch-up reads, isolated from the live multicast reader's,
// read in-process for a before/after delta.
func readResumeReads(t *testing.T) float64 {
	t.Helper()
	v := expvar.Get("gateway_resume_reads_total")
	if v == nil {
		t.Fatal("gateway_resume_reads_total is not published; the resume lane is unmeasured")
	}
	f, ok := v.(*expvar.Float)
	if !ok {
		t.Fatalf("gateway_resume_reads_total is %T, want *expvar.Float", v)
	}
	return f.Value()
}
