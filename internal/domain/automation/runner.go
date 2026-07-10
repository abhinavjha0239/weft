package automation

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/internal/eventlog"
)

const (
	consumerName  = "automations"
	sweepInterval = 5 * time.Second
	batchSize     = 500
	// maxChainDepth caps rule→rule cascades (AU-4): an automation-caused
	// event carries its depth in the event hint; a rule that would exceed
	// the cap records a THROTTLED run instead of executing — visible in the
	// runs debugger, never a silent drop.
	maxChainDepth = 3
)

// Run statuses (automation_run.status, 0006 schema).
const (
	statusRunning   int16 = 1
	statusSuccess   int16 = 2
	statusFailed    int16 = 5
	statusThrottled int16 = 6
)

// Runner is the execution engine: a named, cursor-tracked, txid-gated
// event-log consumer (AU-1: triggers ARE the log). Matching reads the
// CURRENT enabled rules — a rule created or edited later applies to any
// not-yet-consumed events, and a deliberate cursor reset re-evaluates
// history under today's rules; the (automation, event) idempotency key
// absorbs redelivery, so nothing ever double-fires (AU-4).
//
// Guardrails, default-safe: an automation-caused event never triggers the
// automation that caused it (self-loop hard block); triggering OTHER rules
// requires their explicit allow_rule_trigger opt-in; and cascades stop at
// maxChainDepth with a visible throttled run.
type Runner struct {
	pool     *pgxpool.Pool
	consumer *eventlog.Consumer
	msg      *messaging.Service
	log      *slog.Logger
}

func NewRunner(pool *pgxpool.Pool, msg *messaging.Service, log *slog.Logger) *Runner {
	return &Runner{
		pool:     pool,
		consumer: eventlog.NewConsumer(pool, consumerName, batchSize),
		msg:      msg,
		log:      log,
	}
}

// Run blocks until ctx ends: LISTEN on the event channel and process the
// signalled org; a sweep catches anything a missed NOTIFY left behind.
func (r *Runner) Run(ctx context.Context) {
	go r.sweep(ctx)
	for ctx.Err() == nil {
		if err := r.listenLoop(ctx); err != nil && ctx.Err() == nil {
			r.log.Warn("automation: listen loop restarting", "err", err)
			time.Sleep(time.Second)
		}
	}
}

func (r *Runner) listenLoop(ctx context.Context) error {
	conn, err := r.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `LISTEN event_log`); err != nil {
		return err
	}
	for {
		n, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			return err
		}
		if orgID, err := strconv.ParseInt(n.Payload, 10, 64); err == nil {
			if err := r.ProcessOrg(ctx, orgID); err != nil && ctx.Err() == nil {
				r.log.Warn("automation: process", "org", orgID, "err", err)
			}
		}
	}
}

func (r *Runner) sweep(ctx context.Context) {
	t := time.NewTicker(sweepInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			rows, err := r.pool.Query(ctx, `SELECT id FROM org`)
			if err != nil {
				continue
			}
			var ids []int64
			for rows.Next() {
				var id int64
				if rows.Scan(&id) == nil {
					ids = append(ids, id)
				}
			}
			rows.Close()
			for _, id := range ids {
				_ = r.ProcessOrg(ctx, id)
			}
		}
	}
}

// rule is one enabled automation, definition pre-parsed for matching.
type rule struct {
	ID               int64
	ScopeType        int16
	ScopeID          int64
	Def              Definition
	AllowRuleTrigger bool
	ActorUserID      *int64
	Consented        bool
}

// ProcessOrg drains the org's pending events, executing every matching
// enabled rule, then acks the cursor. Exported for tests.
func (r *Runner) ProcessOrg(ctx context.Context, orgID int64) error {
	for {
		batch, err := r.consumer.Poll(ctx, orgID)
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			return nil
		}
		rules, err := r.loadRules(ctx, orgID)
		if err != nil {
			return err
		}
		for _, ev := range batch {
			if len(rules) == 0 {
				break
			}
			for _, rl := range rules {
				if !match(rl, ev) {
					continue
				}
				if err := r.execute(ctx, orgID, rl, ev); err != nil {
					return err
				}
			}
		}
		if err := r.consumer.Ack(ctx, orgID, batch[len(batch)-1].ID); err != nil {
			return err
		}
		// Keep polling until EMPTY, not merely under the batch size: steps
		// append events of their own, and a rule cascade (bounded by the
		// depth cap) should converge within one drain.
	}
}

func (r *Runner) loadRules(ctx context.Context, orgID int64) ([]rule, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, scope_type, scope_id, definition, allow_rule_trigger,
		       actor_user_id, actor_consent_at IS NOT NULL
		FROM automation
		WHERE org_id = $1 AND enabled AND deleted_at IS NULL`, orgID)
	if err != nil {
		return nil, fmt.Errorf("automation: load rules: %w", err)
	}
	defer rows.Close()
	var out []rule
	for rows.Next() {
		var rl rule
		var raw []byte
		if err := rows.Scan(&rl.ID, &rl.ScopeType, &rl.ScopeID, &raw,
			&rl.AllowRuleTrigger, &rl.ActorUserID, &rl.Consented); err != nil {
			return nil, fmt.Errorf("automation: scan rule: %w", err)
		}
		if err := json.Unmarshal(raw, &rl.Def); err != nil {
			continue // unparseable definition: skip, never wedge the org
		}
		out = append(out, rl)
	}
	return out, rows.Err()
}

// eventChannel extracts the channel a message event happened in (zero for
// non-channel events) — the channel-scope filter's key.
func eventChannel(payload json.RawMessage) int64 {
	var p struct {
		ChannelID int64 `json:"channel_id"`
	}
	_ = json.Unmarshal(payload, &p)
	return p.ChannelID
}

// eventDepth reads the automation chain depth from the event hint; human
// events have none and count as depth 0.
func eventDepth(hint json.RawMessage) int {
	if len(hint) == 0 {
		return 0
	}
	var h struct {
		AutomationDepth int `json:"automation_depth"`
	}
	_ = json.Unmarshal(hint, &h)
	return h.AutomationDepth
}

func match(rl rule, ev eventlog.Row) bool {
	if rl.Def.Trigger.Verb != ev.Verb {
		return false
	}
	// Loop guard (AU-4): never self-trigger; other rules' events only with
	// the explicit opt-in.
	if ev.ActorKind == enum.ActorAutomation {
		if ev.ActorID != nil && *ev.ActorID == rl.ID {
			return false
		}
		if !rl.AllowRuleTrigger {
			return false
		}
	}
	// Belt and braces: enabled implies consent (Update enforces it), but a
	// human-actor rule without consent must never run as that human.
	if rl.ActorUserID != nil && !rl.Consented {
		return false
	}
	if rl.ScopeType == ScopeChannel {
		return eventChannel(ev.Payload) == rl.ScopeID
	}
	return true
}

type stepTrace struct {
	Kind      string `json:"kind"`
	Status    string `json:"status"`
	MessageID int64  `json:"message_id,omitempty"`
	Error     string `json:"error,omitempty"`
}

// execute claims and runs one (rule, event) pair in a single transaction:
// the run row's idempotency key is the claim (a replay's INSERT conflicts
// and skips), and the steps' writes commit atomically with the run record.
// A failing step rolls back to its savepoint so the failure trace commits
// without the step's partial writes.
func (r *Runner) execute(ctx context.Context, orgID int64, rl rule, ev eventlog.Row) error {
	return db.WithTx(ctx, r.pool, func(tx pgx.Tx) error {
		var runID int64
		err := tx.QueryRow(ctx, `
			INSERT INTO automation_run (org_id, automation_id, trigger_event_id, status)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (automation_id, trigger_event_id) WHERE trigger_event_id IS NOT NULL
			DO NOTHING
			RETURNING id`,
			orgID, rl.ID, ev.ID, statusRunning).Scan(&runID)
		if err == pgx.ErrNoRows {
			return nil // already ran for this event (AU-4 idempotency)
		}
		if err != nil {
			return fmt.Errorf("automation: claim run: %w", err)
		}

		depth := eventDepth(ev.Hint)
		if depth >= maxChainDepth {
			return finishRun(ctx, tx, runID, statusThrottled, []stepTrace{{
				Status: "throttled",
				Error:  fmt.Sprintf("chain depth %d reached the cap (%d)", depth, maxChainDepth)}})
		}

		authorID := rl.ActorUserID
		if authorID == nil {
			pid, err := automationPrincipal(ctx, tx, orgID)
			if err != nil {
				return err
			}
			authorID = &pid
		}

		traces := make([]stepTrace, 0, len(rl.Def.Steps))
		status := statusSuccess
		for _, st := range rl.Def.Steps {
			target := st.ChannelID
			if target == 0 {
				target = rl.ScopeID // channel-scope default: its own channel
			}
			sp, err := tx.Begin(ctx)
			if err != nil {
				return fmt.Errorf("automation: savepoint: %w", err)
			}
			msgID, err := r.msg.PostToChannelAsAutomation(ctx, sp,
				orgID, *authorID, rl.ID, target, depth+1, st.Content)
			if err != nil {
				_ = sp.Rollback(ctx)
				traces = append(traces, stepTrace{Kind: st.Kind, Status: "error", Error: err.Error()})
				status = statusFailed
				break
			}
			if err := sp.Commit(ctx); err != nil {
				return fmt.Errorf("automation: step commit: %w", err)
			}
			traces = append(traces, stepTrace{Kind: st.Kind, Status: "ok", MessageID: msgID})
		}
		return finishRun(ctx, tx, runID, status, traces)
	})
}

func finishRun(ctx context.Context, tx pgx.Tx, runID int64, status int16, traces []stepTrace) error {
	steps, err := json.Marshal(traces)
	if err != nil {
		return fmt.Errorf("automation: marshal trace: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE automation_run SET status = $2, steps = $3, finished_at = now()
		WHERE id = $1`, runID, status, steps); err != nil {
		return fmt.Errorf("automation: finish run: %w", err)
	}
	return nil
}

// automationPrincipal is the per-org agent account automations author as
// when no human actor is named (F-13's scope identity) — created lazily,
// race-safe via the user_account origin key.
func automationPrincipal(ctx context.Context, tx pgx.Tx, orgID int64) (int64, error) {
	if _, err := tx.Exec(ctx, `
		INSERT INTO user_account (org_id, kind, full_name, role, origin_system, origin_id)
		VALUES ($1, 2, 'Automations', 50, 'system', 'automation-principal')
		ON CONFLICT (org_id, origin_system, origin_id) WHERE origin_system IS NOT NULL
		DO NOTHING`, orgID); err != nil {
		return 0, fmt.Errorf("automation: principal insert: %w", err)
	}
	var id int64
	if err := tx.QueryRow(ctx, `
		SELECT id FROM user_account
		WHERE org_id = $1 AND origin_system = 'system' AND origin_id = 'automation-principal'`,
		orgID).Scan(&id); err != nil {
		return 0, fmt.Errorf("automation: principal lookup: %w", err)
	}
	return id, nil
}
