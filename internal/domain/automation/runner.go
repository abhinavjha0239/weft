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
	"github.com/abhinavjha0239/weft/internal/domain/notification"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
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
	statusRunning      int16 = 1
	statusSuccess      int16 = 2
	statusPartialError int16 = 4
	statusFailed       int16 = 5
	statusThrottled    int16 = 6
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
	perms    *perms.Service
	notif    *notification.Service
	log      *slog.Logger
}

func NewRunner(pool *pgxpool.Pool, msg *messaging.Service, p *perms.Service, notif *notification.Service, log *slog.Logger) *Runner {
	return &Runner{
		pool:     pool,
		consumer: eventlog.NewConsumer(pool, consumerName, batchSize),
		msg:      msg,
		perms:    p,
		notif:    notif,
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

// rule is one enabled automation, definition pre-parsed for matching. compiled
// holds per-step templating state computed once at load (the literal-side
// mention parse the guard needs) — never re-parsed per event.
type rule struct {
	ID               int64
	ScopeType        int16
	ScopeID          int64
	Def              Definition
	AllowRuleTrigger bool
	ActorUserID      *int64
	Consented        bool
	compiled         []compiledStep
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
		rl.compiled = compileSteps(rl.Def)
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
	if rl.ScopeType == ScopeChannel && eventChannel(ev.Payload) != rl.ScopeID {
		return false
	}
	// Conditions (AU-1 filters) are the last gate, evaluated in memory: a miss
	// returns false so execute() is never reached and NO run row is written.
	return matchConditions(rl.Def.Conditions, ev.Payload)
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
	var runID int64
	var toPing []int64
	err := db.WithTx(ctx, r.pool, func(tx pgx.Tx) error {
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
		var payloadRoot any
		var payloadDecoded bool
		for i, st := range rl.Def.Steps {
			// Templated steps interpolate the payload and pass the mention
			// guard before any write; a static step (every legacy step) posts
			// its content verbatim. The payload is decoded once, lazily, so a
			// non-templated rule carries zero added work.
			body := st.Content
			if rl.compiled[i].templated {
				if !payloadDecoded {
					payloadRoot = decodeUseNumber(ev.Payload)
					payloadDecoded = true
				}
				rendered, rerr := renderStep(i, st.Content, rl.compiled[i].litLabels, payloadRoot)
				if rerr != nil {
					traces = append(traces, stepTrace{Kind: st.Kind, Status: "error", Error: rerr.Error()})
					status = statusFailed
					break
				}
				body = rendered
			}
			target := st.ChannelID
			if target == 0 {
				target = rl.ScopeID // channel-scope default: its own channel
			}
			sp, err := tx.Begin(ctx)
			if err != nil {
				return fmt.Errorf("automation: savepoint: %w", err)
			}
			msgID, err := r.msg.PostToChannelAsAutomation(ctx, sp,
				orgID, *authorID, rl.ID, target, depth+1, body)
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
		if err := finishRun(ctx, tx, runID, status, traces); err != nil {
			return err
		}
		// P-25: a run that ENTERS the failing state alerts whoever administers
		// the rule (the write-gate holders), recorded in this same tx; the
		// live pings fire only after the commit succeeds.
		if status == statusFailed || status == statusPartialError {
			ids, err := r.notifyFailure(ctx, tx, orgID, rl, runID)
			if err != nil {
				return err
			}
			toPing = ids
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, uid := range toPing {
		r.notif.PingNotification(ctx, orgID, uid,
			notification.KindAutomationFailure, int16(enum.EntityAutomationRun), runID)
	}
	return nil
}

// notifyFailure records a kind-6 notification for whoever may administer the
// failed rule — but only on ENTRY into the failing state, throttled to at most
// once an hour while it keeps failing (any success re-arms). Recipients mirror
// the WRITE gate (requireScopeAdmin): org rules → manage_org holders, channel
// rules → administer_channel holders. Returns the users a live ping should
// reach (the ones actually inserted). Runs inside the finish transaction.
func (r *Runner) notifyFailure(ctx context.Context, tx pgx.Tx, orgID int64, rl rule, runID int64) ([]int64, error) {
	// Alert on entry only: inspect the most recent OTHER terminal run for this
	// rule; if it was already failing within the last hour, stay quiet. Rides
	// automation_run_list_idx (automation_id, id DESC).
	var recentlyFailing bool
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE((
		    SELECT status IN (4, 5) AND finished_at > now() - interval '1 hour'
		    FROM automation_run
		    WHERE automation_id = $1 AND id <> $2 AND status <> 1
		    ORDER BY id DESC LIMIT 1), false)`,
		rl.ID, runID).Scan(&recentlyFailing); err != nil {
		return nil, fmt.Errorf("automation: failure throttle probe: %w", err)
	}
	if recentlyFailing {
		return nil, nil
	}
	var recips []int64
	var err error
	switch rl.ScopeType {
	case ScopeOrg:
		recips, err = r.perms.HoldersAt(ctx, tx, orgID, perms.VerbManageOrg, perms.OrgScope(orgID))
	case ScopeChannel:
		chain, cerr := r.perms.ChannelScope(ctx, tx, orgID, rl.ScopeID)
		if cerr != nil {
			return nil, cerr
		}
		recips, err = r.perms.HoldersAt(ctx, tx, orgID, perms.VerbAdministerChannel, chain)
	default:
		return nil, nil // no admin verb for this scope yet — nobody to notify
	}
	if err != nil {
		return nil, err
	}
	if len(recips) == 0 {
		return nil, nil
	}
	return r.notif.RecordAutomationFailure(ctx, tx, orgID, runID, recips)
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
