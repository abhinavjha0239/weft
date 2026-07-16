package automation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/notification"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/internal/eventlog"
	"github.com/abhinavjha0239/weft/internal/platform/egress"
)

const (
	consumerName  = "automations"
	sweepInterval = 5 * time.Second
	batchSize     = 500
	// scheduleInterval is how often the scheduler lane claims due schedules;
	// scheduleClaimSize caps one claim's batch.
	scheduleInterval  = 30 * time.Second
	scheduleClaimSize = 100
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
	// egress is the SSRF-guarded client the delivery lane sends through — the
	// ONLY path an http_request step's outbound call may ride. Wired via
	// SetEgress; when nil the delivery lane is a no-op (tests that never
	// exercise deliveries construct the runner without it).
	egress *egress.Client
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

// SetEgress wires the SSRF-guarded egress client used by the delivery lane
// (P-24). Kept out of NewRunner so the event-consumer paths, which never dial,
// need no client — mirroring the SetMessaging/SetFiles wiring idiom.
func (r *Runner) SetEgress(c *egress.Client) { r.egress = c }

// Run blocks until ctx ends: LISTEN on the event channel and process the
// signalled org; a sweep catches anything a missed NOTIFY left behind.
func (r *Runner) Run(ctx context.Context) {
	go r.sweep(ctx)
	go r.scheduleLane(ctx)
	go r.deliveryLane(ctx)
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
	if !triggerMatches(rl, ev) {
		return false
	}
	// Loop guard (AU-4): never self-trigger; other rules' events only with
	// the explicit opt-in. Only automation-authored events carry this risk;
	// the schedule/webhook/slash lane events are system- or human-actored.
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
	// Channel-scope coverage applies to event and slash triggers (both carry a
	// channel_id in payload): a channel-scope rule fires only for its own
	// channel, an org-scope rule for any. Schedule/webhook triggers are
	// targeted by automation_id and carry no channel, so the filter neither
	// applies nor makes sense for them.
	switch rl.Def.Trigger.Kind {
	case kindSchedule, kindWebhook:
	default:
		if rl.ScopeType == ScopeChannel && eventChannel(ev.Payload) != rl.ScopeID {
			return false
		}
	}
	// Conditions (AU-1 filters) are the last gate, evaluated in memory: a miss
	// returns false so execute() is never reached and NO run row is written.
	return matchConditions(rl.Def.Conditions, ev.Payload)
}

// triggerMatches reports whether the event fires this rule's trigger, by kind.
// event: the subscribed verb. schedule/webhook: the lane's internal verb AND
// the event targets THIS rule (payload.automation_id) — these events can never
// fire another rule. slash: the invocation verb AND the command matches (scope
// coverage rides match's channel-scope filter). A legacy definition (no kind)
// normalizes to event via the default arm.
func triggerMatches(rl rule, ev eventlog.Row) bool {
	switch rl.Def.Trigger.Kind {
	case kindSchedule:
		return ev.Verb == verbScheduleDue && eventAutomationID(ev.Payload) == rl.ID
	case kindWebhook:
		return ev.Verb == verbWebhookReceived && eventAutomationID(ev.Payload) == rl.ID
	case kindSlash:
		return ev.Verb == verbSlashInvoked && rl.Def.Trigger.Command == eventCommand(ev.Payload)
	default:
		return ev.Verb == rl.Def.Trigger.Verb
	}
}

// eventAutomationID reads the target rule id from a targeted lane event's
// payload (zero when absent).
func eventAutomationID(payload json.RawMessage) int64 {
	var p struct {
		AutomationID int64 `json:"automation_id"`
	}
	_ = json.Unmarshal(payload, &p)
	return p.AutomationID
}

// eventCommand reads the invoked command from a slash event's payload.
func eventCommand(payload json.RawMessage) string {
	var p struct {
		Command string `json:"command"`
	}
	_ = json.Unmarshal(payload, &p)
	return p.Command
}

type stepTrace struct {
	Kind       string `json:"kind"`
	Status     string `json:"status"`
	MessageID  int64  `json:"message_id,omitempty"`
	DeliveryID int64  `json:"delivery_id,omitempty"`
	Error      string `json:"error,omitempty"`
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
			// An http_request step ENQUEUES a webhook_delivery row atomically
			// with the run and never dials — a slow or hostile endpoint can
			// never stall the org's event cursor (the send is the delivery
			// lane's job). A failed INSERT is an infra error that aborts the
			// whole run tx (the event stays unacked and retries), never a
			// per-step failure trace.
			if st.Kind == stepHTTPRequest {
				delID, err := enqueueDelivery(ctx, tx, orgID, rl.ID, runID, st.URL, ev)
				if err != nil {
					return err
				}
				traces = append(traces, stepTrace{Kind: st.Kind, Status: "queued", DeliveryID: delID})
				continue
			}
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
	recips, err := r.ruleAdmins(ctx, tx, orgID, rl.ScopeType, rl.ScopeID)
	if err != nil {
		return nil, err
	}
	if len(recips) == 0 {
		return nil, nil
	}
	return r.notif.RecordAutomationFailure(ctx, tx, orgID, runID, recips)
}

// ruleAdmins resolves a rule's write-gate holders — org rules → manage_org
// holders, channel rules → administer_channel holders (the requireScopeAdmin
// mirror, so whoever may edit the rule is who hears about it). Shared by the
// run-failure alert (notifyFailure) and the delivery-failure health alerts.
func (r *Runner) ruleAdmins(ctx context.Context, tx pgx.Tx, orgID int64, scopeType int16, scopeID int64) ([]int64, error) {
	switch scopeType {
	case ScopeOrg:
		return r.perms.HoldersAt(ctx, tx, orgID, perms.VerbManageOrg, perms.OrgScope(orgID))
	case ScopeChannel:
		chain, err := r.perms.ChannelScope(ctx, tx, orgID, scopeID)
		if err != nil {
			return nil, err
		}
		return r.perms.HoldersAt(ctx, tx, orgID, perms.VerbAdministerChannel, chain)
	default:
		return nil, nil // no admin verb for this scope yet — nobody to notify
	}
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

// scheduleLane fires due schedules on a fixed tick, beside sweep. Each
// appended event's NOTIFY wakes the consumer to run the rule.
func (r *Runner) scheduleLane(ctx context.Context) {
	t := time.NewTicker(scheduleInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if _, err := r.RunDueSchedules(ctx, time.Now()); err != nil && ctx.Err() == nil {
				r.log.Warn("automation: schedule lane", "err", err)
			}
		}
	}
}

// RunDueSchedules is the scheduler lane's claim, CAS-shaped: in ONE
// transaction it locks up to scheduleClaimSize due schedule rules (FOR UPDATE
// SKIP LOCKED, so concurrent runners never double-claim), and for each
// advances schedule_next_at to the next fire computed FROM NOW and appends an
// automation.schedule_due event — all atomic. Computing from now means a slot
// missed during downtime fires exactly ONCE on recovery, with no burst
// catch-up; idempotency across redelivery is the consumer's existing
// (automation, trigger_event_id) run key. Clock-injected for tests; the lane
// passes time.Now(). Returns the number of schedules fired.
func (r *Runner) RunDueSchedules(ctx context.Context, now time.Time) (int, error) {
	var fired int
	err := db.WithTx(ctx, r.pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT id, org_id, definition, schedule_next_at
			FROM automation
			WHERE schedule_next_at <= $1 AND enabled AND deleted_at IS NULL
			ORDER BY schedule_next_at
			FOR UPDATE SKIP LOCKED
			LIMIT $2`, now, scheduleClaimSize)
		if err != nil {
			return fmt.Errorf("automation: claim schedules: %w", err)
		}
		type claim struct {
			id, orgID    int64
			def          Definition
			scheduledFor time.Time
		}
		var claims []claim
		for rows.Next() {
			var c claim
			var raw []byte
			if err := rows.Scan(&c.id, &c.orgID, &raw, &c.scheduledFor); err != nil {
				rows.Close()
				return fmt.Errorf("automation: scan schedule: %w", err)
			}
			if err := json.Unmarshal(raw, &c.def); err != nil {
				// Unparseable: zero the definition so the defensive arm below
				// clears the fire time — otherwise the row re-claims forever.
				c.def = Definition{}
			}
			claims = append(claims, c)
		}
		// The cursor must close before we issue writes on the same tx; the
		// FOR UPDATE locks persist for the transaction, so the claim holds.
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for _, c := range claims {
			if c.def.Trigger.Kind != kindSchedule || c.def.Trigger.Schedule == nil {
				// Defensive: a non-schedule rule must never carry a fire time
				// (the lifecycle NULLs it). Clear it so it never re-claims.
				if _, err := tx.Exec(ctx,
					`UPDATE automation SET schedule_next_at = NULL WHERE id = $1`, c.id); err != nil {
					return fmt.Errorf("automation: clear stale schedule: %w", err)
				}
				continue
			}
			loc, lerr := scheduleLocation(c.def.Trigger.Schedule.TZ)
			if lerr != nil {
				loc = time.UTC // validated at write; defensive
			}
			next := nextFire(*c.def.Trigger.Schedule, now, loc)
			if _, err := tx.Exec(ctx,
				`UPDATE automation SET schedule_next_at = $2 WHERE id = $1`, c.id, next); err != nil {
				return fmt.Errorf("automation: advance schedule: %w", err)
			}
			if _, err := eventlog.Append(ctx, tx, eventlog.Event{
				OrgID: c.orgID, ActorKind: enum.ActorSystem,
				EntityType: enum.EntityAutomation, EntityID: c.id, Verb: verbScheduleDue,
				Payload: eventlog.MustPayload(map[string]any{
					"automation_id": c.id, "scheduled_for": c.scheduledFor}),
			}); err != nil {
				return fmt.Errorf("automation: append schedule_due: %w", err)
			}
			fired++
		}
		return nil
	})
	return fired, err
}

const (
	deliveryInterval = 15 * time.Second
	deliveryBatch    = 50
	// maxDeliveryBodyBytes caps the response body read on any attempt.
	maxDeliveryBodyBytes = 64 << 10
	// maxDeliveryAttempts: the 4th failed attempt is terminal; the first three
	// each schedule a retry.
	maxDeliveryAttempts = 4
	// Consecutive-terminal-failure health thresholds (AU-4): alert, alert
	// "disable imminent", auto-disable. Consts v1 (config knobs are a gap).
	deliveryAlert1      = 5
	deliveryAlert2      = 15
	deliveryAutoDisable = 20
)

// deliveryBackoff[attempt-1] is the wait before the next attempt after a
// non-terminal failure (attempts 1..3 → 1m, 5m, 30m).
var deliveryBackoff = [maxDeliveryAttempts - 1]time.Duration{time.Minute, 5 * time.Minute, 30 * time.Minute}

// deliveryLane drains due webhook deliveries on a fixed tick, beside sweep.
func (r *Runner) deliveryLane(ctx context.Context) {
	t := time.NewTicker(deliveryInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if _, err := r.RunDueDeliveries(ctx, time.Now()); err != nil && ctx.Err() == nil {
				r.log.Warn("automation: delivery lane", "err", err)
			}
		}
	}
}

// RunDueDeliveries attempts up to deliveryBatch pending deliveries whose
// next_attempt_at has arrived, EACH in its own short transaction that holds the
// row's lock across the send (FOR UPDATE SKIP LOCKED): the batch bounds the
// tick, and holding only one row's lock per send means a slow endpoint never
// blocks the rest. A crash mid-send releases that row's lock and the row
// retries — AT-LEAST-ONCE, the webhook norm (receivers dedupe on delivery_id);
// the mail lane's mark-then-send at-most-once is the opposite trade. The lane
// is global across orgs v1 (per-org fairness is a recorded gap). Clock-injected
// for tests; the lane passes time.Now(). Returns the number of rows attempted.
func (r *Runner) RunDueDeliveries(ctx context.Context, now time.Time) (int, error) {
	if r.egress == nil {
		return 0, nil // no egress wired (a non-delivery test) — nothing to do
	}
	rows, err := r.pool.Query(ctx, `
		SELECT id FROM webhook_delivery
		WHERE status = 1 AND next_attempt_at <= $1
		ORDER BY next_attempt_at
		LIMIT $2`, now, deliveryBatch)
	if err != nil {
		return 0, fmt.Errorf("automation: scan due deliveries: %w", err)
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, fmt.Errorf("automation: scan delivery id: %w", err)
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	attempted := 0
	for _, id := range ids {
		did, err := r.attemptDelivery(ctx, id, now)
		if err != nil {
			return attempted, err
		}
		if did {
			attempted++
		}
	}
	return attempted, nil
}

// attemptDelivery claims one delivery (re-checking due + status under the row
// lock, SKIP LOCKED so a racing worker's row is left alone) and sends it WHILE
// HOLDING that lock. 2xx → delivered + health reset; a guard rejection is
// terminal immediately (the destination will never become allowed); any other
// failure (non-2xx incl. 3xx, timeout, transport) increments attempts and
// backs off, going terminal on the 4th. Reports whether a row was attempted.
func (r *Runner) attemptDelivery(ctx context.Context, id int64, now time.Time) (bool, error) {
	var (
		attempted bool
		orgID     int64
		toPing    []int64
	)
	err := db.WithTx(ctx, r.pool, func(tx pgx.Tx) error {
		var (
			automationID, runID int64
			rawURL              string
			snapshot            json.RawMessage
			attempts            int
			name                string
			scopeType           int16
			scopeID             int64
			deleted             bool
			rawDef              []byte
		)
		err := tx.QueryRow(ctx, `
			SELECT w.org_id, w.automation_id, w.run_id, w.url, w.payload, w.attempts,
			       a.name, a.scope_type, a.scope_id, a.deleted_at IS NOT NULL, a.definition
			FROM webhook_delivery w
			JOIN automation a ON a.id = w.automation_id
			WHERE w.id = $1 AND w.status = 1 AND w.next_attempt_at <= $2
			FOR UPDATE OF w SKIP LOCKED`, id, now).
			Scan(&orgID, &automationID, &runID, &rawURL, &snapshot, &attempts,
				&name, &scopeType, &scopeID, &deleted, &rawDef)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil // another worker took it, or it advanced since the scan
		}
		if err != nil {
			return fmt.Errorf("automation: claim delivery: %w", err)
		}
		attempted = true

		attempt := attempts + 1
		var def Definition
		_ = json.Unmarshal(rawDef, &def) // headers ride the current definition
		body, err := json.Marshal(deliveryEnvelope{
			AutomationID: automationID, AutomationName: name, RunID: runID,
			DeliveryID: id, Attempt: attempt, Event: snapshot,
		})
		if err != nil {
			return fmt.Errorf("automation: marshal envelope: %w", err)
		}

		code, snippet, sendErr := r.sendDelivery(ctx, rawURL, httpStepHeaders(def, rawURL), body)
		if sendErr == nil && code >= 200 && code < 300 {
			if _, err := tx.Exec(ctx, `
				UPDATE webhook_delivery
				SET status = 2, attempts = $2, last_status_code = $3, delivered_at = $4
				WHERE id = $1`, id, attempt, int32(code), now); err != nil {
				return fmt.Errorf("automation: mark delivered: %w", err)
			}
			// Success resets the O(1) health counter.
			if _, err := tx.Exec(ctx,
				`UPDATE automation SET delivery_failures = 0 WHERE id = $1`, automationID); err != nil {
				return fmt.Errorf("automation: reset delivery failures: %w", err)
			}
			return nil
		}

		// Failure. last_status_code is the HTTP code when we got a response;
		// a guard/transport error leaves it NULL and records the error text.
		var code32 *int32
		lastErr := snippet
		if sendErr == nil {
			c := int32(code)
			code32 = &c
		} else {
			lastErr = sanitizeDeliveryText(sendErr.Error())
		}
		terminal := errors.Is(sendErr, egress.ErrDisallowed) || attempt >= maxDeliveryAttempts
		if !terminal {
			next := now.Add(deliveryBackoff[attempt-1])
			if _, err := tx.Exec(ctx, `
				UPDATE webhook_delivery
				SET attempts = $2, next_attempt_at = $3, last_status_code = $4, last_error = $5
				WHERE id = $1`, id, attempt, next, code32, lastErr); err != nil {
				return fmt.Errorf("automation: reschedule delivery: %w", err)
			}
			return nil
		}
		if _, err := tx.Exec(ctx, `
			UPDATE webhook_delivery
			SET status = 3, attempts = $2, last_status_code = $3, last_error = $4
			WHERE id = $1`, id, attempt, code32, lastErr); err != nil {
			return fmt.Errorf("automation: mark delivery failed: %w", err)
		}
		pings, err := r.recordDeliveryFailure(ctx, tx, orgID, automationID, id, scopeType, scopeID, deleted)
		if err != nil {
			return err
		}
		toPing = pings
		return nil
	})
	if err != nil {
		return attempted, err
	}
	// Live pings fire only after the commit succeeds (the alert row is the
	// durable truth; the ping is best-effort, DND-gated in PingNotification).
	for _, uid := range toPing {
		r.notif.PingNotification(ctx, orgID, uid,
			notification.KindAutomationFailure, int16(enum.EntityWebhookDelivery), id)
	}
	return attempted, nil
}

// sendDelivery performs one guarded POST and returns the status code plus a
// short sanitized snippet of the response body (empty on a guard/transport
// error). The read is capped at maxDeliveryBodyBytes. This is where a
// hostile/private destination is rejected by the egress guard (ErrDisallowed).
func (r *Runner) sendDelivery(ctx context.Context, rawURL string, headers map[string]string, body []byte) (int, string, error) {
	resp, err := r.egress.Post(ctx, rawURL, headers, body)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, maxDeliveryBodyBytes))
	return resp.StatusCode, sanitizeDeliveryText(string(b)), nil
}

// recordDeliveryFailure bumps the O(1) consecutive-failure counter on a
// terminal failure and drives the AU-4 alert-before-auto-disable ladder: alert
// the rule's write-gate holders at 5 and 15 (the P-25 machinery, kind 6, keyed
// on the delivery so each threshold is a distinct row and the 15 is "disable
// imminent"), and at 20 disable the rule with an automation.auto_disabled event
// plus a final alert. A soft-deleted rule's queued deliveries still drain, but
// there is nothing live to alert about or disable. Returns the users a
// post-commit ping should reach.
func (r *Runner) recordDeliveryFailure(ctx context.Context, tx pgx.Tx, orgID, automationID, deliveryID int64, scopeType int16, scopeID int64, deleted bool) ([]int64, error) {
	var failures int
	if err := tx.QueryRow(ctx, `
		UPDATE automation SET delivery_failures = delivery_failures + 1
		WHERE id = $1 RETURNING delivery_failures`, automationID).Scan(&failures); err != nil {
		return nil, fmt.Errorf("automation: bump delivery failures: %w", err)
	}
	if deleted {
		return nil, nil
	}
	switch {
	case failures == deliveryAlert1 || failures == deliveryAlert2:
		return r.alertDeliveryHolders(ctx, tx, orgID, deliveryID, scopeType, scopeID)
	case failures >= deliveryAutoDisable:
		// >= (not ==) so the streak disables even past the threshold — a
		// re-enable WITHOUT its counter reset must trip here immediately (the
		// red/green pin). The `AND enabled` guard fires the event and alert
		// only on the enabled→disabled TRANSITION, so a disabled rule's
		// remaining retries drain without re-firing either.
		tag, err := tx.Exec(ctx,
			`UPDATE automation SET enabled = false, updated_at = now()
			 WHERE id = $1 AND enabled`, automationID)
		if err != nil {
			return nil, fmt.Errorf("automation: auto-disable: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return nil, nil // already disabled: a draining retry, stay quiet
		}
		if _, err := eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: orgID, ActorKind: enum.ActorSystem,
			EntityType: enum.EntityAutomation, EntityID: automationID,
			Verb: "automation.auto_disabled",
			Payload: eventlog.MustPayload(map[string]any{
				"automation_id": automationID, "consecutive_failures": failures}),
		}); err != nil {
			return nil, fmt.Errorf("automation: auto_disabled event: %w", err)
		}
		return r.alertDeliveryHolders(ctx, tx, orgID, deliveryID, scopeType, scopeID)
	default:
		return nil, nil
	}
}

// alertDeliveryHolders records a kind-6 alert (entity = the delivery) for the
// rule's write-gate holders, reusing the P-25 dedupe/recipient machinery.
func (r *Runner) alertDeliveryHolders(ctx context.Context, tx pgx.Tx, orgID, deliveryID int64, scopeType int16, scopeID int64) ([]int64, error) {
	recips, err := r.ruleAdmins(ctx, tx, orgID, scopeType, scopeID)
	if err != nil {
		return nil, err
	}
	if len(recips) == 0 {
		return nil, nil
	}
	return r.notif.RecordAutomationDeliveryFailure(ctx, tx, orgID, deliveryID, recips)
}

// enqueueDelivery inserts one pending webhook_delivery in the run's tx (atomic
// with the run). payload is the trigger-event snapshot that becomes the
// envelope's "event" field at send; next_attempt_at defaults to now(), so the
// lane picks it up on the next tick.
func enqueueDelivery(ctx context.Context, tx pgx.Tx, orgID, automationID, runID int64, rawURL string, ev eventlog.Row) (int64, error) {
	var id int64
	if err := tx.QueryRow(ctx, `
		INSERT INTO webhook_delivery (org_id, automation_id, run_id, url, payload)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id`,
		orgID, automationID, runID, rawURL, eventSnapshot(ev)).Scan(&id); err != nil {
		return 0, fmt.Errorf("automation: enqueue delivery: %w", err)
	}
	return id, nil
}

// eventSnapshot captures the trigger event for the outbound body — the receiver
// gets the whole event (id, verb, time, payload), strictly more than a template
// could extract.
func eventSnapshot(ev eventlog.Row) json.RawMessage {
	payload := ev.Payload
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	return eventlog.MustPayload(map[string]any{
		"id":          ev.ID,
		"verb":        ev.Verb,
		"occurred_at": ev.OccurredAt,
		"payload":     payload,
	})
}

// sanitizeDeliveryText bounds a diagnostic string to 256 bytes of valid,
// NUL-free UTF-8 so it stores cleanly in last_error (a TEXT column).
func sanitizeDeliveryText(s string) string {
	if len(s) > 256 {
		s = s[:256]
	}
	return strings.ReplaceAll(strings.ToValidUTF8(s, ""), "\x00", "")
}
