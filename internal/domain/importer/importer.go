package importer

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/domain/content"
	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/internal/eventlog"
)

const originZulip = "zulip"

type Service struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Service { return &Service{pool: pool} }

// Report is the fidelity contract (ADR-001: "nobody trusts a migrator that
// hides its losses"). Every source entity lands in exactly one bucket.
type Report struct {
	DryRun bool `json:"dry_run"`

	// Imported (lossless or transformed).
	Users, Channels, Threads, Messages, Reactions, Subscriptions int            `json:"-"`
	ImportedCounts                                               map[string]int `json:"imported"`

	// Skipped-with-reason (documented losses of this importer version).
	BotsSkipped        int `json:"bots_skipped"`
	DMMessagesSkipped  int `json:"dm_messages_skipped"`
	EditHistoryDropped int `json:"edit_history_dropped"`
	ReactionsUnmapped  int `json:"reactions_unmapped"`

	// Idempotency: rows already present from a previous run.
	AlreadyImported int `json:"already_imported"`

	// Transformations applied (visible, not silent).
	RenamedChannels map[string]string `json:"renamed_channels,omitempty"`
}

func (r *Report) finalize() {
	r.ImportedCounts = map[string]int{
		"users": r.Users, "channels": r.Channels, "threads": r.Threads,
		"messages": r.Messages, "reactions": r.Reactions,
		"subscriptions": r.Subscriptions,
	}
}

// Run imports an unpacked Zulip export into an existing org. Idempotent:
// every entity upserts by (org, origin_system, origin_id); re-runs count
// AlreadyImported instead of duplicating (ADR-001 D5). dryRun parses and
// reports without writing.
//
// One transaction for atomicity (an import is all-or-nothing); chunked
// streaming per messages file is the scale-tier follow-up for multi-GB
// exports and keeps this exact call shape.
func (s *Service) Run(ctx context.Context, orgID int64, dir string, dryRun bool) (Report, error) {
	ex, err := LoadZulipExport(dir)
	if err != nil {
		return Report{}, err
	}
	rep := Report{DryRun: dryRun, RenamedChannels: map[string]string{}}

	// Dry-run: pure accounting pass.
	if dryRun {
		for _, u := range ex.Users {
			if u.IsBot {
				rep.BotsSkipped++
			} else {
				rep.Users++
			}
		}
		rep.Channels = len(ex.Streams)
		topics := map[string]bool{}
		for _, m := range ex.Messages {
			if _, ok := ex.StreamByRecipient[m.Recipient]; !ok {
				rep.DMMessagesSkipped++
				continue
			}
			rep.Messages++
			if m.EditHistory != nil && *m.EditHistory != "" && *m.EditHistory != "null" {
				rep.EditHistoryDropped++
			}
			topics[fmt.Sprintf("%d\x00%s", ex.StreamByRecipient[m.Recipient], m.Subject)] = true
		}
		rep.Threads = len(topics)
		rep.Reactions = len(ex.Reactions)
		rep.Subscriptions = len(ex.Subscriptions)
		rep.finalize()
		return rep, nil
	}

	err = db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		return s.write(ctx, tx, orgID, ex, &rep)
	})
	if err != nil {
		return Report{}, err
	}
	rep.finalize()
	return rep, nil
}

func (s *Service) write(ctx context.Context, tx pgx.Tx, orgID int64, ex *Export, rep *Report) error {
	// Collision maps are pre-loaded because a unique-violation ERROR aborts
	// the whole transaction — all conflict handling happens in Go, and the
	// only ON CONFLICT used is the origin index (idempotent re-runs).
	emailToID := map[string]int64{}
	rows, err := tx.Query(ctx, `
		SELECT lower(email), id FROM user_account
		WHERE org_id = $1 AND email IS NOT NULL`, orgID)
	if err != nil {
		return err
	}
	for rows.Next() {
		var email string
		var id int64
		if err := rows.Scan(&email, &id); err != nil {
			rows.Close()
			return err
		}
		emailToID[email] = id
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	liveNames := map[string]bool{}
	rows, err = tx.Query(ctx, `
		SELECT lower(name) FROM channel
		WHERE org_id = $1 AND archived_at IS NULL`, orgID)
	if err != nil {
		return err
	}
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			rows.Close()
			return err
		}
		liveNames[n] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	// --- Users (ADR-001 D4: unmatched authors become claimable deactivated
	// placeholders; existing emails are matched, not duplicated).
	userMap := map[int64]int64{}  // zulip user id → our id
	nameMap := map[string]int64{} // full name → our id (mention re-resolution)
	for _, u := range ex.Users {
		if u.IsBot {
			rep.BotsSkipped++
			continue
		}
		if existing, ok := emailToID[strings.ToLower(u.BestEmail())]; ok {
			// Email matches an existing account → map, never duplicate (D4).
			userMap[u.ID] = existing
			nameMap[u.FullName] = existing
			continue
		}
		var id int64
		err := tx.QueryRow(ctx, `
			INSERT INTO user_account
				(org_id, kind, email, full_name, role, created_at,
				 deactivated_at, origin_system, origin_id)
			VALUES ($1, $2, $3, $4, 40, $5,
			        CASE WHEN $6 THEN NULL ELSE now() END, $7, $8)
			ON CONFLICT (org_id, origin_system, origin_id) WHERE origin_system IS NOT NULL
			DO NOTHING
			RETURNING id`,
			orgID, enum.UserImportedPlaceholder, u.BestEmail(), u.FullName,
			ts(u.DateJoined), u.IsActive, originZulip, fmt.Sprint(u.ID)).Scan(&id)
		if err == pgx.ErrNoRows { // re-run: resolve by origin
			if err := tx.QueryRow(ctx, `
				SELECT id FROM user_account
				WHERE org_id = $1 AND origin_system = $2 AND origin_id = $3`,
				orgID, originZulip, fmt.Sprint(u.ID)).Scan(&id); err != nil {
				return fmt.Errorf("resolve imported user %d: %w", u.ID, err)
			}
			rep.AlreadyImported++
		} else if err != nil {
			return fmt.Errorf("import user %d: %w", u.ID, err)
		} else {
			rep.Users++
			emailToID[strings.ToLower(u.BestEmail())] = id
		}
		userMap[u.ID] = id
		nameMap[u.FullName] = id
	}

	// --- Streams → channels (+ root thread). Name collisions with existing
	// channels are renamed visibly (never silently merged).
	channelMap := map[int64]int64{} // zulip stream id → our channel id
	for _, st := range ex.Streams {
		visibility := 1
		if st.InviteOnly {
			visibility = 2
		}
		// Live-name collisions resolved in Go, visibly (never silent merges).
		name := st.Name
		for i := 0; !st.Deactivated && liveNames[strings.ToLower(name)]; i++ {
			name = fmt.Sprintf("%s-%s%d", st.Name, originZulip, i+1)
		}
		if name != st.Name {
			rep.RenamedChannels[st.Name] = name
		}
		var id int64
		reRun := false
		err := tx.QueryRow(ctx, `
			INSERT INTO channel (org_id, name, visibility, description,
				created_at, archived_at, origin_system, origin_id)
			VALUES ($1, $2, $3, $4, $5,
			        CASE WHEN $6 THEN now() ELSE NULL END, $7, $8)
			ON CONFLICT (org_id, origin_system, origin_id) WHERE origin_system IS NOT NULL
			DO NOTHING
			RETURNING id`,
			orgID, name, visibility, st.Description, ts(st.DateCreated),
			st.Deactivated, originZulip, fmt.Sprint(st.ID)).Scan(&id)
		if err == pgx.ErrNoRows { // re-run
			if err := tx.QueryRow(ctx, `
				SELECT id FROM channel WHERE org_id = $1
				 AND origin_system = $2 AND origin_id = $3`,
				orgID, originZulip, fmt.Sprint(st.ID)).Scan(&id); err != nil {
				return fmt.Errorf("resolve imported channel %d: %w", st.ID, err)
			}
			rep.AlreadyImported++
			reRun = true
		} else if err != nil {
			return fmt.Errorf("import stream %q: %w", st.Name, err)
		}
		if reRun {
			channelMap[st.ID] = id
			continue
		}
		liveNames[strings.ToLower(name)] = true
		var rootID int64
		if err := tx.QueryRow(ctx,
			`INSERT INTO thread (org_id, channel_id, kind) VALUES ($1, $2, 2) RETURNING id`,
			orgID, id).Scan(&rootID); err != nil {
			return fmt.Errorf("root thread for %q: %w", name, err)
		}
		if _, err := tx.Exec(ctx,
			`UPDATE channel SET root_thread_id = $1 WHERE id = $2`, rootID, id); err != nil {
			return err
		}
		if _, err := eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: orgID, ActorKind: enum.ActorImporter,
			EntityType: enum.EntityChannel, EntityID: id, Verb: "channel.created",
			OccurredAt: ts(st.DateCreated),
			Payload:    eventlog.MustPayload(map[string]any{"channel_id": id, "name": name}),
		}); err != nil {
			return err
		}
		rep.Channels++
		channelMap[st.ID] = id
	}

	// --- Subscriptions → channel_member.
	for _, sub := range ex.Subscriptions {
		streamID, ok := ex.StreamByRecipient[sub.Recipient]
		if !ok || !sub.Active {
			continue
		}
		chID, ok1 := channelMap[streamID]
		uID, ok2 := userMap[sub.UserProfile]
		if !ok1 || !ok2 {
			continue
		}
		ct, err := tx.Exec(ctx, `
			INSERT INTO channel_member (channel_id, user_id)
			VALUES ($1, $2) ON CONFLICT DO NOTHING`, chID, uID)
		if err != nil {
			return fmt.Errorf("subscription: %w", err)
		}
		if ct.RowsAffected() > 0 {
			rep.Subscriptions++
		}
	}

	// --- Messages: topic → titled Thread (the showcase mapping); content
	// re-rendered through OUR engine with mentions re-resolved against the
	// imported directory; created_at backdated (E3).
	resolve := func(label string) (int64, bool) {
		id, ok := nameMap[label]
		return id, ok
	}
	threadMap := map[string]int64{} // "streamID\x00subject" → thread id
	messageMap := map[int64]int64{} // zulip message id → our id
	for _, m := range ex.Messages {
		streamID, ok := ex.StreamByRecipient[m.Recipient]
		if !ok {
			rep.DMMessagesSkipped++
			continue
		}
		chID, ok1 := channelMap[streamID]
		authorID, ok2 := userMap[m.Sender]
		if !ok1 || !ok2 {
			rep.DMMessagesSkipped++
			continue
		}
		tkey := fmt.Sprintf("%d\x00%s", streamID, m.Subject)
		thID, ok := threadMap[tkey]
		if !ok {
			originID := fmt.Sprintf("topic:%d:%s", streamID, m.Subject)
			err := tx.QueryRow(ctx, `
				INSERT INTO thread (org_id, channel_id, kind, title,
					last_activity_at, origin_system, origin_id)
				VALUES ($1, $2, 1, $3, $4, $5, $6)
				ON CONFLICT (org_id, origin_system, origin_id) WHERE origin_system IS NOT NULL
				DO NOTHING
				RETURNING id`,
				orgID, chID, m.Subject, ts(m.DateSent), originZulip, originID).Scan(&thID)
			if err == pgx.ErrNoRows {
				if err := tx.QueryRow(ctx, `
					SELECT id FROM thread WHERE org_id = $1
					 AND origin_system = $2 AND origin_id = $3`,
					orgID, originZulip, originID).Scan(&thID); err != nil {
					return fmt.Errorf("resolve topic thread: %w", err)
				}
				rep.AlreadyImported++
			} else if err != nil {
				return fmt.Errorf("topic thread %q: %w", m.Subject, err)
			} else {
				rep.Threads++
				if _, err := eventlog.Append(ctx, tx, eventlog.Event{
					OrgID: orgID, ActorKind: enum.ActorImporter,
					EntityType: enum.EntityThread, EntityID: thID, Verb: "thread.created",
					OccurredAt: ts(m.DateSent),
					Payload: eventlog.MustPayload(map[string]any{
						"thread_id": thID, "channel_id": chID, "title": m.Subject}),
				}); err != nil {
					return err
				}
			}
			threadMap[tkey] = thID
		}

		if m.EditHistory != nil && *m.EditHistory != "" && *m.EditHistory != "null" {
			rep.EditHistoryDropped++
		}
		doc := content.Parse(m.Content, resolve)
		var msgID int64
		err := tx.QueryRow(ctx, `
			INSERT INTO message (org_id, thread_id, channel_id, author_id,
				source, ast, rendered, render_version, has_link, created_at,
				origin_system, origin_id)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
			ON CONFLICT (org_id, origin_system, origin_id) WHERE origin_system IS NOT NULL
			DO NOTHING
			RETURNING id`,
			orgID, thID, chID, authorID, m.Content, doc.JSON(),
			content.RenderHTML(doc), content.RenderVersion, doc.HasLink(),
			ts(m.DateSent), originZulip, fmt.Sprint(m.ID)).Scan(&msgID)
		if err == pgx.ErrNoRows {
			rep.AlreadyImported++
			if err := tx.QueryRow(ctx, `
				SELECT id FROM message WHERE org_id = $1
				 AND origin_system = $2 AND origin_id = $3`,
				orgID, originZulip, fmt.Sprint(m.ID)).Scan(&msgID); err != nil {
				return fmt.Errorf("resolve imported message: %w", err)
			}
			messageMap[m.ID] = msgID
			continue
		}
		if err != nil {
			return fmt.Errorf("message %d: %w", m.ID, err)
		}
		messageMap[m.ID] = msgID
		if _, err := tx.Exec(ctx, `
			UPDATE thread SET message_count = message_count + 1,
			       last_activity_at = GREATEST(last_activity_at, $2),
			       root_message_id = COALESCE(root_message_id, $3)
			WHERE id = $1`, thID, ts(m.DateSent), msgID); err != nil {
			return err
		}
		if _, err := eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: orgID, ActorKind: enum.ActorImporter,
			EntityType: enum.EntityMessage, EntityID: msgID, Verb: "message.created",
			OccurredAt: ts(m.DateSent),
			Payload: eventlog.MustPayload(map[string]any{
				"message_id": msgID, "channel_id": chID, "thread_id": thID,
				"mentions": doc.Mentions()}),
		}); err != nil {
			return err
		}
		rep.Messages++
	}

	// --- Reactions.
	for _, r := range ex.Reactions {
		msgID, ok1 := messageMap[r.Message]
		uID, ok2 := userMap[r.UserProfile]
		if !ok1 || !ok2 {
			rep.ReactionsUnmapped++
			continue
		}
		ct, err := tx.Exec(ctx, `
			INSERT INTO reaction (message_id, user_id, emoji, kind)
			VALUES ($1, $2, $3, 1) ON CONFLICT DO NOTHING`,
			msgID, uID, r.EmojiName)
		if err != nil {
			return fmt.Errorf("reaction: %w", err)
		}
		if ct.RowsAffected() > 0 {
			rep.Reactions++
		}
	}
	return nil
}
