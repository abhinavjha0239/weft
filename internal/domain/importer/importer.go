package importer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/domain/content"
	"github.com/abhinavjha0239/weft/internal/domain/files"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/internal/eventlog"
	"github.com/abhinavjha0239/weft/internal/platform/blob"
)

type Service struct {
	pool  *pgxpool.Pool
	store blob.Store
}

// New takes the blob seam for the attachments lane — the importer derives
// keys through files.StorageKey so backfilled blobs dedup with live uploads.
func New(pool *pgxpool.Pool, store blob.Store) *Service {
	return &Service{pool: pool, store: store}
}

// Report is the fidelity contract (ADR-001: "nobody trusts a migrator that
// hides its losses"). Every source entity lands in exactly one bucket.
type Report struct {
	// Source names the loader that produced these numbers. A deployment with
	// two adapters cannot otherwise tell which one a stored report came from,
	// and every bucket below means something slightly different per source.
	Source string `json:"source"`
	DryRun bool   `json:"dry_run"`

	// Imported (lossless or transformed).
	Users, Channels, Threads, Messages, Reactions, Subscriptions int            `json:"-"`
	Groups, GroupMembers, GroupEdges, Watermarks                 int            `json:"-"`
	DMConversations, DMMessages, Attachments, MessageEdits       int            `json:"-"`
	ImportedCounts                                               map[string]int `json:"imported"`

	// Skipped-with-reason (documented losses of this importer version).
	BotsSkipped int `json:"bots_skipped"`
	// A DM conversation imports only when EVERY participant maps to a human
	// account: dropping a bot participant would shrink the canonical key
	// and wrongly merge the history into a different conversation.
	DMMessagesSkipped int `json:"dm_messages_skipped_unmappable_participants"`
	// A channel message whose channel or whose author did not map. "Stream"
	// was Zulip's word for it and does not belong in a source-neutral report.
	ChannelMessagesSkipped int `json:"channel_messages_skipped_unmapped"`
	// Edit-history entries without a mappable editor (bots, or the null
	// user_id of pre-2017 Zulip history) are skipped — attribution is
	// never invented. Topic/stream-move entries carry no prev_content and
	// are not message revisions.
	EditEntriesSkipped int `json:"edit_entries_skipped_unattributable"`
	// Attachment rows whose bytes are absent from the export's uploads/
	// tree (truncated exports).
	AttachmentFilesMissing int `json:"attachment_files_missing"`
	ReactionsUnmapped      int `json:"reactions_unmapped"`
	// Email-matched EXISTING accounts keep their current role — an import
	// never elevates or demotes a live user. Counted when the source role
	// was anything above plain member.
	RoleGrantsSkipped int `json:"role_grants_skipped_existing_users"`
	// A source user MAPPED onto an account that already existed, rather than
	// creating one. It is not a loss, but it is not a plain import either —
	// the fidelity contract is that every source entity lands in exactly one
	// bucket, and before this it landed in none, so a merge was invisible.
	MatchedExistingByEmail int `json:"matched_existing_by_email"`
	// The F-7 watermark marks everything up to the highest READ message as
	// read; sparse unread gaps BELOW that point are coarsened away. Counted,
	// never silent.
	ReadCoarsened int `json:"unread_below_watermark_coarsened"`
	// Losses only a LOADER can see, folded in by the planner so both modes
	// report them identically (ir.go's Losses documents each one). They live
	// here rather than in the loader's own output because the fidelity
	// contract is one report, and an entity dropped before the IR would
	// otherwise land in no bucket at all.
	AttachmentBytesExpired int `json:"attachment_bytes_expired"`
	BroadcastsFlattened    int `json:"broadcast_replies_flattened"`
	SystemNoticesDropped   int `json:"system_notices_dropped"`

	// Idempotency: rows already present from a previous run.
	AlreadyImported int `json:"already_imported"`

	// Transformations applied (visible, not silent).
	RenamedChannels map[string]string `json:"renamed_channels,omitempty"`
	RenamedGroups   map[string]string `json:"renamed_groups,omitempty"`
	// Zulip system role groups map onto the seeded Weft ones (never
	// duplicated); role:fullmembers coarsens to role:members.
	SystemGroupsMapped int `json:"system_groups_mapped"`
}

func (r *Report) finalize() {
	r.ImportedCounts = map[string]int{
		"users": r.Users, "channels": r.Channels, "threads": r.Threads,
		"messages": r.Messages, "reactions": r.Reactions,
		"subscriptions": r.Subscriptions, "groups": r.Groups,
		"group_members": r.GroupMembers, "group_edges": r.GroupEdges,
		"read_watermarks":  r.Watermarks,
		"dm_conversations": r.DMConversations, "dm_messages": r.DMMessages,
		"attachments": r.Attachments, "message_edits": r.MessageEdits,
	}
}

// roleGroup names the seeded Weft group a role preset belongs to.
func roleGroup(role int16) string {
	switch role {
	case 10:
		return "role:owners"
	case 20:
		return "role:admins"
	case 30:
		return "role:moderators"
	case 50:
		return "" // guests hold no role group in the seeded set
	default:
		return "role:members"
	}
}

// Run imports an unpacked Zulip export into an existing org. Idempotent:
// every entity upserts by (org, origin_system, origin_id); re-runs count
// AlreadyImported instead of duplicating (ADR-001 D5).
//
// dryRun answers the same question WITHOUT writing: it loads the same
// resolution context, builds the same plan, and returns the plan's report.
// Both modes therefore produce one number per bucket from one implementation,
// which is what makes "the dry run tells you what this import will do" a
// checkable claim instead of a hopeful one — and it is only true because the
// plan is taken against the REAL org, so a re-run's dry pass says
// "already_imported" exactly where the write would.
//
// One transaction for atomicity (an import is all-or-nothing); the dry run
// uses one too, for a consistent snapshot of an org other people are using.
// Chunked streaming per messages file is the scale-tier follow-up for
// multi-GB exports and keeps this exact call shape.
func (s *Service) Run(ctx context.Context, orgID int64, dir string, dryRun bool) (Report, error) {
	ex, err := LoadZulipExport(dir)
	if err != nil {
		return Report{}, err
	}
	ir := ex.toImport()

	if dryRun {
		var rep Report
		err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
			rc, err := loadResolution(ctx, tx, orgID, ir.Source)
			if err != nil {
				return err
			}
			pl, err := newPlan(ir, rc)
			if err != nil {
				return err
			}
			rep = pl.rep
			return nil
		})
		if err != nil {
			return Report{}, err
		}
		rep.DryRun = true
		return rep, nil
	}

	rep := Report{Source: ir.Source, DryRun: false,
		RenamedChannels: map[string]string{}, RenamedGroups: map[string]string{}}
	if err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		return s.write(ctx, tx, orgID, ir, &rep)
	}); err != nil {
		return Report{}, err
	}
	return rep, nil
}

// write drains a source-neutral Import into the org. It never learns which
// loader produced the IR: the provenance token, the container binding, the
// message order and the upload dialect all arrive as data.
//
// It does not do its own accounting either. `newPlan` decides every bucket
// BEFORE the first row is written, against the same resolution context the dry
// run reads, and this function's job is to land exactly what the plan
// promised — which is what makes "the dry run tells you what this import will
// do" one implementation instead of two that drift.
func (s *Service) write(ctx context.Context, tx pgx.Tx, orgID int64, ir *Import, rep *Report) error {
	// The target org's current shape, read ONCE inside this transaction, so
	// the plan and the writes that execute it see one snapshot. Collision
	// handling happens in Go because a unique-violation ERROR aborts the whole
	// transaction — the only ON CONFLICT used is the origin index (idempotent
	// re-runs).
	rc, err := loadResolution(ctx, tx, orgID, ir.Source)
	if err != nil {
		return err
	}
	pl, err := newPlan(ir, rc)
	if err != nil {
		return err
	}
	dryRun := rep.DryRun
	*rep = pl.rep
	rep.DryRun = dryRun
	emailToID, groupNameToID := rc.emailToID, rc.groupNameToID

	// --- Users (ADR-001 D4: unmatched authors become claimable deactivated
	// placeholders; existing emails are matched, not duplicated). Source
	// roles carry over onto CREATED accounts only — an import never changes
	// an existing user's role.
	userMap := map[string]int64{} // source user id → our id
	nameMap := map[string]int64{} // display name → our id (mention re-resolution)
	for _, u := range ir.Users {
		if u.Bot {
			continue
		}
		// The match key is the source email, lowercased — and an ABSENT email
		// is NOT a key. Without this guard the first emailless user inserts
		// email = '' and caches itself under "", so every LATER emailless user
		// matches it: messages re-attributed, DM canonical keys merged, and no
		// bucket incremented. The preload above selects `email IS NOT NULL`,
		// but '' satisfies that, so the alias survived re-runs too. Zulip
		// always supplies emails, which is why this stayed latent; Slack does
		// not (its export's user records routinely lack one).
		key := strings.ToLower(u.Email)
		if existing, ok := emailToID[key]; ok && key != "" {
			// Email matches an existing account → map, never duplicate (D4).
			userMap[u.SourceID] = existing
			nameMap[u.DisplayName] = existing
			continue
		}
		// SQL NULL, never '': user_account_email_key is partial on
		// `email IS NOT NULL`, so '' occupies a real unique slot (a second
		// emailless user would collide) while NULL does not.
		var email *string
		if key != "" {
			e := u.Email
			email = &e
		}
		var id int64
		err := tx.QueryRow(ctx, `
			INSERT INTO user_account
				(org_id, kind, email, full_name, role, created_at,
				 deactivated_at, origin_system, origin_id)
			VALUES ($1, $2, $3, $4, $5, $6,
			        CASE WHEN $7 THEN NULL ELSE now() END, $8, $9)
			ON CONFLICT (org_id, origin_system, origin_id) WHERE origin_system IS NOT NULL
			DO NOTHING
			RETURNING id`,
			orgID, enum.UserImportedPlaceholder, email, u.DisplayName, u.Role,
			u.JoinedAt, u.Active, ir.Source, u.SourceID).Scan(&id)
		if err == pgx.ErrNoRows { // re-run: resolve by origin
			if err := tx.QueryRow(ctx, `
				SELECT id FROM user_account
				WHERE org_id = $1 AND origin_system = $2 AND origin_id = $3`,
				orgID, ir.Source, u.SourceID).Scan(&id); err != nil {
				return fmt.Errorf("resolve imported user %s: %w", u.SourceID, err)
			}
		} else if err != nil {
			return fmt.Errorf("import user %s: %w", u.SourceID, err)
		} else if key != "" {
			emailToID[key] = id
		}
		userMap[u.SourceID] = id
		nameMap[u.DisplayName] = id
		// Role-group membership so permissions resolve when the placeholder
		// is claimed (idempotent on re-runs).
		if gname := roleGroup(u.Role); gname != "" {
			if gid, ok := groupNameToID[gname]; ok {
				if _, err := tx.Exec(ctx, `
					INSERT INTO user_group_member (group_id, user_id)
					VALUES ($1, $2) ON CONFLICT DO NOTHING`, gid, id); err != nil {
					return fmt.Errorf("role group for user %s: %w", u.SourceID, err)
				}
			}
		}
	}

	// --- Streams → channels (+ root thread). Name collisions with existing
	// channels are renamed visibly (never silently merged).
	channelMap := map[string]int64{}  // source channel id → our channel id
	channelRoot := map[string]int64{} // source channel id → its kind=2 root thread
	for _, ch := range ir.Channels {
		// Live-name collisions were resolved by the plan, in Go and visibly
		// (never silent merges). The name is NOT re-derived here: the walk
		// depends on names claimed earlier in the same run, so a second walk
		// over a half-written org would answer differently and the report's
		// renamed_channels map would stop describing the rows.
		name := pl.channelName[ch.SourceID]
		if name == "" {
			name = ch.Name
		}
		var id, rootID int64
		reRun := false
		err := tx.QueryRow(ctx, `
			INSERT INTO channel (org_id, name, visibility, description,
				created_at, archived_at, origin_system, origin_id)
			VALUES ($1, $2, $3, $4, $5,
			        CASE WHEN $6 THEN now() ELSE NULL END, $7, $8)
			ON CONFLICT (org_id, origin_system, origin_id) WHERE origin_system IS NOT NULL
			DO NOTHING
			RETURNING id`,
			orgID, name, ch.Visibility, ch.Description, ch.CreatedAt,
			ch.Archived, ir.Source, ch.SourceID).Scan(&id)
		if err == pgx.ErrNoRows { // re-run
			// The root thread id comes back too: a loader whose flat-feed
			// messages land there needs it on every run, not only the first.
			if err := tx.QueryRow(ctx, `
				SELECT id, COALESCE(root_thread_id, 0) FROM channel WHERE org_id = $1
				 AND origin_system = $2 AND origin_id = $3`,
				orgID, ir.Source, ch.SourceID).Scan(&id, &rootID); err != nil {
				return fmt.Errorf("resolve imported channel %s: %w", ch.SourceID, err)
			}
			reRun = true
		} else if err != nil {
			return fmt.Errorf("import channel %q: %w", ch.Name, err)
		}
		if reRun {
			channelMap[ch.SourceID] = id
			channelRoot[ch.SourceID] = rootID
			continue
		}
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
			OccurredAt: ch.CreatedAt,
			Payload:    eventlog.MustPayload(map[string]any{"channel_id": id, "name": name}),
		}); err != nil {
			return err
		}
		channelMap[ch.SourceID] = id
		channelRoot[ch.SourceID] = rootID
	}

	// --- Memberships → channel_member. The loader already decided what
	// counts as membership in its source; here it is only a pair of ids.
	for _, mem := range ir.Memberships {
		chID, ok1 := channelMap[mem.ChannelID]
		uID, ok2 := userMap[mem.UserID]
		if !ok1 || !ok2 {
			continue
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO channel_member (channel_id, user_id)
			VALUES ($1, $2) ON CONFLICT DO NOTHING`, chID, uID); err != nil {
			return fmt.Errorf("membership: %w", err)
		}
	}

	// --- Named user groups. System role groups map onto the seeded Weft
	// ones (never duplicated); custom groups import with provenance and
	// visible rename on live-name collisions. Memberships of system groups
	// are NOT copied — role fidelity flows through user_account.role above.
	systemGroup := map[string]bool{} // source group id → is a system group
	groupMap := map[string]int64{}   // source group id → weft group id
	for _, g := range ir.Groups {
		if g.System {
			systemGroup[g.SourceID] = true
			if g.SystemName != "" {
				if wid, ok := groupNameToID[g.SystemName]; ok {
					groupMap[g.SourceID] = wid
				}
			}
			continue
		}
		// The plan resolved the collision (UNIQUE (org_id, name) on user_group
		// is unconditional, unlike channels, so even deactivated groups rename).
		name := pl.groupName[g.SourceID]
		if name == "" {
			name = g.Name
		}
		var id int64
		err := tx.QueryRow(ctx, `
			INSERT INTO user_group (org_id, name, description, is_system,
				created_at, deactivated_at, origin_system, origin_id)
			VALUES ($1, $2, $3, false, COALESCE($4, now()),
			        CASE WHEN $5 THEN now() ELSE NULL END, $6, $7)
			ON CONFLICT (org_id, origin_system, origin_id) WHERE origin_system IS NOT NULL
			DO NOTHING
			RETURNING id`,
			orgID, name, g.Description, nullableTime(g.CreatedAt), g.Deactivated,
			ir.Source, g.SourceID).Scan(&id)
		if err == pgx.ErrNoRows { // re-run
			if err := tx.QueryRow(ctx, `
				SELECT id FROM user_group WHERE org_id = $1
				 AND origin_system = $2 AND origin_id = $3`,
				orgID, ir.Source, g.SourceID).Scan(&id); err != nil {
				return fmt.Errorf("resolve imported group %q: %w", g.Name, err)
			}
			groupMap[g.SourceID] = id
			continue
		} else if err != nil {
			return fmt.Errorf("import group %q: %w", g.Name, err)
		}
		groupNameToID[strings.ToLower(name)] = id
		groupMap[g.SourceID] = id
		if _, err := eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: orgID, ActorKind: enum.ActorImporter,
			EntityType: enum.EntityGroup, EntityID: id, Verb: "usergroup.created",
			OccurredAt: g.CreatedAt,
			Payload:    eventlog.MustPayload(map[string]any{"group_id": id, "name": name}),
		}); err != nil {
			return err
		}
	}
	for _, m := range ir.GroupMembers {
		if systemGroup[m.GroupID] {
			continue // roles carried via user_account.role, never via copy
		}
		gid, ok1 := groupMap[m.GroupID]
		uid, ok2 := userMap[m.UserID]
		if !ok1 || !ok2 {
			continue
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO user_group_member (group_id, user_id)
			VALUES ($1, $2) ON CONFLICT DO NOTHING`, gid, uid); err != nil {
			return fmt.Errorf("group member: %w", err)
		}
	}
	for _, e := range ir.GroupEdges {
		super, ok1 := groupMap[e.GroupID]
		sub, ok2 := groupMap[e.SubgroupID]
		// Equal ids arise when two source groups coarsen onto one Weft group
		// (role:fullmembers + role:members); a self-edge is meaningless.
		if !ok1 || !ok2 || super == sub {
			continue
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO user_group_subgroup (group_id, subgroup_id)
			VALUES ($1, $2) ON CONFLICT DO NOTHING`, super, sub); err != nil {
			return fmt.Errorf("group edge: %w", err)
		}
	}

	// --- Messages: topic → titled Thread (the showcase mapping); content
	// re-rendered through OUR engine with mentions re-resolved against the
	// imported directory; created_at backdated (E3).
	threadMap := map[string]int64{}  // Thread.Key → thread id
	messageMap := map[string]int64{} // source message id → our id
	msgThread := map[string]int64{}  // source message id → our thread id
	fileIDs, err := s.importAttachments(ctx, tx, orgID, ir, userMap, rep)
	if err != nil {
		return err
	}

	// History order was established by the plan (newPlan sorts by Ordinal
	// before it decides anything, because half the plan depends on the order:
	// which message roots a thread, which one a watermark lands on).

	dmCache := map[string]dmInfo{}     // canonical weft key → conversation
	convByKey := map[string][]string{} // Conversation.SourceKey → participants
	for _, c := range ir.Conversations {
		convByKey[c.SourceKey] = c.MemberIDs
	}
	for i := range ir.Messages {
		m := &ir.Messages[i]
		if m.Container.Kind != ContainerChannel {
			if err := s.importDirectMessage(ctx, tx, orgID, ir, m, convByKey,
				userMap, nameMap, channelMap, dmCache, fileIDs, messageMap, msgThread); err != nil {
				return err
			}
			continue
		}
		chID, ok1 := channelMap[m.Container.Key]
		authorID, ok2 := userMap[m.AuthorID]
		if !ok1 || !ok2 {
			continue
		}
		if m.Thread == nil {
			// Unreachable: the plan refuses this IR before any row is written.
			// A loader that MEANS the flat feed says so with Thread.Root; nil
			// means it forgot, and this is the belt that stops the lane from
			// writing a message with no thread id.
			return fmt.Errorf("import message %s: channel message without a thread", m.SourceID)
		}
		var thID int64
		if m.Thread.Root {
			// The flat feed: the channel's own kind=2 root, which the channel
			// lane above already created (or resolved on a re-run). Nothing is
			// inserted and no thread.created event fires — roots are silent,
			// exactly as they are for a natively created channel. The F-15
			// bump below is gated on kind = 1 and no-ops here.
			thID = channelRoot[m.Container.Key]
			if thID == 0 {
				return fmt.Errorf("import message %s: channel %s has no root thread",
					m.SourceID, m.Container.Key)
			}
			threadMap[m.Thread.Key] = thID
		} else if id, ok := threadMap[m.Thread.Key]; ok {
			thID = id
		} else {
			err := tx.QueryRow(ctx, `
				INSERT INTO thread (org_id, channel_id, kind, title,
					last_activity_at, origin_system, origin_id)
				VALUES ($1, $2, 1, $3, $4, $5, $6)
				ON CONFLICT (org_id, origin_system, origin_id) WHERE origin_system IS NOT NULL
				DO NOTHING
				RETURNING id`,
				orgID, chID, m.Thread.Title, m.SentAt, ir.Source, m.Thread.SourceID).Scan(&thID)
			if err == pgx.ErrNoRows {
				if err := tx.QueryRow(ctx, `
					SELECT id FROM thread WHERE org_id = $1
					 AND origin_system = $2 AND origin_id = $3`,
					orgID, ir.Source, m.Thread.SourceID).Scan(&thID); err != nil {
					return fmt.Errorf("resolve imported thread: %w", err)
				}
			} else if err != nil {
				return fmt.Errorf("thread %q: %w", m.Thread.Title, err)
			} else {
				if _, err := eventlog.Append(ctx, tx, eventlog.Event{
					OrgID: orgID, ActorKind: enum.ActorImporter,
					EntityType: enum.EntityThread, EntityID: thID, Verb: "thread.created",
					OccurredAt: m.SentAt,
					Payload: eventlog.MustPayload(map[string]any{
						"thread_id": thID, "channel_id": chID, "title": m.Thread.Title}),
				}); err != nil {
					return err
				}
			}
			threadMap[m.Thread.Key] = thID
		}

		src, hasAttach := ir.rewriteBody(m.Body, fileIDs)
		doc := content.Parse(src, mentionResolver(m, nameMap, userMap),
			content.WithChannelRefs(channelRefResolver(m, channelMap)))
		var msgID int64
		err := tx.QueryRow(ctx, `
			INSERT INTO message (org_id, thread_id, channel_id, author_id,
				source, ast, rendered, render_version, has_link, has_attachment,
				created_at, origin_system, origin_id)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
			ON CONFLICT (org_id, origin_system, origin_id) WHERE origin_system IS NOT NULL
			DO NOTHING
			RETURNING id`,
			orgID, thID, chID, authorID, src, doc.JSON(),
			content.RenderHTML(doc), content.RenderVersion, doc.HasLink(), hasAttach,
			m.SentAt, ir.Source, m.SourceID).Scan(&msgID)
		if err == pgx.ErrNoRows {
			if err := tx.QueryRow(ctx, `
				SELECT id FROM message WHERE org_id = $1
				 AND origin_system = $2 AND origin_id = $3`,
				orgID, ir.Source, m.SourceID).Scan(&msgID); err != nil {
				return fmt.Errorf("resolve imported message: %w", err)
			}
			messageMap[m.SourceID] = msgID
			msgThread[m.SourceID] = thID
			continue
		}
		if err != nil {
			return fmt.Errorf("message %s: %w", m.SourceID, err)
		}
		messageMap[m.SourceID] = msgID
		msgThread[m.SourceID] = thID
		// F-15: only kind=1 threads carry denormalized counters. The gate is
		// a no-op for the Zulip lane, which only ever reaches topic threads —
		// it is here so that merging the channel and DM lanes (or adding a
		// source whose "channel" messages land on a root) cannot quietly
		// corrupt a container. root_message_id is the dangerous one:
		// messaging/move.go rejects a move for ANY message some thread names
		// as its root and does not filter by kind, so a root_message_id set
		// on a kind=2 root makes that message permanently unmovable.
		if _, err := tx.Exec(ctx, `
			UPDATE thread SET message_count = message_count + 1,
			       last_activity_at = GREATEST(last_activity_at, $2),
			       root_message_id = COALESCE(root_message_id, $3)
			WHERE id = $1 AND kind = 1`, thID, m.SentAt, msgID); err != nil {
			return err
		}
		if _, err := eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: orgID, ActorKind: enum.ActorImporter,
			EntityType: enum.EntityMessage, EntityID: msgID, Verb: "message.created",
			OccurredAt: m.SentAt,
			Payload: eventlog.MustPayload(map[string]any{
				"message_id": msgID, "channel_id": chID, "thread_id": thID,
				"mentions": doc.Mentions()}),
		}); err != nil {
			return err
		}
		if err := s.importEditHistory(ctx, tx, msgID, m,
			userMap, mentionResolver(m, nameMap, userMap),
			content.WithChannelRefs(channelRefResolver(m, channelMap))); err != nil {
			return err
		}
	}

	// --- Reactions.
	for _, r := range ir.Reactions {
		msgID, ok1 := messageMap[r.MessageID]
		uID, ok2 := userMap[r.UserID]
		if !ok1 || !ok2 {
			continue
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO reaction (message_id, user_id, emoji, kind)
			VALUES ($1, $2, $3, 1) ON CONFLICT DO NOTHING`,
			msgID, uID, r.Emoji); err != nil {
			return fmt.Errorf("reaction: %w", err)
		}
	}

	// --- Attachment references (the export's m2m is authoritative): each
	// mapped (attachment, message) pair becomes a file_reference, and the
	// message is flagged even when its content carried no inline link.
	for _, ref := range ir.AttachmentRefs {
		fid, ok1 := fileIDs[ref.AttachmentID]
		mid, ok2 := messageMap[ref.MessageID]
		if !ok1 || !ok2 {
			continue
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO file_reference (file_id, entity_type, entity_id)
			VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`,
			fid, int16(enum.EntityMessage), mid); err != nil {
			return fmt.Errorf("attachment reference: %w", err)
		}
		if _, err := tx.Exec(ctx,
			`UPDATE message SET has_attachment = true WHERE id = $1`, mid); err != nil {
			return err
		}
	}

	// --- Read watermarks from the source's read flags (F-7). The REDUCTION —
	// which message each (user, thread) watermark lands on, and which unread
	// messages below it are coarsened away — is the plan's; it has to be, or
	// the dry run could not tell an operator how much read state survives.
	// What is left here is resolving the two source ids the plan chose against
	// the ids this transaction actually minted. Deliberately not
	// event-logged — read state stays off the durable spine (scale contract),
	// exactly like live mark-read.
	for _, w := range pl.watermarks {
		uid, ok1 := userMap[w.userSourceID]
		mid, ok2 := messageMap[w.messageSourceID]
		if !ok1 || !ok2 {
			return fmt.Errorf("watermark for %s on %s: the plan chose a row this "+
				"import did not land", w.userSourceID, w.messageSourceID)
		}
		// Monotone like MarkRead: a watermark already at or above this message
		// is left alone, which the plan predicted, so a re-run is a true no-op
		// on both the rows and the count.
		if _, err := tx.Exec(ctx, `
			INSERT INTO thread_read_watermark (user_id, thread_id, last_read_message_id, updated_at)
			VALUES ($1, $2, $3, now())
			ON CONFLICT (user_id, thread_id) DO UPDATE
			SET last_read_message_id = EXCLUDED.last_read_message_id, updated_at = now()
			WHERE thread_read_watermark.last_read_message_id < EXCLUDED.last_read_message_id`,
			uid, msgThread[w.messageSourceID], mid); err != nil {
			return fmt.Errorf("watermark: %w", err)
		}
	}

	// Role grants and group writes above bypass the perms service, so a full
	// closure recompute is REQUIRED — but never in this transaction: at
	// import scale that is the S2 async case. The enqueue commits atomically
	// with the import's writes; the rebuild worker fills a new closure
	// version and flips the fence after we commit. Until that flip, imported
	// users resolve permissions through the pre-import closure — the
	// documented async gap the import CLI closes by draining the queue
	// before it exits (tests drive RunOnce the same way).
	if err := perms.New(s.pool).EnqueueRebuild(ctx, tx, orgID); err != nil {
		return fmt.Errorf("closure rebuild enqueue: %w", err)
	}
	// Imported messages ride importer-actor events the notification consumer
	// deliberately skips (backfills never notify), so the S6 O(1) unread
	// counters would read 0 against real imported unread state. Seed them
	// from the just-imported watermarks in the same tx — import fidelity
	// includes the unread badge.
	if err := messaging.SeedUnreadCounters(ctx, tx, orgID); err != nil {
		return fmt.Errorf("unread counter seed: %w", err)
	}
	return nil
}

func nullableTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

type dmInfo struct{ spaceID, threadID int64 }

// importDirectMessage lands one direct message: resolve the participant set,
// create-or-get the conversation by its canonical key (the SAME derivation
// the native dm module uses, so imported and native history share one
// dm_space), then the provenance-idempotent message write. A conversation
// with any unmappable participant (a bot) is skipped and counted — dropping
// the participant would shrink the key and wrongly merge the history into a
// different conversation.
func (s *Service) importDirectMessage(ctx context.Context, tx pgx.Tx, orgID int64,
	ir *Import, m *Message, convByKey map[string][]string, userMap map[string]int64,
	nameMap, channelMap map[string]int64, dmCache map[string]dmInfo, fileIDs map[string]int64,
	messageMap, msgThread map[string]int64) error {

	sourceIDs, ok := convByKey[m.Container.Key]
	if !ok {
		return nil // counted by the plan as an unmappable-participant loss
	}
	weftIDs := make([]int64, 0, len(sourceIDs))
	for _, sid := range sourceIDs {
		uid, mapped := userMap[sid]
		if !mapped {
			return nil
		}
		weftIDs = append(weftIDs, uid)
	}
	authorID, ok := userMap[m.AuthorID]
	if !ok {
		return nil
	}
	sort.Slice(weftIDs, func(i, j int) bool { return weftIDs[i] < weftIDs[j] })
	parts := make([]string, len(weftIDs))
	for i, id := range weftIDs {
		parts[i] = fmt.Sprint(id)
	}
	key := strings.Join(parts, ":")

	info, cached := dmCache[key]
	if !cached {
		var kind int16
		switch {
		case len(weftIDs) == 1:
			kind = 3
		case len(weftIDs) == 2:
			kind = 1
		default:
			kind = 2
		}
		err := tx.QueryRow(ctx, `
			INSERT INTO dm_space (org_id, kind, dm_key) VALUES ($1, $2, $3)
			ON CONFLICT (org_id, dm_key) DO NOTHING
			RETURNING id`, orgID, kind, key).Scan(&info.spaceID)
		if err == pgx.ErrNoRows { // native or previous-run conversation
			if err := tx.QueryRow(ctx,
				`SELECT id FROM dm_space WHERE org_id = $1 AND dm_key = $2`,
				orgID, key).Scan(&info.spaceID); err != nil {
				return fmt.Errorf("resolve dm space: %w", err)
			}
			if err := tx.QueryRow(ctx,
				`SELECT id FROM thread WHERE dm_space_id = $1 AND kind = 2`,
				info.spaceID).Scan(&info.threadID); err != nil {
				return fmt.Errorf("resolve dm thread: %w", err)
			}
		} else if err != nil {
			return fmt.Errorf("dm space: %w", err)
		} else {
			for _, uid := range weftIDs {
				if _, err := tx.Exec(ctx,
					`INSERT INTO dm_participant (dm_space_id, user_id) VALUES ($1, $2)`,
					info.spaceID, uid); err != nil {
					return fmt.Errorf("dm participant: %w", err)
				}
			}
			if err := tx.QueryRow(ctx, `
				INSERT INTO thread (org_id, dm_space_id, kind) VALUES ($1, $2, 2)
				RETURNING id`, orgID, info.spaceID).Scan(&info.threadID); err != nil {
				return fmt.Errorf("dm thread: %w", err)
			}
			if _, err := eventlog.Append(ctx, tx, eventlog.Event{
				OrgID: orgID, ActorKind: enum.ActorImporter,
				EntityType: enum.EntityDM, EntityID: info.spaceID, Verb: "dm.opened",
				OccurredAt: m.SentAt,
				Payload: eventlog.MustPayload(map[string]any{
					"dm_space_id": info.spaceID, "root_thread_id": info.threadID,
					"user_ids": weftIDs}),
			}); err != nil {
				return err
			}
		}
		dmCache[key] = info
	}

	src, hasAttach := ir.rewriteBody(m.Body, fileIDs)
	resolve := mentionResolver(m, nameMap, userMap)
	doc := content.Parse(src, resolve,
		content.WithChannelRefs(channelRefResolver(m, channelMap)))
	var msgID int64
	err := tx.QueryRow(ctx, `
		INSERT INTO message (org_id, thread_id, dm_space_id, author_id,
			source, ast, rendered, render_version, has_link, has_attachment,
			created_at, origin_system, origin_id)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
		ON CONFLICT (org_id, origin_system, origin_id) WHERE origin_system IS NOT NULL
		DO NOTHING
		RETURNING id`,
		orgID, info.threadID, info.spaceID, authorID, src, doc.JSON(),
		content.RenderHTML(doc), content.RenderVersion, doc.HasLink(), hasAttach,
		m.SentAt, ir.Source, m.SourceID).Scan(&msgID)
	if err == pgx.ErrNoRows {
		if err := tx.QueryRow(ctx, `
			SELECT id FROM message WHERE org_id = $1
			 AND origin_system = $2 AND origin_id = $3`,
			orgID, ir.Source, m.SourceID).Scan(&msgID); err != nil {
			return fmt.Errorf("resolve imported dm message: %w", err)
		}
		messageMap[m.SourceID] = msgID
		msgThread[m.SourceID] = info.threadID
		return nil
	}
	if err != nil {
		return fmt.Errorf("dm message %s: %w", m.SourceID, err)
	}
	messageMap[m.SourceID] = msgID
	msgThread[m.SourceID] = info.threadID
	if _, err := eventlog.Append(ctx, tx, eventlog.Event{
		OrgID: orgID, ActorKind: enum.ActorImporter,
		EntityType: enum.EntityMessage, EntityID: msgID, Verb: "message.created",
		OccurredAt: m.SentAt,
		Payload: eventlog.MustPayload(map[string]any{
			"message_id": msgID, "dm_space_id": info.spaceID,
			"thread_id": info.threadID, "mentions": doc.Mentions()}),
	}); err != nil {
		return err
	}
	if err := s.importEditHistory(ctx, tx, msgID, m, userMap, resolve,
		content.WithChannelRefs(channelRefResolver(m, channelMap))); err != nil {
		return err
	}
	return nil
}

// importAttachments stores every attachment blob (content-addressed through
// files.StorageKey, so backfilled bytes dedup with live uploads) and records
// provenance-idempotent file rows. Returns attachment SourceID→file id, which
// is both what the m2m references need and what the loader's link rewriter
// resolves against.
//
// The bytes arrive as an opener, so this lane never learns where they live.
// It reads each attachment ONCE and rewinds, because the storage key is the
// content hash and the hash has to exist before the Put.
func (s *Service) importAttachments(ctx context.Context, tx pgx.Tx, orgID int64, ir *Import, userMap map[string]int64, rep *Report) (map[string]int64, error) {
	fileIDs := map[string]int64{}
	for _, a := range ir.Attachments {
		// Bytes the source cannot produce are a counted loss, never a failed
		// import: truncated exports are ordinary. A loader that supplied no
		// opener at all is the same answer, not a panic.
		if a.Probe == nil || a.Open == nil {
			continue // the plan counted it missing off the same two nil checks
		}
		if _, err := a.Probe(); err != nil {
			continue
		}
		f, err := a.Open()
		if err != nil {
			// THE ONE CORRECTION THE PLAN CANNOT MAKE. Every other bucket is
			// decided before a byte moves, but "the probe said the bytes are
			// there and the open then failed" is only knowable by opening, and
			// opening every attachment is exactly the cost the Probe/Open pair
			// exists to avoid (for a remote source it is a download). So the
			// plan predicts off the probe and reality corrects it here, in the
			// single place where the prediction is provably not derivable
			// cheaply. A dry run can therefore be off by exactly this case,
			// and by nothing else.
			rep.Attachments--
			rep.AttachmentFilesMissing++
			continue
		}
		h := sha256.New()
		size, err := io.Copy(h, f)
		if err != nil {
			f.Close()
			return nil, fmt.Errorf("hash attachment %s: %w", a.SourceID, err)
		}
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			f.Close()
			return nil, err
		}
		sum := h.Sum(nil)
		key := files.StorageKey(orgID, hex.EncodeToString(sum))
		// Put is idempotent (content-addressed): a crash between blob write
		// and commit just re-puts on the retry.
		err = s.store.Put(ctx, key, f)
		f.Close()
		if err != nil {
			return nil, fmt.Errorf("store attachment %s: %w", a.SourceID, err)
		}
		mime := "application/octet-stream"
		if a.MIME != "" {
			mime = a.MIME
		}
		var owner *int64
		if uid, ok := userMap[a.OwnerID]; ok {
			owner = &uid
		}
		var id int64
		err = tx.QueryRow(ctx, `
			INSERT INTO file (org_id, kind, name, mime, size_bytes, sha256,
				storage_key, uploader_id, created_at, origin_system, origin_id)
			VALUES ($1, 1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
			ON CONFLICT (org_id, origin_system, origin_id) WHERE origin_system IS NOT NULL
			DO NOTHING
			RETURNING id`,
			orgID, a.Name, mime, size, sum, key, owner,
			a.CreatedAt, ir.Source, a.SourceID).Scan(&id)
		if err == pgx.ErrNoRows { // re-run
			if err := tx.QueryRow(ctx, `
				SELECT id FROM file WHERE org_id = $1
				 AND origin_system = $2 AND origin_id = $3`,
				orgID, ir.Source, a.SourceID).Scan(&id); err != nil {
				return nil, fmt.Errorf("resolve imported file %s: %w", a.SourceID, err)
			}
		} else if err != nil {
			return nil, fmt.Errorf("import attachment %s: %w", a.SourceID, err)
		} else {
			if _, err := eventlog.Append(ctx, tx, eventlog.Event{
				OrgID: orgID, ActorKind: enum.ActorImporter,
				EntityType: enum.EntityFile, EntityID: id, Verb: "file.uploaded",
				OccurredAt: a.CreatedAt,
				Payload: eventlog.MustPayload(map[string]any{
					"file_id": id, "name": a.Name, "size_bytes": size}),
			}); err != nil {
				return nil, err
			}
		}
		fileIDs[a.SourceID] = id
	}
	return fileIDs, nil
}

// importEditHistory materializes a message's content edits as kind-1
// revisions, oldest first, re-parsing prior content through our engine.
// Entries without an attributable editor are skipped and counted; entries
// that are not content revisions at all never reach the IR.
func (s *Service) importEditHistory(ctx context.Context, tx pgx.Tx, msgID int64, m *Message, userMap map[string]int64, resolve func(string) (int64, bool), chRefs content.Option) error {
	if len(m.Edits) == 0 {
		return nil
	}
	revNo := 0
	// edited_at is stamped from the latest LANDED edit, and only when its
	// timestamp is after the epoch — the same floor the shipped `lastEdit >
	// 0` guard applied to a Unix float, kept so a source timestamp of zero
	// still means "no usable edit time" rather than 1970.
	epoch := time.Unix(0, 0).UTC()
	lastEdit := epoch
	for _, e := range m.Edits {
		if e.EditorID == "" {
			continue // the plan counted it as unattributable
		}
		editor, ok := userMap[e.EditorID]
		if !ok {
			continue
		}
		revNo++
		prevDoc := content.Parse(e.PrevBody, resolve, chRefs)
		if _, err := tx.Exec(ctx, `
			INSERT INTO message_revision
				(message_id, revision_no, kind, prev_source, prev_ast, edited_by, edited_at)
			VALUES ($1, $2, 1, $3, $4, $5, $6)
			ON CONFLICT (message_id, revision_no) DO NOTHING`,
			msgID, revNo, e.PrevBody, prevDoc.JSON(), editor, e.At); err != nil {
			return fmt.Errorf("import revision: %w", err)
		}
		if e.At.After(lastEdit) {
			lastEdit = e.At
		}
	}
	if lastEdit.After(epoch) {
		if _, err := tx.Exec(ctx,
			`UPDATE message SET edited_at = $1 WHERE id = $2 AND edited_at IS NULL`,
			lastEdit, msgID); err != nil {
			return fmt.Errorf("stamp edited_at: %w", err)
		}
	}
	return nil
}
