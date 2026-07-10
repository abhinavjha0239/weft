package importer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/domain/content"
	"github.com/abhinavjha0239/weft/internal/domain/files"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/internal/eventlog"
	"github.com/abhinavjha0239/weft/internal/platform/blob"
)

const originZulip = "zulip"

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
	DryRun bool `json:"dry_run"`

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
	DMMessagesSkipped     int `json:"dm_messages_skipped_unmappable_participants"`
	StreamMessagesSkipped int `json:"stream_messages_skipped_unmapped"`
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
	// The F-7 watermark marks everything up to the highest READ message as
	// read; sparse unread gaps BELOW that point are coarsened away. Counted,
	// never silent.
	ReadCoarsened int `json:"unread_below_watermark_coarsened"`

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

// weftRole maps Zulip UserProfile.role constants to Weft role presets.
func weftRole(zulipRole int) int16 {
	switch zulipRole {
	case 100:
		return 10 // realm owner → owner
	case 200:
		return 20 // realm administrator → admin
	case 300:
		return 30 // moderator
	case 600:
		return 50 // guest
	default:
		return 40 // member (400 and anything unknown)
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

// zulipSystemGroup maps Zulip's system group names onto the seeded Weft
// ones. role:fullmembers coarsens to role:members (Weft has no waiting
// period); role:nobody and role:internet have no Weft counterpart.
func zulipSystemGroup(name string) string {
	switch name {
	case "role:owners":
		return "role:owners"
	case "role:administrators":
		return "role:admins"
	case "role:moderators":
		return "role:moderators"
	case "role:members", "role:fullmembers":
		return "role:members"
	case "role:everyone":
		return "role:everyone"
	default:
		return ""
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
	rep := Report{DryRun: dryRun,
		RenamedChannels: map[string]string{}, RenamedGroups: map[string]string{}}

	// Dry-run: pure accounting pass (no DB — collision renames and
	// existing-user role skips are only knowable at write time).
	if dryRun {
		bots := map[int64]bool{}
		for _, u := range ex.Users {
			if u.IsBot {
				rep.BotsSkipped++
				bots[u.ID] = true
			} else {
				rep.Users++
			}
		}
		rep.Channels = len(ex.Streams)
		humans := map[int64]bool{}
		for _, u := range ex.Users {
			if !u.IsBot {
				humans[u.ID] = true
			}
		}
		// DM tkeys feed the watermark accounting below; a conversation with
		// any non-human participant is skipped whole (write-path rule).
		dmTkey := map[int64]string{}
		dmConvos := map[string]bool{}
		topics := map[string]bool{}
		for _, m := range ex.Messages {
			if _, ok := ex.StreamByRecipient[m.Recipient]; !ok {
				ids, ok := dmParticipants(ex, m)
				allHuman := ok && humans[m.Sender]
				if ok {
					for _, id := range ids {
						if !humans[id] {
							allHuman = false
							break
						}
					}
				}
				if !ok || !allHuman {
					rep.DMMessagesSkipped++
					continue
				}
				parts := make([]string, len(ids))
				for i, id := range ids {
					parts[i] = fmt.Sprint(id)
				}
				key := "dm:" + strings.Join(parts, ":")
				dmTkey[m.ID] = key
				if !dmConvos[key] {
					dmConvos[key] = true
					rep.DMConversations++
				}
				rep.DMMessages++
				for _, e := range parseEditHistory(m.EditHistory) {
					if e.PrevContent == nil {
						continue
					}
					if e.UserID == nil || !humans[*e.UserID] {
						rep.EditEntriesSkipped++
					} else {
						rep.MessageEdits++
					}
				}
				continue
			}
			rep.Messages++
			for _, e := range parseEditHistory(m.EditHistory) {
				if e.PrevContent == nil {
					continue
				}
				if e.UserID == nil || !humans[*e.UserID] {
					rep.EditEntriesSkipped++
				} else {
					rep.MessageEdits++
				}
			}
			topics[fmt.Sprintf("%d\x00%s", ex.StreamByRecipient[m.Recipient], m.Subject)] = true
		}
		rep.Threads = len(topics)
		rep.Reactions = len(ex.Reactions)
		rep.Subscriptions = len(ex.Subscriptions)
		for _, a := range ex.Attachments {
			if a.PathID == "" || strings.Contains(a.PathID, "..") {
				rep.AttachmentFilesMissing++
				continue
			}
			if _, err := os.Stat(filepath.Join(ex.Dir, "uploads", a.PathID)); err != nil {
				rep.AttachmentFilesMissing++
			} else {
				rep.Attachments++
			}
		}
		// Groups: mirror the write-path mapping (system → seeded, fullmembers
		// coarsening → potential self-edges dropped) without touching the DB.
		sysWeft := map[int64]string{}
		custom := map[int64]bool{}
		for _, g := range ex.NamedGroups {
			if g.IsSystem {
				if wname := zulipSystemGroup(g.Name); wname != "" {
					sysWeft[g.ID] = wname
					rep.SystemGroupsMapped++
				}
				continue
			}
			custom[g.ID] = true
			rep.Groups++
		}
		for _, m := range ex.GroupMembers {
			if custom[m.UserGroup] && !bots[m.UserProfile] {
				rep.GroupMembers++
			}
		}
		for _, e := range ex.GroupEdges {
			superSys, superOK := sysWeft[e.Supergroup]
			subSys, subOK := sysWeft[e.Subgroup]
			mapped := (superOK || custom[e.Supergroup]) && (subOK || custom[e.Subgroup])
			selfEdge := superOK && subOK && superSys == subSys
			if mapped && !selfEdge {
				rep.GroupEdges++
			}
		}
		// Read watermarks: source ids are imported in order, so
		// max-by-source-id selects the same message the write pass lands on.
		msgTkey := map[int64]string{}
		for _, m := range ex.Messages {
			if sid, ok := ex.StreamByRecipient[m.Recipient]; ok {
				msgTkey[m.ID] = fmt.Sprintf("%d\x00%s", sid, m.Subject)
			} else if tk, ok := dmTkey[m.ID]; ok {
				msgTkey[m.ID] = tk
			}
		}
		type drk struct {
			user int64
			tkey string
		}
		dmax := map[drk]int64{}
		for _, um := range ex.UserMessages {
			if um.FlagsMask&umReadFlag == 0 || bots[um.UserProfile] {
				continue
			}
			tk, ok := msgTkey[um.Message]
			if !ok {
				continue
			}
			k := drk{um.UserProfile, tk}
			if um.Message > dmax[k] {
				dmax[k] = um.Message
			}
		}
		rep.Watermarks = len(dmax)
		for _, um := range ex.UserMessages {
			if um.FlagsMask&umReadFlag != 0 || bots[um.UserProfile] {
				continue
			}
			tk, ok := msgTkey[um.Message]
			if !ok {
				continue
			}
			if wm, ok := dmax[drk{um.UserProfile, tk}]; ok && um.Message < wm {
				rep.ReadCoarsened++
			}
		}
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

	// Seeded role/system groups, for role grants and system-group mapping.
	groupNameToID := map[string]int64{}
	rows, err = tx.Query(ctx,
		`SELECT lower(name), id FROM user_group WHERE org_id = $1`, orgID)
	if err != nil {
		return err
	}
	for rows.Next() {
		var n string
		var id int64
		if err := rows.Scan(&n, &id); err != nil {
			rows.Close()
			return err
		}
		groupNameToID[n] = id
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	// --- Users (ADR-001 D4: unmatched authors become claimable deactivated
	// placeholders; existing emails are matched, not duplicated). Source
	// roles carry over onto CREATED accounts only — an import never changes
	// an existing user's role.
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
			if weftRole(u.Role) != 40 {
				rep.RoleGrantsSkipped++
			}
			continue
		}
		role := weftRole(u.Role)
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
			orgID, enum.UserImportedPlaceholder, u.BestEmail(), u.FullName, role,
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
		// Role-group membership so permissions resolve when the placeholder
		// is claimed (idempotent on re-runs).
		if gname := roleGroup(role); gname != "" {
			if gid, ok := groupNameToID[gname]; ok {
				if _, err := tx.Exec(ctx, `
					INSERT INTO user_group_member (group_id, user_id)
					VALUES ($1, $2) ON CONFLICT DO NOTHING`, gid, id); err != nil {
					return fmt.Errorf("role group for user %d: %w", u.ID, err)
				}
			}
		}
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

	// --- Named user groups. System role groups map onto the seeded Weft
	// ones (never duplicated); custom groups import with provenance and
	// visible rename on live-name collisions. Memberships of system groups
	// are NOT copied — role fidelity flows through user_account.role above.
	zgroupIsSystem := map[int64]bool{}
	zgroupToWeft := map[int64]int64{} // zulip group id → weft group id
	for _, g := range ex.NamedGroups {
		if g.IsSystem {
			zgroupIsSystem[g.ID] = true
			if wname := zulipSystemGroup(g.Name); wname != "" {
				if wid, ok := groupNameToID[wname]; ok {
					zgroupToWeft[g.ID] = wid
					rep.SystemGroupsMapped++
				}
			}
			continue
		}
		// UNIQUE (org_id, name) on user_group is unconditional (unlike
		// channels), so even deactivated groups rename on collision.
		name := g.Name
		for i := 0; groupNameToID[strings.ToLower(name)] != 0; i++ {
			name = fmt.Sprintf("%s-%s%d", g.Name, originZulip, i+1)
		}
		if name != g.Name {
			rep.RenamedGroups[g.Name] = name
		}
		created := time.Time{}
		if g.DateCreated != nil {
			created = ts(*g.DateCreated)
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
			orgID, name, g.Description, nullableTime(created), g.Deactivated,
			originZulip, fmt.Sprint(g.ID)).Scan(&id)
		if err == pgx.ErrNoRows { // re-run
			if err := tx.QueryRow(ctx, `
				SELECT id FROM user_group WHERE org_id = $1
				 AND origin_system = $2 AND origin_id = $3`,
				orgID, originZulip, fmt.Sprint(g.ID)).Scan(&id); err != nil {
				return fmt.Errorf("resolve imported group %q: %w", g.Name, err)
			}
			rep.AlreadyImported++
			zgroupToWeft[g.ID] = id
			continue
		} else if err != nil {
			return fmt.Errorf("import group %q: %w", g.Name, err)
		}
		groupNameToID[strings.ToLower(name)] = id
		zgroupToWeft[g.ID] = id
		if _, err := eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: orgID, ActorKind: enum.ActorImporter,
			EntityType: enum.EntityGroup, EntityID: id, Verb: "usergroup.created",
			OccurredAt: created,
			Payload:    eventlog.MustPayload(map[string]any{"group_id": id, "name": name}),
		}); err != nil {
			return err
		}
		rep.Groups++
	}
	for _, m := range ex.GroupMembers {
		if zgroupIsSystem[m.UserGroup] {
			continue // roles carried via user_account.role, never via copy
		}
		gid, ok1 := zgroupToWeft[m.UserGroup]
		uid, ok2 := userMap[m.UserProfile]
		if !ok1 || !ok2 {
			continue
		}
		ct, err := tx.Exec(ctx, `
			INSERT INTO user_group_member (group_id, user_id)
			VALUES ($1, $2) ON CONFLICT DO NOTHING`, gid, uid)
		if err != nil {
			return fmt.Errorf("group member: %w", err)
		}
		if ct.RowsAffected() > 0 {
			rep.GroupMembers++
		}
	}
	for _, e := range ex.GroupEdges {
		super, ok1 := zgroupToWeft[e.Supergroup]
		sub, ok2 := zgroupToWeft[e.Subgroup]
		// Equal ids arise when two source groups coarsen onto one Weft group
		// (role:fullmembers + role:members); a self-edge is meaningless.
		if !ok1 || !ok2 || super == sub {
			continue
		}
		ct, err := tx.Exec(ctx, `
			INSERT INTO user_group_subgroup (group_id, subgroup_id)
			VALUES ($1, $2) ON CONFLICT DO NOTHING`, super, sub)
		if err != nil {
			return fmt.Errorf("group edge: %w", err)
		}
		if ct.RowsAffected() > 0 {
			rep.GroupEdges++
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
	msgThread := map[int64]int64{}  // zulip message id → our thread id
	pathToFile, fileByAttach, err := s.importAttachments(ctx, tx, orgID, ex, userMap, rep)
	if err != nil {
		return err
	}

	dmCache := map[string]dmInfo{} // canonical weft key → conversation
	for _, m := range ex.Messages {
		streamID, ok := ex.StreamByRecipient[m.Recipient]
		if !ok {
			if err := s.importDMMessage(ctx, tx, orgID, ex, m, userMap,
				nameMap, dmCache, pathToFile, messageMap, msgThread, rep); err != nil {
				return err
			}
			continue
		}
		chID, ok1 := channelMap[streamID]
		authorID, ok2 := userMap[m.Sender]
		if !ok1 || !ok2 {
			rep.StreamMessagesSkipped++
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

		src, hasAttach := rewriteUploads(m.Content, pathToFile)
		doc := content.Parse(src, resolve)
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
			msgThread[m.ID] = thID
			continue
		}
		if err != nil {
			return fmt.Errorf("message %d: %w", m.ID, err)
		}
		messageMap[m.ID] = msgID
		msgThread[m.ID] = thID
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
		if err := s.importEditHistory(ctx, tx, msgID, m, userMap, resolve, rep); err != nil {
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

	// --- Attachment references (the export's m2m is authoritative): each
	// mapped (attachment, message) pair becomes a file_reference, and the
	// message is flagged even when its content carried no inline link.
	for _, am := range ex.AttachmentMsg {
		fid, ok1 := fileByAttach[am.Attachment]
		mid, ok2 := messageMap[am.Message]
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

	// --- Read watermarks from usermessage read flags (F-7). Per (user,
	// thread), the watermark lands on the highest READ message; sparse
	// unread gaps below it are coarsened away and counted. Deliberately not
	// event-logged — read state stays off the durable spine (scale
	// contract), exactly like live mark-read.
	type rk struct{ user, thread int64 }
	maxRead := map[rk]int64{}
	for _, um := range ex.UserMessages {
		if um.FlagsMask&umReadFlag == 0 {
			continue
		}
		uid, ok1 := userMap[um.UserProfile]
		mid, ok2 := messageMap[um.Message]
		if !ok1 || !ok2 {
			continue // flags on unimported (DM) messages carry nothing
		}
		k := rk{uid, msgThread[um.Message]}
		if mid > maxRead[k] {
			maxRead[k] = mid
		}
	}
	for _, um := range ex.UserMessages {
		if um.FlagsMask&umReadFlag != 0 {
			continue
		}
		uid, ok1 := userMap[um.UserProfile]
		mid, ok2 := messageMap[um.Message]
		if !ok1 || !ok2 {
			continue
		}
		if wm, ok := maxRead[rk{uid, msgThread[um.Message]}]; ok && mid < wm {
			rep.ReadCoarsened++
		}
	}
	for k, mid := range maxRead {
		// Monotone like MarkRead: the guard makes re-runs true no-ops, so
		// idempotency is visible in the count too.
		ct, err := tx.Exec(ctx, `
			INSERT INTO thread_read_watermark (user_id, thread_id, last_read_message_id, updated_at)
			VALUES ($1, $2, $3, now())
			ON CONFLICT (user_id, thread_id) DO UPDATE
			SET last_read_message_id = EXCLUDED.last_read_message_id, updated_at = now()
			WHERE thread_read_watermark.last_read_message_id < EXCLUDED.last_read_message_id`,
			k.user, k.thread, mid)
		if err != nil {
			return fmt.Errorf("watermark: %w", err)
		}
		if ct.RowsAffected() > 0 {
			rep.Watermarks++
		}
	}

	// Role grants and group writes above bypass the perms service, so the
	// flattened closure is rebuilt once here (same recursive CTE the
	// service uses).
	if err := perms.New(s.pool).RebuildClosure(ctx, tx, orgID); err != nil {
		return fmt.Errorf("closure rebuild: %w", err)
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

// importDMMessage lands one direct message: resolve the participant set,
// create-or-get the conversation by its canonical key (the SAME derivation
// the native dm module uses, so imported and native history share one
// dm_space), then the provenance-idempotent message write. A conversation
// with any unmappable participant (a bot) is skipped and counted — dropping
// the participant would shrink the key and wrongly merge the history into a
// different conversation.
func (s *Service) importDMMessage(ctx context.Context, tx pgx.Tx, orgID int64,
	ex *Export, m zulipMessage, userMap map[int64]int64,
	nameMap map[string]int64, dmCache map[string]dmInfo, pathToFile map[string]int64,
	messageMap, msgThread map[int64]int64, rep *Report) error {

	zulipIDs, ok := dmParticipants(ex, m)
	if !ok {
		rep.DMMessagesSkipped++
		return nil
	}
	weftIDs := make([]int64, 0, len(zulipIDs))
	for _, zid := range zulipIDs {
		uid, mapped := userMap[zid]
		if !mapped {
			rep.DMMessagesSkipped++
			return nil
		}
		weftIDs = append(weftIDs, uid)
	}
	authorID, ok := userMap[m.Sender]
	if !ok {
		rep.DMMessagesSkipped++
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
			rep.AlreadyImported++
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
				OccurredAt: ts(m.DateSent),
				Payload: eventlog.MustPayload(map[string]any{
					"dm_space_id": info.spaceID, "root_thread_id": info.threadID,
					"user_ids": weftIDs}),
			}); err != nil {
				return err
			}
			rep.DMConversations++
		}
		dmCache[key] = info
	}

	src, hasAttach := rewriteUploads(m.Content, pathToFile)
	resolve := func(label string) (int64, bool) {
		id, ok := nameMap[label]
		return id, ok
	}
	doc := content.Parse(src, resolve)
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
		ts(m.DateSent), originZulip, fmt.Sprint(m.ID)).Scan(&msgID)
	if err == pgx.ErrNoRows {
		rep.AlreadyImported++
		if err := tx.QueryRow(ctx, `
			SELECT id FROM message WHERE org_id = $1
			 AND origin_system = $2 AND origin_id = $3`,
			orgID, originZulip, fmt.Sprint(m.ID)).Scan(&msgID); err != nil {
			return fmt.Errorf("resolve imported dm message: %w", err)
		}
		messageMap[m.ID] = msgID
		msgThread[m.ID] = info.threadID
		return nil
	}
	if err != nil {
		return fmt.Errorf("dm message %d: %w", m.ID, err)
	}
	messageMap[m.ID] = msgID
	msgThread[m.ID] = info.threadID
	if _, err := eventlog.Append(ctx, tx, eventlog.Event{
		OrgID: orgID, ActorKind: enum.ActorImporter,
		EntityType: enum.EntityMessage, EntityID: msgID, Verb: "message.created",
		OccurredAt: ts(m.DateSent),
		Payload: eventlog.MustPayload(map[string]any{
			"message_id": msgID, "dm_space_id": info.spaceID,
			"thread_id": info.threadID, "mentions": doc.Mentions()}),
	}); err != nil {
		return err
	}
	if err := s.importEditHistory(ctx, tx, msgID, m, userMap, resolve, rep); err != nil {
		return err
	}
	rep.DMMessages++
	return nil
}

// importAttachments stores every attachment blob (content-addressed through
// files.StorageKey, so backfilled bytes dedup with live uploads) and records
// provenance-idempotent file rows. Returns path_id→file id (for content
// link rewriting) and attachment id→file id (for the m2m references).
func (s *Service) importAttachments(ctx context.Context, tx pgx.Tx, orgID int64, ex *Export, userMap map[int64]int64, rep *Report) (map[string]int64, map[int64]int64, error) {
	pathToFile := map[string]int64{}
	fileByAttach := map[int64]int64{}
	for _, a := range ex.Attachments {
		if a.PathID == "" || strings.Contains(a.PathID, "..") {
			rep.AttachmentFilesMissing++
			continue
		}
		full := filepath.Join(ex.Dir, "uploads", a.PathID)
		f, err := os.Open(full)
		if err != nil {
			rep.AttachmentFilesMissing++
			continue
		}
		h := sha256.New()
		size, err := io.Copy(h, f)
		if err != nil {
			f.Close()
			return nil, nil, fmt.Errorf("hash attachment %d: %w", a.ID, err)
		}
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			f.Close()
			return nil, nil, err
		}
		sum := h.Sum(nil)
		key := files.StorageKey(orgID, hex.EncodeToString(sum))
		// Put is idempotent (content-addressed): a crash between blob write
		// and commit just re-puts on the retry.
		err = s.store.Put(ctx, key, f)
		f.Close()
		if err != nil {
			return nil, nil, fmt.Errorf("store attachment %d: %w", a.ID, err)
		}
		mime := "application/octet-stream"
		if a.ContentType != nil && *a.ContentType != "" {
			mime = *a.ContentType
		}
		var owner *int64
		if uid, ok := userMap[a.Owner]; ok {
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
			orgID, a.FileName, mime, size, sum, key, owner,
			ts(a.CreateTime), originZulip, fmt.Sprint(a.ID)).Scan(&id)
		if err == pgx.ErrNoRows { // re-run
			if err := tx.QueryRow(ctx, `
				SELECT id FROM file WHERE org_id = $1
				 AND origin_system = $2 AND origin_id = $3`,
				orgID, originZulip, fmt.Sprint(a.ID)).Scan(&id); err != nil {
				return nil, nil, fmt.Errorf("resolve imported file %d: %w", a.ID, err)
			}
			rep.AlreadyImported++
		} else if err != nil {
			return nil, nil, fmt.Errorf("import attachment %d: %w", a.ID, err)
		} else {
			if _, err := eventlog.Append(ctx, tx, eventlog.Event{
				OrgID: orgID, ActorKind: enum.ActorImporter,
				EntityType: enum.EntityFile, EntityID: id, Verb: "file.uploaded",
				OccurredAt: ts(a.CreateTime),
				Payload: eventlog.MustPayload(map[string]any{
					"file_id": id, "name": a.FileName, "size_bytes": size}),
			}); err != nil {
				return nil, nil, err
			}
			rep.Attachments++
		}
		pathToFile[a.PathID] = id
		fileByAttach[a.ID] = id
	}
	return pathToFile, fileByAttach, nil
}

// rewriteUploads swaps /user_uploads/<path_id> links (relative or absolute)
// for our managed-file URLs so imported content renders working links.
func rewriteUploads(contentSrc string, pathToFile map[string]int64) (string, bool) {
	if len(pathToFile) == 0 || !strings.Contains(contentSrc, "/user_uploads/") {
		return contentSrc, false
	}
	changed := false
	for path, id := range pathToFile {
		needle := "/user_uploads/" + path
		if strings.Contains(contentSrc, needle) {
			contentSrc = strings.ReplaceAll(contentSrc, needle, fmt.Sprintf("/api/v1/files/%d", id))
			changed = true
		}
	}
	return contentSrc, changed
}

// importEditHistory materializes a message's content edits as kind-1
// revisions, oldest first, re-parsing prior content through our engine.
// Entries without an attributable editor are skipped and counted; topic and
// stream moves carry no prev_content and are not message revisions.
func (s *Service) importEditHistory(ctx context.Context, tx pgx.Tx, msgID int64, m zulipMessage, userMap map[int64]int64, resolve func(string) (int64, bool), rep *Report) error {
	entries := parseEditHistory(m.EditHistory)
	if len(entries) == 0 {
		return nil
	}
	revNo := 0
	var lastEdit float64
	for _, e := range entries {
		if e.PrevContent == nil {
			continue
		}
		if e.UserID == nil {
			rep.EditEntriesSkipped++
			continue
		}
		editor, ok := userMap[*e.UserID]
		if !ok {
			rep.EditEntriesSkipped++
			continue
		}
		revNo++
		prevDoc := content.Parse(*e.PrevContent, resolve)
		ct, err := tx.Exec(ctx, `
			INSERT INTO message_revision
				(message_id, revision_no, kind, prev_source, prev_ast, edited_by, edited_at)
			VALUES ($1, $2, 1, $3, $4, $5, $6)
			ON CONFLICT (message_id, revision_no) DO NOTHING`,
			msgID, revNo, *e.PrevContent, prevDoc.JSON(), editor, ts(e.Timestamp))
		if err != nil {
			return fmt.Errorf("import revision: %w", err)
		}
		if ct.RowsAffected() > 0 {
			rep.MessageEdits++
		}
		if e.Timestamp > lastEdit {
			lastEdit = e.Timestamp
		}
	}
	if lastEdit > 0 {
		if _, err := tx.Exec(ctx,
			`UPDATE message SET edited_at = $1 WHERE id = $2 AND edited_at IS NULL`,
			ts(lastEdit), msgID); err != nil {
			return fmt.Errorf("stamp edited_at: %w", err)
		}
	}
	return nil
}
