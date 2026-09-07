package importer

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
)

// This file is the importer's accounting. Exactly one thing in the module
// decides what a Report says, and it is `newPlan` below — the dry run and the
// write path both project their numbers from it, over the same IR, against the
// same picture of the target org.
//
// It exists because the two used to decide separately and had drifted:
// `subscriptions` read 10 on the dry side against 3 on the write side, a
// re-run read full counts with `already_imported: 0` against all-zeros with
// `already_imported: 17`, and two loss buckets were unreachable on the dry
// side entirely. Three rules resolve that, and they are the reason this is a
// planner rather than a shared helper:
//
//   - A BUCKET MEANS WHAT THE WRITE WOULD LAND, never what the source
//     contains. "This export holds ten subscription rows" is not something an
//     operator can act on; "three channel memberships will appear" is.
//   - THE PLAN IS TAKEN AGAINST THE REAL ORG. Existing origin keys, live
//     names and the email index all feed in, so the answer is true for the
//     org in front of the operator and not only for a virgin one — which is
//     precisely the org nobody needs to ask about.
//   - THE COUNTING UNIT IS ROWS AFFECTED. Every write in this module is
//     idempotent (ON CONFLICT DO NOTHING, or the watermark's monotone DO
//     UPDATE), so a plan that counted keys would over-promise wherever a row
//     already sits at or above what we would write.

// pairKey is a two-id composite key (channel/user, group/user, group/subgroup,
// user/thread).
type pairKey struct{ a, b int64 }

type reactionKey struct {
	message, user int64
	emoji         string
}

// resolution is the target org as the importer needs to see it: the keys an
// incoming entity can already match, and nothing else. It is READ-ONLY — the
// planner copies whatever it has to mutate, so the same value can serve a dry
// run and a write.
//
// SCALE. Every map here is loaded once per import, and each is bounded either
// by the org's group/channel/user directory (already the shape the write path
// preloaded before this slice) or by what THIS SOURCE has already imported —
// never by org-wide message or membership volume. The provenance lookups ride
// the partial unique index the origin upserts already use, `(org_id,
// origin_system, origin_id) WHERE origin_system IS NOT NULL`, as a two-column
// prefix scan. A virgin org loads zero rows into six of these maps. The
// recorded upgrade for a multi-GB export is the same one the module already
// carries — per-chunk streaming — at which point the provenance maps become
// per-chunk probes instead of one preload.
type resolution struct {
	// emailToID is the D4 match index: lower(email) → account id. '' is never
	// a key (the preload's IS NOT NULL admits it, so the planner guards).
	emailToID map[string]int64
	// liveNames is lower(name) of every NON-archived channel — the collision
	// set the visible rename walks.
	liveNames map[string]bool
	// groupNameToID is lower(name) → id for EVERY group, system or not: it is
	// both the system-group mapping target and the group rename's collision
	// set.
	groupNameToID map[string]int64

	// Provenance: origin_id → our id, per table, scoped to ONE source. An org
	// that has ingested two exports must never see the other loader's keys.
	users    map[string]int64
	channels map[string]int64
	threads  map[string]int64
	messages map[string]int64
	groups   map[string]int64
	files    map[string]int64

	// dmSpaces is keyed by the CANONICAL dm key (sorted weft ids), not by
	// provenance: dm_space carries no origin columns because an imported
	// conversation and a native one are the same conversation.
	dmSpaces map[string]dmInfo

	// Row-level idempotency sets, for the writes whose bucket counts rows
	// affected rather than keys seen.
	members      map[pairKey]bool // (channel_id, user_id)
	groupMembers map[pairKey]bool // (group_id, user_id)
	groupEdges   map[pairKey]bool // (group_id, subgroup_id)
	reactions    map[reactionKey]bool
	watermarks   map[pairKey]int64 // (user_id, thread_id) → last_read_message_id
}

// loadResolution reads the target org's current shape inside the caller's
// transaction, so every decision downstream is taken against ONE snapshot.
func loadResolution(ctx context.Context, tx pgx.Tx, orgID int64, source string) (*resolution, error) {
	rc := &resolution{
		emailToID:     map[string]int64{},
		liveNames:     map[string]bool{},
		groupNameToID: map[string]int64{},
		users:         map[string]int64{},
		channels:      map[string]int64{},
		threads:       map[string]int64{},
		messages:      map[string]int64{},
		groups:        map[string]int64{},
		files:         map[string]int64{},
		dmSpaces:      map[string]dmInfo{},
		members:       map[pairKey]bool{},
		groupMembers:  map[pairKey]bool{},
		groupEdges:    map[pairKey]bool{},
		reactions:     map[reactionKey]bool{},
		watermarks:    map[pairKey]int64{},
	}
	if err := scanPairs(ctx, tx, `
		SELECT lower(email), id FROM user_account
		WHERE org_id = $1 AND email IS NOT NULL`,
		[]any{orgID}, rc.emailToID); err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `
		SELECT lower(name) FROM channel
		WHERE org_id = $1 AND archived_at IS NULL`, orgID)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			rows.Close()
			return nil, err
		}
		rc.liveNames[n] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := scanPairs(ctx, tx,
		`SELECT lower(name), id FROM user_group WHERE org_id = $1`,
		[]any{orgID}, rc.groupNameToID); err != nil {
		return nil, err
	}

	for _, t := range []struct {
		table string
		dst   map[string]int64
	}{
		{"user_account", rc.users},
		{"channel", rc.channels},
		{"thread", rc.threads},
		{"message", rc.messages},
		{"user_group", rc.groups},
		{"file", rc.files},
	} {
		// The table name is a compile-time constant from the list above, never
		// caller input; org and source stay bound parameters.
		q := `SELECT origin_id, id FROM ` + t.table +
			` WHERE org_id = $1 AND origin_system = $2 AND origin_id IS NOT NULL`
		if err := scanPairs(ctx, tx, q, []any{orgID, source}, t.dst); err != nil {
			return nil, err
		}
	}

	// DM conversations are matched by canonical key, and their root thread id
	// comes along because the message lane needs it.
	dmRows, err := tx.Query(ctx, `
		SELECT ds.dm_key, ds.id, t.id
		FROM dm_space ds JOIN thread t ON t.dm_space_id = ds.id AND t.kind = 2
		WHERE ds.org_id = $1`, orgID)
	if err != nil {
		return nil, err
	}
	for dmRows.Next() {
		var key string
		var info dmInfo
		if err := dmRows.Scan(&key, &info.spaceID, &info.threadID); err != nil {
			dmRows.Close()
			return nil, err
		}
		rc.dmSpaces[key] = info
	}
	dmRows.Close()
	if err := dmRows.Err(); err != nil {
		return nil, err
	}

	for _, p := range []struct {
		sql  string
		args []any
		dst  map[pairKey]bool
	}{
		// Memberships of channels THIS source already imported. Rows survive
		// unsubscribe, and an unsubscribed row still blocks the insert, so the
		// scan is deliberately unfiltered on unsubscribed_at.
		{`SELECT cm.channel_id, cm.user_id FROM channel_member cm
		    JOIN channel c ON c.id = cm.channel_id
		   WHERE c.org_id = $1 AND c.origin_system = $2`,
			[]any{orgID, source}, rc.members},
		// Memberships of groups this source already imported. System groups
		// are excluded by construction: the importer never copies their
		// membership (role fidelity flows through user_account.role).
		{`SELECT m.group_id, m.user_id FROM user_group_member m
		    JOIN user_group g ON g.id = m.group_id
		   WHERE g.org_id = $1 AND g.origin_system = $2`,
			[]any{orgID, source}, rc.groupMembers},
		// Nesting edges are org-wide: an imported group nests UNDER a seeded
		// system group, so scoping this to the source would miss the very
		// edges the import writes. Bounded by the group graph, never by
		// membership.
		{`SELECT s.group_id, s.subgroup_id FROM user_group_subgroup s
		    JOIN user_group g ON g.id = s.group_id WHERE g.org_id = $1`,
			[]any{orgID}, rc.groupEdges},
	} {
		rows, err := tx.Query(ctx, p.sql, p.args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var k pairKey
			if err := rows.Scan(&k.a, &k.b); err != nil {
				rows.Close()
				return nil, err
			}
			p.dst[k] = true
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}

	reactRows, err := tx.Query(ctx, `
		SELECT r.message_id, r.user_id, r.emoji FROM reaction r
		  JOIN message m ON m.id = r.message_id
		 WHERE m.org_id = $1 AND m.origin_system = $2`, orgID, source)
	if err != nil {
		return nil, err
	}
	for reactRows.Next() {
		var k reactionKey
		if err := reactRows.Scan(&k.message, &k.user, &k.emoji); err != nil {
			reactRows.Close()
			return nil, err
		}
		rc.reactions[k] = true
	}
	reactRows.Close()
	if err := reactRows.Err(); err != nil {
		return nil, err
	}

	// Watermarks on the threads this import can reach: the ones it wrote
	// before, plus every DM root (a DM conversation is matched by canonical
	// key, so an import lands in NATIVE conversations too and a native
	// watermark there is exactly the "already at or above" case).
	wmRows, err := tx.Query(ctx, `
		SELECT w.user_id, w.thread_id, w.last_read_message_id
		FROM thread_read_watermark w JOIN thread t ON t.id = w.thread_id
		WHERE t.org_id = $1 AND (t.origin_system = $2 OR t.dm_space_id IS NOT NULL)`,
		orgID, source)
	if err != nil {
		return nil, err
	}
	for wmRows.Next() {
		var k pairKey
		var last int64
		if err := wmRows.Scan(&k.a, &k.b, &last); err != nil {
			wmRows.Close()
			return nil, err
		}
		rc.watermarks[k] = last
	}
	wmRows.Close()
	if err := wmRows.Err(); err != nil {
		return nil, err
	}
	return rc, nil
}

// scanPairs drains a two-column (text, bigint) query into dst.
func scanPairs(ctx context.Context, tx pgx.Tx, sql string, args []any, dst map[string]int64) error {
	rows, err := tx.Query(ctx, sql, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var k string
		var v int64
		if err := rows.Scan(&k, &v); err != nil {
			return err
		}
		dst[k] = v
	}
	return rows.Err()
}

// landing is where a row will sit once the import commits. `fresh` rows are
// the ones this run creates; they always end up with ids ABOVE every id the
// table already holds, and among themselves they ascend with the message
// ordinal because the write path inserts in ordinal order. That is what lets
// the plan predict an id-ORDERED outcome for rows whose ids do not exist yet.
//
// id is always populated and always unique: the real id for a row that exists,
// a virtual one otherwise, so it can be used as a composite-key component
// (a reaction's message) without a fresh row ever aliasing a landed one.
type landing struct {
	id      int64
	fresh   bool
	ordinal int64 // orders fresh rows against each other
}

// above reports whether l will hold a HIGHER id than other.
func (l landing) above(other landing) bool {
	if l.fresh != other.fresh {
		return l.fresh
	}
	if l.fresh {
		return l.ordinal > other.ordinal
	}
	return l.id > other.id
}

// watermarkWrite is one (user, message) pair the read-state reducer chose. The
// executor resolves both source ids against the maps it built while writing —
// the plan never has to know a freshly minted id.
type watermarkWrite struct {
	userSourceID    string
	messageSourceID string
}

// plan is one import's decided outcome. rep is the whole of the accounting;
// the rest is what the executor needs in order to LAND exactly what rep
// promises.
type plan struct {
	rep Report

	// channelName / groupName carry the post-collision name for entities this
	// run creates. The executor must not re-derive them: the walk depends on
	// names claimed earlier in the same run, and a second walk over a
	// half-written org would answer differently.
	channelName map[string]string
	groupName   map[string]string

	// watermarks is the read-state reduction, in a stable order.
	watermarks []watermarkWrite
}

// virtualIDs hands out ids for rows that do not exist yet. They are negative
// so they can never be mistaken for a real id or collide with one, and they
// are distinct so that two source entities which will become two rows never
// share a composite key.
type virtualIDs struct{ next int64 }

func (v *virtualIDs) mint() int64 {
	v.next--
	return v.next
}

// newPlan decides everything a Report says, for this IR against this org.
// It writes nothing and it opens no attachment — the only source I/O it does
// is the cheap existence Probe, which is exactly what that hook is for.
//
// Message order is established here, once, because half the plan depends on it
// (which message roots a thread, which one a watermark lands on).
func newPlan(ir *Import, rc *resolution) (*plan, error) {
	sortByOrdinal(ir.Messages)

	pl := &plan{
		rep:         Report{Source: ir.Source, RenamedChannels: map[string]string{}, RenamedGroups: map[string]string{}},
		channelName: map[string]string{},
		groupName:   map[string]string{},
	}
	rep := &pl.rep
	var virt virtualIDs

	// --- Users. The order of the two match lanes is load-bearing and mirrors
	// the write path exactly: an email that names a LIVE account matches it
	// (D4) before provenance is ever consulted, which is why a re-run of an
	// export whose users carry emails reports MatchedExistingByEmail and not
	// AlreadyImported.
	emailToID := copyStringInt(rc.emailToID)
	userID := map[string]int64{}
	for _, u := range ir.Users {
		if u.Bot {
			rep.BotsSkipped++
			continue
		}
		key := strings.ToLower(u.Email)
		if existing, ok := emailToID[key]; ok && key != "" {
			userID[u.SourceID] = existing
			rep.MatchedExistingByEmail++
			if u.Role != 40 {
				rep.RoleGrantsSkipped++
			}
			continue
		}
		if id, ok := rc.users[u.SourceID]; ok {
			userID[u.SourceID] = id
			rep.AlreadyImported++
			continue
		}
		if id, ok := userID[u.SourceID]; ok {
			// The same source id twice in one export: the second INSERT hits
			// the origin index and resolves onto the first.
			userID[u.SourceID] = id
			rep.AlreadyImported++
			continue
		}
		id := virt.mint()
		userID[u.SourceID] = id
		rep.Users++
		if key != "" {
			// A LATER user carrying the same email matches this one, exactly
			// as it would once the row exists.
			emailToID[key] = id
		}
	}

	// --- Channels. The rename walk claims names as it goes; a channel that
	// already exists by provenance does NOT claim its name (preserved: it is
	// what makes a re-run report a phantom rename it will not perform).
	liveNames := copyStringBool(rc.liveNames)
	channelID := map[string]int64{}
	for _, ch := range ir.Channels {
		name := ch.Name
		for i := 0; !ch.Archived && liveNames[strings.ToLower(name)]; i++ {
			name = fmt.Sprintf("%s-%s%d", ch.Name, ir.Source, i+1)
		}
		if name != ch.Name {
			rep.RenamedChannels[ch.Name] = name
		}
		if id, ok := rc.channels[ch.SourceID]; ok {
			channelID[ch.SourceID] = id
			rep.AlreadyImported++
			continue
		}
		if id, ok := channelID[ch.SourceID]; ok {
			channelID[ch.SourceID] = id
			rep.AlreadyImported++
			continue
		}
		liveNames[strings.ToLower(name)] = true
		channelID[ch.SourceID] = virt.mint()
		pl.channelName[ch.SourceID] = name
		rep.Channels++
	}

	// --- Memberships. Counted as ROWS AFFECTED: a pair already present (from
	// a previous run, or twice in this export) adds nothing.
	planned := map[pairKey]bool{}
	for _, mem := range ir.Memberships {
		chID, ok1 := channelID[mem.ChannelID]
		uID, ok2 := userID[mem.UserID]
		if !ok1 || !ok2 {
			continue
		}
		k := pairKey{chID, uID}
		if planned[k] || rc.members[k] {
			continue
		}
		planned[k] = true
		rep.Subscriptions++
	}

	// --- Groups. System groups MAP onto seeded ones and are counted only when
	// the seeded counterpart actually exists in this org — the write path's
	// condition, not "the source called it a system group".
	groupNameToID := copyStringInt(rc.groupNameToID)
	groupID := map[string]int64{}
	systemGroup := map[string]bool{}
	for _, g := range ir.Groups {
		if g.System {
			systemGroup[g.SourceID] = true
			if g.SystemName != "" {
				if wid, ok := groupNameToID[g.SystemName]; ok {
					groupID[g.SourceID] = wid
					rep.SystemGroupsMapped++
				}
			}
			continue
		}
		// UNIQUE (org_id, name) on user_group is unconditional (unlike
		// channels), so even deactivated groups rename on collision.
		name := g.Name
		for i := 0; groupNameToID[strings.ToLower(name)] != 0; i++ {
			name = fmt.Sprintf("%s-%s%d", g.Name, ir.Source, i+1)
		}
		if name != g.Name {
			rep.RenamedGroups[g.Name] = name
		}
		if id, ok := rc.groups[g.SourceID]; ok {
			groupID[g.SourceID] = id
			rep.AlreadyImported++
			continue
		}
		if id, ok := groupID[g.SourceID]; ok {
			groupID[g.SourceID] = id
			rep.AlreadyImported++
			continue
		}
		id := virt.mint()
		groupNameToID[strings.ToLower(name)] = id
		groupID[g.SourceID] = id
		pl.groupName[g.SourceID] = name
		rep.Groups++
	}
	plannedGM := map[pairKey]bool{}
	for _, m := range ir.GroupMembers {
		if systemGroup[m.GroupID] {
			continue // roles carried via user_account.role, never via copy
		}
		gid, ok1 := groupID[m.GroupID]
		uid, ok2 := userID[m.UserID]
		if !ok1 || !ok2 {
			continue
		}
		k := pairKey{gid, uid}
		if plannedGM[k] || rc.groupMembers[k] {
			continue
		}
		plannedGM[k] = true
		rep.GroupMembers++
	}
	plannedGE := map[pairKey]bool{}
	for _, e := range ir.GroupEdges {
		super, ok1 := groupID[e.GroupID]
		sub, ok2 := groupID[e.SubgroupID]
		// Equal ids arise when two source groups coarsen onto one Weft group
		// (role:fullmembers + role:members); a self-edge is meaningless.
		if !ok1 || !ok2 || super == sub {
			continue
		}
		k := pairKey{super, sub}
		if plannedGE[k] || rc.groupEdges[k] {
			continue
		}
		plannedGE[k] = true
		rep.GroupEdges++
	}

	// --- Attachments. The PROBE decides, never an Open: for a remote source
	// that is the difference between a stat and a download, and the IR's whole
	// reason for carrying both hooks.
	fileID := map[string]int64{}
	for _, a := range ir.Attachments {
		if a.Probe == nil || a.Open == nil {
			rep.AttachmentFilesMissing++
			continue
		}
		if _, err := a.Probe(); err != nil {
			rep.AttachmentFilesMissing++
			continue
		}
		if id, ok := rc.files[a.SourceID]; ok {
			fileID[a.SourceID] = id
			rep.AlreadyImported++
			continue
		}
		if id, ok := fileID[a.SourceID]; ok {
			fileID[a.SourceID] = id
			rep.AlreadyImported++
			continue
		}
		fileID[a.SourceID] = virt.mint()
		rep.Attachments++
	}

	// --- Messages, threads and conversations, in ordinal order.
	convByKey := map[string][]string{}
	for _, c := range ir.Conversations {
		convByKey[c.SourceKey] = c.MemberIDs
	}
	threadID := map[string]int64{}       // Thread.Key → id
	threadByOrigin := map[string]int64{} // Thread.SourceID claimed this run
	dmCache := map[string]dmInfo{}
	msgLanding := map[string]landing{} // source message id → where it will sit
	msgThread := map[string]int64{}    // source message id → thread id

	for i := range ir.Messages {
		m := &ir.Messages[i]
		if m.Container.Kind != ContainerChannel {
			pl.planDirectMessage(m, rc, convByKey, userID, dmCache,
				msgLanding, msgThread, &virt)
			continue
		}
		_, ok1 := channelID[m.Container.Key]
		_, ok2 := userID[m.AuthorID]
		if !ok1 || !ok2 {
			rep.ChannelMessagesSkipped++
			continue
		}
		if m.Thread == nil {
			// The one invariant the IR's types cannot express. Refused HERE,
			// before a single row is written, so a dry run answers it too.
			return nil, fmt.Errorf("import message %s: channel message without a thread", m.SourceID)
		}
		thID, ok := threadID[m.Thread.Key]
		if !ok {
			if id, existing := rc.threads[m.Thread.SourceID]; existing {
				thID = id
				rep.AlreadyImported++
			} else if id, claimed := threadByOrigin[m.Thread.SourceID]; claimed {
				// Two in-run grouping keys sharing one provenance key: the
				// second INSERT resolves onto the row the first created.
				thID = id
				rep.AlreadyImported++
			} else {
				thID = virt.mint()
				threadByOrigin[m.Thread.SourceID] = thID
				rep.Threads++
			}
			threadID[m.Thread.Key] = thID
		}
		if id, ok := rc.messages[m.SourceID]; ok {
			msgLanding[m.SourceID] = landing{id: id}
			msgThread[m.SourceID] = thID
			rep.AlreadyImported++
			continue // a re-run imports no edits: the message row is not new
		}
		if _, ok := msgLanding[m.SourceID]; ok {
			msgThread[m.SourceID] = thID
			rep.AlreadyImported++
			continue
		}
		msgLanding[m.SourceID] = landing{id: virt.mint(), fresh: true, ordinal: m.Ordinal}
		msgThread[m.SourceID] = thID
		pl.planEdits(m, userID)
		rep.Messages++
	}

	// --- Reactions. Rows affected again: the PK is (message, user, emoji),
	// and a message this run creates carries none yet — its landing id is
	// virtual, so it can never match a row already in the table.
	plannedReact := map[reactionKey]bool{}
	for _, r := range ir.Reactions {
		land, ok1 := msgLanding[r.MessageID]
		uID, ok2 := userID[r.UserID]
		if !ok1 || !ok2 {
			rep.ReactionsUnmapped++
			continue
		}
		k := reactionKey{message: land.id, user: uID, emoji: r.Emoji}
		if plannedReact[k] || rc.reactions[k] {
			continue
		}
		plannedReact[k] = true
		rep.Reactions++
	}

	pl.planReadState(ir, rc, userID, msgLanding, msgThread)
	pl.rep.finalize()
	return pl, nil
}

// planDirectMessage mirrors the DM lane: a conversation imports only when
// EVERY participant maps to a human account, and it is matched by the
// canonical key the native dm module computes, so imported history joins a
// native conversation instead of forking one.
func (pl *plan) planDirectMessage(m *Message, rc *resolution, convByKey map[string][]string,
	userID map[string]int64, dmCache map[string]dmInfo,
	msgLanding map[string]landing, msgThread map[string]int64, virt *virtualIDs) {

	rep := &pl.rep
	sourceIDs, ok := convByKey[m.Container.Key]
	if !ok {
		rep.DMMessagesSkipped++
		return
	}
	weftIDs := make([]int64, 0, len(sourceIDs))
	for _, sid := range sourceIDs {
		uid, mapped := userID[sid]
		if !mapped {
			rep.DMMessagesSkipped++
			return
		}
		weftIDs = append(weftIDs, uid)
	}
	if _, ok := userID[m.AuthorID]; !ok {
		rep.DMMessagesSkipped++
		return
	}
	sort.Slice(weftIDs, func(i, j int) bool { return weftIDs[i] < weftIDs[j] })
	parts := make([]string, len(weftIDs))
	for i, id := range weftIDs {
		parts[i] = fmt.Sprint(id)
	}
	// A key holding any virtual id cannot match a live conversation, which is
	// the right answer: a conversation whose participants do not all exist yet
	// cannot already exist either.
	key := strings.Join(parts, ":")

	info, cached := dmCache[key]
	if !cached {
		if existing, ok := rc.dmSpaces[key]; ok {
			info = existing
			rep.AlreadyImported++
		} else {
			info = dmInfo{spaceID: virt.mint(), threadID: virt.mint()}
			rep.DMConversations++
		}
		dmCache[key] = info
	}
	if id, ok := rc.messages[m.SourceID]; ok {
		msgLanding[m.SourceID] = landing{id: id}
		msgThread[m.SourceID] = info.threadID
		rep.AlreadyImported++
		return
	}
	if _, ok := msgLanding[m.SourceID]; ok {
		msgThread[m.SourceID] = info.threadID
		rep.AlreadyImported++
		return
	}
	msgLanding[m.SourceID] = landing{id: virt.mint(), fresh: true, ordinal: m.Ordinal}
	msgThread[m.SourceID] = info.threadID
	pl.planEdits(m, userID)
	rep.DMMessages++
}

// planEdits counts a NEW message's content revisions. It is only ever reached
// for a message this run creates, which is why no existing-revision lookup is
// needed: a row that does not exist yet carries no revisions.
func (pl *plan) planEdits(m *Message, userID map[string]int64) {
	for _, e := range m.Edits {
		if e.EditorID == "" {
			pl.rep.EditEntriesSkipped++
			continue
		}
		if _, ok := userID[e.EditorID]; !ok {
			pl.rep.EditEntriesSkipped++
			continue
		}
		pl.rep.MessageEdits++
	}
}

// planReadState is the F-7 reducer. Per (user, thread) the watermark lands on
// the message that will hold the HIGHEST id among the read ones — which is the
// reduction the write path performs, because the watermark is an id cutoff and
// any lower choice would leave a message the source called read sitting above
// the line. Fresh messages outrank landed ones and order among themselves by
// ORDINAL, never by source id: Slack's `ts` strings have no `>` that means
// "later", so the source-id max the dry branch used could not survive a second
// loader.
//
// Unread messages BELOW the chosen watermark are the F-7 coarsening, counted
// and never silent.
func (pl *plan) planReadState(ir *Import, rc *resolution, userID map[string]int64,
	msgLanding map[string]landing, msgThread map[string]int64) {

	type chosen struct {
		land     landing
		sourceID string
		user     string
	}
	best := map[pairKey]chosen{}
	for _, rs := range ir.ReadState {
		if !rs.Read {
			continue
		}
		uid, ok1 := userID[rs.UserID]
		land, ok2 := msgLanding[rs.MessageID]
		if !ok1 || !ok2 {
			continue // flags on messages this import did not land carry nothing
		}
		k := pairKey{uid, msgThread[rs.MessageID]}
		if cur, seen := best[k]; !seen || land.above(cur.land) {
			best[k] = chosen{land: land, sourceID: rs.MessageID, user: rs.UserID}
		}
	}
	for _, rs := range ir.ReadState {
		if rs.Read {
			continue
		}
		uid, ok1 := userID[rs.UserID]
		land, ok2 := msgLanding[rs.MessageID]
		if !ok1 || !ok2 {
			continue
		}
		if wm, ok := best[pairKey{uid, msgThread[rs.MessageID]}]; ok && wm.land.above(land) {
			pl.rep.ReadCoarsened++
		}
	}

	keys := make([]pairKey, 0, len(best))
	for k := range best {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].a != keys[j].a {
			return keys[i].a < keys[j].a
		}
		return keys[i].b < keys[j].b
	})
	for _, k := range keys {
		c := best[k]
		if !c.land.fresh {
			// The upsert is monotone: a watermark already at or above the id
			// we would write affects no row, so it is not an import.
			if existing, ok := rc.watermarks[k]; ok && existing >= c.land.id {
				continue
			}
		}
		pl.rep.Watermarks++
		pl.watermarks = append(pl.watermarks,
			watermarkWrite{userSourceID: c.user, messageSourceID: c.sourceID})
	}
}

func copyStringInt(src map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}

func copyStringBool(src map[string]bool) map[string]bool {
	out := make(map[string]bool, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}
