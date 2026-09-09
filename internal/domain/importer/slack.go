package importer

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// The Slack loader. It parses an UNPACKED Slack export directory and projects
// it onto the source-neutral IR; everything Slack-shaped stops here, exactly
// as everything Zulip-shaped stops in zulip.go.
//
// Grounded in zerver/data_import/slack.py, which is the format authority for
// the export's shape. Where Weft's model is genuinely different the divergence
// is recorded at its site — threads, channel references, broadcast mentions,
// bot authors and the offline file convention are all such places.
//
// FULLY OFFLINE. The reference implementation reaches for users.list,
// bots.info, emoji.list and team.info, and downloads every attachment from
// files.slack.com. This loader reads the directory and nothing else: an
// operator's pre-fetch step is what puts bytes on disk (see resolveUploads).

// originSlack is this loader's provenance token. Like Zulip's, it reaches the
// database and the operator's eyes only as Import.Source.
const originSlack = "slack"

// --- Export shapes (field names are Slack's own) ---

type slackProfile struct {
	Email       string `json:"email"`
	RealName    string `json:"real_name"`
	DisplayName string `json:"display_name"`
}

type slackUser struct {
	ID                string       `json:"id"`
	Name              string       `json:"name"`
	RealName          string       `json:"real_name"`
	Deleted           bool         `json:"deleted"`
	IsBot             bool         `json:"is_bot"`
	IsOwner           bool         `json:"is_owner"`
	IsPrimaryOwner    bool         `json:"is_primary_owner"`
	IsAdmin           bool         `json:"is_admin"`
	IsRestricted      bool         `json:"is_restricted"`
	IsUltraRestricted bool         `json:"is_ultra_restricted"`
	Profile           slackProfile `json:"profile"`
}

// displayName is the name a human would recognise. The reference's own helper
// answers `name` (the handle) for a deleted account and for anyone whose
// profile carries a real name, which reads as a bug rather than a decision;
// this prefers what Slack shows in the client and falls back the same way.
func (u slackUser) displayName() string {
	for _, s := range []string{u.Profile.RealName, u.RealName, u.Profile.DisplayName, u.Name} {
		if s != "" {
			return s
		}
	}
	return u.ID
}

// weftSlackRole maps Slack's account flags onto Weft role presets. Guest is
// checked LAST and wins, mirroring the reference: Slack's multi-channel and
// single-channel guests both set is_restricted, and the P-5 ceiling puts both
// on the guest preset.
func weftSlackRole(u slackUser) int16 {
	role := int16(40)
	if u.IsOwner || u.IsPrimaryOwner {
		role = 10
	} else if u.IsAdmin {
		role = 20
	}
	if u.IsRestricted || u.IsUltraRestricted {
		role = 50
	}
	return role
}

// isBot follows the reference: the is_bot flag, plus Slackbot, which carries
// no flag and is identified by name.
func (u slackUser) isBot() bool {
	return u.IsBot || strings.EqualFold(u.Name, "slackbot") ||
		strings.EqualFold(u.RealName, "slackbot")
}

type slackTextValue struct {
	Value string `json:"value"`
}

// slackConversation covers channels.json, groups.json, mpims.json and
// dms.json — the four files differ in what they mean, not in their shape.
type slackConversation struct {
	ID         string         `json:"id"`
	Name       string         `json:"name"`
	Created    int64          `json:"created"`
	IsArchived bool           `json:"is_archived"`
	Purpose    slackTextValue `json:"purpose"`
	Members    []string       `json:"members"`
}

type slackReaction struct {
	Name  string   `json:"name"`
	Users []string `json:"users"`
}

// slackFile is one upload record. `mode` and `file_access` are how a free-plan
// export admits the bytes are gone.
type slackFile struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	Title      string `json:"title"`
	MimeType   string `json:"mimetype"`
	Timestamp  int64  `json:"timestamp"`
	User       string `json:"user"`
	Mode       string `json:"mode"`
	FileAccess string `json:"file_access"`
	URLPrivate string `json:"url_private"`
}

type slackMessage struct {
	Type     string `json:"type"`
	Subtype  string `json:"subtype"`
	TS       string `json:"ts"`
	ThreadTS string `json:"thread_ts"`
	User     string `json:"user"`
	BotID    string `json:"bot_id"`
	Text     string `json:"text"`
	MimeType string `json:"mimetype"`
	// A `file_share` carries its upload in `file`; everything else uses
	// `files`. Both are read.
	File      *slackFile      `json:"file"`
	Files     []slackFile     `json:"files"`
	Reactions []slackReaction `json:"reactions"`
}

// sender follows the reference's own precedence. A bot_id sender has no
// users.json row, so it maps to nothing and its message becomes a counted
// channel-message loss — the CURRENT skip-and-count, which P-27d revisits.
func (m slackMessage) sender() string {
	if m.User != "" {
		return m.User
	}
	if m.File != nil && m.File.User != "" {
		return m.File.User
	}
	return m.BotID
}

// systemNoticeSubtypes are the container events Slack stores AS MESSAGES.
// Weft records joins, leaves and renames on the event log, so importing them
// as conversation would put administrative noise in everyone's history. The
// reference drops the same set; here they are also COUNTED.
var systemNoticeSubtypes = map[string]bool{
	"channel_join": true, "channel_leave": true, "channel_name": true,
	"channel_purpose": true, "channel_topic": true, "channel_archive": true,
	"channel_unarchive": true, "group_join": true, "group_leave": true,
	"pinned_item": true, "unpinned_item": true,
}

// --- The parsed export ---

// slackContainer is one conversation directory, already classified.
type slackContainer struct {
	id      string
	dir     string // the directory name the messages live under
	kind    ContainerKind
	members []string
}

// slackLanded is the newest message a (container, thread) pair can land, kept
// with its ordinal so the running maximum costs no rescan.
type slackLanded struct {
	sourceID string
	ordinal  int64
}

type slackPlacedMessage struct {
	container slackContainer
	msg       slackMessage
}

// SlackExport is an unpacked export, parsed and indexed for the projection.
type SlackExport struct {
	Dir    string
	Source string

	Users    []slackUser
	Channels []slackConversation // channels.json + groups.json, in that order
	// private marks which of Channels came from groups.json.
	private  map[string]bool
	MPIMs    []slackConversation
	DMs      []slackConversation
	Messages []slackPlacedMessage

	// uploads maps a Slack file id to the file on disk that holds its bytes.
	// Resolved ONCE, at load time, so an ambiguous directory is a hard error
	// rather than a silent choice made per attachment (see resolveUploads).
	uploads map[string]string
}

// LoadSlackExport reads an UNPACKED Slack export directory.
//
// UNPACKED, NEVER THE ZIP — and that is forced, not stylistic: archive/zip's
// File.Open returns a NON-seekable reader, while the IR's attachment contract
// is io.ReadSeekCloser because storage keys are content-addressed (the lane
// hashes the stream, rewinds, and hands the same one to blob.Put). Unpacking
// stays the operator's step, exactly as it is for Zulip.
func LoadSlackExport(dir string) (*SlackExport, error) {
	ex := &SlackExport{
		Dir: dir, Source: originSlack,
		private: map[string]bool{},
		uploads: map[string]string{},
	}
	if err := readJSON(filepath.Join(dir, "users.json"), &ex.Users); err != nil {
		return nil, fmt.Errorf("importer: users.json: %w", err)
	}
	var public []slackConversation
	if err := readJSON(filepath.Join(dir, "channels.json"), &public); err != nil {
		return nil, fmt.Errorf("importer: channels.json: %w", err)
	}
	ex.Channels = append(ex.Channels, public...)
	// groups.json / mpims.json / dms.json are all optional: a workspace with
	// no private channels simply has no file, and the reference treats a
	// missing one the same way.
	var private []slackConversation
	if err := readOptionalJSON(filepath.Join(dir, "groups.json"), &private); err != nil {
		return nil, fmt.Errorf("importer: groups.json: %w", err)
	}
	for _, g := range private {
		ex.private[g.ID] = true
	}
	ex.Channels = append(ex.Channels, private...)
	if err := readOptionalJSON(filepath.Join(dir, "mpims.json"), &ex.MPIMs); err != nil {
		return nil, fmt.Errorf("importer: mpims.json: %w", err)
	}
	if err := readOptionalJSON(filepath.Join(dir, "dms.json"), &ex.DMs); err != nil {
		return nil, fmt.Errorf("importer: dms.json: %w", err)
	}

	// Conversation directories. Channels and MPIMs are stored under their
	// NAME and 1:1 DMs under their ID — the reference's own directory list,
	// and the reason the message origin id below is keyed on the id instead.
	var containers []slackContainer
	for _, c := range ex.Channels {
		containers = append(containers, slackContainer{
			id: c.ID, dir: c.Name, kind: ContainerChannel, members: c.Members})
	}
	for _, c := range append(append([]slackConversation{}, ex.MPIMs...), ex.DMs...) {
		name := c.Name
		if name == "" {
			name = c.ID
		}
		containers = append(containers, slackContainer{
			id: c.ID, dir: name, kind: ContainerDirect, members: c.Members})
	}
	for _, c := range containers {
		msgs, err := readConversationDir(filepath.Join(dir, c.dir))
		if err != nil {
			return nil, err
		}
		for _, m := range msgs {
			ex.Messages = append(ex.Messages, slackPlacedMessage{container: c, msg: m})
		}
	}

	if err := ex.resolveUploads(); err != nil {
		return nil, err
	}
	return ex, nil
}

// readConversationDir reads one conversation's dated message files in name
// order. A conversation the export lists but ships no directory for is not an
// error — a channel with no history is ordinary.
func readConversationDir(dir string) ([]slackMessage, error) {
	names, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	var out []slackMessage
	for _, n := range names {
		// Slack drops canvases into conversation directories under this name;
		// they are not messages and the reference does not parse them either.
		if filepath.Base(n) == "canvas_in_the_conversation.json" {
			continue
		}
		var msgs []slackMessage
		if err := readJSON(n, &msgs); err != nil {
			return nil, fmt.Errorf("importer: %s: %w", filepath.Base(n), err)
		}
		out = append(out, msgs...)
	}
	return out, nil
}

// resolveUploads indexes the pre-fetched attachment bytes.
//
// THE LAYOUT IS `__uploads/<slack_file_id>/<filename>`, rooted at the same
// unpacked directory channels.json lives in. That is byte-identical to what
// slack-advanced-exporter and slackdump already write, so an operator gets a
// working pre-fetch step from tooling that exists, and Weft ships no fetcher
// and no credential surface.
//
// RESOLUTION IS BY ID; THE FILENAME IS COSMETIC. Comparing the on-disk name to
// the JSON `name` would reintroduce every unicode, slash and dedup
// sanitization mismatch between what Slack stored and what a filesystem can
// hold. Zero matches is a skipped-with-reason attachment (the export is
// truncated); two or more is a HARD, NAMED error, because ambiguity must never
// silently pick a file.
func (ex *SlackExport) resolveUploads() error {
	root := filepath.Join(ex.Dir, "__uploads")
	entries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // a metadata-only export: every attachment is a counted loss
		}
		return fmt.Errorf("importer: __uploads: %w", err)
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		inner, err := os.ReadDir(filepath.Join(root, e.Name()))
		if err != nil {
			return fmt.Errorf("importer: __uploads/%s: %w", e.Name(), err)
		}
		var files []string
		for _, f := range inner {
			if f.Type().IsRegular() {
				files = append(files, f.Name())
			}
		}
		switch len(files) {
		case 0:
			continue
		case 1:
			ex.uploads[e.Name()] = filepath.Join(root, e.Name(), files[0])
		default:
			sort.Strings(files)
			return fmt.Errorf("importer: __uploads/%s holds %d files (%s) — a Slack "+
				"file id names exactly one upload, and this import will not guess which",
				e.Name(), len(files), strings.Join(files, ", "))
		}
	}
	return nil
}

// --- Projection onto the IR ---

// toImport projects the parsed export onto the source-neutral IR. Every Slack
// concept stops here.
//
// The two composite keys below are WIRE CONTRACTS and must stay byte-identical
// across versions, or a re-import stops recognising what it created last time
// and duplicates instead of counting AlreadyImported. Both are keyed on the
// conversation's ID rather than its name, so a rename does not break a re-run:
//
//	message origin id  "<conversation id>:<ts>"   (ts is conversation-scoped)
//	thread  origin id  "thread:<channel id>:<thread_ts>"
func (ex *SlackExport) toImport() *Import {
	ir := &Import{Source: ex.Source}

	dialect := slackDialect{
		userName:    map[string]string{},
		channelName: map[string]string{},
	}
	human := map[string]bool{} // source user id → maps to a real account
	// Slack's users.json carries no join date. The workspace's earliest
	// channel creation is the closest thing the EXPORT knows, it is
	// deterministic, and it is never later than the history it has to precede.
	joined := ex.workspaceFloor()
	for _, u := range ex.Users {
		name := u.displayName()
		dialect.userName[u.ID] = name
		bot := u.isBot()
		human[u.ID] = !bot
		ir.Users = append(ir.Users, User{
			SourceID:    u.ID,
			Email:       u.Profile.Email,
			DisplayName: name,
			Role:        weftSlackRole(u),
			Active:      !u.Deleted,
			Bot:         bot,
			JoinedAt:    joined,
		})
	}

	for _, c := range ex.Channels {
		visibility := int16(1)
		if ex.private[c.ID] {
			visibility = 2
		}
		dialect.channelName[c.ID] = c.Name
		ir.Channels = append(ir.Channels, Channel{
			SourceID:    c.ID,
			Name:        c.Name,
			Description: c.Purpose.Value,
			Visibility:  visibility,
			Archived:    c.IsArchived,
			CreatedAt:   time.Unix(c.Created, 0).UTC(),
		})
		for _, uid := range dedupeStrings(c.Members) {
			ir.Memberships = append(ir.Memberships,
				Membership{ChannelID: c.ID, UserID: uid})
		}
	}
	for _, c := range append(append([]slackConversation{}, ex.MPIMs...), ex.DMs...) {
		// A self-DM lists the same member twice; a duplicate would widen the
		// canonical key onto a different real conversation.
		ir.Conversations = append(ir.Conversations,
			Conversation{SourceKey: c.ID, MemberIDs: dedupeStrings(c.Members)})
	}

	// Attachments, and the upload dialect that turns their links into managed
	// ones. Slack's own link is the signed url_private, so that is the needle.
	urlByFile := map[string]string{}
	seenFile := map[string]bool{}
	// last[container][threadKey] = the newest message the import can land
	// there — the synthetic read watermark's target (D2).
	last := map[string]map[string]slackLanded{}

	for _, pm := range ex.Messages {
		m, c := pm.msg, pm.container
		if m.Type != "" && m.Type != "message" {
			continue
		}
		if systemNoticeSubtypes[m.Subtype] {
			ir.Losses.SystemNoticesDropped++
			continue
		}
		// Slack "Posts" are HTML documents wearing a message; the reference
		// skips them and so does this, counted with the other notices rather
		// than silently.
		if m.MimeType == "application/vnd.slack-docs" {
			ir.Losses.SystemNoticesDropped++
			continue
		}
		sender := m.sender()
		// "U00" appears in some export-generated rename messages and is not a
		// user; the reference skips it by name.
		if sender == "" || sender == "U00" {
			ir.Losses.SystemNoticesDropped++
			continue
		}
		ordinal, sentAt, ok := slackTS(m.TS)
		if !ok {
			ir.Losses.SystemNoticesDropped++
			continue
		}
		if m.Subtype == "thread_broadcast" {
			// A reply Slack ALSO showed in the flat feed. Weft has one place
			// for a message: it lands in its thread, once, and the double
			// placement is counted.
			ir.Losses.BroadcastsFlattened++
		}

		body, mentions, chanRefs := dialect.convertBody(m.Text)
		msg := Message{
			SourceID:              c.id + ":" + m.TS,
			Ordinal:               ordinal,
			AuthorID:              sender,
			Container:             Container{Kind: c.kind, Key: c.id},
			Body:                  body,
			SentAt:                sentAt,
			MentionsBySourceID:    mentions,
			ChannelRefsBySourceID: chanRefs,
		}
		if c.kind == ContainerChannel {
			msg.Thread = slackThread(c.id, m)
		}

		// Files. A `file_share` carries its upload in `file`, everything else
		// in `files`; both are read.
		files := m.Files
		if m.File != nil {
			files = append(append([]slackFile{}, files...), *m.File)
		}
		var links []string
		// Deduped WITHIN the message: a `file_share` puts its upload in
		// `file`, but nothing stops an export from listing the same id in
		// `files` too, and the body would then carry the link twice.
		inMessage := map[string]bool{}
		for _, f := range files {
			if f.ID == "" || inMessage[f.ID] {
				continue
			}
			inMessage[f.ID] = true
			// The source itself says the bytes are gone: `tombstone` and
			// `hidden_by_limit` are the free-plan cap, and the access stubs are
			// files Slack declares unreachable. No re-export recovers these, so
			// they are their own counted loss and never an attachment row.
			if f.Mode == "tombstone" || f.Mode == "hidden_by_limit" ||
				f.FileAccess == "access_denied" || f.FileAccess == "file_not_found" {
				ir.Losses.AttachmentBytesExpired++
				continue
			}
			name := f.Name
			if name == "" {
				name = f.Title
			}
			if name == "" {
				name = f.ID
			}
			if f.URLPrivate != "" {
				links = append(links, "["+name+"]("+f.URLPrivate+")")
			}
			if !slackHostedFile(f) {
				// An external link (a Drive file, say) is a link and nothing
				// more — there are no bytes to fetch and none to lose.
				continue
			}
			ir.AttachmentRefs = append(ir.AttachmentRefs,
				AttachmentRef{AttachmentID: f.ID, MessageID: msg.SourceID})
			if seenFile[f.ID] {
				continue
			}
			seenFile[f.ID] = true
			urlByFile[f.ID] = f.URLPrivate
			att := Attachment{
				SourceID:  f.ID,
				Name:      name,
				MIME:      f.MimeType,
				OwnerID:   f.User,
				CreatedAt: time.Unix(f.Timestamp, 0).UTC(),
			}
			att.Probe, att.Open = ex.attachmentBytes(f.ID)
			ir.Attachments = append(ir.Attachments, att)
		}
		if len(links) > 0 {
			msg.Body = strings.TrimSpace(msg.Body + "\n" + strings.Join(links, "\n"))
		}

		for _, r := range m.Reactions {
			// Skin-tone variants encode as `clap::skin-tone-2`; Weft stores the
			// base name, and two variants from one person dedupe on insert.
			emoji := r.Name
			if i := strings.Index(emoji, "::"); i >= 0 {
				emoji = emoji[:i]
			}
			for _, uid := range r.Users {
				ir.Reactions = append(ir.Reactions,
					Reaction{UserID: uid, MessageID: msg.SourceID, Emoji: emoji})
			}
		}

		ir.Messages = append(ir.Messages, msg)
		if human[sender] {
			key := ""
			if msg.Thread != nil {
				key = msg.Thread.Key
			}
			if last[c.id] == nil {
				last[c.id] = map[string]slackLanded{}
			}
			if prev, seen := last[c.id][key]; !seen || prev.ordinal < ordinal {
				last[c.id][key] = slackLanded{sourceID: msg.SourceID, ordinal: ordinal}
			}
		}
	}

	ir.RewriteAttachmentLinks = func(body string, fileIDs map[string]int64) (string, bool) {
		return rewriteSlackUploads(body, urlByFile, fileIDs)
	}
	ex.seedReadState(ir, last, human)
	return ir
}

// slackThread decides which thread a CHANNEL message belongs to.
//
// The discriminator is Slack's own, from its threading documentation: a parent
// carries thread_ts == ts and a reply carries thread_ts != ts. `thread_ts` is
// on PARENTS TOO, so "has thread_ts ⇒ is a reply" would make every parent its
// own reply and scatter one conversation across as many threads as it has
// messages.
//
// A message with no thread_ts at all is unthreaded and lands on the channel's
// FLAT FEED (Thread.Root) — the same kind=2 root Weft's own send path uses for
// a channel message with no thread, and where ~90% of a Slack channel lives.
//
// Weft's threads take thread_ts DIRECTLY as their identity, and the thread is
// UNTITLED — which ADR-001 D1 calls the Slack-thread shape. The reference has
// to synthesize a topic NAME out of the date, a content snippet and a collision
// counter because Zulip keys conversations by topic string; Weft keys them by
// id, so porting that workaround would invent a title nobody wrote.
func slackThread(channelID string, m slackMessage) *Thread {
	if m.ThreadTS == "" {
		return &Thread{Key: "root:" + channelID, Root: true}
	}
	return &Thread{
		Key:      "thread:" + channelID + "\x00" + m.ThreadTS,
		SourceID: "thread:" + channelID + ":" + m.ThreadTS,
	}
}

// seedReadState mints the synthetic read state that makes imported history
// arrive READ (D2).
//
// A Slack export carries no per-user read state, so the alternative to minting
// one is not "no badges": with no watermark the first LIVE message creates a
// counter row, the S6 reconcile sweep then recomputes it from the same
// `m.id > COALESCE(w.last_read_message_id, 0)` aggregate, repairs it to the
// FULL imported history and Warn-logs a divergence that is not one. The
// counter is a documented cache and the watermark is truth, so read state has
// to be fixed AT THE WATERMARK.
//
// Vendor precedent, as a bug FIX rather than a taste call: mmetl (the official
// Slack→Mattermost ETL) PR #88 does exactly this, and Zulip's single
// UserMessage builder hardcodes the read flag for Slack, Mattermost,
// Rocket.Chat and Teams alike. The tell is that remediation docs exist only
// where the code did not — Mattermost ships a "fixing unread channels" SQL
// block, Gitter→Matrix tells users to hit "mark all as read".
//
// One ReadState row per (member, thread), not per (member, message): the
// planner reduces to the highest LANDED message per (user, thread) anyway, so
// naming that message directly is the same answer for a fraction of the rows.
// The target must be a message the import can actually land, or the planner
// drops the pair and the member keeps a full badge — hence the human-author
// filter its caller applies.
func (ex *SlackExport) seedReadState(ir *Import, last map[string]map[string]slackLanded, human map[string]bool) {
	type container struct {
		id      string
		members []string
	}
	var all []container
	for _, c := range ex.Channels {
		all = append(all, container{c.ID, c.Members})
	}
	for _, c := range append(append([]slackConversation{}, ex.MPIMs...), ex.DMs...) {
		all = append(all, container{c.ID, c.Members})
	}
	for _, c := range all {
		threads := last[c.id]
		if len(threads) == 0 {
			continue
		}
		keys := make([]string, 0, len(threads))
		for k := range threads {
			keys = append(keys, k)
		}
		sort.Strings(keys) // deterministic emission order
		for _, uid := range dedupeStrings(c.members) {
			if !human[uid] {
				continue
			}
			for _, k := range keys {
				ir.ReadState = append(ir.ReadState,
					ReadState{UserID: uid, MessageID: threads[k].sourceID, Read: true})
			}
		}
	}
}

// attachmentBytes builds the probe/open pair for one Slack file id against the
// pre-fetched __uploads tree. A file the tree does not carry answers the same
// "the bytes are not there" the write path already counts as a missing
// attachment.
func (ex *SlackExport) attachmentBytes(fileID string) (func() (int64, error), func() (io.ReadSeekCloser, error)) {
	path, ok := ex.uploads[fileID]
	if !ok {
		absent := fmt.Errorf("importer: no bytes for Slack file %s under __uploads/", fileID)
		return func() (int64, error) { return 0, absent },
			func() (io.ReadSeekCloser, error) { return nil, absent }
	}
	return func() (int64, error) {
			fi, err := os.Stat(path)
			if err != nil {
				return 0, err
			}
			return fi.Size(), nil
		}, func() (io.ReadSeekCloser, error) {
			return os.Open(path)
		}
}

// rewriteSlackUploads swaps the signed url_private links this loader wrote
// into a body for managed-file URLs. Same shape as the Zulip rewriter: a scan
// over the LANDED attachments, so a file whose bytes were missing keeps its
// original (broken) link rather than pointing at nothing.
func rewriteSlackUploads(body string, urlByFile map[string]string, fileIDs map[string]int64) (string, bool) {
	if len(fileIDs) == 0 {
		return body, false
	}
	changed := false
	for fileSourceID, fileID := range fileIDs {
		url := urlByFile[fileSourceID]
		if url == "" || !strings.Contains(body, url) {
			continue
		}
		body = strings.ReplaceAll(body, url, fmt.Sprintf("/api/v1/files/%d", fileID))
		changed = true
	}
	return body, changed
}

// slackHostedFile reports whether Slack itself holds the bytes. Anything else
// (a Drive or Box integration) is a link the message already carries and never
// an attachment: there is nothing on disk to import and nothing to lose.
func slackHostedFile(f slackFile) bool {
	return strings.HasPrefix(f.URLPrivate, "https://files.slack.com/") ||
		strings.HasPrefix(f.URLPrivate, "http://files.slack.com/")
}

// slackTS parses a Slack timestamp ("1550000000.000200"), which is both the
// message's clock and — scoped to its conversation — its identity.
//
// The ordinal is MICROSECONDS, parsed as two integers rather than through a
// float: a float64 cannot hold a microsecond-resolution epoch exactly, and two
// messages a microsecond apart must not collide or swap. This is the whole
// reason the IR carries an explicit ordinal — a `>` on these strings would
// order "1550000000.000200" after "1550000001.0".
func slackTS(ts string) (int64, time.Time, bool) {
	whole, frac, _ := strings.Cut(ts, ".")
	sec, err := strconv.ParseInt(whole, 10, 64)
	if err != nil {
		return 0, time.Time{}, false
	}
	if len(frac) > 6 {
		frac = frac[:6]
	}
	for len(frac) < 6 {
		frac += "0"
	}
	micros, err := strconv.ParseInt(frac, 10, 64)
	if err != nil {
		return 0, time.Time{}, false
	}
	return sec*1_000_000 + micros, time.Unix(sec, micros*1000).UTC(), true
}

// workspaceFloor is the earliest conversation creation the export knows.
func (ex *SlackExport) workspaceFloor() time.Time {
	var floor int64
	for _, c := range ex.Channels {
		if c.Created > 0 && (floor == 0 || c.Created < floor) {
			floor = c.Created
		}
	}
	return time.Unix(floor, 0).UTC()
}

func dedupeStrings(in []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(in))
	for _, s := range in {
		if s == "" || seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	return out
}

// readOptionalJSON reads a file the export may legitimately not ship.
func readOptionalJSON(path string, v any) error {
	err := readJSON(path, v)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}
