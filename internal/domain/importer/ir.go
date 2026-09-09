package importer

import (
	"io"
	"sort"
	"time"
)

// The source-neutral intermediate representation. ADR-003 E5 and
// ARCHITECTURE.md have both described this module as "source adapters → IR →
// domain writers" since before it was true: the write path consumed a
// Zulip-shaped Export, so a second source would have meant a second importer.
// These types are the seam. A loader parses ONE source and projects it onto
// them; write() consumes only them and never learns which source it is
// draining.
//
// Two rules the types enforce rather than document:
//
//   - SOURCE IDS ARE `string`, EVERYWHERE. Zulip's are integers, Slack's are
//     opaque tokens ("U024BE7LH", "C123"). Typing them `any` would leave the
//     write path's `%d` formats compiling and silently emitting
//     `%!d(string=C123)` into a provenance column; `string` turns each one
//     into a diagnosable error at build time. Nothing here ever does
//     arithmetic on a source id — where the write path needs an ORDER it
//     uses Message.Ordinal, which is a separate field for exactly that
//     reason.
//   - ORDER IS EXPLICIT. A loader is not required to emit messages sorted;
//     the write path sorts by Ordinal. Zulip's ids happen to ascend with
//     time, but Slack's `ts` strings have no `>` that means "later", so an
//     implicit "the loader emits them in order" contract would break
//     silently at the second source. Four things depend on this order: which
//     message becomes a thread's root, the event log replaying ≈ the
//     original history, the read-watermark reducer, and the per-topic
//     last_activity_at.
//
// Meta on each entity is the reserved carrier for the `origin_meta JSONB`
// column that exists on all six importer-written tables and has zero writers
// tree-wide. No loader populates it and no writer drains it yet; it is
// declared here so a second loader inherits a shape instead of inventing one.
type Import struct {
	// Source is the provenance token stamped into every origin_system column
	// and into the visible collision-rename suffix ("general-zulip1"). It
	// travels with the data rather than being a constant in the write path,
	// which is what lets one write path serve several loaders.
	Source string

	Users          []User
	Channels       []Channel
	Memberships    []Membership
	Groups         []Group
	GroupMembers   []GroupMembership
	GroupEdges     []GroupEdge
	Conversations  []Conversation
	Attachments    []Attachment
	AttachmentRefs []AttachmentRef
	Messages       []Message
	Reactions      []Reaction
	ReadState      []ReadState

	// RewriteAttachmentLinks turns the SOURCE's own upload links inside a
	// message body into managed-file URLs. The dialect belongs to the loader
	// (Zulip writes /user_uploads/<path_id>; Slack writes signed URLs) and
	// the file ids belong to the write path, so this hook is the seam
	// between them: fileIDs is keyed by Attachment.SourceID and holds only
	// the attachments that actually landed.
	//
	// The bool is has_attachment: whether the body now points at a managed
	// file a reader can open. It is deliberately NOT a field on Message,
	// because its value is only knowable after the write path knows which
	// attachments landed — a body linking bytes that were missing from the
	// export gets false, and the link stays broken.
	//
	// nil means "this source has no upload dialect": bodies pass through.
	RewriteAttachmentLinks func(body string, fileIDs map[string]int64) (string, bool)

	// Losses the LOADER observed. See the type.
	Losses Losses
}

// Losses is the accounting only a LOADER can contribute, because the fact is
// visible while PARSING and nowhere afterwards: the planner sees the IR, and
// an entity the loader dropped is not in it. The planner folds these into the
// report verbatim, so the dry run and the write report them identically and
// the fidelity contract — every source entity lands in exactly one bucket —
// still holds for the entities that never reach the write path.
//
// The vocabulary stays source-neutral, like every other name here. Anything
// whose value depends on what the WRITE lands belongs in the planner instead:
// a loader cannot know that.
type Losses struct {
	// AttachmentBytesExpired: the SOURCE itself says the bytes are gone —
	// Slack's `mode: tombstone` / `hidden_by_limit` (the free-plan cap) and
	// its access-denied stubs. Deliberately not folded into
	// AttachmentFilesMissing, which means "the export tree does not carry a
	// file it referenced": that one is fixed by re-exporting and this one
	// never can be.
	AttachmentBytesExpired int
	// BroadcastsFlattened: a reply the source ALSO showed in the container's
	// flat feed (Slack's `thread_broadcast`). Weft has one place for a
	// message, so it lands once, in its thread, and the double placement is
	// counted rather than silently halved.
	BroadcastsFlattened int
	// SystemNoticesDropped: join/leave/pin/rename notices the source stores
	// AS MESSAGES. They are container events, not conversation, and Weft
	// keeps those on the event log — dropped on purpose, counted so a
	// message-count difference against the source has an explanation.
	SystemNoticesDropped int
}

// User is one source account. Role is already a WEFT preset (10 owner · 20
// admin · 30 moderator · 40 member · 50 guest): mapping the source's own role
// vocabulary is the loader's job. Email may be absent, and an absent email is
// NOT a match key — the write path stores SQL NULL and never aliases two
// emailless users onto one account.
type User struct {
	SourceID    string
	Email       string
	DisplayName string
	Role        int16
	Active      bool
	Bot         bool
	JoinedAt    time.Time
	Meta        map[string]any
}

// Channel is one source channel. Visibility is Weft's (1 public · 2 private;
// 3 web-public exists in the schema and no loader emits it yet).
type Channel struct {
	SourceID    string
	Name        string
	Description string
	Visibility  int16
	Archived    bool
	CreatedAt   time.Time
	Meta        map[string]any
}

// Membership is one channel membership, already filtered to the ones that
// should become channel_member rows (the loader drops inactive or
// non-channel subscriptions rather than making the write path re-derive
// what a subscription meant in the source).
type Membership struct {
	ChannelID string // Channel.SourceID
	UserID    string // User.SourceID
}

// Group is one named user group. When System is true the group is NOT
// created: SystemName, if non-empty, names the SEEDED Weft group it maps
// onto, and an empty SystemName means the source group has no counterpart
// and is dropped.
type Group struct {
	SourceID    string
	Name        string
	Description string
	System      bool
	SystemName  string
	Deactivated bool
	CreatedAt   time.Time // zero → now()
	Meta        map[string]any
}

type GroupMembership struct {
	GroupID string // Group.SourceID
	UserID  string // User.SourceID
}

// GroupEdge: GroupID CONTAINS SubgroupID.
type GroupEdge struct {
	GroupID    string
	SubgroupID string
}

// Conversation is a direct-message container with an EXPLICIT participant
// set. Zulip reaches it through a recipient indirection and Slack through
// dms.json/mpims.json; neither concept survives into the write path, which
// only ever asks "who is in this conversation". MemberIDs is deduped — a
// self-DM is one participant, and a duplicate would widen the canonical key
// onto a different real conversation.
type Conversation struct {
	SourceKey string   // matches Container.Key on its messages
	MemberIDs []string // User.SourceID
}

// ContainerKind says which lane a message lands in.
type ContainerKind int

const (
	ContainerChannel ContainerKind = 1
	ContainerDirect  ContainerKind = 2
)

// Container binds a message to exactly one place. Key is a Channel.SourceID
// for ContainerChannel and a Conversation.SourceKey for ContainerDirect; a
// direct message whose conversation the loader could not resolve carries an
// empty Key and is counted as an unmappable-participant loss.
type Container struct {
	Kind ContainerKind
	Key  string
}

// Thread is the conversation grouping INSIDE a container: Zulip's topic,
// Slack's thread_ts parent. Every message of one topic shares one *Thread.
//
// SourceID is the provenance key the thread row is written under, and it is
// a WIRE CONTRACT: change its shape and a post-upgrade re-import stops
// recognising the threads it created last time and duplicates them instead of
// counting AlreadyImported. Key is the loader's in-run grouping key and may
// differ (Zulip's uses a NUL separator that cannot collide, where the
// provenance key uses a colon that in principle could).
type Thread struct {
	Key      string
	SourceID string
	Title    string
	// Root routes the message to its CONTAINER's root thread — the flat
	// channel feed — instead of a thread of its own. That row already exists
	// (the channel lane creates it with the channel), so nothing is created,
	// nothing is counted, and SourceID/Title are ignored.
	//
	// It is a distinct flag rather than "a nil Thread means the root",
	// because a nil Thread means the LOADER FORGOT and that stays an error:
	// a channel message must always SAY where it goes. Slack needs this
	// because most of its channel messages are unthreaded, and it is where
	// Weft's own send path puts an unthreaded channel message
	// (messaging.go: threadID == 0 → channel.root_thread_id, kind 2). F-15
	// is what makes it safe: every bump, native and imported, is gated on
	// `kind = 1`, so a root never takes a counter or a root_message_id —
	// the last of which messaging/move.go reads WITHOUT filtering by kind
	// and would turn into "permanently unmovable".
	Root bool
	Meta map[string]any
}

// Message is one source message.
type Message struct {
	SourceID string
	// Ordinal orders history. See the package-level note: it exists so the
	// write path never has to interpret a source id.
	Ordinal   int64
	AuthorID  string // User.SourceID
	Container Container
	// Thread is required for ContainerChannel and nil for ContainerDirect,
	// whose messages land in their conversation's root thread.
	Thread *Thread
	// Body is already-normalized WEFT markdown. Translating the source's
	// markup dialect is the loader's job (one owner, per the LLD rule): the
	// write path re-renders through our engine and never guesses a dialect.
	Body   string
	SentAt time.Time
	Edits  []Edit
	// MentionsBySourceID maps a literal label appearing in Body to a source
	// user id, for dialects that mention people by opaque id (Slack's
	// <@U123>) rather than by name (Zulip's @**Full Name**). Both lanes
	// resolve against the same imported directory; a loader whose dialect
	// already names people leaves this nil and the display-name lane
	// answers.
	MentionsBySourceID map[string]string
	// ChannelRefsBySourceID maps a channel-reference label appearing in Body
	// to a source CHANNEL id — the same shape MentionsBySourceID uses for
	// people. Resolution goes through the ID and never the name, so a
	// reference written before a rename still points at the right channel,
	// and a label the loader did not pair with a source channel stays an
	// UNRESOLVED reference, which is still rendered (inert, label only).
	ChannelRefsBySourceID map[string]string
	Meta                  map[string]any
}

// Edit is one content revision, oldest first. An EMPTY EditorID means the
// source could not attribute it (Zulip's pre-2017 history has a null
// user_id): the write path counts it as a loss and never invents an author.
// Entries that are not content revisions at all — topic and channel moves —
// are the loader's to drop.
type Edit struct {
	EditorID string
	At       time.Time
	PrevBody string
}

// Attachment is one file, with its bytes behind an opaque opener so the IR
// never names a filesystem layout.
type Attachment struct {
	SourceID  string
	Name      string
	MIME      string // "" → application/octet-stream
	OwnerID   string // User.SourceID; unmatched → no uploader
	CreatedAt time.Time

	// Probe reports the bytes' size without reading them, so a caller that
	// only needs to know whether an export is truncated does not open every
	// blob. RECORDED, NOT FIXED: Probe and Open disagree on a DIRECTORY at
	// the path — Stat succeeds, and the read then fails and aborts the whole
	// import rather than counting one missing file.
	Probe func() (int64, error)
	// Open streams the bytes; the caller closes. The result must be
	// seekable because storage keys are content-addressed: the lane hashes
	// the stream, rewinds, and hands the same stream to blob.Put. A source
	// whose bytes are remote therefore has to materialize them first, which
	// is the pre-fetch convention the Slack loader still owes.
	Open func() (io.ReadSeekCloser, error)
	Meta map[string]any
}

// AttachmentRef is the source's authoritative message↔attachment m2m. It is
// what flags a message as carrying a file even when its body had no link.
type AttachmentRef struct {
	AttachmentID string
	MessageID    string
}

type Reaction struct {
	UserID    string
	MessageID string
	Emoji     string
}

// ReadState is one per-user-per-message read flag. Both polarities are
// carried: the unread rows are what make the F-7 coarsening COUNTABLE rather
// than silent.
type ReadState struct {
	UserID    string
	MessageID string
	Read      bool
}

// rewriteBody applies the loader's upload dialect, if it has one.
func (ir *Import) rewriteBody(body string, fileIDs map[string]int64) (string, bool) {
	if ir.RewriteAttachmentLinks == nil {
		return body, false
	}
	return ir.RewriteAttachmentLinks(body, fileIDs)
}

// sortByOrdinal puts history in order, in place and stably. Loaders are not
// required to emit sorted (see the Ordinal note): this is the one place the
// write path establishes the order everything downstream assumes.
func sortByOrdinal(msgs []Message) {
	sort.SliceStable(msgs, func(i, j int) bool { return msgs[i].Ordinal < msgs[j].Ordinal })
}

// mentionResolver turns a message's two mention lanes into the single
// label→user-id function the content engine takes. The by-source-id lane is
// consulted first and only for labels the loader explicitly paired with a
// source user; everything else resolves by display name.
func mentionResolver(m *Message, byName, bySourceID map[string]int64) func(string) (int64, bool) {
	return func(label string) (int64, bool) {
		if src, ok := m.MentionsBySourceID[label]; ok {
			if id, ok := bySourceID[src]; ok {
				return id, true
			}
		}
		id, ok := byName[label]
		return id, ok
	}
}

// channelRefResolver turns a message's channel-reference lane into the
// label→channel-id function the content engine takes. There is no by-name
// fallback on purpose: a reference resolves through the source id the loader
// paired with the label, or it stays unresolved. Resolving a bare label
// against live channel names would let a channel created AFTER the export
// capture a reference that never meant it.
func channelRefResolver(m *Message, bySourceID map[string]int64) func(string) (int64, bool) {
	return func(label string) (int64, bool) {
		src, ok := m.ChannelRefsBySourceID[label]
		if !ok {
			return 0, false
		}
		id, ok := bySourceID[src]
		return id, ok
	}
}
