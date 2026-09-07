// Package importer implements migration adapters (ADR-003 E5): source →
// intermediate representation → dry-run fidelity report → provenance-keyed
// idempotent writes with backdated timestamps (E3).
//
// The IR lives in ir.go and the write path consumes ONLY it, so a second
// source is a loader and not a second importer. Nothing outside this file is
// Zulip-shaped any more: the dry run and the write path share one planner
// (plan.go) over the IR, so a bucket has one meaning — what the write would
// LAND in THIS org — and one implementation.
//
// LLD note (ARCHITECTURE.md exception, tracked in REALITY.md): the importer
// writes owning-module tables directly in backfill mode. ADR-003 E4's
// "through domain services with backfill flags" is the convergence target —
// today the backfill semantics (no notifications, no automations, backdated
// events with actor_kind=importer) are satisfied because those consumers key
// off the event log, which this module feeds correctly.
package importer

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Zulip export structures — field names match zerver/lib/export.py output
// (the tarball's realm.json + chunked messages-*.json).

type zulipRealmFile struct {
	Users         []zulipUser         `json:"zerver_userprofile"`
	Streams       []zulipStream       `json:"zerver_stream"`
	Recipients    []zulipRecipient    `json:"zerver_recipient"`
	Subscriptions []zulipSubscription `json:"zerver_subscription"`
	NamedGroups   []zulipNamedGroup   `json:"zerver_namedusergroup"`
	GroupMembers  []zulipGroupMember  `json:"zerver_usergroupmembership"`
	GroupEdges    []zulipGroupEdge    `json:"zerver_groupgroupmembership"`
	Attachments   []zulipAttachment   `json:"zerver_attachment"`
	AttachmentMsg []zulipAttachMsg    `json:"zerver_attachment_messages"`
}

type zulipUser struct {
	ID            int64   `json:"id"`
	DeliveryEmail string  `json:"delivery_email"`
	Email         string  `json:"email"`
	FullName      string  `json:"full_name"`
	IsActive      bool    `json:"is_active"`
	IsBot         bool    `json:"is_bot"`
	Role          int     `json:"role"`
	DateJoined    float64 `json:"date_joined"`
}

// BestEmail is Zulip's email precedence: delivery_email is the real address
// and `email` may be a per-realm alias, so the delivery address wins when the
// export carries one. An export may carry NEITHER, and an absent email is not
// a match key — the write path guards for that.
func (u zulipUser) BestEmail() string {
	if u.DeliveryEmail != "" {
		return u.DeliveryEmail
	}
	return u.Email
}

// weftRole maps Zulip UserProfile.role constants to Weft role presets. The
// input domain is literally Zulip's (zerver/models/users.py: 100 owner, 200
// administrator, 300 moderator, 400 member, 600 guest), which is why the
// mapping belongs to the loader and not to the write path.
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

type zulipStream struct {
	ID          int64   `json:"id"`
	Name        string  `json:"name"`
	Description string  `json:"description"`
	InviteOnly  bool    `json:"invite_only"`
	Deactivated bool    `json:"deactivated"`
	DateCreated float64 `json:"date_created"`
}

// zulipRecipient routes messages: type 1 personal DM, 2 stream, 3 huddle
// (group DM). TypeID is the stream id for type 2.
type zulipRecipient struct {
	ID     int64 `json:"id"`
	Type   int   `json:"type"`
	TypeID int64 `json:"type_id"`
}

type zulipSubscription struct {
	ID          int64 `json:"id"`
	UserProfile int64 `json:"user_profile"`
	Recipient   int64 `json:"recipient"`
	Active      bool  `json:"active"`
}

// zulipNamedGroup is a zerver_namedusergroup row. Zulip keeps group names on
// NamedUserGroup (an MTI child of UserGroup); the exported "id" duplicates
// the usergroup pointer, i.e. it IS the group id memberships reference.
// Anonymous UserGroup rows (setting values) are deliberately not modeled.
type zulipNamedGroup struct {
	ID          int64    `json:"id"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	IsSystem    bool     `json:"is_system_group"`
	Deactivated bool     `json:"deactivated"`
	DateCreated *float64 `json:"date_created"`
}

type zulipGroupMember struct {
	UserProfile int64 `json:"user_profile"`
	UserGroup   int64 `json:"user_group"`
}

// zulipGroupEdge: supergroup CONTAINS subgroup (zerver_groupgroupmembership).
type zulipGroupEdge struct {
	Supergroup int64 `json:"supergroup"`
	Subgroup   int64 `json:"subgroup"`
}

// zulipUserMessage carries per-user flags; bit 0 of flags_mask is "read"
// (AbstractUserMessage.ALL_FLAGS order in zerver/models/messages.py).
type zulipUserMessage struct {
	UserProfile int64 `json:"user_profile"`
	Message     int64 `json:"message"`
	FlagsMask   int64 `json:"flags_mask"`
}

const umReadFlag = 1

// zulipAttachment mirrors zerver_attachment; the bytes live in the export's
// uploads/<path_id> tree, and message content links them as
// /user_uploads/<path_id>.
type zulipAttachment struct {
	ID          int64   `json:"id"`
	FileName    string  `json:"file_name"`
	PathID      string  `json:"path_id"`
	Owner       int64   `json:"owner"`
	Size        int64   `json:"size"`
	ContentType *string `json:"content_type"`
	CreateTime  float64 `json:"create_time"`
}

type zulipAttachMsg struct {
	Attachment int64 `json:"attachment"`
	Message    int64 `json:"message"`
}

// editEntry is one EditHistoryEvent (zerver/lib/types.py): only content
// edits carry prev_content; user_id is null for pre-2017 history.
type editEntry struct {
	UserID      *int64  `json:"user_id"`
	Timestamp   float64 `json:"timestamp"`
	PrevContent *string `json:"prev_content"`
}

// parseEditHistory decodes a message's edit_history JSON, oldest first.
func parseEditHistory(raw *string) []editEntry {
	if raw == nil || *raw == "" || *raw == "null" {
		return nil
	}
	var entries []editEntry
	if json.Unmarshal([]byte(*raw), &entries) != nil {
		return nil
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Timestamp < entries[j].Timestamp })
	return entries
}

type zulipMessageFile struct {
	Messages     []zulipMessage     `json:"zerver_message"`
	Reactions    []zulipReaction    `json:"zerver_reaction"`
	UserMessages []zulipUserMessage `json:"zerver_usermessage"`
}

type zulipMessage struct {
	ID          int64   `json:"id"`
	Sender      int64   `json:"sender"`
	Recipient   int64   `json:"recipient"`
	Subject     string  `json:"subject"` // the topic — becomes a titled Thread
	Content     string  `json:"content"`
	DateSent    float64 `json:"date_sent"`
	EditHistory *string `json:"edit_history"`
}

type zulipReaction struct {
	ID          int64  `json:"id"`
	UserProfile int64  `json:"user_profile"`
	Message     int64  `json:"message"`
	EmojiName   string `json:"emoji_name"`
}

// originZulip is this loader's provenance token. It reaches the database and
// the operator's eyes only as Import.Source — every origin_system column and
// the visible "-zulip1" collision-rename suffix come from there, so the write
// path holds no Zulip literal at all.
const originZulip = "zulip"

// Export is the parsed source, pre-indexed for the writer.
type Export struct {
	Dir           string // the unpacked export root (uploads/ lives here)
	Source        string // originZulip; travels onto Import.Source
	Users         []zulipUser
	Streams       []zulipStream
	Subscriptions []zulipSubscription
	NamedGroups   []zulipNamedGroup
	GroupMembers  []zulipGroupMember
	GroupEdges    []zulipGroupEdge
	Attachments   []zulipAttachment
	AttachmentMsg []zulipAttachMsg
	Messages      []zulipMessage
	Reactions     []zulipReaction
	UserMessages  []zulipUserMessage

	// recipient id → stream id (type 2 only).
	StreamByRecipient map[int64]int64
	// DM routing: type 1 (legacy personal) targets one user; type 3 (direct
	// message group — modern Zulip routes ALL DMs here, 1:1 included) gets
	// its participants from subscriptions on the recipient.
	PersonalTarget map[int64]int64   // recipient id → target user id
	DMGroupMembers map[int64][]int64 // recipient id → participant user ids
}

// LoadZulipExport reads an UNPACKED export directory (realm.json +
// messages-*.json). Tarball unpacking is the operator's step for now.
func LoadZulipExport(dir string) (*Export, error) {
	var realm zulipRealmFile
	if err := readJSON(filepath.Join(dir, "realm.json"), &realm); err != nil {
		return nil, fmt.Errorf("importer: realm.json: %w", err)
	}
	ex := &Export{
		Dir:               dir,
		Source:            originZulip,
		Users:             realm.Users,
		Streams:           realm.Streams,
		Subscriptions:     realm.Subscriptions,
		NamedGroups:       realm.NamedGroups,
		GroupMembers:      realm.GroupMembers,
		GroupEdges:        realm.GroupEdges,
		Attachments:       realm.Attachments,
		AttachmentMsg:     realm.AttachmentMsg,
		StreamByRecipient: map[int64]int64{},
		PersonalTarget:    map[int64]int64{},
		DMGroupMembers:    map[int64][]int64{},
	}
	groupRecipient := map[int64]bool{}
	for _, r := range realm.Recipients {
		switch r.Type {
		case 2:
			ex.StreamByRecipient[r.ID] = r.TypeID
		case 1:
			ex.PersonalTarget[r.ID] = r.TypeID
		case 3:
			groupRecipient[r.ID] = true
		}
	}
	for _, sub := range realm.Subscriptions {
		if groupRecipient[sub.Recipient] {
			ex.DMGroupMembers[sub.Recipient] = append(ex.DMGroupMembers[sub.Recipient], sub.UserProfile)
		}
	}

	chunks, err := filepath.Glob(filepath.Join(dir, "messages-*.json"))
	if err != nil {
		return nil, err
	}
	sort.Strings(chunks)
	if len(chunks) == 0 {
		return nil, fmt.Errorf("importer: no messages-*.json in %s", dir)
	}
	for _, f := range chunks {
		var mf zulipMessageFile
		if err := readJSON(f, &mf); err != nil {
			return nil, fmt.Errorf("importer: %s: %w", filepath.Base(f), err)
		}
		ex.Messages = append(ex.Messages, mf.Messages...)
		ex.Reactions = append(ex.Reactions, mf.Reactions...)
		ex.UserMessages = append(ex.UserMessages, mf.UserMessages...)
	}
	// Deterministic id order keeps event-log ordering ≈ original history.
	sort.Slice(ex.Messages, func(i, j int) bool { return ex.Messages[i].ID < ex.Messages[j].ID })
	return ex, nil
}

// toImport projects the parsed export onto the source-neutral IR. This is
// where every Zulip concept stops: the recipient indirection becomes explicit
// containers and conversations, role integers become Weft presets, flags
// masks become booleans, and the bytes behind an attachment become an opener.
//
// The two composite keys below are computed HERE and must stay
// byte-identical. The topic provenance key in particular is a wire contract:
// a re-import after an upgrade that changed its shape would fail to recognise
// the threads it created last time and duplicate them instead of counting
// AlreadyImported.
func (ex *Export) toImport() *Import {
	ir := &Import{Source: ex.Source}

	for _, u := range ex.Users {
		ir.Users = append(ir.Users, User{
			SourceID:    fmt.Sprint(u.ID),
			Email:       u.BestEmail(),
			DisplayName: u.FullName,
			Role:        weftRole(u.Role),
			Active:      u.IsActive,
			Bot:         u.IsBot,
			JoinedAt:    ts(u.DateJoined),
		})
	}

	for _, st := range ex.Streams {
		visibility := int16(1)
		if st.InviteOnly {
			visibility = 2
		}
		ir.Channels = append(ir.Channels, Channel{
			SourceID:    fmt.Sprint(st.ID),
			Name:        st.Name,
			Description: st.Description,
			Visibility:  visibility,
			Archived:    st.Deactivated,
			CreatedAt:   ts(st.DateCreated),
		})
	}

	// A subscription is channel membership only when it is ACTIVE and its
	// recipient is a stream; the type-3 rows are DM participation and belong
	// to Conversations, not here.
	for _, sub := range ex.Subscriptions {
		streamID, ok := ex.StreamByRecipient[sub.Recipient]
		if !ok || !sub.Active {
			continue
		}
		ir.Memberships = append(ir.Memberships, Membership{
			ChannelID: fmt.Sprint(streamID),
			UserID:    fmt.Sprint(sub.UserProfile),
		})
	}

	for _, g := range ex.NamedGroups {
		// An absent date_created stays the zero time, which the write path
		// turns into now() — distinct from a date_created of 0, which is a
		// real (epoch) timestamp the source asserted.
		created := time.Time{}
		if g.DateCreated != nil {
			created = ts(*g.DateCreated)
		}
		systemName := ""
		if g.IsSystem {
			systemName = zulipSystemGroup(g.Name)
		}
		ir.Groups = append(ir.Groups, Group{
			SourceID:    fmt.Sprint(g.ID),
			Name:        g.Name,
			Description: g.Description,
			System:      g.IsSystem,
			SystemName:  systemName,
			Deactivated: g.Deactivated,
			CreatedAt:   created,
		})
	}
	for _, m := range ex.GroupMembers {
		ir.GroupMembers = append(ir.GroupMembers, GroupMembership{
			GroupID: fmt.Sprint(m.UserGroup), UserID: fmt.Sprint(m.UserProfile)})
	}
	for _, e := range ex.GroupEdges {
		ir.GroupEdges = append(ir.GroupEdges, GroupEdge{
			GroupID: fmt.Sprint(e.Supergroup), SubgroupID: fmt.Sprint(e.Subgroup)})
	}

	// Attachments: bytes live at uploads/<path_id> and message bodies link
	// them as /user_uploads/<path_id>. Both facts stay inside this file.
	pathByAttachment := map[string]string{}
	for _, a := range ex.Attachments {
		id := fmt.Sprint(a.ID)
		pathByAttachment[id] = a.PathID
		mime := ""
		if a.ContentType != nil {
			mime = *a.ContentType
		}
		att := Attachment{
			SourceID:  id,
			Name:      a.FileName,
			MIME:      mime,
			OwnerID:   fmt.Sprint(a.Owner),
			CreatedAt: ts(a.CreateTime),
		}
		att.Probe, att.Open = ex.attachmentBytes(a.PathID)
		ir.Attachments = append(ir.Attachments, att)
	}
	for _, am := range ex.AttachmentMsg {
		ir.AttachmentRefs = append(ir.AttachmentRefs, AttachmentRef{
			AttachmentID: fmt.Sprint(am.Attachment), MessageID: fmt.Sprint(am.Message)})
	}
	ir.RewriteAttachmentLinks = func(body string, fileIDs map[string]int64) (string, bool) {
		return rewriteZulipUploads(body, pathByAttachment, fileIDs)
	}

	// Messages, and the containers they resolve to. Zulip ids ascend with
	// history, so they double as the ordinal.
	threads := map[string]*Thread{}
	seenConversation := map[string]bool{}
	for _, m := range ex.Messages {
		msg := Message{
			SourceID: fmt.Sprint(m.ID),
			Ordinal:  m.ID,
			AuthorID: fmt.Sprint(m.Sender),
			Body:     m.Content,
			SentAt:   ts(m.DateSent),
		}
		for _, e := range parseEditHistory(m.EditHistory) {
			// No prev_content means a topic or channel move, which is not a
			// message revision at all — dropped here rather than counted.
			if e.PrevContent == nil {
				continue
			}
			ed := Edit{At: ts(e.Timestamp), PrevBody: *e.PrevContent}
			if e.UserID != nil {
				ed.EditorID = fmt.Sprint(*e.UserID)
			}
			msg.Edits = append(msg.Edits, ed)
		}
		if streamID, ok := ex.StreamByRecipient[m.Recipient]; ok {
			msg.Container = Container{Kind: ContainerChannel, Key: fmt.Sprint(streamID)}
			key := fmt.Sprintf("%d\x00%s", streamID, m.Subject)
			th, ok := threads[key]
			if !ok {
				th = &Thread{
					Key:      key,
					SourceID: fmt.Sprintf("topic:%d:%s", streamID, m.Subject),
					Title:    m.Subject,
				}
				threads[key] = th
			}
			msg.Thread = th
		} else {
			// Not a stream recipient, so a direct message. An unresolvable
			// recipient leaves the container Key empty, which no conversation
			// can match — the write path counts it as an unmappable
			// participant loss, exactly as it did when it asked per message.
			msg.Container = Container{Kind: ContainerDirect}
			if ids, ok := dmParticipants(ex, m); ok {
				parts := make([]string, len(ids))
				for i, id := range ids {
					parts[i] = fmt.Sprint(id)
				}
				key := "dm:" + strings.Join(parts, ":")
				msg.Container.Key = key
				if !seenConversation[key] {
					seenConversation[key] = true
					ir.Conversations = append(ir.Conversations,
						Conversation{SourceKey: key, MemberIDs: parts})
				}
			}
		}
		ir.Messages = append(ir.Messages, msg)
	}

	for _, r := range ex.Reactions {
		ir.Reactions = append(ir.Reactions, Reaction{
			UserID:    fmt.Sprint(r.UserProfile),
			MessageID: fmt.Sprint(r.Message),
			Emoji:     r.EmojiName,
		})
	}
	for _, um := range ex.UserMessages {
		ir.ReadState = append(ir.ReadState, ReadState{
			UserID:    fmt.Sprint(um.UserProfile),
			MessageID: fmt.Sprint(um.Message),
			Read:      um.FlagsMask&umReadFlag != 0,
		})
	}
	return ir
}

// attachmentBytes builds the probe/open pair for one path_id. An empty or
// traversing path is refused outright — the export is naming a file it has no
// business naming — and the refusal reaches the write path as the same
// "bytes are not there" answer a truncated export gives, which is how it has
// always been counted.
func (ex *Export) attachmentBytes(pathID string) (func() (int64, error), func() (io.ReadSeekCloser, error)) {
	if pathID == "" || strings.Contains(pathID, "..") {
		refuse := fmt.Errorf("importer: unusable attachment path %q", pathID)
		return func() (int64, error) { return 0, refuse },
			func() (io.ReadSeekCloser, error) { return nil, refuse }
	}
	full := filepath.Join(ex.Dir, "uploads", pathID)
	return func() (int64, error) {
			fi, err := os.Stat(full)
			if err != nil {
				return 0, err
			}
			return fi.Size(), nil
		}, func() (io.ReadSeekCloser, error) {
			f, err := os.Open(full)
			if err != nil {
				return nil, err
			}
			return f, nil
		}
}

// rewriteZulipUploads swaps /user_uploads/<path_id> links (relative or
// absolute) for our managed-file URLs so imported content renders working
// links. The scan is over the LANDED attachments for every message —
// O(landed × messages) — which is the shape that shipped; a per-message link
// index is a scale change, not a refactor, and belongs to whoever measures it.
func rewriteZulipUploads(body string, pathByAttachment map[string]string, fileIDs map[string]int64) (string, bool) {
	if len(fileIDs) == 0 || !strings.Contains(body, "/user_uploads/") {
		return body, false
	}
	changed := false
	for attachmentID, fileID := range fileIDs {
		path := pathByAttachment[attachmentID]
		if path == "" {
			continue
		}
		needle := "/user_uploads/" + path
		if strings.Contains(body, needle) {
			body = strings.ReplaceAll(body, needle, fmt.Sprintf("/api/v1/files/%d", fileID))
			changed = true
		}
	}
	return body, changed
}

func readJSON(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

func ts(f float64) time.Time {
	sec := int64(f)
	nsec := int64((f - float64(sec)) * 1e9)
	return time.Unix(sec, nsec).UTC()
}

// dmParticipants resolves a non-stream message's participant set (source
// user ids, sorted, deduped): legacy personal recipients (type 1) pair the
// sender with the target; direct-message groups (type 3 — modern Zulip
// routes ALL DMs here, 1:1 included) take the recipient's subscribers.
func dmParticipants(ex *Export, m zulipMessage) ([]int64, bool) {
	var raw []int64
	if target, ok := ex.PersonalTarget[m.Recipient]; ok {
		raw = []int64{m.Sender, target}
	} else if members, ok := ex.DMGroupMembers[m.Recipient]; ok && len(members) > 0 {
		raw = members
	} else {
		return nil, false
	}
	set := map[int64]bool{}
	for _, id := range raw {
		set[id] = true
	}
	ids := make([]int64, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, true
}
