// Package importer implements migration adapters (ADR-003 E5): source →
// intermediate representation → dry-run fidelity report → provenance-keyed
// idempotent writes with backdated timestamps (E3).
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
	"os"
	"path/filepath"
	"sort"
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

func (u zulipUser) BestEmail() string {
	if u.DeliveryEmail != "" {
		return u.DeliveryEmail
	}
	return u.Email
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

// Export is the parsed source, pre-indexed for the writer.
type Export struct {
	Users         []zulipUser
	Streams       []zulipStream
	Subscriptions []zulipSubscription
	NamedGroups   []zulipNamedGroup
	GroupMembers  []zulipGroupMember
	GroupEdges    []zulipGroupEdge
	Messages      []zulipMessage
	Reactions     []zulipReaction
	UserMessages  []zulipUserMessage

	// recipient id → stream id (type 2 only); non-stream recipients counted
	// as skipped DMs.
	StreamByRecipient map[int64]int64
	DMRecipients      map[int64]bool
}

// LoadZulipExport reads an UNPACKED export directory (realm.json +
// messages-*.json). Tarball unpacking is the operator's step for now.
func LoadZulipExport(dir string) (*Export, error) {
	var realm zulipRealmFile
	if err := readJSON(filepath.Join(dir, "realm.json"), &realm); err != nil {
		return nil, fmt.Errorf("importer: realm.json: %w", err)
	}
	ex := &Export{
		Users:             realm.Users,
		Streams:           realm.Streams,
		Subscriptions:     realm.Subscriptions,
		NamedGroups:       realm.NamedGroups,
		GroupMembers:      realm.GroupMembers,
		GroupEdges:        realm.GroupEdges,
		StreamByRecipient: map[int64]int64{},
		DMRecipients:      map[int64]bool{},
	}
	for _, r := range realm.Recipients {
		if r.Type == 2 {
			ex.StreamByRecipient[r.ID] = r.TypeID
		} else {
			ex.DMRecipients[r.ID] = true
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
