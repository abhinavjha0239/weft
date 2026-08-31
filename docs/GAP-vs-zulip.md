# Gap analysis — what Zulip has that Weft doesn't (micro-feature level)

*Scope: apples-to-apples on Zulip's IMPLEMENTED product surface vs Weft's
implemented surface (per `docs/REALITY.md`). Video/voice calling excluded by
request. Zulip citations are code-verified against the Zulip tree; Weft status
is reconciled against the REALITY ledger, not guessed.*

*Legend — **MISSING**: no analog in Weft. **PARTIAL**: Weft has a coarser or
narrower analog; the delta is described. **N/A**: doesn't apply to Weft's data
model (a real difference, not a deficiency). **Weft-ahead**: noted where Weft
actually leads, so this stays honest.*

*One structural caveat that recurs below: **Zulip topics are a lightweight
per-message string (`subject`); Weft threads are first-class objects.** Many
"gaps" (cross-channel topic move, propagation modes, merge, `topic:` search,
`@**topic**`) flow from that one modeling choice. They're real feature gaps, but
several are "N/A under a different model" rather than "Weft is behind." Flagged
inline.*

---

## 0. Executive summary — the gap, ranked

The pattern from the CTO verdict holds at micro-level: **Weft's mechanisms are
often cleaner, but Zulip has vastly more accumulated user-facing surface inside
each feature.** The biggest gap axes, roughly by user-visible impact:

1. **Integrations breadth** — ~91 format-aware incoming-webhook integrations
   (GitHub/Jira/Sentry/PagerDuty/…), 16 categories, per-integration docs. Weft's
   generic webhook trigger receives HTTP but parses no vendor payloads. **~90-integration gap.**
2. **Bot users** — 4 bot types, bot owners, per-bot API keys, `.zuliprc`,
   embedded bots, system bots. Weft has an automations engine but **no bot identity**.
3. **Group-valued settings architecture** — ~25 realm + 11 channel + 6 group
   settings whose *value is a user group* (incl. anonymous ad-hoc groups). Weft
   has the resolver but no product surface, and only ~7/40 verbs registered.
4. **Message formatting depth** — spoilers, LaTeX/math, `/me`, slash commands,
   wildcard/silent/group mentions, code syntax highlighting, linkifiers, global time.
5. **Notification granularity** — the medium × message-class matrix (24 fields:
   separate desktop/email/push/audible per stream / followed-topic / DM-mention).
6. **Search/narrow operators** — ~15 operators Weft lacks + `-` negation +
   search pills/typeahead.
7. **Widgets** — polls, to-do lists, zform interactive widgets.
8. **Read receipts, typing indicators, reminders, starred messages, mark-as-unread.**
9. **Sidebar views** — Combined feed, Mentions, Starred, Recent conversations,
   Inbox, Drafts, Scheduled, Reminders + full keyboard-shortcut system.
10. **Custom profile fields, display-preference matrix, org permission policies,
    auth backends (SAML/LDAP), import sources (Slack/Mattermost/RocketChat/Teams),
    email-visibility levels, muted users, spectators.**

**Where Weft is genuinely ahead (not gaps):** DND model (snooze + VIP pierce +
DM breakthrough — Zulip only has invisible-mode + away flag, no scheduled quiet
hours); compliance export (zip byte-bundle with private/DM/edit-history/
tombstones + audit read API); the permission *closure* mechanism (version-fenced
incremental maintenance); the event-log spine; per-org multicast gateway.

---

## 1. Messaging & composition

### Formatting / markdown
| Feature | Zulip does | Zulip cite | Weft |
|---|---|---|---|
| Spoiler blocks | ` ```spoiler Header ``` ` collapsible | `zerver/lib/markdown/fenced_code.py:253,351` | **MISSING** |
| LaTeX / math (KaTeX) | inline `$$…$$` + ` ```math ``` ` | `zerver/lib/markdown/__init__.py:1385,2357` | **MISSING** |
| Global-time `<time:…>` | per-viewer localized `<time>` | `zerver/lib/markdown/__init__.py:1239,2346` | **MISSING** |
| `/me` status messages | italic third-person line | `zerver/lib/event_types.py:170,246` | **MISSING** |
| Wildcard mentions | `@**all/everyone/stream/channel/topic**` distinct notify | `zerver/lib/mention.py:41-42,1708` | **MISSING** (Weft: typed `@**name**` only) |
| Silent mentions | `@_**name**` / `@_**\|user_id**` non-notifying | `zerver/lib/mention.py:35,38` | **MISSING** |
| User-group mentions | `@*group*` expands to members | `zerver/lib/markdown/__init__.py:1776,2367` | **MISSING** |
| Channel/topic links | `#**chan**`, `#**chan>topic**`, `…@msgid` | `zerver/lib/markdown/__init__.py:1822,1847,1893` | **MISSING** |
| Code syntax highlighting | Pygments + language typeahead | `zerver/lib/markdown/fenced_code.py:90,300` | **PARTIAL** (Weft has code blocks, no server highlighting/lang) |
| Collapsible ` ```quote ``` ` | fenced nestable quote | `zerver/lib/markdown/fenced_code.py:249` | **PARTIAL** (has quote-reply, not fenced variant) |
| Linkifiers | realm regex → URL auto-link | `zerver/lib/markdown/__init__.py:1675,2398` | **MISSING** |
| Inline audio/video players | `AudioInlineProcessor` | `zerver/lib/markdown/__init__.py:2057` | **MISSING** (Weft has image thumbnails only) |

### Message actions menu
| Feature | Zulip cite | Weft |
|---|---|---|
| View source (raw markdown) | `message_actions_popover.hbs:110` | **MISSING** |
| Move message (cross-channel, `M`) | `message_actions_popover.hbs:33`; `message_actions_popover.ts:167` | **MISSING** (Weft: intra-channel move only) |
| Report message | `message_actions_popover.hbs:50`; `zerver/lib/message_report.py` | **MISSING** |
| Mark as unread from here (`Shift+U`) | `message_actions_popover.hbs:73`; `unread_ops.ts` | **MISSING** |
| Remind me about this | `message_actions_popover.hbs:82`; `message_reminder.ts` | **MISSING** |
| Collapse / expand message (`-`) | `message_actions_popover.hbs:90` | **MISSING** |
| View read receipts (`Shift+V`) | `message_actions_popover.hbs:119`; `zerver/views/read_receipts.py:14` | **MISSING** |
| Star / unstar (distinct personal flag) | `message_flags.ts:84`; `starred_messages_ui.ts` | **MISSING** (Weft's "save-for-later" is a *different* flag; the kind-2 star is deferred per REALITY) |
| Copy link to message | `message_actions_popover.ts:248`; `clipboard_handler.ts:29` | **MISSING** |

### Move / edit propagation
| Feature | Zulip cite | Weft |
|---|---|---|
| Propagation modes (change_one/later/all) | `message_edit.ts:1006`; `actions/message_edit.py:1052,1660` | **MISSING** |
| Move-with-notice to old/new thread | `message_edit.ts:1008,1256` | **MISSING** |
| Cross-channel message/topic move | `views/message_edit.py:160`; `actions/message_edit.py:687` | **MISSING** (REALITY: intra-channel only) |
| Edit-history diff viewer UI | `message_edit_history.ts:40`; `templates/message_edit_history.hbs` | **PARTIAL** (Weft captures revisions; no history-read endpoint/UI — REALITY gap) |
| Edit-history visibility policy | `message_edit_history.ts:20` | **MISSING** |
| `(edited)` vs `(moved)` markers | `message_list_view.ts:62` | **PARTIAL** (verify Weft distinguishes) |
| Topic-vs-content edit time-limit policy | `message_edit.ts:102-146` | **MISSING** (Weft: author-only, no window knob) |

### Compose box
| Feature | Zulip cite | Weft |
|---|---|---|
| Preview mode toggle | `compose_control_buttons.hbs:3`; `compose_ui.ts:552` | **MISSING** |
| Formatting toolbar (bold/italic/list/quote/spoiler/code/math) | `compose_control_buttons.hbs:44` | **MISSING** |
| Stream/topic/slash/emoji/lang autocomplete | `composebox_typeahead.ts:84,88,94` | **PARTIAL** (Weft has `@`-mention autocomplete only) |
| Slash commands (`/me`,`/poll`,`/todo`) | `composebox_typeahead.ts:595` | **MISSING** |
| Saved snippets (canned text) | `saved_snippets.ts`; `compose_control_buttons.hbs:34` | **MISSING** |
| GIPHY / GIF picker | `gif_picker_ui.ts`; `compose_control_buttons.hbs:29` | **MISSING** |
| Typing notifications | `actions/typing.py:34`; `typing.ts` | **MISSING** (Weft has *DM/channel typing signals* on the wire — verify compose-box indicator parity; REALITY: typing signals exist → **PARTIAL**) |
| Compose banners (mention-out-of-channel, wildcard confirm, not-subscribed) | `compose_banner.ts`; `compose_validate.ts` | **MISSING** |
| Send-later menu in compose + edit scheduled | `compose_send_menu_popover.ts`; `scheduled_messages_ui.ts:109` | **PARTIAL** (Weft HAS scheduled send server-side; in-compose menu + edit-scheduled UI missing) |
| List auto-continuation | `compose_ui.ts:608` | **MISSING** |

### Widgets / interactive
| Feature | Zulip cite | Weft |
|---|---|---|
| Polls (`/poll`) | `zerver/lib/widget.py:29,43`; `poll_widget.ts` | **MISSING** |
| To-do lists (`/todo`) | `zerver/lib/widget.py:60`; `todo_widget.ts` | **MISSING** |
| zform / generic widgets | `zform.ts`; `actions/submessage.py` | **MISSING** |

### Read state / reliability
| Feature | Zulip cite | Weft |
|---|---|---|
| Read receipts | `zerver/views/read_receipts.py:14` | **MISSING** |
| Mark all / channel / topic as read | `unread_ops.ts:788,805,826` | **PARTIAL** (Weft has per-thread mark-read + O(1) counters; bulk mark-channel/all + mark-as-unread MISSING) |
| Message flags (starred/collapsed/read/mentioned) | `message_flags.ts` | **PARTIAL** (Weft has alert-word + saved; star/collapse/read-flag set MISSING) |
| Local echo + resend on failure | `echo.ts:98,130,154` | **PARTIAL** (client mechanism; Weft's dogfood client lacks it) |

**Weft-ahead / parity:** drafts (Weft has server-stored CRUD — REALITY; Zulip
adds cross-device localStorage sync), scheduled send (Weft HAS — agents wrongly
guessed missing), reactions + custom-emoji-in-reactions (parity), forward/quote
(parity, Weft's is quite thorough incl. mention-neutralization).

---

## 2. Channels & topics

### Topic model *(much of this is N/A-under-different-model, flagged)*
| Feature | Zulip cite | Weft |
|---|---|---|
| Topic = mutable per-message string | `zerver/lib/topic.py:46` | **N/A** (Weft threads are objects — a real modeling divergence) |
| Empty topic / "general chat" fallback | `models/messages.py:186`; `topic.py:382` | **PARTIAL/N/A** |
| Per-channel topics policy (threaded vs flat / require-topic) | `models/streams.py:26-30,166` | **MISSING** |
| Topic autocomplete from history | `views/streams.py:1188`; `topic.py:269` | **PARTIAL** (verify compose typeahead) |
| Resolved = `✔ ` title prefix (searchable) | `topic.py:34,343,413` | **PARTIAL** (Weft resolves as thread state, not title convention) |

### Topic actions
| Feature | Zulip cite | Weft |
|---|---|---|
| Move topic to another channel | `views/message_edit.py:160`; `actions/message_edit.py:687` | **MISSING** |
| Propagation modes on rename/move | `actions/message_edit.py:1052,1660` | **MISSING** |
| Merge topics | `message_edit.ts:1207` | **MISSING** |
| Move-with-notice (old/new location breadcrumb) | `message_edit.ts:1008,1256`; `actions/message_edit.py:1356` | **PARTIAL** |
| `@**topic**` wildcard mention | `mention.py:41,291` | **MISSING** |
| Follow/mute/unmute w/ INHERIT + unmuted-in-muted-stream | `models/user_topics.py:26-44` | **PARTIAL** (Weft has follow/mute/unmute; lacks 4-state INHERIT + unmuted-in-muted-channel) |
| Auto-follow / auto-unmute policies | `models/users.py:272-280` | **MISSING** |
| Mark topic / all topics read | `unread_ops.ts:805,826` | **PARTIAL** |
| Time-limit guard on change_all | `actions/message_edit.py:1429` | **MISSING** |

### Channel settings & per-channel permissions *(all group-valued in Zulip)*
| Feature | Zulip cite | Weft |
|---|---|---|
| Who can post (posting policy / announcement-only) | `models/streams.py:92-102,148,210` | **MISSING** |
| Who can add/remove subscribers | `models/streams.py:126,147,205` | **MISSING** (Weft: self-join only) |
| Who can administer channel (per-channel admin) | `models/streams.py:129,174` | **PARTIAL** (Weft has `administer_channel` verb, not group-configurable per channel) |
| Who can create topics | `models/streams.py:132,180` | **MISSING** |
| Who can move messages in/out of channel | `models/streams.py:141,144,195,200` | **MISSING** |
| Who can resolve topics | `models/streams.py:152,220` | **MISSING** |
| Per-channel message retention days | `models/streams.py:115-119` | **PARTIAL** (Weft has channel retention override; verify auto-delete window) |
| Markdown channel description | `models/streams.py:43` | **PARTIAL** |
| Subscriber count denormalized | `models/streams.py:48` | **PARTIAL** (Weft has counters) |

### Discovery & system messages
| Feature | Zulip cite | Weft |
|---|---|---|
| Recent conversations view | `recent_view_ui.ts` | **MISSING** |
| Inbox (grouped unread) view | `inbox_ui.ts` | **PARTIAL** (Weft has kanban + badges, not grouped-unread inbox) |
| Browse-all-channels directory + search | `stream_settings_ui.ts` | **PARTIAL** (Weft has discoverability listing) |
| Channel traffic stats / recently-active | `zerver/lib/stream_traffic.py:11`; `models/streams.py:164` | **MISSING** |
| Default stream *groups* (named bundles) | `zerver/lib/default_streams.py`; `DefaultStreamGroup` | **PARTIAL** (Weft has default channels, not grouped bundles) |
| New-channel announcement to announcements channel | `views/streams.py:1031` | **MISSING** |
| Realm announcement channels (new-stream/signup/updates) | `actions/create_realm.py:355` | **MISSING** |
| Per-channel notif split (desktop/audible/push/email/wildcard, each inherit) | `models/streams.py:399-403` | **PARTIAL** (Weft: single per-channel level) |
| Followed-topic notification tier | `models/scheduled_jobs.py:79-107` | **MISSING** |

**Weft-ahead / parity:** channel folders + default channels, per-user sidebar
pin+color+mute (parity), private-channel existence-masking (Weft's is unusually
rigorous), web-public + protected-history (parity), archive with alias
reservation.

---

## 3. User groups, roles & permissions

### Group CRUD (product feature) — largely MISSING in Weft
| Feature | Zulip cite | Weft |
|---|---|---|
| Create / edit / deactivate / reactivate group | `views/user_groups.py:59,135`; `actions/user_groups.py:664,686` | **MISSING** (Weft group mutators are service-level only) |
| Add/remove members (API) | `views/user_groups.py:344,401` | **MISSING** |
| Add/remove subgroups (nesting API) | `views/user_groups.py:453,509` | **MISSING** (Weft imports nesting; no end-user API) |
| Membership/subgroup query endpoints | `zproject/urls.py:501,507,510` | **PARTIAL** (resolver exists, no product API) |
| Group management UI | `user_group_create.ts`, `user_group_edit.ts` | **MISSING** |

### Group-valued settings (Zulip's core architecture) — MISSING as a surface
A setting's *value* is a system group, named group, or **anonymous ad-hoc group**
`{direct_members[], direct_subgroups[]}` with normalization + optimistic-
concurrency (`GroupSettingChangeRequest`). `zerver/lib/user_groups.py:1217,1233`.

- **Realm-level (~25):** `create_multiuse_invite_group`, `can_access_all_users_group`,
  `can_add_subscribers_group`, `can_add_custom_emoji_group`, `can_create_bots_group`,
  `can_create_groups`, `can_manage_all_groups`, `can_create_{public,private,web_public}_channel_group`,
  `can_delete_{any,own}_message_group`, `can_invite_users_group`, `can_mention_many_users_group`,
  `can_move_messages_between_{channels,topics}_group`, `can_resolve_topics_group`,
  `direct_message_{initiator,permission}_group`, … — `zerver/models/realms.py:796-940`. **PARTIAL** (Weft resolver can answer, but no settings storage/UI, no `require_system_group`/`allowed_system_groups`/`allow_nobody`-`everyone` constraints).
- **Channel-level (11):** `can_send_message_group`, `can_administer_channel_group`,
  `can_add_subscribers_group`, `can_remove_subscribers_group`, `can_subscribe_group`,
  `can_create_topic_group`, `can_move_messages_{out_of,within}_channel_group`,
  `can_delete_{any,own}_message_group`, `can_resolve_topics_group` + content-vs-metadata
  access tiers — `zerver/models/streams.py:168-234`. **MISSING**.
- **Group-level (6):** `can_manage_group`, `can_add_members_group`, `can_join_group`,
  `can_leave_group`, `can_remove_members_group`, `can_mention_group` — self-service
  per-group governance — `zerver/models/groups.py:115-152`. **MISSING** entirely.

### Roles, system groups, mentions
| Feature | Zulip cite | Weft |
|---|---|---|
| System groups as setting values (`role:owners/administrators/moderators/members/fullmembers/everyone/nobody/internet`) | `models/groups.py:11-18` | **PARTIAL** (Weft has owner/admin/mod/member/guest; missing `fullmembers`, `nobody`, `everyone`, `internet` as selectable values) |
| Full-member vs new-member (waiting period) | `models/users.py:788` (`waiting_period_threshold`) | **MISSING** |
| Reserved-prefix enforcement for custom group names | `models/groups.py:49` | **MISSING** |
| Group mention + `can_mention_group` gating | `models/groups.py:140`; `mention.py:37` | **MISSING** |
| Recursive supergroup-union / `role:internet` short-circuit queries | `lib/user_groups.py:837,1279` | **PARTIAL** |
| Anonymous-group pill picker UI | `group_setting_pill.ts` | **MISSING** |

**Correction to agent inference:** Weft **HAS** a moderator role (`moderate_messages`
defaults to `role:moderators` — REALITY "Message edit/delete" row), so moderator
tier is *not* a gap. Weft also HAS role-on-invite + role ceiling (invites row).

**Weft-ahead:** permission *closure* maintenance (version-fenced incremental
deltas, async rebuild lane) is mechanically more advanced than Zulip's approach.

---

## 4. Notifications, presence, status & DND

### Notification-settings matrix — the single biggest granularity gap
Zulip `UserBaseSettings` has a full **medium × message-class** matrix (24 fields,
`modern_notification_settings` at `models/users.py:328-357`):
- **Stream:** `enable_stream_{desktop,email,push,audible}_notifications` (`:201-204`)
- **Followed-topic:** `enable_followed_topic_{desktop,email,push,audible}_notifications`
  + `_wildcard_mentions_notify` (`:209-213`) — a *whole parallel medium set*
- **DM/mention:** `enable_desktop_notifications`, `enable_sounds`,
  `enable_offline_{email,push}_notifications`, `enable_online_push_notifications`,
  `wildcard_mentions_notify` (`:206,216-222`)

**Weft:** **PARTIAL/MISSING** — Weft has per-kind defaults (dm/mention/keyword) +
per-channel level; not the separate desktop/email/push/audible axis, nor the
dedicated followed-topic medium set, nor "online push."

| Feature | Zulip cite | Weft |
|---|---|---|
| Notification sound selection + ~40-sound library | `models/users.py:205`; `lib/sounds.py:6`; `static/audio/notification_sounds/` | **MISSING** |
| Email batching-period (configurable seconds) | `models/users.py:198` | **PARTIAL** (Weft coalesces, not user-set) |
| Redact message content in email/push/desktop | `models/users.py:217,220` | **MISSING** |
| Org-name-in-email-subject policy | `models/users.py:243` | **MISSING** |
| Weekly realm digest for inactive users | `lib/digest.py:48,117` | **MISSING** (Weft has offline digest, not scheduled weekly activity digest) |
| New-login / security email | `models/users.py:239`; `signals.py:49` | **MISSING** |
| Mobile push via APNs/FCM bouncer | `push_notifications.py:280,462` | **PARTIAL** (Weft has raw Web Push; no native-mobile bouncer) |
| Remove push after read | `push_notifications.py:1278` | **MISSING** |
| Test-push button | `views/push_notifications.py:108` | **MISSING** |
| Invisible mode (stop broadcasting presence) | `models/users.py:241`; `user_status.ts:52` | **MISSING** |
| Legacy away/unavailable flag | `lib/user_status.py:19` | **PARTIAL** |
| Auto-follow/unmute policies feeding notifications | `models/users.py:272-280` | **MISSING** |
| `RealmUserDefault` org-level notification defaults + security-sensitive guard | `models/users.py:438,411-426` | **MISSING** |
| Distinct wildcard trigger types (topic vs stream, in-followed-topic) | `models/scheduled_jobs.py:71-82` | **PARTIAL** |
| Group-mention notifications w/ smallest-group precedence | `lib/notification_data.py:322` | **MISSING** |
| Desktop unread-badge count policy | `models/users.py:224-236` | **MISSING** |
| Buddy list (presence dots, activity sort, "N online") | `buddy_list.ts`, `activity.ts` | **PARTIAL/MISSING** (dogfood client) |
| Reminders → Notification Bot DM | `lib/reminders.py:167`; `models/scheduled_jobs.py:152` | **MISSING** |

**Weft-ahead (NOT gaps):** DND model — `snoozed_until` + VIP priority-contact
pierce + one-per-day DM breakthrough. Zulip has only invisible-mode + away flag;
**no scheduled quiet-hours**. User-status core (emoji+text+clear-after) is parity.
In-app badge accrual is structural in Weft (accrues even when delivery suppressed).

---

## 5. Search, narrows & navigation

### Narrow operators — ~15 Weft lacks (+ `-` negation)
Full set from `web/src/filter.ts` + `zerver/lib/narrow.py`. Weft HAS: `from:`,
`in:` (channel), `has:link/attachment/image`, `is:resolved/unresolved`, `is:dm`,
`before:/after:`, quoted phrases.

**MISSING:** `topic:` (`filter.ts:187`), `near:` (`:222`), `with:` (move-stable
conversation link, `:223`), `id:` (`:155`), `dm:`/`pm-with:` (specific-conversation,
`:196`), `dm-including:` (`:210`), `group-pm-with:` (`narrow.py:651`),
`channels:public/web-public/archived` (`:165-182`), `in:home`/`in:all` (`:145-150`),
`is:starred/mentioned/alerted/followed/muted` (`:124-140`), `has:reaction` (`:114`),
`sender:me` + `Name|id` disambiguation (`:256`), **and `-` negation on every
operator** (`filter.ts:797`). Also **operator-compatibility validation** and
synonym canonicalization (`narrow.py:394`).

### Search UX — MISSING (Weft has raw-text FTS only)
Search pills/tokens (`search_pill.ts`), operator typeahead + `is:`/`has:`/
`channels:`/person/topic suggestions (`search_suggestion.ts:764,803,738,403,471`),
human-readable narrow description (`filter.ts:840-962`), negated-search suggestions.

### Sidebar views
| View | Zulip cite | Weft |
|---|---|---|
| Combined feed / All messages (`A`) | `left_sidebar_navigation_area.ts:234` | **PARTIAL/MISSING** (no unified feed) |
| Mentions view (`Cmd+@`) | `left_sidebar_navigation_area.ts:76` | **MISSING** |
| Starred messages view (`*`) | `starred_messages_ui.ts` | **MISSING** |
| Recent conversations (`T`) | `recent_view_ui.ts` | **MISSING** |
| Inbox (`Shift+I`) | `inbox_ui.ts` | **PARTIAL** |
| Drafts (`D`) | `drafts_overlay_ui.ts` | **MISSING** (Weft has draft storage, no overlay UI) |
| Scheduled messages | `scheduled_messages_overlay_ui.ts` | **MISSING** (has backend, no UI) |
| Reminders | `reminders_overlay_ui.ts` | **MISSING** |
| Channel→topic tree w/ per-topic unread | `stream_list.ts:1468` | **PARTIAL** |

### Keyboard shortcuts, permalinks, mark-read, user cards
- **Full hotkey system** (`hotkey.ts:142-245`) + `?` help modal — **MISSING**.
- **Copy-link everywhere** (message/topic/channel/`near`/`with`) — **MISSING**
  (`clipboard_handler.ts`, `message_actions_popover.ts:248`).
- **`#narrow/…` URLs + browser history** — **PARTIAL** (thin client).
- **Mark-read granularity** (channel/topic/all/on-scroll/on-narrow + banners) —
  **PARTIAL/MISSING** (`unread_ops.ts:788-826`, `unread_ui.ts`).
- **User card popovers** (role, status, local time, send-DM, admin actions) —
  **MISSING** (`user_card_popover.ts`).
- **Gear/personal/help menus, emoji/GIF pickers** — **MISSING/PARTIAL**.

---

## 6. Org/admin settings, profile, bots, integrations, import/export

### Org profile & policies
| Feature | Zulip cite | Weft |
|---|---|---|
| Org type classification | `models/realms.py:617,80` | **MISSING** |
| Day/night org logo (separate) | `models/realms.py:959,966` | **PARTIAL** |
| Email-domain allowlist / disposable-email block / `+`-alias block | `models/realms.py:216,221`; `:1427` | **MISSING** |
| Invite-required (open/closed) + max-invites | `models/realms.py:218,220` | **PARTIAL** |
| Require-unique-names / lock name-email-avatar changes (SSO) | `models/realms.py:260-263` | **MISSING** |
| Inline image vs URL-embed preview split + media size | `models/realms.py:232,233,235` | **PARTIAL** (Weft has one link-preview toggle) |
| GIF rating policy / default code-block language | `models/realms.py:721,725` | **MISSING** |
| Read-receipts org toggle | `models/realms.py:729` | **MISSING** |
| Message edit/delete policy + time limits | `models/realms.py:445,441,449` | **PARTIAL/MISSING** |
| Move-messages time-limit windows | `models/realms.py:302,306` | **MISSING** |
| DM initiation/permission policy (who can DM whom) | `models/realms.py:358,364` | **MISSING** |
| Wildcard-mention policy | `models/realms.py:134,415` | **MISSING** |
| Waiting-period for full member | `models/realms.py:433` | **MISSING** |
| Message visibility limit / first-visible-message | `models/realms.py:515,518` | **MISSING** |

*(The ~20 `can_*_group` policy axes overlap §3 group-valued settings.)*

### Custom profile fields — entirely MISSING
8 types (short/long text, choice, date, URL, user-picker, external-account,
pronouns) + ordering + "display in summary" (max 2) + hint text —
`models/custom_profile_fields.py:92-131`. **MISSING** (Weft has *work-item*
custom fields P-13, a different system — not user profiles).

### Linkifiers, playgrounds
Linkifiers (regex→URL, `models/linkifiers.py`) and code playgrounds
(`models/realm_playgrounds.py:16`) — both **MISSING**. Custom emoji admin: Weft **HAS**.

### Auth backends
Weft HAS OIDC + password. **MISSING:** SAML (`backends.py:3575`), LDAP
(`:1157,1440`), remote-user SSO (`:2016`); turnkey social providers
(Google/GitHub/GitLab/Apple/Azure/Discord, `:2988-3209`) are **PARTIAL** (generic
OIDC can cover some, no per-provider buttons). Per-org enable/disable of methods:
**PARTIAL**.

### Bots — MISSING (Weft's automations engine is the closest analog)
4 bot types (generic/incoming-webhook/outgoing-webhook/embedded,
`models/users.py:452-472`), bot owners, per-bot API keys + `.zuliprc`, bot
avatars, deactivate/reactivate, admin+personal bot tables, allowed-bot-types
gating, system/cross-realm bots (Notification/Welcome bots). Weft has automations
(rules engine: triggers incl. webhook/slash, outbound HTTP steps) covering the
*outbound/inbound webhook* use cases **PARTIAL**, but **no bot user identity**.

### Integrations — the largest breadth gap
- **~91 format-aware incoming-webhook integrations** (`zerver/webhooks/` = 94
  dirs; `lib/integrations.py` = 91 `IncomingWebhookIntegration`, 121 total):
  GitHub, GitLab, Jira, Sentry, PagerDuty, Stripe, Zendesk, Datadog, Grafana,
  CircleCI, … — **MISSING** (Weft's generic webhook trigger parses no vendor payloads).
- **16 categories** + per-integration docs/screenshots + URL-builder UI +
  Hubot + embedded-bot framework — **MISSING**.
- Outgoing webhooks / slash-to-bot — **PARTIAL** (automations cover reasonably).
- Full documented REST API + personal API key — **PARTIAL**.

### Invitations, user admin, display prefs, account
- **Invitations:** email + multiuse — Weft HAS. Pre-selected channels on invite —
  Weft HAS (default channels). Configurable expiry / pending-admin-table / resend —
  **PARTIAL**.
- **User admin:** deactivate/reactivate, change role, deactivated-users list,
  admin-manage-from-profile — **PARTIAL** (verify against Weft's account rows).
- **Display-preference matrix (~25 settings):** theme, 24h clock, timezone,
  home view, font-size, line-height, emoji-set (Google/Twitter/native/text),
  translate-emoticons, high-contrast, fluid-width, user-list style, demote-inactive-
  streams, starred-counts, mark-read-on-scroll policy, animate-previews, … —
  `models/users.py:46+`. Mostly **MISSING**; `RealmUserDefault` org defaults **MISSING**.
- **Account:** change email flow, personal API key, 2FA (TOTP), self-service data
  download, **email-address-visibility (5 levels)** + delivery-vs-display email —
  mostly **MISSING** (Weft HAS change-password, rename, avatar).

### Muted users, spectators, import sources
| Feature | Zulip cite | Weft |
|---|---|---|
| Mute a specific person | `muted_users.ts`; `settings_muted_users.ts` | **MISSING** |
| Muted topics/streams UI | `settings_user_topics.ts` | **PARTIAL** |
| Alert-words UI | `alert_words_ui.ts` | **PARTIAL** (Weft HAS alert words backend + API; no settings UI) |
| Spectators / logged-out web-public viewing | `models/realms.py:225`; `spectators.ts` | **MISSING** (Weft has web-public *authed*; no anonymous spectator app) |
| Slack import | `data_import/slack.py` | **MISSING** |
| Mattermost import | `data_import/mattermost.py` | **MISSING** |
| Rocket.Chat import | `data_import/rocketchat.py` | **MISSING** |
| Microsoft Teams import | `data_import/microsoft_teams.py` | **MISSING** |

**Weft-ahead (NOT gaps):** storage quota (parity), and **data export** — Weft's
zip byte-bundle (private channels + DMs + edit history + tombstones) + audit read
API + retention/legal-holds **exceeds** Zulip's export. Zulip adds public/with-
consent export *variants* + single-user GDPR export (**PARTIAL** for Weft).

---

## 7. Honest closing note

Every "Weft MISSING" above is real against the current REALITY ledger, but two
honesty caveats carry over from the CTO verdict:

1. **N/A ≠ behind.** The topic-model gaps (`topic:`, cross-channel move,
   propagation modes, merge, `@**topic**`) flow from Weft choosing first-class
   thread objects over Zulip's per-message topic string. That's a *design fork*,
   not a deficiency — but it does mean these specific Zulip affordances have no
   Weft equivalent today.

2. **"Weft-ahead" claims are still unproven at load.** DND, compliance export,
   and the closure mechanism read as cleaner/broader than Zulip's — but nothing
   Weft has built has run in production, and the MarkRead O(N) regression is the
   standing reminder that clean design still ships bugs only real load reveals.

**The through-line:** Zulip's lead here is *accumulated surface area* — hundreds
of small, battle-tested affordances (91 integrations, 24 notification toggles, 15
search operators, 25 display prefs, 8 profile-field types) built over a decade.
Weft's core mechanisms are competitive or better; its *breadth of finished
user-facing micro-features* is a fraction of Zulip's, and that is exactly what a
decade of real users surfaces and forces you to build.
