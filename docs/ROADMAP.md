# ROADMAP — the PR queue, as executable specs

This is the work queue for v1 feature-completeness, written so that an
executor model (Opus) can pick up a slice and implement it WITHOUT making
design decisions. The division of labor is fixed:

- **Design lives here** (done by the strongest model): scope, module
  ownership, semantics, edge cases, performance notes, ACL rules, test
  matrix. If it isn't written here, it isn't decided.
- **Executors implement SPEC-READY entries verbatim** under `CLAUDE.md`
  (read it first, every time): branch off dev, commit discipline, gates,
  hard stop before push — every executed slice is REVIEWED by the
  strongest model before anything is pushed or merged.
- An entry marked **NEEDS-DESIGN** must NOT be dispatched or improvised —
  the missing decisions get made and written here first.
- Ambiguity rule for executors: if the spec and the code disagree, or a
  case isn't covered here, STOP and report — never invent.

Status: `[ ]` queued · `[~]` in flight · `[x]` shipped (with PR#).
Effort: S (≤2 commits), M (3–4), L (5+ or migration-heavy).

Shipped so far in the M1-completion run: reactions (#44), compliance
export (#45), drafts+scheduled (#46), invites+guests (#47), email medium
(#48), alert words (#49), status+DND+VIP (#51).

---

## Tier 1 — SPEC-READY (dispatch in this order unless the user reorders)

### P-01 `notification: DM breakthrough (N-2).` — S — **[x] shipped #53**
Completes N-2. The `dm_breakthrough` table (0006) gives a DM sender ONE
"notify anyway" per recipient per day when the recipient is snoozed.
**Design (decided):**
- `POST /api/v1/dms/{id}/breakthrough` — actor must be a participant of
  that dm_space; exactly one OTHER participant allowed (1:1 and self
  conversations only → group DMs get 400 "breakthrough is for direct
  conversations"; self-DM 400).
- Semantics: consumes the (sender, recipient, current-date) row via
  `INSERT ... ON CONFLICT DO NOTHING`; if the row already existed → 409
  "breakthrough already used today". On success: re-fan the recipient's
  MOST RECENT unseen kind-1 notification from this sender in this
  conversation over `hub.NotifyUser` (bypassing the DND gate — that is
  the point), and mark nothing else. If there is no unseen kind-1 row
  from this sender → 409 "nothing pending to break through" (don't burn
  the daily use — insert the row only after this check, same tx).
- If the recipient is NOT currently snoozed → 409 "recipient is not in
  do-not-disturb" (don't burn the use).
- No email side: breakthrough is a live-ping concept; the pending email
  delivers on the normal post-snooze sweep. Not event-logged (personal
  signal), but DO append `dm.breakthrough_used` to the event log —
  actually NO: decided — no event (symmetric with DND/VIP precedent).
**Edge cases:** date boundary uses UTC `current_date` (document it);
recipient deactivated → the participant check already 404s; sender
snoozed themselves is irrelevant (only recipient's DND matters).
**Performance:** two PK lookups + one indexed notification probe, one tx.
**Tests (TestDMBreakthrough):** happy path (snoozed recipient, pending
unseen DM → fan captured via the capturing fanout, row consumed); second
call same day 409; next-day works (backdate `used_on` via SQL, never
sleep); not-snoozed 409 (use not burned — assert no row); no-pending 409
(not burned); group-DM 400; non-participant 404 (oracle-free).
**Gaps to record:** client affordance (the "notify anyway" button) is
client work; per-org disable knob later.

### P-02 `messaging: Pins and saved items.` — M — **[x] saved items shipped #54; pins → P-02b**
Wakes `pin` and `saved_item` (0004 — read their DDL first).
**Design (decided):**
- Pins are PER-CONTAINER curation, gated like moderation-lite:
  `PUT/DELETE /api/v1/messages/{id}/pin` requires the actor to pass the
  same three-way read ACL AND, for channel messages, `administer_channel`
  OR being the message author is NOT enough — decided: pinning in
  channels requires `administer_channel`; in DMs any participant may pin;
  space-thread messages: `edit_items`. `GET /api/v1/channels/{id}/pins`
  (and `GET /api/v1/threads/{id}/pins` for DM/space threads) lists with
  message previews (id, author, source excerpt ≤160 chars, created_at).
- Pin cap: 50 per container (409 beyond — Slack parity is 100 in
  channels; we choose 50, cheap to raise).
- Pinning IS event-logged (`message.pinned`/`message.unpinned`, entity =
  message, payload carries container ids so the gateway routes it) —
  pins are shared state, unlike personal flags.
- Saved items are PERSONAL (read-state precedent): `PUT/DELETE
  /api/v1/messages/{id}/save`, `GET /api/v1/saved` (newest first, cap
  200, includes container ids + excerpt). No events. Saving requires
  read visibility (three-way ACL, oracle-free 404). A saved message that
  is later deleted stays in the list but renders as tombstone (excerpt
  ""; include `deleted: true`) — do NOT filter it out silently.
**Edge cases:** double-pin idempotent 200 (no second event — RowsAffected
gate, the reactions pattern); pin on deleted message 404; unpin
not-pinned idempotent 200; saved list pagination NOT needed v1 (cap).
**Performance:** pins list = one indexed query per container; saved list
= per-user index (check 0004 for indexes; if `saved_item` lacks a
(user_id, id DESC) index, note it in the PR scale review — do NOT add a
migration without flagging).
**Tests:** channel pin permission matrix (member 403, admin 200), DM
participant pin, cap 409, event emitted once, saved privacy (other users
never see your saved list), tombstone rendering, ACL 404s.
**Gaps:** pin ordering is insertion-order v1; no pin "system message".
**EXECUTION NOTE (#54):** the executor correctly STOPPED on the pins
half — this spec required DM/space pins but the `pin` table is
`channel_id NOT NULL` (channel-only). Saved items shipped alone; pins
re-specced below as P-02b.

### P-02b `messaging: Channel pins.` — S — **[x] shipped #57**
Pins, re-specced to MATCH the 0004 schema: channel messages only.
**Design (decided):**
- `PUT/DELETE /api/v1/messages/{id}/pin` — CHANNEL messages only
  (DM/space-thread messages → 400 "pins are for channel messages";
  their pin support is a schema change deferred until the fusion UX
  needs it — record as the gap). Actor needs the three-way read ACL
  AND `administer_channel` on the message's channel (ChannelScope).
- `GET /api/v1/channels/{id}/pins` — member-gated (requireMember),
  newest-pinned first, message previews (id, author_id, ≤160-rune
  excerpt via the saved-items helper, pinned_by, pinned_at); deleted
  messages are auto-dropped from the list (the JOIN filters
  deleted_at) AND DeleteMessage clears the message's pin rows in the
  same tx (add that — it keeps the cap honest).
- Cap 50 per channel (409). Idempotent double-pin/unpin (RowsAffected
  gate, no second event). Events `message.pinned`/`message.unpinned`
  (entity=message, payload carries channel_id for gateway routing).
**Tests:** permission matrix (member 403 / admin 200 / non-member 404
oracle-free), DM message 400, cap 409, idempotence + single event,
delete-clears-pin, list ordering + preview shape, guest visibility
(guest sees pins only in their channels via the member gate).

### P-03 `messaging: Forwarding and quoting.` — M — **[x] shipped #55**
The `message.forwarded_from_message_id` column (0004) wakes.
**Design (decided):**
- `POST /api/v1/messages/{id}/forward` {thread_id, comment?} — copies
  ONE message into another thread the actor may post to. The actor must
  pass the three-way READ ACL on the source (oracle-free 404) and the
  FULL send gate on the target (reuse `deliverToThread` via a new
  `messaging.ForwardMessage` that runs both gates in one tx).
- The forwarded message's source becomes: the optional comment, then a
  quoted block: `> ` -prefixed lines of the ORIGINAL source (cap the
  quote at 1000 chars with a trailing `…`), then a plain attribution
  line `— forwarded from @**Author Name**`. `forwarded_from_message_id`
  set. Mentions inside the QUOTED text must NOT notify — strip the
  mention syntax from the quoted portion by replacing `@**` with `@​**`
  (zero-width space) in the quote only, never the comment. This is the
  key edge case; test it.
- Attachments do NOT re-attach on forward (the file links in the quote
  are inert text; the union-ACL means a reader of the forward can only
  download if some message THEY can read references the file — do not
  create new file_reference rows). Document this in the PR.
- Deleted/tombstoned source → 404. Forwarding a forward is allowed (the
  chain just records the immediate parent).
- `GET /api/v1/messages/{id}` response gains `forwarded_from` (id, may
  be null) — clients resolve.
**Tests:** cross-channel forward happy path; private-source 404 for
non-members; target send-gate 403 (e.g. guest forwarding into a channel
they're not in); quoted-mention does NOT create a notification (assert
via runner.ProcessOrg + kindsFor pattern); comment mentions DO notify;
quote truncation; attachment not re-referenced (no new file_reference
row); forwarded_from surfaced.
**Performance:** one tx, two gates, one insert — same cost as a send.

### P-04 `messaging: Move a message between threads.` — M — **[x] shipped #58**
Revision kind 3 (`prev_thread_id`) wakes — Zulip's "move message".
**Design (decided):**
- `POST /api/v1/messages/{id}/move` {thread_id} — target thread must be
  in the SAME channel as the source (v1: intra-channel moves only;
  cross-channel moves are a later slice — reject with 400 "target must
  be in the same channel"). DM/space messages: 400 (channel messages
  only). Channel root messages (F-15) cannot move.
- Permission: message author OR `moderate_messages` (the edit/delete
  precedent — read edit.go first and mirror its loadForWrite/gate shape).
- Mechanics in one tx: FOR UPDATE the message row; insert revision kind 3
  with `prev_thread_id`; UPDATE message.thread_id; bump BOTH threads'
  denormalized counters (source: message_count-1 if kind=1 thread,
  target: +1 and last_activity_at=now() if kind=1); append
  `message.moved` event (payload: message_id, from_thread_id,
  to_thread_id, channel_id — gateway routes by channel; clients reload
  both threads).
- The search row needs no change (search_tsv is content-based;
  thread/channel filters read live columns — verify search joins by
  channel_id which is unchanged intra-channel).
**Edge cases:** move to the SAME thread → 400; move to channel-root
thread is ALLOWED (that's "move to channel chat"); target thread
archived-channel check is inherent (same channel); watermarks: read
state is per-thread — a moved message may appear unread in the target;
decided: acceptable v1, note in PR (Zulip has the same wrinkle).
**Tests:** author move; moderator move of another's message; member 403;
cross-channel 400; DM 400; root-message 400; counters on both threads
asserted; revision row (kind 3, prev_thread_id) asserted; event payload.

### P-05 `gateway: Idle and away presence.` — M — **[x] shipped #59**
Presence currently binary (online on first socket, offline on last).
**Design (decided):**
- States: 1 active, 2 idle, 3 offline (matches the presence table
  comment; the UNLOGGED table stays UNUSED — presence remains in-Hub
  memory, per-process, exactly as today).
- Idle = no client activity signal for 10 minutes while connected. The
  dogfood client already sends typing/read frames; add a lightweight
  `{"type":"active"}` client frame throttled client-side to ≥60s apart
  (minimal JS: send on visibilitychange-to-visible and on
  keydown/click, throttled). Hub tracks lastActive per user; a 60s
  ticker sweeps userConns and demotes online→idle at >10min, promotes
  on any activity frame or any other inbound frame (typing counts).
- Broadcast `presence.changed` with `state: "active"|"idle"|"offline"`
  ONLY on transitions (not per frame). `GET /api/v1/presence` returns
  the tri-state map. Keep the wire field `state` (the dogfood client
  already reads it — check signals.go and index.html presence handling
  first, extend not replace; the old "online" value becomes "active":
  update BOTH sides in the same commit and say so in the PR).
- Invisible mode: NOT this slice (needs user setting + read-side
  masking; record as gap).
**Edge cases:** multi-device — a user is as active as their MOST active
connection; reconnect resets idle timer; the sweep must not hold h.mu
while broadcasting (copy the transition list under lock, fan after —
read presence.go's existing lock discipline first and match it).
**Tests:** extend the existing gateway/presence test file's harness
(wsClient + waitForPresence): connect → active; inject idleness by
manipulating the hub's clock — add a test seam `hub.SetNowFunc` or make
the threshold a field (decided: exported field `IdleAfter time.Duration`
default 10min; test sets 50ms and polls waitForPresence for "idle" —
bounded polling, no fixed sleeps); activity frame promotes back;
disconnect → offline.
**Performance:** one ticker per hub, O(connections) sweep per minute.

### P-06 `files: Avatars and custom emoji.` — L — **[x] shipped #60**
`user_account.avatar_file_id` and `custom_emoji` (0004/0006 FKs) wake.
**Design (decided):**
- `PUT /api/v1/me/avatar` — multipart "file", image-only allowlist BY
  MAGIC BYTES (png/jpeg/webp/gif via net/http DetectContentType — never
  trust the client mime), ≤2 MiB (400 beyond), stored through the normal
  files.Upload path (content-addressed, org-scoped) then
  `user_account.avatar_file_id` updated; `DELETE /api/v1/me/avatar`
  clears the pointer (never deletes the file row — GC's unclaimed lane
  ignores avatar-FK'd files already; once cleared, the old file becomes
  GC-eligible after grace — say this in the PR, it's the designed
  lifecycle). No image resizing v1 (record as gap: thumbnail pipeline).
- Avatar READ: `GET /api/v1/users/{id}/avatar` streams via a NEW
  files-service method `OpenAvatar(ctx, actor, userID)` — ACL: any org
  member may fetch any member's avatar (avatars are org-public by
  design; guests may fetch only their visible users — reuse
  guestVisibleClause semantics via an EXISTS). nosniff + `inline`
  disposition is SAFE here because the bytes were magic-validated as
  images at upload (say this in the code comment); cache headers
  `Cache-Control: private, max-age=3600`.
- Directory/profiles gain `avatar_file_id` (nullable) — clients build
  the URL.
- Custom emoji: `POST /api/v1/emoji` {name} + multipart file (same image
  gate, ≤256 KiB), name `[a-z0-9_]{2,32}` unique per org (409 on dup,
  including with a DEACTIVATED emoji's name — decided: name stays
  reserved, Zulip parity); `GET /api/v1/emoji` (live only);
  `DELETE /api/v1/emoji/{name}` = soft (deactivated_at) — gated
  `manage_org` for create/delete v1 (record gap: per-org "who can add
  emoji" knob). Reacting with a custom emoji name already works
  (reactions store tokens); the emoji list endpoint is how clients
  resolve names→files. Event-log `emoji.created`/`emoji.deleted`.
**Edge cases:** SVG is NOT an image here (XSS vector — explicitly
rejected; test it); animated gif fine; a deleted user's avatar keeps
serving (historical rendering); emoji file rows are avatar-style
FK-pinned (custom_emoji FK already guards GC).
**Tests:** magic-byte rejection (rename a .txt to .png), size caps,
avatar round trip incl. guest visibility, clear→GC-eligible (backdate +
sweep proves the pointer-clear lifecycle), emoji name rules/409/soft
delete/list, reaction with custom emoji name renders in aggregates.

### P-07 `blob: S3 driver and signed download URLs.` — M — **[x] shipped #61**
**Design (decided):**
- S3 driver implements the existing `blob.Store` verbatim (Put/Open/
  Delete) using the AWS SDK v2 (`aws-sdk-go-v2` — the ONE new dependency;
  justify in the PR; MinIO/R2 compatible via custom endpoint).
  Config: `WEFT_BLOB_DRIVER=s3`, `WEFT_S3_BUCKET`, `WEFT_S3_REGION`,
  `WEFT_S3_ENDPOINT` (optional, for MinIO/R2), `WEFT_S3_PREFIX`
  (optional key prefix), credentials via the SDK default chain (env/IAM
  — never our own config keys for secrets). Put stays idempotent
  (PutObject is), Open = GetObject, Delete tolerates NoSuchKey.
- Signed URLs: do NOT use S3 presigned URLs (they bypass our union-ACL).
  Instead: `POST /api/v1/files/{id}/link` runs the SAME OpenDownload ACL
  and returns `/api/v1/files/{id}?sig=<hmac>&exp=<unix>` — an HMAC
  (SHA-256, key = new `WEFT_SIGNING_SECRET`, required when the feature
  is used; refuse link-minting with a clear 500-config error if unset)
  over `file_id|exp|org_id`, expiry 10 minutes. The download handler
  accepts EITHER a bearer token (existing path) OR a valid unexpired
  sig (skips the per-request ACL — the sig IS the capability; this is
  what makes <img src> tags work in clients). Constant-time compare.
- Integration test for s3 driver runs ONLY when `TEST_S3_ENDPOINT` is
  set (skip otherwise — CI has no MinIO yet; record gap: MinIO service
  in CI). The fs-driver test suite must still pass untouched — the seam
  proves itself by the driver swap needing zero core changes.
**Edge cases:** sig for a file that gets GC'd meanwhile → 404 (row check
still runs); clock skew — allow 30s leeway on exp; sig is org-pinned so
a leaked link cannot cross orgs.
**Tests:** HMAC path (mint → fetch without auth header → bytes; expired
→ 401; tampered → 401; foreign-org sig → 404), config-missing error,
fs-suite regression green.

### P-08 `dm: Group conversation management.` — M
**Design (decided):**
- Group DMs (kind 2) gain `POST /api/v1/dms/{id}/participants`
  {user_ids} (add) and `DELETE /api/v1/dms/{id}/participants/{userID}`
  (remove SELF only — leaving; nobody removes others, Slack model).
  1:1 and self conversations: 400 (immutable participant sets — a 1:1
  plus one = a NEW group conversation; clients call POST /dms with the
  three ids).
- CRITICAL consistency rule: dm_space.dm_key is the canonical sorted
  participant set. Changing participants MUST recompute and update
  dm_key in the same tx. If the new key collides with an EXISTING
  conversation (unique index) → 409 "a conversation with these people
  already exists" (do not merge histories — Slack behavior).
- Adders must be participants; added users must be live org members
  (guest rule: a GUEST may only be added by someone sharing a channel
  with them; a guest ADDER may only add people from their channels —
  reuse the P-5 reach predicate from dm.Open).
- History visibility: joiners see FULL history (dm participation is the
  permission — document that this is the Slack-group-DM model, not
  channels' history_from).
- Events: `dm.participants_changed` with user_ids (the gateway filter
  already refreshes DM views on dm.opened; extend the filter's refresh
  trigger to this verb for affected users — read gateway.go filter
  first).
- Cap: 9 participants (Slack parity), 400 beyond.
**Tests:** add → new member reads history + sends; leave → 404 on read
after; dm_key recompute asserted; collision 409; guest reach matrix;
cap; non-participant adder 404.

### P-09 `messaging: Channel folders and default channels.` — M
Wakes `channel_folder`, `default_channel`, `sidebar_section` (0003).
**Design (decided):** SPLIT — this slice ships folders + defaults;
sidebar_section (personal ordering) is P-14.
- Folders: org-level named groups of channels for the directory/sidebar.
  `POST/GET/PATCH/DELETE /api/v1/channel-folders` (name 1..60 unique per
  org live, soft delete? — decided: hard delete, folders are pure
  organization; channels in a deleted folder get folder_id NULL) gated
  `manage_org`. `PATCH /api/v1/channels/{id}` gains `folder_id`
  (administer_channel, validated org-local + live folder).
  ListChannels response gains folder_id. Event-logged
  (`folder.created/updated/deleted`, `channel.updated` carries folder).
- Default channels: `PUT /api/v1/default-channels` {channel_ids}
  replace-set (manage_org; ≤20; public live channels only — 400 for
  private: defaults must be joinable) + GET. CONSUMER (the honest-rungs
  requirement): invite acceptance auto-joins the org's default channels
  IN ADDITION to the invite's explicit channels (dedup; guests do NOT
  get defaults — their channels are exactly the invite's enumeration,
  P-5). Bootstrap seeds #general as a default.
**Tests:** folder CRUD + channel assignment + list surfacing; default-set
validation (private 400); invite accept lands member in defaults ∪
explicit; guest gets explicit only; bootstrap seed asserted.

### P-10 `search: Query operators.` — M
**Design (decided):**
- Extend `GET /api/v1/search` q parsing with operators:
  `from:<user-id-or-@**Name**>`, `in:<channel-id-or-name>`, `has:link`,
  `has:attachment`, `has:image`, `is:dm`, plus bare terms → FTS as
  today. Parser: split on spaces EXCEPT inside double quotes ("exact
  phrase" → phraseto_tsquery); unknown `key:` tokens are treated as
  literal terms (never an error — search must not 400 on user input).
- Each operator maps to an indexed WHERE clause on the existing search
  query (`m.author_id =`, `m.channel_id =`, `m.has_link` etc. — the
  has_* flags exist for exactly this, S-3). `in:` by name resolves via
  the live-name unique index, restricted by the caller's read ACL
  exactly as the current search already restricts (verify: search
  already joins membership — extend, don't fork).
- Name resolution for from:@**Full Name** uses the same in-tx resolver
  shape as mentions (exact full_name match, first by id).
**Edge cases:** `in:` a channel you can't read → empty results, NOT an
error (no oracle); combined operators AND together; quoted phrase +
operators mix; empty q after operator extraction → filters-only search
allowed (cap 50 rows as today).
**Tests:** each operator alone + combined; the oracle case; phrase
search; unknown-operator-as-term; guest search stays inside their
channels (existing membership join must already ensure — assert it).

---

## Tier 2 — scoped, but DO NOT dispatch until promoted to Tier 1
(Each needs a final-spec pass by the strongest model; the bullets below
record the scope and the known design questions so nothing is lost.)

- **P-11 `worktrack: Sprints.`** M — sprint entity per space, start/
  close, item assignment, `sprint.started/closed` events (automation
  triggers). OPEN: carry-over semantics for unfinished items; whether
  sprint is a field or a join table (schema check first).
- **P-12 `worktrack: Board ordering (LexoRank) and saved views.`** M —
  OPEN: rank column collision/rebalance policy; view = stored query
  (ADR-008 View) minimal shape.
- **P-13 `worktrack: Custom fields and item links.`** L — typed field
  defs per space + values + F-5 relationship consent ceremony polish.
  OPEN: field type set v1; validation model.
- **P-14 `messaging: Sidebar sections.`** S — personal channel ordering
  (read-state precedent, no events). OPEN: interaction with folders on
  the client.
- **P-15 `messaging: Link previews (unfurl).`** M — NEEDS-DESIGN:
  SSRF-guarded egress (Zulip OutgoingSession port — the guard design
  must be specced in detail before any executor touches it), cache
  table, opt-out. Security-critical: strongest-model execution
  recommended, not just review.
- **P-16 `channels: Web-public channels + history_from enforcement.`**
  L — SECURITY-CRITICAL (anonymous read path + membership history
  boundaries). Strongest-model execution, full spec first.
- **P-17 `compliance: Message retention vacuum.`** L — archive-then-
  vacuum with restore window (AD-3 completion). OPEN: archive storage
  shape (rows vs export-file), restore API. Strongest-model execution.
- **P-18 `files: Image thumbnails + inline rendering allowlist.`** M —
  OPEN: image processing dependency choice (pure-Go vs libvips),
  thumbnail storage keys, srcset shape.
- **P-19 `files: Upload scan hook + org quotas.`** S — F-7 hook
  interface + per-org byte quota enforcement on Upload/StoreDocument.
- **P-20 `notification: Email templates + unsubscribe.`** S — HTML
  wrapper + per-user unsubscribe token endpoint flipping all email
  prefs off. OPEN: none really — near Tier-1.
- **P-21 `notification: Push medium.`** L — NEEDS-DESIGN: web-push
  (VAPID) vs FCM seam; device registration table (needs migration).
- **P-22 `automation: Conditions and templating.`** M — field-compare
  conditions + `{{event.*}}` substitution in post_message. OPEN: exact
  template grammar + escaping rules (spec carefully — injection).
- **P-23 `automation: Schedules, inbound webhooks, slash triggers.`** L
  — cron triggers (due-runner exists as pattern), signed inbound
  webhook endpoint, slash-command trigger. OPEN: webhook auth model.
- **P-24 `automation: Outbound HTTP steps + delivery health.`** L —
  NEEDS-DESIGN: SSRF guard (shares P-15's egress design), retry/backoff,
  alert-before-auto-disable (AU-4).
- **P-25 `automation: Maintainer failure notifications.`** S — failed
  runs DM the rule's scope admins via the notification pipeline (kind:
  item-event class). Near Tier-1.
- **P-26 `automation: LLM steps + budgets + approval gates.`** XL —
  NEEDS-DESIGN: model gateway seam, budget metering, run status 8 flow.
- **P-27 `importer: Slack.`** XL — export parsing + slack_incoming
  compat endpoint. Ground in Slack export format docs before speccing.
- **P-28 `importer: Jira.`** XL — deliberately last (M3 exit).
- **P-29 `identity: Profile edit + session revocation + password
  reset.`** M — near Tier-1; OPEN: reset-token flow without email
  templates (depends P-20).
- **P-30 `identity: OIDC login.`** L — NEEDS-DESIGN: library choice,
  account linking rules, JIT provisioning vs invite-only.
- **P-31 `admin: Audit read API.`** M — filtered event-log reads for
  compliance_officer (AD-2). Near Tier-1: spec the filter params.
- **P-32 `compliance: Export byte-bundles (eDiscovery format).`** M —
  attachments IN the archive; depends on export pins (#45) — spec the
  bundle layout.

## Deliberately NOT in this queue (backend-first directive)
The real web client, mobile apps, calls/LiveKit, the automation builder
UI, and federation (ADR-004 T2) — v2 or post-directive items. The
dogfood UI gets minimal hooks only, inside feature slices.
