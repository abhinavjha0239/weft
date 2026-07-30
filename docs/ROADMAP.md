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

### P-08 `dm: Group-DM leave.` — M — **[x] shipped #66** (hard-leave, not soft-leave)
**Shipped as HARD-LEAVE** (superseding the soft-leave/left_at sketch below): deleting the leaver's own dm_participant row makes all ~16 EXISTS-based DM ACL sites exclude them automatically — zero predicate edits, no migration. Rejoin = ensure-actor on the existing create-or-get; dm_key preserved. Contract note: thread read/send return 403 for a non-participant while single-message Get is oracle-free 404 (pre-existing inconsistency; normalizing it is a separate messaging-ACL slice). Original audit note below.

**Audit correction:** dm_key = join(sorted ids, ":") is the canonical
MEMBERSHIP IDENTITY, shared verbatim by dm.Open create-or-get AND the
importer's huddle path. So "add a participant to an existing group keeping
history" fights the data model — changing the set changes the conversation's
identity. The old add/remove-with-rekey spec is WITHDRAWN.
**Design (decided):**
- "Adding people" is NOT a mutation: it is opening the conversation for the
  new set via the existing `POST /dms` create-or-get (Slack's "this starts a
  new conversation"). No add endpoint. Document that clients call POST /dms
  with the enlarged id set; the existing guest-reach gate already applies.
- The ONE new operation is LEAVE: `DELETE /api/v1/dms/{id}/participants/me`.
  Migration 0013 adds `left_at TIMESTAMPTZ` to dm_participant. Leaving sets
  left_at (a SOFT leave) — the row STAYS so dm_key remains canonical and
  create-or-get for the original set still resolves. The leaver is filtered
  from dm.List (WHERE left_at IS NULL), excluded from gateway loadDMs, and
  gets 404 on read/send (requireParticipant treats left_at IS NOT NULL as
  not-a-participant). Re-opening the same set via POST /dms clears left_at
  (rejoin). Group (kind 2) only; 1:1/self → 400 "cannot leave a direct
  conversation".
- Event `dm.participants_changed` {dm_space_id, user_ids:[leaver]}; the
  gateway filter refreshes the leaver like dm.opened (read gateway.go filter).
**Invariants preserved:** dm_key never re-keyed (no collision case); importer
untouched; participation-is-permission unchanged for remaining members.
**Tests:** leave hides from the leaver's list + 404s their read/send; other
participants still see it; fan-out skips the leaver; rejoin clears left_at;
1:1 leave 400; dm_key row unchanged after leave.

### P-09 `messaging: Channel folders and default channels.` — M — **[x] shipped #65**
**Audit correction:** the schema is WORKSPACE-scoped, not org-flat:
channel_folder(org_id, workspace_id nullable, name, position) and
default_channel PK(workspace_id, channel_id) + `bundle` (C-3
DefaultChannelGroup). Weft seeds one workspace/org at bootstrap and has no
workspace-selection API yet (org-hierarchy UX is Tier-2). Two honest-rung
reductions, both documented in the PR:
**Design (decided):**
- Resolve "the workspace" server-side as the org's bootstrap workspace for
  v1; the API stays workspace-implicit. Org-hierarchy later threads
  workspace_id through.
- Folders: `POST/GET/PATCH/DELETE /api/v1/channel-folders` (name 1..60,
  position; workspace = resolved; manage_org; HARD delete → member channels'
  folder_id NULL). `PATCH /channels/{id}` gains folder_id (administer_channel;
  folder must be same-workspace + live). ListChannels surfaces folder_id.
  Events folder.created/updated/deleted.
- Defaults: `PUT/GET /api/v1/default-channels` {channel_ids} replace-set into
  default_channel with **bundle=NULL** (the "always" bundle; the C-3 bundle
  concept stays DORMANT — documented reduction). manage_org; <=20; PUBLIC live
  channels only (private → 400). CONSUMER (honest-rungs): invite-accept
  auto-joins the resolved workspace's default channels (bundle IS NULL) IN
  ADDITION to the invite's explicit channels (dedup); GUESTS get explicit only
  (P-5). Bootstrap seeds #general as a default.
**Tests:** folder CRUD + assignment + list; default-set validation (private
400); invite accept = defaults ∪ explicit; guest gets explicit only; bootstrap
seed asserted.

### P-10 `search: Query operators.` — M — **[x] shipped #63** (most pre-existed; #63 added has:attachment, has:image, is:dm, from:<id>)
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

### P-20 `notification: HTML digest emails + one-click unsubscribe.` — M — **[x] shipped #71**
Zero migrations. Read `prefs.go`, `email.go`, `platform/mail/mail.go`,
and `files/signed.go` (the MAC + self-auth precedents) first.
**Design (decided):**
- Commit 1 (standalone bug fix): the email worker's zero-rows default
  (`n.kind IN (1, 2)` in `email.go` RunOnce) disagrees with
  `defaultEmailEnabled` (dm/mention/KEYWORD) — the prefs API shows
  keyword email ON by default but the worker never sends it. Fix the
  SQL to `(1, 2, 4)`. Test: a keyword notification with zero pref rows
  gets emailed (red/green: revert the SQL → the test fails).
- Commit 2 (seam refactor, no behavior change): `mail.Sender` becomes
  `Send(m Message) error` with `Message{To, Subject, Text, HTML,
  ListUnsubscribe string, ListUnsubscribePost bool}`. smtp driver:
  multipart/alternative when HTML != "" (the text/plain part ALWAYS
  present, listed first), `List-Unsubscribe: <url>` +
  `List-Unsubscribe-Post: List-Unsubscribe=One-Click` (RFC 8058) when
  set, crypto-random boundary, existing sanitizeHeader on every header
  value. log driver logs to/subject/text_bytes/html_bytes. Update the
  single caller (`email.go`) mechanically.
- Commit 3 (feature):
  - `config.BaseURL` (`WEFT_BASE_URL`, default `http://localhost:8080`,
    trailing `/` trimmed). weftd threads BaseURL + cfg.SigningSecret
    into the EmailWorker (the files.SetSigningSecret pattern).
  - Unsubscribe MAC: `hex(HMAC-SHA256(secret, "unsub|<org_id>|<user_id>"))`
    — the literal `unsub|` prefix domain-separates it from the files
    MAC (which starts with a digit). NO expiry: unsubscribe links must
    not rot; secret rotation invalidating them is documented operator
    behavior.
  - Link: `{base}/api/v1/unsubscribe?o=<org>&u=<user>&sig=<mac>`.
  - Digests gain an HTML alternative (html/template const in a new
    `template.go`: brand.Name header, `<ul>` of the existing line()
    strings — auto-escaped, unsubscribe footer link) and a text footer
    `Unsubscribe: <url>`. HTML always ships; the unsubscribe link +
    List headers appear ONLY when the secret is configured (degrade
    gracefully, never emit a broken link).
  - Endpoints registered OUTSIDE withAuth (the handleDownloadFile
    self-auth precedent); they never consult Authorization:
    - `GET /api/v1/unsubscribe` → secret unset: 404. o/u must be > 0
      and the sig must verify (hmac.Equal) → else 401. Returns a
      minimal inline-HTML page whose ONLY action is a
      `<form method="post">` button. MUST NOT change state — mail
      clients prefetch GET links.
    - `POST /api/v1/unsubscribe` (same params; body ignored) → same
      verification; the user must exist in org o (deactivated users
      MAY still unsubscribe) else 404 → upsert
      `notification_medium_pref (user, kind, 2=email, enabled=false)`
      for EVERY kind in `prefKinds` (one statement via unnest) → 200
      `{"unsubscribed": true}`. Idempotent.
  - Domain logic lives in notification (`Unsubscribe(ctx, orgID,
    userID)` + MAC mint/verify helpers); the handler stays thin.
  - `prefKinds` is the single registry the flip iterates. P-25 appends
    kind 6 to the same slice — a trivial adjacent-line merge is
    expected at serial-merge time.
**Edge cases:** forged/truncated sig → 401 via constant-time compare;
unknown user under a valid-looking sig → 404; repeated POST stays 200;
GET never flips (assert it); no Authorization header required or read.
**Performance:** one MAC per user per digest; the flip is one unnest
upsert bounded by len(prefKinds).
**Tests (TestUnsubscribe + email-worker extensions):** commit-1 keyword
default; capture a real digest via a recording Sender → link parses;
GET returns the form page AND prefs stay unchanged; POST flips every
kind (ListMediumPrefs all-false) and a due DM notification is NOT
emailed afterward; second POST idempotent; forged sig (correct-length
hex over the right o/u minted with the WRONG secret — the forgedURL
precedent) → 401, proven load-bearing (neuter hmac.Equal → it returns
200: show red); one-nibble-flipped sig → 401; secret unset → 404 both
verbs; smtp Message assembly unit test (both MIME parts, headers only
when set, injection-safe headers).
**Gaps to record:** per-kind unsubscribe page (v1 is all-or-nothing);
resubscribe = the existing prefs API; HTML polish is client-era work.

### P-25 `automation: Failure notifications to scope admins.` — M — **[x] shipped #72**
Read `perms.go` (Require + chain), `automation/runner.go`
(execute/finishRun), `notification/runner.go` insert (payload + ping
pattern), and the 0010 dedupe index first.
**Design (decided):**
- Commit 1 (perms prep): exported `HoldersAt(ctx, tx, orgID, verb,
  chain) ([]int64, error)` — resolve the WINNING assignment exactly as
  Require does (chain VALUES join, `ORDER BY pa.scope_type DESC LIMIT
  1`), then expand it:
  `SELECT c.user_id FROM user_group_closure c JOIN user_account u ON
  u.id = c.user_id AND u.deactivated_at IS NULL AND u.kind = 1 WHERE
  c.group_id = $win ORDER BY c.user_id`. No assignment in the chain →
  (nil, nil). Unit tests in perms_test: org default (manage_org →
  role:admins) returns admins ∪ owners (closure nesting); a
  channel-scope assignment beats the org one; deactivated accounts and
  agents (kind 2) excluded.
- Commit 2 (feature):
  - enum: `EntityAutomationRun EntityType = 18` (append-only).
  - notification: `KindAutomationFailure = 6`; append to `prefKinds`;
    update SetMediumPref's kind-list error message; `defaultEmailEnabled`
    UNCHANGED (kind 6 email default OFF — in-app only; admins opt in
    via the existing prefs PUT). email.go line(): kind 6 → "- An
    automation run failed".
  - notification: extract the dndSuppressed SQL into a shared
    package-level helper (mechanical move, Runner + Service both call
    it). `RecordAutomationFailure(ctx, tx, orgID, runID int64,
    recipients []int64) ([]int64, error)`: one
    `INSERT … SELECT unnest($3::bigint[]) … ON CONFLICT (user_id, kind,
    entity_type, entity_id) DO NOTHING RETURNING user_id` with kind 6 /
    entity (18, runID); returns the users actually inserted.
    `PingNotification(ctx, orgID, userID, kind, entityType, entityID)`:
    DND-gated (actor 0 → no VIP pierce), payload EXACTLY the
    materializer shape `{kind, entity_type, entity_id, actor_id}`
    (thread_id omitted).
  - runner: `NewRunner(pool, msg, perms, notif, log)` — update weftd +
    tests. In execute(), when the final status ∈ {4 partial-error,
    5 failed}: throttle probe in the SAME tx —
    `SELECT status IN (4,5) AND finished_at > now() - interval '1 hour'
    FROM automation_run WHERE automation_id=$1 AND id <> $2 AND
    status <> 1 ORDER BY id DESC LIMIT 1`
    (rides automation_run_list_idx; semantics: alert on ENTRY into the
    failing state, at most hourly while continuously failing, any
    success re-arms). Not throttled → recipients: scope org →
    HoldersAt(manage_org, OrgScope); scope channel →
    HoldersAt(administer_channel, ChannelScope(...)) — only scopes 1/3
    exist (requireScopeAdmin precedent). RecordAutomationFailure in the
    finishRun tx; ping the RETURNED ids after WithTx commits.
  - Rationale (recorded): recipients mirror the WRITE gate
    (requireScopeAdmin) — whoever can edit the rule hears it broke. No
    fan-out cap needed: hourly throttle per rule + kind-6 email
    defaults OFF.
**Edge cases:** zero holders → no rows, the run still finalizes;
no creator special-case; re-execution of the same (automation, event)
cannot double-insert (run idempotency + dedupe key); statuses 3/6/7/8
never notify.
**Performance:** failure path only — one indexed probe + one holders
resolve + one batch insert; zero cost on the success path.
**Tests (TestAutomationFailureNotifies):** failing rule → every org
admin AND the owner get exactly one kind-6 row (entity 18, run id);
member/guest get none (red/green: resolve recipients without the
closure/verb path → this fails); immediate second failure → zero new
rows; backdate finished_at >1h via SQL → notifies again; a success
between failures re-arms; channel-scoped rule with a channel-specific
administer_channel assignment notifies THAT group, not org admins;
ping captured for an active admin, suppressed for a snoozed one
(capturing fanout); prefs PUT accepts kind 6; the email worker skips
kind 6 by default and sends it after opt-in.
**Gaps to record:** failure detail lives in ListRuns (the notification
is the doorbell); per-rule mute knob later; workspace/space scopes when
their admin verbs land.
**EXECUTION NOTES (#72):** the predicted P-20 overlap surfaced at the
serial-merge rebase as two test adaptations, folded into the feature
commit: `capturedMail.body` → `.text` (the Message seam rename) and
TestUnsubscribe's kind count 5 → 6 — whose failure log (`6:false`)
proved the prefKinds-registry pattern: the unsubscribe endpoint,
written before kind 6 existed, flipped it automatically.

### P-29 `identity: Profile edit, password change, session management.` — M — **[x] shipped #73**
Zero migrations (`auth_session` already has ip/user_agent/revoked_at;
FromToken already enforces revocation). Read `auth/auth.go`,
`identity/identity.go` (Bootstrap validation + Me), and
`middleware.go` clientIP first.
**Design (decided):**
- Commit 1 (metadata prep, no new endpoints): `CreateSession(ctx, q,
  userID, ip, userAgent)` and `Login(..., ip, ua)`; REST
  login/bootstrap/invite-accept handlers pass `clientIP(r)` and
  `r.UserAgent()` capped at 256 bytes, sanitized to a single line.
  Sweep ALL callers (`git grep CreateSession`). Empty values allowed.
- Commit 2 (sessions): auth exports `TokenHash` (today's hashToken).
  identity gains:
  - `Sessions(ctx, actor, currentHash)`: live rows (revoked_at IS NULL
    AND expires_at > now()) for the actor, newest first;
    `current` = token_hash match; pre-slice rows return "" ip/ua.
  - `RevokeSession(ctx, actor, sessionID)`: `UPDATE auth_session SET
    revoked_at = now() WHERE id=$1 AND user_id=$2 AND revoked_at IS
    NULL`; 0 rows → NotFound("session not found") — foreign, absent,
    and already-revoked are indistinguishable (oracle-free). Revoking
    the CURRENT session is allowed (that is logout).
  - `RevokeOtherSessions(ctx, actor, currentHash) (int64, error)`.
  - REST: `GET /api/v1/me/sessions` ·
    `DELETE /api/v1/me/sessions/{id}` (204) ·
    `DELETE /api/v1/me/sessions` → `{"revoked": n}`. Handlers derive
    currentHash from the presented bearer token.
  - KNOWN GAP (PR body + REALITY.md): an open websocket authed by a
    now-revoked session lives until reconnect (gateway auths at
    connect); REST is immediate. Live kick = a queued slice.
- Commit 3 (profile + password):
  - `UpdateMe(ctx, actor, fullName)`: trim → non-empty required (400)
    → max 100 runes (400) → control chars rejected (status.go
    precedent); interior whitespace allowed. (CORRECTED: this bullet
    originally said "mirror Bootstrap's full_name validation EXACTLY"
    — Bootstrap has NONE, it defaults an empty name to the email; the
    executor correctly STOPPED and these rules were pinned by the
    reviewer. Bootstrap's defaulting is untouched.) Org-scoped UPDATE.
    NOT event-logged (avatar/status precedent — comment it; clients
    see it on the next profile fetch).
    `PATCH /api/v1/me` {full_name} → updated MyProfile.
  - `auth.ChangePassword(ctx, pool, userID, current, new)`: verify
    current against user_credential (a missing credential row → the
    same Forbidden); wrong → apperr.Forbidden("current password is
    incorrect") (the token IS valid — 401 would be wrong). New
    password: mirror Bootstrap's rule + reject > 72 BYTES (bcrypt
    truncates silently; if Bootstrap lacks the 72 check, enforce here
    only and note it). Same tx: update hash + updated_at AND revoke
    all OTHER live sessions. `POST /api/v1/me/password`
    {current_password, new_password} → `{"revoked_sessions": n}`.
**Edge cases:** guests manage their own account normally; no agent
special-case; expired sessions absent from the list and 404 on revoke;
concurrent password changes → the loser's `current` fails.
**Performance:** single-row indexed ops on auth_session_user_idx.
**Tests (TestSessions / TestProfileEdit / TestChangePassword):** two
logins with distinct UA → list=2, correct `current`, ip/ua recorded;
revoke other → that token 401s immediately, current fine;
revoke-all-others with 3 live → {revoked:2}; FOREIGN session id → 404
and the victim's token stays valid (red/green: drop user_id from the
UPDATE's WHERE → this test catches cross-user revocation — the
load-bearing assertion); revoking the current session 401s your next
request (logout); PATCH name reflected in Me + Profiles; empty/too
long/control-char names 400; wrong current password → 403 and the old
password still logs in; correct change → old password fails at login,
the OTHER session's token 401s, the current token survives, the new
password logs in.
**Gaps to record:** password reset via email (P-35, needs P-20);
websocket kick on revoke; session labels/geo.
**EXECUTION NOTES (#73):** the executor STOPPED on the original
full_name bullet (spec bug, corrected above) and resumed after the
reviewer pinned the rules. Two behavior-required deviations shipped:
`RevokeSession` adds `expires_at > now()` (the spec's own "expired →
404" edge case demands it) and `auth.ChangePassword` takes the
presenting session's token hash (needed to revoke all OTHER sessions
in the same tx while the presenting one survives).

### P-31 `compliance: Audit read API.` — M — **[x] shipped #74**
Read `compliance/compliance.go` (gating pattern), `0001_event_log.sql`
(columns + the (org_id, id) index), and the search add() builder
precedent first.
**Design (decided):**
- `GET /api/v1/audit/events` — org-scope `compliance_officer` ONLY
  (`perms.Require(VerbComplianceOfficer, OrgScope)` inside the tx, the
  compliance.go pattern). F-9 invariant: owners/admins WITHOUT the
  explicit grant get 403.
- Filters (optional, AND-composed): `entity_type` (>0), `verb` (exact
  string; >64 chars → 400), `actor_id` (>0), `entity_id` (>0),
  `since`/`until` (RFC3339, on occurred_at; malformed → 400), `cursor`
  (id < cursor), `limit` (1..200, default 50, clamp not error).
  Dynamic WHERE via the parameterized add()-placeholder pattern —
  never interpolated values.
- Query: SELECT id, occurred_at, recorded_at, actor_kind, actor_id,
  entity_type, entity_id, verb, workspace_id, payload, origin FROM
  event_log WHERE org_id=$1 [...] ORDER BY id DESC LIMIT $n. Newest
  first; `next_cursor` = last id on a FULL page, else 0. `hint` is
  deliberately excluded (delivery routing noise, not audit data).
  Payload returns verbatim — F-4 guarantees structural deltas +
  revision references, never the only copy of content.
- Response: `{"events": [...], "next_cursor": N}` with RFC3339
  timestamps; payload/origin as raw JSON.
- No new index (decided): pages ride (org_id, id DESC); filters are
  scan predicates. Officer-gated cold path with LIMIT-bounded pages;
  sparse filters over a huge org can scan long — RECORD in the PR
  scale review; a covering (org_id, verb, id) index is the queued
  mitigation. No txid gate — this is a historical read, not a
  consumer; a late-committing row may appear on a later page (comment
  this at the query).
**Edge cases:** empty result → `events: []`, next_cursor 0; unknown
verb string → empty 200 (NO verb-registry validation — audits may
query removed verbs); partitioning is invisible to the query.
**Performance:** one indexed DESC scan per page; JSONB payload size
dominates — the 200 cap keeps pages sane.
**Tests (TestAuditReadAPI):** owner WITHOUT the grant → 403 (red/green:
drop Require → 200 — THE load-bearing test); with the grant (reuse
compliance_test's grant helper) → newest-first real events; verb /
entity_type / actor_id filters narrow exactly; since/until (place rows
via SQL UPDATE of occurred_at); cursor walk over >1 page — no overlap,
no gap vs one big query; limit clamps (0→50, 500→200); malformed since
→ 400; two orgs → each officer sees only their org.
**Gaps to record:** export lane pairs with P-32; per-entity
convenience route later; workspace-scoped officer reads when workspace
admin verbs land.

### P-33 `messaging: Oracle-free 404 for DM non-participants.` — S — **[x] shipped #75**
Resolves the 403/404 inconsistency recorded at P-08/#66: DM thread
read/send/mark-read return 403 to non-participants
(requireParticipant → Forbidden) while single-message Get is an
oracle-free 404.
**Design (decided):**
- `threads.go` requireParticipant: `apperr.Forbidden("not a participant
  of this conversation")` → `apperr.NotFound("conversation not
  found")`, with a contract comment: participation IS visibility — a
  non-participant cannot distinguish absent from denied (matches
  messaging.Get).
- That one change covers all three call sites (ListMessages, the
  InsertThreadMessage path, readstate). Sweep
  `git grep requireParticipant` to confirm nothing maps the Forbidden
  type specially.
- Update every test pinning 403 on DM paths — dm_leave_test.go asserts
  it explicitly (assertions AND comments) — via a
  `git grep -nE "403|Forbidden"` sweep over messaging/dm tests.
- Explicit NON-goal (decided): requireChannelMember STAYS 403. Channel
  existence semantics differ (public channels are listable; the
  private-channel question is P-34 NEEDS-DESIGN). Touch no channel
  gate.
**Edge cases:** gateway DM routing filters by participation (no error
path — unaffected); search ACL is a filter (unaffected); dm.Open/Leave
are already oracle-free (unaffected).
**Tests:** non-participant AND a departed leaver get 404 with body
"conversation not found" on thread read + send + mark-read (assert the
body does NOT echo the dm_space_id); participant flows unchanged;
single-message Get still 404 (regression pin).
**Gaps to record:** P-34 channel existence masking (NEEDS-DESIGN).

### P-35 `identity: Password reset via email.` — M — **[x] shipped #78**
Migration **0013_password_reset.sql** (the number is PINNED — P-19 in
this batch takes 0014; db.Migrate is filename-keyed and gap-tolerant,
verified). Read `auth/auth.go` (ChangePassword, session pattern),
`platform/mail/mail.go` (Message), and the router's preAuth limiter
first.
**Design (decided):**
- Migration 0013: `password_reset(id identity PK, user_id BIGINT NOT
  NULL REFERENCES user_account, token_hash TEXT NOT NULL UNIQUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), expires_at
  TIMESTAMPTZ NOT NULL, used_at TIMESTAMPTZ)` + index on (user_id).
  DB rows, not a stateless MAC: single-use and revoke-on-change
  REQUIRE server state.
- `identity.SetMailer(sender mail.Sender)` wired in weftd (the
  SetSigningSecret composition pattern). No mailer configured → the
  request endpoint still returns 200 and sends nothing (log once).
- `POST /api/v1/password-reset/request` {org_slug, email} — ALWAYS
  200 `{"ok": true}` regardless of outcome (user enumeration is the
  threat model). Behind the SAME preAuth withIPLimit as login. When
  the (org, email) resolves to a LIVE kind-1 user WITH a credential
  row: mint 32 random bytes hex, store sha256 hash (auth.TokenHash),
  TTL 1 hour; send a TEXT mail.Message: subject "[<brand>] Password
  reset", body carries the org slug, the TOKEN itself, the 1-hour
  expiry, and "ignore if you didn't request this" (API-first: no web
  page exists yet — the client-era link format is a recorded gap).
  Per-user throttle: if the user already has 3 unused unexpired
  tokens, return 200 and send NOTHING (silent — no oracle).
  Placeholder (kind 3) and credential-less accounts get the silent
  200 (claiming imported placeholders is its own future slice).
- `POST /api/v1/password-reset/confirm` {token, new_password} — also
  behind preAuth. Look up by token hash where used_at IS NULL AND
  expires_at > now() AND the user is live; every failure mode
  (unknown/expired/used token, deactivated user) is the SAME 401
  "invalid or expired token" (oracle-free). New password: min 8, max
  72 bytes (the ChangePassword rules). In ONE tx: verify+claim the
  token (UPDATE ... SET used_at = now() WHERE ... AND used_at IS NULL
  — the row claim IS the race guard), upsert the credential hash,
  revoke ALL live sessions (no exception — mailbox control resets
  everything), and DELETE the user's other outstanding reset rows.
  200 `{"ok": true}`.
- `auth.ChangePassword` (P-29) additionally DELETEs the user's
  password_reset rows in its tx — a changed password voids any
  in-flight reset mail.
**Edge cases:** double-confirm of the same token → second gets 401
(the claim UPDATE affected 0 rows); confirm then login works
immediately; a reset for a user with zero sessions still works;
email lookup is lower(email) org-scoped (the Login pattern);
concurrent confirms of the same token → exactly one wins.
**Performance:** all single-row indexed ops; the throttle count is
one indexed probe.
**Tests (TestPasswordReset):** full loop (request → capture the mail
via a recording Sender → extract token → confirm → old password 401s
at login, new works, EVERY prior session token 401s, second confirm
of the same token 401s); unknown email → 200 AND nothing sent
(assert zero captures — the oracle test); expired token 401
(backdate expires_at via SQL); 4th request in an hour sends nothing
(3 captured total); deactivated user silent; me/password change
voids an outstanding token (request → change via P-29 → confirm 401);
red/green: neuter the used_at claim (drop `AND used_at IS NULL`) →
the double-confirm test goes green-when-it-should-fail — prove red.
**Gaps to record:** client link format + reset web page (client
era); placeholder claiming; org-configurable TTL.

### P-14 `messaging: Sidebar pins and colors.` — S — **[x] shipped #79**
Zero migrations — `channel_member.pinned` (BOOLEAN, default false)
and `channel_member.color` (TEXT, nullable) have been dormant since
0003 (the C-4 quartet). This slice WAKES them; named custom sections
stay dormant (they need new schema — recorded gap). Read the
channel_member DDL and the existing `PUT /channels/{id}/notification`
handler/domain path first.
**Design (decided):**
- `PUT /api/v1/channels/{id}/sidebar` {pinned: bool, color: string} —
  a SEPARATE endpoint from /notification (different concern: sidebar
  presentation vs delivery), same gate shape: the caller must have a
  LIVE membership row (unsubscribed_at IS NULL) in an org-scoped
  channel → else oracle-free 404. Updates ONLY the caller's own row.
- color: "" clears (NULL); otherwise must match `^#[0-9a-f]{6}$`
  case-insensitively, stored lowercase → else 400 "color must be
  #rrggbb". pinned is a plain bool. Both fields REQUIRED in the body
  (PUT replaces the pair — no PATCH semantics; clients send current
  values).
- ListChannels surfaces `pinned` and `color` ("" for NULL) on each
  row. Ordering is UNCHANGED (clients sort pinned-first; backend-first
  honesty — recorded).
- NOT event-logged (personal presentation, the read-state precedent);
  no gateway work.
**Edge cases:** non-member and foreign-org channel → 404 (assert the
same body); unsubscribed member → 404; idempotent re-PUT; pin+color
survive channel rename/archive (archived channels still list for
members — flags ride along); guests may pin their own channels.
**Performance:** one UPDATE on the (channel_id, user_id) PK row.
**Tests (TestSidebarPrefs):** set pin+color → ListChannels reflects
both; clear color with "" → ""; bad colors 400 (`red`, `#12345`,
`#gggggg`); non-member 404 oracle-free (red/green: drop the
membership predicate from the UPDATE's WHERE → the non-member case
writes and the test catches it); second user's flags are independent
(personal, not shared); guest can pin their own channel.
**Gaps to record:** named sections (schema), pinned-first server
ordering, folder/section client interaction (client era).

### P-19 `files: Upload scan seam + org storage quota.` — M — **[x] shipped #80**
Migration **0014_file_org_live_index.sql** (PINNED; see P-35 note).
`file.scan_status` (0 pending · 1 clean · 2 quarantined) has been
dormant since 0006 (F-7). Read `files.go` Upload (the spool+hash
shape), `handlers_files.go`/`signed.go`/avatar+emoji read paths, and
`identity/admin.go` for the manage_org gate pattern first.
**Design (decided):**
- Migration 0014: `CREATE INDEX file_org_live_idx ON file (org_id)
  INCLUDE (size_bytes) WHERE deleted_at IS NULL;` — the quota SUM
  rides it.
- Scan seam: `files.Scanner` interface —
  `Scan(ctx, name, mime string, r io.Reader) (Verdict, error)` with
  `Verdict` = Clean | Quarantined. `files.SetScanner(s)` at
  composition (nil = no scanning, status STAYS 0 pending — we never
  fake "clean" without a scanner; the honest-rungs rule). Upload
  calls it AFTER the spool is fully read and size-checked (re-seek
  the spool; bounded by the 25 MiB cap), BEFORE store.Put. Clean →
  status 1; Quarantined → the file row is STILL created (status 2,
  the bytes stored — compliance/holds may need the evidence) but the
  response is 422 "file rejected by malware scan" and NO reference
  can ever form (see below). Scanner ERROR → fail CLOSED:
  apperr.Internal, no row, no blob.
- Quarantine gate at EVERY byte-read path: OpenDownload /
  authorizeDownload (bearer + signed link), OpenAvatar, and the emoji
  read all treat scan_status = 2 as an oracle-free 404 (same shape as
  deleted). The reference hook (message-link attach) skips
  quarantined files exactly like deleted ones — a quarantined file id
  in a message body creates NO reference.
- weftd config: NO driver registry yet (no real scanner exists —
  recorded honest rung). The seam ships with a test double only;
  clamav/ICAP is a later one-file driver.
- Quota: org.settings JSONB key `"storage_quota_bytes"` (int64; 0 or
  absent = unlimited). `PUT /api/v1/admin/storage-quota`
  {max_bytes >= 0} — manage_org-gated (org-scope Require), writes the
  settings key (jsonb_set), event-logged `org.quota_changed` (entity
  org, admin act — the admin.go precedent). `GET
  /api/v1/admin/storage-quota` → {max_bytes, used_bytes} (the SUM,
  manage_org). Enforcement in Upload AND StoreDocument: after size
  is known, `SELECT COALESCE(SUM(size_bytes),0) FROM file WHERE
  org_id=$1 AND deleted_at IS NULL` + incoming size > cap → 413
  "storage quota exceeded" (apperr taxonomy: add/reuse a 413-mapping
  error — if the taxonomy lacks one, use Invalid with the message and
  RECORD the 400-vs-413 reduction in the PR). Dedup note: an upload
  whose bytes already exist (same sha) still counts its row's
  size_bytes — quota is row-accounting, not blob-accounting
  (documented; blob-level accounting would undercharge the org that
  deletes its first copy).
- StoreDocument (compliance exports) IS quota-enforced (an org's
  export storage is its own usage) — record the operator note.
**Edge cases:** quota exactly at the boundary (== cap passes, +1
fails); GC purge frees quota (deleted_at set → SUM drops — assert
after a purge); quarantined files COUNT toward quota until GC'd
(rows exist); a 0-byte quota blocks all uploads; setting max_bytes
below current usage is allowed (blocks new uploads only).
**Performance:** one indexed SUM per upload (partial index, INCLUDE
column → index-only scan); self-host scale fine, per-org counter
table is the queued mitigation if uploads go hot.
**Tests (TestUploadScan / TestStorageQuota):** stub scanner: clean
upload → status 1, downloadable; quarantined → 422, row status 2,
bearer download 404, signed-link mint on it 404 (authorizeDownload),
avatar/emoji set with it impossible (upload path rejects first),
message link creates NO reference, GC treats it normally; scanner
error → 500 and NO row/blob; nil scanner → status stays 0 and
downloads work (today's behavior pinned). Quota: set cap → upload
under passes, over 413 (red/green: drop the SUM check → the over
case succeeds and the test catches it); == boundary passes; GET
shows used_bytes moving; purge frees; non-admin PUT quota 403;
quota event logged.
**Gaps to record:** no real scanner driver (seam only); async
re-scan lane; per-user quotas; blob-level dedup accounting; 413 vs
400 taxonomy if reduced.
**EXECUTION NOTES (#80):** no reduction was needed — apperr gained
append-only 413/422 kinds (spec-sanctioned). The executor flagged a
spec-internal tension and resolved it correctly, pinned by tests:
`max_bytes=0` = unlimited (the explicit definition wins); the
"0-byte quota blocks" edge bullet reads as a FULL quota rejecting.
Lesson recorded: edge-case bullets must not contradict the
definition they illustrate.

### P-32 `compliance: Export byte-bundles (zip).` — M — **[x] shipped #81**
Zero migrations (`export_job.scope` JSONB carries the new flag). Read
`compliance/export.go` (worker, the #45 pin mechanism), `files.go`
StoreDocument, and `blob.Store` first.
**Design (decided):**
- Commit 1 (prep, no behavior change): `files.StoreDocumentStream(
  ctx, actor, name, mime string, r io.Reader) (File, error)` — the
  existing spool+hash Upload shape without the 25 MiB cap (system
  artifacts; document why) but WITH the quota check (P-19 may or may
  not have merged — write the quota call ONLY if it exists on your
  base; otherwise record the interaction for the reviewer to wire at
  serial-merge, do NOT invent). `StoreDocument([]byte)` becomes a
  bytes.Reader wrapper over it.
- Commit 2 (feature): `POST /admin/exports` scope gains
  `include_files: bool` (default false — today's JSON behavior
  unchanged). When true the worker produces `export-<id>.zip`
  (mime application/zip) INSTEAD of the bare JSON:
  - `export.json` — byte-identical content to today's document.
  - `manifest.json` — {job_id, generated_at, file_count,
    total_bytes, missing: [file ids]}.
  - `files/<file_id>_<sanitized_name>` — one entry per file PINNED by
    this job's EntityExportJob references (#45's set, exactly that
    query), streamed via blob Open → zip entry (io.Copy — never
    buffer a whole file).
  - Streaming end-to-end: zip.Writer → io.Pipe → StoreDocumentStream
    in a goroutine; a write error on either side cancels both (the
    worker's existing failed-status lane).
  - A file whose blob is missing/unreadable at bundle time does NOT
    fail the job: skip the entry, list the id under manifest
    `missing`, keep going (evidence degradation is visible, never
    fatal).
- Name collisions inside the zip are impossible (the file_id prefix);
  sanitizeName reused for the suffix.
- The result file is pinned/downloadable exactly like today's JSON
  result (same result_file_id lane, same officer-gated download).
**Edge cases:** include_files on a job with ZERO attachments → a
valid zip with export.json + an empty-files manifest; a job whose
source messages died since pinning still bundles the bytes (that is
what the #45 pins are FOR — assert it); include_files=false output
is byte-identical to today (regression pin on the existing test's
expectations).
**Performance:** memory is O(one zip block), not O(bundle);
per-file streaming through the blob seam; the pin query is the
existing indexed reference lookup.
**Tests (TestExportBundle):** end-to-end: upload files, reference
them in messages, export include_files → download the zip (officer
token), open it in-test (archive/zip over the downloaded bytes):
export.json parses and matches the plain export of the same scope;
every pinned file present with exact bytes; delete a source message
+ GC-purge one file's blob out from under the pins via SQL/blob
delete → re-export: the purged one lands in manifest.missing while
the rest bundle (red/green: make the missing-file path fail the job
→ this test catches it); include_files=false byte-parity with the
pre-slice fixture; non-officer download still 403/404 per the
existing contract.
**Gaps to record:** eDiscovery/partner manifest format (this is the
raw bundle); zip64 for >4 GiB bundles (archive/zip handles it —
verify and note); no incremental/delta exports.
**EXECUTION NOTES (#81):** the spec-mandated "do NOT invent the
quota interaction" hold worked: the executor recorded it, and the
reviewer wired P-19's checkQuota into StoreDocumentStream while
resolving the predicted files.go rebase conflict, adding the e2e
pin (an over-quota export FAILS: status 4, no result) plus the
SetPerms wiring the pre-P-19 test scaffold lacked.

### P-15 `unfurl: Link previews with SSRF-guarded egress.` — L — **[x] shipped #84 (strongest-model executed)**
Security-critical: the server fetches attacker-chosen URLs. The egress
guard built here is the reusable core P-24 (outbound webhook steps)
inherits. Migration **0015_link_previews.sql**. One new dependency:
`golang.org/x/net/html` (the x/crypto precedent — parsing HTML with
regexes is how unfurlers get owned).
**Design (decided):**
- **Commit 1 — `egress: SSRF-guarded outbound HTTP client.`**
  New `internal/platform/egress`: a hardened client factory whose
  DialContext resolves the host itself, VETS every resolved IP, and
  dials ONLY a vetted literal IP (resolve-once-dial-pinned closes the
  DNS-rebinding TOCTOU; SNI/Host stay the original name). Rejected IP
  classes: loopback, RFC1918 private, link-local v4 (169.254/16 —
  cloud metadata) and v6 (fe80::/10), unique-local (fc00::/7),
  unspecified, multicast/broadcast, and IPv4-mapped v6 forms of all
  of those (Go's netip normalizes — Unmap() before classify). Scheme
  allowlist http/https; ports 80/443 only; userinfo in the URL →
  reject. Redirects: max 5, EVERY hop re-vetted (each redirect
  re-enters the pinned dialer; CheckRedirect re-runs the scheme/
  port/userinfo checks). Transport.Proxy = nil (env proxies would
  bypass the pinning). Timeouts: 5s dial, 10s total. Fixed
  User-Agent `<brand>Bot/1.0 (+link-preview)`; never sends
  Authorization or cookies. `Options.LookupIP` injectable and
  `Options.AllowLoopbackForTests bool` (grep-able, never set by
  weftd) exist ONLY so tests can reach httptest listeners; the
  SSRF matrix is unit-level and network-free.
- **Commit 2 — `unfurl: Link-preview cache schema (0015).`**
  `link_preview(id identity PK, url_hash TEXT NOT NULL UNIQUE —
  sha256 hex of the exact URL, url TEXT NOT NULL, title TEXT NOT
  NULL DEFAULT '', description TEXT NOT NULL DEFAULT '', image_url
  TEXT NOT NULL DEFAULT '', site_name TEXT NOT NULL DEFAULT '',
  status SMALLINT NOT NULL — 1 ok · 2 failed · 3 disallowed,
  fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(), expires_at
  TIMESTAMPTZ NOT NULL)` — GLOBAL cache (no org_id: a URL's preview
  is objective content; dedup across orgs, the Zulip per-server
  precedent). `message_link_preview(message_id BIGINT NOT NULL,
  preview_id BIGINT NOT NULL REFERENCES link_preview(id), position
  SMALLINT NOT NULL, PRIMARY KEY (message_id, preview_id))` + index
  on (message_id).
- **Commit 3 — `unfurl: Fetch consumer, read surface, org toggle.`**
  - `content.Links()` (mirrors Mentions): MarkLink hrefs in document
    order, SafeURL-filtered, http/https only (mailto skipped),
    deduped, first 2 per message (cap).
  - New `internal/domain/unfurl`: Runner = the standard named
    consumer ("unfurl", NOTIFY + sweep, txid-gated) on
    message.created events. Per event: skip if the org toggle is
    off; load the message's ast (org-scoped); extract links; for
    each: cache hit (url_hash, unexpired) → associate; miss → fetch
    through egress (Content-Type must be text/html or
    application/xhtml+xml; body read via 1 MiB LimitReader;
    x/net/html parse; og:title/og:description/og:site_name/og:image
    with <title> fallback; caps title 200 / description 500 /
    site_name 100 runes, control chars stripped) → upsert by
    url_hash (`ON CONFLICT DO UPDATE` refreshes content + expiry) →
    associate via message_link_preview. TTLs: ok 24h, failed 1h,
    disallowed 24h (guard verdicts don't flap). Guard-rejected URL →
    status 3 disallowed, NO retry loop. Self-URLs (config BaseURL
    host) skipped. image_url is STORED, never fetched (client-era
    camo/proxy is the recorded gap — rendering it raw leaks reader
    IPs; the API note says so).
  - Association event: when at least one preview attached, append
    `message.preview_added` {message_id + the message's container
    ids} in the SAME tx as the association — rides the standard
    gateway routing so open clients refresh the message.
  - Read surface: ListMessages/Get aggregates gain `link_previews:
    [{url, title, description, image_url, site_name}]` (status 1
    rows only), position-ordered — the ReactionAgg loading pattern.
  - Org toggle: `org.settings.link_previews_enabled` (ABSENT =
    enabled — Zulip/Slack default-on). `PUT/GET
    /api/v1/admin/link-previews {enabled}` — manage_org, jsonb_set,
    event `org.link_previews_changed` (the storage-quota endpoint
    precedent, owned by unfurl via SetPerms).
**Edge cases:** message edited to remove the link keeps existing
associations v1 (recorded gap: no un-unfurl); deleted message's
associations are dead rows reaped with the message lane later
(recorded); two messages with the same URL share one cache row; a
URL failing DNS entirely → failed (2), 1h retry; redirect chain
public→public→private rejected at the private hop (status 3);
non-HTML (image/pdf) → failed row, no parse; oversized HTML
truncated at 1 MiB and parsed as-is (x/net/html tolerates
truncation); titles with control chars/newlines flattened; consumer
crash between fetch and Ack → at-least-once re-run hits the cache
(idempotent upsert + PK association ON CONFLICT DO NOTHING).
**Performance:** fetches happen ONLY in the consumer (send path
untouched); ≤2 fetches per message, cache-deduped globally; 10s
worst-case per fetch bounded by the runner's serial sweep (a slow
site delays only the unfurl lane, never sends). Failed/disallowed
caching prevents refetch storms.
**Tests:**
- `egress` unit matrix (network-free, injected resolver): every
  rejected IP class incl. 169.254.169.254 and ::ffff:127.0.0.1;
  userinfo/scheme/port rejections; redirect-to-private rejected
  mid-chain; proxy env ignored. RED/GREEN: neuter the IP
  classification → the loopback case must fail the suite.
- `TestLinkPreviews` (e2e, AllowLoopbackForTests + httptest): OG
  page → preview row + association + `message.preview_added` event
  + ListMessages carries it; SECOND message with the same URL → hit
  counter proves ONE fetch; failed page (500) cached failed; length
  caps + control-char stripping asserted; org toggle off → zero
  fetches; redirect-to-loopback → disallowed (the e2e SSRF pin —
  RED/GREEN: drop the redirect re-vetting and this fails); non-HTML
  skipped; guest/member read previews identically (ride the message
  ACL — previews add NO new visibility surface).
**Gaps to record:** image proxy/camo (client era); un-unfurl on
edit; per-message/user opt-out; charset sniffing beyond UTF-8;
oEmbed providers; P-24 shares the egress guard when it lands.

### P-16 `channels: Web-public channels + history_from enforcement.` — L — **[x] shipped #87** (strongest-model execution; all 3 security assertions red/green-proven)
Security-critical: it opens a NEW exposure surface (an anonymous,
unauthenticated read path) and enforces a membership-history boundary
that has never been enforced. ZERO migrations — `channel.visibility`
(value 3 web_public), `channel.history_mode` (2 protected), and
`channel_member.history_from` all exist since 0003 and have only been
carried in comments. Read the channel DDL, `requireMember`,
`requireThreadSend` (line ~515), `JoinChannel`, and the router's
`withAuth`/`withIPLimit` first.
**AUDIT FINDINGS (drive the design):**
- The send path `requireThreadSend` CALLS `requireMember` — so the
  read gate must NOT be loosened in place; a separate read gate is
  mandatory or non-members could post to web-public channels.
- `history_from` is referenced only in comments; nothing sets or
  reads it, and no API path creates a `history_mode=2` channel. The
  feature is unbuilt, not merely unwired.
**Design (decided):**
- **Commit 1 — `channels: Create and expose web-public channels.`**
  `CreateChannelParams` gains `Visibility string`
  ("public"|"private"|"web_public"; empty falls back to the legacy
  `Private bool` for wire compatibility) → column 1/2/3. A web_public
  channel is FORCED `history_mode=1` (shared) — world-readable history
  is the point; protected+web_public is a 400. `JoinChannel` allows
  self-join for public AND web_public (private stays invitation-only).
  `ChannelSummary` gains `visibility string` (keep `private` bool);
  ListChannels shows web_public org-wide like public. Event payload
  carries visibility.
- **Commit 2 — `messaging: Diverge the channel read gate for web-public.`**
  New `requireChannelRead` = live membership OR
  (`channel.visibility=3 AND archived_at IS NULL`). Applied to the
  authenticated READ paths ONLY: `ListThreads`, `ListThreadMessages`,
  and the `Get` message inline ACL (add a web_public branch). WRITE
  paths are UNTOUCHED — `requireThreadSend`, `CreateThread`, pins keep
  `requireMember`. This is the divergence the audit demands: a
  web-public channel is world-readable, member-writable. Scope: reads
  cover messages + thread lists; pins/files/search stay member-only
  (recorded gap).
- **Commit 3 — `rest: Anonymous read path for web-public channels.`**
  A `withPublic` wrapper (mirrors `withAuth` but resolves NO identity,
  reads NO token, applies the per-IP limiter) over a small, explicit
  allowlist of routes under `/api/v1/public/`:
  `GET /public/channels/{id}` (metadata),
  `GET /public/channels/{id}/threads`,
  `GET /public/threads/{id}/messages`. New identity-free domain
  methods (`PublicChannel`, `PublicChannelThreads`,
  `PublicThreadMessages`) enforce `visibility=3 AND archived_at IS
  NULL` in SQL (defense in depth — the gate is in the query, not just
  the routing); anything else is an oracle-free 404. Message
  projection: id, thread_id, author_id, source, rendered, created_at,
  plus `link_previews` (objective public content). Reactions are
  EXCLUDED — `ReactionAgg.UserIDs` would leak org-member identities to
  the anonymous internet (recorded gap: public reaction counts later).
  An `authors` map {user_id → {full_name}} for the authors of the
  RETURNED messages only (the Zulip web-public model — bounded, never
  the directory); avatar bytes stay on the authed endpoint (gap).
  Gateway, search, DMs, posting, joining: all remain authed and thus
  closed to anonymous — the safe default, asserted.
- **Commit 4 — `messaging: Enforce protected-channel history_from.`**
  `CreateChannelParams` gains `Protected bool` → `history_mode=2`,
  valid ONLY with private (else 400; web_public/public force shared).
  Every membership INSERT into a protected channel stamps
  `history_from = now()` on the NEW row (JoinChannel never applies —
  protected ⟹ private ⟹ invitation; the real site is
  `identity.joinChannelOnAccept`, the guarded INSERT…SELECT from #68 —
  add `history_from = CASE WHEN c.history_mode=2 THEN now() END`).
  Rejoin PRESERVES the original (the ON CONFLICT branch only clears
  unsubscribed_at — already correct, assert it). The read paths
  (`requireChannelRead`/ListThreadMessages/Get) filter a member's
  view of a protected channel to `created_at >= history_from` (NULL =
  all; the creator's NULL means "from creation" = all, correct). A
  web_public reader never hits this (web_public ⟹ shared).
**Edge cases:** archived web_public → not readable (anon 404, authed
non-member gate closes); a private channel is never anonymously
visible (oracle-free 404, same body as nonexistent); an authed
non-member can READ a web_public channel but a POST still 403s
(requireMember on the write path — THE audit pin); guest reading
web_public is allowed (world-readable trumps the guest boundary);
protected history_from is per-member and immune to leave/rejoin;
deleted messages never appear on either path.
**Performance:** the read gate is one added EXISTS branch (indexed on
channel PK + the membership PK); the history_from filter is a
parameter on the existing message query; the anonymous methods are
single indexed reads. No new indexes.
**Tests:**
- `TestWebPublicChannels` (authed): non-member reads a web_public
  channel's threads + messages + Get; non-member POST → 403 (RED/
  GREEN: point the write path at requireChannelRead → the non-member
  posts and this fails — the exact audit hole); private non-member
  still blocked; archived web_public not readable; ListChannels shows
  web_public to a non-member.
- `TestPublicChannelAnon` (no Authorization header): reads metadata +
  threads + messages + author names + link previews; a public
  (non-web) channel and a private channel both 404 (oracle-free, same
  body); reactions absent from the projection; POST/join/search/
  gateway on the public namespace or without a token → 401/404
  (closed). RED/GREEN: drop the `visibility=3` clause from
  PublicThreadMessages → a private channel's messages leak to
  anonymous and the oracle test fails.
- `TestProtectedHistory`: create private protected; post m1; invite a
  new member (history_from stamped); the newcomer sees only
  post-join messages, the creator sees all; leave+rejoin preserves
  the boundary. RED/GREEN: drop the `created_at >= history_from`
  filter → the newcomer sees m1 and the test fails.
**Gaps to record:** web-public pins/files/search/reactions;
anonymous avatar bytes; live updates for anonymous (poll only);
per-channel "allow web-public" org policy knob; robots/SEO/caching
headers on the public routes (client era); protected mode only
settable at creation (a PATCH to flip history_mode is a later slice).

---

### P-17 `compliance: Message retention vacuum.` — L — **[x] shipped #90** (strongest-model execution; hold guard + restore-window bound both red/green-proven; the security/compliance trio is complete)
Completes AD-3: `retention_policy.duration_days` has had full CRUD +
validation + `TestRetentionPolicy` since #62, but NOTHING consumes it —
no lane removes messages by age. This slice is the enforcement lane.
Read `compliance/janitor.go` (the host — its file/scrub lanes are the
pattern), `messaging/edit.go` `DeleteMessage` (the tombstone shape to
reuse), and the `retention_policy`/`legal_hold` DDL first.
**AUDIT FINDINGS (drive the design):**
- `duration_days` is fully wired end-to-end EXCEPT enforcement; `-1`
  means keep forever (the common case — must be excluded from the
  sweep).
- `DeleteMessage` already defines the tombstone: capture content into a
  kind-4 `message_revision` row, blank the live row (`source=''`,
  `ast='{}'`, `rendered=''`, flags off, `deleted_at=now()`), drop pins,
  decrement the thread count, emit an event. The vacuum reuses this
  shape, writing the same tables directly from the compliance package
  exactly as `scrubRevisions` already does.
- `event_log.entity_id` is a plain BIGINT with NO FK to `message` — the
  audit spine survives removal; the lane logs its own verbs.
- FKs INTO `message(id)` that a hard purge must clear first (NOT NULL
  unless noted): `message_revision`, `reaction`, `pin`, `saved_item`,
  `message_user_flag`, `message_report`; nullable back-refs to NULL:
  `scheduled_message.sent_message_id`, `reminder.message_id`,
  `thread.root_message_id`; `message_link_preview` has no FK (P-15) but
  is cleared to avoid orphans.
- The scope ladder is `scrubRevisions`' nearest-wins channel(3)→org(1);
  workspace/space/dm rungs are a recorded gap there and stay one here
  for consistency.
**Design (decided): archive-shape = in-place tombstone** (reuses the
delete machinery, stays in-package, one nullable marker column, cheapest
to migrate away from later; the dedicated-archive-table variant is a
scale optimization deferred until volume calls for it).
- **Commit 1 — `compliance: Vacuum messages past their retention age.`**
  Migration adds `message.retention_vacuumed_at TIMESTAMPTZ` (nullable;
  distinguishes a retention tombstone from a user delete). New janitor
  lane `vacuumMessages`: candidate scan for `deleted_at IS NULL` rows
  whose `created_at < now() - (effective duration_days)` where the
  effective policy is not forever (-1), NOT under an active
  `legal_hold` (custodian = author OR held channel = message channel —
  the scrub lane's guard), batched with the same per-row lock +
  in-tx recheck the file lane uses. Each eligible row is tombstoned
  (kind-4 revision capture + live-row blanking + pin drop + thread-count
  decrement) and logged `retention.message_vacuumed` (system actor).
  Invisible everywhere immediately — every read filters `deleted_at IS
  NULL`, incl. the P-16 authed + anonymous paths.
- **Commit 2 — `compliance: Purge vacuumed messages after the restore window.`**
  New janitor field `VacuumRestoreWindow` (default 30d, mirrors the file
  `DeadRefWindow`). New lane `purgeVacuumedMessages`: rows with
  `retention_vacuumed_at < now() - window` AND still not legal-held are
  permanently removed — clear the eight child rows / null the three
  back-refs above inside one tx per message (the lock+recheck pattern),
  then `DELETE` the row, log `retention.message_purged`. The window is
  the safety net against a misconfigured short policy. A re-held message
  (hold added during the window) is skipped and stays tombstoned.
**Edge cases:** a `-1` (forever) effective policy never vacuums; a hold
placed AFTER a soft-vacuum freezes the purge (message stays recoverable);
a message already user-deleted is skipped by the vacuum (`deleted_at`
set) but its own `deleted_at`-based file/revision reclaim is unaffected;
purging a thread's root message nulls `thread.root_message_id` (the
thread row and its summary survive); a forwarded message whose original
is purged keeps its own copied content (forwards copy, never reference —
P-03). Cross-org impossible: every query is org-scoped via the message's
`org_id` and the policy's `org_id`.
**Performance:** both lanes are batched (200/sweep) keyset scans over
`message` filtered by `deleted_at`/`retention_vacuumed_at` + a correlated
policy lookup; no new index in v1 (the sweep is a low-frequency
background lane, hourly like the file janitor) — a partial index on
`(retention_vacuumed_at)` is a noted follow-up if sweep latency shows up.
**Tests (`TestRetentionVacuum`, real Postgres):**
- age boundary: a message older than a 1-day channel policy is
  vacuumed; a fresh one is not; a `-1` org scope never vacuums.
- ladder: a channel policy overrides a stricter/looser org default.
- **legal-hold guard, RED/GREEN**: a message under a custodian/channel
  hold is NOT vacuumed; neutering the hold predicate vacuums it and the
  assertion fails. Same red/green for the purge lane (a hold added
  during the window blocks permanent removal).
- tombstone correctness: content moved to a kind-4 revision, live row
  blanked, pins dropped, thread count decremented, invisible on the
  P-16 authed + anonymous read paths, `retention.message_vacuumed`
  logged, `event_log` intact.
- purge correctness: after the window every listed child row is gone /
  back-ref nulled, the message row is removed, `retention.message_purged`
  logged; a not-yet-elapsed tombstone is left intact (RED/GREEN on the
  window boundary).
**Gaps to record:** first-class restore API (needs content re-render,
which lives in messaging — recovery is DB-level within the window for
now); workspace/space/dm policy rungs; per-partial-index tuning;
attachment blob reclaim still flows through the existing file dead-ref
lane once the message tombstone lands (documented linkage, not a new
path).

---

### P-11 `worktrack: Sprints.` — M — **[x] shipped #92** (Opus-executed; carry-over guard red/green-proven)
ZERO migrations: the `sprint` table (state 1 future · 2 active · 3
closed, `starts_at/ends_at/completed_at`, `sprint_space_idx`) and
`work_item.sprint_id` (FK + partial index) have existed since 0005 with
no writers — the Tier-2 OPEN "field vs join table" was answered by the
schema (a COLUMN: one sprint per item, Jira's active-sprint model).
Read `internal/domain/worktrack/worktrack.go` (CreateSpace/CreateItem/
UpdateItem patterns, loadSpace, org-scoped `perms.VerbEditItems`) and
`migrations/0005_work_tracking.sql` lines 154–172 first.
**Design (decided):**
- Service methods in a NEW `internal/domain/worktrack/sprints.go`; REST
  in `handlers_worktrack.go` + router entries. All gates
  `perms.VerbEditItems` at org scope (the existing item-edit gate; a
  per-space admin verb is a recorded gap for the whole module).
- `POST /api/v1/spaces/{id}/sprints` {name, goal, starts_at, ends_at}
  → a `state=1` (future) sprint. name required, trimmed, 1..100 runes;
  goal ≤2000; when both dates present, ends_at > starts_at (400).
  Space must be org-local + live (oracle-free 404 otherwise — the
  loadSpace shape). Event `sprint.created` (EntityType: new
  `enum.EntitySprint` — add the next free constant; payload sprint_id,
  space_id, name).
- `GET /api/v1/spaces/{id}/sprints` → all the space's sprints ordered
  state ASC, id DESC, each with {id, name, goal, state, starts_at,
  ends_at, completed_at, item_count} — item_count from ONE grouped
  `count(*) FILTER (WHERE trashed_at IS NULL)` join, no N+1.
- `POST /api/v1/sprints/{id}/start` {starts_at?, ends_at?} → 1→2 only
  (starting an active/closed sprint 400). Stamps starts_at (body value
  or now), ends_at if given. ONE ACTIVE SPRINT PER SPACE (Jira parity):
  if another state=2 sprint exists in the space → 409. Event
  `sprint.started`.
- `POST /api/v1/sprints/{id}/close` {move_to_sprint_id?} → 2→3 only
  (400 otherwise); stamps completed_at. CARRY-OVER (the Tier-2 OPEN,
  decided): UNFINISHED items (`resolved_at IS NULL AND trashed_at IS
  NULL AND sprint_id = this`) move to `move_to_sprint_id` — which must
  be same-space and state ≠ 3 (400) — or to the backlog
  (`sprint_id = NULL`) when absent. FINISHED items keep their sprint_id
  (sprint history is the report). One UPDATE, count returned in the
  `sprint.closed` event payload {sprint_id, moved, moved_to}.
- `UpdateItemParams` gains `SprintID *int64` (0 clears — the AssigneeID
  precedent). Validation in the SAME loadItem tx: the sprint exists,
  belongs to the item's space, and state ≠ 3 (assigning into a closed
  sprint 400; clearing always allowed). Covered by the existing
  `workitem.updated` event (add "sprint_id" to its changed-fields
  payload the way the current fields are reported).
**Edge cases:** two spaces each with an active sprint — fine (the rule
is per-space); starting a second sprint in the same space 409; closing
with move target = the sprint being closed 400; a trashed item never
carries over; a foreign-org sprint/space is an oracle-free 404
everywhere; items in OTHER sprints are untouched by close.
**Tests (`TestSprints`, real Postgres, e2e REST):** lifecycle
create→start→close with date stamps + events; the one-active 409;
carry-over — unfinished move to the named target, finished stay, a
second close moving to backlog (NULL); assign/clear sprint on items
incl. closed-sprint 400 and cross-space 400; foreign-org 404s.
RED/GREEN (pin in a comment): drop `resolved_at IS NULL` from the
carry-over UPDATE → finished items move too and the "finished stay"
assertion fails.
**Gaps to record:** per-space admin verb (sprint CRUD rides
edit_items org-wide); sprint reports/velocity; auto-create-next on
close; workitem.sprint_changed as a first-class automation trigger.

---

### P-12 `worktrack: Board ordering (LexoRank) and saved views.` — M — **[x] shipped #93** (Opus-executed; rank-collision red/green-proven)
ZERO migrations: `work_item.rank TEXT COLLATE "C"` +
`rank_context_id` + `work_item_rank_idx` and the whole `view_def`
table (layout 1 list · 2 kanban · 3 timeline · 4 saved-search, query
JSONB, owner_id) have existed since 0005; REALITY records "rank is
static v1". Every space already owns a rank_context (seeded at
CreateSpace). Read worktrack.go (ListItems ordering, loadSpace),
0005 lines 67–72 + 239–254 first.
**Design (decided):**
- **Rank alphabet**: lowercase `a`–`z` only, byte order (COLLATE "C"
  makes it authoritative). Helper `rankBetween(lo, hi string) (string,
  ok)` in a new `rank.go` with unit tests: midpoint of two strings
  ("" lo = start sentinel, "" hi = end sentinel); appends when needed
  (between "ab" and "ac" → "abm"...); returns !ok only when lo and hi
  are equal or adjacent with no midpoint (caller rebalances).
- `POST /api/v1/items/{id}/move` {after_item_id?, before_item_id?} —
  at least one (400 if neither); both items must be live, org-local,
  and share the moved item's rank_context (else 400/404 — cross-space
  moves are NOT this slice). Gate `VerbEditItems`. In ONE tx: lock the
  context's items `FOR UPDATE` ordered by rank; **NULL-rank backfill**
  — if any item in the context has NULL rank (pre-P-12 rows), assign
  evenly spaced ranks in id order first; compute the neighbor pair
  from after/before, `rankBetween`; on !ok REBALANCE the whole context
  (evenly respaced, ~3-char strings) and retry once. Update the moved
  item's rank; event `workitem.reordered` {item_id, after, before}.
- `ListItems` ORDER BY changes to `rank NULLS LAST, id` — the board
  order surfaces everywhere items list.
- **Saved views** (`views.go` + handlers): personal in v1 (owner_id =
  creator; sharing is a recorded gap).
  `POST /api/v1/views` {name 1..100, layout 1..4, space_id, query,
  config?} — query shape v1: `{"filters":[{"field":F,"op":O,"value":V}]}`
  with F ∈ {status_id, type_id, assignee_id, sprint_id, label,
  flagged}, O ∈ {eq, in} (in = array value); anything else 400 AT
  WRITE. space_id must be org-local (404). Stored with space_id inside
  query JSON (the column set has no space_id — keep the validated
  space in the query object).
  `GET /api/v1/views` → caller's own views. `PATCH /api/v1/views/{id}`
  (name/layout/query/config, re-validated) and `DELETE` — owner-only,
  a foreign or other-owner view is an oracle-free 404.
  `GET /api/v1/views/{id}` → one view (owner-only, oracle-free 404).
  ListItems is NOT auto-filtered by a view — a view is a saved query
  the client applies; server-side view execution is a recorded gap.
**Edge cases:** move with neither neighbor 400; neighbors in a
different rank_context 400; a context whose items are all NULL-rank
backfills before the move; rebalance-then-retry once when the midpoint
is exhausted; an unknown filter field/op 400 at both POST and PATCH;
another owner's view is an oracle-free 404 on GET/PATCH/DELETE.
**Tests (`TestBoardOrder` + `TestSavedViews`, real Postgres):**
`rankBetween` unit table (start/end sentinels, appends, adjacent
!ok); reorder via after/before with a live re-query proving the new
order; NULL-rank backfill; a forced rebalance (seed adjacent ranks)
and the single retry; cross-context 400. Views: CRUD, query-validation
400s, owner-isolation 404s. RED/GREEN (pin in a comment): make
`rankBetween` return the `lo` bound → two items collide on rank and
the order assertion fails.
**Gaps to record:** server-side view execution (filter → ListItems);
shared/team views; cross-space/cross-context moves; timeline/kanban
server support beyond storage.

---

### P-13 `worktrack: Custom fields and item links.` — L — **[x] shipped #94** (Opus-executed; field-type-check red/green-proven)
ZERO migrations: `field_def` (typed taxonomy, `applies_to` item-type
ids, `required`, `options` JSONB, `UNIQUE(space_id,key)`),
`work_item.fields` JSONB (GIN-indexed values), and `link_type` +
`work_item_link` (keyed by internal ids so links survive moves,
`UNIQUE(from,to,type)`) all exist since 0005. Read worktrack.go
(CreateItem/UpdateItem, loadSpace, how status_set/item_type are
seeded) and 0005 lines 45–63 + 124–145 first.
**Design (decided):**
- **Field type set v1** — a Go registry constant, a subset of the
  schema taxonomy: `text_short`, `text_long`, `number`, `date`,
  `checkbox`, `select`, `multi_select`. Any other type 400s until its
  validator lands (recorded gap). One `fieldValidate(def, value)` in a
  new `fields.go`, unit-tested per type.
- **Field defs** (`fields.go` + handlers), gate `VerbEditItems` org
  scope: `POST /api/v1/spaces/{id}/field-defs` {key, name, field_type,
  applies_to?, required?, options?} — key matches the schema CHECK
  `^[a-z][a-z0-9_]{0,62}$`, unique per space (pre-check the index like
  CreateChannel → 409 on dup); select/multi_select require
  `options.choices` (non-empty string array) else 400; applies_to ids
  must be the space's item types (400). Event `field_def.created`
  (new `enum.EntityFieldDef`). `GET` → the space's defs ordered
  position, id. `PATCH`/`DELETE` — name/required/options/position
  mutable; key + field_type IMMUTABLE (a type change would strand
  stored values). DELETE removes the def only; existing values in
  `work_item.fields` are left as inert orphans (a strip-sweep is a
  recorded gap) — no mass UPDATE, so it stays O(1) at scale.
- **Value validation** woven into CreateItem/UpdateItem: a new
  `Fields map[string]any` on both param structs. On write, load the
  space's field_defs ONCE, validate each supplied key against its def
  (unknown key 400; v1 validates SUPPLIED values and does not force
  required-field presence — recorded gap), then store into
  `work_item.fields`. UpdateItem merges (absent key = leave, explicit
  null = clear); the `workitem.updated` payload lists the changed
  field KEYS only (never values — the mention-id precedent).
- **Item links** (`links.go` + handlers), gate `VerbEditItems`:
  `POST /api/v1/items/{id}/links` {to_item_id, link_type_id} — both
  items org-local + live; link_type org-local; no self-link (400);
  `UNIQUE(from,to,type)` → 409 on dup; the inverse is IMPLICIT (ONE
  row, rendered both ways via link_type inward/outward — never two
  rows). Event `workitem.linked`. `GET /api/v1/items/{id}/links` →
  links in both directions with the resolved phrase and the other
  item's {id, key, title, status}. `DELETE .../links/{link_id}` from
  either endpoint (event `workitem.unlinked`). System link types
  (blocks / is blocked by; relates to) seeded per org — mirror EXACTLY
  how status_set/item_type seeding works (read it; do not invent a
  second seeding path).
- **F-5 consent polish**: a cross-space link is allowed only when the
  actor can edit both spaces — `VerbEditItems` is org-wide in v1 so
  this holds today; the per-space consent ceremony is a recorded gap,
  stated not faked.
**Edge cases:** unknown field key 400; wrong-type value 400 (string
into number, non-choice into select, non-array into multi_select);
clear via explicit null; def delete leaves inert orphan values;
self-link 400; duplicate link 409; foreign-org item/link 404; unlink
from either endpoint; a link survives a status/rank change on either
item (keyed by id, asserted).
**Tests (`TestCustomFields` + `TestItemLinks`, real Postgres):**
`fieldValidate` unit table per type; def CRUD incl. dup-key 409 and
options validation; value set/merge/clear with the GIN round-trip;
def delete leaving orphans. Links: both-direction render, self 400,
dup 409, unlink, cross-org 404, link survives an item change.
RED/GREEN (pin in a comment): drop the type check in `fieldValidate`
→ a string lands in a number field and the type assertion fails.
**Gaps to record:** required-field presence enforcement; the remaining
field types (user/version/cascading/…); per-space admin verb (the
whole module rides `edit_items` org-wide); orphan-value strip sweep;
link graph / blocked-by rollups; cross-space consent ceremony.

---

### The automation cluster ships SERIALLY: P-22 → P-23 → P-24
All three rewrite the SAME functions (`Definition`/`validateDefinition`
in automation.go, `match`/`execute` in runner.go) — parallel branches
would guarantee semantic merge conflicts in injection-sensitive code.
Each slice branches off dev only AFTER the previous one has merged,
and each spec assumes its predecessor is in the tree. Migration
numbers are fixed by this order: P-23 = 0017, P-24 = 0018.

### P-22 `automation: Conditions and templating.` — M — **[x] shipped #97** (Opus-executed; mention-multiset guard + strict-eq-typing red/green-proven)
ZERO migrations: `automation.definition` is JSONB and the 0006 comment
already reads "trigger → conditions → steps". Read automation.go
(Definition/validateDefinition), runner.go (match/execute, how
`ev.Payload` flows), content/parse.go + content.go (goldmark parse,
`@**Full Name**` mention syntax, `NodeMention`, how labels render, the
`MentionResolver` seam — a resolver returning ok=false still creates
the mention NODE with its label), and the four trigger verbs' payload
append sites (threads.go message.created; reactions.go;
worktrack.go workitem.created/status_changed) first.
**Design (decided):**
- **Conditions** — Definition gains `Conditions []Condition` (optional;
  absent = legacy definitions stay valid forever; DisallowUnknownFields
  stays on). `Condition{Path, Op, Value}`; ≤10, ANDed. Path grammar:
  `event.` + 1..5 dot segments of `[a-z0-9_]+`, resolved into the
  trigger event's payload. Ops: `eq`/`ne` (number|string|bool,
  same-JSON-type only), `gt`/`lt`/`gte`/`lte` (numbers only),
  `contains` (string substring), `in` (array of ≤20 same-type scalars),
  `exists`/`not_exists` (Value must be ABSENT — 400 otherwise).
  validateDefinition 400s bad path syntax, unknown ops, and
  type-invalid values per op.
- **STRICT typing, no coercion ever**: `"42"` never equals `42`.
  Decode `ev.Payload` ONCE per evaluation with
  `json.Decoder.UseNumber()` — BIGINT ids exceed float64's 2^53, so
  `eq`/`ne`/`in` on numbers compare the canonical `json.Number` string
  when both sides are integers (no `.`/`e`), else via Float64;
  `gt`/`lt`/`gte`/`lte` compare Float64. Missing path: only
  `exists`/`not_exists` can pass; a present-but-null value counts as
  absent (document in the code).
- **Conditions evaluate inside `match()`** (in-memory, before any DB
  work): a condition miss creates NO run row — deliberate at Slack
  scale (an org-wide message.created rule with conditions must not
  write one row per non-matching message). The AU-2 dry-run/expression
  debugger is the recorded gap for "why didn't it fire".
- **Templating** — `{{event.path}}` spans in `post_message` Content
  only (same path grammar). validateDefinition scans content: every
  `{{…}}` span must parse as a valid path (400 else); ≤20 spans; text
  outside valid spans is untouched. At execute, per step: resolve
  against the UseNumber-decoded payload — string verbatim,
  `json.Number` verbatim, bool `true`/`false`, missing/null → `""`;
  object/array → the step FAILS with trace error "path resolves to a
  non-scalar". Post-expansion content must be 1..4000 chars, else the
  step fails (never silently truncate).
- **Mention-injection guard (the load-bearing security assertion).**
  Payloads today carry no free text, but P-23 adds webhook bodies and
  slash text — attacker-influenced values MUST NOT mint mentions
  (`@**Name**` fans out real notifications via `doc.Mentions()`).
  The guard is STRUCTURAL, not escaping: (1) at validate time, parse
  the literal step content (no-op resolver) and 400 if any mention
  node's LABEL contains a template span — authors may not template
  inside mention syntax (the mention-a-variable-user feature is a
  recorded gap; it returns later as an id-typed step field, which is
  injection-free by construction); (2) at execute time, parse the
  EXPANDED content (no-op resolver) and require its mention-label
  MULTISET to equal the literal content's — any drift fails the step
  with trace error "template expansion may not alter mentions".
  Multiset equality (not node COUNT) is required: a crafted value can
  backslash-suppress a literal mention while smuggling a new one for a
  net-zero count. Formatting injection from values (bold/code spans)
  is cosmetic and accepted — record it. The literal-side parse happens
  once per rule load, not per event.
**Edge cases:** legacy definitions (no `conditions` key, no spans)
behave exactly as today; `{{` without a valid path inside → 400 at
write, never a runtime surprise; unknown-at-runtime path → `""`;
condition on a P-23 verb's nested field (`event.body.x`) works via the
dot path; strict-type mismatch (string vs number) is simply false, not
an error.
**Tests (`TestAutomationConditions` + `TestAutomationTemplating`, real
Postgres, end-to-end through the runner like `TestAutomationRunner`):**
unit tables for the path parser, condition eval (every op, strict-type
mismatches, integer-exact eq beyond 2^53, null-as-absent), and the
template scanner. E2E: an eq condition on channel_id gates a rule (one
channel fires, the other doesn't — and the non-matching event leaves
NO run row, asserted); `in`/`exists`; a workitem.created rule posting
"New item {{event.key}}" produces the real key; non-scalar path and
post-expansion overflow both surface as failed runs with the trace
error. **Mention smuggle e2e:** append a synthetic event via
`eventlog.Append` in the test whose payload carries
`"x": "@**<real member name>**"`, rule content `{{event.x}}` → the
step FAILS, no message posts, no mention notification exists.
RED/GREEN (pin in a comment): neuter the label-multiset comparison →
the smuggled mention posts, `doc.Mentions()` fans out, and the
no-notification assert goes red. Second pin: allow type coercion in
`eq` → `"42"` matches `42` and the strict-typing assert goes red.
**Gaps to record:** OR/if-else condition groups; query conditions
(ADR-010 DSL); user/group conditions; for-each + `{{issue}}`-style
rebinding (AU-1); dry-run + expression preview (AU-2); template
filters/`{{json …}}`; mention-a-variable-user as an id-typed field;
templating in step kinds beyond post_message.

### P-23 `automation: Schedules, inbound webhooks, slash triggers.` — L — **[x] shipped #98** (Opus-executed; webhook-token + slash-membership red/green-proven; review folded in a scheduler busy-loop fix + a DST-gap past-fire fix)
Migration `0017_automation_triggers.sql`: `ALTER TABLE automation ADD
COLUMN schedule_next_at TIMESTAMPTZ, ADD COLUMN webhook_token TEXT;`
plus `CREATE INDEX automation_schedule_due_idx ON automation
(schedule_next_at) WHERE schedule_next_at IS NOT NULL AND enabled AND
deleted_at IS NULL;` (the claim query's exact predicate). Read
runner.go (Run/sweep/match/execute), automation.go (Create/Update —
where enabled/definition transitions happen), messaging's scheduled
loop (`RunScheduledLoop` — the claim pattern), the P-16 public
endpoints + P-20 unsubscribe (the outside-withAuth precedent), and
ratelimit usage in router.go first.
**Design (decided):**
- **Trigger kinds** — `Trigger{Kind, Verb, Schedule *Schedule,
  Command}`. Kind absent = `"event"` (every stored definition stays
  valid; normalize on load). `event` → Verb from triggerVerbs as
  today. `schedule` → Schedule required. `webhook`/`slash` → no Verb.
  The three internal verbs (`automation.schedule_due`,
  `automation.webhook_received`, `automation.slash_invoked`) NEVER
  enter triggerVerbs — they are not event-pattern-subscribable;
  matching is targeted (below). P-22's conditions + templating apply
  to ALL kinds (`{{event.body.x}}` on webhook, `{{event.text}}` on
  slash — this is exactly why P-22's mention guard landed first).
- **Schedule grammar** (structured; no cron dep — go.mod stays clean):
  `{"every":"minutes","n":N≥5}` | `{"every":"hour","minute":0..59}` |
  `{"every":"day","at":"HH:MM"}` | `{"every":"week","on":"mon".."sun",
  "at":"HH:MM"}`, optional `"tz"` = IANA name validated via
  `time.LoadLocation` (default UTC). `nextFire(sched, now, loc)` is a
  pure function, unit-tested including DST transitions (a nonexistent
  wall-clock time normalizes forward — assert the exact instants) and
  week rollover. Cron-string grammar is a recorded gap.
- **schedule_next_at lifecycle**: computed on enable (Update
  enabled=true with a schedule trigger); NULLed on disable; recomputed
  on definition change while enabled. Create stores rules DISABLED so
  it never sets one. The F-13 arc composes: a definition edit on a
  human-actor rule disables it, which NULLs schedule_next_at.
- **Scheduler lane** (a goroutine in Runner.Run beside sweep, every
  30s): one tx — `SELECT id, org_id, definition FROM automation WHERE
  schedule_next_at <= now() AND enabled AND deleted_at IS NULL FOR
  UPDATE SKIP LOCKED LIMIT 100`; per row compute + UPDATE the next
  fire and `eventlog.Append` verb `automation.schedule_due`,
  `ActorSystem`, EntityAutomation/rule id, payload `{automation_id,
  scheduled_for}` — all in that tx. The existing consumer picks it up
  (NOTIFY fires from Append). Idempotency is the existing
  `(automation, trigger_event_id)` run key. Downtime: a missed slot
  fires ONCE on recovery and the next fire computes FROM NOW — no
  burst catch-up (document).
- **Inbound webhook** — `POST /api/v1/hooks/rules/{id}/{token}`,
  UNAUTHENTICATED, outside withAuth (unsubscribe precedent), behind
  the per-IP authLimit AND a new per-RULE limiter (~1 rps burst 10 —
  this is also the bound on external-service echo loops; over → 429).
  Auth: load the rule (org-agnostic by id), require enabled AND
  deleted_at IS NULL AND trigger kind webhook AND constant-time token
  match (`subtle.ConstantTimeCompare` over sha256s of both) — EVERY
  auth failure (absent id, wrong token, disabled, wrong kind) is one
  indistinguishable 404. Then: body ≤64KB and `json.Valid` (else
  400/413 — only AFTER auth passed), append verb
  `automation.webhook_received`, `ActorSystem`, payload
  `{automation_id, body: <raw json>}` → 202 `{"ok":true}`. Sender
  retries create distinct runs (at-least-once from the sender's view;
  a dedupe key is a recorded gap).
- **webhook_token**: 32 bytes crypto/rand hex, minted when a
  definition's trigger becomes kind webhook, NULLed when it stops
  being one; surfaced to scope admins in List (that surface is already
  requireScopeAdmin-gated — the capability-URL model, documented);
  `POST /api/v1/automations/{id}/webhook-token` (scope-admin)
  rotates + event `automation.webhook_token_rotated` (the token itself
  NEVER appears in any event payload or log line).
- **Slash trigger** — Definition: `{"kind":"slash","command":X}`,
  command `^[a-z0-9][a-z0-9_-]{0,31}$`. Invocation:
  `POST /api/v1/automations/slash {command, channel_id, text?}`,
  authed. Gate = the channel SEND gate (VerbSendMessage + membership +
  live channel): do this as a small PREP COMMIT exporting the existing
  channel-send gate from messaging (requireThreadSend's channel
  branch) so automation calls it — NEVER duplicate access control.
  text ≤2000 runes, control chars stripped (the P-29 sanitizer
  precedent). Append verb `automation.slash_invoked`, `ActorHuman` +
  the caller's id, payload `{command, channel_id, user_id, text}` →
  202. The response cannot know which rules fire (the runner is
  async); runs are the debugger (AU-2). Multiple rules may match one
  command — OQ-AU12 stays open, recorded.
- **match() extension** — kind event: today's path + conditions. Kind
  schedule/webhook: verb match AND `payload.automation_id == rl.ID`
  (targeted — these events can never fire another rule). Kind slash:
  verb match AND command equal AND scope covers `payload.channel_id`
  (org scope: any channel; channel scope: equal). Depth: none of the
  three carries an automation_depth hint → depth 0; cascades from
  their runs inherit depth+1 exactly as today.
**Edge cases:** disabled rules never claim (index predicate + WHERE,
belt and braces); a schedule rule that is enabled but has a NULL
next_at (crash between writes) is caught by recompute-on-enable — the
claim never invents fires; webhook to a channel-scope rule still obeys
"posts only its own channel" (validateDefinition already enforces);
foreign-org ids in slash payloads impossible (actor-org-pinned send
gate); two rules with the same slash command both fire.
**Tests (`TestAutomationSchedules` / `TestAutomationWebhooks` /
`TestAutomationSlash`, real Postgres):** nextFire unit table (all four
grammars, DST skip, week rollover, n<5 and bad tz → 400). Schedules
e2e: enable computes next_at; force next_at into the past, run the
claim, exactly ONE run fires and next_at moves forward; a second claim
fires nothing; disable NULLs. Webhooks e2e: happy 202 → run executes
with `{{event.body.x}}` template flowing; wrong token / unknown id /
disabled rule / non-webhook rule → four IDENTICAL 404s; oversize 413;
invalid JSON 400; per-rule 429 after the burst. Slash e2e: a member
fires the rule (text templated into the post); org-scope rule fires
from any channel, channel-scope only from its own; a NON-member
invoking against a private channel gets the send gate's oracle-free
denial. RED/GREEN (pin in comments): (1) neuter the token comparison
(accept any token) → the wrong-token 404 assert goes red — the
load-bearing line; (2) drop the membership check from the slash gate
→ the non-member denial assert goes red.
**Gaps to record:** cron-string grammar; sender dedupe key;
Slack-payload-compatible inbound posting (that's P-27's
slack_incoming, a different feature); slash discovery/autocomplete +
namespace collisions (OQ-AU12); per-org scheduler sharding at fleet
scale; catch-up policy knobs.

### P-24 `automation: Outbound HTTP steps + delivery health.` — L — **[x] shipped #99** (egress-sensitive; executor drafted then hit its session limit, reviewer-completed; egress-bypass + reset-drop red/green-proven — the security/compliance-grade review standard)
Migration `0018_webhook_delivery.sql`: `CREATE TABLE webhook_delivery
(id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, org_id BIGINT NOT
NULL REFERENCES org (id), automation_id BIGINT NOT NULL REFERENCES
automation (id), run_id BIGINT NOT NULL REFERENCES automation_run
(id), url TEXT NOT NULL, payload JSONB NOT NULL, status SMALLINT NOT
NULL DEFAULT 1, -- 1 pending · 2 delivered · 3 failed (terminal)
attempts INT NOT NULL DEFAULT 0, next_attempt_at TIMESTAMPTZ NOT NULL
DEFAULT now(), last_status_code INT, last_error TEXT NOT NULL DEFAULT
'', created_at TIMESTAMPTZ NOT NULL DEFAULT now(), delivered_at
TIMESTAMPTZ);` + `webhook_delivery_due_idx ON (next_attempt_at) WHERE
status = 1` + `webhook_delivery_rule_idx ON (automation_id, id DESC)`
+ `ALTER TABLE automation ADD COLUMN delivery_failures INT NOT NULL
DEFAULT 0;` (consecutive terminal failures — O(1) health state). Read
internal/platform/egress/egress.go IN FULL (the P-15 guard: vetURL,
vetAddr, the pinned dialer, AllowLoopbackForTests), its test harness,
unfurl's runner (how egress wires + body caps), P-25's notifyFailure,
and the P-22/P-23 definition code first.
**Design (decided):**
- **Step kind `http_request`**: `{kind, url, headers?}`. URL is STATIC
  and vetURL-shape-checked at definition time (http/https, no
  userinfo, standard ports) — **NO templating in URLs, ever**
  (attacker-influenced text choosing the destination is the
  SSRF-adjacent shape the guard exists to kill; 400 any `{{` in a
  url). headers: ≤5, name `[A-Za-z0-9-]{1,64}` case-insensitively NOT
  in {host, content-length, content-type, transfer-encoding,
  connection, user-agent}, value ≤512 printable chars with CR/LF
  rejected (header injection — validate at write for early feedback;
  Authorization IS allowed — that's the auth use-case, and definition
  visibility is already scope-admin-gated, the Jira model). Method:
  POST only v1. ≤3 http steps per definition; Content/ChannelID must
  be absent on http steps (400).
- **NO author body templating v1** — the request body is a FIXED
  server-marshaled envelope: `{"automation_id", "automation_name",
  "run_id", "delivery_id", "attempt", "event": {"id", "verb",
  "occurred_at", "payload"}}`. The receiver gets the whole trigger
  event — strictly more than templates could extract — and the
  JSON-injection + template-SSRF classes are deleted outright. Custom
  body shapes are a recorded gap (they return with the template
  grammar's maturity). Content-Type application/json; UA from egress
  opts.
- **execute() enqueues, never dials**: an http step INSERTs a
  webhook_delivery row (payload = the event snapshot object) in the
  run's own tx — atomic with the run — and traces
  `{kind, status: "queued", delivery_id}`. A queued-only run finishes
  success; delivery outcome is tracked on the delivery row (document:
  run status = "did the steps dispatch", delivery status = "did the
  endpoint accept"). This is why a slow endpoint can NEVER stall the
  org's event cursor (AU-4 per-org-queue spirit).
- **egress gains `Post(ctx, url, headers, body)`** beside Get — same
  vetURL, same pinned dialer, same no-credential policy, and for POST
  the CheckRedirect REFUSES (a 30x is a delivery failure: re-sending a
  body cross-host after a redirect is a header/credential-leak shape;
  unfurl's Get keeps its redirect budget untouched).
- **Delivery lane** (Runner goroutine, every 15s): claim `SELECT …
  FROM webhook_delivery WHERE status = 1 AND next_attempt_at <= now()
  FOR UPDATE SKIP LOCKED LIMIT 50`, attempt WHILE HOLDING the row lock
  (SKIP LOCKED means no other worker waits; a crash mid-send releases
  the lock and the row retries = AT-LEAST-ONCE, the webhook industry
  norm — receivers dedupe on `delivery_id`, document it; the mail
  lane's mark-then-send at-most-once is the OPPOSITE trade and stays
  as-is). 2xx → delivered. Failure → attempts++, next_attempt_at =
  now + {1m, 5m, 30m}[attempt-1]; the 4th failure is TERMINAL (status
  3). An `egress.ErrDisallowed` rejection is terminal IMMEDIATELY (the
  destination will never become allowed — no retries). Response read
  ≤64KB; record last_status_code + first 256 bytes into last_error on
  failure.
- **Health = alert-before-auto-disable (AU-4), O(1)**: on delivered →
  `UPDATE automation SET delivery_failures = 0`; on TERMINAL failure →
  `delivery_failures = delivery_failures + 1 RETURNING` — at exactly
  5 and 15, notify the rule's write-gate holders (the P-25
  recipients/dedupe machinery, kind 6; the 15 message is "disable
  imminent"); at 20 → `enabled = false` + event
  `automation.auto_disabled {automation_id, consecutive_failures}` +
  a final notification. **Update(enabled=true) resets
  delivery_failures to 0** — self-serve re-enable (AU-4) must get a
  fresh window, not insta-disable on stale history. Success also
  resets. Numbers are consts v1 (config knobs = gap).
- **`GET /api/v1/automations/{id}/deliveries?limit`** — scope-admin
  gated exactly like ListRuns, newest first: the AU-4 health dashboard
  v1.
**Edge cases:** a PUBLIC literal-IP url is allowed (same as unfurl —
vetAddr at dial is the gate, not the shape); DNS rebinding is already
dead (the P-15 pinned dialer — reuse its fake-resolver test harness);
guard rejections count toward delivery_failures (a private-IP url IS a
failing endpoint); 3xx = failure; timeout = retryable; the delivery
lane is global across orgs v1 (batch 50 bounds per-tick starvation;
per-org fairness = recorded gap); rule deleted with deliveries pending
→ the claim skips them? No — deliveries execute regardless (the run
already committed; a soft-deleted rule's queued deliveries drain,
document) but auto-disable of a deleted rule is a no-op.
**Tests (`TestAutomationHTTPSteps`, real Postgres + httptest with the
egress AllowLoopbackForTests option, exactly like the P-15 tests):**
happy path: rule fires → run success with queued trace → lane
delivers → envelope body asserted (event payload, delivery_id,
attempt) → status delivered + delivery_failures reset. Retry: endpoint
500s twice then 200s → attempts=3, delivered (drive time by UPDATEing
next_attempt_at, the fixture pattern). Terminal after 4 failures.
**SSRF: a step url resolving to a private address (fake resolver) →
delivery TERMINAL with ErrDisallowed and the endpoint proves it was
NEVER dialed.** Health: drive 20 terminal failures → admins notified
at 5 and 15, rule auto-disabled + event at 20; re-enable → next single
failure does NOT insta-disable (counter reset asserted). Validation
400s: templated url, bad header name/value, CR/LF value, >3 http
steps, Content on an http step. RED/GREEN (pin in comments): (1)
bypass the egress client (plain http.Client) for the send → the
private-address delivery SUCCEEDS and the never-dialed assert goes
red — the load-bearing line; (2) drop the counter reset from
Update(enabled=true) → the re-enable test insta-disables and goes
red.
**Gaps to record:** custom body templating; non-POST methods; HMAC
request signing; per-endpoint (vs per-rule) health; delivery-row
retention/purge lane (compliance janitor candidate); config-able
thresholds/backoff; per-org delivery fairness; redirect-following
opt-in.

### The mixed quartet ships in PARALLEL: P-18 + P-34 + P-21 + P-30
Four different subsystems (files, channels, notifications, identity) —
unlike the automation cluster there is no shared function to fight
over. Expected merge overlap is only router.go/main.go/REALITY.md
appends (the worktrack pattern: reviewer serial-merges, taking dev's
file and appending each slice's additions, regression-proven).
Migration numbers are PRE-ASSIGNED so parallel branches cannot
collide: P-21 = 0019, P-30 = 0020; P-18 and P-34 are zero-migration.
If merge order differs from the numbers, the reviewer renumbers at
merge.

### P-18 `files: Image thumbnails + inline rendering allowlist.` — M — **[x] shipped #102** (Opus-executed; decompression-bomb cap red/green-proven; thumb key deviated to a `.thumb/` sibling — reviewer-verified)
ZERO migrations. A thumbnail is a DERIVED BLOB, not a File: it lives
at a deterministic key derived from the ORIGINAL's content hash
(`StorageKey(org, sha) + "/thumb/w480.jpg"`), so org dedup rides free,
no file row, no FK, and blob GC of the original must also delete its
thumb (one extra `store.Delete` beside the existing one — find the
purge call in compliance.Janitor's file lanes and mirror it). Read
files.go (Upload's spool+hash shape, authorizeDownload, StorageKey),
avatar.go + handlers_media.go (the magic-byte allowlist png/jpeg/webp/
gif and SVG-rejection stance), handlers_files.go (why general
downloads are attachment-disposition), the blob.Store interface, and
compliance's file purge lanes first.
**Design (decided):**
- **Pure-Go imaging behind a seam** — no libvips, no cgo (the no-dep
  bias; the seam keeps vips swappable later). New
  `internal/platform/imaging`: `Thumbnail(r io.Reader, maxDim int)
  ([]byte, Meta, error)` where Meta = {SrcW, SrcH, W, H}. Decode via
  stdlib image/png, image/jpeg, image/gif + `golang.org/x/image/webp`
  (decode-only is fine); scale with `golang.org/x/image/draw`
  (CatmullRom); encode ALWAYS as JPEG q80 over a white-composited
  background (universal, small; animated GIF = first frame, recorded
  gap). `golang.org/x/image` is a new dependency — same trust tier as
  the existing x/crypto and x/time, explicitly allowed here.
- **THE SECURITY PIN — decompression-bomb cap**: call
  `image.DecodeConfig` FIRST (bounded header read) and refuse
  `width*height > 40_000_000` (40MP) or either dimension > 12000
  BEFORE any full decode — a 50000×50000 PNG is a few KB on disk and
  gigabytes decoded. Only then decode. The cap lives in imaging with
  its own unit test.
- **Generation**: synchronous inside Upload, AFTER the malware scan
  verdict and store.Put, best-effort — sniff the spooled bytes with
  http.DetectContentType; if in the image allowlist, generate + Put
  the thumb blob. A generation failure (corrupt image, over-cap) is
  logged and SKIPPED, never a failed upload. Max dim 480.
- **Serve-by-convention + lazy backfill**:
  `GET /api/v1/files/{id}/thumbnail` — authorize with EXACTLY
  authorizeDownload (a file you cannot download has no thumbnail —
  oracle-free 404), then Open the derived key; on miss AND the
  original is an allowlisted image within caps, generate-and-store
  once (pre-P-18 uploads backfill lazily), else 404. Serve INLINE:
  `Content-Type: image/jpeg`, nosniff, `Cache-Control: private,
  max-age=3600`, inline disposition — safe because WE encoded these
  bytes (the avatar precedent); this is the whole "inline rendering
  allowlist": only weft-encoded renditions ever serve inline, all
  originals keep attachment disposition (the stored-XSS stance in
  handlers_files.go is unchanged). Remote/markdown image URLs keep
  rendering as links (parse.go's existing behavior — privacy: no
  hotlink fetches).
- **Response meta**: the thumbnail response carries `X-Image-Width`/
  `X-Image-Height` (original) + the thumb dimensions as headers from
  imaging.Meta — no schema change; clients that need layout hints
  read them (a dimensions column is a recorded gap).
**Edge cases:** non-image file → 404 (never generated, never
inline); quarantined (scan_status=2) or deleted file → the existing
authorizeDownload denial; over-cap image → 404 thumbnail while the
original still downloads; tiny image (≤480 both dims) → thumb is a
re-encode at original size (never upscale); webp with alpha →
white-composited JPEG; GC purge of the original removes the thumb
blob (assert via the janitor test pattern); two files sharing sha
(dedup) share the thumb key — deleting ONE file's row must NOT delete
the shared thumb while the twin lives (the existing twin rule —
reuse the same liveness check the blob purge uses, cite it).
**Tests (`TestImageThumbnails` e2e + imaging unit table):** unit:
each format decodes + scales, JPEG output dims exact, bomb cap
refuses a crafted 50000×50000 PNG header, no upscale. E2E (real PG +
the fs blob store like files tests): upload png → thumbnail 200
inline jpeg with sane headers; lazy backfill for a file uploaded
through StoreDocument (no thumb at upload) — first GET generates;
non-image upload → 404; foreign-org/no-ACL file → 404; GC purge
removes thumb; dedup-twin keeps it. RED/GREEN (pin in a comment):
drop the DecodeConfig pixel cap → the crafted-header bomb generates a
thumbnail and the `bomb → 404` assert goes red — the load-bearing
line.
**Gaps to record:** srcset/multiple sizes; animated GIF/video
previews; PDF/code previews (ADR-012's full list); dimensions in a
queryable column; full-size validated-inline lightbox path; EXIF
orientation (v1 ignores it — document); libvips behind the seam if
fleet-scale demands.

### P-34 `channels: Private-channel existence masking.` — S — **[x] shipped #103** (Opus-executed; oracle-indistinguishability red/green-proven; PromoteThread residual leak recorded as a follow-up, not folded in)
Semantics survey DONE, decision made: private channels 404 like DMs.
Today is INTERNALLY INCONSISTENT — single-message Get already masks
(web_public_test.go asserts non-member private Get = 404, the P-33
contract) while ListThreads/join/send return 403 through
requireMember/requireChannelRead (threads.go:356/375, neither
org-pinned). Slack semantics; Zulip agrees for unsubscribed private
streams. PUBLIC channels stay 403 for non-members — they are
directory-listable (the "listable-public asymmetry" is real and
correct); web-public stays readable (P-16).
**Design (decided):**
- **One org-pinned access probe** in messaging:
  `channelAccess(ctx, tx, orgID, channelID, userID) (visibility
  int16, member bool, err)` — one query joining channel (org-pinned,
  any archived state) + membership EXISTS. Absent/foreign-org row →
  `apperr.NotFound("channel not found")` — indistinguishable from the
  masked case by construction.
- **requireMember and requireChannelRead become wrappers** over the
  probe, gaining the orgID param (thread it from each caller's
  actor — all nine call sites have actor in scope: pins.go:147,
  readstate.go:41, subscriptions.go:48/91, threads.go:58/153/232/
  296/543). Denial mapping, THE decision table:
  visibility=2 ∧ !member → NotFound (the mask);
  visibility=1 ∧ !member → Forbidden (public is knowable — a join
  affordance, and the send-before-join 403 tests stay honest);
  visibility=3 → read allowed as today (write still needs member,
  and a non-member write miss on web-public stays Forbidden — it is
  world-READABLE, so existence is public anyway).
- **JoinChannel** (channels.go, the P-16 self-join): joining a
  private channel you cannot see must ALSO be NotFound (today 403 —
  channels_test.go:106 asserts it; that test flips). Joining public
  stays as-is.
- **Channel Get/meta + every by-id surface**: sweep EVERY handler
  that resolves a channel id (git grep the handlers for channel
  PathValue) — folders add/remove, mark-read, typing, pins list,
  subscriptions — anything reaching the gates inherits the fix via
  the wrappers; anything doing its own channel lookup must be
  audited to the same table. List the audited sites in the PR.
- **Guests**: unchanged mechanics (they ride the same gates). A
  guest probing a PUBLIC channel by id still gets 403 (existence of
  public channels is org-public; the guest-directory question is a
  recorded gap, not this slice).
- **Events/invites**: invite-prejoined private channels and
  admin-verb holders (administer_channel resolved up the chain) are
  MEMBERS or admin-scoped already — administering a private channel
  you are not in: perms.ChannelScope loads the channel row org-pinned
  and the admin verb is checked BEFORE membership in admin paths —
  verify admins retain access (they must; masking is for the
  unprivileged) and assert it in the test.
**Edge cases:** nonexistent id, foreign-org id, and private-non-member
must be THREE INDISTINGUISHABLE 404s (assert byte-identical bodies,
the P-16 test pattern); member of an archived private channel keeps
history (lifecycle contract — the probe must not filter archived for
members); public-channel non-member send stays 403
(channels_test.go:93/115 keep passing); web-public anonymous reads
(P-16 public.go) are UNTOUCHED — they never pass through these gates.
**Tests (`TestPrivateChannelMasking`, real PG):** the three-way
indistinguishability on ListThreads + Join + send + pins + mark-read;
public non-member still 403 everywhere; member unaffected; admin
non-member retains admin surfaces; guest unchanged; update the two
flipped asserts (web_public_test.go:161-163 list 403→404,
channels_test.go:106-107 join 403→404) IN THE SAME COMMIT as the gate
change. RED/GREEN (pin in a comment): revert the wrapper's private
mapping (NotFound → Forbidden) → the indistinguishability assert goes
red (a 403 distinguishes denied-from-absent — the oracle reopens).
**Gaps to record:** guest existence-masking for public channels
(needs the directory design); named-404 UX for clients (a "you may
need an invite" hint would re-open the oracle — deliberately NOT
doing it); P-16's anonymous surface already masks (no change).

### P-21 `notification: Push medium.` — L — **[x] shipped #105** (migration 0019; executor stalled twice, reviewer-completed; RFC 8291 Appendix A vector + egress-bypass + seen-claim red/green-proven)
Migration `0019_push_subscriptions.sql`: `CREATE TABLE
push_subscription (id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY
KEY, org_id BIGINT NOT NULL REFERENCES org (id), user_id BIGINT NOT
NULL REFERENCES user_account (id), endpoint TEXT NOT NULL, p256dh
BYTEA NOT NULL, auth BYTEA NOT NULL, created_at TIMESTAMPTZ NOT NULL
DEFAULT now(), last_ok_at TIMESTAMPTZ, UNIQUE (user_id, endpoint));`
+ `ALTER TABLE notification ADD COLUMN pushed_at TIMESTAMPTZ;` + the
due partial index mirroring 0012's email one: `(created_at) WHERE
seen_at IS NULL AND pushed_at IS NULL`. The medium plumbing ALREADY
exists: notification_medium_pref medium 3 = push (0012, "reserved"),
and channel_member.notif_push (0003) stays dormant (recorded gap —
kind-level prefs only in v1). Read email.go IN FULL (RunOnce's
claim/mark/deliver, the prefs COALESCE with zero-row defaults, the
DND unmarked-skip), prefs.go, mail.go (the driver seam + log-driver
default), config.go, and the P-24 egress additions first.
**Design (decided):**
- **Web Push (RFC 8030/8291/8292) implemented in-house — NO new
  dependency**: new `internal/platform/webpush` with (a) VAPID ES256
  JWTs (stdlib crypto/ecdsa P-256; aud = the endpoint origin, exp
  12h, sub from config); (b) RFC 8291 aes128gcm payload encryption —
  ephemeral ECDH over crypto/ecdh P-256 + x/crypto/hkdf (already a
  transitive dep of x/crypto in go.mod... verify; if hkdf is not
  importable from the existing x/crypto version, STOP and report) +
  stdlib AES-GCM. **The implementation MUST reproduce RFC 8291
  Appendix A byte-for-byte**: encode the RFC's exact keys/salt/
  plaintext as a unit-test fixture and assert the exact ciphertext —
  this vector test is non-negotiable and is what makes hand-rolling
  responsible. FCM is NOT in v1 (a later Sender implementation
  behind the same seam; recorded gap).
- **Config**: `WEFT_VAPID_PUBLIC_KEY` / `WEFT_VAPID_PRIVATE_KEY`
  (base64url raw keys) + `WEFT_PUSH_SUBJECT` (mailto:/https:).
  Unset → the push lane is a structural no-op and the subscribe API
  409s with a clear "push not configured" (the mail log-driver
  spirit: dev/CI never need keys). `weftd gen-vapid-keys` subcommand
  prints a fresh pair (mirror how existing subcommands are wired in
  main.go).
- **Subscription API** (authed, self-scoped): `POST
  /api/v1/me/push-subscriptions {endpoint, keys:{p256dh, auth}}` —
  endpoint must pass `egress.VetURLShape` AND be https (400
  otherwise), keys base64url-decoded and length-checked (p256dh = 65
  bytes uncompressed point, auth = 16 bytes), ≤10 live subscriptions
  per user (409 over), upsert on (user, endpoint); `GET` lists the
  caller's own (endpoint TRUNCATED for display — it is a capability
  URL); `DELETE /api/v1/me/push-subscriptions/{id}` self-scoped
  oracle-free 404 (the sessions precedent). VAPID public key
  discovery: `GET /api/v1/push/vapid-key` (authed) → {key} or 404
  when unconfigured.
- **Push lane** (`PushWorker` beside EmailWorker, 30s tick,
  mark-then-send AT-MOST-ONCE — the email trade, right for
  notifications): claim exactly like RunOnce but `pushed_at IS NULL`,
  medium = 3, NO age delay (push is the immediacy medium; a
  seen-races-send extra push is acceptable, document), zero-rows
  default = **dm + mention ON for push** (mirror email's kind IN
  (1,2,4)? DECISION: push defaults ON for kinds 1 and 2 only —
  keyword email's opt-in-by-setting-a-word logic does not carry to
  push; a stored medium-3 pref row overrides, and the existing
  PUT /notification-prefs already accepts medium 3 — verify, it was
  built "reserved"), DND unmarked-skip identical to email (VIP
  pierce included). Per claimed row: build a minimal JSON payload
  {kind, org_id, entity_type, entity_id, actor_name, channel_name —
  who/where ONLY, NEVER message content (the email-digest privacy
  rule)}, encrypt per-subscription, POST via **the egress guard**
  with headers TTL:86400, Urgency:normal, Content-Encoding:
  aes128gcm, Authorization: vapid t=…,k=…. egress needs the
  Content-Type override — add `egress.PostRaw(ctx, url, headers,
  body []byte, contentType string)` sharing Post's spine verbatim
  (vetURL, pinned dialer, no redirects for POST, no cookies) with
  the caller-set content type; Post itself is UNTOUCHED (P-24's
  envelope contract).
- **Endpoint lifecycle**: 200/201 → last_ok_at = now(); 404/410 from
  the push service → DELETE the subscription row (the standard
  contract — the browser revoked it); 429/5xx/timeout → leave the
  row, the notification is already marked (at-most-once, a drop not
  a dup); egress.ErrDisallowed (a private/odd endpoint that slipped
  registration — DNS moved) → delete the subscription too and log.
- **Fan-out**: one notification row → push to EVERY live
  subscription of that user (a phone and a laptop both ring).
**Edge cases:** push unconfigured → subscribe 409, lane no-op, zero
errors in logs; subscription registered against a private-IP
endpoint → 400 at registration (VetURLShape passes https/ports but
the ADDRESS check happens at send — registration-time DNS vetting
would be a TOCTOU lie, so the guard at SEND is the truth and such a
row dies on first delivery via the ErrDisallowed path — document
this shape explicitly); deactivated user's rows never claimed (the
email JOIN already excludes — mirror it); seen-before-tick → never
pushed; the SAME notification emailed AND pushed is correct (media
are independent).
**Tests (`TestPushMedium` e2e + webpush unit):** unit: THE RFC 8291
Appendix A vector byte-exact; VAPID JWT parses + verifies with the
public key, aud/exp correct. E2E (real PG + an httptest push service
+ egress AllowLoopbackForTests, the P-24 pattern): subscribe →
mention → lane run → the fake service receives a request with
aes128gcm encoding + a vapid Authorization + a payload that DECRYPTS
(test-side RFC 8291 decrypt with the subscription keys) to the
who/where JSON with NO message content; DND snooze skips unmarked
then delivers after; medium-3 pref off → nothing; 410 → row deleted;
seen → never pushed; two subscriptions → two deliveries; unconfigured
→ 409 + no-op. RED/GREEN (pins in comments): (1) neuter the
egress-guarded send (plain http.Client, the P-24 pin shape) → a
subscription pointing at a loopback/private endpoint DELIVERS and the
never-dialed assert goes red — the load-bearing line; (2) drop the
`seen_at IS NULL` clause from the claim → an already-seen
notification pushes and the `seen → no push` assert goes red.
**Gaps to record:** FCM/APNs senders behind the seam; per-channel
notif_push tri-state override; delivery receipts/collapse keys; rate
limiting per endpoint origin; push for web-public anonymous (never);
subscription pruning by age; the client service-worker (client-era).

### P-30 `identity: OIDC login.` — L — **[x] shipped #104** (migration 0020; go-oidc/v3 + x/oauth2; verified-email linking + single-use state both red/green-proven; every IdP dial rides the egress guard)
Migration `0020_oidc.sql`: `CREATE TABLE auth_provider (id BIGINT
GENERATED ALWAYS AS IDENTITY PRIMARY KEY, org_id BIGINT NOT NULL
REFERENCES org (id), name TEXT NOT NULL, -- url-safe slug, unique per
org issuer TEXT NOT NULL, client_id TEXT NOT NULL, client_secret TEXT
NOT NULL, enabled BOOLEAN NOT NULL DEFAULT false, created_at
TIMESTAMPTZ NOT NULL DEFAULT now(), UNIQUE (org_id, name));` +
`CREATE TABLE external_identity (id BIGINT GENERATED ALWAYS AS
IDENTITY PRIMARY KEY, org_id BIGINT NOT NULL REFERENCES org (id),
user_id BIGINT NOT NULL REFERENCES user_account (id), provider_id
BIGINT NOT NULL REFERENCES auth_provider (id), subject TEXT NOT NULL,
email_at_link TEXT, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
UNIQUE (provider_id, subject));` + `CREATE TABLE oidc_flow
(state_hash TEXT PRIMARY KEY, provider_id BIGINT NOT NULL REFERENCES
auth_provider (id), pkce_verifier TEXT NOT NULL, nonce TEXT NOT NULL,
created_at TIMESTAMPTZ NOT NULL DEFAULT now(), used_at TIMESTAMPTZ);`
(the password_reset single-use-row pattern: only the state's sha256
is stored, used_at IS NULL is the replay guard, expired rows swept).
Read auth.go IN FULL (Login's credential JOIN — an OIDC-only user
simply has NO user_credential row and password login naturally
excludes them; CreateSession; FromToken), identity.go, invites.go
(the authorization model), password_reset.go (the single-use claim +
oracle-free discipline), and the P-16/P-20 outside-withAuth endpoint
precedents first.
**Design (decided):**
- **Dependency: `github.com/coreos/go-oidc/v3` + `golang.org/x/
  oauth2`** — ID-token verification (discovery, JWKS rotation, alg
  pinning, iss/aud/exp) is the one thing this codebase must NOT
  hand-roll; go-oidc is the de-facto standard. This is a DELIBERATE
  exception to the no-dep bias — say so in the commit body. Pin
  latest stable; `go mod tidy` diff should show only these two
  roots + their minimal graph.
- **Provider CRUD** (manage_org-gated, the admin surface):
  `POST/GET/PATCH/DELETE /api/v1/admin/auth-providers` — name
  `^[a-z0-9][a-z0-9-]{0,31}$` unique per org; issuer must be https
  and pass `egress.VetURLShape` (400); client_secret is WRITE-ONLY —
  GET/List return `has_secret: true`, never the value (the
  invite-token show-once spirit); a PATCH may rotate it. Creating
  disabled; enabling requires a successful DISCOVERY probe (fetch
  issuer/.well-known/openid-configuration through go-oidc at enable
  time → 422 with the provider error on failure, so a typo'd issuer
  never strands logins). Every mutation event-logged (no secret in
  payloads, ever).
- **Login flow** (pre-auth, outside withAuth, per-IP authLimit like
  login): `GET /api/v1/auth/oidc/{org_slug}/{provider}/start` →
  resolve org+enabled provider (absent/disabled = one oracle-free
  404), mint state (32B random; store sha256 + PKCE S256 verifier +
  nonce in oidc_flow), 302 to the IdP authorize URL (scope "openid
  email", response_type=code, PKCE S256, nonce). `GET
  /api/v1/auth/oidc/{org_slug}/{provider}/callback?code&state` →
  claim the flow row IN ONE TX (`used_at IS NULL` single-use — the
  password-reset race guard verbatim, red/green target), 10-min TTL;
  exchange the code (go-oidc/oauth2 with the PKCE verifier — inject
  an egress-guarded http.Client via oauth2.HTTPClient/context so
  even the token exchange rides the pinned dialer); VERIFY the ID
  token with go-oidc (iss, aud = client_id, exp, sig) AND assert the
  nonce claim equals the flow row's nonce (replay/mix-up defense —
  red/green target).
- **Account resolution — the decision table** (kind=1 humans only,
  deactivated always excluded):
  1. `external_identity(provider, sub)` exists → mint session
     (CreateSession with ip/UA like Login). DONE.
  2. Else if token has `email` AND `email_verified == true` AND
     exactly one LIVE kind=1 user_account matches (org_id,
     lower(email)) → LINK: insert external_identity + mint session.
     An UNVERIFIED email must NEVER link (red/green target — the
     account-takeover shape: an IdP that hands out unverified
     mailboxes must not grant someone else's account).
  3. Else → 403 `{"error": "no account for this identity — ask an
     admin for an invite"}`. NO JIT provisioning: the invite IS the
     authorization (the founding model). Domain-scoped auto-join is
     a recorded gap, deliberately config-gated future work.
  Placeholder (kind=3) claiming via OIDC: recorded gap (belongs with
  the importer-claim design).
- **Session handoff**: the callback answers 200 JSON `{token,
  user_id, org_id}` — the API-era austerity (the password-reset
  "mail carries the token, no web page" precedent). A redirect-based
  client handoff needs an allowlisted redirect target and is a
  recorded gap. Sessions/logout/self-management: UNCHANGED (the
  P-29 surfaces work on OIDC sessions identically).
- **Coexistence**: a user with BOTH a credential and external
  identities logs in either way; ChangePassword/reset untouched
  (no credential row → reset silently sends nothing, already true).
  An org knob to disable password login is a recorded gap.
**Edge cases:** disabled provider mid-flow (start ok, callback after
disable) → the callback re-checks enabled → 404; state replay → the
used_at guard → 401; expired flow row (>10min) → 401; two users
sharing an email cannot exist (user_account_email_key) so rule 2's
"exactly one" is structural — but assert the deactivated-user
exclusion; email claim ABSENT → rule 3; a second provider linking a
second identity to the same user is fine (UNIQUE is per provider);
org slug/provider mismatches all collapse to the one 404.
**Tests (`TestOIDCLogin`, real PG + a FAKE IdP httptest server —
discovery doc + JWKS + token endpoint signing RS256 with a test key;
fully offline, the fixtures discipline):** happy first login links
by verified email + session works via /me; second login rides
external_identity (no re-link); unverified email → 403 AND no
external_identity row (red/green pin 1: drop the email_verified
check → the takeover succeeds and the no-link assert goes red — the
load-bearing line); unknown email → 403 no-JIT; state replay → 401
(red/green pin 2: drop the used_at single-use clause → the replayed
callback mints a SECOND session and the replay assert goes red);
nonce mismatch → 401; disabled provider → 404; provider CRUD: secret
never echoed, discovery-probe failure → 422, non-admin → 403;
password login for a linked user still works; OIDC-only user cannot
password-login (no credential row).
**Gaps to record:** JIT provisioning behind an org policy knob +
domain allowlist; placeholder claiming; group/role mapping from IdP
claims; RP-initiated logout / backchannel logout; refresh tokens
(sessions are weft-native — none needed); client redirect handoff;
encrypting client_secret at rest (DB-column crypto is a platform
decision, not this slice).

---

## Tier 1 — SCALE CLUSTER (promoted 2026-07-17)

### Cluster: `scale: Hold a 100k mega-org in one cell.` (P-36…P-43 ≡ S0…S7)

Full research dossier (verified current-state map with `file:line`
evidence, starting-map corrections, honest-deferral ledger):
`~/Documents/oss-chat-platform/scale-mega-cell-spec.md`. The S-numbers
below are used for cross-references inside the cluster; the P-numbers
keep the queue convention. Entries are SPEC-READY unless marked
DEFERRED.

**North star.** Weft must genuinely hold a mega-org at ~100k inside a
SINGLE Postgres cell on all three axes simultaneously: membership (100k
members in one org, and 100k in one ANNOUNCEMENT channel — see the
membership re-scope below), throughput (sustained write+consume volume),
and connections (~100k live WebSockets). The cell model puts a whole org
in ONE cell, so sharding cannot rescue a hot path. The non-negotiable
invariant:

> **NO per-message cost may scale with membership, connection count, or
> message volume. Every per-message op is O(1); all O(N) work is pushed
> to RARE events (joins, pref edits, group edits) and made incremental,
> async, or coalesced there.**

**Membership re-scope (decided post-S6, 2026-07-18 — the honest
amendment, not a quiet drop).** The original wording said "100k members
in one channel" unqualified, and S6 could not honour it. Unread is
defined over EVERY member — unlike F-17 notification candidacy, which is
opt-in — so for a channel keeping the full conversational contract the
increment is irreducibly O(members) rows per message. #117 shipped that
cost DISCLOSED (`unreadcounter.go`'s package doc states it); #118 fixed
the undisclosed half (MarkRead's O(container-history) recompute). The
residue is not an algorithm we have yet to find; it is the CONTRACT.
**Decision: a 100k-member CONVERSATIONAL channel is not a goal.** It is
unusable as a conversation (1% daily participation = ~1000 msg/day
scrolling past) and no incumbent supports it — the real 100k channels
are announcement channels, which is exactly why Zulip ships
`stream_post_policy` (`zerver/models/streams.py:92-95`). The membership
axis is therefore carried by **P-44 announcement mode**, whose REDUCED
feature contract makes unread derivable by arithmetic at O(1) per
message, and conversational channels stay bounded with that bound made
honest there. S1/S2/S3/S5 are unaffected — they are org-axis and
connection-axis work, required regardless of any channel's size.

**The O(N)-blowup ledger this cluster closes:**

| Axis | Blowup today | Slice |
|---|---|---|
| Connections | O(connections) `event_log` queries per message (`gateway.go:256-263`) | **S3** |
| Membership | O(members) live join per message (`runner.go:195-207`) | **S1** |
| Membership | O(members) unread aggregate per read (`readstate.go:131-143`) | **S6** |
| Membership | O(members) unread counter WRITE per message (`unreadcounter.go`, S6's disclosed residue — irreducible while the full conversational contract holds) | **P-44** |
| Membership | O(members) synchronous closure rebuild per group edit (`perms.go:235-257`), 2× per invite | **S2** |
| Throughput | DB-global xmin stall (`consumer.go:60`); NOTIFY per append | **S4** |
| Connections | O(connections) presence fans under one mutex; multi-node fragmentation (`presence.go`) | **S5** |
| (proof) | no lag/fan metrics; rig never drove 100k (`ARCHITECTURE.md:101-106`, `PERF.md:69`) | **S0** |
| (security) | agent write-back has no grant∩permission enforcement (`capability_grant` unused) | **S7 (deferred)** |

**Migrations pre-assigned** (current max = `0020`): `0021` S1, `0022`
S2, `0023` S6, `0024` S4. S0, S3, S5, S7 are migration-free or reuse
dormant tables. Append-only rule holds.

**Dispatch topology.** S0 and S1 dispatch in PARALLEL on separate
worktrees (reviewer-approved deviation from the dossier's serial-first
ordering: the metrics seam API is PINNED below, so S1 codes against it;
S0 merges FIRST and S1 is rebased on it, the reviewer reconciling the
shared `internal/platform/metrics` package and the runner counter —
same reviewer-owned cross-slice wiring precedent as the worktrack
cluster). Then S2 (serial, Fable execution) → S6 → S4 (serial, Fable
execution, its own window) → S5 (serial with S3's hub changes). S3 may
run parallel with S2/S6 once S0 is merged. S7 stays DEFERRED.

**Decisions locked at promotion (were flagged as options in the
dossier):**

1. **S0 metrics driver: `expvar` (stdlib, no dep).** A
   `platform/metrics` seam with a `noop` default and an expvar-backed
   driver; Prometheus stays a one-file driver an operator can add later
   (mirrors the `blob.Store` seam precedent).
2. **S4 feed: `jackc/pglogrepl`.** A go-oidc-class justified dep
   exception (WAL replication protocol + keepalives + slot management
   are not responsibly hand-rollable; same author family as the
   existing `pgx` dep). **APPROVED 2026-07-18** — the written
   justification is in the P-40 entry.
   The no-dep per-org commit-fence fallback is recorded in the dossier.
3. **S5 presence plane: the dormant UNLOGGED `presence` table +
   LISTEN/NOTIFY** (wakes `0006:56-62`; no new dep; cell-local). An
   external cache is a later driver behind the same seam.
4. **S6 DM counters: same `channel_unread_counter` table** for
   symmetry (DM spaces are containers like channels).

---

### P-36 (S0) `scale: 100k-org proof harness + consumer-lag metrics.` — L — **[x] shipped #113** (Opus-executed; consumer-lag gauge + mega-org loadgen; expvar no-dep metrics driver behind a seam)

**What & why.** The cluster's whole point is designed→built+**PROVEN**.
Today there is no way to prove any scale guard: no lag metric
(`ARCHITECTURE.md:101-106` designed, not built — grep finds no
`prometheus`/`expvar` anywhere in `internal/` or `cmd/`), and the rig
has never driven a single mega-org (`PERF.md:69`). S0 builds the
proving ground FIRST so every later slice attaches a red/green scale
pin to real numbers.

**O(1) invariant defended:** none directly — it MEASURES the invariants
so the others can prove them. **O(N) blowup prevented:** measurement gap
(unfalsifiable scale claims).

**Current verified state.** `cmd/loadgen`/`internal/loadtest` is
many-tenant small-fan (`loadtest.go:36-75`); no metrics registry
anywhere; consumer cursor lag is derivable from `event_consumer_cursor`
(`0001:60-66`) but nothing exposes it.

**The build (decided):**

- **Metrics seam** at `internal/platform/metrics`, config-picked driver
  (`noop` default, `expvar` the shipped driver — DECIDED, no new dep).
  The API is **PINNED** (S1/S2/S3/S4 assert against it verbatim):

  ```go
  package metrics

  type Registry interface {
      Counter(name string, labelNames ...string) Counter
      Gauge(name string, labelNames ...string) Gauge
  }
  type Counter interface {
      Add(delta float64, labelValues ...string)
  }
  type Gauge interface {
      Set(value float64, labelValues ...string)
  }
  func Nop() Registry       // zero-cost default when metrics are off
  func NewExpvar() Registry // the "expvar" driver; exposes /debug/vars
  ```

  `NewExpvar()` MUST be safe to construct repeatedly in one process
  (tests run `-p 1` in a single binary): look up an already-published
  `expvar.Map` before publishing (`expvar.Get` first) — a bare
  `expvar.Publish` panics on a duplicate name. Labeled series publish
  as map keys (`label=value` tuples) under the metric name. Metric
  names carry NO brand prefix (brand-token lint).
- **Counters/gauges:** `fanout_events_total{consumer}`,
  `fanout_deliveries_total`, `consumer_lag{consumer,org}` (max
  committed id − cursor last_id, THE health signal per
  `ARCHITECTURE.md`/`SCHEMA.md`), `gateway_connections{org}`,
  `gateway_pump_queries_total`,
  `notification_candidates_scanned_total`, `closure_rebuild_seconds`.
- **`consumer_lag` derivation:** one query per (consumer, org):
  `MAX(event_log.id) − event_consumer_cursor.last_id` gated by the same
  xmin horizon the consumers use.
- **Loadgen mega-org mode:** a `-mega` config that provisions ONE org
  with a single channel of N members (default 100k, via the service
  layer, outside the timing window per `PERF.md:8-10`), opens M
  connections (default 100k) to that org, and drives a fixed send rate
  while recording (a) per-message fan-out DB query count, (b) delivery
  p50/p99 by id-correlation (reuse the `loadtest.go` histogram), (c)
  closure rebuild wall-time when a group edit is injected. No new
  external dep for the rig.
- **Instrument the four hot sites** (counters only, NO behavior
  change): the gateway pump (`gateway.go:254`), the notification
  candidate scan (`runner.go:195`), `RebuildClosure` (`perms.go:235`),
  and `Consumer.Poll` (`consumer.go:50`). Thread the `Registry` in as
  an optional dependency defaulting to `Nop()` — keep constructor
  churn minimal.

**Migrations.** None (metrics read existing tables; loadgen provisions
via the service layer).

**Red/green proof plan.** S0 is itself the proof INSTRUMENT, so its own
tests assert the instrument reads reality: `TestConsumerLagMetric`
(append events, do not consume, assert `consumer_lag` rises by exactly
the backlog; consume, assert it falls to 0) and
`TestMegaOrgHarnessSmoke` (provision a 2k-member channel — bounded for
CI — 200 connections, one send, assert the pump-query counter rose by
~connection-count, proving the S3 blowup is measurable BEFORE S3 fixes
it). The full 100k run is an operator procedure documented beside
`PERF.md`, not a CI gate (environment-bound, per `PERF.md:69` and
`ARCHITECTURE.md:113`).

**Risks / pre-mortem.** (1) Provisioning 100k members through the
service layer is slow — keep it outside the timing window and cache a
provisioned org between runs. (2) The harness must not itself become
the bottleneck (the `PERF.md:44-51` VM-tax lesson) — run loadgen on
separate hosts; document it. (3) The expvar duplicate-publish panic
(mitigated above).

**Dependencies + ordering.** FIRST to merge. S1 runs in parallel on its
own worktree but merges after; every other slice attaches its red/green
scale pin here.

**Deferred (honest):** the full 100k CI floor stays deferred
(rig-bound, `PERF.md:69`); S0 ships the harness + metrics + a bounded
CI smoke, and the operator procedure for the real number.

---

### P-37 (S1) `notification: Materialized deliverability set + batch coalescing (F-17).` — L — migration 0021 — **[x] shipped #114** (Fable-executed; live per-message channel_member join → O(reasons); red/green-proven)

**What & why.** The materializer joins ALL ~100k `channel_member` rows
for EVERY message in a big channel (`runner.go:195-207`). F-17
(`ADR-011:100-110`) makes the per-message candidate set O(reasons), not
O(members): a maintained per-channel deliverability set records only
the users with a NON-DEFAULT delivery reason (followers, `level=all`,
alert-word holders), invalidated on the rare events that change those.
A normal message in a 100k channel where nobody set `level=all` then
notifies only its mentions (a handful) — O(mentions), not O(100k).

**O(1) invariant defended:** per-message notification cost = O(actual
reasons on the message), independent of channel membership.
**O(N) blowup prevented:** the O(members) live join + O(members)
candidate scan per message (`runner.go:195-207`).

**Current verified state.** LIVE join per message: `runner.go:195-207`;
keyword join `runner.go:360-375`; per-recipient insert
`runner.go:282-313`. No deliverability table exists (SCHEMA
"intentionally absent" list, `docs/SCHEMA.md:154-156`).

**The build (decided):**

- **Table `channel_deliverability` (0021):** `(channel_id, user_id,
  reason SMALLINT, medium SMALLINT, org_id)`, PRIMARY KEY
  `(channel_id, user_id, reason, medium)`, `fillfactor` tuned like
  `channel_member`. `reason`: 1 follow (thread-level rides
  `thread_subscription`), 2 level=all, 3 alert-word-holder. A row
  EXISTS only for a non-default reason — a channel where everyone is
  on defaults has ZERO rows, so the per-message join returns only
  mentions. Index `(channel_id, reason)`.
- **Invalidation triggers (the O(N)→rare-event push):** the set is
  rebuilt/patched ONLY on: channel notification-level change
  (`PUT /channels/{id}/notification`), thread follow/mute change
  (`thread_subscription`), alert-word edit (`alert_word`), and
  membership change (`member.joined`/`member.left`). Consume
  `member.joined/left` via the event log (keeps messaging free of a
  notification import); membership add/remove patches a single user's
  rows (O(1) per join, not a channel rebuild). This invalidation
  dispatch is built to be SHARED — S2 reuses it.
- **Consumer-contract change:** `runner.processEvent` (`runner.go:149`)
  replaces the live `channel_member` join (`:195-207`) with a join over
  `channel_deliverability` for the activity/follow/keyword reasons;
  mentions stay driven by the event payload (`p.Mentions`, already
  O(1)). The DM path (`:251-273`) is unchanged (participant counts are
  small by nature). The per-recipient insert + `dndSuppressed`
  (`:282-313`) is unchanged in shape but now runs only for set members.
- **Batch-id coalescing (F-17b):** bulk producers (sprint close
  `sprint.closed`, automation sweeps, retention vacuum) stamp a
  `batch_id` in the event `hint` (the reserved field, `eventlog.go:44`,
  `ADR-002 P1`). The materializer groups per (user, batch_id) into ONE
  digest notification row instead of N. Add `notification.batch_id
  BIGINT NULL` (0021) + a partial index; the dedupe key (`0010`) gains
  batch awareness. This prevents a 100k-item sprint close from minting
  100k×members notifications.
- **Backfill:** lazy, keyed by a `channel.deliverability_built_at`
  marker (DECIDED — no big-bang migration over a live org): the set is
  built for a channel on first use after deploy, then maintained by
  the invalidation triggers.
- **Metrics:** increment `notification_candidates_scanned_total` (the
  PINNED S0 API) in the new candidate path. If
  `internal/platform/metrics` is absent on your branch (S0 in flight),
  create it as a separate prep commit implementing EXACTLY the pinned
  API above and flag it in your report — the reviewer reconciles with
  S0's package at merge.

**Migrations.** `0021_notification_deliverability.sql`:
`channel_deliverability` + indexes; `notification.batch_id`; a partial
index for coalesced rows. Comments cite ADR-011 F-17.

**Red/green proof plan.** `TestDeliverabilitySet` on real PG: (1) a
big-member channel (bounded to e.g. 5k in CI) with nobody on level=all
— send one message with one mention, assert exactly ONE notification
row and assert the candidate-scan counter rose by O(1), NOT
O(members). **RED:** revert `processEvent` to the live `channel_member`
join → the counter rises by O(members) and the "scan is O(reasons)"
assert goes red. (2) A user sets level=all → the set gains one row →
the next message notifies them; unset → row gone. (3) Batch
coalescing: a synthetic bulk event with N item-payloads + one batch_id
→ ONE digest row per user, not N (RED: drop the batch_id grouping → N
rows, assert goes red).

**Risks / pre-mortem.** (1) **Invalidation drift** — a missed
invalidation silently drops notifications (worse than the
slow-but-correct status quo). Mitigation: a periodic reconciliation
sweep (low-frequency, like the janitor) that recomputes a channel's set
and logs divergences; the set is a CACHE of a derivable truth, never
the source. (2) The membership-change patch rides the event log
(`member.joined/left`), NOT a same-tx write from messaging — keeps
module ownership clean. (3) level=all in a genuinely 100k-active
channel is inherently O(N) recipients — accepted and bounded by batch
coalescing + the digest; documented as the residual honest rung.

**Dependencies + ordering.** Dispatched parallel with S0 on its own
worktree; MERGES AFTER S0 (reviewer rebases + reconciles the metrics
package and the runner counter). Serial pair with S2 (S1 defines the
invalidation surface S2 reuses).

**Deferred (honest):** the editable NotificationScheme matrix
(`notification_scheme`, `0006:115-118`, N-5) stays dormant; item-event
recipient resolvers stay out of scope; the genuinely-O(N) all-active
channel is bounded, not eliminated.

---

### P-38 (S2) `perms: Async closure rebuild behind a version fence.` — L — migration 0022 — **[x] shipped #116** (Fable-executed, correctness-critical; incremental delta + version fence; diamond-nesting parity + flat-rebuild red/green-proven)

**What & why.** A 100k-member org-wide group edit rebuilds the ENTIRE
`user_group_closure` synchronously in the writer's transaction
(`perms.go:235-257`), holding a long lock and blocking every concurrent
join/invite. Invite accept does it TWICE (`invites.go:335`+`:338`).
This is the membership axis's rare-event O(N): correct today, but a
mega-org group edit is a multi-second full-table churn inside a user
request.

**O(1) invariant defended:** the hot check `Require` stays ONE indexed
closure lookup (`perms.go:85-101`) — UNCHANGED. **O(N) blowup
prevented:** a full-org synchronous rebuild on the request path; the
double rebuild per join.

**Current verified state.** `RebuildClosure` synchronous full DELETE +
recursive INSERT: `perms.go:235-257`. Callers: `perms.go:216, 226`,
`invites.go:338`, `importer.go:879`. Double rebuild: `invites.go:335`
(via `AddUserToGroup`→`perms.go:226`) + `invites.go:338`.

**The build (design level).**

- **Prep commit (no-op fix):** remove the redundant `invites.go:338`
  `RebuildClosure` — `AddUserToGroup` already rebuilds (`perms.go:226`).
  Separate commit. Red/green: a test asserting the closure is correct
  after invite accept passes with ONE rebuild.
- **Incremental delta maintenance (the primary build):** replace the
  full DELETE+recompute with a DELTA. Adding user U to group G inserts
  the closure rows for U into G and every ANCESTOR of G (walk
  `user_group_subgroup` upward — bounded by nesting depth, not
  membership); removing U deletes U's rows for G and ancestors that no
  longer reach U. Group nesting edits (add/remove subgroup) patch only
  the affected subtree × its members. This makes AddUserToGroup
  O(depth), not O(org). The hot table and `Require` call site are
  untouched (the `SCHEMA.md:48-52`/`perms.go:229-234` contract).
- **Async rebuild behind a version fence (for the rare full recompute
  — bulk import, subgroup restructure):** add
  `user_group.closure_version` and `user_group_closure.version` (0022).
  A full rebuild writes into the new version, then atomically flips a
  per-org `closure_current_version` pointer; `Require`/`HoldersAt` read
  `WHERE version = current`. Readers never see a half-built closure and
  never block on the rebuild. A `closure_rebuild_job` row (claimed
  `FOR UPDATE SKIP LOCKED`, multi-node-safe) drives the async recompute;
  the version fence means an in-flight rebuild degrades gracefully.
- **Consumer-contract change:** none for `Require`'s callers (the query
  gains a `version = current` predicate transparently). Importer
  (`importer.go:879`) switches to enqueue-an-async-rebuild.

**Migrations.** `0022_closure_versioning.sql`: `closure_version`
columns, `closure_rebuild_job` table, a per-org current-version
row/table, indexes including `(group_id, user_id, version)`. Cite
ADR-006 F-16 + `perms.go:229-234`.

**Red/green proof plan.** `TestClosureIncremental` on real PG: (1)
correctness parity — after a sequence of add/remove/nest edits, the
incremental closure equals a from-scratch `WITH RECURSIVE` recompute
(exhaustive equality, the load-bearing correctness pin). (2) Scale — a
big-member org (bounded in CI), time `AddUserToGroup`; assert wall-time
is flat as membership grows (and `closure_rebuild_seconds` via S0).
**RED:** revert to the full DELETE+recompute → the metric scales with
membership and the "flat" assert goes red. (3) Version fence — start a
rebuild, assert concurrent `Require` calls read the OLD version and
never block or see partial state (RED: drop the version predicate → a
reader sees a half-built closure mid-rebuild). (4) Invite accept does
exactly ONE rebuild (RED: the double-rebuild regression).

**Risks / pre-mortem.** (1) **Incremental maintenance is subtly wrong
on diamond nesting** (a user reachable via two paths — removing one
path must not delete the closure row). Mitigation: the delta-delete
recomputes reachability for the affected (group, user) pairs, never
blind-deletes; the parity test with the recursive recompute is the
guard. Highest-correctness-risk slice → Fable execution. (2) The
version flip must be atomic and org-scoped (cell invariant). (3)
Deactivation/`kind` changes affect `HoldersAt` (`perms.go:151-157`) —
keep them consistent with the closure version.

**Dependencies + ordering.** After S1 merges (reuses the shared
membership-change invalidation dispatch). SERIAL — do not parallelize
with S1.

**Deferred (honest):** permission_profile / custom-group governance
(`ADR-006 P-3`) stays out of scope; only the closure maintenance
changes.

---

### P-39 (S3) `gateway: Per-org multicast — one query per event, fan in memory.` — L — no migration — **[x] shipped #115** (O(connections) event reads → O(1) per org; +#119 marshal-once)

**What & why.** One message to a 100k-connection org triggers ~100k
independent `event_log` queries — each connection runs its own catch-up
(`gateway.go:256-263`) after `wakeOrgLocked` signals all of them
(`gateway.go:142-149`). This is THE connections-axis per-message
blowup. The fix (designed in `PERF.md:66-71`): the dispatcher queries
the org's new events ONCE, then fans the rows to that org's connections
in memory, each connection applying its O(1) ACL filter
(`gateway.go:320-346`) against the shared batch.

**O(1) invariant defended:** per-message DB cost on the connections
axis = O(1) `event_log` read per org per event-batch, independent of
connection count. **O(N) blowup prevented:** O(connections) `event_log`
queries per message.

**Current verified state.** Per-connection pump query
`gateway.go:256-263`; `wakeOrgLocked` signals every conn
`gateway.go:142-149`; the O(1) ACL filter already exists
`gateway.go:320-346`; the resume/gap machinery (F-2, `ADR-002:60-75`)
and checkpoint heartbeat `gateway.go:242-248`.

**The build (design level).**

- **Per-org reader goroutine.** Replace per-connection pump-on-wake
  with ONE per-org "multicast reader": on wake (NOTIFY or sweep) it
  runs the single query (the existing pump SQL, hoisted to org scope,
  from the org-min live cursor), then iterates its connections applying
  `client.filter` (`gateway.go:320`) to the shared rows, delivering to
  each connection whose ACL passes and advancing that connection's
  `lastID`.
- **Cursor model.** Connections at different `lastID` split into two
  lanes: (a) LIVE connections caught up to the org head share the
  single multicast read; (b) a resuming connection behind the org head
  runs its OWN bounded catch-up (the current pump) UNTIL it reaches the
  head, then joins the live lane. The per-connection query survives
  only for the resume gap (rare, bounded by the 24h replay window),
  never for steady-state live traffic.
- **Concurrency:** the per-org reader owns delivery; `h.mu` is split
  into a per-org lock (or a sharded lock map) so a 100k-connection org
  does not serialize the whole hub. Writes to each conn stay serialized
  by `client.writeMu` (`gateway.go:54-55`) — unchanged.
- **No protocol change:** `seq` is still the event-log id, gaps still
  expected (F-2, `ADR-002:60-75`); checkpoints unchanged
  (`gateway.go:242-248`). The membership-refresh-on-`member.joined`
  (`gateway.go:328-330`) still runs per affected connection.

**Migrations.** None (in-memory dispatcher restructure).

**Red/green proof plan.** `TestGatewayMulticast` + the S0 harness: N
connections (bounded in CI, 100k operator run) to one org, send one
message, assert `gateway_pump_queries_total` rose by O(1) per
event-batch, NOT O(N). **RED:** revert to per-connection pump → the
counter rises by ~N and the "one query per org" assert goes red. Plus
correctness carry-over from the existing gateway suite: resume-gap
replay still works, ACL filtering still drops non-member events,
checkpoint heartbeat still fires.

**Risks / pre-mortem.** (1) **A slow/dead connection must not stall the
shared multicast** — deliver best-effort per conn (the existing
`fanEphemeral` posture, `signals.go:150-153`), failing only that conn.
(2) The live/resume lane split must not drop an event at the hand-off
(connection reaches head exactly as a new event arrives) — the reader
holds the org lock across "read batch + snapshot connection cursors" so
the hand-off is a single consistent point. (3) Backpressure: a
connection whose write buffer fills should be disconnected (resume
later), not block the org — bounded per-conn send with a
drop-then-checkpoint policy (F-2 "no undetectable loss" tolerates this
— the client resyncs).

**Dependencies + ordering.** After S0 merges (needs
`gateway_pump_queries_total`). Parallel-capable with S2/S6 (disjoint:
in-memory gateway, no shared tables). Must precede S5 (S5 shares the
Hub lock split).

**Deferred (honest):** WebTransport/QUIC pipe (`ADR-002:8-10`);
cross-node connection routing (that is sticky-routing + S5's shared
plane, not this slice).

---

### P-40 (S4) `eventlog: Logical-decoding consumer feed behind the Consumer interface.` — L — migration 0024 — **[x] shipped #124** (Fable-executed; `jackc/pglogrepl` accepted as a go-oidc-class dependency exception — justification below — and the no-dep per-org commit-fence fallback stays the recorded alternative; xmin remains the DEFAULT driver, and the `consumer_lag` blind spot this slice surfaced in it is fixed as a review follow-up)

**What & why.** The `txid < pg_snapshot_xmin(...)` gate
(`consumer.go:60`) is DATABASE-GLOBAL: one long write tx anywhere
stalls delivery for every org and every consumer (gateway,
notifications, automations, unfurl, search). The designed replacement
(`SCHEMA.md:127-130`, `PERF.md:73-74`): a logical-decoding feed where
WAL order = commit order, so NO gate is needed — behind the SAME
`Consumer` interface (`consumer.go:31`) so every consumer swaps
transparently.

**O(1) invariant defended:** delivery latency independent of unrelated
concurrent transactions; no global stall point. **Blowup prevented:**
one long tx stalling ALL delivery; NOTIFY storms at high event rates.

**Current verified state.** Global gate `consumer.go:50-63` (`:60`) and
its copy `gateway.go:260`; NOTIFY per append `eventlog.go:83-84`; the
scale-tier note `consumer.go:21-31`, `SCHEMA.md:127-134`. Consumers:
`notification` (`runner.go:48`), `automations`, `unfurl`, gateway pump.

**The build (decided: `jackc/pglogrepl` — the justified dep
exception; the written justification is below).**

- **Dependency: `github.com/jackc/pglogrepl`** — the SECOND deliberate
  exception to the no-dep bias, of exactly the go-oidc class
  (P-30): the streaming replication protocol is not something this
  codebase may responsibly hand-roll. It is a binary sub-protocol over
  `COPY BOTH` — `START_REPLICATION`, `XLogData`/`PrimaryKeepalive`
  framing, standby status updates on a deadline (miss them and the
  walsender kills the connection AND the slot stops advancing, so WAL
  piles up until the disk fills), the pgoutput logical message grammar
  (Begin/Relation/Insert/Commit + the v2 streaming variants), LSN
  arithmetic and slot lifecycle. Getting any of it subtly wrong is a
  silent data-loss or disk-exhaustion bug, not a compile error.
  `pglogrepl` is the reference Go client for it, from the SAME author
  family as the `github.com/jackc/pgx/v5` this repo already depends on
  and built on the same `pgconn` — so it shares our transport, our
  connection-string parsing, and our TLS story rather than adding a
  second driver stack. `go mod tidy` adds exactly this root plus ONE
  transitive (`github.com/jackc/pgio`, buffer helpers) — no
  cgo, no reflection framework, no vendored protocol tables. Pin
  latest stable and say "deliberate exception" in the commit body.
  **The alternative stays recorded, not deleted:** the no-dep per-org
  commit-fence (a per-org fence row advanced in the writer's tx so
  consumers gate on a per-org horizon instead of the DB-global xmin)
  removes the cross-org stall without a new dependency, but it costs a
  contended write per org per transaction and still cannot see commit
  ORDER — it narrows the blast radius of the xmin gate rather than
  removing it. If the slot operationally does not pay for itself, that
  is the fallback to build.
- **A logical-decoding reader** consumes the WAL via a replication slot
  + publication on `event_log`, decoding committed INSERTs in COMMIT
  order — no xmin gate because uncommitted rows never appear. The
  `Consumer` interface gains a logical-backed implementation.
- **Interface preservation:** `eventlog.Consumer` (`consumer.go:31`)
  keeps `Poll`/`Ack`; a new `LogicalConsumer` implements the same
  shape. Offsets become LSN-based; `event_consumer_cursor`
  (`0001:60-66`) gains a nullable `lsn` column (0024) alongside
  `last_id` (id ordering stays the client-facing `seq`, LSN is the
  internal cursor). Replay = reset LSN. Idempotency (dedupe keys /
  ON CONFLICT) is already required (outbox rule) so at-least-once is
  safe.
- **NOTIFY coalescing (folds in):** coalesce `pg_notify` per (tx, org)
  instead of per append (`eventlog.go:83-84`; `SCHEMA.md:134`). With
  logical decoding the feed is push-driven, so NOTIFY becomes a
  wake-hint only.
- **Cell invariant:** the slot/publication is per-cell (one Postgres);
  decoding is per-org-ordered by filtering `org_id` in the decode
  handler. No cross-org coordination (`SCHEMA.md:112-120`). Slot
  creation is a deploy-time operator step, documented in the runbook
  (not a schema migration).

**Migrations.** `0024_logical_decoding_offsets.sql`:
`event_consumer_cursor.lsn` column. Publication/slot creation is
documented as an operator step. Cite ADR-003 F-1 + `consumer.go:21-31`.

**Red/green proof plan.** `TestLogicalFeedNoGlobalStall`: open a
deliberately long-running write tx in org A, then send+consume in org
B. **RED:** point the consumer at the old xmin-gated `Poll` → org B
stalls behind org A's long tx and the DELIVERY assert goes red (the
exact blowup `consumer.go:22-26` describes). Plus parity: no event
skipped, commit order preserved under the logical feed.

> **CORRECTED AT EXECUTION (#124), kept as a worked example.** This
> plan originally read "assert org B's `consumer_lag` (S0) stays ~0".
> That pin **cannot go red** — it passes on BOTH drivers, because the
> xmin `Lag` query carried the SAME global horizon as `Poll`, so during
> the stall the numerator froze along with the cursor and the gauge
> reported 0 while the backlog grew. The pin was inverted to assert
> DELIVERY, and the blind gauge became its own follow-up slice (the
> gate is now removed from `Lag`; `TestConsumerLagSeesGlobalStall`
> pins it). **The lesson generalises: a spec may name a pin that is
> unfalsifiable, so an executor must PROVE red before trusting green,
> and report rather than quietly adjusting.** Left visible here on
> purpose — a deleted mistake teaches nobody.

**Risks / pre-mortem.** (1) Replication slots retain WAL if a consumer
dies — a stuck slot fills the disk. Mitigation: slot monitoring via S0
metrics + a max-lag alarm + documented operator runbook; a bounded slot
with a drop-and-resync policy. (2) Highest-risk slice — changes the
delivery mechanism under EVERY consumer → Fable execution, its own
serial window, only after S1 (stable consumer logic) and ideally S3
(the feed plugs into the multicast reader, one place). (3) It is a
latency/liveness fix, not a per-message-O(N) fix — sequenced after the
true O(N) blowups.

**Dependencies + ordering.** After S1, ideally after S3. SERIAL, its
own window.

**Deferred (honest):** NATS/Kafka broker (`ADR-003 E1`); the no-dep
per-org commit-fence fallback stays recorded in the dossier.

**Spec corrections found in execution (read these before touching the
feed again):**

1. **The gate is LOSSY, not merely slow.** `txid` is stamped at a
   transaction's FIRST write, the event id at APPEND, so a transaction
   with the LOWER txid can hold the HIGHER id. When it commits first
   the gate admits it, the cursor moves past a lower id still in
   flight, and that event is never delivered — no error, no detectable
   gap (F-2 violated). `TestLogicalFeedNoCommitOrderSkip` demonstrates
   it against the shipped poller. S4 is a CORRECTNESS fix, not only a
   latency fix; the entry above and `SCHEMA.md` said "the commit-order
   race cannot skip events", which is only true of the common case.
2. **The named red/green as written could not go red.** "Assert org
   B's `consumer_lag` stays ~0" passes on BOTH drivers, because the
   xmin `Lag` query carries the SAME global horizon as the xmin `Poll`
   — during the stall it reports 0 while events pile up. The pin had to
   be inverted: assert that org B's event is DELIVERED and that lag
   SEES the backlog (1, then 0 after acking). The blind gauge is
   itself an S0 finding, recorded in REALITY.
3. **The gateway multicast reader cannot take this feed without a wire
   change.** Its `seq` IS the event id and `deliverShared` skips
   `r.id <= c.lastID`, so commit-order delivery would drop
   out-of-order-committed events at the connection. Removing its gate
   without commit-order delivery just trades the stall for more resume
   gaps. Either is a `seq`-contract decision, so S4 left the gateway
   alone; a follow-up slice owns it.
4. **A slot is single-reader per cell.** Only one connection may hold
   it, so with N app nodes exactly one streams the feed and the others'
   consumers report `ErrFeedNotReady`. Useful as a crude takeover
   lease, but it is a deployment fact the runbook now states.

---

### P-41 (S5) `gateway: Multi-node shared presence plane.` — M — no migration (wakes the dormant `presence` table) — **[x] shipped #120** (Opus-drafted, reviewer-completed after a watchdog stall; shared plane via the dormant table + LISTEN/NOTIFY, no dep; cross-node red/green-proven; node-crash staleness recorded as a follow-up)

**What & why.** Presence is per-process (`presence.go:9-26`): with 100k
connections across N gateway nodes, each node knows only its own
connections, so presence is fragmented and wrong. Also, every presence
transition fans to the whole org under the single `h.mu`
(`presence.go:150-154` → `signals.go:141-154`), and
connects/disconnects are constant at 100k. S5 gives presence a shared
plane so any node sees org-wide presence, and makes the fan efficient.

**O(1) invariant defended:** presence correctness independent of which
node holds a connection; per-transition fan bounded. **Blowup
prevented:** fragmented/wrong presence across nodes; whole-org fans
and whole-registry sweeps under one global mutex.

**Current verified state.** Per-process registry `presence.go:22-71`;
whole-org broadcast `presence.go:150-154`; sweep holds `h.mu` over all
`userConns` `presence.go:113-146`; the UNLOGGED `presence` table is
DORMANT (`0006:56-62`). `NotifyUser` scans all org conns for one user
`presence.go:175-178`.

**The build (decided: the dormant UNLOGGED `presence` table +
LISTEN/NOTIFY as the shipped driver, behind a `platform/presence`
seam so an external cache is a later driver swap).**

- **Shared presence plane:** node-local presence deltas publish to the
  plane; each node subscribes and maintains an org-wide view.
- **Fan efficiency:** presence transitions publish a coalesced delta
  (state changed for user U) once; each node fans to ITS local
  connections only. The sweep (`presence.go:113-146`) copies the
  transition list under the lock and fans after release — the lock
  becomes per-org/sharded (shared with S3's lock split).
- **Invisible mode (optional fold-in, ADR-011 N-3):** the plane carries
  a read-side mask so `presence_enabled=false` users appear offline.
  Can defer.
- **Consumer-contract:** presence stays EPHEMERAL — never event-logged
  (`ADR-002 P5`, `SCHEMA.md:72-74`). The plane is a side-channel, not
  the event log.

**Migrations.** None (the `presence` table exists, `0006:56-62`).

**Red/green proof plan.** `TestMultiNodePresence` (two in-process Hubs
sharing one plane, simulating two nodes on one PG): user connects to
node 1 → node 2's `PresenceSnapshot`/live delta shows them active;
disconnect → both see offline. **RED:** point the plane at the
per-process registry (status quo) → node 2 never sees node 1's user
and the cross-node assert goes red. Plus fan efficiency: a presence
transition fans O(local conns) per node, asserted via S0's fan counter.

**Risks / pre-mortem.** (1) **Presence flapping** across nodes on
reconnect — debounce at the plane (last-writer-wins with a short grace,
mirroring `presence.go:32-92`). (2) The UNLOGGED table is truncated on
crash — fine, presence is rebuildable from live connections
(`SCHEMA.md:72-74`); document the rebuild-on-reconnect. (3) Cell
invariant: the plane is per-cell, per-org-scoped; never cross-org.

**Dependencies + ordering.** After S3 (shares the Hub lock split).
SERIAL with S3 (same files, `internal/gateway`); parallel-capable with
the durable path (S2/S6).

**Deferred (honest):** typing-indicator fan efficiency at 100k (same
plane, later); OS-level away vs socket-silence idle; cross-node typing;
the external-cache driver.

---

### P-42 (S6) `messaging: O(1) unread counters (F-17 twin).` — M — migration 0023 — **[x] shipped #117** (maintained per-(user,channel) counter off the notification pass; +#118 MarkRead O(delta))

**What & why.** `Unreads` (`readstate.go:130-158`) is an O(user's
channels × messages-since-watermark) aggregate per request — its own
comment (`readstate.go:126-129`) names the unbuilt tier: "a maintained
per-(user,channel) counter updated on the notification path (F-17
deliverability sets) — same result, O(1) read. Documented, not built."
S6 builds it, reusing S1's invalidation/notification machinery. Also
finally wakes the mention badge: `ChannelUnread.Mentioned`
(`readstate.go:83`) is declared but never populated, and
`message_user_flag` is DORMANT (only a janitor DELETE, zero writers).

**O(1) invariant defended:** unread-count READ = O(user's channels)
index scan of a counter, never a re-aggregation over messages.
**O(N) blowup prevented:** per-request re-count over all messages since
the watermark in a high-volume 100k channel.

**Current verified state.** Live aggregate `readstate.go:131-143`;
`DMUnreads` `readstate.go:95-105`; scale note `readstate.go:126-129`;
`ChannelUnread.Mentioned` declared-but-unpopulated `readstate.go:83`.

**The build (decided: DM spaces use the SAME counter table for
symmetry).**

- **Table `channel_unread_counter` (0023):** `(user_id, channel_id,
  unread_count INT, mention_count INT, org_id)`, PK `(user_id,
  channel_id)`, `fillfactor` tuned (HOT updates, like the watermark).
- **Maintenance (consumer-driven, NOT send-path):** the notification
  materializer (S1) already visits every message's recipients — it
  increments `unread_count` there (and `mention_count` on a mention
  row), so the counter rides the EXISTING consumer pass, never the O(1)
  send path. MarkRead (`readstate.go:30`) resets the counter in the
  same tx as the watermark write. This wakes the `Mentioned` field via
  `mention_count`.
- **Consistency model:** the counter is a CACHE; a periodic
  reconciliation sweep (like S1's) recomputes from the watermark truth
  and logs divergence. The watermark (`thread_read_watermark`) stays
  the source of truth (`SCHEMA.md:37-45`).
- **`Unreads` read (`readstate.go:130`)** becomes a plain index scan of
  the counter table; the live aggregate stays as the reconciliation
  recompute only.

**Migrations.** `0023_unread_counters.sql`: `channel_unread_counter` +
index. Cite F-17 + `readstate.go:126-129`.

**Red/green proof plan.** `TestUnreadCounters`: in a high-volume
channel (bounded in CI), assert `GET /unreads` runs in O(channels) with
no per-message scan (S0 counter). Correctness: counter equals the live
aggregate after sends + mark-reads + edits/deletes. **RED:** point
`Unreads` back at the live aggregate → the scan cost scales with
message volume and the "O(1) read" assert goes red. Mention badge: a
mention increments `mention_count`, surfaced in
`ChannelUnread.Mentioned` (RED: the field stays false today).

**Risks / pre-mortem.** (1) **Counter drift** on edits/deletes/moves
(P-04 move changes a message's thread) — the reconciliation sweep is
the backstop; deletes decrement in `DeleteMessage`'s tx. (2) Must not
add cost to the O(1) send path — all increments ride the notification
consumer (already O(reasons) after S1), never `Send`.

**Dependencies + ordering.** After S1 (reuses the invalidation +
consumer-visit machinery). Independent of S3/S4/S5 —
parallel-capable with S3/S5 once S1 lands.

**Deferred (honest):** per-thread unread breakdown; the exact
edit/delete decrement vs recompute-on-read trade is the executor's to
flag if ambiguous in practice.

---

### P-43 (S7) `perms: CC-6 grant∩permission enforcement for agents.` — M — **DEFERRED — do not dispatch with this cluster**

Security-adjacent, not a per-message hot path. `capability_grant`
(`0002:143-156`) is a hook only — zero Go references; the reserved
intersection point is `perms.go:70-71`. When promoted (sequenced with
the automations write-back milestone, alongside P-26): `Require` gains,
for agent-kind principals (actor_kind 2), an intersection after the
closure `EXISTS` passes — effective verbs = group permissions ∩
`grant.scopes`, gated by `revoked_at IS NULL AND (expires_at IS NULL
OR expires_at > now())`; deny if no live grant covers the (verb,
scope). Grants NARROW only, never extend; human principals bypass.
One indexed lookup on `capability_grant_principal_idx`. Red/green:
`TestCapabilityGrant` (no grant → denied despite group permission;
live grant → allowed; revoked/expired → denied; RED: skip the
intersection → the agent writes on group permission alone).
Security-critical → Opus execution with the P-46 compensating
controls (the standing model directive prohibits Fable; see CLAUDE.md
directive 8).

---

### P-44 `channels: Announcement mode — O(1) unread at unbounded membership.` — L — migration 0026 — **SPEC-READY (carries the cluster's membership axis; the constant-write pin is the load-bearing line)**

**What & why.** After S6 (#117) and its follow-up (#118), the ONE
remaining per-message cost that scales with membership is
`ApplyMessageUnread`'s bulk UPSERT — one counter row per live member
per message (`unreadcounter.go`, honestly disclosed in its package
doc). It cannot be fixed by a better algorithm: unread is defined over
EVERY member, so unlike F-17 candidacy there is no opt-in set to shrink
to. The escape is a REDUCED CONTRACT. `channel.kind = 4 announcement`
has been specced since ADR-008 C-1 and sitting DORMANT in the schema
since 0003 (`migrations/0003_containers.sql:18-19`, "1 text · 2 forum ·
3 voice · 4 announcement") with zero code reading it — this slice wakes
it. Grounding: Zulip's `stream_post_policy`
(`zerver/models/streams.py:92-105` — EVERYONE / ADMINS / MODERATORS /
RESTRICT_NEW_MEMBERS) is the battle-tested precedent for the same idea.

**The enabling insight (state it in the commit body).** The restriction
IS the performance property. Send-restricted ⇒ low write rate ⇒ a
per-channel hot counter row is SAFE — which is precisely why F-15
banned that row for normal channels (`SCHEMA.md:30-32`, "no per-message
hot row on the root"): the objection there is write-RATE, not
principle. Flat (no reply threads) ⇒ no per-thread watermark scatter ⇒
unread becomes a subtraction instead of a per-thread aggregate. Read
`unreadcounter.go` IN FULL first (the drift ledger and the #118
delta discipline are the model to extend, not replace), plus
`readstate.go`, `channels.go`, `threads.go`'s send gate, and 0023.

**Design (decided):**
- **The channel counter** (migration 0026, SIBLING table
  `channel_announce_counter (channel_id PK, org_id, live_countable_total
  BIGINT NOT NULL DEFAULT 0)` — a sibling keeps `channel` narrow and
  gives the hot row its own `fillfactor=70`). `+1` per posted message,
  `-1` per delete of a live message. The column must appear in NO index
  so every bump is HOT-eligible (the 0003 discipline).
- **The reader position**: `container_unread_counter` (0023) gains
  `read_total BIGINT` — the value of `live_countable_total` when that
  reader last read. **unread = GREATEST(0, live_countable_total -
  read_total)**, computed at READ time. O(1) per container, and NO
  per-member write on send.
- **Send path** (kind=4 only): ONE counter increment + set the AUTHOR's
  own `read_total` to the new total in the same tx (an author has read
  their own post — this replaces the `author_id <> user_id` exclusion).
  Two row-writes per message, membership-INDEPENDENT. The kind=4 branch
  lives INSIDE the maintainer so `messaging` stays the single owner of
  counter writes (the LLD rule) — `ApplyMessageUnread` skips its bulk
  UPSERT for announcement channels.
- **MarkRead**: `read_total = live_countable_total`, one row, O(1). The
  #118 per-thread delta logic does not apply (single thread).
- **Mentions stay SPARSE**: a per-user `@**Name**` still writes only the
  mentioned members' `mention_count` — O(mentioned), the blessed
  `message_user_flag` shape (F-7). **Broadcast mentions (@channel/@here)
  DO NOT EXIST today** (verified: `content/parse.go` implements only
  `@**Full Name**`) and MUST NEVER be enabled for kind=4 — a structural
  rule, not a preference: one use would be O(members) notification rows.
- **Posting restriction**: a NEW registry verb `post_announcement`,
  default-assigned to `role:moderators`, retargetable through the
  existing `PUT /admin/verbs`. It resolves through the F-16 closure like
  every other verb — one join, no new mechanism — and stays orthogonal
  to `administer_channel` (managing a channel ≠ speaking in it).
- **Flat by construction**: a kind=4 channel has ONLY its channel-root
  thread (the F-15 kind-2 root). `CreateThread` against kind=4 → 400.
- **Read receipts stay refused** (definitionally O(members)) — reject
  the config with a clear error, the honest-rungs rule.

**Drift & the sweep.** Deleting an ALREADY-READ message drops
`live_countable_total` while that reader's `read_total` does not move →
under-count by 1, floored at 0, healed by the existing hourly
reconcile. This is the SAME bounded-drift class #118 established and
must be documented the same way; announcement delete rate is ~0.
`ReconcileUnreadOnce` needs a kind=4 arm that recomputes the
subtraction form and must NOT "repair" those rows with the member
aggregate (that arm would reintroduce the O(members) scan).

**Edge cases:** convert regular→announcement (seed each existing
member's `read_total` from their current unread — one-time O(members)
at a RARE event, which the charter explicitly permits) and
announcement→regular (seed `unread_count` from the subtraction); a
member JOINING an announcement channel gets `read_total =
live_countable_total`, so they start clean instead of inheriting a
100k-message badge (matches the `history_from` protected posture);
archived kind=4 freezes the counter; guests ride the same gates
unchanged; the author-is-also-a-reader case is covered by the send-path
rule above; a kind=4 channel with zero posts reads 0, never NULL.

**The conversational ceiling must be made HONEST in this slice.** Either
enforce a documented member cap on kind=1 channels with an error that
steers to announcement mode, or — if enforcement is deferred — record
the practical bound in REALITY WITH its O(members) per-message cost
stated. Do NOT leave it undefined; an unstated bound is how a 40k-member
"regular" channel reaches production unplanned.

**Tests (`TestAnnouncementChannel`, real PG).** The O(1) proof is the
load-bearing one: reuse the #117 QueryTracer/EXPLAIN pin discipline to
assert ONE send into an announcement channel writes a CONSTANT number
of counter rows (2) INDEPENDENT of membership — run at N=50 and N=500
members and assert the two counts are EQUAL, not merely small. Plus:
unread correct for a non-reader, 0 for the author, 0 after MarkRead;
delete decrements; the mention badge stays sparse and clears on a full
read; `post_announcement` non-holder 403 / holder 201; `CreateThread`
400; join seeds `read_total` (no backlog); both conversion directions
preserve per-user unread; the reconcile kind=4 arm repairs a seeded
divergence. **RED/GREEN (pin in a comment):** delete the kind=4 branch
so announcement sends fall back to the bulk UPSERT → the equal-counts
assert goes red (writes scale with N again) — the load-bearing line.

**Gaps to record:** a linked discussion channel for replies (the
designed answer to "flat"); announcement scheduling/digests; per-post
read receipts (NEVER — O(members)); Zulip's RESTRICT_NEW_MEMBERS tier;
importer mapping of Zulip announcement-only streams → kind=4; the
`forum` (kind=2) and `voice` (kind=3) kinds stay dormant.

---

### P-27 `importer: Slack.` — L (was XL — see the sizing correction) — ZERO migrations — **SPEC-READY (the IR prep refactor is commit 1 and is what makes this tractable; `slack_incoming` split out as P-27b)**

**Grounding done.** Zulip ships a mature Slack importer —
`~/Documents/zulip/zerver/data_import/slack.py` (1951 lines) +
`slack_message_conversion.py` — and it is the format authority for this
slice: every export-shape claim below was read out of it, not recalled.
Read BOTH in full before writing code, plus our
`internal/domain/importer/{importer.go,zulip.go,importer_test.go}`.

**The export format (verified in `slack.py`):** `users.json` ·
`channels.json` (public) · `groups.json` (private) · `mpims.json`
(group DMs) · `dms.json` (1:1) · one directory per conversation holding
dated `.json` message files, walked by an iterator. Threading is
`thread_ts` on replies pointing at the parent's `ts`. Reactions,
`files`, and `subtype` ride the message objects.

**SIZING CORRECTION — why this is L, not XL.** Our importer already
splits cleanly: `zulip.go` owns PARSING (`LoadZulipExport(dir)
(*Export, error)`) and `importer.go` owns WRITING (`Run` → `write`,
~900 lines: upsert-by-origin, the fidelity `Report`, role mapping, DM
landing, the attachments lane through `files.StorageKey`). A second
source needs only a second loader — **except that `Export` is
Zulip-SHAPED, not neutral**: `[]zulipUser`/`[]zulipStream`/
`[]zulipMessage` plus Zulip's recipient indirection
(`StreamByRecipient`, `PersonalTarget`, `DMGroupMembers`), none of
which has a Slack analogue. So:

- **Commit 1 is a PURE PREP REFACTOR (no behaviour change):** extract a
  source-neutral IR that a loader produces and `write` consumes;
  `LoadZulipExport` produces the IR. **The existing Zulip importer
  tests, unchanged and green, ARE the no-op proof** — do not edit them
  in this commit; if one needs editing, the refactor is not pure and
  you must stop and report. This obeys "prep refactors separate from
  features" and is the only way to add Slack without duplicating the
  write path (the LLD rule: no duplicated derivations).
- Later commits add `slack.go` — the loader only.

**Design (decided):**
- **Weft's thread model is a DIRECT fit — do NOT port Zulip's topic
  synthesis.** Zulip has topics, not threads, so `slack.py` had to
  invent topic names (`get_zulip_thread_topic_name`,
  `create_topic_name_for_message`, the `convert_slack_threads` option,
  `get_parent_user_id_from_thread_message`). Weft has first-class
  threads, so a Slack `thread_ts` maps 1:1 onto a Weft thread and the
  parent `ts` identifies its root. This is the single biggest
  simplification over the reference implementation; state it in the
  commit body so nobody ports the workaround.
- **Containers:** `channels.json` → public channels · `groups.json` →
  private · `mpims.json` → `dm_space` kind 2 (group) · `dms.json` →
  kind 1 (1:1). Weft's canonical participant key already arbitrates
  both, so no new DM machinery.
- **Users:** `users.json`; `deleted` → deactivated; `is_admin`/
  `is_owner` → role presets through the SAME shape as `weftRole`/
  `roleGroup`; Slack single-channel guests → the Weft guest role,
  honouring the P-5 role ceiling (invites mint member/guest only).
  Bots (`is_bot`, `is_slackbot`, `is_integration_bot_message`) map to
  the non-human user kind — never to kind=1 humans.
- **Markup conversion** (ground in `slack_message_conversion.py`):
  `<@U123|name>` → `@**Full Name**`; `<#C123|name>` → the channel ref;
  `<url|label>` → a markdown link; mailto; `*bold*`/`_italic_`/
  `~strike~` → the AST equivalents; `:emoji:`. **`@channel`/`@here`/
  `@everyone` MUST become INERT TEXT.** Weft has no broadcast mentions
  (verified: `content/parse.go` implements only `@**Full Name**`) and
  P-44 bans them structurally; rendering them as anything live would
  either lie about what the workspace can do or mint the exact
  O(members) shape the scale cluster spent seven slices removing.
  Slack `blocks` render best-effort to text (Zulip's `render_block` is
  the reference); anything unrenderable is a Report bucket, never a
  silent drop.
- **Subtypes:** `file_share` carries its upload in `file` rather than
  `files` — handle both. `channel_join`/`channel_leave` are noise and
  are dropped (Zulip drops them too) but COUNTED in the Report.
- **Files — v1 is OFFLINE, deliberately.** A standard Slack export
  carries file METADATA with `url_private`, not bytes, and Zulip's
  importer takes a download callback. Our Zulip importer is offline
  (reads `uploads/` from the unpacked dir) and staying offline keeps
  this slice free of a network+credential surface. So: if the bytes are
  present in the export dir (pre-fetched by operator tooling), land them
  through `files.StorageKey` exactly as the Zulip lane does; otherwise
  record each file in the Report as skipped WITH its reason. Slack's
  `mode: tombstone` / `hidden_by_limit` files (free-plan exports lose
  file content permanently) must be counted explicitly — that is a real
  fidelity loss and the Report exists so nobody discovers it later.
  Token-authenticated fetch THROUGH the P-15/P-24 egress guard is a
  recorded follow-up, not this slice.
- **The importer invariants hold unchanged:** actor kind 4, so
  backfills NEVER notify; upsert by `(org, origin_system='slack',
  origin_id)` so re-runs count `AlreadyImported`; `dryRun` parses and
  reports without writing; every source entity lands in exactly one
  Report bucket (ADR-001: "nobody trusts a migrator that hides its
  losses"). **Verify the S6 rule still holds on the new path:** the
  importer-actor early return must precede the unread-counter call, or
  a backfill inflates every member's badge.

**Edge cases:** a `thread_ts` whose parent is missing (deleted parent,
or a partial export) → the reply becomes a root rather than being
dropped, counted as such; messages in a channel absent from
`channels.json`; a DM whose participant is absent from `users.json`;
bot messages with no author; duplicate `ts` within one channel (Slack
permits it) — `origin_id` must stay unique, so compose it from
(channel, ts) rather than `ts` alone; archived channels; `mpims` that
are really 1:1 after departures.

**Tests (`TestSlackImport`, fixture-driven and fully OFFLINE — the
`importer_test.go` discipline).** Build a small hand-written export
fixture covering: public + private channels, a 1:1 and a group DM,
a threaded conversation, reactions, a bot message, a tombstoned file,
a `channel_join` subtype, and one of every markup form. Assert STATE —
row counts, thread parentage, DM participant sets, Report bucket
totals, and that the event log carries actor kind 4 with ZERO
notification rows minted. **RED/GREEN (pin both in comments):** (1)
ignore `thread_ts` → replies flatten onto the channel root and the
thread-shape assert goes red — the load-bearing line, since threading
is the whole reason this maps better to Weft than to Zulip; (2) render
`@channel` as a live mention → the inert-text assert goes red.

**Scale.** Slack exports are commonly multi-GB with one file per
conversation per day. v1 keeps the Zulip lane's one-transaction
all-or-nothing shape for consistency, and inherits its SAME recorded
follow-up (chunked streaming per messages file, which "keeps this exact
call shape"). Say so honestly in the PR rather than implying it
streams. Nothing here touches a per-message hot path.

**Split out — P-27b `messaging: Slack-compatible incoming webhook
(slack_incoming).`** — S. The compat endpoint is LIVE-traffic
compatibility, not import: it accepts Slack's incoming-webhook payload
shape so existing Slack integrations keep posting. It shares no code
with the importer, and P-23's capability-token webhook lane already
provides the auth model. Dispatch separately.

**Gaps to record:** token-authenticated file fetch behind the egress
guard; Slack Enterprise/compliance export shapes; canvases, huddles,
workflows, and saved items; user custom profile fields (Zulip maps
these — we have no counterpart surface yet); per-channel notification
prefs; shared/Connect channels (ADR-004 territory, not import).

---

### P-45 `gateway: Put the multicast pump on the logical feed (the seq contract).` — M — ZERO migrations — **SPEC-READY (the LAST consumer on the xmin gate; the decision this slice needed is made below)**

**What & why.** S4 (#124) moved notification, automation, and unfurl onto
the commit-ordered feed, but **deliberately left the gateway on the old
xmin gate**, because switching it is a WIRE-CONTRACT decision rather
than an implementation detail. So the undetectable-skip race
(`consumer.go` scale-contract block: a lower-txid transaction can hold a
higher id, and its commit advances the head past a still-in-flight lower
id) is closed for durable consumers and **still open for live fan-out**.
This slice closes it.

**The blocker, precisely.** `Envelope.Seq` IS the event id
(`gateway.go:513`, `:642`), and the live lane treats `r.id <= c.lastID`
as "already delivered" (`deliverShared`, `gateway.go:625`). Under
commit-order delivery a lower id legitimately arrives AFTER a higher one
— that is the entire point of the feed — so the existing skip would
DROP it at the connection. Removing the skip naively instead trades the
stall for duplicate storms and broken resume.

**THE DECISION (made at spec time; this is what was blocking dispatch):**
- **`seq` STAYS the event id.** It is the client's resume cursor and it
  must remain QUERYABLE — resume replays with `WHERE id > seq`. A
  decode-time delivery ordinal would order correctly but could not be
  replayed without persisting it, which would put durable state back in
  a gateway that ADR-002 designed to hold none. No wire-format change.
- **What changes is the SKIP, not the ordering.** `id <= lastID` must
  stop meaning "already sent". The live lane tracks what it has ACTUALLY
  delivered (commit-order cursor plus a bounded recently-delivered
  window covering the in-flight span), so a late-committing lower id is
  DELIVERED rather than silently dropped.
- **The protocol gains one honest requirement: clients MUST tolerate a
  duplicate `seq`.** They already tolerate GAPS (filtered events —
  `gateway.go:652` and the F-2 note), so the client contract moves from
  "gaps possible" to "gaps and rare duplicates possible", bounded to the
  live/resume hand-off. **This is the right trade and the reason to take
  it now: the alternative is silent LOSS, duplicates are strictly easier
  for a client to absorb than a missing event, REST remains the source
  of truth, and `docs/REALITY.md` records that NO real client exists
  yet — so this is the cheapest moment this contract will ever change.**
  Document it in ADR-002's protocol section, not just in code.

**Edge cases:** the live↔resume hand-off (already race-free under
`sh.mu` — do NOT regress it); a connection resuming with a `last_id`
above an event that commits later (the duplicate case — assert it is a
duplicate, never a drop); the sweep's empty gated read; the xmin driver
must keep working UNCHANGED, since it stays the default (`Open`'s
`""`/`xmin` case) — this slice must be correct on BOTH feeds.

**Tests.** Extend the S4 pins to the gateway: with the logical driver, a
crossing commit interleave (the `TestLogicalFeedNoCommitOrderSkip`
scenario) must reach a live WebSocket subscriber — **RED:** keep the
`id <= lastID` skip → the late-committing lower id never arrives and the
delivery assert goes red, which is the same undetectable loss S4 proved
at the consumer layer, now proved at the connection. Also: no duplicate
under steady state, at most one at the hand-off, `TestGatewayMulticast`'s
one-read-per-org counter unchanged, and the ACL negative (the outsider
still receives nothing) still green on the new path.

**Gaps to record:** cross-node routing (S5's plane already carries
presence; event routing stays per-cell); cohort/jittered resume for the
reconnect storm (recorded separately).

---

### P-46 `gateway: Complete the read ACL — history floor and visibility scope.` — M — ZERO migrations (both hooks exist) — **SPEC-READY (SECURITY-CRITICAL — Opus execution per the standing model directive, with the compensating controls below MANDATORY)**

**What & why.** `docs/REALITY.md` has rated this WORKS-THIN since it
landed: *"membership-set filter + refresh on membership events. Missing:
history_mode/protected floor, VisibilityScope."* Two real holes remain
in `client.filter` (`gateway.go:691+`):

1. **No protected-history floor.** `channel_member.history_from`
   (`0003_containers.sql:140`) is the ADR-008 C-2 protected-history
   boundary that REST enforces. The gateway does not consult it, so a
   member of a protected channel who RESUMES with a `last_id` from
   before their join replays events predating their access.
2. **Container-less events fan ORG-WIDE.** `filter`'s own doc says it:
   space/work-item events have no channel or DM to gate on, so every
   connection in the org receives them regardless of space access or
   guest status. `visibility_scope` (`0005_work_tracking.sql:224`) and
   `work_item.security_scope_id` are the dormant hooks for exactly this,
   and `actor.IsGuest()` already gates the equivalent REST surfaces
   (P-5).

**Exposure is METADATA, not content — say so honestly and fix it
anyway.** Event payloads carry ids and verbs, never message bodies (F-4),
and REST re-checks every read with an oracle-free 404. But ids, verbs,
actor ids and timing are exactly what an existence oracle is made of —
this is the same class P-33/P-34/#107/#111 spent four slices closing on
the REST side, and leaving the realtime plane exempt makes those slices
partial. S3's shared per-org read also concentrated everything into this
one predicate, so the filter is now the single choke point for the whole
org.

**Design (decided):**
- **The history floor is TIME-based, and that is the subtle part.**
  `history_from` is `TIMESTAMPTZ`, while the gateway orders by event id.
  Do NOT invent an id-space floor. Carry the event's `occurred_at` on
  `eventRow` (it is already on `event_log`) and compare against the
  per-channel floor loaded alongside the membership set in
  `loadChannels`, refreshed by the same membership events. State the
  chosen comparison and its boundary semantics (inclusive/exclusive) in
  the code — an off-by-one here is a silent leak or a silent gap.
- **Container-less events resolve visibility instead of fanning.** Load
  the connection's space-visibility set the way `channels`/`dms` are
  loaded, refresh it on the events that change it, and default to
  WITHHOLD when a scope cannot be resolved. **Fail closed** — an
  unresolvable scope must never fall through to org-wide delivery, which
  is precisely today's behaviour and the bug.
- **Guests ride the same predicate**, no role branch, matching how P-34
  handled guests (they ride the same gates, no special case).

**Edge cases:** a member who left and rejoined (the `history_from`
preserved on the surviving row — the #123 shape); an unsubscribed member
with a live connection; work-item events with a NULL
`security_scope_id`; a scope whose visibility changes mid-connection
(refresh must be driven by an event, or the stale set is a leak);
org-wide-visible space threads, which are legitimately org-wide and must
NOT regress into over-withholding.

**Tests (`TestGatewayReadACL`, real PG + real WebSockets).** The
load-bearing shape is the NEGATIVE: an outsider connection must receive
NOTHING, asserted the P-33/P-34 way — the outsider is a live fan target
so the assert cannot pass vacuously. Cover: a protected-history member
resuming from before their join receives nothing from before it, while a
shared-history member does; a guest receives no space/work-item events
outside their access; a non-member of a scoped space receives none;
membership and scope CHANGES refresh mid-connection. **RED/GREEN:**
(1) drop the history floor → the pre-join replay assert goes red;
(2) restore the org-wide fan for container-less events → the guest/space
assert goes red — the load-bearing line.

**COMPENSATING CONTROLS (mandatory — this slice is security-critical
and the model tier that the old rule reserved for such work is no
longer available, so the rigour has to come from PROCESS):**
reviewer line-by-line review before the PR is opened, not after; a
red/green pin for EVERY access-control claim in the commit message,
not just the two named above; every negative test written as a live
fan target so it cannot pass vacuously (the P-33/P-34 discipline); and
an explicit written argument in the PR body for why each predicate
FAILS CLOSED. If the executor cannot produce those, it must stop and
report rather than ship.

**Gaps to record:** the client-side snapshot-refetch rule (REALITY's
third missing item — it needs a real client, so it stays recorded);
per-scope refresh granularity if the reload proves hot.

---

## Tier 2 — scoped, but DO NOT dispatch until promoted to Tier 1
(Each needs a final-spec pass by the strongest model; the bullets below
record the scope and the known design questions so nothing is lost.)

- **P-11** — **promoted to Tier 1** (full spec above; zero migrations —
  the sprint table + work_item.sprint_id exist since 0005).
- **P-12** — **promoted to Tier 1** (full spec above; zero migrations —
  rank/rank_context + view_def exist since 0005).
- **P-13** — **promoted to Tier 1** (full spec above; zero migrations —
  field_def + work_item.fields + the link tables exist since 0005).
- **P-14** — **promoted to Tier 1** (full spec above; re-scoped to
  waking the dormant pinned/color columns — named sections need
  schema and stay here).
- **P-15** — **promoted to Tier 1** (full spec above; STRONGEST-MODEL
  EXECUTION — the egress guard is the reusable core P-24 inherits).
- **P-16** — **promoted to Tier 1** (full spec above; STRONGEST-MODEL EXECUTION; zero migrations — the columns exist since 0003).
- **P-17** — **promoted to Tier 1** (full spec above; strongest-model
  execution; archive shape decided = in-place tombstone; restore API
  deferred as a recorded gap).
- **P-18** — **[x] shipped #102** (zero migrations; pure-Go x/image
  behind a seam, thumbs as derived blobs; bomb-cap pin proven).
  Opens the mixed quartet.
- **P-19** — **promoted to Tier 1** (full spec above).
- **P-20** — **promoted to Tier 1** (full spec above).
- **P-21** — **[x] shipped #105** (migration 0019; in-house Web Push
  proven against RFC 8291 Appendix A, no new dep; every send through
  the egress guard; reviewer-completed after the executor stalled).
- **P-22** — **[x] shipped #97** (mention-injection guard is the
  structural label-multiset comparison, not escaping, as decided).
- **P-23** — **[x] shipped #98** (migration 0017; capability-token
  webhooks, constant-time compare, oracle-free 404; structured
  schedule grammar, no cron dep).
- **P-24** — **[x] shipped #99** (migration 0018; async delivery lane
  behind the P-15 egress guard, static URLs + fixed envelope, O(1)
  health with alert at 5/15 and auto-disable at 20, reset on
  re-enable). Closes the automation cluster.
- **P-25** — **promoted to Tier 1** (full spec above).
- **P-26 `automation: LLM steps + budgets + approval gates.`** XL —
  NEEDS-DESIGN: model gateway seam, budget metering, run status 8 flow.
- **P-27** — **promoted to Tier 1** (full spec above; grounded in
  Zulip's own `data_import/slack.py` rather than format docs. Sized
  DOWN to L: the write path is already reusable, so commit 1 is a pure
  IR prep refactor and the rest is a loader. Weft's first-class threads
  take Slack's `thread_ts` directly — Zulip's topic-synthesis
  workaround must NOT be ported. `slack_incoming` split out as P-27b).
- **P-28 `importer: Jira.`** XL — deliberately last (M3 exit).
- **P-29** — **promoted to Tier 1** (full spec above; password RESET
  split out as P-35 — it depends on P-20's mail plumbing).
- **P-30** — **[x] shipped #104** (migration 0020; go-oidc/v3 +
  x/oauth2, the deliberate dep exception; verified-email linking, no
  JIT; single-use state + IdP dials through the egress guard).
- **P-31** — **promoted to Tier 1** (full spec above).
- **P-32** — **promoted to Tier 1** (full spec above; raw zip bundle —
  the eDiscovery/partner manifest FORMAT stays here as a later
  refinement).
- **P-34** — **[x] shipped #103** (zero migrations; private channels
  404 like DMs, public keeps 403; oracle-indistinguishability pin
  proven. Closes the mixed quartet. FOLLOW-UP: worktrack.PromoteThread
  still 403s a private non-member — a narrow residual oracle to close
  in a later slice).
- **P-35** — **promoted to Tier 1** (full spec above; token storage
  decided: DB rows — single-use + revoke-on-change require server
  state).

## Deliberately NOT in this queue (backend-first directive)
The real web client, mobile apps, calls/LiveKit, the automation builder
UI, and federation (ADR-004 T2) — v2 or post-directive items. The
dogfood UI gets minimal hooks only, inside feature slices.
