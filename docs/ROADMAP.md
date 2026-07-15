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
- **P-14** — **promoted to Tier 1** (full spec above; re-scoped to
  waking the dormant pinned/color columns — named sections need
  schema and stay here).
- **P-15** — **promoted to Tier 1** (full spec above; STRONGEST-MODEL
  EXECUTION — the egress guard is the reusable core P-24 inherits).
- **P-16 `channels: Web-public channels + history_from enforcement.`**
  L — SECURITY-CRITICAL (anonymous read path + membership history
  boundaries). Strongest-model execution, full spec first.
- **P-17 `compliance: Message retention vacuum.`** L — archive-then-
  vacuum with restore window (AD-3 completion). OPEN: archive storage
  shape (rows vs export-file), restore API. Strongest-model execution.
- **P-18 `files: Image thumbnails + inline rendering allowlist.`** M —
  OPEN: image processing dependency choice (pure-Go vs libvips),
  thumbnail storage keys, srcset shape.
- **P-19** — **promoted to Tier 1** (full spec above).
- **P-20** — **promoted to Tier 1** (full spec above).
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
- **P-25** — **promoted to Tier 1** (full spec above).
- **P-26 `automation: LLM steps + budgets + approval gates.`** XL —
  NEEDS-DESIGN: model gateway seam, budget metering, run status 8 flow.
- **P-27 `importer: Slack.`** XL — export parsing + slack_incoming
  compat endpoint. Ground in Slack export format docs before speccing.
- **P-28 `importer: Jira.`** XL — deliberately last (M3 exit).
- **P-29** — **promoted to Tier 1** (full spec above; password RESET
  split out as P-35 — it depends on P-20's mail plumbing).
- **P-30 `identity: OIDC login.`** L — NEEDS-DESIGN: library choice,
  account linking rules, JIT provisioning vs invite-only.
- **P-31** — **promoted to Tier 1** (full spec above).
- **P-32** — **promoted to Tier 1** (full spec above; raw zip bundle —
  the eDiscovery/partner manifest FORMAT stays here as a later
  refinement).
- **P-34 `channels: Private-channel existence masking.`** S —
  NEEDS-DESIGN: requireChannelMember returns 403 today; decide whether
  private channels 404 like DMs (survey Zulip/Slack semantics, client
  impact, and the listable-public asymmetry) before any executor
  touches it. Split off from P-33.
- **P-35** — **promoted to Tier 1** (full spec above; token storage
  decided: DB rows — single-use + revoke-on-change require server
  state).

## Deliberately NOT in this queue (backend-first directive)
The real web client, mobile apps, calls/LiveKit, the automation builder
UI, and federation (ADR-004 T2) — v2 or post-directive items. The
dogfood UI gets minimal hooks only, inside feature slices.
