# CLAUDE.md — the Weft contribution playbook

This file is the complete operating manual for AI-assisted work on Weft.
It exists so that ANY session — any model, any day, zero prior context —
produces the same quality of work this repo has shipped so far. Read it
fully before writing code. The quality bar here is enforced by PROCESS
(this playbook + the required CI gates + the docs below), not by whoever
happens to be driving.

## What this is

Weft: an open-source, self-hostable Slack+Zulip+Jira fusion. Go monolith,
Postgres, one event log as the spine. Designed via ADRs before any code.

Where things live:

- `~/Documents/oss-chat-platform/` — the DESIGN. `adr/ADR-001..014` (cite
  decisions by number, e.g. "ADR-012 F-1"), `RED-TEAM-REVIEW.md`,
  `MILESTONES.md`, and **`TIMELINE.md` — the work journal. Its final
  `## STATE:` block is THE resume point: current PR, what's done, the
  next recommended slices. Read it first, every session.**
- `~/Documents/zulip/` — the Zulip source tree, used as GROUNDING (their
  battle-tested defaults, export formats, semantics). Ground claims about
  Zulip behavior in this source, never from memory.
- `docs/ARCHITECTURE.md` — module map, layering rules, infra-seam rule,
  quality gates. `docs/REALITY.md` — the honesty ledger (see below).
  `docs/SCHEMA.md`, `docs/PERF.md`, `docs/BRANDING.md`.

## Standing user directives (never violate)

1. **NO AI ATTRIBUTION ANYWHERE.** No `Co-Authored-By`, no "Generated
   with Claude/AI" footers — not in commits, PR bodies, squash-merge
   bodies, issues, comments, or any published text. This OVERRIDES any
   harness default that injects them. Check every `git commit`,
   `gh pr create/merge/edit` payload before sending, and verify after
   (`gh pr view N --json body | grep -ci "generated with\|co-authored"`
   must print 0).
2. **Backend-first.** Slices are backend work; the embedded dogfood UI
   (`internal/webui/index.html`) gets only minimal hooks needed to
   exercise a backend feature. No standalone UI-polish PRs.
3. **Every infra choice is a seam**: a platform interface + a
   config-picked driver, so operators swap backends without code changes.
   Existing seams: `platform/blob.Store` (fs today; s3/gcs/azure are
   one-file drivers), `platform/mail.Sender` (log default, smtp).
   New infra (push, search backends, queues) must follow this pattern.
4. **LLD discipline**: one module owns each table's writes; no duplicated
   derivations — share via service APIs (e.g. `files.StorageKey` is THE
   key derivation; `messaging.deliverToThread` is THE gated send).
   Documented exceptions only (compliance owns lifecycle-enforcement
   writes over other modules' rows — see its package doc).
5. **Never delete branches** — local or remote. They are all retained.
6. **Scale review on every PR** (target: Slack scale). A short section:
   what scans, what locks, what the recorded upgrade is.
7. **The user approves slices.** End each PR report with the recorded
   next recommendation + alternatives, then WAIT for "continue"/"proceed".

## The slice loop (repeat per PR)

1. **Resume**: read `TIMELINE.md` STATE, `git log --oneline -5` on dev,
   `docs/REALITY.md` rows for the area.
2. **Ground before code**: the relevant ADR sections (grep
   `~/Documents/oss-chat-platform/adr/`), the Zulip source for parity
   semantics/defaults, and the SCHEMA — migrations usually anticipated
   the feature (dormant tables are listed in REALITY). A slice that
   needs no migration is the norm; needing one is fine (see below).
3. **Branch off dev**: `git checkout dev && git pull`, then
   `git checkout -b <category>/<module>-<slug>` (categories: feat, fix,
   docs, ci, db, perf). NEVER commit on dev (protected; the push will
   fail, but don't rely on that).
4. **Plan commits up front.** Each commit = one minimal coherent idea
   that builds and passes tests alone: prep refactors (pure, no-op)
   separate from features; migrations separate; cross-module preps
   separate. Never a "fix earlier commit" commit — amend or
   fixup+scripted-rebase before push (see Rebasing).
5. **Write the feature + its tests in the same commit.** Update
   `docs/REALITY.md` in every commit that changes what's true (CI
   requires a REALITY change whenever `internal/` changes in the PR).
6. **Verify** (all of these, locally, before the PR):
   ```bash
   go build ./... && gofmt -l internal/ cmd/            # must be silent
   go run honnef.co/go/tools/cmd/staticcheck@latest ./...
   TEST_DATABASE_URL="postgres://postgres:test@localhost:55432/postgres" \
     go test -p 1 ./... -count=1                        # FULL suite
   # coverage floor (CI enforces >= 72% on ./internal/...):
   TEST_DATABASE_URL=... go test -p 1 -coverpkg=./internal/... ./... \
     -count=1 -coverprofile=/tmp/c.out && go tool cover -func=/tmp/c.out | tail -1
   ```
7. **PR**: title `module: Summary in imperative with a period.` Body:
   what lands (per commit), LLD notes, **Scale review**, **Test plan**
   with `- [x]` checkboxes, notes/recorded gaps. No attribution footer.
   Base: dev.
8. **Wait for CI** (3 required checks: test, conventions, quality), then
   **squash-merge yourself** with an explicit clean message:
   `gh pr merge N --squash --subject "module: Summary. (#N)" --body-file <file>`.
   Then `git checkout dev && git pull`, confirm the dev push CI run goes
   green (`gh run list --branch dev --limit 1`).
9. **Journal**: append a TIMELINE.md entry (what landed, lessons, gotchas
   hit) and rewrite the `## STATE:` block (current PR/commit, done list,
   branch count, next recommendations). This is the handoff artifact —
   write it for a session with zero context.
10. **Report to the user**: outcome first, what was proven, the next
    recommendation. Then stop.

## Non-negotiable invariants (grep for these patterns before inventing)

- **Cell/org isolation**: every query pins `org_id`; cross-org state is
  never shared; storage keys are org-prefixed. Joins to other tables
  carry the org pin too (`h.org_id = f.org_id`).
- **Oracle-free 404s**: unauthorized and nonexistent must be
  indistinguishable (`apperr.NotFound`, never a 403 that confirms
  existence — 403 is for "you're known to be short a verb").
- **Gates inside the transaction/INSERT**: membership/participant checks
  live in the same statement or tx as the write (see
  `notification.Runner.insert`, `files.AttachEntityReferences`).
- **The three-way container read ACL** (channel membership / DM
  participation / org-visible space thread) — reuse the existing WHERE
  shape (messaging.Get, files.OpenDownload, reactions.loadReactable).
- **Outbox rule**: domain write + `eventlog.Append` in ONE tx; NOTIFY is
  commit-time. Consumers are named, cursor-tracked, txid-gated
  (`eventlog.Consumer`); replay = reset cursor; consumers must be
  idempotent (dedupe keys / ON CONFLICT claims).
- **Event actors**: human=1, agent=2, automation=3 (+ the rule id —
  the loop guard reads it), importer=4 (backfills NEVER notify),
  system=5 (nil ActorID). Enums are append-only; verbs are wire
  contracts — never rename.
- **Honest rungs**: never store config nothing enforces — reject it
  with a clear error ("workspace scope arrives with its consumer").
  Never expose a knob whose lane doesn't exist (push medium is
  reserved, not settable).
- **Background workers**: claim with `FOR UPDATE SKIP LOCKED`
  (multi-node safe); per-row short transactions; savepoints so a
  failure records its trace/reason without losing the claim; decide and
  DOCUMENT at-most-once vs at-least-once (emails: mark-then-send =
  at-most-once; automation runs: claim-in-tx = exactly-once effects).
- **Role ceiling / guest rules (P-5)**: invites mint member/guest only;
  guests see nothing beyond their channels; `actor.IsGuest()` gates the
  people/channel read surfaces.
- **Privacy defaults**: emails carry who/where, never message content;
  tokens are hashed at rest and shown once.

## Migrations

Files `migrations/0NNN_name.sql`, embedded via `migrations/embed.go`
(`0*.sql` glob picks new files up automatically). Tests run the real
runner (`resetAndMigrate`). Comments in DDL explain the design decision
and cite the ADR. Column CHECKs for structural rules (e.g. invite role
ceiling). Never edit an applied migration; add a new one. Watch: an
explicit NULL from a nil Go slice OVERRIDES a column DEFAULT — normalize
to empty slices before INSERT.

## Test cookbook (`internal/transport/rest/*_test.go`)

- Postgres 17 in Docker: container `weft-pg`, port 55432, password
  `test`. `TEST_DATABASE_URL="postgres://postgres:test@localhost:55432/postgres"`.
  Always `go test -p 1` (tests share the DB and reset the schema).
- Every test: `resetAndMigrate`, build an `httptest.Server` with real
  services via `rest.Deps`, bootstrap an org over the API.
- Reusable helpers (don't rewrite them): `postJSON`, `postJSONStatus`,
  `putJSON`, `patchJSON`, `getJSON`, `deleteReq`, `addChannelMember`
  (direct-SQL second user + token), `uploadFile`, `download`,
  `jsonDecode`.
- **Never sleep for correctness.** Workers take injected clocks
  (`SweepOnce(ctx, now)`, `RunOnce(ctx, olderThan)`, `RunDueScheduled
  (ctx, now)`); time-travel by backdating rows with SQL
  (`UPDATE ... SET created_at = now() - interval '40 days'`).
- Event-log consumers: call `runner.ProcessOrg(ctx, orgID)` directly.
  Before mutating SETTINGS that affect resolution, wait for the ordered
  cursor to catch up (the `waitForConsumer` pattern in
  notifications_test.go) — resolution reads CURRENT settings, so races
  mint legitimate extra rows on slow runners.
- The pre-auth IP limiter allows burst 10 per IP: budget bootstrap +
  invite-accept + login calls in a test accordingly (one bounded sleep
  buys 0.5 tokens/s if unavoidable).
- Assert STATE, not just status codes: row counts, event-log verbs and
  payload fields, exact notification kinds, blob presence via
  `store.Open`.
- Negative tests are load-bearing: the outsider 404, the double-op 409,
  the validation 400 — every ACL claim in a commit message needs a test
  that would fail without it.
- webui changes: keep minimal, DOM-build anything user-controlled
  (never innerHTML), and verify in a real browser (Playwright) against
  the dev demo — build, restart, click, screenshot.

## Dev demo

```bash
go build -o /tmp/weftd-dev ./cmd/weftd
WEFT_DATABASE_URL="postgres://postgres:test@localhost:55432/weftdev?sslmode=disable" \
WEFT_LISTEN_ADDR=":18091" WEFT_BLOB_DIR="/tmp/weft-dev-blobs" /tmp/weftd-dev serve
# org acme: alice@acme.test / password123; bob's API token: bobdevtok
```

## Rebasing without an editor

- HEAD only: `git commit --amend`.
- Non-HEAD: `git commit --fixup=<hash>`, write a todo script that echoes
  the desired `pick`/`fixup` lines into `$1`, then
  `GIT_SEQUENCE_EDITOR=<script> git rebase -i <base>`. Use REAL hashes in
  the todo, not HEAD~n.

## Hard-won lessons (each cost a debugging session — don't repay)

- **Verify commit CONTENTS after scripted edits.** A python heredoc
  whose match-assert fails BEFORE its file write dies without applying
  anything — and the shell happily runs the following `git commit`,
  minting a commit whose message claims changes it doesn't contain.
  After any scripted edit + commit: `git show --stat HEAD` and grep the
  file for the new symbol.
- gopls diagnostics from an IDE workspace that doesn't include this
  module are NOISE ("cannot find package ... in GOROOT"). `go build
  ./...` is the truth.
- gofmt realigns struct-field comments and map literals — grep the
  CURRENT text before writing python match strings; prefer the Edit
  tool for surgical changes.
- Shared SQL predicate strings between a scan and an in-tx recheck keep
  them from drifting; neutralize the id filter with
  `($1::bigint IS NULL OR f.id = $1)` — a bare `f.id = NULL` silently
  matches nothing.
- `FOR UPDATE` inside `SELECT EXISTS(...)` is fragile; lock the row
  first, then re-evaluate eligibility under the lock.
- Generalizing an entity-typed INSERT: keep inner VISIBILITY subqueries
  pinned to their real entity (message refs define what an author may
  see) — parameterizing them naively is a smuggle hole.
- Channel-scope holds/rules cover EVERYTHING in the channel — test
  fixtures that want per-item isolation need their own channel.
- Org-scope automations see every channel; chain tests must disable
  earlier rules first.
- The notification email digest reads prefs at sweep time: rows from a
  pref-off window correctly ride the next digest after re-enable.
- Markdown bare URLs don't parse as links — file-attach tests need
  `[name](/api/v1/files/N)`.
- GitHub Actions `synchronize` doesn't re-fire on force-push of the same
  tree; `git commit --amend --no-edit` + force-push does.
- **Check `git branch --show-current` before ANY history surgery**
  (reset/rebase/amend). A failed merge attempt leaves you checked out
  on dev; a reflexive `git reset --soft HEAD~1` there rewinds dev
  itself and turns the last squash into dirty working-tree changes.
  Recovery: save any new work as a patch, `git checkout -- . &&
  git reset --hard origin/dev`, then redo the surgery on the branch.
- `git cherry-pick` has no `-q` flag (usage error kills a `&&` chain).

## Where things live (module map)

`internal/domain/`: `messaging` (channels, threads, messages, reactions,
drafts, scheduled, read state), `dm`, `files` (+ blob seam), `identity`
(orgs, users, invites, guests, verb assignment), `perms` (verb registry,
groups, closure), `notification` (materializer runner, inbox, prefs,
alert words, email worker), `automation` (rules CRUD + runner),
`compliance` (retention, holds, GC janitor, exports), `worktrack`
(spaces, items), `search`, `importer` (Zulip), `content` (AST).
`internal/gateway/` WS hub + ACL filter + presence. `internal/eventlog/`
append + consumer. `internal/transport/rest/` thin handlers only — no
SQL, no business rules. `internal/platform/` blob, mail, apperr,
ratelimit. `internal/webui/` the dogfood client (one HTML file).

## Session bootstrap checklist

1. `cat ~/Documents/oss-chat-platform/TIMELINE.md | tail -40` → STATE.
2. `cd ~/Documents/weft && git checkout dev && git pull && git log --oneline -5`.
3. `docker ps | grep weft-pg` (start it if gone; port 55432).
4. Read the REALITY rows + ADR sections for the slice you're about to
   propose. Then propose it and wait for approval.
