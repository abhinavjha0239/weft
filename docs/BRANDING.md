# Branding & the rename procedure

The product name is a launch-time requirement to keep **easily changeable**.
Rules that make that true:

1. **One source of truth in code:** `internal/brand/brand.go` (`Name`, `Slug`,
   `EnvPrefix`, `Tagline`). No **code or machine surface** — Go source, config,
   migrations, manifests, UI copy, errors — may contain the product name as a
   literal. The CI `conventions` job enforces this (a Go grep for quoted brand
   strings outside `internal/brand`, and a non-Go grep over machine surfaces).
   **Prose is exempt**: `docs/` and `README.md` naturally name the product;
   the rule protects rebrandability of code/config, not documentation.
2. **Env vars** all read through `brand.EnvPrefix`.
3. **Database, schema, and wire protocol are name-free**: no table, column,
   event type, or API path contains the brand. (They never should anyway.)
4. **The two mechanical exceptions**, changed by one rewrite each:
   - the Go module path (`github.com/abhinavjha0239/weft`) — a rename is one
     `gofmt -r`/sed across imports plus `go.mod`;
   - the binary directory `cmd/weftd` — a directory rename.
5. **Self-hosters may rebrand at runtime** (white-label) via config overriding
   `Name`/`Tagline` for UI surfaces — planned when the web client lands; the
   constant is the default, not a cage.

Rename checklist: edit `brand.go` → rename `cmd/<slug>d` → rewrite module path →
update repo name/remotes → done. Everything else follows.
