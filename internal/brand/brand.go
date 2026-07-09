// Package brand is the single source of the product's name and identity.
//
// The project is rebrandable by design (a launch requirement): no other file
// may contain the product name as a literal string — not in code, UI copy,
// env vars, or user-facing errors. The full rename procedure (including the
// one mechanical go-module-path rewrite) is documented in docs/BRANDING.md.
package brand

const (
	// Name is the product's display name, used in UI and user-facing text.
	Name = "Weft"

	// Slug is the lowercase machine name: binary names, config paths,
	// default database name, telemetry identifiers.
	Slug = "weft"

	// EnvPrefix prefixes every environment variable the server reads.
	EnvPrefix = "WEFT_"

	// Tagline is the one-line product description.
	Tagline = "chat, threads, and work tracking, woven into one self-hosted fabric"
)
