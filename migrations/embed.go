// Package migrations embeds the SQL schema, so every binary carries its own
// migrations: no working-directory or packaging dependence for self-hosters.
// The //go:embed directive fails the BUILD if the pattern matches nothing,
// which retires the silently-empty-glob failure mode outright.
package migrations

import "embed"

//go:embed 0*.sql
var FS embed.FS
