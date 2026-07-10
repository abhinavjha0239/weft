// Package webui serves the embedded minimal web client — the dogfood UI
// (the full client app is a later milestone). Zero build toolchain: one HTML
// file, vanilla JS, talks to /api/v1 and the gateway WebSocket.
//
// The product name is injected from the brand token at serve time; the HTML
// template contains no brand literal (docs/BRANDING.md).
package webui

import (
	_ "embed"
	"html/template"
	"net/http"

	"github.com/abhinavjha0239/weft/internal/brand"
)

//go:embed index.html
var indexHTML string

// Handler serves the client at "/".
func Handler() (http.Handler, error) {
	tpl, err := template.New("index").Parse(indexHTML)
	if err != nil {
		return nil, err
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		_ = tpl.Execute(w, map[string]string{
			"Brand":   brand.Name,
			"Tagline": brand.Tagline,
		})
	}), nil
}
