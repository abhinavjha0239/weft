package notification

import (
	"html/template"
	"strings"

	"github.com/abhinavjha0239/weft/internal/brand"
)

// digestHTML is the multipart/alternative HTML companion to the plain-text
// digest. Every dynamic value (the per-notification lines, the unsubscribe
// URL) is auto-escaped by html/template — the lines carry who/where only,
// never message content, so escaping is belt-and-suspenders. Deliberately
// minimal: richer HTML is client-era polish (recorded gap).
var digestHTML = template.Must(template.New("digest").Parse(`<!doctype html>
<html>
<body>
<h2>{{.Brand}}</h2>
<ul>
{{range .Lines}}<li>{{.}}</li>
{{end}}</ul>
<p>Open {{.Brand}} to read and reply.</p>
{{if .UnsubURL}}<p><a href="{{.UnsubURL}}">Unsubscribe</a></p>{{end}}
</body>
</html>
`))

type digestData struct {
	Brand    string
	Lines    []string
	UnsubURL string
}

// renderDigestHTML builds the HTML alternative for a digest. An empty unsubURL
// (no signing secret configured) omits the unsubscribe footer.
func renderDigestHTML(lines []string, unsubURL string) (string, error) {
	var sb strings.Builder
	if err := digestHTML.Execute(&sb, digestData{
		Brand: brand.Name, Lines: lines, UnsubURL: unsubURL,
	}); err != nil {
		return "", err
	}
	return sb.String(), nil
}
