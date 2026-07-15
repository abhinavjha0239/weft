package unfurl

import (
	"io"
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/net/html"
)

// pageMeta is what a page says about itself.
type pageMeta struct {
	title, description, image, site string
}

// parseMeta tokenizes (never DOM-builds — the input is capped, hostile
// HTML) and collects OpenGraph tags, Twitter-card fallbacks, and the
// <title> text. Priority per field: og:* > twitter:* > <title>. The
// tokenizer tolerates truncated input (the 1 MiB cap can cut mid-tag), so a
// giant page still yields whatever metadata sat in its head.
func parseMeta(r io.Reader) pageMeta {
	var meta pageMeta
	var twitter pageMeta
	var docTitle string

	z := html.NewTokenizer(r)
	inTitle := false
	for {
		switch z.Next() {
		case html.ErrorToken:
			// io.EOF or malformed input: return what we have either way.
			if meta.title == "" {
				meta.title = twitter.title
			}
			if meta.title == "" {
				meta.title = docTitle
			}
			if meta.description == "" {
				meta.description = twitter.description
			}
			if meta.image == "" {
				meta.image = twitter.image
			}
			return meta
		case html.TextToken:
			if inTitle && docTitle == "" {
				docTitle = string(z.Text())
			}
		case html.StartTagToken, html.SelfClosingTagToken:
			name, hasAttr := z.TagName()
			switch string(name) {
			case "title":
				inTitle = true
			case "meta":
				var prop, metaName, content string
				for hasAttr {
					var k, v []byte
					k, v, hasAttr = z.TagAttr()
					switch strings.ToLower(string(k)) {
					case "property":
						prop = strings.ToLower(string(v))
					case "name":
						metaName = strings.ToLower(string(v))
					case "content":
						content = string(v)
					}
				}
				if content == "" {
					continue
				}
				switch prop {
				case "og:title":
					if meta.title == "" {
						meta.title = content
					}
				case "og:description":
					if meta.description == "" {
						meta.description = content
					}
				case "og:image":
					if meta.image == "" {
						meta.image = content
					}
				case "og:site_name":
					if meta.site == "" {
						meta.site = content
					}
				}
				switch metaName {
				case "twitter:title":
					if twitter.title == "" {
						twitter.title = content
					}
				case "twitter:description":
					if twitter.description == "" {
						twitter.description = content
					}
				case "twitter:image":
					if twitter.image == "" {
						twitter.image = content
					}
				}
			}
		case html.EndTagToken:
			if name, _ := z.TagName(); string(name) == "title" {
				inTitle = false
			}
		}
	}
}

// clean flattens whitespace runs (newlines included) to single spaces,
// strips other control characters, trims, and caps at max runes — preview
// strings render as single-line text, whatever the page shipped.
func clean(s string, max int) string {
	var b strings.Builder
	b.Grow(len(s))
	space := false
	for _, r := range s {
		if unicode.IsSpace(r) {
			space = true
			continue
		}
		if unicode.IsControl(r) {
			continue
		}
		if space && b.Len() > 0 {
			b.WriteByte(' ')
		}
		space = false
		b.WriteRune(r)
	}
	out := b.String()
	if utf8.RuneCountInString(out) > max {
		runes := []rune(out)
		out = string(runes[:max])
	}
	return out
}
