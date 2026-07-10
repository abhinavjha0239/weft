// Package content is the Portable AST engine (ADR-007 M-1): source markdown
// parses into a vendor-neutral typed AST (the canonical stored form — an
// ADF-compatible shape, since our node set must be a superset of Jira ADF),
// and HTML renders FROM the AST only. No raw HTML ever passes through, which
// makes the renderer XSS-safe by construction: every text byte is escaped,
// every attribute is generated, link schemes are allowlisted.
package content

import (
	"encoding/json"
	"html"
	"net/url"
	"strconv"
	"strings"
)

// RenderVersion stamps message.render_version; bump when render output
// changes so cached renders can be re-rendered lazily.
const RenderVersion = 2

// Node is the AST shape: {type, attrs?, content?, text?, marks?}.
type Node struct {
	Type    string         `json:"type"`
	Attrs   map[string]any `json:"attrs,omitempty"`
	Content []*Node        `json:"content,omitempty"`
	Text    string         `json:"text,omitempty"`
	Marks   []Mark         `json:"marks,omitempty"`
}

type Mark struct {
	Type  string         `json:"type"`
	Attrs map[string]any `json:"attrs,omitempty"`
}

// Node types (ADR-007 superset; grows toward full ADF coverage).
const (
	NodeDoc         = "doc"
	NodeParagraph   = "paragraph"
	NodeHeading     = "heading"
	NodeBlockquote  = "blockquote"
	NodeBulletList  = "bullet_list"
	NodeOrderedList = "ordered_list"
	NodeListItem    = "list_item"
	NodeCodeBlock   = "code_block"
	NodeRule        = "rule"
	NodeHardBreak   = "hard_break"
	NodeTable       = "table"
	NodeTableRow    = "table_row"
	NodeTableCell   = "table_cell"
	NodeMention     = "mention"
	NodeEmoji       = "emoji"
	NodeText        = "text"
)

const (
	MarkStrong = "strong"
	MarkEm     = "em"
	MarkStrike = "strike"
	MarkCode   = "code"
	MarkLink   = "link"
)

// JSON serializes the AST for the message.ast column.
func (n *Node) JSON() json.RawMessage {
	b, err := json.Marshal(n)
	if err != nil {
		// A Node is plain data; this cannot fail on real input.
		panic("content: marshal ast: " + err.Error())
	}
	return b
}

// Mentions returns resolved user ids in document order (feeds the
// message.created payload and, later, notification candidates).
func (n *Node) Mentions() []int64 {
	var out []int64
	n.walk(func(node *Node) {
		if node.Type == NodeMention {
			if id, ok := node.Attrs["user_id"].(int64); ok {
				out = append(out, id)
			} else if f, ok := node.Attrs["user_id"].(float64); ok {
				out = append(out, int64(f))
			}
		}
	})
	return out
}

// HasLink reports whether any link mark or autolink exists (message.has_link).
func (n *Node) HasLink() bool {
	found := false
	n.walk(func(node *Node) {
		for _, m := range node.Marks {
			if m.Type == MarkLink {
				found = true
			}
		}
	})
	return found
}

func (n *Node) walk(fn func(*Node)) {
	fn(n)
	for _, c := range n.Content {
		c.walk(fn)
	}
}

// RenderHTML produces the cached render. Everything is escaped or generated;
// there is no raw pass-through path.
func RenderHTML(doc *Node) string {
	var b strings.Builder
	renderNodes(&b, doc.Content)
	return b.String()
}

func renderNodes(b *strings.Builder, nodes []*Node) {
	for _, n := range nodes {
		renderNode(b, n)
	}
}

func renderNode(b *strings.Builder, n *Node) {
	switch n.Type {
	case NodeParagraph:
		b.WriteString("<p>")
		renderNodes(b, n.Content)
		b.WriteString("</p>")
	case NodeHeading:
		lvl := attrInt(n.Attrs, "level", 1)
		if lvl < 1 || lvl > 6 {
			lvl = 6
		}
		tag := "h" + strconv.Itoa(lvl)
		b.WriteString("<" + tag + ">")
		renderNodes(b, n.Content)
		b.WriteString("</" + tag + ">")
	case NodeBlockquote:
		b.WriteString("<blockquote>")
		renderNodes(b, n.Content)
		b.WriteString("</blockquote>")
	case NodeBulletList:
		b.WriteString("<ul>")
		renderNodes(b, n.Content)
		b.WriteString("</ul>")
	case NodeOrderedList:
		start := attrInt(n.Attrs, "start", 1)
		if start != 1 {
			b.WriteString(`<ol start="` + strconv.Itoa(start) + `">`)
		} else {
			b.WriteString("<ol>")
		}
		renderNodes(b, n.Content)
		b.WriteString("</ol>")
	case NodeListItem:
		b.WriteString("<li>")
		if checked, ok := n.Attrs["checked"].(bool); ok {
			if checked {
				b.WriteString(`<input type="checkbox" checked disabled> `)
			} else {
				b.WriteString(`<input type="checkbox" disabled> `)
			}
		}
		renderNodes(b, n.Content)
		b.WriteString("</li>")
	case NodeCodeBlock:
		lang := attrString(n.Attrs, "language")
		if lang != "" && isSafeToken(lang) {
			b.WriteString(`<pre><code class="language-` + html.EscapeString(lang) + `">`)
		} else {
			b.WriteString("<pre><code>")
		}
		b.WriteString(html.EscapeString(n.Text))
		b.WriteString("</code></pre>")
	case NodeRule:
		b.WriteString("<hr>")
	case NodeHardBreak:
		b.WriteString("<br>")
	case NodeTable:
		b.WriteString("<table>")
		renderNodes(b, n.Content)
		b.WriteString("</table>")
	case NodeTableRow:
		b.WriteString("<tr>")
		renderNodes(b, n.Content)
		b.WriteString("</tr>")
	case NodeTableCell:
		tag := "td"
		if h, _ := n.Attrs["header"].(bool); h {
			tag = "th"
		}
		b.WriteString("<" + tag + ">")
		renderNodes(b, n.Content)
		b.WriteString("</" + tag + ">")
	case NodeMention:
		label := html.EscapeString(attrString(n.Attrs, "label"))
		if id := attrInt64(n.Attrs, "user_id"); id != 0 {
			b.WriteString(`<span class="mention" data-user-id="` +
				strconv.FormatInt(id, 10) + `">@` + label + `</span>`)
		} else {
			b.WriteString(`<span class="mention mention-unresolved">@` + label + `</span>`)
		}
	case NodeEmoji:
		b.WriteString(`<span class="emoji" title=":` +
			html.EscapeString(attrString(n.Attrs, "shortcode")) + `:">` +
			html.EscapeString(n.Text) + `</span>`)
	case NodeText:
		renderText(b, n)
	}
}

func renderText(b *strings.Builder, n *Node) {
	open, close := markTags(n.Marks)
	b.WriteString(open)
	b.WriteString(html.EscapeString(n.Text))
	b.WriteString(close)
}

// markTags nests marks deterministically: link outermost, then code, strong,
// em, strike.
func markTags(marks []Mark) (string, string) {
	var open, close strings.Builder
	var closers []string
	push := func(o, c string) {
		open.WriteString(o)
		closers = append(closers, c)
	}
	for _, m := range marks {
		if m.Type == MarkLink {
			href := attrString(m.Attrs, "href")
			if safe := SafeURL(href); safe != "" {
				push(`<a href="`+html.EscapeString(safe)+`" rel="noopener noreferrer" target="_blank">`, "</a>")
			}
		}
	}
	for _, m := range marks {
		switch m.Type {
		case MarkCode:
			push("<code>", "</code>")
		case MarkStrong:
			push("<strong>", "</strong>")
		case MarkEm:
			push("<em>", "</em>")
		case MarkStrike:
			push("<del>", "</del>")
		}
	}
	for i := len(closers) - 1; i >= 0; i-- {
		close.WriteString(closers[i])
	}
	return open.String(), close.String()
}

// SafeURL allowlists link schemes; anything else renders as plain text.
func SafeURL(raw string) string {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return ""
	}
	switch strings.ToLower(u.Scheme) {
	case "http", "https", "mailto":
		return u.String()
	}
	return ""
}

func isSafeToken(s string) bool {
	for _, r := range s {
		if !(r == '-' || r == '+' || r == '#' ||
			(r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9')) {
			return false
		}
	}
	return len(s) > 0 && len(s) <= 40
}

func attrInt(a map[string]any, k string, def int) int {
	switch v := a[k].(type) {
	case int:
		return v
	case float64:
		return int(v)
	}
	return def
}

func attrInt64(a map[string]any, k string) int64 {
	switch v := a[k].(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case float64:
		return int64(v)
	}
	return 0
}

func attrString(a map[string]any, k string) string {
	s, _ := a[k].(string)
	return s
}

// MentionIDs extracts resolved mention user ids from a STORED ast (the
// message row's JSONB) — used to diff mentions across an edit without
// re-resolving names against a directory that may have changed.
func MentionIDs(ast []byte) []int64 {
	var doc map[string]any
	if json.Unmarshal(ast, &doc) != nil {
		return nil
	}
	var out []int64
	var walk func(n map[string]any)
	walk = func(n map[string]any) {
		if n["type"] == "mention" {
			if attrs, ok := n["attrs"].(map[string]any); ok {
				if id, ok := attrs["user_id"].(float64); ok {
					out = append(out, int64(id))
				}
			}
		}
		if kids, ok := n["content"].([]any); ok {
			for _, k := range kids {
				if m, ok := k.(map[string]any); ok {
					walk(m)
				}
			}
		}
	}
	walk(doc)
	return out
}

// Links returns every link destination in document order (message-write
// hooks scan these for attachment references).
func (n *Node) Links() []string {
	var out []string
	n.walk(func(node *Node) {
		for _, m := range node.Marks {
			if m.Type == MarkLink {
				if href := attrString(m.Attrs, "href"); href != "" {
					out = append(out, href)
				}
			}
		}
	})
	return out
}
