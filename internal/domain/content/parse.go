package content

import (
	"bytes"

	"github.com/yuin/goldmark"
	gast "github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/extension"
	east "github.com/yuin/goldmark/extension/ast"
	"github.com/yuin/goldmark/parser"
	"github.com/yuin/goldmark/text"
	"github.com/yuin/goldmark/util"
)

// MentionResolver maps a mention label ("Full Name") to a user id; ok=false
// leaves the mention unresolved (rendered as inert text-like span).
type MentionResolver func(label string) (int64, bool)

// Parse converts markdown source to the portable AST. Chat semantics: soft
// line breaks are hard breaks (Slack/Zulip convention — a newline is a
// newline). GFM tables, strikethrough, task lists, and autolinks are on.
// Raw HTML in source is treated as literal text (never parsed, never
// emitted).
func Parse(source string, resolve MentionResolver) *Node {
	md := goldmark.New(
		goldmark.WithExtensions(extension.Table, extension.Strikethrough,
			extension.TaskList, extension.Linkify),
		goldmark.WithParserOptions(
			parser.WithInlineParsers(
				util.Prioritized(&mentionParser{}, 150),
				util.Prioritized(&emojiParser{}, 160),
			),
		),
	)
	src := []byte(source)
	root := md.Parser().Parse(text.NewReader(src))
	c := &converter{src: src, resolve: resolve}
	doc := &Node{Type: NodeDoc}
	for child := root.FirstChild(); child != nil; child = child.NextSibling() {
		if n := c.block(child); n != nil {
			doc.Content = append(doc.Content, n)
		}
	}
	if len(doc.Content) == 0 {
		doc.Content = []*Node{{Type: NodeParagraph}}
	}
	return doc
}

type converter struct {
	src     []byte
	resolve MentionResolver
}

func (c *converter) block(n gast.Node) *Node {
	switch v := n.(type) {
	case *gast.Paragraph, *gast.TextBlock:
		p := &Node{Type: NodeParagraph}
		c.inlines(p, n, nil)
		return p
	case *gast.Heading:
		h := &Node{Type: NodeHeading, Attrs: map[string]any{"level": v.Level}}
		c.inlines(h, n, nil)
		return h
	case *gast.Blockquote:
		b := &Node{Type: NodeBlockquote}
		for ch := n.FirstChild(); ch != nil; ch = ch.NextSibling() {
			if bn := c.block(ch); bn != nil {
				b.Content = append(b.Content, bn)
			}
		}
		return b
	case *gast.List:
		t := NodeBulletList
		var attrs map[string]any
		if v.IsOrdered() {
			t = NodeOrderedList
			attrs = map[string]any{"start": v.Start}
		}
		l := &Node{Type: t, Attrs: attrs}
		for ch := n.FirstChild(); ch != nil; ch = ch.NextSibling() {
			if item := c.listItem(ch); item != nil {
				l.Content = append(l.Content, item)
			}
		}
		return l
	case *gast.FencedCodeBlock:
		return &Node{Type: NodeCodeBlock,
			Attrs: map[string]any{"language": string(v.Language(c.src))},
			Text:  c.linesText(n)}
	case *gast.CodeBlock:
		return &Node{Type: NodeCodeBlock, Text: c.linesText(n)}
	case *gast.ThematicBreak:
		return &Node{Type: NodeRule}
	case *gast.HTMLBlock:
		// Raw HTML is literal text — never markup (XSS by construction).
		return &Node{Type: NodeParagraph, Content: []*Node{
			{Type: NodeText, Text: c.linesText(n)}}}
	case *east.Table:
		return c.table(v)
	}
	return nil
}

func (c *converter) listItem(n gast.Node) *Node {
	item := &Node{Type: NodeListItem}
	for ch := n.FirstChild(); ch != nil; ch = ch.NextSibling() {
		if bn := c.block(ch); bn != nil {
			item.Content = append(item.Content, bn)
		}
	}
	// GFM task list: the checkbox is the first inline of the first block.
	if fc := n.FirstChild(); fc != nil {
		if box, ok := fc.FirstChild().(*east.TaskCheckBox); ok {
			item.Attrs = map[string]any{"checked": box.IsChecked}
		}
	}
	return item
}

func (c *converter) table(t *east.Table) *Node {
	tbl := &Node{Type: NodeTable}
	for section := t.FirstChild(); section != nil; section = section.NextSibling() {
		switch sec := section.(type) {
		case *east.TableHeader:
			tbl.Content = append(tbl.Content, c.tableRow(sec, true))
		case *east.TableRow:
			tbl.Content = append(tbl.Content, c.tableRow(sec, false))
		}
	}
	return tbl
}

func (c *converter) tableRow(row gast.Node, header bool) *Node {
	r := &Node{Type: NodeTableRow}
	for cell := row.FirstChild(); cell != nil; cell = cell.NextSibling() {
		cn := &Node{Type: NodeTableCell}
		if header {
			cn.Attrs = map[string]any{"header": true}
		}
		c.inlines(cn, cell, nil)
		r.Content = append(r.Content, cn)
	}
	return r
}

// inlines flattens goldmark's inline tree into text nodes with mark stacks,
// plus mention/emoji/hard-break nodes.
func (c *converter) inlines(dst *Node, parent gast.Node, marks []Mark) {
	for n := parent.FirstChild(); n != nil; n = n.NextSibling() {
		switch v := n.(type) {
		case *gast.Text:
			seg := v.Segment
			txt := string(seg.Value(c.src))
			if txt != "" {
				dst.Content = append(dst.Content, &Node{Type: NodeText, Text: txt, Marks: marks})
			}
			// Chat semantics: every line break is a hard break.
			if v.HardLineBreak() || v.SoftLineBreak() {
				dst.Content = append(dst.Content, &Node{Type: NodeHardBreak})
			}
		case *gast.String:
			dst.Content = append(dst.Content, &Node{Type: NodeText, Text: string(v.Value), Marks: marks})
		case *gast.CodeSpan:
			var buf bytes.Buffer
			for t := n.FirstChild(); t != nil; t = t.NextSibling() {
				if tt, ok := t.(*gast.Text); ok {
					buf.Write(tt.Segment.Value(c.src))
				}
			}
			dst.Content = append(dst.Content,
				&Node{Type: NodeText, Text: buf.String(), Marks: appendMark(marks, Mark{Type: MarkCode})})
		case *gast.Emphasis:
			m := Mark{Type: MarkEm}
			if v.Level == 2 {
				m = Mark{Type: MarkStrong}
			}
			c.inlines(dst, n, appendMark(marks, m))
		case *east.Strikethrough:
			c.inlines(dst, n, appendMark(marks, Mark{Type: MarkStrike}))
		case *gast.Link:
			c.inlines(dst, n, appendMark(marks, Mark{Type: MarkLink,
				Attrs: map[string]any{"href": string(v.Destination)}}))
		case *gast.AutoLink:
			u := string(v.URL(c.src))
			label := string(v.Label(c.src))
			dst.Content = append(dst.Content, &Node{Type: NodeText, Text: label,
				Marks: appendMark(marks, Mark{Type: MarkLink, Attrs: map[string]any{"href": u}})})
		case *gast.Image:
			// Markdown images render as links in chat (files travel via the
			// attachment pipeline, ADR-012).
			alt := string(v.Text(c.src))
			if alt == "" {
				alt = string(v.Destination)
			}
			dst.Content = append(dst.Content, &Node{Type: NodeText, Text: alt,
				Marks: appendMark(marks, Mark{Type: MarkLink, Attrs: map[string]any{"href": string(v.Destination)}})})
		case *gast.RawHTML:
			// Inline raw HTML → literal text.
			var buf bytes.Buffer
			for i := 0; i < v.Segments.Len(); i++ {
				seg := v.Segments.At(i)
				buf.Write(seg.Value(c.src))
			}
			dst.Content = append(dst.Content, &Node{Type: NodeText, Text: buf.String(), Marks: marks})
		case *mentionNode:
			attrs := map[string]any{"label": v.label}
			if c.resolve != nil {
				if id, ok := c.resolve(v.label); ok {
					attrs["user_id"] = id
				}
			}
			dst.Content = append(dst.Content, &Node{Type: NodeMention, Attrs: attrs})
		case *emojiNode:
			dst.Content = append(dst.Content, &Node{Type: NodeEmoji, Text: v.unicode,
				Attrs: map[string]any{"shortcode": v.shortcode}})
		case *east.TaskCheckBox:
			// handled at list-item level
		default:
			c.inlines(dst, n, marks)
		}
	}
}

func appendMark(marks []Mark, m Mark) []Mark {
	out := make([]Mark, 0, len(marks)+1)
	out = append(out, marks...)
	return append(out, m)
}

func (c *converter) linesText(n gast.Node) string {
	var buf bytes.Buffer
	lines := n.Lines()
	for i := 0; i < lines.Len(); i++ {
		seg := lines.At(i)
		buf.Write(seg.Value(c.src))
	}
	return buf.String()
}

// ---- Weft inline syntax: @**Full Name** mentions ----

type mentionNode struct {
	gast.BaseInline
	label string
}

var kindMention = gast.NewNodeKind("ContentMention")

func (n *mentionNode) Kind() gast.NodeKind { return kindMention }
func (n *mentionNode) Dump(src []byte, level int) {
	gast.DumpHelper(n, src, level, map[string]string{"label": n.label}, nil)
}

type mentionParser struct{}

func (p *mentionParser) Trigger() []byte { return []byte{'@'} }

func (p *mentionParser) Parse(parent gast.Node, block text.Reader, pc parser.Context) gast.Node {
	line, _ := block.PeekLine()
	if !bytes.HasPrefix(line, []byte("@**")) {
		return nil
	}
	rest := line[3:]
	end := bytes.Index(rest, []byte("**"))
	if end < 1 || end > 100 {
		return nil
	}
	label := string(rest[:end])
	block.Advance(3 + end + 2)
	return &mentionNode{label: label}
}

// ---- :shortcode: emoji ----

type emojiNode struct {
	gast.BaseInline
	shortcode string
	unicode   string
}

var kindEmoji = gast.NewNodeKind("ContentEmoji")

func (n *emojiNode) Kind() gast.NodeKind { return kindEmoji }
func (n *emojiNode) Dump(src []byte, level int) {
	gast.DumpHelper(n, src, level, map[string]string{"shortcode": n.shortcode}, nil)
}

type emojiParser struct{}

func (p *emojiParser) Trigger() []byte { return []byte{':'} }

func (p *emojiParser) Parse(parent gast.Node, block text.Reader, pc parser.Context) gast.Node {
	line, _ := block.PeekLine()
	if len(line) < 3 || line[0] != ':' {
		return nil
	}
	end := bytes.IndexByte(line[1:], ':')
	if end < 1 || end > 60 {
		return nil
	}
	code := string(line[1 : 1+end])
	for _, r := range code {
		if !(r == '_' || r == '+' || r == '-' ||
			(r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')) {
			return nil
		}
	}
	uni, ok := emojiMap[code]
	if !ok {
		return nil
	}
	block.Advance(end + 2)
	return &emojiNode{shortcode: code, unicode: uni}
}

// emojiMap is the starter set; the full shortcode table is a data file that
// lands with the emoji picker (REALITY.md tracks the gap).
var emojiMap = map[string]string{
	"smile": "😄", "grin": "😁", "joy": "😂", "wink": "😉", "heart": "❤️",
	"+1": "👍", "-1": "👎", "tada": "🎉", "rocket": "🚀", "fire": "🔥",
	"eyes": "👀", "thinking": "🤔", "check": "✅", "x": "❌", "wave": "👋",
	"clap": "👏", "pray": "🙏", "bug": "🐛", "sparkles": "✨", "warning": "⚠️",
}
