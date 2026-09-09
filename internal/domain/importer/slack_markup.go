package importer

import (
	"strings"
	"unicode"
)

// Slack's mrkdwn dialect, translated to the Weft markdown the IR carries.
// The dialect belongs to the LOADER (ADR/LLD: one owner), so nothing below
// this file's boundary ever learns what an angle-bracket token is.
//
// Grounded in zerver/data_import/slack_message_conversion.py, which is the
// format authority, with three deliberate divergences recorded at their sites:
// broadcast mentions stay inert instead of becoming a wildcard mention,
// `_italic_` is left alone, and the entity escapes Slack applies to every
// message are undone (the reference does not, and the corruption is visible).

// slackDialect resolves the ids inside a message's tokens against the export's
// own directories. Both maps are the EXPORT's, never the org's: a token names
// a Slack id, and whether Weft has a row for it is decided later, by the write
// path, through the by-source-id lanes this returns.
type slackDialect struct {
	userName    map[string]string // slack user id → display name
	channelName map[string]string // slack channel id → channel name
}

// convertBody turns one message's `text` into Weft markdown, plus the two
// by-source-id lanes the IR carries: label → source user id for mentions, and
// label → source channel id for channel references. A label the loader could
// not pair with a source entity is simply absent from its map, which is what
// makes the reference render unresolved instead of resolving onto whatever the
// target org happens to call things.
func (d slackDialect) convertBody(raw string) (body string, mentions, chanRefs map[string]string) {
	mentions = map[string]string{}
	chanRefs = map[string]string{}

	var out strings.Builder
	var plain strings.Builder
	// Emphasis is converted per PLAIN run, so a '*' or '~' inside a URL or a
	// resolved label is never rewritten.
	flush := func() {
		out.WriteString(convertSlackEmphasis(plain.String()))
		plain.Reset()
	}
	for i := 0; i < len(raw); {
		if raw[i] != '<' {
			plain.WriteByte(raw[i])
			i++
			continue
		}
		// A literal '<' in a Slack message arrives as `&lt;`, so an unescaped
		// one always opens a token — unless it never closes, or a newline
		// intervenes, in which case the export is not speaking the dialect and
		// the byte stays literal.
		end := strings.IndexByte(raw[i:], '>')
		if end < 0 || strings.ContainsRune(raw[i:i+end], '\n') {
			plain.WriteByte(raw[i])
			i++
			continue
		}
		flush()
		out.WriteString(d.token(raw[i+1:i+end], mentions, chanRefs))
		i += end + 1
	}
	flush()
	return unescapeSlackEntities(out.String()), mentions, chanRefs
}

// token renders ONE `<...>` entity (the angle brackets already stripped).
func (d slackDialect) token(tok string, mentions, chanRefs map[string]string) string {
	if tok == "" {
		return ""
	}
	target, label := tok, ""
	if pipe := strings.IndexByte(tok, '|'); pipe >= 0 {
		target, label = tok[:pipe], tok[pipe+1:]
	}
	switch target[0] {
	case '@':
		return d.userToken(target[1:], label, mentions)
	case '#':
		return d.channelToken(target[1:], label, chanRefs)
	case '!':
		return broadcastToken(target[1:], label)
	}
	return linkToken(target, label)
}

// userToken maps `<@U123>` / `<@U123|handle>` onto Weft's @**Name** mention.
// The label is the imported DISPLAY NAME, and the pairing that follows is what
// lets the write path resolve it by ID rather than by name — Slack's own
// short-name is not stable and two people can share a display name.
//
// A sender the export has no `users.json` row for cannot be a mention: it
// becomes inert text, because inventing an unresolved mention would render a
// person-shaped span for someone who is not in the org.
func (d slackDialect) userToken(id, label string, mentions map[string]string) string {
	name := d.userName[id]
	if name == "" || !safeInlineLabel(name) {
		if label == "" {
			label = id
		}
		return "@" + label
	}
	mentions[name] = id
	return "@**" + name + "**"
}

// channelToken maps `<#C123|name>` — and the pipe-less `<#C123>`, which the
// reference implementation ignores — onto Weft's #**name** channel reference.
// A channel absent from channels.json/groups.json keeps its label and lands
// UNRESOLVED, which still renders: an inert reference is information, and
// dropping it is not.
func (d slackDialect) channelToken(id, label string, chanRefs map[string]string) string {
	name := d.channelName[id]
	if name != "" {
		label = name
	}
	if label == "" {
		label = id
	}
	if !safeInlineLabel(label) {
		return "#" + label
	}
	if name != "" {
		chanRefs[label] = id
	}
	return "#**" + label + "**"
}

// broadcastToken renders `<!channel>`, `<!here>`, `<!everyone>` and the
// user-group form `<!subteam^S123|@eng>` as INERT TEXT.
//
// Weft has no broadcast mention and P-44 bans one structurally, so there is
// nothing to resolve to; the reference implementation maps all three onto
// Zulip's @**all** wildcard, which Weft cannot and will not honour. Leaving
// the raw token alone is NOT the same answer: `<!channel>` renders as visible
// corruption, and this renders as the "@channel" the author actually typed.
func broadcastToken(target, label string) string {
	name, arg, _ := strings.Cut(target, "^")
	switch name {
	case "channel", "here", "everyone", "subteam":
		// Exactly one '@', whichever of the three sources named it.
		for _, s := range []string{strings.TrimPrefix(label, "@"), arg, name} {
			if s != "" {
				return "@" + s
			}
		}
		return "@" + name
	}
	// Everything else in the `<!…>` family is a SPECIAL, not a mention:
	// `<!date^1554100000^{date_short}|Apr 1, 2019>` is a formatted date, and
	// its fallback label IS what the reader saw. With no fallback there is
	// nothing to show, and the raw machinery is not it.
	return label
}

// linkToken maps `<url>` → url and `<url|label>` → [label](url). A mailto
// keeps only its address: the reference does the same, because Slack's own
// label for one is the address again.
func linkToken(target, label string) string {
	if strings.HasPrefix(strings.ToLower(target), "mailto:") {
		return target
	}
	if label == "" || label == target || !safeLinkLabel(label) {
		return target
	}
	return "[" + label + "](" + target + ")"
}

// safeInlineLabel keeps a label out of the @**…** / #**…** delimiters when it
// would break out of them. The parser bails on a label over 100 bytes too, so
// the check mirrors it rather than emitting a token that silently degrades.
func safeInlineLabel(s string) bool {
	return s != "" && len(s) <= 100 && !strings.ContainsAny(s, "*\n")
}

func safeLinkLabel(s string) bool {
	return !strings.ContainsAny(s, "[]()\n")
}

// unescapeSlackEntities undoes the three escapes Slack applies to EVERY
// message body (`&` `<` `>`, documented in its message-formatting reference).
//
// DELIBERATE DIVERGENCE from the reference implementation, which does not undo
// them: leaving them is not neutral, it renders "R&amp;D" to every reader
// forever. It runs LAST, after tokenising, so an escaped `&lt;` can never be
// mistaken for the start of a token, and `&amp;` is undone last so that
// `&amp;lt;` yields the literal `&lt;` the author wrote. Nothing unsafe can
// come of it: the AST engine escapes every text byte on render and treats raw
// HTML as literal text.
func unescapeSlackEntities(s string) string {
	s = strings.ReplaceAll(s, "&lt;", "<")
	s = strings.ReplaceAll(s, "&gt;", ">")
	return strings.ReplaceAll(s, "&amp;", "&")
}

// convertSlackEmphasis maps Slack's single-delimiter emphasis onto CommonMark.
//
//   - `*bold*` → `**bold**`: CommonMark reads a single '*' as EMPHASIS, so
//     leaving it turns every bold word italic.
//   - `~strike~` → `~~strike~~`: GFM strikethrough needs the doubled form, so
//     leaving it renders the tildes literally.
//   - `_italic_` is LEFT ALONE, which is a divergence from the reference's
//     `_x_` → `*x*`. CommonMark already reads `_x_` as emphasis and, like
//     Slack, refuses to do it intra-word; rewriting it to '*' would make it
//     MORE greedy than the source, which is a fidelity loss rather than a fix.
func convertSlackEmphasis(s string) string {
	return mapOutsideCode(s, func(seg string) string {
		seg = convertDelimiter(seg, '*', "**")
		return convertDelimiter(seg, '~', "~~")
	})
}

// convertDelimiter doubles one emphasis delimiter, using the same boundary
// rule the reference's regexes encode: the delimiter must sit against
// punctuation, whitespace, a symbol or the edge of the text, and never against
// '`', '@', '\' or a quote/bracket facing the wrong way. Slack has no
// intra-word formatting, and this is what keeps `a*b*c` and `2 * 3 * 4` alone.
func convertDelimiter(s string, delim rune, doubled string) string {
	src := []rune(s)
	var b strings.Builder
	for i := 0; i < len(src); {
		if src[i] != delim || !opensEmphasis(src, i, delim) {
			b.WriteRune(src[i])
			i++
			continue
		}
		j := i + 1
		for j < len(src) && src[j] != delim {
			j++
		}
		if j >= len(src) || j == i+1 || !closesEmphasis(src, j, delim) {
			b.WriteRune(src[i])
			i++
			continue
		}
		b.WriteString(doubled)
		b.WriteString(string(src[i+1 : j]))
		b.WriteString(doubled)
		i = j + 1
	}
	return b.String()
}

func opensEmphasis(src []rune, i int, delim rune) bool {
	if i == 0 {
		return true
	}
	p := src[i-1]
	if p == delim || p == '`' || p == '@' || p == '\\' ||
		unicode.In(p, unicode.Pf, unicode.Pe) {
		return false
	}
	return isBoundaryRune(p)
}

func closesEmphasis(src []rune, j int, delim rune) bool {
	if j+1 >= len(src) {
		return true
	}
	n := src[j+1]
	if n == delim || n == '`' || n == '@' || n == '\\' ||
		unicode.In(n, unicode.Pi, unicode.Ps) {
		return false
	}
	return isBoundaryRune(n)
}

func isBoundaryRune(r rune) bool {
	return unicode.IsPunct(r) || unicode.IsSpace(r) || unicode.IsSymbol(r)
}

// mapOutsideCode applies fn to every run of s that is NOT inside a backtick
// span, so emphasis conversion never rewrites the inside of `a*b*c` or a
// fenced block into something the reader sees as literal asterisks. A run of
// n backticks opens a span the next run of n closes; an unmatched run is
// literal, and scanning simply continues past it.
func mapOutsideCode(s string, fn func(string) string) string {
	var b strings.Builder
	for i := 0; i < len(s); {
		if s[i] != '`' {
			j := i
			for j < len(s) && s[j] != '`' {
				j++
			}
			b.WriteString(fn(s[i:j]))
			i = j
			continue
		}
		n := 0
		for i+n < len(s) && s[i+n] == '`' {
			n++
		}
		run := s[i : i+n]
		rest := s[i+n:]
		k := strings.Index(rest, run)
		if k < 0 {
			b.WriteString(run)
			i += n
			continue
		}
		b.WriteString(run)
		b.WriteString(rest[:k])
		b.WriteString(run)
		i += n + k + n
	}
	return b.String()
}
