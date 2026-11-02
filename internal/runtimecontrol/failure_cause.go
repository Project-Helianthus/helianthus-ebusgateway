package runtimecontrol

import (
	"regexp"
	"strings"
)

// MaxFailureCauseChars bounds the cause text of a recovery view. The cause is
// printable ASCII, so characters and bytes coincide.
const MaxFailureCauseChars = 160

// failureCauseRedaction replaces every token of a cause text that has the shape
// of an address.
const failureCauseRedaction = "<redacted>"

// failureCauseTokenShapes are the shapes of a single whitespace-delimited token
// that name a network, hardware, file or account address: an IPv6 or hardware
// address group, a host:port pair, a URL scheme, an absolute or drive path, a
// user@host form and a dotted host name (an IPv4 address is one, so it has no
// shape of its own). A token with any of them is replaced as a whole, so no
// fragment of the address survives.
var failureCauseTokenShapes = []*regexp.Regexp{
	regexp.MustCompile(`::|(?:[0-9A-Fa-f]{1,4}:){2,}`),
	regexp.MustCompile(`\S:[0-9]{1,5}(?:[^0-9]|$)`),
	regexp.MustCompile(`[A-Za-z][A-Za-z0-9+.-]*://`),
	regexp.MustCompile(`(?:^|[\s(])/[A-Za-z0-9._-]|[A-Za-z]:\\|\\`),
	regexp.MustCompile(`\S@\S`),
	regexp.MustCompile(`[A-Za-z0-9-]\.[A-Za-z0-9-]`),
}

// failureCauseOperandPrefix finds the keyword whose operand names a host: the
// target of a dial, a lookup, a resolve or a connect. A single-label host name
// has no other tell, so it is recognized by the word before it.
var failureCauseOperandPrefix = regexp.MustCompile(`(?i)\b(?:dial|lookup|resolve|connect to)\s+`)

var failureCauseNetworkWord = regexp.MustCompile(`^(?i:tcp|udp)\s+`)

// sanitizeFailureCause returns the display text of a failure cause for the
// recovery view: printable ASCII on one line, every address replaced, at most
// MaxFailureCauseChars characters. The text it returns satisfies
// failureCauseAddressFree; when no redaction can make the bounded text safe it
// is empty, which the view admits.
func sanitizeFailureCause(text string) string {
	text = failureCauseASCII(text)
	for pass := 0; pass < 4; pass++ {
		text = redactFailureCauseTokens(text)
		text = redactFailureCauseOperands(text)
		if len(text) <= MaxFailureCauseChars {
			break
		}
		// A cut can create an address shape (a long port number cut to five
		// digits, for example), so the shortened text is redacted again.
		text = strings.TrimRight(text[:MaxFailureCauseChars], " ")
	}
	if len(text) > MaxFailureCauseChars || !failureCauseAddressFree(text) {
		return ""
	}
	return text
}

// failureCauseASCII reduces text to printable ASCII: controls and white space
// become a space, any other character becomes "?". The redaction pass that
// follows puts the words on one line.
func failureCauseASCII(text string) string {
	var b strings.Builder
	b.Grow(len(text))
	for _, r := range text {
		switch {
		case r >= ' ' && r <= '~':
			b.WriteRune(r)
		case r < ' ' || r == 0x7f || r == 0x85 || r == 0xa0 || r == 0x2028 || r == 0x2029 || r == 0x3000 || (r >= 0x2000 && r <= 0x200a):
			b.WriteByte(' ')
		default:
			b.WriteByte('?')
		}
	}
	return b.String()
}

// redactFailureCauseTokens puts the words of text on one line, separated by one
// space, and replaces every word that has the shape of an address.
func redactFailureCauseTokens(text string) string {
	fields := strings.Fields(text)
	for i, field := range fields {
		for _, shape := range failureCauseTokenShapes {
			if shape.MatchString(field) {
				fields[i] = failureCauseRedaction
				break
			}
		}
	}
	return strings.Join(fields, " ")
}

// redactFailureCauseOperands replaces the operand of a dial, lookup, resolve or
// connect that is a bare host label. The network word (tcp or udp) is not an
// operand, and an operand not followed by a colon or the end of the text is a
// word of the sentence, not a host.
func redactFailureCauseOperands(text string) string {
	var out strings.Builder
	last := 0
	for _, prefix := range failureCauseOperandPrefix.FindAllStringIndex(text, -1) {
		start := prefix[1]
		if start < last {
			continue
		}
		if network := failureCauseNetworkWord.FindStringIndex(text[start:]); network != nil {
			start += network[1]
		}
		end := start
		for end < len(text) && failureCauseLabelByte(text[end]) {
			end++
		}
		if end == start || (end < len(text) && text[end] != ':') {
			continue
		}
		if word := strings.ToLower(text[start:end]); word == "tcp" || word == "udp" {
			continue
		}
		out.WriteString(text[last:start])
		out.WriteString(failureCauseRedaction)
		last = end
	}
	out.WriteString(text[last:])
	return out.String()
}

func failureCauseLabelByte(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-'
}

// failureCauseAddressFree reports whether text has none of the shapes that the
// MCP contract refuses in a recovery cause: the token shapes anywhere in the
// text and the bare host label after dial, lookup, resolve or connect to.
func failureCauseAddressFree(text string) bool {
	for _, c := range []byte(text) {
		if c < ' ' || c > '~' {
			return false
		}
	}
	for _, shape := range failureCauseTokenShapes {
		if shape.MatchString(text) {
			return false
		}
	}
	for _, prefix := range failureCauseOperandPrefix.FindAllStringIndex(text, -1) {
		rest := text[prefix[1]:]
		if failureCauseOperandAt(rest) {
			return false
		}
		if network := failureCauseNetworkWord.FindStringIndex(rest); network != nil && failureCauseOperandAt(rest[network[1]:]) {
			return false
		}
	}
	return true
}

// failureCauseOperandAt reports whether rest starts with a host label followed
// by a colon or the end of the text, where the label is not the word tcp or udp.
func failureCauseOperandAt(rest string) bool {
	end := 0
	for end < len(rest) && failureCauseLabelByte(rest[end]) {
		end++
	}
	if end == 0 || (end < len(rest) && rest[end] != ':') {
		return false
	}
	return !failureCauseNetworkLabel(rest[:end])
}

func failureCauseNetworkLabel(label string) bool {
	word := strings.ToLower(label)
	return word == "tcp" || word == "udp"
}
