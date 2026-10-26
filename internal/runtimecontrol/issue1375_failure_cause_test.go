package runtimecontrol

// Issue #1375: the cause text of a recovery view is display text that the MCP
// contract bounds and keeps free of addresses. The shapes below are the ones
// the contract's checker refuses in last_failure_cause: an IPv4 address, an
// IPv6 or hardware address group, a host:port pair, a URL scheme, an absolute
// or drive path, a user@host form, a dotted host name, and the single-label
// host name after dial, lookup, resolve or connect to. The oracle is the
// contractcheck rendering of the checker, written apart from the redaction
// and compared with the checker itself, so the production redaction is never
// checked against its own code.

import (
	"math/rand"
	"strings"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol/contractcheck"
)

// contractRejectsCause is the contract's failure_cause rule.
func contractRejectsCause(s string) bool { return !contractcheck.CauseValid(s) }

// issue1375AddressCauses are the contract's own rejected examples.
var issue1375AddressCauses = []struct{ raw, want string }{
	{"dial tcp 192.0.2.10: connection refused", "dial tcp <redacted> connection refused"},
	{"dial tcp 192.0.2.10:9000: connection refused", "dial tcp <redacted> connection refused"},
	{"refused by adapter:9000", "refused by <redacted>"},
	{"dial tcp [2001:db8::1]: connection refused", "dial tcp <redacted> connection refused"},
	{"adapter aa:bb:cc:dd:ee:ff did not answer", "adapter <redacted> did not answer"},
	{"dial tcp://adapter: connection refused", "dial <redacted> connection refused"},
	{"dial example.invalid: no such host", "dial <redacted> no such host"},
	{"login as user@adapter refused", "login as <redacted> refused"},
	{"open /dev/ttyUSB0: no such device", "open <redacted> no such device"},
	{"open C:\\com3: access denied", "open <redacted> access denied"},
	{"lookup gateway: no such host", "lookup <redacted>: no such host"},
	{"dial ebus-adapter: connection refused", "dial <redacted>: connection refused"},
	{"dial tcp ebus-adapter: connection refused", "dial tcp <redacted>: connection refused"},
	// The other keywords and a label that ends the text.
	{"resolve gateway: no such host", "resolve <redacted>: no such host"},
	{"connect to gateway: refused", "connect to <redacted>: refused"},
	{"giving up on lookup gateway", "giving up on lookup <redacted>"},
	// A bare address of each shape, with surrounding punctuation.
	{"peer (192.0.2.10) reset", "peer <redacted> reset"},
	{"peer 2001:db8::1 reset", "peer <redacted> reset"},
	{"peer fe80::1%eth0 reset", "peer <redacted> reset"},
	{"error at host.example", "error at <redacted>"},
	{"serial ttyUSB0 at /dev/ttyUSB0", "serial ttyUSB0 at <redacted>"},
	{`drive C:\temp lost`, "drive <redacted> lost"},
	{"proxy http://gateway/path unreachable", "proxy <redacted> unreachable"},
}

// issue1375AcceptedCauses are causes the contract accepts and the redaction
// must leave alone.
var issue1375AcceptedCauses = []string{
	"",
	"dial: connection refused",
	"connect: connection refused",
	"lookup: no such host",
	"i/o timeout",
	"read: connection reset by peer",
	"write: broken pipe",
	"EOF",
	"ebus: payload does not match expected schema",
	"ebus: no response within timeout window",
	"connection lost (adapter_connection_lost)",
	"dial tcp: connection refused",
	"dial tcp udp: odd but not an address",
	"redial the adapter later",
	"connected to the adapter",
	// A keyword followed by a plain word of the sentence is not a host.
	"dial attempt after timeout",
	"lookup failed to complete",
	"resolve the name later",
	"connect to the adapter failed",
	"features 0x00 want 0x01",
	"dial tcp <redacted> connection refused",
	strings.Repeat("x", 160),
}

func TestIssue1375FailureCauseRemovesEveryContractAddressShape(t *testing.T) {
	for _, tc := range issue1375AddressCauses {
		if !contractRejectsCause(tc.raw) {
			t.Fatalf("oracle accepts %q; the fixture is not an address shape", tc.raw)
		}
		got := sanitizeFailureCause(tc.raw)
		if got != tc.want {
			t.Errorf("sanitize(%q) = %q; want %q", tc.raw, got, tc.want)
		}
		if contractRejectsCause(got) {
			t.Errorf("sanitize(%q) = %q is refused by the contract", tc.raw, got)
		}
		if again := sanitizeFailureCause(got); again != got {
			t.Errorf("sanitize is not idempotent: %q -> %q -> %q", tc.raw, got, again)
		}
	}
}

// The contract's keywords are lower case; the redaction does not depend on case.
func TestIssue1375FailureCauseRedactsKeywordOperandsInAnyCase(t *testing.T) {
	for raw, want := range map[string]string{
		"Dial UDP Gateway: closed":     "Dial UDP <redacted>: closed",
		"LOOKUP Gateway: no such host": "LOOKUP <redacted>: no such host",
		"Connect To gateway: refused":  "Connect To <redacted>: refused",
	} {
		if got := sanitizeFailureCause(raw); got != want {
			t.Errorf("sanitize(%q) = %q; want %q", raw, got, want)
		}
	}
}

func TestIssue1375FailureCauseKeepsTextTheContractAccepts(t *testing.T) {
	for _, text := range issue1375AcceptedCauses {
		if contractRejectsCause(text) {
			t.Fatalf("oracle refuses %q; the fixture is not an accepted cause", text)
		}
		if got := sanitizeFailureCause(text); got != text {
			t.Errorf("sanitize(%q) = %q; want it unchanged", text, got)
		}
	}
}

func TestIssue1375FailureCauseIsOneBoundedPrintableAsciiLine(t *testing.T) {
	for name, tc := range map[string]struct{ raw, want string }{
		"newline and tab":      {"line one\nline two\tend", "line one line two end"},
		"carriage return":      {"a\r\nb", "a b"},
		"leading and trailing": {"  padded  ", "padded"},
		"runs of space":        {"a    b", "a b"},
		"NUL and DEL":          {"a\x00b\x7fc", "a b c"},
		"non-ASCII letter":     {"caf\u00e9 closed", "caf? closed"},
		"non-ASCII space":      {"a\u00a0b\u2028c", "a b c"},
		"emoji":                {"closed \U0001F50C now", "closed ? now"},
		"length 160":           {strings.Repeat("x", 160), strings.Repeat("x", 160)},
		"length 161":           {strings.Repeat("x", 161), strings.Repeat("x", 160)},
		"length 1000":          {strings.Repeat("ab ", 400), strings.TrimRight(strings.Repeat("ab ", 400)[:160], " ")},
	} {
		got := sanitizeFailureCause(tc.raw)
		if got != tc.want {
			t.Errorf("%s: sanitize = %q; want %q", name, got, tc.want)
		}
		if len(got) > MaxFailureCauseChars || contractRejectsCause(got) {
			t.Errorf("%s: %q is longer than %d or refused by the contract", name, got, MaxFailureCauseChars)
		}
	}
	if MaxFailureCauseChars != 160 {
		t.Fatalf("MaxFailureCauseChars = %d; the contract bounds the cause at 160", MaxFailureCauseChars)
	}
}

// A cut at 160 characters can leave the first digits of a longer port number,
// which is a host:port shape the uncut text did not have.
func TestIssue1375FailureCauseCutNeverLeavesAnAddressShape(t *testing.T) {
	for digits := 6; digits <= 9; digits++ {
		for pad := 140; pad <= 156; pad++ {
			raw := strings.Repeat("a", pad) + " host:" + strings.Repeat("7", digits)
			got := sanitizeFailureCause(raw)
			if contractRejectsCause(got) {
				t.Fatalf("pad %d, %d digits: %q is refused by the contract", pad, digits, got)
			}
			if len(got) > MaxFailureCauseChars {
				t.Fatalf("pad %d, %d digits: %d characters", pad, digits, len(got))
			}
		}
	}
	// The uncut text has no shape; the cut one does, and is redacted.
	raw := strings.Repeat("a", 150) + " host:12345678"
	if contractcheck.CauseValid(raw[:MaxFailureCauseChars]) || !contractcheck.CauseValid(raw[:MaxFailureCauseChars-5]) {
		t.Fatal("fixture drifted: the text cut at 160 characters must be refused, and the same text cut earlier accepted")
	}
	// The redaction is longer than the port it replaces, so the bounded text
	// ends inside it.
	if got := sanitizeFailureCause(raw); got != strings.Repeat("a", 150)+" <redacted" {
		t.Fatalf("sanitize = %q; want the cut port redacted within 160 characters", got)
	}
}

// Whatever the input, the result is within the contract, and sanitizing it
// again changes nothing.
func TestIssue1375FailureCauseInvariantOverGeneratedText(t *testing.T) {
	pieces := []string{
		"dial", "tcp", "udp", "lookup", "resolve", "connect to", "connect", "to", " ", " ", " ", ":", "::", "/", "//", "\\", "@", ".", "-", "_",
		"[", "]", "(", ")", "0", "1", "255", "12345", "123456", "a", "ab", "host", "gateway", "example", "ttyUSB0", "aa:bb", "fe80", "2001:db8",
		"://", "http", "\n", "\t", "\u00e9", "\u2028", "refused", "timeout", "<redacted>", "%", "=", ",", ";",
	}
	random := rand.New(rand.NewSource(1375))
	for i := 0; i < 20000; i++ {
		var b strings.Builder
		for n := 1 + random.Intn(40); n > 0; n-- {
			b.WriteString(pieces[random.Intn(len(pieces))])
			if random.Intn(3) == 0 {
				b.WriteByte(' ')
			}
		}
		raw := b.String()
		got := sanitizeFailureCause(raw)
		if contractRejectsCause(got) {
			t.Fatalf("sanitize(%q) = %q is refused by the contract", raw, got)
		}
		if again := sanitizeFailureCause(got); again != got {
			t.Fatalf("sanitize(%q) is not idempotent: %q then %q", raw, got, again)
		}
	}
}

// The check that backs the redaction never accepts text the contract refuses
// (it may refuse more: it treats the keywords in any case).
func TestIssue1375FailureCauseGuardRefusesWhatTheContractRefuses(t *testing.T) {
	var texts []string
	for _, tc := range issue1375AddressCauses {
		texts = append(texts, tc.raw)
	}
	texts = append(texts, issue1375AcceptedCauses...)
	random := rand.New(rand.NewSource(13751))
	pieces := []string{"dial", "tcp", "udp", "lookup", "resolve", "connect to", " ", ":", "::", "/", "\\", "@", ".", "-", "0", "1", "12345", "123456", "a", "host", "aa:bb", "://", "\t", "\u00e9"}
	for i := 0; i < 20000; i++ {
		var b strings.Builder
		for n := 1 + random.Intn(20); n > 0; n-- {
			b.WriteString(pieces[random.Intn(len(pieces))])
		}
		texts = append(texts, b.String())
	}
	for _, text := range texts {
		if len(text) <= MaxFailureCauseChars && contractRejectsCause(text) && failureCauseAddressFree(text) {
			t.Fatalf("the guard accepts %q, which the contract refuses", text)
		}
	}
	for _, tc := range issue1375AddressCauses {
		if failureCauseAddressFree(tc.raw) {
			t.Errorf("the guard accepts the address shape %q", tc.raw)
		}
		if !failureCauseAddressFree(tc.want) {
			t.Errorf("the guard refuses the redacted text %q", tc.want)
		}
	}
}

func FuzzIssue1375FailureCause(f *testing.F) {
	for _, tc := range issue1375AddressCauses {
		f.Add(tc.raw)
	}
	for _, text := range issue1375AcceptedCauses {
		f.Add(text)
	}
	f.Add(strings.Repeat("a", 150) + " host:12345678")
	f.Add("dial tcp tcp: x\x00\xff")
	f.Fuzz(func(t *testing.T, raw string) {
		got := sanitizeFailureCause(raw)
		if contractRejectsCause(got) {
			t.Fatalf("sanitize(%q) = %q is refused by the contract", raw, got)
		}
		if again := sanitizeFailureCause(got); again != got {
			t.Fatalf("sanitize(%q) is not idempotent: %q then %q", raw, got, again)
		}
	})
}
