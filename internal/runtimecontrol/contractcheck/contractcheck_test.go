package contractcheck

// The vectors below are the accept and reject cases of the public contract
// checker's own tests for the recovery member, carried over so that this
// rendering of the rule gives the checker's answer on every one of them.

import (
	"encoding/json"
	"strings"
	"testing"
)

type recoveryLiterals struct{ attempt, stamp, class, cause string }

func goodRecovery() recoveryLiterals {
	return recoveryLiterals{`3`, `"2000-01-01T00:00:08Z"`, `"DIAL_FAILED"`, `"dial: connection refused"`}
}

func (r recoveryLiterals) recovery() string {
	return `{"attempt":` + r.attempt + `,"next_attempt_at":` + r.stamp + `,"last_failure_class":` + r.class + `,"last_failure_cause":` + r.cause + `}`
}

func driverText(state, recovery string) string {
	text := `{"driver_id":"ebus.primary","enabled":true,"desired_state":"RUNNING","desired_revision":"7","observed_state":"` + state +
		`","reason":"RETRY_SCHEDULED","safety_quarantined":false,"revision":"29"`
	if recovery != "" {
		text += `,"recovery":` + recovery
	}
	return text + `}`
}

func valid(t *testing.T, text string) bool {
	t.Helper()
	value, err := Decode([]byte(text))
	if err != nil {
		return false
	}
	return Driver(value)
}

func quote(t *testing.T, v any) string {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func TestDriverObjectAcceptsBackoffWithAndWithoutTheMember(t *testing.T) {
	if !valid(t, driverText("BACKOFF", goodRecovery().recovery())) || !valid(t, driverText("BACKOFF", "")) {
		t.Fatal("a BACKOFF driver with or without recovery is refused")
	}
}

func TestDriverObjectRefusesTheMemberOutsideBackoff(t *testing.T) {
	for _, state := range []string{"DISABLED", "STOPPED", "STARTING", "RUNNING", "DEGRADED", "STOPPING", "FAILED"} {
		if valid(t, driverText(state, goodRecovery().recovery())) {
			t.Errorf("recovery accepted in %s", state)
		}
		if !valid(t, driverText(state, "")) {
			t.Errorf("%s without recovery refused", state)
		}
	}
	for _, state := range []string{"RETRY_SCHEDULED", "backoff", "", "null", "[\"BACKOFF\"]"} {
		text := strings.Replace(driverText("BACKOFF", goodRecovery().recovery()), `"observed_state":"BACKOFF"`, `"observed_state":`+quoteState(state), 1)
		if valid(t, text) {
			t.Errorf("observed state %s admitted the member", state)
		}
	}
}

func quoteState(s string) string {
	if s == "null" || strings.HasPrefix(s, "[") {
		return s
	}
	return `"` + s + `"`
}

func TestDriverObjectIsClosed(t *testing.T) {
	for _, member := range []string{"extra", "recovery_id", "attempts", "Recovery"} {
		top := strings.TrimSuffix(driverText("BACKOFF", goodRecovery().recovery()), "}") + `,"` + member + `":1}`
		if valid(t, top) {
			t.Errorf("driver member %q accepted", member)
		}
		inner := strings.Replace(driverText("BACKOFF", goodRecovery().recovery()), `{"attempt"`, `{"`+member+`":1,"attempt"`, 1)
		if valid(t, inner) {
			t.Errorf("recovery member %q accepted", member)
		}
	}
	for _, value := range []string{"null", "{}", "[]", `""`, "0", "false", "[" + goodRecovery().recovery() + "]"} {
		if valid(t, driverText("BACKOFF", value)) {
			t.Errorf("recovery %s accepted", value)
		}
	}
	for _, member := range driverMembers {
		text := driverText("BACKOFF", goodRecovery().recovery())
		var object map[string]json.RawMessage
		if err := json.Unmarshal([]byte(text), &object); err != nil {
			t.Fatal(err)
		}
		delete(object, member)
		raw, _ := json.Marshal(object)
		if valid(t, string(raw)) {
			t.Errorf("driver without %q accepted", member)
		}
	}
	for _, member := range []string{"attempt", "next_attempt_at", "last_failure_class", "last_failure_cause"} {
		var object map[string]json.RawMessage
		if err := json.Unmarshal([]byte(goodRecovery().recovery()), &object); err != nil {
			t.Fatal(err)
		}
		delete(object, member)
		raw, _ := json.Marshal(object)
		if valid(t, driverText("BACKOFF", string(raw))) {
			t.Errorf("recovery without %q accepted", member)
		}
	}
	if valid(t, strings.Replace(driverText("BACKOFF", ""), `"revision":"29"`, `"revision":"29","revision":"29"`, 1)) {
		t.Error("duplicate member accepted")
	}
}

func TestRecoveryAttempt(t *testing.T) {
	for _, attempt := range []string{"1", "2", "3", "1000000"} {
		r := goodRecovery()
		r.attempt = attempt
		if !valid(t, driverText("BACKOFF", r.recovery())) {
			t.Errorf("attempt %s refused", attempt)
		}
	}
	for _, attempt := range []string{"0", "-1", `"3"`, "3.0", "1.5", "true", "false", "null", "[3]"} {
		r := goodRecovery()
		r.attempt = attempt
		if valid(t, driverText("BACKOFF", r.recovery())) {
			t.Errorf("attempt %s accepted", attempt)
		}
	}
}

func TestRecoveryTimestamp(t *testing.T) {
	for _, stamp := range []string{"2000-01-01T00:00:08Z", "2000-01-01T00:00:08.5Z", "2000-01-01T00:00:08.123456789Z", "2000-02-29T23:59:59Z", "2026-12-31T23:59:59Z"} {
		r := goodRecovery()
		r.stamp = quote(t, stamp)
		if !valid(t, driverText("BACKOFF", r.recovery())) {
			t.Errorf("timestamp %s refused", stamp)
		}
	}
	for _, stamp := range []string{
		"2000-01-01T00:00:08+00:00", "2000-01-01T01:00:08+01:00", "2000-01-01T00:00:08z", "2000-01-01T00:00:08",
		"2000-01-01 00:00:08Z", "2000-01-01t00:00:08Z", "2000-01-01T00:00Z", "2000-01-01T00:00:08.Z",
		"2000-01-01T00:00:08.1234567890Z", "2000-02-30T00:00:08Z", "2001-02-29T00:00:08Z", "2000-13-01T00:00:08Z",
		"2000-01-01T24:00:00Z", "2000-01-01T00:60:00Z", "2000-01-01T00:00:60Z", "0000-01-01T00:00:08Z",
		"2000-01-01T00:00:08Z\n", "\u0662000-01-01T00:00:08Z", "",
	} {
		r := goodRecovery()
		r.stamp = quote(t, stamp)
		if valid(t, driverText("BACKOFF", r.recovery())) {
			t.Errorf("timestamp %q accepted", stamp)
		}
	}
	for _, stamp := range []string{"946684808", "null", `["2000-01-01T00:00:08Z"]`} {
		r := goodRecovery()
		r.stamp = stamp
		if valid(t, driverText("BACKOFF", r.recovery())) {
			t.Errorf("timestamp %s accepted", stamp)
		}
	}
}

func TestRecoveryFailureClass(t *testing.T) {
	for _, class := range []string{"DIAL_FAILED", "TIMEOUT", "TRANSPORT_CLOSED", "OTHER"} {
		r := goodRecovery()
		r.class = quote(t, class)
		if !valid(t, driverText("BACKOFF", r.recovery())) {
			t.Errorf("class %s refused", class)
		}
	}
	for _, class := range []string{`"dial_failed"`, `"Timeout"`, `"UNKNOWN"`, `"NONE"`, `"RETRY_SCHEDULED"`, `"DEPENDENCY_UNAVAILABLE"`, `"DIAL_FAILED "`, `""`, "null", "1", "true", `["TIMEOUT"]`, `{"TIMEOUT":1}`} {
		r := goodRecovery()
		r.class = class
		if valid(t, driverText("BACKOFF", r.recovery())) {
			t.Errorf("class %s accepted", class)
		}
	}
}

// The checker's address-shape causes. Each is refused, and the same text with
// the address removed is accepted.
var addressCauses = []string{
	"dial tcp 192.0.2.10: connection refused",
	"dial tcp 192.0.2.10:9000: connection refused",
	"refused by adapter:9000",
	"dial tcp [2001:db8::1]: connection refused",
	"adapter aa:bb:cc:dd:ee:ff did not answer",
	"dial tcp://adapter: connection refused",
	"dial example.invalid: no such host",
	"login as user@adapter refused",
	"open /dev/ttyUSB0: no such device",
	"open C:\\com3: access denied",
	"lookup gateway: no such host",
	"dial ebus-adapter: connection refused",
	"dial tcp ebus-adapter: connection refused",
}

func TestRecoveryCause(t *testing.T) {
	for _, cause := range []string{"", "dial: connection refused", "i/o timeout", "read: connection reset by peer", strings.Repeat("x", 160)} {
		r := goodRecovery()
		r.cause = quote(t, cause)
		if !valid(t, driverText("BACKOFF", r.recovery())) {
			t.Errorf("cause %q refused", cause)
		}
	}
	for _, cause := range []string{strings.Repeat("x", 161), strings.Repeat("x", 1000), "caf\u00e9", "line\nbreak", "tab\there", "nul\x00", "del\x7f", "\u2028"} {
		r := goodRecovery()
		r.cause = quote(t, cause)
		if valid(t, driverText("BACKOFF", r.recovery())) {
			t.Errorf("cause %q accepted", cause)
		}
	}
	for _, cause := range []string{"null", "5", "true", `["x"]`, `{"cause":"x"}`} {
		r := goodRecovery()
		r.cause = cause
		if valid(t, driverText("BACKOFF", r.recovery())) {
			t.Errorf("cause %s accepted", cause)
		}
	}
	for _, cause := range addressCauses {
		if CauseValid(cause) {
			t.Errorf("address cause %q accepted", cause)
		}
		r := goodRecovery()
		r.cause = quote(t, cause)
		if valid(t, driverText("BACKOFF", r.recovery())) {
			t.Errorf("driver with address cause %q accepted", cause)
		}
	}
	for _, stripped := range []string{
		"dial tcp connection refused", "refused by adapter", "dial tcp: connection refused", "adapter did not answer",
		"login as refused", "open no such device", "lookup: no such host", "dial: connection refused",
		"dial tcp: lookup failed to start", "redial adapter later", "connected to adapter",
	} {
		if !CauseValid(stripped) {
			t.Errorf("stripped cause %q refused", stripped)
		}
	}
}

// causeVectors are short texts with the checker's own answer (true: the
// checker accepts the cause), taken by running the public checker on each.
// They pin the edges of the rule: the host label after dial, lookup, resolve
// and connect to, the network words, the port length and the dotted forms.
var causeVectors = map[string]bool{
	"dial attempt after timeout":    true,
	"lookup failed to complete":     true,
	"resolve the name later":        true,
	"connect to the adapter failed": true,
	"dial the adapter failed":       true,
	"dial x y":                      true,
	"":                              true,
	" ":                             true,
	"dial tcp":                      true,
	"dial tcp udp: x":               true,
	"dial gateway":                  false,
	"dial gateway: x":               false,
	"dial gateway x":                true,
	"resolve gateway:":              false,
	"connect gateway: x":            true,
	"dial tcp gateway":              false,
	"dial tcp gateway x":            true,
	"dial a-b-c: x":                 false,
	"dial tcp-x: y":                 true,
	"lookup 192.0.2.4":              false,
	"host:12345 failed":             false,
	"host:123456 failed":            true,
	"host:0":                        false,
	"::1":                           false,
	"aa:bb: x":                      false,
	"time 12:30 end":                false,
	"x@y":                           false,
	"path /x":                       false,
	"slash a/b":                     true,
	"https://e":                     false,
	"v1.2":                          false,
	"end.":                          true,
	"two. words":                    true,
	"back\\slash":                   false,
	"colon:":                        true,
	"drive C:\\x":                   false,
	"(/x)":                          false,
	"tcp://":                        false,
	"192.0.2":                       false,
	"192.0.2.4.5":                   false,
	"999.999.999.999":               false,
	"12:34:56":                      false,
	"a:b":                           true,
	"dial\tgateway: x":              false,
	"xdial gateway: x":              true,
	"dial gateway_name: x":          true,
	"dial gateway-name: x":          false,
	"dial tcpgateway: x":            false,
	"dial tcp.gateway: x":           false,
	"dial tcp gateway:80":           false,
	"dial 80":                       false,
	"dial tcp 80":                   false,
	"lookup udp gateway":            false,
	"lookup udp":                    true,
	"lookup tcp tcp":                true,
	"resolve tcp tcp: x":            true,
	"resolve tcp udp x: y":          true,
	"x1.5":                          false,
	"3.5":                           false,
	"a .b":                          true,
	"a. b":                          true,
	"-.-":                           false,
	"@":                             true,
	"a@":                            true,
	"@a":                            true,
	"a@b":                           false,
	"dial ":                         true,
	"dial :":                        true,
	"dial x:":                       false,
	"dial tcp :":                    true,
	"dial tcp x:":                   false,
	"dial tcp tcp x":                true,
	"dial tcp udp":                  true,
	"dial tcp udp:":                 true,
	"dial tcp udp x":                true,
	"dial tcp udp x:":               true,
	"dial tcp tcp x: y":             true,
	"dial tcp tcp: y":               true,
	"dial tcp -:":                   false,
	"dial tcp -":                    false,
	"dial -":                        false,
	"dial --:":                      false,
	"dial tcp 198.51.100.8":         false,
	"lookup ::":                     false,
	"nothing to see":                true,
	"caf\u00e9":                     false,
	"a\u0000b":                      false,
	"a\u007fb":                      false,
	"redial adapter later":          true,
	"connected to adapter":          true,
	"tcp: dial tcp":                 true,
	"dial\ngateway: x":              false,
	"\\":                            false,
	"C:\\":                          false,
	"C:/x":                          true,
	"a/b:80":                        false,
	"a:80b":                         false,
	"a:1234567":                     true,
	":80":                           true,
	" :80":                          true,
	"a :80":                         true,
	"a: 80":                         true,
}

func TestCauseVectorsMatchTheCheckersAnswer(t *testing.T) {
	for text, want := range causeVectors {
		if got := CauseValid(text); got != want {
			t.Errorf("CauseValid(%q) = %t; the checker answers %t", text, got, want)
		}
	}
}

func TestTimestampPredicate(t *testing.T) {
	if Timestamp(nil) || Timestamp(5) || Timestamp(json.Number("5")) {
		t.Fatal("non-string timestamp accepted")
	}
}
