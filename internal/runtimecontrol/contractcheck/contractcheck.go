// Package contractcheck is a Go rendering of the driver-object rule that the
// public MCP contract checker applies to the runtime tools' driver object and
// its optional recovery member. It is written independently of the gateway's
// own redaction and rendering, and only tests import it, so a gateway output
// can be held to the contract's rule without trusting the gateway's code.
//
// The rule is the contract's: the driver object has eight required members
// and one optional member, recovery, which is allowed only while the observed
// state is BACKOFF; recovery is closed with four required members.
package contractcheck

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// CauseMaxChars is the contract's bound of last_failure_cause.
const CauseMaxChars = 160

var (
	driverMembers = []string{"driver_id", "enabled", "desired_state", "desired_revision", "observed_state", "reason", "safety_quarantined", "revision"}

	observedStates = map[string]bool{"DISABLED": true, "STOPPED": true, "STARTING": true, "RUNNING": true, "DEGRADED": true, "BACKOFF": true, "STOPPING": true, "FAILED": true}
	desiredStates  = map[string]bool{"RUNNING": true, "STOPPED": true}
	failureClasses = map[string]bool{"DIAL_FAILED": true, "TIMEOUT": true, "TRANSPORT_CLOSED": true, "OTHER": true}

	driverID  = regexp.MustCompile(`^[a-z][a-z0-9]*(?:[.-][a-z0-9]+)*$`)
	reasonRe  = regexp.MustCompile(`^[A-Z][A-Z_]*$`)
	revisionR = regexp.MustCompile(`^(?:0|[1-9][0-9]{0,19})$`)
	utcStamp  = regexp.MustCompile(`^([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})(?:\.[0-9]{1,9})?Z$`)

	addrIPv4   = regexp.MustCompile(`[0-9]{1,3}(?:\.[0-9]{1,3}){3}`)
	addrGroups = regexp.MustCompile(`::|(?:[0-9A-Fa-f]{1,4}:){2,}`)
	addrScheme = regexp.MustCompile(`[A-Za-z][A-Za-z0-9+.-]*://`)
	addrPath   = regexp.MustCompile(`(?:^|[\s(])/[A-Za-z0-9._-]|[A-Za-z]:\\|\\`)
	addrUser   = regexp.MustCompile(`\S@\S`)
	addrDotted = regexp.MustCompile(`[A-Za-z0-9-]\.[A-Za-z0-9-]`)
	addrKey    = regexp.MustCompile(`\b(?:dial|lookup|resolve|connect to)\s+`)
	addrNet    = regexp.MustCompile(`^(?:tcp|udp)\s+`)
)

func wordByte(c byte) bool {
	return c == '_' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

func labelByte(c byte) bool { return wordByte(c) && c != '_' || c == '-' }

func space(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v'
}

// hostPort: a non-space byte, a colon, then one to five digits that are not
// followed by another digit.
func hostPort(s string) bool {
	for i := 1; i < len(s); i++ {
		if s[i] != ':' || space(s[i-1]) {
			continue
		}
		digits := 0
		for i+1+digits < len(s) && s[i+1+digits] >= '0' && s[i+1+digits] <= '9' {
			digits++
		}
		if digits >= 1 && digits <= 5 {
			return true
		}
	}
	return false
}

// operandAt: a host label that is not the word tcp or udp and is followed by a
// colon or the end of the text.
func operandAt(rest string) bool {
	for _, word := range []string{"tcp", "udp"} {
		if strings.HasPrefix(rest, word) && (len(rest) == len(word) || !wordByte(rest[len(word)])) {
			return false
		}
	}
	n := 0
	for n < len(rest) && labelByte(rest[n]) {
		n++
	}
	return n > 0 && (n == len(rest) || rest[n] == ':')
}

func hostOperand(s string) bool {
	for _, key := range addrKey.FindAllStringIndex(s, -1) {
		rest := s[key[1]:]
		if operandAt(rest) {
			return true
		}
		if network := addrNet.FindStringIndex(rest); network != nil && operandAt(rest[network[1]:]) {
			return true
		}
	}
	return false
}

// CauseValid reports whether s is an acceptable last_failure_cause: at most
// 160 characters of printable ASCII with no address shape.
func CauseValid(s string) bool {
	if len(s) > CauseMaxChars {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < ' ' || s[i] > '~' {
			return false
		}
	}
	return !addrIPv4.MatchString(s) && !addrGroups.MatchString(s) && !hostPort(s) && !addrScheme.MatchString(s) &&
		!addrPath.MatchString(s) && !addrUser.MatchString(s) && !addrDotted.MatchString(s) && !hostOperand(s)
}

// Timestamp reports whether v is an RFC 3339 UTC timestamp with the Z
// designator: a real calendar date and clock time, whole seconds or one to nine
// fractional digits, never an offset.
func Timestamp(v any) bool {
	s, ok := v.(string)
	if !ok {
		return false
	}
	m := utcStamp.FindStringSubmatch(s)
	if m == nil {
		return false
	}
	var n [6]int
	for i := range n {
		n[i], _ = strconv.Atoi(m[i+1])
	}
	if n[0] < 1 || n[1] < 1 || n[1] > 12 || n[2] < 1 || n[3] > 23 || n[4] > 59 || n[5] > 59 {
		return false
	}
	t := time.Date(n[0], time.Month(n[1]), n[2], n[3], n[4], n[5], 0, time.UTC)
	return t.Day() == n[2]
}

func revision(v any) bool {
	s, ok := v.(string)
	if !ok || !revisionR.MatchString(s) {
		return false
	}
	_, err := strconv.ParseUint(s, 10, 64)
	return err == nil
}

func integerAtLeastOne(v any) bool {
	n, ok := v.(json.Number)
	if !ok {
		return false
	}
	i, err := strconv.ParseInt(string(n), 10, 64)
	return err == nil && i >= 1
}

func hasExactly(object map[string]any, members ...string) bool {
	if len(object) != len(members) {
		return false
	}
	for _, member := range members {
		if _, ok := object[member]; !ok {
			return false
		}
	}
	return true
}

// Recovery reports whether v is a valid recovery object: closed, with the four
// required members, each in range.
func Recovery(v any) bool {
	object, ok := v.(map[string]any)
	if !ok || !hasExactly(object, "attempt", "next_attempt_at", "last_failure_class", "last_failure_cause") {
		return false
	}
	class, classOK := object["last_failure_class"].(string)
	cause, causeOK := object["last_failure_cause"].(string)
	return integerAtLeastOne(object["attempt"]) && Timestamp(object["next_attempt_at"]) &&
		classOK && failureClasses[class] && causeOK && CauseValid(cause)
}

// Driver reports whether v is a valid driver object of the runtime tools: the
// eight required members, at most the one optional recovery member, and that
// member only while observed_state is BACKOFF.
func Driver(v any) bool {
	object, ok := v.(map[string]any)
	if !ok {
		return false
	}
	for _, member := range driverMembers {
		if _, ok := object[member]; !ok {
			return false
		}
	}
	_, hasRecovery := object["recovery"]
	if len(object) != len(driverMembers)+boolInt(hasRecovery) {
		return false
	}
	id, idOK := object["driver_id"].(string)
	_, enabledOK := object["enabled"].(bool)
	_, quarantinedOK := object["safety_quarantined"].(bool)
	desired, desiredOK := object["desired_state"].(string)
	state, stateOK := object["observed_state"].(string)
	reason, reasonOK := object["reason"].(string)
	desiredRevision, _ := object["desired_revision"].(string)
	membersValid := idOK && len(id) <= 128 && driverID.MatchString(id) && enabledOK && quarantinedOK &&
		desiredOK && desiredStates[desired] && revision(object["desired_revision"]) && desiredRevision != "0" &&
		stateOK && observedStates[state] && reasonOK && reasonRe.MatchString(reason) && revision(object["revision"])
	if !membersValid {
		return false
	}
	return !hasRecovery || (state == "BACKOFF" && Recovery(object["recovery"]))
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// Decode parses one JSON document the way the checker does: a member that
// appears twice in one object is an error, numbers keep their literal text, and
// nothing may follow the document.
func Decode(raw []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	value, err := decodeValue(decoder)
	if err != nil {
		return nil, err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("contractcheck: trailing data")
	}
	return value, nil
}

func decodeValue(decoder *json.Decoder) (any, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	switch token {
	case json.Delim('{'):
		object := map[string]any{}
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return nil, err
			}
			key := keyToken.(string)
			if _, duplicate := object[key]; duplicate {
				return nil, errors.New("contractcheck: duplicate member " + key)
			}
			if object[key], err = decodeValue(decoder); err != nil {
				return nil, err
			}
		}
		if _, err := decoder.Token(); err != nil {
			return nil, err
		}
		return object, nil
	case json.Delim('['):
		array := []any{}
		for decoder.More() {
			element, err := decodeValue(decoder)
			if err != nil {
				return nil, err
			}
			array = append(array, element)
		}
		if _, err := decoder.Token(); err != nil {
			return nil, err
		}
		return array, nil
	}
	return token, nil
}
