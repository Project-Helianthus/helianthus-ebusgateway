package main

// Issue #1373: during a recovery and during a START, an adapter that answers
// the INIT handshake with features 0x00, rejects INIT, sends a malformed
// payload or closes the connection is degraded or restarting, not
// misconfigured. Each of these is a retryable failure: the persistent series
// of #1369 continues on schedule and reaches RUNNING once the adapter answers
// again, without a restart. CONFIG_INVALID is left only for a configuration
// that local validation rejects before any dial. The failed attempt's log
// line carries the handshake outcome: the INIT attempts inside the
// construction, the features seen and the payload class of every failed INIT
// attempt.
//
// The process tests drive the production composition against the scripted
// loopback endpoint of issue1367_recovery_test.go, with the production retry
// policy and the injected manager clock of issue1369_persistent_recovery_test.go.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"regexp"
	"strings"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// issue1373Malformed is an ENH second byte without a first byte: the
// adapter's INIT answer does not match the expected schema.
var issue1373Malformed = []byte{0x80}

func issue1373Resetted(features byte) []byte {
	frame := transport.EncodeENH(transport.ENHResResetted, features)
	return frame[:]
}

// issue1373IncidentInit answers INIT like the observed adapter: features
// 0x00 for three requests, then malformed payloads.
func issue1373IncidentInit(_, request int) []byte {
	if request <= 3 {
		return issue1373Resetted(0x00)
	}
	return issue1373Malformed
}

func issue1373MalformedInit(int, int) []byte { return issue1373Malformed }

func issue1373RejectedInit(int, int) []byte {
	frame := transport.EncodeENH(transport.ENHResErrorEBUS, 0x01)
	return frame[:]
}

const issue1373InvalidPayloadText = `error="ebus: payload does not match expected schema"`

// issue1373Handshake scripts one way the adapter fails the INIT handshake and
// the fields of the attempt line it produces.
type issue1373Handshake struct {
	name     string
	protocol ebusgateway.TransportProtocol
	stream   bool
	reply    func(session, request int) []byte
	// fields are the failed attempt's log fields.
	fields string
}

var issue1373Handshakes = []issue1373Handshake{
	{"adapter_direct_features_then_malformed", ebusgateway.TransportAdapterDirect, true, issue1373IncidentInit,
		"class=invalid_payload, " + issue1373InvalidPayloadText + ", handshake=(init_attempts=5, features_seen=0x00/0x00/0x00, payload_class=invalid_payload/invalid_payload)"},
	{"adapter_direct_rejected", ebusgateway.TransportAdapterDirect, true, issue1373RejectedInit,
		"class=invalid_payload, " + issue1373InvalidPayloadText + ", handshake=(init_attempts=5, features_seen=none, payload_class=invalid_payload/invalid_payload/invalid_payload/invalid_payload/invalid_payload)"},
	{"proxy_class_malformed", ebusgateway.TransportENH, false, issue1373MalformedInit,
		"class=invalid_payload, " + issue1373InvalidPayloadText + ", handshake=(init_attempts=1, features_seen=none, payload_class=invalid_payload)"},
	{"proxy_class_rejected", ebusgateway.TransportENH, false, issue1373RejectedInit,
		"class=invalid_payload, " + issue1373InvalidPayloadText + ", handshake=(init_attempts=1, features_seen=none, payload_class=invalid_payload)"},
}

// issue1373AttemptLines is the ordered log of one series: each BACKOFF with
// its schedule, each attempt with its phase and reason, and each failed
// attempt with its fields.
func issue1373AttemptLines(retries []issue1369Retry, firstReason string, failed []string) []string {
	var lines []string
	reason := firstReason
	for n := range retries {
		attempt := n + 1
		phase := "fast"
		if attempt > 5 {
			phase = "persistent"
		}
		lines = append(lines,
			fmt.Sprintf("ebus.primary: lifecycle BACKOFF (reason=RETRY_SCHEDULED, next_attempt=%d, next_attempt_at=%s, delay=%s, phase=%s)",
				attempt, retries[n].snapshot.Retry.NotBeforeUTC.UTC().Format(ebusRecoveryTimeLayout), retries[n].delay.Round(time.Millisecond), phase),
			fmt.Sprintf("ebus.primary: recovery attempt %d (reason=%s, phase=%s)", attempt, reason, phase))
		if n < len(failed) {
			lines = append(lines, fmt.Sprintf("ebus.primary: recovery attempt %d failed (%s)", attempt, failed[n]))
			reason = "retry_after_" + strings.TrimPrefix(strings.SplitN(failed[n], ",", 2)[0], "class=")
		}
	}
	return lines
}

func issue1373Repeat(fields string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fields
	}
	return out
}

// issue1373RequireRecovered asserts the end of a recovery without a restart:
// RUNNING in the service view, MCP list and Portal with the unchanged desired
// revision and no schedule, the manager at the next generation, and the
// version answered by the session that healed.
func issue1373RequireRecovered(t *testing.T, process *rctlProcess, endpoint *issue1367Endpoint, logs *issue1367Logs, desired runtimecontrol.DesiredRevision, generation uint64) drivermanager.Snapshot {
	t.Helper()
	status := waitIssue1367Status(t, process, drivermanager.ObservedRunning, drivermanager.ReasonNone, logs)
	if status.NextAttempt != 0 || !status.NextAttemptUTC.IsZero() || status.DesiredRevision != desired {
		t.Fatalf("recovered view = %+v; want no schedule and the unchanged desired revision %d (no restart)", status, desired)
	}
	if got, reason := portalConfigReadbackStatus(status); got != "effective" || reason != "running" {
		t.Fatalf("Portal readback after recovery = %s/%s; want effective/running", got, reason)
	}
	requireIssue1367List(t, process, drivermanager.ObservedRunning, drivermanager.ReasonNone)
	recovered := process.control.driver.Snapshot()
	if recovered.ObservedState != drivermanager.ObservedRunning || recovered.Generation != generation || recovered.Retry != nil {
		t.Fatalf("manager snapshot = %s generation %d retry %+v; want RUNNING generation %d", recovered.ObservedState, recovered.Generation, recovered.Retry, generation)
	}
	requireIssue1367Version(t, process, endpoint.sessionCount())
	if logs.count("ebus.primary: lifecycle FAILED") != 0 || logs.count("CONFIG_INVALID") != 0 {
		t.Fatalf("recovery logged a terminal state:\n%s", issue1367RecoveryLines(logs.text()))
	}
	issue1369RequireNoEndpointInRecoveryLines(t, logs, endpoint)
	return recovered
}

// After the stall (adapter-direct) or the loss (proxy class) the adapter
// answers INIT badly for two attempts; the series continues on schedule and
// the third attempt, answered with features 0x01, recovers without a
// restart.
func TestIssue1373FailedInitHandshakeDuringRecoveryIsRetriedUntilTheAdapterAnswers(t *testing.T) {
	for _, tc := range issue1373Handshakes {
		t.Run(tc.name, func(t *testing.T) {
			clock := newIssue1369Clock(issue1369NoJitter)
			withIssue1369Seams(t, clock)
			logs := captureIssue1367Logs(t)
			endpoint := newIssue1367Endpoint(t, tc.stream)
			process := startIssue1369Process(t, clock, endpoint, tc.protocol)
			first := process.control.driver.Snapshot()
			before := process.status()
			cursor := process.control.Service().List().EventCursor

			const healAt = 3
			reply := tc.reply
			endpoint.initReply.Store(&reply)
			clock.setOnRetry(func(attempt int) issue1369Action {
				if attempt == healAt {
					endpoint.initReply.Store(nil)
				}
				return issue1369Proceed
			})
			loss := ebusLossAdapterConnection
			if tc.stream {
				endpoint.stall()
			} else {
				loss = ebusLossTransportClosed
				endpoint.drop()
				issue1367DetectProxyLoss(t, process, logs)
			}

			retries := clock.waitRetries(t, healAt, logs)
			recovered := issue1373RequireRecovered(t, process, endpoint, logs, before.DesiredRevision, first.Generation+1)
			issue1369RequireSchedule(t, retries, before.DesiredRevision, issue1369NoJitter)
			if len(clock.recorded()) != healAt || endpoint.sessionCount() != 1+healAt {
				t.Fatalf("scheduled retries = %d, sessions = %d; want %d and %d", len(clock.recorded()), endpoint.sessionCount(), healAt, 1+healAt)
			}
			issue1369RequireNoTerminalEvent(t, process, cursor)

			lines := []string{fmt.Sprintf("ebus.primary: connection lost (reason=%s, generation=%d); %s", loss, first.Generation, issue1373Schedule)}
			lines = append(lines, issue1373AttemptLines(retries, loss, issue1373Repeat(tc.fields, healAt-1))...)
			lines = append(lines,
				fmt.Sprintf("ebus.primary: recovery attempt %d reconnected (generation=%d); awaiting admission", healAt, recovered.Generation),
				fmt.Sprintf("ebus.primary: recovered on attempt %d (generation=%d)", healAt, recovered.Generation),
				fmt.Sprintf("ebus.primary: lifecycle RUNNING (generation=%d, recovered)", recovered.Generation))
			logs.requireOrdered(t, lines...)
			if tc.stream {
				// The multiplexer's own INIT lines still reach the process log.
				issue1373RequireMuxInitLines(t, logs, tc.reply, healAt-1)
			}
		})
	}
}

// issue1373Schedule is the schedule text of the production policy.
const issue1373Schedule = "recovery continues while desired RUNNING (5 fast attempts, then backoff from 1s doubling to 5m0s)"

// issue1373RequireMuxInitLines asserts that the multiplexer's per-attempt
// INIT lines of every failed construction reached the process log.
func issue1373RequireMuxInitLines(t *testing.T, logs *issue1367Logs, reply func(int, int) []byte, failed int) {
	t.Helper()
	for request := 1; request <= 5; request++ {
		want := fmt.Sprintf("adaptermux: INIT attempt %d failed: ", request)
		if bytes.Equal(reply(0, request), issue1373Resetted(0x00)) {
			want = fmt.Sprintf("adaptermux: INIT attempt %d: features=0x00 (want 0x01), retrying", request)
		}
		if got := logs.count(want); got < failed {
			t.Fatalf("process log has %d lines %q; want one per failed construction (%d)", got, want, failed)
		}
	}
}

// At boot the adapter answers INIT badly for the opening START's first
// attempt and the first retry; the START is admitted into the recovery and
// reaches RUNNING at the second retry, in one epoch.
func TestIssue1373FailedInitHandshakeAtBootEntersRecoveryAndRuns(t *testing.T) {
	for _, tc := range []issue1373Handshake{issue1373Handshakes[0], issue1373Handshakes[2]} {
		t.Run(tc.name, func(t *testing.T) {
			clock := newIssue1369Clock(issue1369NoJitter)
			logs := captureIssue1367Logs(t)
			withIssue1371Seams(t, clock, logs)
			endpoint := newIssue1367Endpoint(t, tc.stream)
			reply := tc.reply
			endpoint.initReply.Store(&reply)

			const healAt = 2
			clock.setOnRetry(func(attempt int) issue1369Action {
				if attempt == healAt {
					endpoint.initReply.Store(nil)
				}
				return issue1369Proceed
			})
			process := startIssue1371Process(t, clock, endpoint, tc.protocol)
			retries := clock.waitRetries(t, healAt, logs)
			desired := process.status().DesiredRevision
			waitIssue1367Status(t, process, drivermanager.ObservedRunning, drivermanager.ReasonNone, logs)
			running := issue1373RequireRecovered(t, process, endpoint, logs, desired, process.control.driver.Snapshot().Generation)
			issue1369RequireSchedule(t, retries, desired, issue1369NoJitter)
			if len(clock.recorded()) != healAt || endpoint.sessionCount() != 1+healAt {
				t.Fatalf("scheduled retries = %d, sessions = %d; want %d and %d", len(clock.recorded()), endpoint.sessionCount(), healAt, 1+healAt)
			}
			issue1371RequireOneEpoch(t, process, drivermanager.ObservedRunning)

			lines := []string{fmt.Sprintf("ebus.primary: start attempt 1 failed (%s); %s", tc.fields, issue1373Schedule)}
			lines = append(lines, issue1373AttemptLines(retries, "retry_after_invalid_payload", issue1373Repeat(tc.fields, healAt-1))...)
			lines = append(lines,
				fmt.Sprintf("ebus.primary: recovered on attempt %d (generation=%d)", healAt, running.Generation),
				fmt.Sprintf("ebus.primary: lifecycle RUNNING (generation=%d, recovered)", running.Generation))
			logs.requireOrdered(t, lines...)
			if logs.count("ebus.primary: start attempt 2") != 0 {
				t.Fatalf("the boot recovery logged a second start:\n%s", issue1367RecoveryLines(logs.text()))
			}
		})
	}
}

// The endpoint accepts and drops every connection for two attempts and then
// refuses dials into the persistent phase; once it answers again the seventh
// attempt recovers, every delay on the production schedule.
func TestIssue1373ClosedEndpointRecoversOnSchedule(t *testing.T) {
	for _, tc := range []struct {
		name      string
		protocol  ebusgateway.TransportProtocol
		stream    bool
		handshake string
	}{
		{"adapter_direct", ebusgateway.TransportAdapterDirect, true,
			`handshake=\(init_attempts=5, features_seen=none, payload_class=transport_closed/transport_closed/transport_closed/transport_closed/transport_closed\)`},
		{"proxy_class", ebusgateway.TransportENH, false,
			`handshake=\(init_attempts=1, features_seen=none, payload_class=transport_closed\)`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := newIssue1369Clock(issue1369NoJitter)
			withIssue1369Seams(t, clock)
			logs := captureIssue1367Logs(t)
			endpoint := newIssue1367Endpoint(t, tc.stream)
			process := startIssue1369Process(t, clock, endpoint, tc.protocol)
			first := process.control.driver.Snapshot()
			before := process.status()
			cursor := process.control.Service().List().EventCursor

			const refuseAt, healAt = 3, 7
			endpoint.closeOnAccept.Store(true)
			clock.setOnRetry(func(attempt int) issue1369Action {
				switch attempt {
				case refuseAt:
					endpoint.refuse()
					endpoint.closeOnAccept.Store(false)
				case healAt:
					endpoint.open()
				}
				return issue1369Proceed
			})
			loss := ebusLossAdapterConnection
			if tc.stream {
				endpoint.stall()
			} else {
				loss = ebusLossTransportClosed
				endpoint.drop()
				issue1367DetectProxyLoss(t, process, logs)
			}

			retries := clock.waitRetries(t, healAt, logs)
			recovered := issue1373RequireRecovered(t, process, endpoint, logs, before.DesiredRevision, first.Generation+1)
			issue1369RequireSchedule(t, retries, before.DesiredRevision, issue1369NoJitter)
			if len(clock.recorded()) != healAt || endpoint.sessionCount() != 1+(refuseAt-1)+1 {
				t.Fatalf("scheduled retries = %d, sessions = %d; want %d and %d", len(clock.recorded()), endpoint.sessionCount(), healAt, refuseAt+1)
			}
			issue1369RequireNoTerminalEvent(t, process, cursor)

			// The text of a dropped connection depends on when the drop is
			// seen (end of stream or reset); its class does not.
			text := logs.text()
			for attempt := 1; attempt < refuseAt; attempt++ {
				pattern := regexp.MustCompile(fmt.Sprintf(`ebus\.primary: recovery attempt %d failed \(class=transport_closed, error="[^"]*", %s\)`, attempt, tc.handshake))
				if !pattern.MatchString(text) {
					t.Fatalf("attempt %d line does not match %s; recovery log:\n%s", attempt, pattern, issue1367RecoveryLines(text))
				}
			}
			refused := `class=dial_refused, error="connect: connection refused"`
			lines := []string{fmt.Sprintf("ebus.primary: connection lost (reason=%s, generation=%d); %s", loss, first.Generation, issue1373Schedule)}
			attemptLines := issue1373AttemptLines(retries, loss, append([]string{"class=transport_closed", "class=transport_closed"}, issue1373Repeat(refused, healAt-refuseAt)...))
			for _, line := range attemptLines {
				// The dropped-connection lines are matched above.
				if strings.Contains(line, "failed (class=transport_closed") {
					line = strings.TrimSuffix(line, ")")
				}
				lines = append(lines, line)
			}
			lines = append(lines,
				fmt.Sprintf("ebus.primary: recovered on attempt %d (generation=%d)", healAt, recovered.Generation),
				fmt.Sprintf("ebus.primary: lifecycle RUNNING (generation=%d, recovered)", recovered.Generation))
			logs.requireOrdered(t, lines...)
		})
	}
}

// A configuration that local validation rejects still fails the START with
// CONFIG_INVALID, before any dial and without a retry: an adapter-direct
// option that does not parse, and a proxy-class protocol the gateway cannot
// open. Both name the scripted endpoint, which never sees a connection.
func TestIssue1373ValidationRejectedConfigurationFailsBeforeAnyDial(t *testing.T) {
	for _, tc := range []struct {
		name   string
		config func(*ebusgateway.Config)
		line   string
	}{
		{"adapter_direct", func(cfg *ebusgateway.Config) {
			cfg.TransportConfig.Protocol = ebusgateway.TransportAdapterDirect
			cfg.PhantomInitiatorRejectBytes = "0xZZ"
		}, `ebus.primary: start attempt 1 failed (class=config_rejected, error="invalid syntax")`},
		{"proxy_class", func(cfg *ebusgateway.Config) {
			cfg.TransportConfig.Protocol = "enh-unsupported"
		}, `ebus.primary: start attempt 1 failed (class=config_rejected, ` + issue1373InvalidPayloadText + `)`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := newIssue1369Clock(issue1369NoJitter)
			logs := captureIssue1367Logs(t)
			withIssue1371Seams(t, clock, logs)
			endpoint := newIssue1367Endpoint(t, true)
			env := newRCtlEnv(t)
			process := env.start(rctlProcessOptions{mcp: true, config: func(cfg *ebusgateway.Config) {
				cfg.TransportConfig.Network = "tcp"
				cfg.TransportConfig.Address = endpoint.address
				cfg.TransportConfig.Dial = nil
				cfg.TransportConfig.DialTimeout = time.Second
				tc.config(cfg)
			}})
			clock.process.Store(process)

			failed := waitIssue1367Status(t, process, drivermanager.ObservedFailed, drivermanager.ReasonConfigInvalid, logs)
			if failed.SafetyQuarantined || failed.DesiredState != runtimecontrol.DesiredRunning || failed.NextAttempt != 0 {
				t.Fatalf("terminal view = %+v; want desired RUNNING, not quarantined, no schedule", failed)
			}
			requireIssue1367List(t, process, drivermanager.ObservedFailed, drivermanager.ReasonConfigInvalid)
			if got, reason := portalConfigReadbackStatus(failed); got != "error" || reason != "config_invalid" {
				t.Fatalf("Portal readback = %s/%s; want error/config_invalid", got, reason)
			}
			// The failed START stopped the manager after its one attempt.
			if snapshot := process.control.driver.Snapshot(); snapshot.Retry != nil || snapshot.Generation != 0 || snapshot.Attempt != 1 {
				t.Fatalf("manager snapshot = %+v; want one attempt, nothing constructed, no retry", snapshot)
			}
			time.Sleep(50 * time.Millisecond)
			if len(clock.recorded()) != 0 || endpoint.sessionCount() != 0 {
				t.Fatalf("scheduled retries = %d, endpoint sessions = %d; want no retry and no dial", len(clock.recorded()), endpoint.sessionCount())
			}
			for _, event := range issue1367ObservedSince(process, 0) {
				if event.ObservedState == drivermanager.ObservedBackoff {
					t.Fatalf("a rejected configuration published BACKOFF: %+v", event)
				}
			}
			logs.requireOrdered(t, tc.line)
			if logs.count("recovery continues") != 0 || logs.count("ebus.primary: recovery attempt") != 0 || logs.count("handshake=") != 0 {
				t.Fatalf("a rejected configuration opened a recovery or reached a handshake:\n%s", issue1367RecoveryLines(logs.text()))
			}
		})
	}
}

// Classification: only a configuration rejected by local validation before
// any dial is CONFIG_INVALID. Every answer of the peer, whatever it carries,
// is a retryable dependency failure; the identity mismatch and a stale or
// unavailable runtime keep their own terminal reasons.
func TestIssue1373OnlyLocalRejectionIsConfigInvalid(t *testing.T) {
	incident := fmt.Errorf("start multiplexer: %w", &ebusHandshakeFailure{
		err:     fmt.Errorf("adaptermux: initial connect: adaptermux: INIT handshake failed after 5 attempts: %w", ebuserrors.ErrInvalidPayload),
		outcome: ebusHandshakeOutcome{attempts: 5, features: []byte{0, 0, 0}, payloads: []string{"invalid_payload", "invalid_payload"}},
	})
	normalize := fmt.Errorf("invalid gateway transport endpoint: %w: %w", ebuserrors.ErrInvalidPayload, errors.New("missing scheme"))
	for name, tc := range map[string]struct {
		err       error
		retryable bool
		reason    drivermanager.ReasonCode
		class     string
	}{
		"INIT features then malformed": {incident, true, drivermanager.ReasonDependencyUnavailable, "invalid_payload"},
		"INIT rejected (proxy)": {ebusProxyHandshakeFailure(fmt.Errorf("gateway transport init failed: enh init ebus error 0x01: %w", ebuserrors.ErrInvalidPayload)),
			true, drivermanager.ReasonDependencyUnavailable, "invalid_payload"},
		"malformed payload":         {ebuserrors.ErrInvalidPayload, true, drivermanager.ReasonDependencyUnavailable, "invalid_payload"},
		"INIT timeout":              {fmt.Errorf("adaptermux: INIT handshake failed after 5 attempts: %w", ebuserrors.ErrTimeout), true, drivermanager.ReasonDependencyUnavailable, "handshake_timeout"},
		"connection dropped":        {fmt.Errorf("enh transport read closed: %w", ebuserrors.ErrTransportClosed), true, drivermanager.ReasonDependencyUnavailable, "transport_closed"},
		"adapter host error":        {fmt.Errorf("enh init host error 0x01: %w", ebuserrors.ErrAdapterHostError), true, drivermanager.ReasonDependencyUnavailable, "start_failed"},
		"unvalidated invalid input": {normalize, true, drivermanager.ReasonDependencyUnavailable, "invalid_payload"},
		"endpoint rejected":         {rejectEBusConfig(normalize), false, drivermanager.ReasonConfigInvalid, "config_rejected"},
		"option rejected wrapped":   {fmt.Errorf("start: %w", rejectEBusConfig(errors.New("invalid syntax"))), false, drivermanager.ReasonConfigInvalid, "config_rejected"},
		"rejection joined":          {errors.Join(errors.New("construction"), rejectEBusConfig(normalize)), false, drivermanager.ReasonConfigInvalid, "config_rejected"},
		"identity mismatch":         {errEBusControlHubNotLive, false, drivermanager.ReasonInternalError, "start_failed"},
		"runtime unavailable":       {transport.ErrDriverUnavailable, false, drivermanager.ReasonProviderUnavailable, "runtime_unavailable"},
		"manager unavailable":       {drivermanager.ErrUnavailable, false, drivermanager.ReasonProviderUnavailable, "start_failed"},
	} {
		failure := classifyEBusDriverError(tc.err)
		if failure.Reason.Retryable != tc.retryable || failure.Reason.Code != tc.reason {
			t.Fatalf("%s: classified %s retryable=%t; want %s retryable=%t", name, failure.Reason.Code, failure.Reason.Retryable, tc.reason, tc.retryable)
		}
		if got := ebusRecoveryFailureCategory(tc.err); got != tc.class {
			t.Fatalf("%s: class %q; want %q", name, got, tc.class)
		}
	}
	if got := ebusHandshakeText(incident); got != ", handshake=(init_attempts=5, features_seen=0x00/0x00/0x00, payload_class=invalid_payload/invalid_payload)" {
		t.Fatalf("incident handshake text = %q", got)
	}
	if got := ebusHandshakeText(rejectEBusConfig(normalize)); got != "" {
		t.Fatalf("a rejected configuration carries a handshake: %q", got)
	}
}

// The proxy-class validation agrees with the transport constructor: every
// configuration it rejects, the constructor rejects with the same sentinel
// (before its dial or right after it), and every configuration it accepts,
// empty fields filled with the defaults included, the constructor dials and
// opens without a configuration error.
func TestIssue1373ProxyClassValidationMatchesTheTransportConstructor(t *testing.T) {
	udp, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = udp.Close() }()
	for name, tc := range map[string]struct {
		config ebusgateway.TransportConfig
		valid  bool
	}{
		"enh":                {ebusgateway.TransportConfig{Protocol: ebusgateway.TransportENH, Network: "tcp", Address: "adapter.invalid:9999"}, true},
		"default protocol":   {ebusgateway.TransportConfig{Network: "tcp", Address: "adapter.invalid:9999"}, true},
		"ens":                {ebusgateway.TransportConfig{Protocol: ebusgateway.TransportENS, Network: "tcp", Address: "adapter.invalid:9999"}, true},
		"enh uri":            {ebusgateway.TransportConfig{Address: "enh://adapter.invalid:9999"}, true},
		"tcp-plain":          {ebusgateway.TransportConfig{Protocol: ebusgateway.TransportTCPPlain, Network: "tcp", Address: "adapter.invalid:9999"}, true},
		"ebusd alias":        {ebusgateway.TransportConfig{Protocol: "ebusd", Network: "tcp", Address: "adapter.invalid:8888"}, true},
		"udp-plain":          {ebusgateway.TransportConfig{Protocol: ebusgateway.TransportUDPPlain, Network: "udp", Address: udp.LocalAddr().String()}, true},
		"unknown scheme":     {ebusgateway.TransportConfig{Address: "unsupported://adapter.invalid:9999"}, false},
		"uri without port":   {ebusgateway.TransportConfig{Address: "tcp-plain://adapter.invalid"}, false},
		"default network":    {ebusgateway.TransportConfig{Protocol: ebusgateway.TransportENH, Address: "adapter.invalid:9999"}, true},
		"default address":    {ebusgateway.TransportConfig{Protocol: ebusgateway.TransportENH, Network: "tcp"}, true},
		"unknown protocol":   {ebusgateway.TransportConfig{Protocol: "enh-unsupported", Network: "tcp", Address: "adapter.invalid:9999"}, false},
		"udp-plain over tcp": {ebusgateway.TransportConfig{Protocol: ebusgateway.TransportUDPPlain, Network: "tcp", Address: "adapter.invalid:9999"}, false},
	} {
		cfg := ebusgateway.DefaultConfig()
		cfg.TransportConfig = tc.config
		cfg.TransportConfig.ReadTimeout = 50 * time.Millisecond
		cfg.TransportConfig.WriteTimeout = 50 * time.Millisecond
		_, verdict := validateEBusDriverTransport(cfg)
		if (verdict == nil) != tc.valid {
			t.Fatalf("%s: validation = %v; want valid=%t", name, verdict, tc.valid)
		}
		if verdict != nil && (!errors.Is(verdict, ebuserrors.ErrInvalidPayload) || ebusRecoveryErrorText(verdict) == "") {
			t.Fatalf("%s: validation error %v does not carry the configuration sentinel", name, verdict)
		}
		dials := 0
		cfg.TransportConfig.Dial = func(_ context.Context, network, _ string, _ time.Duration) (net.Conn, error) {
			dials++
			if strings.HasPrefix(network, "udp") {
				return net.DialUDP("udp", nil, udp.LocalAddr().(*net.UDPAddr))
			}
			client, server := net.Pipe()
			// The peer drops at once: an INIT, where the protocol has one,
			// fails with a closed transport, never with a configuration error.
			_ = server.Close()
			return client, nil
		}
		opened, closeFn, openErr := ebusgateway.OpenEBusDriverTransport(context.Background(), cfg)
		if opened != nil && closeFn != nil {
			_ = closeFn()
		}
		if tc.valid {
			if dials != 1 || errors.Is(openErr, ebuserrors.ErrInvalidPayload) {
				t.Fatalf("%s: accepted configuration: constructor dials = %d, error = %v; want one dial and no configuration error", name, dials, openErr)
			}
			continue
		}
		if !errors.Is(openErr, ebuserrors.ErrInvalidPayload) {
			t.Fatalf("%s: rejected configuration: constructor error = %v; want the configuration sentinel", name, openErr)
		}
	}
	injected := ebusgateway.DefaultConfig()
	injected.Transport = &issue1373NopTransport{}
	injected.TransportConfig = ebusgateway.TransportConfig{}
	if _, err := validateEBusDriverTransport(injected); err != nil {
		t.Fatalf("injected transport validation = %v; want nothing to validate", err)
	}
}

type issue1373NopTransport struct{ transport.RawTransport }

// The adapter-direct handshake trace records every INIT attempt line of the
// multiplexer until the construction returns and passes every line to the
// process log unchanged. Each field of the outcome is varied on its own.
func TestIssue1373HandshakeTraceRecordsTheMultiplexerInitAttempts(t *testing.T) {
	var out bytes.Buffer
	previousOutput, previousFlags, previousPrefix := log.Writer(), log.Flags(), log.Prefix()
	log.SetOutput(&out)
	log.SetFlags(0)
	log.SetPrefix("gw ")
	t.Cleanup(func() {
		log.SetOutput(previousOutput)
		log.SetFlags(previousFlags)
		log.SetPrefix(previousPrefix)
	})
	failure := errors.New("adaptermux: initial connect: adaptermux: INIT handshake failed after 5 attempts: boom")
	featureLine := func(attempt int, features byte) string {
		return fmt.Sprintf("adaptermux: INIT attempt %d: features=0x%02X (want 0x%02X), retrying", attempt, features, 0x01)
	}
	failedLine := func(attempt int, cause error) string {
		return fmt.Sprintf("adaptermux: INIT attempt %d failed: %v (retrying)", attempt, cause)
	}
	for name, tc := range map[string]struct {
		lines []string
		err   error
		want  string
	}{
		"incident": {[]string{featureLine(1, 0), featureLine(2, 0), featureLine(3, 0), failedLine(4, ebuserrors.ErrInvalidPayload), failedLine(5, ebuserrors.ErrInvalidPayload)}, failure,
			"init_attempts=5, features_seen=0x00/0x00/0x00, payload_class=invalid_payload/invalid_payload"},
		"attempt count": {[]string{failedLine(1, ebuserrors.ErrInvalidPayload), failedLine(2, ebuserrors.ErrInvalidPayload)}, failure,
			"init_attempts=2, features_seen=none, payload_class=invalid_payload/invalid_payload"},
		"feature values and order": {[]string{featureLine(1, 0x02), featureLine(2, 0x80), featureLine(3, 0x00), failedLine(4, ebuserrors.ErrTimeout)}, failure,
			"init_attempts=4, features_seen=0x02/0x80/0x00, payload_class=timeout"},
		"payload classes": {[]string{
			failedLine(1, fmt.Errorf("enh init ebus error 0x01: %w", ebuserrors.ErrInvalidPayload)),
			failedLine(2, fmt.Errorf("enh transport read closed: %w", ebuserrors.ErrTransportClosed)),
			failedLine(3, fmt.Errorf("enh init host error 0x02: %w", ebuserrors.ErrAdapterHostError)),
			failedLine(4, ebuserrors.ErrAdapterReset),
			failedLine(5, errors.New("unexpected")),
		}, failure, "init_attempts=5, features_seen=none, payload_class=invalid_payload/transport_closed/adapter_host_error/adapter_reset/failed"},
		"only features": {[]string{featureLine(1, 0x00)}, failure, "init_attempts=1, features_seen=0x00, payload_class=none"},
	} {
		out.Reset()
		trace := &ebusHandshakeTrace{}
		logger := trace.logger()
		logger.Printf("adaptermux: dialing")
		for _, line := range tc.lines {
			logger.Print(line)
		}
		got := trace.finish(tc.err)
		var handshake *ebusHandshakeFailure
		if !errors.As(got, &handshake) || got.Error() != tc.err.Error() || !errors.Is(got, tc.err) {
			t.Fatalf("%s: finish = %v; want the failure with its handshake", name, got)
		}
		if handshake.outcome.String() != tc.want {
			t.Fatalf("%s: outcome = %q; want %q", name, handshake.outcome.String(), tc.want)
		}
		wantLog := "gw adaptermux: dialing\n"
		for _, line := range tc.lines {
			wantLog += "gw " + line + "\n"
		}
		if out.String() != wantLog {
			t.Fatalf("%s: process log = %q; want every line unchanged %q", name, out.String(), wantLog)
		}
		// After the construction returned nothing more is recorded, but the
		// lines still reach the process log.
		logger.Print(failedLine(9, ebuserrors.ErrInvalidPayload))
		trace.mu.Lock()
		recorded := trace.outcome.String()
		trace.mu.Unlock()
		if recorded != tc.want || !strings.HasSuffix(out.String(), "(retrying)\n") {
			t.Fatalf("%s: a line after the construction was recorded (%q) or not logged", name, recorded)
		}
	}

	trace := &ebusHandshakeTrace{}
	trace.logger().Print("adaptermux: dial tcp: connection refused")
	if got := trace.finish(failure); got != failure {
		t.Fatalf("a failure before INIT = %v; want it unchanged", got)
	}
	success := &ebusHandshakeTrace{}
	success.logger().Print(featureLine(1, 0x00))
	if got := success.finish(nil); got != nil {
		t.Fatalf("a successful construction = %v; want nil", got)
	}
}
