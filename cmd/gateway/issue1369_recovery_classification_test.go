package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// Issue #1369: under the persistent policy only a failure that retrying
// cannot heal ends the recovery. A dial failure, a timeout, a transport close
// and (since #1373) an INIT the endpoint rejects are retryable; a
// configuration rejected before any dial, a lifecycle identity mismatch and a
// stale or unavailable runtime (an explicit stop or restart superseded the
// attempt) are terminal, each with its own reason.
func TestIssue1369OnlyNonHealingFailuresAreTerminal(t *testing.T) {
	refusedDial := issue1369RefusedDial(t)
	for name, tc := range map[string]struct {
		err       error
		retryable bool
		reason    drivermanager.ReasonCode
	}{
		"dial refused":        {fmt.Errorf("adaptermux: initial connect: %w", refusedDial), true, drivermanager.ReasonDependencyUnavailable},
		"dial timeout":        {&net.OpError{Op: "dial", Net: "tcp", Err: issue1367Timeout{}}, true, drivermanager.ReasonDependencyUnavailable},
		"handshake timeout":   {fmt.Errorf("adaptermux: INIT handshake failed: %w", ebuserrors.ErrTimeout), true, drivermanager.ReasonDependencyUnavailable},
		"canceled attempt":    {context.DeadlineExceeded, true, drivermanager.ReasonDependencyUnavailable},
		"transport closed":    {fmt.Errorf("enh: %w", ebuserrors.ErrTransportClosed), true, drivermanager.ReasonDependencyUnavailable},
		"connection loss":     {&ebusConnectionLoss{reason: ebusLossAdapterConnection}, true, drivermanager.ReasonDependencyUnavailable},
		"proxy listen":        {&net.OpError{Op: "listen", Net: "tcp", Err: errors.New("address already in use")}, true, drivermanager.ReasonDependencyUnavailable},
		"admission gate":      {fmt.Errorf("ebus control: replacement admission gate: %w", context.DeadlineExceeded), true, drivermanager.ReasonDependencyUnavailable},
		"endpoint rejected":   {rejectEBusConfig(fmt.Errorf("invalid gateway transport endpoint: %w: %w", ebuserrors.ErrInvalidPayload, errors.New("missing scheme"))), false, drivermanager.ReasonConfigInvalid},
		"INIT rejected":       {fmt.Errorf("gateway transport init failed: enh init ebus error 0x01: %w", ebuserrors.ErrInvalidPayload), true, drivermanager.ReasonDependencyUnavailable},
		"identity mismatch":   {errEBusControlHubNotLive, false, drivermanager.ReasonInternalError},
		"identity wrapped":    {fmt.Errorf("hold: %w", errEBusControlHubNotLive), false, drivermanager.ReasonInternalError},
		"stale correlation":   {fmt.Errorf("%w: %w", drivermanager.ErrUnavailable, errEBusControlStale), false, drivermanager.ReasonProviderUnavailable},
		"runtime unavailable": {transport.ErrDriverUnavailable, false, drivermanager.ReasonProviderUnavailable},
	} {
		failure := classifyEBusDriverError(tc.err)
		if failure.Reason.Retryable != tc.retryable || failure.Reason.Code != tc.reason {
			t.Fatalf("%s: classified %s retryable=%t; want %s retryable=%t", name, failure.Reason.Code, failure.Reason.Retryable, tc.reason, tc.retryable)
		}
	}
}

func issue1369RefusedDial(t *testing.T) error {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	conn, dialErr := net.Dial("tcp", address)
	if dialErr == nil {
		_ = conn.Close()
		t.Fatal("dial to a closed loopback port succeeded")
	}
	return fmt.Errorf("dial tcp/%s: %w", address, dialErr)
}

// The logged cause text is the innermost cause of the failure. The wrapping
// that names the endpoint is dropped, anything address-like left in the text
// is redacted, and the text stays one bounded line.
func TestIssue1369RecoveryErrorTextNeverNamesTheEndpoint(t *testing.T) {
	refusedDial := issue1369RefusedDial(t)
	host, port, _ := net.SplitHostPort(strings.TrimPrefix(strings.SplitN(refusedDial.Error(), ": ", 2)[0], "dial tcp/"))
	for name, tc := range map[string]struct {
		err    error
		want   string
		absent []string
	}{
		"refused dial": {fmt.Errorf("adaptermux: initial connect: %w", refusedDial), "connect: connection refused", []string{host, port}},
		"unknown host": {fmt.Errorf("dial tcp/adapter.invalid:9999: %w", &net.OpError{Op: "dial", Net: "tcp", Err: &net.DNSError{Err: "no such host", Name: "adapter.invalid"}}),
			"lookup: no such host", []string{"adapter.invalid", "9999"}},
		"dial timeout": {&net.OpError{Op: "dial", Net: "tcp", Addr: &net.TCPAddr{IP: net.ParseIP("192.0.2.10"), Port: 9999}, Err: issue1367Timeout{}},
			"i/o timeout", []string{"192.0.2.10", "9999"}},
		"rejected handshake": {fmt.Errorf("adaptermux: INIT handshake failed after 5 attempts: enh init ebus error 0x01: %w", ebuserrors.ErrInvalidPayload),
			ebuserrors.ErrInvalidPayload.Error(), nil},
		"rejected endpoint": {fmt.Errorf("invalid gateway transport endpoint: %w: %w", ebuserrors.ErrInvalidPayload, fmt.Errorf("parse %q", "enh://192.0.2.10:9999")),
			ebuserrors.ErrInvalidPayload.Error(), []string{"192.0.2.10"}},
		"identity mismatch": {errEBusControlHubNotLive, errEBusControlHubNotLive.Error(), nil},
		"address-like text": {errors.New("boom at adapter.invalid:9999 via enh://user@192.0.2.10:3333 and [2001:db8::1]:80 or 192.0.2.11"),
			"", []string{"adapter.invalid:9999", "192.0.2.10", "192.0.2.11", "2001:db8", "user@", "3333"}},
		"multi-line text": {errors.New("first line\nsecond line"), "first line second line", nil},
	} {
		got := ebusRecoveryErrorText(tc.err)
		if tc.want != "" && got != tc.want {
			t.Fatalf("%s: text = %q; want %q", name, got, tc.want)
		}
		for _, fragment := range tc.absent {
			if fragment != "" && strings.Contains(got, fragment) {
				t.Fatalf("%s: text %q names %q", name, got, fragment)
			}
		}
		if strings.ContainsAny(got, "\n\r") || len(got) > ebusRecoveryErrorTextLimit {
			t.Fatalf("%s: text %q is not one bounded line", name, got)
		}
	}
	if got := ebusRecoveryErrorText(errors.New(strings.Repeat("x", 4*ebusRecoveryErrorTextLimit))); len(got) != ebusRecoveryErrorTextLimit {
		t.Fatalf("long text kept %d bytes; want %d", len(got), ebusRecoveryErrorTextLimit)
	}
	if got := ebusRecoveryErrorText(nil); got != "" {
		t.Fatalf("nil error text = %q", got)
	}
}

// A failed start outside a recovery series is logged with its error class
// and cause and counted until a generation is admitted or the driver is
// explicitly stopped.
func TestIssue1369StartAttemptFailuresAreLoggedWithTheirCause(t *testing.T) {
	sink := &issue1367LineSink{}
	recovery := &ebusRecoveryLog{budget: 5, persistent: ebusDriverRetryPolicy.Persistent, logf: sink.logf}
	refused := issue1369RefusedDial(t)
	recovery.freshStart()
	recovery.startFinished(refused)
	recovery.freshStart()
	recovery.startFinished(fmt.Errorf("gateway transport init failed: %w", ebuserrors.ErrTimeout))
	recovery.freshStart()
	recovery.startFinished(nil)
	recovery.admitted(1)
	recovery.freshStart()
	recovery.startFinished(refused)
	recovery.reset()
	recovery.startFinished(refused)

	want := []string{
		`ebus.primary: start attempt 1 failed (class=dial_refused, error="connect: connection refused")`,
		`ebus.primary: start attempt 2 failed (class=handshake_timeout, error="ebus: no response within timeout window")`,
		`ebus.primary: start attempt 1 failed (class=dial_refused, error="connect: connection refused")`,
		`ebus.primary: start attempt 1 failed (class=dial_refused, error="connect: connection refused")`,
	}
	got := sink.snapshot()
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("start lines:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}
