package main

// Issue #1375: the recovery view names the failure that scheduled a retry by
// one of four classes. Each internal error category of the recovery log maps
// to exactly one of them, a lost connection maps from its closed trigger, and
// the cause text is the log's redacted text. DriverManager carries both to the
// schedule it publishes.

import (
	"context"
	"errors"
	"fmt"
	"net"
	"syscall"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol/contractcheck"
	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// issue1375Categories is every category ebusRecoveryFailureCategory returns,
// with the class it maps to.
var issue1375Categories = map[string]drivermanager.FailureClass{
	"dial_refused":        drivermanager.FailureDialFailed,
	"dial_unreachable":    drivermanager.FailureDialFailed,
	"dial_failed":         drivermanager.FailureDialFailed,
	"dial_timeout":        drivermanager.FailureTimeout,
	"handshake_timeout":   drivermanager.FailureTimeout,
	"transport_closed":    drivermanager.FailureTransportClosed,
	"invalid_payload":     drivermanager.FailureOther,
	"proxy_listen_failed": drivermanager.FailureOther,
	"runtime_unavailable": drivermanager.FailureOther,
	"start_failed":        drivermanager.FailureOther,
	"canceled":            drivermanager.FailureOther,
	"drain_timeout":       drivermanager.FailureOther,
	"close_unconfirmed":   drivermanager.FailureOther,
	"config_rejected":     drivermanager.FailureOther,
	"none":                drivermanager.FailureOther,
}

func TestIssue1375EveryErrorCategoryMapsToOneClass(t *testing.T) {
	for category, want := range issue1375Categories {
		if got := ebusCategoryFailureClass(category); got != want {
			t.Errorf("category %q maps to %q; want %q", category, got, want)
		}
	}
	for _, unknown := range []string{"", "new_category", "DIAL_FAILED", "dial", "timeout"} {
		if got := ebusCategoryFailureClass(unknown); got != drivermanager.FailureOther {
			t.Errorf("unknown category %q maps to %q; want OTHER", unknown, got)
		}
	}
}

func TestIssue1375FailureClassOfAnAttempt(t *testing.T) {
	refused, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := refused.Addr().String()
	_ = refused.Close()
	_, dialErr := net.Dial("tcp", address)
	if dialErr == nil {
		t.Fatal("dial to a closed loopback port succeeded")
	}
	for _, tc := range []struct {
		name     string
		err      error
		category string
		want     drivermanager.FailureClass
	}{
		{"dial refused", fmt.Errorf("adaptermux: initial connect: dial tcp/%s: %w", address, dialErr), "dial_refused", drivermanager.FailureDialFailed},
		{"dial unreachable host", &net.OpError{Op: "dial", Net: "tcp", Err: &net.OpError{Op: "connect", Err: syscall.EHOSTUNREACH}}, "dial_unreachable", drivermanager.FailureDialFailed},
		{"dial unreachable network", &net.OpError{Op: "dial", Net: "tcp", Err: &net.OpError{Op: "connect", Err: syscall.ENETUNREACH}}, "dial_unreachable", drivermanager.FailureDialFailed},
		{"dial other", &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("other")}, "dial_failed", drivermanager.FailureDialFailed},
		{"dial timeout", &net.OpError{Op: "dial", Net: "tcp", Err: issue1367Timeout{}}, "dial_timeout", drivermanager.FailureTimeout},
		{"INIT timeout", fmt.Errorf("adaptermux: INIT handshake failed: %w", ebuserrors.ErrTimeout), "handshake_timeout", drivermanager.FailureTimeout},
		{"transport closed", fmt.Errorf("enh transport read closed: %w", ebuserrors.ErrTransportClosed), "transport_closed", drivermanager.FailureTransportClosed},
		{"adapter reset", fmt.Errorf("adaptermux: INIT: %w", ebuserrors.ErrAdapterReset), "start_failed", drivermanager.FailureTransportClosed},
		{"deadline exceeded", fmt.Errorf("attempt: %w", context.DeadlineExceeded), "canceled", drivermanager.FailureTimeout},
		{"canceled", fmt.Errorf("attempt: %w", context.Canceled), "canceled", drivermanager.FailureOther},
		{"invalid payload", fmt.Errorf("x: %w", ebuserrors.ErrInvalidPayload), "invalid_payload", drivermanager.FailureOther},
		{"listen", &net.OpError{Op: "listen", Net: "tcp", Err: errors.New("in use")}, "proxy_listen_failed", drivermanager.FailureOther},
		{"runtime unavailable", transport.ErrDriverUnavailable, "runtime_unavailable", drivermanager.FailureOther},
		{"unclassified", errors.New("boom"), "start_failed", drivermanager.FailureOther},
		{"nil", nil, "none", drivermanager.FailureOther},
	} {
		if got := ebusRecoveryFailureCategory(tc.err); got != tc.category {
			t.Fatalf("%s: category = %q; want %q (the fixture drifted)", tc.name, got, tc.category)
		}
		if got := ebusFailureClass(tc.err); got != tc.want {
			t.Errorf("%s: class = %q; want %q", tc.name, got, tc.want)
		}
		if want, ok := issue1375Categories[tc.category]; !ok || (tc.category != "canceled" && tc.category != "start_failed" && want != tc.want) {
			t.Errorf("%s: category %q is not in the class table or disagrees with it", tc.name, tc.category)
		}
	}
}

func TestIssue1375FailureCauseOfAnAttemptNamesNoEndpoint(t *testing.T) {
	refused, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := refused.Addr().String()
	_ = refused.Close()
	_, dialErr := net.Dial("tcp", address)
	host, port, _ := net.SplitHostPort(address)
	detail := ebusFailureDetail(fmt.Errorf("adaptermux: initial connect: dial tcp/%s: %w", address, dialErr))
	if detail.Class != drivermanager.FailureDialFailed || detail.Cause != "connect: connection refused" {
		t.Fatalf("detail = %+v; want DIAL_FAILED with the socket verdict only", detail)
	}
	for _, text := range []string{host, port} {
		if text != "" && (detail.Cause == text) {
			t.Fatalf("cause %q is the endpoint", detail.Cause)
		}
	}
	for _, err := range []error{
		errors.New("boom at adapter.invalid:9999"),
		fmt.Errorf("x: %w", &net.DNSError{Err: "no such host", Name: "adapter.invalid", IsNotFound: true}),
		errors.New("dial adapter-host: failed"),
		errors.New("open /dev/ttyUSB0: no such device"),
	} {
		cause := ebusFailureDetail(err).Cause
		if cause == "" || len(cause) > 160 {
			t.Errorf("cause of %v = %q", err, cause)
		}
	}
}

func TestIssue1375LossDetailNamesItsClosedTrigger(t *testing.T) {
	for reason, want := range map[string]drivermanager.FailureDetail{
		ebusLossAdapterConnection: {Class: drivermanager.FailureTransportClosed, Cause: "connection lost (adapter_connection_lost)"},
		ebusLossTransportClosed:   {Class: drivermanager.FailureTransportClosed, Cause: "connection lost (transport_closed)"},
		ebusLossProxyListener:     {Class: drivermanager.FailureOther, Cause: "connection lost (proxy_listener_failed)"},
		ebusLossProviderFailed:    {Class: drivermanager.FailureOther, Cause: "connection lost (provider_failed)"},
	} {
		if got := ebusLossDetail(reason); got != want {
			t.Errorf("loss %q: detail = %+v; want %+v", reason, got, want)
		}
		if !contractcheck.CauseValid(ebusLossDetail(reason).Cause) {
			t.Errorf("loss %q: cause %q is refused by the contract", reason, ebusLossDetail(reason).Cause)
		}
	}
}

// The classifier hands the detail to DriverManager only with a retryable
// failure: a terminal class and an unavailable runtime schedule nothing.
func TestIssue1375ClassifierAttachesTheDetailToRetryableFailuresOnly(t *testing.T) {
	retryable := classifyEBusDriverError(fmt.Errorf("adaptermux: INIT handshake failed: %w", ebuserrors.ErrTimeout))
	if !retryable.Reason.Retryable || retryable.Detail.Class != drivermanager.FailureTimeout || retryable.Detail.Cause == "" {
		t.Fatalf("retryable failure = %+v; want TIMEOUT with a cause", retryable)
	}
	for name, err := range map[string]error{
		"configuration rejected": rejectEBusConfig(errors.New("bad endpoint")),
		"runtime unavailable":    transport.ErrDriverUnavailable,
		"hub not live":           errEBusControlHubNotLive,
	} {
		if failure := classifyEBusDriverError(err); failure.Reason.Retryable || failure.Detail != (drivermanager.FailureDetail{}) {
			t.Errorf("%s: failure = %+v; want terminal without a detail", name, failure)
		}
	}
}
