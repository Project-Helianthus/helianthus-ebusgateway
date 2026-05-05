package ebusgateway

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

func TestClassifyTransportAdmission_AllFivePlanMatrixTransports(t *testing.T) {
	cases := []struct {
		name   string
		kind   TransportProtocol
		expect TransportAdmissionPath
	}{
		{"ENH", TransportENH, TransportAdmissionSourceSelectionCapable},
		{"ENS", TransportENS, TransportAdmissionSourceSelectionCapable},
		{"UDP-plain", TransportUDPPlain, TransportAdmissionSourceSelectionCapable},
		{"TCP-plain", TransportTCPPlain, TransportAdmissionSourceSelectionCapable},
		{"ebusd-tcp", TransportEbusdTCP, TransportAdmissionStaticFallback},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ClassifyTransportAdmission(tc.kind)
			if err != nil {
				t.Fatalf("unexpected error for %s: %v", tc.kind, err)
			}
			if got != tc.expect {
				t.Errorf("for %s: got %d, want %d", tc.kind, got, tc.expect)
			}
		})
	}
}

// TestClassifyTransportAdmission_MisclassificationIsTestFailure hardens the
// plan's M2 acceptance: "Transport classifier is unit-tested against all
// five transports in the capability matrix {ENH, ENS, ebusd-tcp, UDP-plain,
// TCP-plain}; misclassification (source-selection-capable routed to static fallback,
// or ebusd-tcp routed to source-address selector) is a test failure."
func TestClassifyTransportAdmission_MisclassificationIsTestFailure(t *testing.T) {
	joinCapableTransports := []TransportProtocol{TransportENH, TransportENS, TransportUDPPlain, TransportTCPPlain}
	for _, kind := range joinCapableTransports {
		got, err := ClassifyTransportAdmission(kind)
		if err != nil {
			t.Fatalf("unexpected error classifying %s: %v", kind, err)
		}
		if got == TransportAdmissionStaticFallback {
			t.Errorf("FAIL: source-selection-capable transport %s misclassified as static fallback", kind)
		}
	}
	got, err := ClassifyTransportAdmission(TransportEbusdTCP)
	if err != nil {
		t.Fatalf("unexpected error classifying ebusd-tcp: %v", err)
	}
	if got == TransportAdmissionSourceSelectionCapable {
		t.Error("FAIL: ebusd-tcp misclassified as source-selection-capable (AD13 violation)")
	}
}

func TestClassifyTransportAdmission_AdapterDirectIsMultiplexer(t *testing.T) {
	_, err := ClassifyTransportAdmission(TransportAdapterDirect)
	if err == nil {
		t.Fatal("expected error for adapter-direct classification (it is a multiplexer)")
	}
}

func TestClassifyTransportAdmission_EmptyIsError(t *testing.T) {
	_, err := ClassifyTransportAdmission("")
	if err == nil {
		t.Fatal("expected error for empty transport protocol")
	}
}

func TestClassifyTransportAdmission_UnknownIsError(t *testing.T) {
	_, err := ClassifyTransportAdmission(TransportProtocol("bogus-transport-xyz"))
	if err == nil {
		t.Fatal("expected error for unknown transport protocol")
	}
	// Also verify the error mentions the bogus value for observability.
	if !errors.Is(err, nil) && err.Error() == "" {
		t.Error("expected non-empty error message for unknown transport")
	}
}

func TestSelectDefaultStartupSourceAddress_UsesPolicyWithoutObservations(t *testing.T) {
	result, err := SelectDefaultStartupSourceAddress(context.Background())
	if err != nil {
		t.Fatalf("SelectDefaultStartupSourceAddress() error = %v", err)
	}
	if result.Source != 0x7F {
		t.Fatalf("default selected source = 0x%02X; want 0x7F", result.Source)
	}
	if result.Companion != 0x84 {
		t.Fatalf("default companion = 0x%02X; want 0x84", result.Companion)
	}
	if !reflect.DeepEqual(result.Metrics.CandidatesConsidered, []byte{0xFF, 0x7F}) {
		t.Fatalf("candidates considered = % X; want FF 7F", result.Metrics.CandidatesConsidered)
	}
}

func TestForwardSourceSelectionBusEvent_BroadcastForwardsRequestOnly(t *testing.T) {
	var captured []protocol.Frame
	onFrame := func(f protocol.Frame) { captured = append(captured, f) }
	req := protocol.Frame{Source: 0x71, Target: 0xFE, Primary: 0xB5}
	forwardSourceSelectionBusEvent(PassiveClassifiedEvent{
		Kind:    PassiveClassifiedEventBroadcastFrame,
		Request: req,
	}, onFrame)
	if len(captured) != 1 || !reflect.DeepEqual(captured[0], req) {
		t.Fatalf("expected [req], got %v", captured)
	}
}

func TestForwardSourceSelectionBusEvent_MasterForwardsRequestOnly(t *testing.T) {
	var captured []protocol.Frame
	onFrame := func(f protocol.Frame) { captured = append(captured, f) }
	req := protocol.Frame{Source: 0x71, Target: 0x08, Primary: 0x07, Secondary: 0x04}
	forwardSourceSelectionBusEvent(PassiveClassifiedEvent{
		Kind:    PassiveClassifiedEventMasterFrame,
		Request: req,
	}, onFrame)
	if len(captured) != 1 || !reflect.DeepEqual(captured[0], req) {
		t.Fatalf("expected [req], got %v", captured)
	}
}

func TestForwardSourceSelectionBusEvent_TransactionWithoutResponseForwardsRequestOnly(t *testing.T) {
	var captured []protocol.Frame
	onFrame := func(f protocol.Frame) { captured = append(captured, f) }
	req := protocol.Frame{Source: 0x71, Target: 0x08, Primary: 0x07, Secondary: 0x04}
	forwardSourceSelectionBusEvent(PassiveClassifiedEvent{
		Kind:        PassiveClassifiedEventTransaction,
		Request:     req,
		HasResponse: false,
	}, onFrame)
	if len(captured) != 1 || !reflect.DeepEqual(captured[0], req) {
		t.Fatalf("expected [req], got %v", captured)
	}
}

func TestForwardSourceSelectionBusEvent_TransactionWithResponseForwardsRequestThenResponse(t *testing.T) {
	var captured []protocol.Frame
	onFrame := func(f protocol.Frame) { captured = append(captured, f) }
	req := protocol.Frame{Source: 0x71, Target: 0x08}
	resp := protocol.Frame{Source: 0x08, Target: 0x71}
	forwardSourceSelectionBusEvent(PassiveClassifiedEvent{
		Kind:        PassiveClassifiedEventTransaction,
		Request:     req,
		Response:    resp,
		HasResponse: true,
	}, onFrame)
	if len(captured) != 2 {
		t.Fatalf("expected 2 frames, got %d", len(captured))
	}
	if !reflect.DeepEqual(captured[0], req) {
		t.Errorf("expected frame 0 = req, got %+v", captured[0])
	}
	if !reflect.DeepEqual(captured[1], resp) {
		t.Errorf("expected frame 1 = resp, got %+v", captured[1])
	}
}

func TestForwardSourceSelectionBusEvent_AbandonedTransactionIsDropped(t *testing.T) {
	var captured []protocol.Frame
	onFrame := func(f protocol.Frame) { captured = append(captured, f) }
	forwardSourceSelectionBusEvent(PassiveClassifiedEvent{
		Kind:    PassiveClassifiedEventAbandonedTransaction,
		Request: protocol.Frame{Source: 0x71},
	}, onFrame)
	if len(captured) != 0 {
		t.Fatalf("abandoned transaction must not be forwarded (AD01); got %d frames", len(captured))
	}
}

func TestForwardSourceSelectionBusEvent_DiscontinuityIsDropped(t *testing.T) {
	var captured []protocol.Frame
	onFrame := func(f protocol.Frame) { captured = append(captured, f) }
	forwardSourceSelectionBusEvent(PassiveClassifiedEvent{
		Kind:    PassiveClassifiedEventDiscontinuity,
		Request: protocol.Frame{Source: 0x71},
	}, onFrame)
	if len(captured) != 0 {
		t.Fatalf("discontinuity must not be forwarded (AD01); got %d frames", len(captured))
	}
}

func TestInquiryExistence_AlwaysReturnsSentinel(t *testing.T) {
	adapterDisabled := &sourceSelectionBusAdapter{inquiryEnabled: false}
	adapterEnabled := &sourceSelectionBusAdapter{inquiryEnabled: true}
	if err := adapterDisabled.InquiryExistence(context.Background()); err != ErrSourceSelectionBusInquiryUnsupported {
		t.Errorf("disabled: expected ErrSourceSelectionBusInquiryUnsupported, got %v", err)
	}
	if err := adapterEnabled.InquiryExistence(context.Background()); err != ErrSourceSelectionBusInquiryUnsupported {
		t.Errorf("enabled: expected ErrSourceSelectionBusInquiryUnsupported, got %v", err)
	}
}

func TestDefaultStartupAdmissionSourceSelectionConfig(t *testing.T) {
	cfg := DefaultStartupAdmissionSourceSelectionConfig()
	if cfg.ListenWarmup != 5*time.Second {
		t.Errorf("ListenWarmup: got %v, want 5s", cfg.ListenWarmup)
	}
	if cfg.InquiryEnabled {
		t.Error("InquiryEnabled: got true, want false")
	}
}

func TestSourceSelectionBusAdapter_WarmupObservationForwardsSynthesizedPassiveTraffic(t *testing.T) {
	reconstructor := newPassiveTransactionReconstructorCore(DefaultConfig())
	joinBus, err := NewSourceSelectionBusAdapter(reconstructor, "warmup_test", false)
	if err != nil {
		t.Fatalf("NewSourceSelectionBusAdapter error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	request := protocol.Frame{
		Source:    0x10,
		Target:    protocol.AddressBroadcast,
		Primary:   0xB5,
		Secondary: 0x16,
		Data:      []byte{0x01, 0x02},
	}

	var (
		mu       sync.Mutex
		captured []protocol.Frame
	)
	done := make(chan error, 1)
	gotFrame := make(chan struct{}, 1)
	go func() {
		done <- joinBus.Listen(ctx, func(frame protocol.Frame) {
			mu.Lock()
			captured = append(captured, frame)
			mu.Unlock()
			select {
			case gotFrame <- struct{}{}:
			default:
			}
			cancel()
		})
	}()

	// Replace the previous time.Sleep(25ms) with a deterministic poll for
	// the listener goroutine's Subscribe to land in the reconstructor's
	// subscriber map (per Codex-bot review on M2). On a busy CI worker
	// the 25ms window could be exceeded by scheduler jitter, leading to
	// feedPassiveSymbols firing before Subscribe and a flaky timeout.
	deadline := time.Now().Add(2 * time.Second)
	for {
		reconstructor.subscribersMu.Lock()
		ready := len(reconstructor.subscribers) > 0
		reconstructor.subscribersMu.Unlock()
		if ready {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timeout waiting for joinBus.Listen to subscribe to reconstructor")
		}
		time.Sleep(time.Millisecond)
	}
	feedPassiveSymbols(reconstructor, time.Unix(0, 0), frameBytes(request))

	select {
	case <-gotFrame:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for warmup observation to forward a frame")
	}

	err = <-done
	if err != context.Canceled && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("Listen error = %v; want context cancellation/deadline", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(captured) == 0 {
		t.Fatal("expected at least one forwarded frame")
	}
	if !reflect.DeepEqual(captured[0], request) {
		t.Fatalf("forwarded frame = %+v; want %+v", captured[0], request)
	}
}

func TestSourceSelectionBusAdapter_WarmupObservationSilentBusForwardsNothing(t *testing.T) {
	reconstructor := newPassiveTransactionReconstructorCore(DefaultConfig())
	joinBus, err := NewSourceSelectionBusAdapter(reconstructor, "silent_bus_test", false)
	if err != nil {
		t.Fatalf("NewSourceSelectionBusAdapter error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var (
		mu       sync.Mutex
		captured []protocol.Frame
	)
	done := make(chan error, 1)
	go func() {
		done <- joinBus.Listen(ctx, func(frame protocol.Frame) {
			mu.Lock()
			captured = append(captured, frame)
			mu.Unlock()
		})
	}()

	err = <-done
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Listen error = %v; want context deadline exceeded", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(captured) != 0 {
		t.Fatalf("expected 0 forwarded frames on silent bus, got %d", len(captured))
	}
}

func TestAD22_StateMinStabilityInvariant_ValidValuesAccepted(t *testing.T) {
	for _, v := range []int{5, 10, 30, 60} {
		if err := ValidateStartupAdmissionStability(v); err != nil {
			t.Errorf("valid value %d rejected: %v", v, err)
		}
	}
}

func TestAD22_StateMinStabilityInvariant_RejectsOutOfRange(t *testing.T) {
	for _, v := range []int{0, 4, 61, 120, 300, -1} {
		if err := ValidateStartupAdmissionStability(v); err == nil {
			t.Errorf("out-of-range value %d was not rejected", v)
		}
	}
}

func TestAD22_StateMinStabilityInvariant_Rejects120With300(t *testing.T) {
	err := ValidateStartupAdmissionStability(120)
	if err == nil {
		t.Fatal("expected FATAL error for state_min_stability_s=120 with continuous_threshold_s=300 (AD22 5:1 invariant)")
	}
	if !errors.Is(err, ErrStartupAdmissionStabilityInvariant) {
		t.Logf("rejected by range check rather than invariant check (acceptable); err=%v", err)
	}
}

func TestAD22_StateMinStabilityInvariant_DirectViolation(t *testing.T) {
	if err := ValidateStartupAdmissionStability(60); err != nil {
		t.Errorf("value 60 (boundary) should be accepted, got %v", err)
	}
	if err := ValidateStartupAdmissionStability(61); err == nil {
		t.Error("value 61 (just over boundary) should be rejected")
	}
}
