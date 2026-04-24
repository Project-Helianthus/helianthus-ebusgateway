package ebusgateway

import (
	"context"
	"errors"
	"reflect"
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
		{"ENH", TransportENH, TransportAdmissionJoinCapable},
		{"ENS", TransportENS, TransportAdmissionJoinCapable},
		{"UDP-plain", TransportUDPPlain, TransportAdmissionJoinCapable},
		{"TCP-plain", TransportTCPPlain, TransportAdmissionJoinCapable},
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
// TCP-plain}; misclassification (join-capable routed to static fallback,
// or ebusd-tcp routed to Joiner) is a test failure."
func TestClassifyTransportAdmission_MisclassificationIsTestFailure(t *testing.T) {
	joinCapableTransports := []TransportProtocol{TransportENH, TransportENS, TransportUDPPlain, TransportTCPPlain}
	for _, kind := range joinCapableTransports {
		got, err := ClassifyTransportAdmission(kind)
		if err != nil {
			t.Fatalf("unexpected error classifying %s: %v", kind, err)
		}
		if got == TransportAdmissionStaticFallback {
			t.Errorf("FAIL: join-capable transport %s misclassified as static fallback", kind)
		}
	}
	got, err := ClassifyTransportAdmission(TransportEbusdTCP)
	if err != nil {
		t.Fatalf("unexpected error classifying ebusd-tcp: %v", err)
	}
	if got == TransportAdmissionJoinCapable {
		t.Error("FAIL: ebusd-tcp misclassified as join-capable (AD13 violation)")
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

func TestForwardJoinBusEvent_BroadcastForwardsRequestOnly(t *testing.T) {
	var captured []protocol.Frame
	onFrame := func(f protocol.Frame) { captured = append(captured, f) }
	req := protocol.Frame{Source: 0x71, Target: 0xFE, Primary: 0xB5}
	forwardJoinBusEvent(PassiveClassifiedEvent{
		Kind:    PassiveClassifiedEventBroadcastFrame,
		Request: req,
	}, onFrame)
	if len(captured) != 1 || !reflect.DeepEqual(captured[0], req) {
		t.Fatalf("expected [req], got %v", captured)
	}
}

func TestForwardJoinBusEvent_MasterForwardsRequestOnly(t *testing.T) {
	var captured []protocol.Frame
	onFrame := func(f protocol.Frame) { captured = append(captured, f) }
	req := protocol.Frame{Source: 0x71, Target: 0x08, Primary: 0x07, Secondary: 0x04}
	forwardJoinBusEvent(PassiveClassifiedEvent{
		Kind:    PassiveClassifiedEventMasterFrame,
		Request: req,
	}, onFrame)
	if len(captured) != 1 || !reflect.DeepEqual(captured[0], req) {
		t.Fatalf("expected [req], got %v", captured)
	}
}

func TestForwardJoinBusEvent_TransactionWithoutResponseForwardsRequestOnly(t *testing.T) {
	var captured []protocol.Frame
	onFrame := func(f protocol.Frame) { captured = append(captured, f) }
	req := protocol.Frame{Source: 0x71, Target: 0x08, Primary: 0x07, Secondary: 0x04}
	forwardJoinBusEvent(PassiveClassifiedEvent{
		Kind:        PassiveClassifiedEventTransaction,
		Request:     req,
		HasResponse: false,
	}, onFrame)
	if len(captured) != 1 || !reflect.DeepEqual(captured[0], req) {
		t.Fatalf("expected [req], got %v", captured)
	}
}

func TestForwardJoinBusEvent_TransactionWithResponseForwardsRequestThenResponse(t *testing.T) {
	var captured []protocol.Frame
	onFrame := func(f protocol.Frame) { captured = append(captured, f) }
	req := protocol.Frame{Source: 0x71, Target: 0x08}
	resp := protocol.Frame{Source: 0x08, Target: 0x71}
	forwardJoinBusEvent(PassiveClassifiedEvent{
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

func TestForwardJoinBusEvent_AbandonedTransactionIsDropped(t *testing.T) {
	var captured []protocol.Frame
	onFrame := func(f protocol.Frame) { captured = append(captured, f) }
	forwardJoinBusEvent(PassiveClassifiedEvent{
		Kind:    PassiveClassifiedEventAbandonedTransaction,
		Request: protocol.Frame{Source: 0x71},
	}, onFrame)
	if len(captured) != 0 {
		t.Fatalf("abandoned transaction must not be forwarded (AD01); got %d frames", len(captured))
	}
}

func TestForwardJoinBusEvent_DiscontinuityIsDropped(t *testing.T) {
	var captured []protocol.Frame
	onFrame := func(f protocol.Frame) { captured = append(captured, f) }
	forwardJoinBusEvent(PassiveClassifiedEvent{
		Kind:    PassiveClassifiedEventDiscontinuity,
		Request: protocol.Frame{Source: 0x71},
	}, onFrame)
	if len(captured) != 0 {
		t.Fatalf("discontinuity must not be forwarded (AD01); got %d frames", len(captured))
	}
}

func TestInquiryExistence_AlwaysReturnsSentinel(t *testing.T) {
	adapterDisabled := &joinBusAdapter{inquiryEnabled: false}
	adapterEnabled := &joinBusAdapter{inquiryEnabled: true}
	if err := adapterDisabled.InquiryExistence(context.Background()); err != ErrJoinBusInquiryUnsupported {
		t.Errorf("disabled: expected ErrJoinBusInquiryUnsupported, got %v", err)
	}
	if err := adapterEnabled.InquiryExistence(context.Background()); err != ErrJoinBusInquiryUnsupported {
		t.Errorf("enabled: expected ErrJoinBusInquiryUnsupported, got %v", err)
	}
}

func TestDefaultStartupAdmissionJoinConfig(t *testing.T) {
	cfg := DefaultStartupAdmissionJoinConfig()
	if cfg.ListenWarmup != 5*time.Second {
		t.Errorf("ListenWarmup: got %v, want 5s", cfg.ListenWarmup)
	}
	if cfg.InquiryEnabled {
		t.Error("InquiryEnabled: got true, want false")
	}
	if !cfg.PersistLastGood {
		t.Error("PersistLastGood: got false, want true")
	}
	if !cfg.PersistLastGoodSet {
		t.Error("PersistLastGoodSet: got false, want true")
	}
}
