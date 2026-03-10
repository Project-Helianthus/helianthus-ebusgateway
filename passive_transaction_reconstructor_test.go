package ebusgateway

import (
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

func TestPassiveTransactionReconstructor_ClassifiesBroadcast(t *testing.T) {
	t.Parallel()

	reconstructor := newPassiveTransactionReconstructorCore(DefaultConfig())
	subscription, err := reconstructor.Subscribe("test", PassiveSubscriberCritical, 8)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	frame := protocol.Frame{
		Source:    0x10,
		Target:    protocol.AddressBroadcast,
		Primary:   0xB5,
		Secondary: 0x16,
		Data:      []byte{0x01, 0x02},
	}
	feedPassiveSymbols(reconstructor, time.Unix(0, 0), frameBytes(frame))

	event := requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventBroadcastFrame)
	if !event.HasRequest {
		t.Fatal("broadcast event missing request")
	}
	if event.Request.Target != protocol.AddressBroadcast {
		t.Fatalf("request target = 0x%02x; want broadcast", event.Request.Target)
	}
}

func TestPassiveTransactionReconstructor_ClassifiesMasterMaster(t *testing.T) {
	t.Parallel()

	reconstructor := newPassiveTransactionReconstructorCore(DefaultConfig())
	subscription, err := reconstructor.Subscribe("test", PassiveSubscriberCritical, 8)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	frame := protocol.Frame{
		Source:    0x30,
		Target:    0x10,
		Primary:   0x01,
		Secondary: 0x02,
		Data:      []byte{0x03},
	}
	payload := append(frameBytes(frame), protocol.SymbolAck, protocol.SymbolSyn)
	feedPassiveSymbols(reconstructor, time.Unix(0, 0), payload)

	event := requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventMasterFrame)
	if event.FrameType != protocol.FrameTypeInitiatorInitiator {
		t.Fatalf("frame type = %v; want initiator-initiator", event.FrameType)
	}
}

func TestPassiveTransactionReconstructor_ClassifiesDirectTransaction(t *testing.T) {
	t.Parallel()

	reconstructor := newPassiveTransactionReconstructorCore(DefaultConfig())
	subscription, err := reconstructor.Subscribe("test", PassiveSubscriberCritical, 8)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	request := protocol.Frame{
		Source:    0x10,
		Target:    0x08,
		Primary:   0xB5,
		Secondary: 0x09,
		Data:      []byte{0x01},
	}
	payload := append(frameBytes(request), protocol.SymbolAck)
	payload = append(payload, responseSegmentBytes([]byte{0x11, 0x55})...)
	payload = append(payload, protocol.SymbolAck, protocol.SymbolSyn)
	feedPassiveSymbols(reconstructor, time.Unix(0, 0), payload)

	event := requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventTransaction)
	if !event.HasResponse {
		t.Fatal("transaction event missing response")
	}
	if got, want := event.Response.Source, request.Target; got != want {
		t.Fatalf("response source = 0x%02x; want 0x%02x", got, want)
	}
	if got, want := len(event.Response.Data), 2; got != want {
		t.Fatalf("response data len = %d; want %d", got, want)
	}
}

func TestPassiveTransactionReconstructor_NoResponseBecomesAbandoned(t *testing.T) {
	t.Parallel()

	reconstructor := newPassiveTransactionReconstructorCore(DefaultConfig())
	subscription, err := reconstructor.Subscribe("test", PassiveSubscriberCritical, 8)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	request := protocol.Frame{
		Source:    0x10,
		Target:    0x08,
		Primary:   0xB5,
		Secondary: 0x09,
		Data:      []byte{0x01},
	}
	payload := append(frameBytes(request), protocol.SymbolAck, protocol.SymbolSyn)
	feedPassiveSymbols(reconstructor, time.Unix(0, 0), payload)

	event := requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventAbandonedTransaction)
	if event.AbandonReason != PassiveAbandonReasonNoResponse {
		t.Fatalf("abandon reason = %q; want %q", event.AbandonReason, PassiveAbandonReasonNoResponse)
	}
}

func TestPassiveTransactionReconstructor_ResetAbandonsInFlight(t *testing.T) {
	t.Parallel()

	reconstructor := newPassiveTransactionReconstructorCore(DefaultConfig())
	subscription, err := reconstructor.Subscribe("test", PassiveSubscriberCritical, 8)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	request := protocol.Frame{
		Source:    0x10,
		Target:    0x08,
		Primary:   0xB5,
		Secondary: 0x09,
		Data:      []byte{0x01},
	}
	base := time.Unix(0, 0)
	feedPassiveSymbols(reconstructor, base, append(frameBytes(request), protocol.SymbolAck))
	reconstructor.OnPassiveTapEvent(PassiveTapEvent{
		Kind:       PassiveTapEventReset,
		ObservedAt: base.Add(100 * time.Millisecond),
	})

	abandoned := requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventAbandonedTransaction)
	if abandoned.AbandonReason != PassiveAbandonReasonTransportReset {
		t.Fatalf("abandon reason = %q; want %q", abandoned.AbandonReason, PassiveAbandonReasonTransportReset)
	}
	discontinuity := requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventDiscontinuity)
	if discontinuity.DiscontinuityReason != PassiveDiscontinuityTransportReset {
		t.Fatalf("discontinuity reason = %q; want %q", discontinuity.DiscontinuityReason, PassiveDiscontinuityTransportReset)
	}
}

func feedPassiveSymbols(reconstructor *PassiveTransactionReconstructor, start time.Time, symbols []byte) {
	for index, symbol := range symbols {
		reconstructor.OnPassiveTapEvent(PassiveTapEvent{
			Kind:       PassiveTapEventSymbol,
			Symbol:     symbol,
			ObservedAt: start.Add(time.Duration(index) * 10 * time.Millisecond),
		})
	}
}

func requirePassiveClassifiedEvent(t *testing.T, subscription *PassiveClassifiedSubscription, kind PassiveClassifiedEventKind) PassiveClassifiedEvent {
	t.Helper()

	timeout := time.NewTimer(time.Second)
	defer timeout.Stop()

	for {
		select {
		case event, ok := <-subscription.Events():
			if !ok {
				t.Fatal("subscription channel closed before event arrived")
			}
			if event.Kind == kind {
				return event
			}
		case <-timeout.C:
			t.Fatalf("timeout waiting for passive classified event kind %d", kind)
		}
	}
}

func responseSegmentBytes(data []byte) []byte {
	raw := make([]byte, 0, 2+len(data))
	raw = append(raw, byte(len(data)))
	raw = append(raw, data...)
	raw = append(raw, protocol.CRC(raw))
	return raw
}
