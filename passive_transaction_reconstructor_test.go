package ebusgateway

import (
	"testing"
	"time"

	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
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

func TestPassiveTransactionReconstructor_ReadTimeoutHonorsWatchdog(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.PassiveTransactionWatchdog = 250 * time.Millisecond
	reconstructor := newPassiveTransactionReconstructorCore(cfg)
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
		Kind:       PassiveTapEventReadTimeout,
		ObservedAt: base.Add(100 * time.Millisecond),
		Err:        ebuserrors.ErrTimeout,
	})
	assertNoPassiveClassifiedEvent(t, subscription, 25*time.Millisecond)

	reconstructor.OnPassiveTapEvent(PassiveTapEvent{
		Kind:       PassiveTapEventReadTimeout,
		ObservedAt: base.Add(400 * time.Millisecond),
		Err:        ebuserrors.ErrTimeout,
	})

	abandoned := requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventAbandonedTransaction)
	if abandoned.AbandonReason != PassiveAbandonReasonNoProgress {
		t.Fatalf("abandon reason = %q; want %q", abandoned.AbandonReason, PassiveAbandonReasonNoProgress)
	}
}

type testLocalSnapshotter struct {
	address byte
	known   bool
}

func (s testLocalSnapshotter) LocalAddressSnapshot() LocalAddressSnapshot {
	return LocalAddressSnapshot{Address: s.address, Known: s.known}
}

func TestReconstructorSelfEchoOnParseFailure(t *testing.T) {
	t.Parallel()

	reconstructor := newPassiveTransactionReconstructorCore(DefaultConfig())
	reconstructor.SetLocalAddressSnapshotter(testLocalSnapshotter{address: 0x71, known: true})
	subscription, err := reconstructor.Subscribe("test", PassiveSubscriberCritical, 8)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	// Build a raw request that starts with local address 0x71 but has invalid CRC.
	// Format: [source=0x71, target=0x08, primary=0xB5, secondary=0x09, dataLen=1, data=0x01, badCRC=0xFF]
	raw := []byte{0x71, 0x08, 0xB5, 0x09, 0x01, 0x01, 0xFF}
	payload := append(raw, protocol.SymbolSyn)
	feedPassiveSymbols(reconstructor, time.Unix(0, 0), payload)

	event := requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventAbandonedTransaction)
	if event.AbandonReason != PassiveAbandonReasonSelfEcho {
		t.Fatalf("abandon reason = %q; want %q", event.AbandonReason, PassiveAbandonReasonSelfEcho)
	}
}

func TestReconstructorNoSelfEchoForThirdParty(t *testing.T) {
	t.Parallel()

	reconstructor := newPassiveTransactionReconstructorCore(DefaultConfig())
	reconstructor.SetLocalAddressSnapshotter(testLocalSnapshotter{address: 0x71, known: true})
	subscription, err := reconstructor.Subscribe("test", PassiveSubscriberCritical, 8)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	// Build a raw request that starts with third-party address 0x31 (ebusd) with invalid CRC.
	raw := []byte{0x31, 0x08, 0xB5, 0x09, 0x01, 0x01, 0xFF}
	payload := append(raw, protocol.SymbolSyn)
	feedPassiveSymbols(reconstructor, time.Unix(0, 0), payload)

	event := requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventAbandonedTransaction)
	if event.AbandonReason != PassiveAbandonReasonCorruptedRequest {
		t.Fatalf("abandon reason = %q; want %q", event.AbandonReason, PassiveAbandonReasonCorruptedRequest)
	}
}

func TestReconstructorSelfEchoACKPhase(t *testing.T) {
	t.Parallel()

	reconstructor := newPassiveTransactionReconstructorCore(DefaultConfig())
	reconstructor.SetLocalAddressSnapshotter(testLocalSnapshotter{address: 0x71, known: true})
	subscription, err := reconstructor.Subscribe("test", PassiveSubscriberCritical, 8)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	// Build a valid frame from our local address, then send SYN during ACK wait
	// (simulating a collision where the target never ACKs our request).
	request := protocol.Frame{
		Source:    0x71,
		Target:    0x08,
		Primary:   0xB5,
		Secondary: 0x09,
		Data:      []byte{0x01},
	}
	// frameBytes includes the trailing SYN which transitions to ACK wait phase.
	// Then send another SYN to trigger the unexpected_syn/self_echo path.
	payload := append(frameBytes(request), protocol.SymbolSyn)
	feedPassiveSymbols(reconstructor, time.Unix(0, 0), payload)

	event := requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventAbandonedTransaction)
	if event.AbandonReason != PassiveAbandonReasonSelfEcho {
		t.Fatalf("abandon reason = %q; want %q", event.AbandonReason, PassiveAbandonReasonSelfEcho)
	}
}

func TestReconstructorSelfEchoNotTriggeredWithoutSnapshotter(t *testing.T) {
	t.Parallel()

	reconstructor := newPassiveTransactionReconstructorCore(DefaultConfig())
	// No snapshotter set — isSelfOriginatedRaw returns false even if source matches.
	subscription, err := reconstructor.Subscribe("test", PassiveSubscriberCritical, 8)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	raw := []byte{0x71, 0x08, 0xB5, 0x09, 0x01, 0x01, 0xFF}
	payload := append(raw, protocol.SymbolSyn)
	feedPassiveSymbols(reconstructor, time.Unix(0, 0), payload)

	event := requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventAbandonedTransaction)
	if event.AbandonReason != PassiveAbandonReasonCorruptedRequest {
		t.Fatalf("abandon reason = %q; want %q (no snapshotter, should fall through to corrupted_request)",
			event.AbandonReason, PassiveAbandonReasonCorruptedRequest)
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

func assertNoPassiveClassifiedEvent(t *testing.T, subscription *PassiveClassifiedSubscription, wait time.Duration) {
	t.Helper()

	timer := time.NewTimer(wait)
	defer timer.Stop()

	select {
	case event, ok := <-subscription.Events():
		if !ok {
			return
		}
		t.Fatalf("unexpected passive classified event: %+v", event)
	case <-timer.C:
	}
}

func responseSegmentBytes(data []byte) []byte {
	raw := make([]byte, 0, 2+len(data))
	raw = append(raw, byte(len(data)))
	raw = append(raw, data...)
	raw = append(raw, protocol.CRC(raw))
	return raw
}
