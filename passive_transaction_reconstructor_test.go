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
	// M2I (FrameTypeInitiatorInitiator) wire: SRC DST PB SB LEN data
	// CRC ACK SYN — no SYN between command CRC and the target's ACK
	// (Spec_Prot_7 §3, P7).
	payload := append(requestFrameBytes(frame), protocol.SymbolAck, protocol.SymbolSyn)
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
	// M2T wire: SRC DST PB SB LEN data CRC ACK RESP_LEN resp_data
	// RESP_CRC ACK SYN — no SYN between command CRC and the target's
	// ACK (Spec_Prot_7 §3, P7).
	payload := append(requestFrameBytes(request), protocol.SymbolAck)
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
	// M2T no-response wire: SRC DST PB SB LEN data CRC ACK SYN — no
	// SYN between command CRC and the target's ACK; the trailing SYN
	// (with empty response phase) signals no_response (P7).
	payload := append(requestFrameBytes(request), protocol.SymbolAck, protocol.SymbolSyn)
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
	feedPassiveSymbols(reconstructor, base, append(requestFrameBytes(request), protocol.SymbolAck))
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
	feedPassiveSymbols(reconstructor, base, append(requestFrameBytes(request), protocol.SymbolAck))

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
	// requestFrameBytes is the M2T-shape (no trailing SYN), matching
	// real wire. The parser hits LEN+CRC, transitions to WaitACK
	// (P7), then the SYN below is processed as an unexpected SYN
	// during ACK wait → self_echo classification because src matches
	// the local snapshotter address.
	payload := append(requestFrameBytes(request), protocol.SymbolSyn)
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

// feedPassiveSymbols is the standard test helper. After P6 (inter-frame
// SYN gate), every legitimate frame on the wire is preceded by at least
// one SymbolSyn — startup-from-cold injection of a non-SYN byte would
// be dropped by Layer 1. To preserve backwards compatibility with the
// existing golden tests (which assume the parser starts ready to
// classify), this helper auto-prepends SymbolSyn whenever the first
// symbol is not already SYN. Tests that intentionally need to inject
// pre-SYN bytes (e.g. the Layer 1 startup-requires-SYN coverage) MUST
// use feedPassiveSymbolsRaw, which does NOT auto-prepend — see Codex
// P6 Pass 4 MINOR FINDING_5.
func feedPassiveSymbols(reconstructor *PassiveTransactionReconstructor, start time.Time, symbols []byte) {
	if len(symbols) == 0 || symbols[0] == protocol.SymbolSyn {
		feedPassiveSymbolsRaw(reconstructor, start, symbols)
		return
	}
	prepended := make([]byte, 0, len(symbols)+1)
	prepended = append(prepended, protocol.SymbolSyn)
	prepended = append(prepended, symbols...)
	feedPassiveSymbolsRaw(reconstructor, start, prepended)
}

// feedPassiveSymbolsRaw is the lower-level injection helper. It does
// NOT auto-prepend SymbolSyn. Use it whenever a test needs to assert
// behavior on bytes that arrive BEFORE the first observed SYN
// (startup-from-cold, post-discontinuity, etc.).
//
// Auto-marks 0xAA bytes inside a candidate frame body as
// WasEscaped=true so that the F-19d (batch-17) wire-side
// disambiguation treats them as data, matching the pre-F-19d
// heuristic isMidRequestFrame() semantics used by P7.1 regression
// tests. The detection is conservative: only 0xAA bytes that
// follow at least one initiator-class byte and precede the next
// wire SYN are marked. Tests that need to inject a real wire SYN
// 0xAA (not an escape-decoded one) should use
// feedPassiveSymbolsRawWithEscapes with an explicit per-byte mask.
func feedPassiveSymbolsRaw(reconstructor *PassiveTransactionReconstructor, start time.Time, symbols []byte) {
	mask := autoEscapeMaskForLegacyTests(symbols)
	feedPassiveSymbolsRawWithEscapes(reconstructor, start, symbols, mask)
}

// feedPassiveSymbolsRawWithEscapes is the F-19d-aware injection
// helper. The escapes slice is parallel to symbols: escapes[i] is
// the WasEscaped flag for the byte at symbols[i]. The caller must
// size escapes to len(symbols). Use a zero-mask
// (`make([]bool, len(symbols))`) to feed every byte as raw-wire.
func feedPassiveSymbolsRawWithEscapes(reconstructor *PassiveTransactionReconstructor, start time.Time, symbols []byte, escapes []bool) {
	if len(escapes) != len(symbols) {
		panic("feedPassiveSymbolsRawWithEscapes: escapes slice length must equal symbols length")
	}
	for index, symbol := range symbols {
		reconstructor.OnPassiveTapEvent(PassiveTapEvent{
			Kind:       PassiveTapEventSymbol,
			Symbol:     symbol,
			ObservedAt: start.Add(time.Duration(index) * 10 * time.Millisecond),
			WasEscaped: escapes[index],
		})
	}
}

// autoEscapeMaskForLegacyTests reproduces the pre-F-19d
// isMidRequestFrame() heuristic for synthetic test byte streams:
// a 0xAA byte is marked WasEscaped=true if and only if it appears
// inside the request body (positions 6 .. 6+NN_m-1 + CRC at
// 6+NN_m) AND/OR inside the response body (positions 1..NN_s+1
// after S_ACK). The walker conservatively assumes the stream is
// either a broadcast (req only, terminated by SYN) or a full MS
// transaction (req + ACK + resp + ACK + SYN). Anything ambiguous
// is left unmarked (treated as wire SYN by F-19d's data path).
//
// Used by tests that pre-date F-19d's per-byte escape plumbing —
// they feed full frames as a single byte slice and expect logical-
// 0xAA-as-data semantics for the data/CRC region. F-19d-aware
// tests should use feedPassiveSymbolsRawWithEscapes with an
// explicit mask.
func autoEscapeMaskForLegacyTests(symbols []byte) []bool {
	mask := make([]bool, len(symbols))
	i := 0
	for i < len(symbols) {
		b := symbols[i]
		if b == 0xAA {
			// Wire SYN between frames (or leading SYN gate).
			// Leave unmarked.
			i++
			continue
		}
		// Frame header expected at symbols[i .. i+4] (QQ ZZ PB SB
		// NN_m). Bail out if the slice is too short.
		if i+5 > len(symbols) {
			break
		}
		dst := symbols[i+1]
		nnM := int(symbols[i+4])
		if nnM > 16 {
			// Out-of-spec LEN — leave the rest of the slice
			// unmarked; F-19c will catch this anyway.
			i++
			continue
		}
		// Request body spans positions i+5 .. i+5+nnM (data) and
		// i+5+nnM (CRC). Mark any 0xAA bytes in that range.
		bodyEnd := i + 5 + nnM + 1 // exclusive
		if bodyEnd > len(symbols) {
			bodyEnd = len(symbols)
		}
		for j := i + 5; j < bodyEnd; j++ {
			if symbols[j] == 0xAA {
				mask[j] = true
			}
		}
		i = bodyEnd
		if dst == protocol.AddressBroadcast {
			// Broadcast: trailing SYN at i (wire SYN, not data).
			continue
		}
		// MS / MI: ACK byte at i. The ACK is structural (0x00
		// /0xFF, not 0xAA), so no mask needed.
		if i >= len(symbols) {
			break
		}
		i++ // consume ACK
		// MS only: response LEN at i, data at i+1..i+1+NN_s-1,
		// CRC at i+1+NN_s, final ACK at i+2+NN_s, SYN at
		// i+3+NN_s. Skip MI (which has only ACK + SYN).
		if i >= len(symbols) {
			break
		}
		// Peek to decide MS vs MI: if the next byte is a SYN, MI
		// terminated and the stream is done with this frame.
		if symbols[i] == 0xAA {
			continue
		}
		nnS := int(symbols[i])
		if nnS > 16 {
			i++
			continue
		}
		respEnd := i + 1 + nnS + 1 // exclusive (LEN + data + CRC)
		if respEnd > len(symbols) {
			respEnd = len(symbols)
		}
		for j := i + 1; j < respEnd; j++ {
			if symbols[j] == 0xAA {
				mask[j] = true
			}
		}
		i = respEnd
		// Final ACK + trailing SYN — consume both.
		if i < len(symbols) {
			i++ // final ACK
		}
		if i < len(symbols) && symbols[i] == 0xAA {
			i++ // trailing SYN
		}
	}
	return mask
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
