package ebusgateway

import (
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

func BenchmarkPassiveTransactionReconstructorRejectPath(b *testing.B) {
	reconstructor := newPassiveTransactionReconstructorCore(DefaultConfig())
	base := time.Unix(0, 0)
	payload := []byte{0x10, 0x08, 0xB5, 0x09, 0x01, 0x01, 0x00, protocol.SymbolSyn}

	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		feedPassiveSymbols(reconstructor, base, payload)
	}
}

func BenchmarkPassiveTransactionReconstructorRetainedTransaction(b *testing.B) {
	reconstructor := newPassiveTransactionReconstructorCore(DefaultConfig())
	subscription, err := reconstructor.Subscribe("bench", PassiveSubscriberCritical, 1)
	if err != nil {
		b.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	request := protocol.Frame{
		Source:    0x10,
		Target:    0x08,
		Primary:   0xB5,
		Secondary: 0x09,
		Data:      []byte{0x01},
	}
	// M2T wire shape (P7 — no SYN between command CRC and target ACK).
	payload := append(requestFrameBytes(request), protocol.SymbolAck)
	payload = append(payload, responseSegmentBytes([]byte{0x11, 0x55})...)
	payload = append(payload, protocol.SymbolAck, protocol.SymbolSyn)
	base := time.Unix(0, 0)

	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		feedPassiveSymbols(reconstructor, base, payload)
		select {
		case <-subscription.Events():
		default:
			b.Fatal("expected retained transaction event")
		}
	}
}
