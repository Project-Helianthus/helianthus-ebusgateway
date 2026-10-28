package main

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

func o18Poller(payload []byte, requests *int) *vaillantSemanticPoller {
	return &vaillantSemanticPoller{
		controller: 0x15, source: 0x7f,
		sendFrameFn: func(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
			(*requests)++
			return testB524ResponseForSelectorPayload(frame.Data, payload...), nil
		},
	}
}

func TestO18FloatSelectorIsolation(t *testing.T) {
	// Each neighbor changes just one selector field. Instance is deliberately
	// irrelevant to the raw-only rule; measured flow remains readable at both.
	for _, tc := range []struct {
		name                    string
		opcode, group, instance byte
		register                uint16
		want                    bool
	}{
		{"raw", 2, 2, 0, 0x20, false},
		{"opcode", 6, 2, 0, 0x20, true},
		{"group", 2, 3, 0, 0x20, true},
		{"instance", 2, 2, 1, 0x20, false},
		{"register", 2, 2, 0, 8, true},
		{"measured_instance", 2, 2, 1, 8, true},
		{"requested", 2, 2, 0, 7, true},
	} {
		for _, width := range []int{1, 4} {
			t.Run(fmt.Sprintf("%s/width%d", tc.name, width), func(t *testing.T) {
				payload := testB524Float32Payload(42.5)
				if width == 1 {
					payload = []byte{0}
				}
				requests := 0
				p := o18Poller(payload, &requests)
				got, ok := p.readB524Float32LE(context.Background(), tc.opcode, tc.group, tc.instance, tc.register)
				want := tc.want && width == 4
				if ok != want || (want && got != 42.5) {
					t.Fatalf("value=%v ok=%v; want present=%v, value 42.5", got, ok, want)
				}
				if !tc.want && requests != 0 {
					t.Fatalf("raw selector acquired %d times", requests)
				}
				if tc.want && requests == 0 {
					t.Fatal("neighbor was not acquired")
				}
			})
		}
	}
}

func TestO18RefreshDoesNotAcquireRawTemperature(t *testing.T) {
	for _, instance := range []byte{0, 1, 9} {
		for _, width := range []int{1, 4} {
			t.Run(fmt.Sprintf("instance%d/width%d", instance, width), func(t *testing.T) {
				now := time.Time{}.Add(time.Hour)
				kind := uint16(1)
				prior := 77.0
				provider := graphql.NewLiveSemanticProvider()
				requests := 0
				p := &vaillantSemanticPoller{
					controller: 0x15, source: 0x7f, provider: provider,
					nowFn:                 func() time.Time { return now },
					lastCircuitFullScanAt: now, lastCircuitFullScanComplete: true,
					circuits: map[byte]*vaillantCircuitSnapshot{instance: {Instance: instance, Active: true, CircuitTypeRaw: &kind, CalcFlowTempC: &prior}},
					sendFrameFn: func(_ context.Context, f protocol.Frame) (*protocol.Frame, error) {
						if len(f.Data) != 6 || f.Data[0] != 2 || f.Data[2] != 2 || f.Data[3] != instance {
							t.Fatalf("unexpected selector %v", f.Data)
						}
						addr := uint16(f.Data[4]) | uint16(f.Data[5])<<8
						var payload []byte
						switch addr {
						case 2:
							payload = []byte{1, 0}
							if instance == 9 {
								payload = []byte{0, 0}
							}
						case 8:
							payload = testB524Float32Payload(41.5)
						case 7:
							payload = testB524Float32Payload(46.25)
						case 0x20:
							requests++
							payload = []byte{0}
							if width == 4 {
								payload = testB524Float32Payload(55)
							}
						default:
							payload = []byte{0}
						}
						return testB524ResponseForSelectorPayload(f.Data, payload...), nil
					},
				}
				p.refreshCircuits(context.Background())
				if requests != 0 {
					t.Fatalf("RR0020 requests=%d; want zero", requests)
				}
				entry := p.circuits[instance]
				if entry == nil || entry.CalcFlowTempC != nil || entry.FlowTemperatureC == nil || *entry.FlowTemperatureC != 41.5 {
					t.Fatalf("snapshot=%+v", entry)
				}
				if instance != 9 && (entry.FlowSetpointC == nil || *entry.FlowSetpointC != 46.25) {
					t.Fatal("requested flow lost")
				}
				out := provider.Circuits()
				if len(out) != 1 || out[0].State.CalcFlowTempC != nil || out[0].State.FlowTemperatureC == nil || *out[0].State.FlowTemperatureC != 41.5 {
					t.Fatalf("publication=%+v", out)
				}
			})
		}
	}
}

func TestO18RetainedAndIncomingSnapshots(t *testing.T) {
	flow, requested, legacy, updated := 41.5, 46.25, 55.0, 42.0
	old := &vaillantCircuitSnapshot{Instance: 1, Active: true, FlowTemperatureC: &flow, FlowSetpointC: &requested, CalcFlowTempC: &legacy}
	for _, tc := range []struct {
		name     string
		incoming *vaillantCircuitSnapshot
		want     float64
	}{
		{"nil", nil, 41.5},
		{"partial", &vaillantCircuitSnapshot{Instance: 1, CalcFlowTempC: &legacy}, 41.5},
		{"recovery", &vaillantCircuitSnapshot{Instance: 1, FlowTemperatureC: &updated, CalcFlowTempC: &legacy}, 42},
	} {
		t.Run(tc.name, func(t *testing.T) {
			merged := mergeCircuitSnapshotNonDestructive(old, tc.incoming)
			if merged.CalcFlowTempC != nil || merged.FlowTemperatureC == nil || *merged.FlowTemperatureC != tc.want || merged.FlowSetpointC == nil || *merged.FlowSetpointC != 46.25 {
				t.Fatalf("merged=%+v", merged)
			}
		})
	}
	if old.CalcFlowTempC == nil || *old.CalcFlowTempC != 55 {
		t.Fatal("input mutated")
	}
	// Cache-source publication uses cloneCircuitSnapshot even without a refresh.
	p := &vaillantSemanticPoller{provider: graphql.NewLiveSemanticProvider(), circuits: map[byte]*vaillantCircuitSnapshot{1: old}}
	p.publishCircuits(semanticSnapshotSourceCache)
	out := p.provider.Circuits()
	if len(out) != 1 || out[0].State.CalcFlowTempC != nil || out[0].State.FlowTemperatureC == nil || *out[0].State.FlowTemperatureC != 41.5 || out[0].State.FlowSetpointC == nil || *out[0].State.FlowSetpointC != 46.25 {
		t.Fatalf("cached publication=%+v", out)
	}
}

func TestO18RawCannotEstablishPseudoCircuit(t *testing.T) {
	for _, payload := range [][]byte{{0}, testB524Float32Payload(55)} {
		requests := 0
		p := o18Poller(payload, &requests)
		// A one-byte measured-flow reply is known but cannot establish presence.
		p.sendFrameFn = func(_ context.Context, f protocol.Frame) (*protocol.Frame, error) {
			if f.Data[4] == 0x20 {
				requests++
				return testB524ResponseForSelectorPayload(f.Data, payload...), nil
			}
			return testB524ResponseForSelectorPayload(f.Data, 0), nil
		}
		snapshot, known := p.readDhwPseudoCircuitEvidence(context.Background(), 9)
		if snapshot != nil || !known || requests != 0 {
			t.Fatalf("snapshot=%+v known=%v raw requests=%d", snapshot, known, requests)
		}
	}
}

func TestO18RawEvidenceStillReadable(t *testing.T) {
	for _, payload := range [][]byte{{0}, testB524Float32Payload(55)} {
		t.Run(fmt.Sprintf("width%d", len(payload)), func(t *testing.T) {
			requests := 0
			p := o18Poller(payload, &requests)
			got, ok := p.readB524Value(context.Background(), 2, 2, 0, 0x20)
			if !ok || !bytes.Equal(got, payload) || requests != 1 {
				t.Fatalf("raw=%v ok=%v requests=%d", got, ok, requests)
			}
		})
	}
}
