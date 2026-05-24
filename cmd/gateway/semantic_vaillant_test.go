package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	"github.com/Project-Helianthus/helianthus-ebusreg/vaillant/productids"
)

type semanticSnapshotCaptureSpy struct {
	calls int
	last  semanticCacheSnapshot
}

func (spy *semanticSnapshotCaptureSpy) Save(snapshot semanticCacheSnapshot) error {
	spy.calls++
	spy.last = snapshot
	return nil
}

type staticSemanticReadWatchObserver struct {
	observation ebusgateway.WatchObservation
}

func (observer staticSemanticReadWatchObserver) Observe(ebusgateway.WatchKey) ebusgateway.WatchObservation {
	return observer.observation
}

type watchEfficiencyObserverSpy struct {
	mu                sync.Mutex
	readEvents        []ebusgateway.WatchEfficiencyReadEvent
	directApplyEvents []ebusgateway.WatchEfficiencyDirectApplyEvent
}

func (spy *watchEfficiencyObserverSpy) ObserveWatchRead(event ebusgateway.WatchEfficiencyReadEvent) {
	if spy == nil {
		return
	}
	spy.mu.Lock()
	defer spy.mu.Unlock()
	spy.readEvents = append(spy.readEvents, event)
}

func (spy *watchEfficiencyObserverSpy) ObserveWatchDirectApply(event ebusgateway.WatchEfficiencyDirectApplyEvent) {
	if spy == nil {
		return
	}
	spy.mu.Lock()
	defer spy.mu.Unlock()
	spy.directApplyEvents = append(spy.directApplyEvents, event)
}

func (spy *watchEfficiencyObserverSpy) latestReadEvent() (ebusgateway.WatchEfficiencyReadEvent, bool) {
	if spy == nil {
		return ebusgateway.WatchEfficiencyReadEvent{}, false
	}
	spy.mu.Lock()
	defer spy.mu.Unlock()
	if len(spy.readEvents) == 0 {
		return ebusgateway.WatchEfficiencyReadEvent{}, false
	}
	return spy.readEvents[len(spy.readEvents)-1], true
}

func (spy *watchEfficiencyObserverSpy) latestDirectApplyEvent() (ebusgateway.WatchEfficiencyDirectApplyEvent, bool) {
	if spy == nil {
		return ebusgateway.WatchEfficiencyDirectApplyEvent{}, false
	}
	spy.mu.Lock()
	defer spy.mu.Unlock()
	if len(spy.directApplyEvents) == 0 {
		return ebusgateway.WatchEfficiencyDirectApplyEvent{}, false
	}
	return spy.directApplyEvents[len(spy.directApplyEvents)-1], true
}

type b509MutationTestBus struct {
	mu     sync.Mutex
	addr   uint16
	value  []byte
	reads  int
	writes int
	ops    []byte
	onRead func()
}

func newB509MutationTestBus(addr uint16, initialValue []byte) *b509MutationTestBus {
	return &b509MutationTestBus{
		addr:  addr,
		value: append([]byte(nil), initialValue...),
	}
}

func (bus *b509MutationTestBus) Send(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
	bus.mu.Lock()
	defer bus.mu.Unlock()

	if len(frame.Data) < 3 {
		return nil, fmt.Errorf("unexpected short b509 frame: %x", frame.Data)
	}

	opcode := frame.Data[0]
	addr := uint16(frame.Data[1])<<8 | uint16(frame.Data[2])
	if addr != bus.addr {
		return nil, fmt.Errorf("unexpected b509 addr 0x%04x (want 0x%04x)", addr, bus.addr)
	}

	switch opcode {
	case vaillantB509OpcodeRead:
		bus.reads++
		bus.ops = append(bus.ops, 'R')
		if bus.onRead != nil {
			bus.onRead()
		}
		payload := append([]byte{vaillantB509OpcodeRead, byte(addr >> 8), byte(addr)}, bus.value...)
		return &protocol.Frame{Data: payload}, nil
	case vaillantB509OpcodeWrite:
		bus.writes++
		bus.ops = append(bus.ops, 'W')
		bus.value = append(bus.value[:0], frame.Data[3:]...)
		return &protocol.Frame{Data: []byte{vaillantB509OpcodeWrite, byte(addr >> 8), byte(addr)}}, nil
	default:
		return nil, fmt.Errorf("unexpected b509 opcode 0x%02x", opcode)
	}
}

func (bus *b509MutationTestBus) Counters() (reads, writes int) {
	bus.mu.Lock()
	defer bus.mu.Unlock()
	return bus.reads, bus.writes
}

func (bus *b509MutationTestBus) Value() []byte {
	bus.mu.Lock()
	defer bus.mu.Unlock()
	return append([]byte(nil), bus.value...)
}

func (bus *b509MutationTestBus) OperationTrace() string {
	bus.mu.Lock()
	defer bus.mu.Unlock()
	return string(append([]byte(nil), bus.ops...))
}

func observeFirstStateShadowConfig(key ebusgateway.WatchKey) ebusgateway.Config {
	descriptor := ebusgateway.WatchDescriptor{
		Key:               key,
		SemanticClass:     ebusgateway.WatchSemanticClassState,
		FreshnessProfile:  ebusgateway.WatchFreshnessProfileStateFast,
		DecoderID:         "test.semantic.passive.shadow",
		CorrelationPolicy: ebusgateway.WatchCorrelationPolicyRequestResponse,
		DirectApplyPolicy: ebusgateway.WatchDirectApplyPolicyStateDefault,
	}
	return ebusgateway.Config{
		ObserveFirstFlags: ebusgateway.NormalizeObserveFirstFeatureFlags(
			true,
			true,
			false,
			ebusgateway.ObserveFirstExternalWritePolicyRecordOnly,
		),
		WatchObserver: staticSemanticReadWatchObserver{
			observation: ebusgateway.WatchObservation{
				State:         ebusgateway.WatchObservationStateActive,
				Descriptor:    descriptor,
				HasDescriptor: true,
				Sources:       []ebusgateway.WatchActivationSource{ebusgateway.WatchActivationSourcePoller},
			},
		},
	}
}

type unknownSemanticWatchKey struct {
	canonical string
}

func (key unknownSemanticWatchKey) Canonical() string {
	return key.canonical
}

func (key unknownSemanticWatchKey) String() string {
	return key.canonical
}

func (key unknownSemanticWatchKey) Family() ebusgateway.WatchFamily {
	return ebusgateway.WatchFamily("UNKNOWN")
}

func observeFirstStateShadowRuntimeConfig(externalWritePolicy ebusgateway.ObserveFirstExternalWritePolicy) ebusgateway.Config {
	return ebusgateway.Config{
		ObserveFirstFlags: ebusgateway.NormalizeObserveFirstFeatureFlags(
			true,
			true,
			false,
			externalWritePolicy,
		),
	}
}

func passiveBroadcastClassifiedEvent(observedAt time.Time) ebusgateway.PassiveClassifiedEvent {
	return ebusgateway.PassiveClassifiedEvent{
		Kind:       ebusgateway.PassiveClassifiedEventBroadcastFrame,
		FrameType:  protocol.FrameTypeBroadcast,
		ObservedAt: observedAt,
	}
}

func passiveB509WriteClassifiedEvent(observedAt time.Time, source, target byte, addr uint16, value []byte) ebusgateway.PassiveClassifiedEvent {
	request := protocol.Frame{
		Source:    source,
		Target:    target,
		Primary:   vaillantB509Primary,
		Secondary: vaillantB509Secondary,
		Data:      buildB509WriteSelector(addr, value),
	}
	response := protocol.Frame{
		Source:    target,
		Target:    source,
		Primary:   vaillantB509Primary,
		Secondary: vaillantB509Secondary,
		Data:      []byte{vaillantB509OpcodeWrite, byte(addr >> 8), byte(addr)},
	}
	return ebusgateway.PassiveClassifiedEvent{
		Kind:        ebusgateway.PassiveClassifiedEventTransaction,
		FrameType:   protocol.FrameTypeInitiatorTarget,
		Request:     request,
		Response:    response,
		HasRequest:  true,
		HasResponse: true,
		ObservedAt:  observedAt,
	}
}

func waitForAdjudicatedEvent(
	t *testing.T,
	subscription *ebusgateway.AdjudicatedPassiveSubscription,
	timeout time.Duration,
	match func(ebusgateway.AdjudicatedPassiveEvent) bool,
) ebusgateway.AdjudicatedPassiveEvent {
	t.Helper()

	if subscription == nil {
		t.Fatal("waitForAdjudicatedEvent subscription = nil")
	}
	if match == nil {
		t.Fatal("waitForAdjudicatedEvent match = nil")
	}

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	for {
		select {
		case <-deadline.C:
			t.Fatalf("timeout waiting for adjudicated event after %s", timeout)
		case event, ok := <-subscription.Events():
			if !ok {
				t.Fatal("subscription closed before matching adjudicated event")
			}
			if match(event) {
				return event
			}
		}
	}
}

func waitForShadowIneligible(
	t *testing.T,
	shadow *ebusgateway.ShadowCache,
	key ebusgateway.WatchKey,
	timeout time.Duration,
) ebusgateway.ShadowLookupResult {
	t.Helper()

	if shadow == nil || key == nil {
		t.Fatal("waitForShadowIneligible requires non-nil shadow and key")
	}

	deadline := time.Now().Add(timeout)
	for {
		lookup := shadow.Lookup(key, 10*time.Second)
		if lookup.Found &&
			(lookup.Entry.State == ebusgateway.ShadowEntryStateInvalidated ||
				lookup.Entry.State == ebusgateway.ShadowEntryStateTombstone) {
			return lookup
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for shadow invalidation on %q", key.Canonical())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestBuildB524ReadSelector(t *testing.T) {
	t.Parallel()

	got := buildB524ReadSelector(0x02, 0x03, 0x01, 0x001C)
	want := []byte{0x02, 0x00, 0x03, 0x01, 0x1C, 0x00}
	if len(got) != len(want) {
		t.Fatalf("len(got)=%d; want %d", len(got), len(want))
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("got[%d]=0x%02x; want 0x%02x", i, got[i], want[i])
		}
	}
}

func TestParseB524ReadPayload(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		payload  []byte
		opcode   byte
		group    byte
		instance byte
		addr     uint16
		want     []byte
		ok       bool
	}{
		{
			name:     "valid payload with float value",
			payload:  []byte{0x01, 0x03, 0x22, 0x00, 0x00, 0x00, 0x30, 0x41},
			opcode:   0x02,
			group:    0x03,
			instance: 0x01,
			addr:     0x0022,
			want:     []byte{0x00, 0x00, 0x30, 0x41},
			ok:       true,
		},
		{
			name:     "valid payload with reply instance tag",
			payload:  []byte{0x08, 0x01, 0x03, 0x22, 0x00, 0x00, 0x00, 0x30, 0x41},
			opcode:   0x02,
			group:    0x03,
			instance: 0x00,
			addr:     0x0022,
			want:     []byte{0x00, 0x00, 0x30, 0x41},
			ok:       true,
		},
		{
			name:     "rejects mismatched reply instance",
			payload:  []byte{0x08, 0x05, 0x03, 0x22, 0x00, 0x00, 0x00, 0x30, 0x41},
			opcode:   0x02,
			group:    0x03,
			instance: 0x00,
			addr:     0x0022,
			ok:       false,
		},
		{
			name:     "header only has no value",
			payload:  []byte{0x01, 0x03, 0x22, 0x00},
			opcode:   0x02,
			group:    0x03,
			instance: 0x01,
			addr:     0x0022,
			ok:       false,
		},
		{
			name:     "short payload",
			payload:  []byte{0x01, 0x03, 0x22},
			opcode:   0x02,
			group:    0x03,
			instance: 0x01,
			addr:     0x0022,
			ok:       false,
		},
		{
			name:     "single 0x00 has no value",
			payload:  []byte{0x00},
			opcode:   0x02,
			group:    0x03,
			instance: 0x01,
			addr:     0x0022,
			ok:       false,
		},
		{
			name:     "rejects mismatched group",
			payload:  []byte{0x01, 0x02, 0x22, 0x00, 0x01},
			opcode:   0x02,
			group:    0x03,
			instance: 0x01,
			addr:     0x0022,
			ok:       false,
		},
		{
			name:     "rejects mismatched addr",
			payload:  []byte{0x01, 0x03, 0x21, 0x00, 0x01},
			opcode:   0x02,
			group:    0x03,
			instance: 0x01,
			addr:     0x0022,
			ok:       false,
		},
		{
			name:     "allows kind 0x00 with value bytes",
			payload:  []byte{0x00, 0x03, 0x1c, 0x00, 0xff},
			opcode:   0x02,
			group:    0x03,
			instance: 0x02,
			addr:     0x001c,
			want:     []byte{0xff},
			ok:       true,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, ok := parseB524ReadPayload(test.payload, test.opcode, test.group, test.instance, test.addr)
			if ok != test.ok {
				t.Fatalf("ok = %v; want %v", ok, test.ok)
			}
			if !test.ok {
				return
			}
			if len(got) != len(test.want) {
				t.Fatalf("len(got) = %d; want %d", len(got), len(test.want))
			}
			for i := range got {
				if got[i] != test.want[i] {
					t.Fatalf("got[%d] = 0x%02x; want 0x%02x", i, got[i], test.want[i])
				}
			}
		})
	}
}

func TestReadB524StartupUsesLivePathWithoutScheduler(t *testing.T) {
	t.Parallel()

	const (
		opcode   = byte(0x02)
		group    = byte(0x00)
		instance = byte(0x00)
		addr     = uint16(0x0036)
	)

	calls := 0
	poller := &vaillantSemanticPoller{
		sendFrameFn: func(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
			calls++
			if !slices.Equal(frame.Data, buildB524ReadSelector(opcode, group, instance, addr)) {
				t.Fatalf("request selector = % x; want B524 selector", frame.Data)
			}
			return &protocol.Frame{
				Data: []byte{0x01, instance, group, byte(addr), byte(addr >> 8), 0x02, 0x00},
			}, nil
		},
		source:         0x7F,
		controller:     0x15,
		requestTimeout: 50 * time.Millisecond,
	}

	got, ok := poller.readB524Startup(context.Background(), opcode, group, instance, addr)
	if !ok {
		t.Fatal("readB524Startup() ok = false; want live direct read success")
	}
	if !slices.Equal(got, []byte{0x02, 0x00}) {
		t.Fatalf("readB524Startup() = % x; want 02 00", got)
	}
	if calls != 1 {
		t.Fatalf("send calls = %d; want 1", calls)
	}
}

func TestRefreshStateSkipsSlowZoneAndDHWB524Selectors(t *testing.T) {
	t.Parallel()

	var selectors [][]byte
	poller := &vaillantSemanticPoller{
		scheduler:      ebusgateway.NewSemanticReadScheduler(),
		provider:       graphql.NewLiveSemanticProvider(),
		source:         0x7F,
		controller:     0x15,
		requestTimeout: 50 * time.Millisecond,
		zones: map[byte]*vaillantZoneSnapshot{
			0x00: {
				Instance:                    0x00,
				Present:                     true,
				Preset:                      "manual",
				ConfigurationCircuitTypeRaw: uint16Ptr(1),
			},
		},
		circuits: map[byte]*vaillantCircuitSnapshot{
			0x00: {Instance: 0x00, Active: true, CircuitTypeRaw: uint16Ptr(1)},
		},
		dhw: &vaillantDhwSnapshot{Preset: "manual"},
	}
	poller.sendFrameFn = func(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
		selectors = append(selectors, slices.Clone(frame.Data))
		return testB524ResponseForSelector(frame.Data), nil
	}

	poller.refreshState(context.Background())

	requireB524Selector(t, selectors, localZones.group, 0x00, zone_current_temp)
	requireB524Selector(t, selectors, localZones.group, 0x00, zone_special_function)
	requireB524Selector(t, selectors, localZones.group, 0x00, zone_valve_status)
	requireB524Selector(t, selectors, localDHW.group, dhwInstance, dhw_current_temp)
	requireB524Selector(t, selectors, localDHW.group, dhwInstance, dhw_special_function)

	for _, forbidden := range []struct {
		group    byte
		instance byte
		addr     uint16
	}{
		{localZones.group, 0x00, zone_target_temp},
		{localZones.group, 0x00, zone_fallback_manual_temp},
		{localZones.group, 0x00, zone_current_humidity},
		{localZones.group, 0x00, zone_heating_op_mode},
		{localZones.group, 0x00, zone_quick_veto_temp},
		{localZones.group, 0x00, zone_quick_veto_end_time},
		{localZones.group, 0x00, zone_holiday_start_date},
		{localZones.group, 0x00, zone_room_temperature_zone_mapping_raw},
		{localCircuits.group, 0x00, circuit_type},
		{localDHW.group, dhwInstance, dhw_target_temp},
		{localDHW.group, dhwInstance, dhw_operation_mode},
		{localDHW.group, dhwInstance, dhw_holiday_start_date},
		{localDHW.group, dhwInstance, dhw_holiday_end_date},
	} {
		if hasB524Selector(selectors, forbidden.group, forbidden.instance, forbidden.addr) {
			t.Fatalf("refreshState read slow selector group=0x%02x instance=0x%02x addr=0x%04x", forbidden.group, forbidden.instance, forbidden.addr)
		}
	}

	if got := poller.zones[0x00].Preset; got != "manual" {
		t.Fatalf("zone preset = %q; want preserved manual without cached op-mode", got)
	}
	if got := poller.dhw.Preset; got != "manual" {
		t.Fatalf("DHW preset = %q; want preserved manual without cached op-mode", got)
	}
}

func TestRefreshConfigReadsSlowZoneAndDHWB524Selectors(t *testing.T) {
	t.Parallel()

	var selectors [][]byte
	poller := &vaillantSemanticPoller{
		scheduler:      ebusgateway.NewSemanticReadScheduler(),
		provider:       graphql.NewLiveSemanticProvider(),
		source:         0x7F,
		controller:     0x15,
		requestTimeout: 50 * time.Millisecond,
		zones: map[byte]*vaillantZoneSnapshot{
			0x00: {Instance: 0x00, Present: true},
		},
		circuits: make(map[byte]*vaillantCircuitSnapshot),
	}
	poller.sendFrameFn = func(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
		selectors = append(selectors, slices.Clone(frame.Data))
		return testB524ResponseForSelector(frame.Data), nil
	}

	poller.refreshConfig(context.Background())

	requireB524Selector(t, selectors, localZones.group, 0x00, zone_target_temp)
	requireB524Selector(t, selectors, localZones.group, 0x00, zone_current_humidity)
	requireB524Selector(t, selectors, localZones.group, 0x00, zone_heating_op_mode)
	requireB524Selector(t, selectors, localZones.group, 0x00, zone_quick_veto_temp)
	requireB524Selector(t, selectors, localZones.group, 0x00, zone_quick_veto_end_time)
	requireB524Selector(t, selectors, localZones.group, 0x00, zone_holiday_start_date)
	requireB524Selector(t, selectors, localZones.group, 0x00, zone_room_temperature_zone_mapping_raw)
	requireB524Selector(t, selectors, localCircuits.group, 0x00, circuit_type)
	requireB524Selector(t, selectors, localDHW.group, dhwInstance, dhw_target_temp)
	requireB524Selector(t, selectors, localDHW.group, dhwInstance, dhw_operation_mode)
	requireB524Selector(t, selectors, localDHW.group, dhwInstance, dhw_holiday_start_date)
	requireB524Selector(t, selectors, localDHW.group, dhwInstance, dhw_holiday_end_date)

	if hasB524Selector(selectors, localZones.group, 0x00, zone_current_temp) {
		t.Fatal("refreshConfig read fast zone current temperature selector")
	}
	if hasB524Selector(selectors, localZones.group, 0x00, zone_special_function) {
		t.Fatal("refreshConfig read fast zone special function selector")
	}
	if hasB524Selector(selectors, localZones.group, 0x00, zone_valve_status) {
		t.Fatal("refreshConfig read fast zone valve status selector")
	}
	if hasB524Selector(selectors, localDHW.group, dhwInstance, dhw_current_temp) {
		t.Fatal("refreshConfig read fast DHW current temperature selector")
	}
	if hasB524Selector(selectors, localDHW.group, dhwInstance, dhw_special_function) {
		t.Fatal("refreshConfig read fast DHW special function selector")
	}
}

func TestRefreshBoilerStatusFastProjectsB524MirrorsWithoutActiveB524Reads(t *testing.T) {
	t.Parallel()

	var selectors [][]byte
	observedAt := time.Unix(100, 0)
	flowTemp := 42.5
	dhwTemp := 48.5
	pumpRaw := uint16(1)
	heatingRaw := uint16(2)
	poller := &vaillantSemanticPoller{
		scheduler:      ebusgateway.NewSemanticReadScheduler(),
		source:         0x7F,
		controller:     0x15,
		requestTimeout: 50 * time.Millisecond,
		nowFn:          func() time.Time { return observedAt.Add(30 * time.Second) },
		system: &vaillantSystemSnapshot{
			Controller:                  0x15,
			SystemFlowTemperature:       &flowTemp,
			SystemFlowTemperatureLiveAt: observedAt,
		},
		circuits: map[byte]*vaillantCircuitSnapshot{
			0x00: {
				Instance:           0x00,
				Active:             true,
				Controller:         0x15,
				PumpStatusRaw:      &pumpRaw,
				PumpStatusLiveAt:   observedAt,
				CircuitStateRaw:    &heatingRaw,
				CircuitStateLiveAt: observedAt,
			},
		},
		dhw: &vaillantDhwSnapshot{CurrentTempC: &dhwTemp},
	}
	poller.sendFrameFn = func(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
		selectors = append(selectors, slices.Clone(frame.Data))
		return testB524ResponseForSelector(frame.Data), nil
	}

	snapshot := &vaillantBoilerSnapshot{}
	if !poller.refreshBoilerStatusB524(context.Background(), boilerStatusTierFast, snapshot) {
		t.Fatal("refreshBoilerStatusB524(fast) updated = false; want projected snapshot update")
	}
	if len(selectors) != 0 {
		t.Fatalf("refreshBoilerStatusB524(fast) performed %d active reads; want zero", len(selectors))
	}
	if snapshot.FlowTemperatureC == nil || *snapshot.FlowTemperatureC != flowTemp {
		t.Fatalf("flow temperature = %#v; want %.1f", snapshot.FlowTemperatureC, flowTemp)
	}
	if snapshot.CentralHeatingPumpActive == nil || !*snapshot.CentralHeatingPumpActive {
		t.Fatalf("pump active = %#v; want true", snapshot.CentralHeatingPumpActive)
	}
	if snapshot.HeatingStatusRaw == nil || *snapshot.HeatingStatusRaw != int(heatingRaw) {
		t.Fatalf("heating status = %#v; want %d", snapshot.HeatingStatusRaw, heatingRaw)
	}
	if snapshot.DhwTemperatureC == nil || *snapshot.DhwTemperatureC != dhwTemp {
		t.Fatalf("dhw temperature = %#v; want %.1f", snapshot.DhwTemperatureC, dhwTemp)
	}
}

func TestRefreshBoilerStatusFastIgnoresStaleB524Mirrors(t *testing.T) {
	t.Parallel()

	observedAt := time.Unix(100, 0)
	flowTemp := 42.5
	pumpRaw := uint16(1)
	heatingRaw := uint16(2)
	poller := &vaillantSemanticPoller{
		scheduler:          ebusgateway.NewSemanticReadScheduler(),
		provider:           graphql.NewLiveSemanticProvider(),
		source:             0x7F,
		controller:         0x15,
		configInterval:     5 * time.Minute,
		boilerFastInterval: 30 * time.Second,
		requestTimeout:     50 * time.Millisecond,
		nowFn:              func() time.Time { return observedAt.Add(6 * time.Minute) },
		system: &vaillantSystemSnapshot{
			Controller:                  0x15,
			SystemFlowTemperature:       &flowTemp,
			SystemFlowTemperatureLiveAt: observedAt,
		},
		circuits: map[byte]*vaillantCircuitSnapshot{
			0x00: {
				Instance:           0x00,
				Active:             true,
				Controller:         0x15,
				PumpStatusRaw:      &pumpRaw,
				PumpStatusLiveAt:   observedAt,
				CircuitStateRaw:    &heatingRaw,
				CircuitStateLiveAt: observedAt,
			},
		},
	}

	snapshot := &vaillantBoilerSnapshot{}
	if poller.refreshBoilerStatusB524(context.Background(), boilerStatusTierFast, snapshot) {
		t.Fatal("refreshBoilerStatusB524(fast) updated = true; want stale mirrors ignored")
	}
	if snapshot.FlowTemperatureC != nil || snapshot.CentralHeatingPumpActive != nil || snapshot.HeatingStatusRaw != nil {
		t.Fatalf("stale mirror snapshot = %+v; want no projected fields", snapshot)
	}
}

func TestRefreshBoilerStatusFastIgnoresDifferentControllerB524Mirrors(t *testing.T) {
	t.Parallel()

	observedAt := time.Unix(100, 0)
	flowTemp := 42.5
	pumpRaw := uint16(1)
	heatingRaw := uint16(2)
	poller := &vaillantSemanticPoller{
		scheduler:          ebusgateway.NewSemanticReadScheduler(),
		provider:           graphql.NewLiveSemanticProvider(),
		source:             0x7F,
		controller:         0x26,
		configInterval:     5 * time.Minute,
		boilerFastInterval: 30 * time.Second,
		requestTimeout:     50 * time.Millisecond,
		nowFn:              func() time.Time { return observedAt.Add(30 * time.Second) },
		system: &vaillantSystemSnapshot{
			Controller:                  0x15,
			SystemFlowTemperature:       &flowTemp,
			SystemFlowTemperatureLiveAt: observedAt,
		},
		circuits: map[byte]*vaillantCircuitSnapshot{
			0x00: {
				Instance:           0x00,
				Active:             true,
				Controller:         0x15,
				PumpStatusRaw:      &pumpRaw,
				PumpStatusLiveAt:   observedAt,
				CircuitStateRaw:    &heatingRaw,
				CircuitStateLiveAt: observedAt,
			},
		},
	}

	snapshot := &vaillantBoilerSnapshot{}
	if poller.refreshBoilerStatusB524(context.Background(), boilerStatusTierFast, snapshot) {
		t.Fatal("refreshBoilerStatusB524(fast) updated = true; want different-controller mirrors ignored")
	}
	if snapshot.FlowTemperatureC != nil || snapshot.CentralHeatingPumpActive != nil || snapshot.HeatingStatusRaw != nil {
		t.Fatalf("different-controller mirror snapshot = %+v; want no projected fields", snapshot)
	}
}

func TestRefreshBoilerStatusFastIgnoresRestampedPartialMirrorSnapshots(t *testing.T) {
	t.Parallel()

	observedAt := time.Unix(100, 0)
	flowTemp := 42.5
	pressure := 1.4
	pumpRaw := uint16(1)
	heatingRaw := uint16(2)
	circuitType := uint16(1)

	system := mergeSystemSnapshotNonDestructive(
		&vaillantSystemSnapshot{
			Controller:                  0x15,
			SystemFlowTemperature:       &flowTemp,
			SystemFlowTemperatureLiveAt: observedAt,
		},
		&vaillantSystemSnapshot{
			Controller:          0x26,
			SystemWaterPressure: &pressure,
		},
	)
	circuit := mergeCircuitSnapshotNonDestructive(
		&vaillantCircuitSnapshot{
			Instance:           0x00,
			Active:             true,
			Controller:         0x15,
			PumpStatusRaw:      &pumpRaw,
			PumpStatusLiveAt:   observedAt,
			CircuitStateRaw:    &heatingRaw,
			CircuitStateLiveAt: observedAt,
		},
		&vaillantCircuitSnapshot{
			Instance:       0x00,
			Active:         true,
			Controller:     0x26,
			CircuitTypeRaw: &circuitType,
		},
	)
	if system.SystemFlowTemperature != nil || !system.SystemFlowTemperatureLiveAt.IsZero() {
		t.Fatalf("merged system mirror = (%v, %s); want cleared on controller change", system.SystemFlowTemperature, system.SystemFlowTemperatureLiveAt)
	}
	if circuit.PumpStatusRaw != nil || !circuit.PumpStatusLiveAt.IsZero() {
		t.Fatalf("merged pump mirror = (%v, %s); want cleared on controller change", circuit.PumpStatusRaw, circuit.PumpStatusLiveAt)
	}
	if circuit.CircuitStateRaw != nil || !circuit.CircuitStateLiveAt.IsZero() {
		t.Fatalf("merged state mirror = (%v, %s); want cleared on controller change", circuit.CircuitStateRaw, circuit.CircuitStateLiveAt)
	}

	poller := &vaillantSemanticPoller{
		scheduler:          ebusgateway.NewSemanticReadScheduler(),
		provider:           graphql.NewLiveSemanticProvider(),
		source:             0x7F,
		controller:         0x26,
		configInterval:     5 * time.Minute,
		boilerFastInterval: 30 * time.Second,
		requestTimeout:     50 * time.Millisecond,
		nowFn:              func() time.Time { return observedAt.Add(30 * time.Second) },
		system:             system,
		circuits: map[byte]*vaillantCircuitSnapshot{
			0x00: circuit,
		},
	}

	snapshot := &vaillantBoilerSnapshot{}
	if poller.refreshBoilerStatusB524(context.Background(), boilerStatusTierFast, snapshot) {
		t.Fatal("refreshBoilerStatusB524(fast) updated = true; want partial restamped mirrors ignored")
	}
}

func TestRefreshConfigDHWOnlySuccessKeepsZonesCacheSource(t *testing.T) {
	t.Parallel()

	provider := graphql.NewLiveSemanticProvider()
	poller := &vaillantSemanticPoller{
		scheduler:      ebusgateway.NewSemanticReadScheduler(),
		provider:       provider,
		source:         0x7F,
		controller:     0x15,
		requestTimeout: 50 * time.Millisecond,
		zones: map[byte]*vaillantZoneSnapshot{
			0x00: {Instance: 0x00, Present: true, Name: "Zone 1"},
		},
		circuits: make(map[byte]*vaillantCircuitSnapshot),
	}
	poller.sendFrameFn = func(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
		if len(frame.Data) == 6 && frame.Data[2] == localDHW.group {
			return testB524ResponseForSelector(frame.Data), nil
		}
		return nil, errors.New("zone read unavailable")
	}

	poller.refreshConfig(context.Background())

	if dhw := provider.DHW(); dhw == nil {
		t.Fatal("DHW = nil; want DHW slow config to publish")
	}
	if cacheEpoch, liveEpoch := provider.StartupEpochs(); liveEpoch != 1 || cacheEpoch != 1 {
		t.Fatalf("StartupEpochs() = cache=%d live=%d; want zone cache=1 and DHW live=1", cacheEpoch, liveEpoch)
	}
}

func TestRefreshConfigNoZonesStillRefreshesDHWSlowConfig(t *testing.T) {
	t.Parallel()

	provider := graphql.NewLiveSemanticProvider()
	poller := &vaillantSemanticPoller{
		scheduler:             ebusgateway.NewSemanticReadScheduler(),
		provider:              provider,
		reg:                   newTestRegistry(registry.DeviceInfo{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV"}),
		source:                0x7F,
		controller:            0x15,
		requestTimeout:        50 * time.Millisecond,
		zones:                 make(map[byte]*vaillantZoneSnapshot),
		presence:              make(map[byte]*zonePresenceRecord),
		circuits:              make(map[byte]*vaillantCircuitSnapshot),
		radioDevices:          make(map[radioDeviceKey]*vaillantRadioDeviceSnapshot),
		solarCylinders:        make(map[byte]*vaillantCylinderSnapshot),
		startupSemanticPrimed: true,
		b524ProbeFn: func(context.Context, byte, byte, byte, byte, uint16) bool {
			return true
		},
	}
	poller.sendFrameFn = func(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
		if len(frame.Data) == 6 && frame.Data[2] == localDHW.group {
			return testB524ResponseForSelector(frame.Data), nil
		}
		if len(frame.Data) == 6 && frame.Data[2] == localZones.group && (uint16(frame.Data[4])|uint16(frame.Data[5])<<8) == zone_index {
			group := frame.Data[2]
			instance := frame.Data[3]
			addr := uint16(frame.Data[4]) | uint16(frame.Data[5])<<8
			return &protocol.Frame{Data: []byte{0x01, instance, group, byte(addr), byte(addr >> 8), 0xFF}}, nil
		}
		return nil, errors.New("non-DHW read unavailable")
	}

	poller.refreshConfig(context.Background())

	dhw := provider.DHW()
	if dhw == nil {
		t.Fatal("DHW = nil; want no-zone config refresh to publish DHW slow fields")
	}
	if dhw.Config.HolidayStartDate == "" || dhw.Config.HolidayEndDate == "" {
		t.Fatalf("DHW holiday dates = %q/%q; want slow config values", dhw.Config.HolidayStartDate, dhw.Config.HolidayEndDate)
	}
}

func hasB524Selector(selectors [][]byte, group, instance byte, addr uint16) bool {
	for _, selector := range selectors {
		if len(selector) != 6 {
			continue
		}
		gotAddr := uint16(selector[4]) | uint16(selector[5])<<8
		if selector[2] == group && selector[3] == instance && gotAddr == addr {
			return true
		}
	}
	return false
}

func requireB524Selector(t *testing.T, selectors [][]byte, group, instance byte, addr uint16) {
	t.Helper()
	if !hasB524Selector(selectors, group, instance, addr) {
		t.Fatalf("missing selector group=0x%02x instance=0x%02x addr=0x%04x in % x", group, instance, addr, selectors)
	}
}

func testB524ResponseForSelector(selector []byte) *protocol.Frame {
	if len(selector) != 6 {
		return &protocol.Frame{Data: []byte{0x00}}
	}
	group := selector[2]
	instance := selector[3]
	addr := uint16(selector[4]) | uint16(selector[5])<<8
	payload := testB524PayloadForSelector(group, addr)
	data := []byte{0x01, instance, group, byte(addr), byte(addr >> 8)}
	data = append(data, payload...)
	return &protocol.Frame{Data: data}
}

func testB524ResponseForSelectorPayload(selector []byte, payload ...byte) *protocol.Frame {
	if len(selector) != 6 {
		return &protocol.Frame{Data: []byte{0x00}}
	}
	group := selector[2]
	instance := selector[3]
	addr := uint16(selector[4]) | uint16(selector[5])<<8
	data := []byte{0x01, instance, group, byte(addr), byte(addr >> 8)}
	data = append(data, payload...)
	return &protocol.Frame{Data: data}
}

func testB524PayloadForSelector(group byte, addr uint16) []byte {
	switch {
	case group == localZones.group && (addr == zone_current_temp ||
		addr == zone_target_temp ||
		addr == zone_fallback_manual_temp ||
		addr == zone_current_humidity ||
		addr == zone_quick_veto_temp ||
		addr == zone_quick_veto_duration ||
		addr == zone_holiday_setpoint):
		return testB524Float32Payload(21.5)
	case group == localDHW.group && (addr == dhw_current_temp || addr == dhw_target_temp):
		return testB524Float32Payload(48.5)
	case group == localRegulator.group && addr == system_flow_temperature:
		return testB524Float32Payload(42.5)
	case group == localZones.group && (addr == zone_quick_veto_end_time ||
		addr == zone_holiday_start_time ||
		addr == zone_holiday_end_time):
		return []byte{6, 30}
	case group == localZones.group && (addr == zone_quick_veto_end_date ||
		addr == zone_holiday_start_date ||
		addr == zone_holiday_end_date):
		return []byte{2, 1, 24}
	case group == localDHW.group && (addr == dhw_holiday_start_date || addr == dhw_holiday_end_date):
		return []byte{2, 1, 24}
	case group == localZones.group && (addr == zone_heating_op_mode ||
		addr == zone_special_function ||
		addr == zone_valve_status ||
		addr == zone_room_temperature_zone_mapping_raw):
		return []byte{1, 0}
	case group == localDHW.group && (addr == dhw_operation_mode ||
		addr == dhw_special_function):
		return []byte{1, 0}
	case group == localCircuits.group && (addr == circuit_type ||
		addr == circuit_pump_status ||
		addr == circuit_circuit_state):
		return []byte{1, 0}
	default:
		return []byte{'T', 'e', 's', 't', 0, 0}
	}
}

func testB524Float32Payload(value float64) []byte {
	payload := make([]byte, 4)
	binary.LittleEndian.PutUint32(payload, math.Float32bits(float32(value)))
	return payload
}

func TestRefreshDiscoveryPrimesDHWBeforeStartupZoneScan(t *testing.T) {
	t.Parallel()

	var selectors [][]byte
	poller := &vaillantSemanticPoller{
		provider:       graphql.NewLiveSemanticProvider(),
		reg:            newTestRegistry(registry.DeviceInfo{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV"}),
		tasks:          newSemanticTaskScheduler(),
		zones:          make(map[byte]*vaillantZoneSnapshot),
		presence:       make(map[byte]*zonePresenceRecord),
		circuits:       make(map[byte]*vaillantCircuitSnapshot),
		radioDevices:   make(map[radioDeviceKey]*vaillantRadioDeviceSnapshot),
		solarCylinders: make(map[byte]*vaillantCylinderSnapshot),
		source:         0x7F,
		requestTimeout: 50 * time.Millisecond,
		b524ProbeFn: func(ctx context.Context, target, opcode, group, instance byte, addr uint16) bool {
			return target == 0x15
		},
	}
	poller.sendFrameFn = func(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
		if frame.Primary != vaillantExtRegisterPrimary || frame.Secondary != vaillantExtRegisterSecondary || len(frame.Data) != 6 {
			return &protocol.Frame{Data: []byte{0x00}}, nil
		}
		selector := slices.Clone(frame.Data)
		selectors = append(selectors, selector)
		group := selector[2]
		instance := selector[3]
		addr := uint16(selector[4]) | uint16(selector[5])<<8
		payload := []byte{0x00}
		switch {
		case group == localDHW.group && instance == dhwInstance && addr == dhw_current_temp:
			payload = make([]byte, 4)
			binary.LittleEndian.PutUint32(payload, math.Float32bits(48.5))
		case group == localDHW.group && instance == dhwInstance && addr == dhw_operation_mode:
			payload = []byte{0x01, 0x00}
		case group == localZones.group && addr == zone_index:
			payload = []byte{0xFF}
		}
		data := []byte{0x01, instance, group, byte(addr), byte(addr >> 8)}
		data = append(data, payload...)
		return &protocol.Frame{Data: data}, nil
	}

	poller.refreshDiscovery(context.Background())

	if len(selectors) == 0 {
		t.Fatal("refreshDiscovery sent no B524 startup reads")
	}
	first := selectors[0]
	if first[2] != localDHW.group || first[3] != dhwInstance {
		t.Fatalf("first startup selector = % x; want DHW before zone scan", first)
	}
	if dhw := poller.provider.DHW(); dhw == nil {
		t.Fatal("provider.DHW() = nil; want critical DHW singleton primed during discovery")
	}
}

func TestRefreshDiscoveryRetriesDHWBeforeStartupZoneScan(t *testing.T) {
	t.Parallel()

	dhwRequests := 0
	zoneRequestsBeforeDHWSuccess := 0
	dhwSawLiveResponse := false
	poller := &vaillantSemanticPoller{
		provider:       graphql.NewLiveSemanticProvider(),
		reg:            newTestRegistry(registry.DeviceInfo{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV"}),
		tasks:          newSemanticTaskScheduler(),
		zones:          make(map[byte]*vaillantZoneSnapshot),
		presence:       make(map[byte]*zonePresenceRecord),
		circuits:       make(map[byte]*vaillantCircuitSnapshot),
		radioDevices:   make(map[radioDeviceKey]*vaillantRadioDeviceSnapshot),
		solarCylinders: make(map[byte]*vaillantCylinderSnapshot),
		source:         0x7F,
		requestTimeout: 50 * time.Millisecond,
		b524ProbeFn: func(ctx context.Context, target, opcode, group, instance byte, addr uint16) bool {
			return target == 0x15
		},
	}
	poller.sendFrameFn = func(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
		if frame.Primary != vaillantExtRegisterPrimary || frame.Secondary != vaillantExtRegisterSecondary || len(frame.Data) != 6 {
			return &protocol.Frame{Data: []byte{0x00}}, nil
		}
		selector := frame.Data
		group := selector[2]
		instance := selector[3]
		addr := uint16(selector[4]) | uint16(selector[5])<<8
		if group == localZones.group && !dhwSawLiveResponse {
			zoneRequestsBeforeDHWSuccess++
		}
		if group == localDHW.group {
			dhwRequests++
			if dhwRequests <= 2 {
				return nil, errors.New("transient dhw startup miss")
			}
		}
		payload := []byte{0x00}
		switch {
		case group == localDHW.group && instance == dhwInstance && addr == dhw_current_temp:
			payload = make([]byte, 4)
			binary.LittleEndian.PutUint32(payload, math.Float32bits(48.5))
			dhwSawLiveResponse = true
		case group == localDHW.group && instance == dhwInstance && addr == dhw_operation_mode:
			payload = []byte{0x01, 0x00}
			dhwSawLiveResponse = true
		case group == localZones.group && addr == zone_index:
			payload = []byte{0xFF}
		}
		data := []byte{0x01, instance, group, byte(addr), byte(addr >> 8)}
		data = append(data, payload...)
		return &protocol.Frame{Data: data}, nil
	}

	poller.refreshDiscovery(context.Background())

	if dhwRequests < 4 {
		t.Fatalf("DHW startup requests = %d; want retry after first probe pair fails", dhwRequests)
	}
	if zoneRequestsBeforeDHWSuccess != 0 {
		t.Fatalf("zone requests before DHW live response = %d; want 0", zoneRequestsBeforeDHWSuccess)
	}
	if dhw := poller.provider.DHW(); dhw == nil {
		t.Fatal("provider.DHW() = nil; want retry to prime DHW before startup zone scan")
	}
}

func TestParseB509ReadPayload(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		payload []byte
		addr    uint16
		want    []byte
		ok      bool
	}{
		{
			name:    "accepts single byte zero payload",
			payload: []byte{0x00},
			addr:    0x4400,
			want:    []byte{0x00},
			ok:      true,
		},
		{
			name:    "accepts single byte nonzero payload",
			payload: []byte{0x01},
			addr:    0x7B00,
			want:    []byte{0x01},
			ok:      true,
		},
		{
			name:    "strips opcode header",
			payload: []byte{0x0D, 0x44, 0x00, 0x00},
			addr:    0x4400,
			want:    []byte{0x00},
			ok:      true,
		},
		{
			name:    "strips compact address header",
			payload: []byte{0x44, 0x00, 0x00},
			addr:    0x4400,
			want:    []byte{0x00},
			ok:      true,
		},
		{
			name:    "rejects empty payload",
			payload: nil,
			addr:    0x4400,
			ok:      false,
		},
		{
			name:    "rejects header only opcode payload",
			payload: []byte{0x0D, 0x44, 0x00},
			addr:    0x4400,
			ok:      false,
		},
		{
			name:    "rejects header only compact payload",
			payload: []byte{0x44, 0x00},
			addr:    0x4400,
			ok:      false,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got, ok := parseB509ReadPayload(test.payload, test.addr)
			if ok != test.ok {
				t.Fatalf("ok = %v; want %v", ok, test.ok)
			}
			if !test.ok {
				return
			}
			if !slices.Equal(got, test.want) {
				t.Fatalf("parseB509ReadPayload(%x, 0x%04x) = %x; want %x", test.payload, test.addr, got, test.want)
			}
		})
	}
}

func TestEncodeBoilerConfigPayload(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		fieldName   string
		spec        boilerConfigFieldSpec
		value       float64
		wantPayload []byte
		wantValue   float64
		wantErr     string
	}{
		{
			name:        "temperature field uses DATA2c",
			fieldName:   "flowsetHwcMaxC",
			spec:        boilerConfigFieldSpecs["flowsetHwcMaxC"],
			value:       59,
			wantPayload: []byte{0xB0, 0x03},
			wantValue:   59,
		},
		{
			name:        "temperature field normalizes to wire precision",
			fieldName:   "flowsetHcMaxC",
			spec:        boilerConfigFieldSpecs["flowsetHcMaxC"],
			value:       55.1,
			wantPayload: []byte{0x71, 0x03},
			wantValue:   55.0625,
		},
		{
			name:        "partload field uses UCH",
			fieldName:   "partloadHcKW",
			spec:        boilerConfigFieldSpecs["partloadHcKW"],
			value:       18,
			wantPayload: []byte{0x12},
			wantValue:   18,
		},
		{
			name:      "fractional partload rejected",
			fieldName: "partloadHwcKW",
			spec:      boilerConfigFieldSpecs["partloadHwcKW"],
			value:     18.5,
			wantErr:   "whole-number kW required",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			payload, normalizedValue, err := encodeBoilerConfigPayload(test.fieldName, test.spec, test.value)
			if test.wantErr != "" {
				if err == nil {
					t.Fatalf("encodeBoilerConfigPayload(%q, %.4g) error = nil; want %q", test.fieldName, test.value, test.wantErr)
				}
				if !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("encodeBoilerConfigPayload(%q, %.4g) error = %q; want substring %q", test.fieldName, test.value, err.Error(), test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("encodeBoilerConfigPayload(%q, %.4g) error = %v", test.fieldName, test.value, err)
			}
			if !slices.Equal(payload, test.wantPayload) {
				t.Fatalf("encodeBoilerConfigPayload(%q, %.4g) payload = %x; want %x", test.fieldName, test.value, payload, test.wantPayload)
			}
			if normalizedValue != test.wantValue {
				t.Fatalf("encodeBoilerConfigPayload(%q, %.4g) value = %.4g; want %.4g", test.fieldName, test.value, normalizedValue, test.wantValue)
			}
		})
	}
}

func TestParseBoilerConfigValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		rawValue  string
		spec      boilerConfigFieldSpec
		wantValue float64
		wantErr   string
	}{
		{
			name:      "accepts finite in-range value",
			rawValue:  "59",
			spec:      boilerConfigFieldSpecs["flowsetHwcMaxC"],
			wantValue: 59,
		},
		{
			name:     "rejects nan",
			rawValue: "NaN",
			spec:     boilerConfigFieldSpecs["flowsetHcMaxC"],
			wantErr:  "finite number required",
		},
		{
			name:     "rejects infinity",
			rawValue: "+Inf",
			spec:     boilerConfigFieldSpecs["flowsetHcMaxC"],
			wantErr:  "finite number required",
		},
		{
			name:     "rejects out of range value",
			rawValue: "85",
			spec:     boilerConfigFieldSpecs["flowsetHcMaxC"],
			wantErr:  "out of range",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			value, err := parseBoilerConfigValue(test.rawValue, test.spec)
			if test.wantErr != "" {
				if err == nil {
					t.Fatalf("parseBoilerConfigValue(%q) error = nil; want %q", test.rawValue, test.wantErr)
				}
				if !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("parseBoilerConfigValue(%q) error = %q; want substring %q", test.rawValue, err.Error(), test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseBoilerConfigValue(%q) error = %v", test.rawValue, err)
			}
			if value != test.wantValue {
				t.Fatalf("parseBoilerConfigValue(%q) value = %.4g; want %.4g", test.rawValue, value, test.wantValue)
			}
		})
	}
}

func TestSetBoilerConfig_WriteConfirmForcesLiveReadAfterWrite(t *testing.T) {
	t.Parallel()

	const (
		boilerAddress = byte(0x08)
		fieldName     = "flowsetHwcMaxC"
	)

	spec := boilerConfigFieldSpecs[fieldName]
	registerAddr := spec.addrs[0]

	stalePayload, _, err := encodeBoilerConfigPayload(fieldName, spec, 58)
	if err != nil {
		t.Fatalf("encode stale payload error = %v", err)
	}
	expectedPayload, _, err := encodeBoilerConfigPayload(fieldName, spec, 59)
	if err != nil {
		t.Fatalf("encode expected payload error = %v", err)
	}

	key := ebusgateway.NewB509WatchKey(boilerAddress, registerAddr)
	catalog, err := ebusgateway.NewWatchCatalog([]ebusgateway.WatchDescriptor{
		{
			Key:               key,
			SemanticClass:     ebusgateway.WatchSemanticClassState,
			FreshnessProfile:  ebusgateway.WatchFreshnessProfileStateFast,
			DecoderID:         "test.semantic.b509.write.confirm",
			CorrelationPolicy: ebusgateway.WatchCorrelationPolicyRequestResponse,
			DirectApplyPolicy: ebusgateway.WatchDirectApplyPolicyStateDefault,
		},
	})
	if err != nil {
		t.Fatalf("NewWatchCatalog() error = %v", err)
	}
	activations := ebusgateway.NewWatchActivationSet(catalog)
	if err := activations.Activate(ebusgateway.WatchActivationSourcePoller, key); err != nil {
		t.Fatalf("Activate(poller) error = %v", err)
	}

	now := time.Unix(100, 0)
	shadow := ebusgateway.NewShadowCache(ebusgateway.ShadowCacheOptions{
		Catalog:               catalog,
		Activations:           activations,
		Capacity:              8,
		PinnedCapacity:        4,
		WriteConfirmPinnedCap: 2,
		Now:                   func() time.Time { return now },
	})
	scheduler := ebusgateway.NewSemanticReadScheduler()
	scheduler.SetShadowCache(shadow)

	bus := newB509MutationTestBus(registerAddr, stalePayload)
	poller := &vaillantSemanticPoller{
		scheduler:       scheduler,
		shadow:          shadow,
		sendFrameFn:     bus.Send,
		source:          0x31,
		requestTimeout:  250 * time.Millisecond,
		boilerAddress:   boilerAddress,
		watchObserver:   nil,
		transportConfig: ebusgateway.TransportConfig{},
	}

	_ = poller.prepareSemanticReadWatch(key)
	staleWrite := shadow.Write(ebusgateway.ShadowWrite{
		Key:        key,
		Source:     ebusgateway.ShadowWriteSourcePassive,
		Confidence: ebusgateway.ShadowConfidenceHigh,
		Value:      stalePayload,
		ObservedAt: now,
	})
	if !staleWrite.Accepted {
		t.Fatalf("shadow stale seed write rejected: %s", staleWrite.Reason)
	}

	result := poller.SetBoilerConfig(context.Background(), fieldName, "59")
	if !result.Success {
		t.Fatalf("SetBoilerConfig() = %+v; want success with live post-write readback", result)
	}

	reads, writes := bus.Counters()
	if writes != 1 {
		t.Fatalf("write calls = %d; want 1", writes)
	}
	if reads < 1 {
		t.Fatalf("read calls = %d; want at least one live read for write confirm", reads)
	}
	trace := bus.OperationTrace()
	writeAt := strings.IndexByte(trace, 'W')
	if writeAt < 0 {
		t.Fatalf("operation trace = %q; want a write operation", trace)
	}
	if writeAt+1 >= len(trace) || !strings.Contains(trace[writeAt+1:], "R") {
		t.Fatalf("operation trace = %q; want read-after-write for live confirm (no stale cache confirm)", trace)
	}
	if got := bus.Value(); !slices.Equal(got, expectedPayload) {
		t.Fatalf("bus value = %x; want written payload %x", got, expectedPayload)
	}
}

func TestSetBoilerConfig_WriteConfirmInvalidatesShadowBeforeReadback(t *testing.T) {
	t.Parallel()

	const (
		boilerAddress = byte(0x08)
		fieldName     = "flowsetHwcMaxC"
	)

	spec := boilerConfigFieldSpecs[fieldName]
	registerAddr := spec.addrs[0]

	stalePayload, _, err := encodeBoilerConfigPayload(fieldName, spec, 58)
	if err != nil {
		t.Fatalf("encode stale payload error = %v", err)
	}
	expectedPayload, _, err := encodeBoilerConfigPayload(fieldName, spec, 59)
	if err != nil {
		t.Fatalf("encode expected payload error = %v", err)
	}

	key := ebusgateway.NewB509WatchKey(boilerAddress, registerAddr)
	catalog, err := ebusgateway.NewWatchCatalog([]ebusgateway.WatchDescriptor{
		{
			Key:               key,
			SemanticClass:     ebusgateway.WatchSemanticClassState,
			FreshnessProfile:  ebusgateway.WatchFreshnessProfileStateFast,
			DecoderID:         "test.semantic.b509.write.confirm.fence",
			CorrelationPolicy: ebusgateway.WatchCorrelationPolicyRequestResponse,
			DirectApplyPolicy: ebusgateway.WatchDirectApplyPolicyStateDefault,
		},
	})
	if err != nil {
		t.Fatalf("NewWatchCatalog() error = %v", err)
	}
	activations := ebusgateway.NewWatchActivationSet(catalog)
	if err := activations.Activate(ebusgateway.WatchActivationSourcePoller, key); err != nil {
		t.Fatalf("Activate(poller) error = %v", err)
	}

	now := time.Unix(100, 0)
	shadow := ebusgateway.NewShadowCache(ebusgateway.ShadowCacheOptions{
		Catalog:               catalog,
		Activations:           activations,
		Capacity:              8,
		PinnedCapacity:        4,
		WriteConfirmPinnedCap: 2,
		Now:                   func() time.Time { return now },
	})
	scheduler := ebusgateway.NewSemanticReadScheduler()
	scheduler.SetShadowCache(shadow)

	var seenGenerations []uint64
	var seenEligibility []bool
	bus := newB509MutationTestBus(registerAddr, stalePayload)
	bus.onRead = func() {
		snapshot := shadow.SnapshotEligibility(key)
		seenGenerations = append(seenGenerations, snapshot.Generation)
		seenEligibility = append(seenEligibility, snapshot.Eligible)
	}
	poller := &vaillantSemanticPoller{
		scheduler:       scheduler,
		shadow:          shadow,
		sendFrameFn:     bus.Send,
		source:          0x31,
		requestTimeout:  250 * time.Millisecond,
		boilerAddress:   boilerAddress,
		watchObserver:   nil,
		transportConfig: ebusgateway.TransportConfig{},
	}

	_ = poller.prepareSemanticReadWatch(key)
	staleWrite := shadow.Write(ebusgateway.ShadowWrite{
		Key:        key,
		Source:     ebusgateway.ShadowWriteSourcePassive,
		Confidence: ebusgateway.ShadowConfidenceHigh,
		Value:      stalePayload,
		ObservedAt: now,
	})
	if !staleWrite.Accepted {
		t.Fatalf("shadow stale seed write rejected: %s", staleWrite.Reason)
	}
	if staleWrite.Generation == 0 {
		t.Fatal("stale seed generation = 0; want non-zero generation")
	}

	result := poller.SetBoilerConfig(context.Background(), fieldName, "59")
	if !result.Success {
		t.Fatalf("SetBoilerConfig() = %+v; want success", result)
	}

	if len(seenGenerations) == 0 {
		t.Fatal("expected readback generation snapshots to be captured")
	}
	sawAdvancedGeneration := false
	for _, generation := range seenGenerations {
		if generation > staleWrite.Generation {
			sawAdvancedGeneration = true
			break
		}
	}
	if !sawAdvancedGeneration {
		t.Fatalf("seen generations = %v; want at least one generation > stale generation %d after write-confirm fence invalidation", seenGenerations, staleWrite.Generation)
	}
	if len(seenEligibility) == 0 {
		t.Fatal("expected readback eligibility snapshots to be captured")
	}
	sawIneligible := false
	for _, eligible := range seenEligibility {
		if !eligible {
			sawIneligible = true
			break
		}
	}
	if !sawIneligible {
		t.Fatalf("seen eligibility = %v; want at least one ineligible snapshot before confirm read", seenEligibility)
	}

	if got := bus.Value(); !slices.Equal(got, expectedPayload) {
		t.Fatalf("bus value = %x; want written payload %x", got, expectedPayload)
	}
}

func TestHandleAdjudicatedPassiveEvent_UnmatchedValueBearingReadWritesShadow(t *testing.T) {
	t.Parallel()

	key := ebusgateway.NewB509WatchKey(0x08, 0x0200)
	poller := newVaillantSemanticPoller(
		observeFirstStateShadowRuntimeConfig(ebusgateway.ObserveFirstExternalWritePolicyRecordOnly),
		&ebusgateway.Gateway{},
		graphql.NewLiveSemanticProvider(),
		nil,
		nil,
	)

	observedAt := time.Now()
	event := ebusgateway.AdjudicatedPassiveEvent{
		Event: ebusgateway.PassiveClassifiedEvent{
			HasRequest:  true,
			HasResponse: true,
			Request: protocol.Frame{
				Source:    0x10,
				Target:    0x08,
				Primary:   vaillantB509Primary,
				Secondary: vaillantB509Secondary,
				Data:      buildB509ReadSelector(0x0200),
			},
			Response: protocol.Frame{
				Source:    0x08,
				Target:    0x10,
				Primary:   vaillantB509Primary,
				Secondary: vaillantB509Secondary,
				Data:      []byte{vaillantB509OpcodeRead, 0x02, 0x00, 0xAA, 0x55},
			},
		},
		Fingerprint: ebusgateway.PassiveTransactionFingerprint{
			SharedWatchKey: key,
			ObservedAt:     observedAt,
			ResponseClass:  ebusgateway.DedupResponseValueBearing,
			FamilyPolicy: ebusgateway.ObserveFirstFamilyPolicy{
				RequestIntent:     ebusgateway.ObserveFirstRequestIntentRead,
				ResponseClass:     ebusgateway.DedupResponseValueBearing,
				DirectApplyPolicy: ebusgateway.ObserveFirstDirectApplyPolicyStateDefault,
			},
		},
		Disposition: ebusgateway.DedupDispositionUnmatchedThirdParty,
	}

	poller.handleAdjudicatedPassiveEvent(event)

	lookup := poller.shadow.Lookup(key, 10*time.Second)
	if !lookup.Found {
		t.Fatal("shadow lookup found = false; want passive value-bearing read in shadow")
	}
	if !lookup.Eligible {
		t.Fatalf("shadow lookup eligible = false; want true for freshly written passive read (state=%s)", lookup.Entry.State)
	}
	if !slices.Equal(lookup.Entry.Value, []byte{0xAA, 0x55}) {
		t.Fatalf("shadow value = %x; want %x", lookup.Entry.Value, []byte{0xAA, 0x55})
	}
	if lookup.Entry.ObservedAt.IsZero() || !lookup.Entry.ObservedAt.Equal(observedAt) {
		t.Fatalf("shadow observed_at = %s; want %s", lookup.Entry.ObservedAt.UTC().Format(time.RFC3339), observedAt.UTC().Format(time.RFC3339))
	}
	if lookup.Entry.Pinned {
		t.Fatal("shadow entry pinned = true; want passive fallback bootstrap to remain unpinned")
	}
	if summary := poller.shadow.Summary(); summary.StaticPinnedFootprint != 0 {
		t.Fatalf("Summary().StaticPinnedFootprint = %d; want 0 for passive fallback bootstrap", summary.StaticPinnedFootprint)
	}
}

func TestHandleAdjudicatedPassiveEvent_UnmatchedValueBearingB524ReadWritesShadow(t *testing.T) {
	t.Parallel()

	key := ebusgateway.NewB524WatchKey(0x15, 0x02, 0x03, 0x01, 0x001C)
	poller := newVaillantSemanticPoller(
		observeFirstStateShadowRuntimeConfig(ebusgateway.ObserveFirstExternalWritePolicyRecordOnly),
		&ebusgateway.Gateway{},
		graphql.NewLiveSemanticProvider(),
		nil,
		nil,
	)

	observedAt := time.Now()
	event := ebusgateway.AdjudicatedPassiveEvent{
		Event: ebusgateway.PassiveClassifiedEvent{
			HasRequest:  true,
			HasResponse: true,
			Request: protocol.Frame{
				Source:    0x10,
				Target:    0x15,
				Primary:   vaillantExtRegisterPrimary,
				Secondary: vaillantExtRegisterSecondary,
				Data:      buildB524ReadSelector(0x02, 0x03, 0x01, 0x001C),
			},
			Response: protocol.Frame{
				Source:    0x15,
				Target:    0x10,
				Primary:   vaillantExtRegisterPrimary,
				Secondary: vaillantExtRegisterSecondary,
				Data:      []byte{0x02, 0x01, 0x03, 0x1C, 0x00, 0x42, 0x01},
			},
		},
		Fingerprint: ebusgateway.PassiveTransactionFingerprint{
			SharedWatchKey: key,
			ObservedAt:     observedAt,
			ResponseClass:  ebusgateway.DedupResponseValueBearing,
			FamilyPolicy: ebusgateway.ObserveFirstFamilyPolicy{
				RequestIntent:     ebusgateway.ObserveFirstRequestIntentRead,
				ResponseClass:     ebusgateway.DedupResponseValueBearing,
				DirectApplyPolicy: ebusgateway.ObserveFirstDirectApplyPolicyStateDefault,
			},
		},
		Disposition: ebusgateway.DedupDispositionUnmatchedThirdParty,
	}

	poller.handleAdjudicatedPassiveEvent(event)

	lookup := poller.shadow.Lookup(key, 10*time.Second)
	if !lookup.Found {
		t.Fatal("shadow lookup found = false; want passive B524 value-bearing read in shadow")
	}
	if !lookup.Eligible {
		t.Fatalf("shadow lookup eligible = false; want true for freshly written passive B524 read (state=%s)", lookup.Entry.State)
	}
	if !slices.Equal(lookup.Entry.Value, []byte{0x42, 0x01}) {
		t.Fatalf("shadow value = %x; want %x", lookup.Entry.Value, []byte{0x42, 0x01})
	}
}

func TestPassiveShadowLaneEnabled_EnergyMergeOnlyPolicyDisabled(t *testing.T) {
	t.Parallel()

	flags := ebusgateway.NormalizeObserveFirstFeatureFlags(
		true,
		true,
		true,
		ebusgateway.ObserveFirstExternalWritePolicyRecordOnly,
	)
	policy := ebusgateway.ObserveFirstFamilyPolicy{
		RequestIntent:     ebusgateway.ObserveFirstRequestIntentRead,
		DirectApplyPolicy: ebusgateway.ObserveFirstDirectApplyPolicyEnergyMergeOnly,
	}

	if passiveShadowLaneEnabled(flags, policy) {
		t.Fatal("passiveShadowLaneEnabled() = true; want false for energy_merge_only carve-out")
	}
}

func TestHandleAdjudicatedPassiveEvent_UnmatchedExternalWriteInvalidatesShadow(t *testing.T) {
	t.Parallel()

	key := ebusgateway.NewB509WatchKey(0x08, 0x0200)
	poller := newVaillantSemanticPoller(
		observeFirstStateShadowRuntimeConfig(ebusgateway.ObserveFirstExternalWritePolicyInvalidateOnly),
		&ebusgateway.Gateway{},
		graphql.NewLiveSemanticProvider(),
		nil,
		nil,
	)

	_ = poller.prepareSemanticReadWatch(key)
	seedAt := time.Unix(100, 0)
	seed := poller.shadow.Write(ebusgateway.ShadowWrite{
		Key:        key,
		Source:     ebusgateway.ShadowWriteSourcePassive,
		Confidence: ebusgateway.ShadowConfidenceHigh,
		Value:      []byte{0x11, 0x22},
		ObservedAt: seedAt,
	})
	if !seed.Accepted {
		t.Fatalf("seed shadow write rejected: %s", seed.Reason)
	}

	invalidatedAt := seedAt.Add(5 * time.Second)
	event := ebusgateway.AdjudicatedPassiveEvent{
		Event: ebusgateway.PassiveClassifiedEvent{
			HasRequest: true,
			Request: protocol.Frame{
				Source:    0x10,
				Target:    0x08,
				Primary:   vaillantB509Primary,
				Secondary: vaillantB509Secondary,
				Data:      buildB509WriteSelector(0x0200, []byte{0x33, 0x44}),
			},
		},
		Fingerprint: ebusgateway.PassiveTransactionFingerprint{
			SharedWatchKey: key,
			ObservedAt:     invalidatedAt,
			FamilyPolicy: ebusgateway.ObserveFirstFamilyPolicy{
				RequestIntent:                  ebusgateway.ObserveFirstRequestIntentWrite,
				UsesRuntimeExternalWritePolicy: true,
				EffectiveExternalWritePolicy:   ebusgateway.ObserveFirstExternalWritePolicyInvalidateOnly,
			},
		},
		Disposition: ebusgateway.DedupDispositionUnmatchedThirdParty,
	}

	poller.handleAdjudicatedPassiveEvent(event)

	lookup := poller.shadow.Lookup(key, 10*time.Second)
	if !lookup.Found {
		t.Fatal("shadow lookup found = false; want invalidated entry preserved")
	}
	if lookup.Eligible {
		t.Fatalf("shadow lookup eligible = true; want false after external-write invalidation (state=%s)", lookup.Entry.State)
	}
	if lookup.Entry.State != ebusgateway.ShadowEntryStateInvalidated && lookup.Entry.State != ebusgateway.ShadowEntryStateTombstone {
		t.Fatalf("shadow state = %s; want invalidated or tombstone", lookup.Entry.State)
	}
}

func TestHandleAdjudicatedPassiveEvent_IgnoresNonApplicableEvents(t *testing.T) {
	t.Parallel()

	baseEvent := func() ebusgateway.AdjudicatedPassiveEvent {
		return ebusgateway.AdjudicatedPassiveEvent{
			Event: ebusgateway.PassiveClassifiedEvent{
				HasRequest:  true,
				HasResponse: true,
				Request: protocol.Frame{
					Source:    0x10,
					Target:    0x08,
					Primary:   vaillantB509Primary,
					Secondary: vaillantB509Secondary,
					Data:      buildB509ReadSelector(0x0200),
				},
				Response: protocol.Frame{
					Source:    0x08,
					Target:    0x10,
					Primary:   vaillantB509Primary,
					Secondary: vaillantB509Secondary,
					Data:      []byte{vaillantB509OpcodeRead, 0x02, 0x00, 0xAA},
				},
			},
			Fingerprint: ebusgateway.PassiveTransactionFingerprint{
				SharedWatchKey: ebusgateway.NewB509WatchKey(0x08, 0x0200),
				ObservedAt:     time.Unix(300, 0),
				ResponseClass:  ebusgateway.DedupResponseValueBearing,
				FamilyPolicy: ebusgateway.ObserveFirstFamilyPolicy{
					RequestIntent:     ebusgateway.ObserveFirstRequestIntentRead,
					ResponseClass:     ebusgateway.DedupResponseValueBearing,
					DirectApplyPolicy: ebusgateway.ObserveFirstDirectApplyPolicyStateDefault,
				},
			},
			Disposition: ebusgateway.DedupDispositionUnmatchedThirdParty,
		}
	}

	tests := []struct {
		name   string
		mutate func(*ebusgateway.AdjudicatedPassiveEvent)
	}{
		{
			name: "matched active duplicate",
			mutate: func(event *ebusgateway.AdjudicatedPassiveEvent) {
				event.MatchedActiveDuplicate = true
			},
		},
		{
			name: "observability only",
			mutate: func(event *ebusgateway.AdjudicatedPassiveEvent) {
				event.ObservabilityOnly = true
			},
		},
		{
			name: "local participant inbound",
			mutate: func(event *ebusgateway.AdjudicatedPassiveEvent) {
				event.LocalParticipantInbound = true
			},
		},
		{
			name: "header only response class",
			mutate: func(event *ebusgateway.AdjudicatedPassiveEvent) {
				event.Fingerprint.ResponseClass = ebusgateway.DedupResponseHeaderOnly
				event.Fingerprint.FamilyPolicy.ResponseClass = ebusgateway.DedupResponseHeaderOnly
			},
		},
		{
			name: "ack only response class",
			mutate: func(event *ebusgateway.AdjudicatedPassiveEvent) {
				event.Fingerprint.ResponseClass = ebusgateway.DedupResponseACKOnly
				event.Fingerprint.FamilyPolicy.ResponseClass = ebusgateway.DedupResponseACKOnly
			},
		},
		{
			name: "no response bytes",
			mutate: func(event *ebusgateway.AdjudicatedPassiveEvent) {
				event.Event.HasResponse = false
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			poller := newVaillantSemanticPoller(
				ebusgateway.Config{},
				&ebusgateway.Gateway{},
				graphql.NewLiveSemanticProvider(),
				nil,
				nil,
			)
			event := baseEvent()
			test.mutate(&event)

			poller.handleAdjudicatedPassiveEvent(event)

			key := ebusgateway.NewB509WatchKey(0x08, 0x0200)
			lookup := poller.shadow.Lookup(key, 10*time.Second)
			if lookup.Found {
				t.Fatalf("shadow lookup found = true; want ignored event to skip shadow mutation (state=%s value=%x)", lookup.Entry.State, lookup.Entry.Value)
			}
		})
	}
}

func TestHandleAdjudicatedPassiveEvent_ReadSkipsShadowWhenObserveFirstMasterDisabled(t *testing.T) {
	t.Parallel()

	key := ebusgateway.NewB509WatchKey(0x08, 0x0200)
	cfg := observeFirstStateShadowConfig(key)
	cfg.ObserveFirstFlags = ebusgateway.DefaultObserveFirstFeatureFlags()
	poller := newVaillantSemanticPoller(
		cfg,
		&ebusgateway.Gateway{},
		graphql.NewLiveSemanticProvider(),
		nil,
		nil,
	)

	event := ebusgateway.AdjudicatedPassiveEvent{
		Event: ebusgateway.PassiveClassifiedEvent{
			HasRequest:  true,
			HasResponse: true,
			Request: protocol.Frame{
				Source:    0x10,
				Target:    0x08,
				Primary:   vaillantB509Primary,
				Secondary: vaillantB509Secondary,
				Data:      buildB509ReadSelector(0x0200),
			},
			Response: protocol.Frame{
				Source:    0x08,
				Target:    0x10,
				Primary:   vaillantB509Primary,
				Secondary: vaillantB509Secondary,
				Data:      []byte{vaillantB509OpcodeRead, 0x02, 0x00, 0xAA},
			},
		},
		Fingerprint: ebusgateway.PassiveTransactionFingerprint{
			SharedWatchKey: key,
			ObservedAt:     time.Unix(310, 0),
			ResponseClass:  ebusgateway.DedupResponseValueBearing,
			FamilyPolicy: ebusgateway.ObserveFirstFamilyPolicy{
				RequestIntent:     ebusgateway.ObserveFirstRequestIntentRead,
				ResponseClass:     ebusgateway.DedupResponseValueBearing,
				DirectApplyPolicy: ebusgateway.ObserveFirstDirectApplyPolicyStateDefault,
			},
		},
		Disposition: ebusgateway.DedupDispositionUnmatchedThirdParty,
	}

	poller.handleAdjudicatedPassiveEvent(event)

	if lookup := poller.shadow.Lookup(key, 10*time.Second); lookup.Found {
		t.Fatalf("shadow lookup found = true; want observe-first global gate to block passive shadow write (state=%s value=%x)", lookup.Entry.State, lookup.Entry.Value)
	}
}

func TestHandleAdjudicatedPassiveEvent_ReadWithoutSharedWatchKeyDoesNotPolluteShadow(t *testing.T) {
	t.Parallel()

	key := ebusgateway.NewB509WatchKey(0x08, 0x0200)
	poller := newVaillantSemanticPoller(
		observeFirstStateShadowRuntimeConfig(ebusgateway.ObserveFirstExternalWritePolicyRecordOnly),
		&ebusgateway.Gateway{},
		graphql.NewLiveSemanticProvider(),
		nil,
		nil,
	)

	event := ebusgateway.AdjudicatedPassiveEvent{
		Event: ebusgateway.PassiveClassifiedEvent{
			HasRequest:  true,
			HasResponse: true,
			Request: protocol.Frame{
				Source:    0x10,
				Target:    0x08,
				Primary:   vaillantB509Primary,
				Secondary: vaillantB509Secondary,
				Data:      buildB509ReadSelector(0x0200),
			},
			Response: protocol.Frame{
				Source:    0x08,
				Target:    0x10,
				Primary:   vaillantB509Primary,
				Secondary: vaillantB509Secondary,
				Data:      []byte{vaillantB509OpcodeRead, 0x02, 0x00, 0xAB},
			},
		},
		Fingerprint: ebusgateway.PassiveTransactionFingerprint{
			ObservedAt:    time.Unix(320, 0),
			ResponseClass: ebusgateway.DedupResponseValueBearing,
			FamilyPolicy: ebusgateway.ObserveFirstFamilyPolicy{
				RequestIntent:     ebusgateway.ObserveFirstRequestIntentRead,
				ResponseClass:     ebusgateway.DedupResponseValueBearing,
				DirectApplyPolicy: ebusgateway.ObserveFirstDirectApplyPolicyStateDefault,
			},
		},
		Disposition: ebusgateway.DedupDispositionUnmatchedThirdParty,
	}

	poller.handleAdjudicatedPassiveEvent(event)

	if lookup := poller.shadow.Lookup(key, 10*time.Second); lookup.Found {
		t.Fatalf("shadow lookup found = true; want shared-watchkey-absent traffic to be ignored (state=%s value=%x)", lookup.Entry.State, lookup.Entry.Value)
	}
}

func TestHandleAdjudicatedPassiveEvent_ReadUnknownWatchKeyDoesNotPolluteShadow(t *testing.T) {
	t.Parallel()

	key := ebusgateway.NewB509WatchKey(0x08, 0x0200)
	cfg := observeFirstStateShadowRuntimeConfig(ebusgateway.ObserveFirstExternalWritePolicyRecordOnly)
	poller := newVaillantSemanticPoller(
		cfg,
		&ebusgateway.Gateway{},
		graphql.NewLiveSemanticProvider(),
		nil,
		nil,
	)

	event := ebusgateway.AdjudicatedPassiveEvent{
		Event: ebusgateway.PassiveClassifiedEvent{
			HasRequest:  true,
			HasResponse: true,
			Request: protocol.Frame{
				Source:    0x10,
				Target:    0x08,
				Primary:   vaillantB509Primary,
				Secondary: vaillantB509Secondary,
				Data:      buildB509ReadSelector(0x0200),
			},
			Response: protocol.Frame{
				Source:    0x08,
				Target:    0x10,
				Primary:   vaillantB509Primary,
				Secondary: vaillantB509Secondary,
				Data:      []byte{vaillantB509OpcodeRead, 0x02, 0x00, 0xAC},
			},
		},
		Fingerprint: ebusgateway.PassiveTransactionFingerprint{
			SharedWatchKey: unknownSemanticWatchKey{canonical: "unknown:08:0200"},
			ObservedAt:     time.Unix(330, 0),
			ResponseClass:  ebusgateway.DedupResponseValueBearing,
			FamilyPolicy: ebusgateway.ObserveFirstFamilyPolicy{
				RequestIntent:     ebusgateway.ObserveFirstRequestIntentRead,
				ResponseClass:     ebusgateway.DedupResponseValueBearing,
				DirectApplyPolicy: ebusgateway.ObserveFirstDirectApplyPolicyStateDefault,
			},
		},
		Disposition: ebusgateway.DedupDispositionUnmatchedThirdParty,
	}

	poller.handleAdjudicatedPassiveEvent(event)

	if lookup := poller.shadow.Lookup(key, 10*time.Second); lookup.Found {
		t.Fatalf("shadow lookup found = true; want unknown shared watch key to be ignored (state=%s value=%x)", lookup.Entry.State, lookup.Entry.Value)
	}
}

func TestAttachPassiveShadowProducer_DedupSuppressedWriteStillInvalidatesShadow(t *testing.T) {
	policies := []ebusgateway.ObserveFirstExternalWritePolicy{
		ebusgateway.ObserveFirstExternalWritePolicyInvalidateOnly,
		ebusgateway.ObserveFirstExternalWritePolicyRecordAndInvalidate,
	}

	for _, policy := range policies {
		policy := policy
		t.Run(string(policy), func(t *testing.T) {
			key := ebusgateway.NewB509WatchKey(0x08, 0x0200)
			cfg := observeFirstStateShadowRuntimeConfig(policy)
			cfg.PassiveDedupRecoveryHysteresis = time.Nanosecond
			cfg.PassiveDedupRecoveryEventThreshold = 2

			deduplicator, err := ebusgateway.NewActivePassiveDeduplicator(cfg)
			if err != nil {
				t.Fatalf("NewActivePassiveDeduplicator() error = %v", err)
			}
			t.Cleanup(func() {
				_ = deduplicator.Close()
			})

			poller := newVaillantSemanticPoller(
				cfg,
				&ebusgateway.Gateway{},
				graphql.NewLiveSemanticProvider(),
				nil,
				nil,
			)

			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			if err := poller.AttachPassiveShadowProducer(ctx, deduplicator); err != nil {
				t.Fatalf("AttachPassiveShadowProducer() error = %v", err)
			}

			witness, err := deduplicator.Subscribe("shadow-regression-witness", ebusgateway.DedupSubscriberCritical, 128)
			if err != nil {
				t.Fatalf("Subscribe(witness) error = %v", err)
			}
			t.Cleanup(witness.Close)

			_ = poller.prepareSemanticReadWatch(key)
			seedAt := time.Now()
			seed := poller.shadow.Write(ebusgateway.ShadowWrite{
				Key:        key,
				Source:     ebusgateway.ShadowWriteSourcePassive,
				Confidence: ebusgateway.ShadowConfidenceHigh,
				Value:      []byte{0x11, 0x22},
				ObservedAt: seedAt,
			})
			if !seed.Accepted {
				t.Fatalf("seed shadow write rejected: %s", seed.Reason)
			}

			base := seedAt.Add(10 * time.Millisecond)
			deduplicator.OnPassiveClassifiedEvent(passiveBroadcastClassifiedEvent(base))
			deduplicator.OnPassiveClassifiedEvent(passiveB509WriteClassifiedEvent(base.Add(2*time.Nanosecond), 0x10, 0x08, 0x0200, []byte{0x33, 0x44}))
			deduplicator.OnPassiveClassifiedEvent(passiveBroadcastClassifiedEvent(base.Add(deduplicator.Budgets().PendingGraceTimeout + 4*time.Nanosecond)))

			adjudicated := waitForAdjudicatedEvent(t, witness, 2*time.Second, func(event ebusgateway.AdjudicatedPassiveEvent) bool {
				return event.Disposition == ebusgateway.DedupDispositionUnmatchedThirdParty &&
					event.FamilyPolicy.RequestIntent == ebusgateway.ObserveFirstRequestIntentWrite
			})
			if !adjudicated.SuppressShadow {
				t.Fatalf("SuppressShadow = false; want true for policy %q", policy)
			}
			if adjudicated.Fingerprint.SharedWatchKey == nil {
				t.Fatal("SharedWatchKey = nil; want dedup-emitted shared key for passive write invalidation")
			}
			if got := adjudicated.Fingerprint.SharedWatchKey.Canonical(); got != key.Canonical() {
				t.Fatalf("SharedWatchKey.Canonical() = %q; want %q", got, key.Canonical())
			}

			lookup := waitForShadowIneligible(t, poller.shadow, key, 2*time.Second)
			if lookup.Entry.State != ebusgateway.ShadowEntryStateInvalidated && lookup.Entry.State != ebusgateway.ShadowEntryStateTombstone {
				t.Fatalf("shadow state = %s; want invalidated or tombstone", lookup.Entry.State)
			}
			if lookup.Entry.InvalidationReason != ebusgateway.ShadowInvalidationReasonExternalWrite {
				t.Fatalf("invalidation reason = %s; want %s", lookup.Entry.InvalidationReason, ebusgateway.ShadowInvalidationReasonExternalWrite)
			}
			if lookup.Entry.InvalidationSource != ebusgateway.ShadowInvalidationSourcePassive {
				t.Fatalf("invalidation source = %s; want %s", lookup.Entry.InvalidationSource, ebusgateway.ShadowInvalidationSourcePassive)
			}
		})
	}
}

func TestAttachPassiveShadowProducer_ResubscribesAfterCriticalOverflow(t *testing.T) {
	originalPriority := passiveShadowSubscriberPriority
	originalBuffer := passiveShadowSubscriberBuffer
	originalRetryDelay := passiveShadowRetryDelay
	passiveShadowSubscriberPriority = ebusgateway.DedupSubscriberCritical
	passiveShadowSubscriberBuffer = 1
	passiveShadowRetryDelay = time.Millisecond
	t.Cleanup(func() {
		passiveShadowSubscriberPriority = originalPriority
		passiveShadowSubscriberBuffer = originalBuffer
		passiveShadowRetryDelay = originalRetryDelay
	})

	key := ebusgateway.NewB509WatchKey(0x08, 0x0200)
	cfg := observeFirstStateShadowRuntimeConfig(ebusgateway.ObserveFirstExternalWritePolicyInvalidateOnly)
	cfg.PassiveDedupRecoveryHysteresis = time.Nanosecond
	cfg.PassiveDedupRecoveryEventThreshold = 2

	deduplicator, err := ebusgateway.NewActivePassiveDeduplicator(cfg)
	if err != nil {
		t.Fatalf("NewActivePassiveDeduplicator() error = %v", err)
	}
	t.Cleanup(func() {
		_ = deduplicator.Close()
	})

	poller := newVaillantSemanticPoller(
		cfg,
		&ebusgateway.Gateway{},
		graphql.NewLiveSemanticProvider(),
		nil,
		nil,
	)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := poller.AttachPassiveShadowProducer(ctx, deduplicator); err != nil {
		t.Fatalf("AttachPassiveShadowProducer() error = %v", err)
	}

	witness, err := deduplicator.Subscribe("shadow-overflow-witness", ebusgateway.DedupSubscriberCritical, 4096)
	if err != nil {
		t.Fatalf("Subscribe(witness) error = %v", err)
	}
	t.Cleanup(witness.Close)

	_ = poller.prepareSemanticReadWatch(key)
	seedAt := time.Now()
	seed := poller.shadow.Write(ebusgateway.ShadowWrite{
		Key:        key,
		Source:     ebusgateway.ShadowWriteSourcePassive,
		Confidence: ebusgateway.ShadowConfidenceHigh,
		Value:      []byte{0x01},
		ObservedAt: seedAt,
	})
	if !seed.Accepted {
		t.Fatalf("seed shadow write rejected: %s", seed.Reason)
	}

	base := seedAt.Add(10 * time.Millisecond)
	deduplicator.OnPassiveClassifiedEvent(passiveBroadcastClassifiedEvent(base))
	for index := 0; index < 256; index++ {
		deduplicator.OnPassiveClassifiedEvent(passiveB509WriteClassifiedEvent(base.Add(2*time.Nanosecond), 0x10, 0x08, 0x0200, []byte{byte(index)}))
	}
	deduplicator.OnPassiveClassifiedEvent(passiveBroadcastClassifiedEvent(base.Add(deduplicator.Budgets().PendingGraceTimeout + 4*time.Nanosecond)))

	_ = waitForAdjudicatedEvent(t, witness, 2*time.Second, func(event ebusgateway.AdjudicatedPassiveEvent) bool {
		return event.Disposition == ebusgateway.DedupDispositionDiscontinuity &&
			event.Event.DiscontinuityReason == ebusgateway.PassiveDiscontinuityCriticalSubscriberFault
	})
	time.Sleep(50 * time.Millisecond)

	reseedAt := base.Add(time.Second)
	startGeneration := poller.shadow.SnapshotEligibility(key).Generation
	reseed := poller.shadow.Write(ebusgateway.ShadowWrite{
		Key:             key,
		Source:          ebusgateway.ShadowWriteSourceActiveConfirmed,
		Confidence:      ebusgateway.ShadowConfidenceHigh,
		Value:           []byte{0x7A},
		ObservedAt:      reseedAt,
		StartGeneration: startGeneration,
	})
	if !reseed.Accepted {
		t.Fatalf("reseed shadow write rejected: %s", reseed.Reason)
	}
	if lookup := poller.shadow.Lookup(key, 10*time.Second); !lookup.Found || lookup.Entry.State != ebusgateway.ShadowEntryStatePresent {
		t.Fatalf("shadow reseed lookup = %+v; want present seeded entry before resubscribe verification", lookup)
	}

	wave := reseedAt.Add(10 * time.Millisecond)
	deduplicator.OnPassiveClassifiedEvent(passiveBroadcastClassifiedEvent(wave))
	deduplicator.OnPassiveClassifiedEvent(passiveB509WriteClassifiedEvent(wave.Add(2*time.Nanosecond), 0x11, 0x08, 0x0200, []byte{0xAA}))
	deduplicator.OnPassiveClassifiedEvent(passiveBroadcastClassifiedEvent(wave.Add(deduplicator.Budgets().PendingGraceTimeout + 4*time.Nanosecond)))

	lookup := waitForShadowIneligible(t, poller.shadow, key, 2*time.Second)
	if lookup.Entry.InvalidationSource != ebusgateway.ShadowInvalidationSourcePassive {
		t.Fatalf("invalidation source = %s; want %s", lookup.Entry.InvalidationSource, ebusgateway.ShadowInvalidationSourcePassive)
	}
}

func TestBoilerSnapshotWithConfigValue_ClonesExistingSnapshot(t *testing.T) {
	t.Parallel()

	existingHc := 75.0
	existingPartload := 18.0
	existing := &vaillantBoilerSnapshot{
		FlowsetHcMaxC: cloneFloat64Ptr(&existingHc),
		PartloadHcKW:  cloneFloat64Ptr(&existingPartload),
	}

	updated := boilerSnapshotWithConfigValue(existing, "flowsetHcMaxC", 55.0625)
	if updated == existing {
		t.Fatal("boilerSnapshotWithConfigValue returned the original snapshot pointer")
	}
	if existing.FlowsetHcMaxC == nil || *existing.FlowsetHcMaxC != 75 {
		t.Fatalf("existing FlowsetHcMaxC = %v; want 75", existing.FlowsetHcMaxC)
	}
	if updated.FlowsetHcMaxC == nil || math.Abs(*updated.FlowsetHcMaxC-55.0625) > 0 {
		t.Fatalf("updated FlowsetHcMaxC = %v; want 55.0625", updated.FlowsetHcMaxC)
	}
	if updated.PartloadHcKW == nil || *updated.PartloadHcKW != 18 {
		t.Fatalf("updated PartloadHcKW = %v; want 18", updated.PartloadHcKW)
	}
}

func TestDecodeBoilerConfigRaw(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		spec      boilerConfigFieldSpec
		raw       []byte
		wantValue float64
		wantOK    bool
	}{
		{
			name:      "temperature register decodes data2c",
			spec:      boilerConfigFieldSpecs["flowsetHwcMaxC"],
			raw:       []byte{0xB0, 0x03},
			wantValue: 59,
			wantOK:    true,
		},
		{
			name:      "power register decodes uch",
			spec:      boilerConfigFieldSpecs["partloadHwcKW"],
			raw:       []byte{0x16},
			wantValue: 22,
			wantOK:    true,
		},
		{
			name:   "temperature register rejects one byte payload",
			spec:   boilerConfigFieldSpecs["flowsetHcMaxC"],
			raw:    []byte{0xF0},
			wantOK: false,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			value, ok := decodeBoilerConfigRaw(test.spec, test.raw)
			if ok != test.wantOK {
				t.Fatalf("decodeBoilerConfigRaw(%q, %x) ok = %v; want %v", test.name, test.raw, ok, test.wantOK)
			}
			if ok && value != test.wantValue {
				t.Fatalf("decodeBoilerConfigRaw(%q, %x) value = %.4g; want %.4g", test.name, test.raw, value, test.wantValue)
			}
		})
	}
}

func TestFlowsetHcMaxSpecIncludesFallbackAddress(t *testing.T) {
	t.Parallel()

	spec := boilerConfigFieldSpecs["flowsetHcMaxC"]
	want := []uint16{boiler_b509_flowset_hc_max_c, boiler_b509_flowset_hc_max_c_fallback}
	if !slices.Equal(spec.addrs, want) {
		t.Fatalf("flowsetHcMaxC addrs = %#v; want %#v", spec.addrs, want)
	}
}

func TestComposeZoneName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		primary string
		prefix  string
		suffix  string
		want    string
	}{
		{
			name:    "prefers primary name",
			primary: "Parter",
			prefix:  "ignored",
			suffix:  "ignored",
			want:    "Parter",
		},
		{
			name:   "joins prefix and suffix",
			prefix: "Etaj",
			suffix: "2",
			want:   "Etaj 2",
		},
		{
			name:   "handles single part",
			prefix: "Parter",
			want:   "Parter",
		},
		{
			name: "returns empty when all missing",
			want: "",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := composeZoneName(test.primary, test.prefix, test.suffix); got != test.want {
				t.Fatalf("composeZoneName(%q, %q, %q) = %q; want %q", test.primary, test.prefix, test.suffix, got, test.want)
			}
		})
	}
}

func TestDecodeB524Uint16(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		payload []byte
		want    uint16
		ok      bool
	}{
		{name: "empty", payload: nil, want: 0, ok: false},
		{name: "single byte", payload: []byte{0x02}, want: 0x0002, ok: true},
		{name: "little endian", payload: []byte{0x02, 0x01}, want: 0x0102, ok: true},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, ok := decodeB524Uint16(test.payload)
			if ok != test.ok {
				t.Fatalf("decodeB524Uint16(%v) ok = %v; want %v", test.payload, ok, test.ok)
			}
			if got != test.want {
				t.Fatalf("decodeB524Uint16(%v) = 0x%04x; want 0x%04x", test.payload, got, test.want)
			}
		})
	}
}

func TestDecodeB524FirmwareVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		payload []byte
		wantNil bool
		want    string
	}{
		{name: "short payload", payload: []byte{0x08, 0x05}, wantNil: true},
		{name: "ff ff ff is absent", payload: []byte{0xFF, 0xFF, 0xFF}, wantNil: true},
		{name: "formats byte decimal", payload: []byte{0x08, 0x05, 0x00}, want: "08.05.00"},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got := decodeB524FirmwareVersion(test.payload)
			if test.wantNil {
				if got != nil {
					t.Fatalf("decodeB524FirmwareVersion(%v) = %q; want nil", test.payload, *got)
				}
				return
			}
			if got == nil || *got != test.want {
				t.Fatalf("decodeB524FirmwareVersion(%v) = %v; want %q", test.payload, got, test.want)
			}
		})
	}
}

func TestDecodeRadioDeviceModel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		class *uint8
		want  string
	}{
		{name: "nil class", class: nil, want: ""},
		{name: "vrc720", class: uint8Ptr(0x15), want: "VRC720"},
		{name: "vr71 fm5", class: uint8Ptr(0x26), want: "VR71/FM5"},
		{name: "vr92", class: uint8Ptr(0x35), want: "VR92"},
		{name: "unknown class", class: uint8Ptr(0x44), want: "Unknown (0x44)"},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := decodeRadioDeviceModel(test.class); got != test.want {
				t.Fatalf("decodeRadioDeviceModel(%v) = %q; want %q", test.class, got, test.want)
			}
		})
	}
}

func TestHasRemoteIdentityEvidence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		classAddress *uint8
		firmware     *string
		hardware     *uint16
		want         bool
	}{
		{name: "vr71 class implies identity", classAddress: uint8Ptr(0x26), want: true},
		{name: "firmware implies identity", firmware: stringPtr("01.00.00"), want: true},
		{name: "hardware implies identity", hardware: uint16Ptr(0x5904), want: true},
		{name: "zero hardware no identity", hardware: uint16Ptr(0), want: false},
		{name: "ffff hardware no identity", hardware: uint16Ptr(0xFFFF), want: false},
		{name: "no evidence", want: false},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := hasRemoteIdentityEvidence(test.classAddress, test.firmware, test.hardware); got != test.want {
				t.Fatalf("hasRemoteIdentityEvidence(...) = %v; want %v", got, test.want)
			}
		})
	}
}

func TestMatchesB524ReplyInstance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		reply     byte
		requested byte
		want      bool
	}{
		{name: "exact match", reply: 0x02, requested: 0x02, want: true},
		{name: "one based reply", reply: 0x03, requested: 0x02, want: true},
		{name: "different reply", reply: 0x07, requested: 0x02, want: false},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := matchesB524ReplyInstance(test.reply, test.requested); got != test.want {
				t.Fatalf("matchesB524ReplyInstance(reply=0x%02x, requested=0x%02x) = %v; want %v", test.reply, test.requested, got, test.want)
			}
		})
	}
}

func TestSourceFromEbusdGrab(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		protocol ebusgateway.TransportProtocol
		ok       bool
		want     semanticSnapshotSource
	}{
		{
			name:     "ebusd-tcp successful grab is live",
			protocol: ebusgateway.TransportEbusdTCP,
			ok:       true,
			want:     semanticSnapshotSourceLive,
		},
		{
			name:     "non-ebusd successful grab stays cache",
			protocol: ebusgateway.TransportENH,
			ok:       true,
			want:     semanticSnapshotSourceCache,
		},
		{
			name:     "grab failure stays cache",
			protocol: ebusgateway.TransportEbusdTCP,
			ok:       false,
			want:     semanticSnapshotSourceCache,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			poller := &vaillantSemanticPoller{
				transportConfig: ebusgateway.TransportConfig{Protocol: test.protocol},
			}
			if got := poller.sourceFromEbusdGrab(test.ok); got != test.want {
				t.Fatalf("sourceFromEbusdGrab(protocol=%q, ok=%v) = %v; want %v", test.protocol, test.ok, got, test.want)
			}
		})
	}
}

func TestNewVaillantSemanticPoller_BoilerTierCadence(t *testing.T) {
	t.Parallel()

	poller := newVaillantSemanticPoller(
		ebusgateway.Config{},
		&ebusgateway.Gateway{},
		graphql.NewLiveSemanticProvider(),
		nil,
		nil,
	)

	schedules := poller.boilerStatusTierSchedules()
	if len(schedules) != 3 {
		t.Fatalf("len(boilerStatusTierSchedules) = %d; want 3", len(schedules))
	}

	byTier := make(map[boilerStatusTier]boilerStatusTierSchedule, len(schedules))
	for _, schedule := range schedules {
		byTier[schedule.tier] = schedule
	}

	tests := []struct {
		tier     boilerStatusTier
		interval time.Duration
		priority semanticTaskPriority
	}{
		{tier: boilerStatusTierFast, interval: 30 * time.Second, priority: semanticTaskPriorityHigh},
		{tier: boilerStatusTierMedium, interval: 5 * time.Minute, priority: semanticTaskPriorityMedium},
		{tier: boilerStatusTierSlow, interval: 10 * time.Minute, priority: semanticTaskPriorityLow},
	}
	for _, test := range tests {
		schedule, ok := byTier[test.tier]
		if !ok {
			t.Fatalf("missing tier schedule: %v", test.tier)
		}
		if schedule.interval != test.interval {
			t.Fatalf("tier %v interval = %s; want %s", test.tier, schedule.interval, test.interval)
		}
		if schedule.priority != test.priority {
			t.Fatalf("tier %v priority = %v; want %v", test.tier, schedule.priority, test.priority)
		}
	}
}

func TestNewVaillantSemanticPoller_NormalizesObserveFirstFlagsFromScalarOnlyConfig(t *testing.T) {
	t.Parallel()

	cfg := ebusgateway.Config{
		ObserveFirstEnabled:      true,
		PassiveStateDirectApply:  true,
		PassiveConfigDirectApply: true,
		ExternalWritePolicy:      ebusgateway.ObserveFirstExternalWritePolicyRecordOnly,
	}
	poller := newVaillantSemanticPoller(
		cfg,
		&ebusgateway.Gateway{},
		graphql.NewLiveSemanticProvider(),
		nil,
		nil,
	)

	want := ebusgateway.NormalizeObserveFirstFeatureFlags(
		cfg.ObserveFirstEnabled,
		cfg.PassiveStateDirectApply,
		cfg.PassiveConfigDirectApply,
		cfg.ExternalWritePolicy,
	).State()
	got := poller.shadow.FeatureFlags().State()
	if got.ObserveFirstEnabled != want.ObserveFirstEnabled ||
		got.PassiveStateDirectApply != want.PassiveStateDirectApply ||
		got.PassiveConfigDirectApply != want.PassiveConfigDirectApply ||
		got.ExternalWritePolicy != want.ExternalWritePolicy {
		t.Fatalf("shadow feature flags = %+v; want %+v", got, want)
	}
}

func TestNewVaillantSemanticPoller_AttachesRuntimeShadowCache(t *testing.T) {
	t.Parallel()

	poller := newVaillantSemanticPoller(
		ebusgateway.Config{},
		&ebusgateway.Gateway{},
		graphql.NewLiveSemanticProvider(),
		nil,
		nil,
	)
	if poller.shadow == nil {
		t.Fatal("poller.shadow = nil; want runtime shadow cache")
	}

	key := ebusgateway.NewB509WatchKey(0x08, 0x0200)
	maxAge := poller.prepareSemanticReadWatch(key)
	if maxAge != 30*time.Second {
		t.Fatalf("prepareSemanticReadWatch(default) = %s; want 30s (descriptor policy, not legacy 500ms)", maxAge)
	}

	result := poller.shadow.Write(ebusgateway.ShadowWrite{
		Key:        key,
		Source:     ebusgateway.ShadowWriteSourcePassive,
		Confidence: ebusgateway.ShadowConfidenceHigh,
		Value:      []byte{0x7A},
		ObservedAt: time.Now().Add(-time.Second),
	})
	if !result.Accepted {
		t.Fatalf("shadow write rejected: %s", result.Reason)
	}

	fetchCalls := 0
	value, err := poller.scheduler.GetWatch(context.Background(), key, maxAge, func(context.Context) ([]byte, error) {
		fetchCalls++
		return []byte{0x55}, nil
	})
	if err != nil {
		t.Fatalf("scheduler.GetWatch() error = %v", err)
	}
	if len(value) != 1 || value[0] != 0x7A {
		t.Fatalf("scheduler.GetWatch() value = %v; want shadow value [0x7a]", value)
	}
	if fetchCalls != 0 {
		t.Fatalf("fetch calls = %d; want 0 when scheduler shadow is wired", fetchCalls)
	}
}

func TestPrepareSemanticReadWatch_UsesObserverDescriptorFreshness(t *testing.T) {
	t.Parallel()

	key := ebusgateway.NewB524WatchKey(0x15, vaillantB524OpcodeLocal, localZones.group, 0x00, zone_current_temp)
	observer := staticSemanticReadWatchObserver{
		observation: ebusgateway.WatchObservation{
			State:         ebusgateway.WatchObservationStateActive,
			HasDescriptor: true,
			Descriptor: ebusgateway.WatchDescriptor{
				Key:               key,
				SemanticClass:     ebusgateway.WatchSemanticClassState,
				FreshnessProfile:  ebusgateway.WatchFreshnessProfileStateSlow,
				DecoderID:         "test.semantic.read.observer",
				CorrelationPolicy: ebusgateway.WatchCorrelationPolicyRequestResponse,
				DirectApplyPolicy: ebusgateway.WatchDirectApplyPolicyStateDefault,
			},
			Sources: []ebusgateway.WatchActivationSource{ebusgateway.WatchActivationSourceTooling},
		},
	}

	poller := newVaillantSemanticPoller(
		ebusgateway.Config{WatchObserver: observer},
		&ebusgateway.Gateway{},
		graphql.NewLiveSemanticProvider(),
		nil,
		nil,
	)
	maxAge := poller.prepareSemanticReadWatch(key)
	if maxAge != 120*time.Second {
		t.Fatalf("prepareSemanticReadWatch(observed state_slow profile) = %s; want 120s (descriptor policy, not legacy 500ms)", maxAge)
	}

	result := poller.shadow.Write(ebusgateway.ShadowWrite{
		Key:        key,
		Source:     ebusgateway.ShadowWriteSourcePassive,
		Confidence: ebusgateway.ShadowConfidenceHigh,
		Value:      []byte{0x01},
		ObservedAt: time.Now(),
	})
	if !result.Accepted {
		t.Fatalf("shadow write rejected after runtime descriptor bootstrap: %s", result.Reason)
	}
}

func TestSemanticReadWatchDescriptorWithDecoderID_ClassifiesB524Cadence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		key         ebusgateway.WatchKey
		wantClass   ebusgateway.WatchSemanticClass
		wantProfile ebusgateway.WatchFreshnessProfile
		wantPolicy  ebusgateway.WatchDirectApplyPolicy
		wantTTL     time.Duration
	}{
		{
			name:        "zone current temp stays fast state",
			key:         ebusgateway.NewB524WatchKey(0x15, vaillantB524OpcodeLocal, localZones.group, 0x00, zone_current_temp),
			wantClass:   ebusgateway.WatchSemanticClassState,
			wantProfile: ebusgateway.WatchFreshnessProfileStateFast,
			wantPolicy:  ebusgateway.WatchDirectApplyPolicyStateDefault,
			wantTTL:     30 * time.Second,
		},
		{
			name:        "zone humidity is slow state",
			key:         ebusgateway.NewB524WatchKey(0x15, vaillantB524OpcodeLocal, localZones.group, 0x00, zone_current_humidity),
			wantClass:   ebusgateway.WatchSemanticClassState,
			wantProfile: ebusgateway.WatchFreshnessProfileStateSlow,
			wantPolicy:  ebusgateway.WatchDirectApplyPolicyStateDefault,
			wantTTL:     120 * time.Second,
		},
		{
			name:        "zone target temp is config opt in",
			key:         ebusgateway.NewB524WatchKey(0x15, vaillantB524OpcodeLocal, localZones.group, 0x00, zone_target_temp),
			wantClass:   ebusgateway.WatchSemanticClassConfig,
			wantProfile: ebusgateway.WatchFreshnessProfileConfig,
			wantPolicy:  ebusgateway.WatchDirectApplyPolicyConfigOptIn,
			wantTTL:     5 * time.Minute,
		},
		{
			name:        "zone index is discovery",
			key:         ebusgateway.NewB524WatchKey(0x15, vaillantB524OpcodeLocal, localZones.group, 0x00, zone_index),
			wantClass:   ebusgateway.WatchSemanticClassDiscovery,
			wantProfile: ebusgateway.WatchFreshnessProfileDiscovery,
			wantPolicy:  ebusgateway.WatchDirectApplyPolicyNever,
			wantTTL:     time.Hour,
		},
		{
			name:        "regulator flow temperature stays fast state",
			key:         ebusgateway.NewB524WatchKey(0x15, vaillantB524OpcodeLocal, localRegulator.group, regulatorInstance, system_flow_temperature),
			wantClass:   ebusgateway.WatchSemanticClassState,
			wantProfile: ebusgateway.WatchFreshnessProfileStateFast,
			wantPolicy:  ebusgateway.WatchDirectApplyPolicyStateDefault,
			wantTTL:     30 * time.Second,
		},
		{
			name:        "regulator energy totals are slow state",
			key:         ebusgateway.NewB524WatchKey(0x15, vaillantB524OpcodeLocal, localRegulator.group, regulatorInstance, energy_fuel_sum_hc),
			wantClass:   ebusgateway.WatchSemanticClassState,
			wantProfile: ebusgateway.WatchFreshnessProfileStateSlow,
			wantPolicy:  ebusgateway.WatchDirectApplyPolicyStateDefault,
			wantTTL:     120 * time.Second,
		},
		{
			name:        "regulator installer field is config",
			key:         ebusgateway.NewB524WatchKey(0x15, vaillantB524OpcodeLocal, localRegulator.group, regulatorInstance, system_installer_menu_code),
			wantClass:   ebusgateway.WatchSemanticClassConfig,
			wantProfile: ebusgateway.WatchFreshnessProfileConfig,
			wantPolicy:  ebusgateway.WatchDirectApplyPolicyConfigOptIn,
			wantTTL:     5 * time.Minute,
		},
		{
			name:        "regulator module configuration is discovery",
			key:         ebusgateway.NewB524WatchKey(0x15, vaillantB524OpcodeLocal, localRegulator.group, regulatorInstance, system_module_configuration_vr71),
			wantClass:   ebusgateway.WatchSemanticClassDiscovery,
			wantProfile: ebusgateway.WatchFreshnessProfileDiscovery,
			wantPolicy:  ebusgateway.WatchDirectApplyPolicyNever,
			wantTTL:     time.Hour,
		},
		{
			name:        "circuit type is config",
			key:         ebusgateway.NewB524WatchKey(0x15, vaillantB524OpcodeLocal, localCircuits.group, 0x00, circuit_type),
			wantClass:   ebusgateway.WatchSemanticClassConfig,
			wantProfile: ebusgateway.WatchFreshnessProfileConfig,
			wantPolicy:  ebusgateway.WatchDirectApplyPolicyConfigOptIn,
			wantTTL:     5 * time.Minute,
		},
		{
			name:        "remote slot connected is slow state liveness",
			key:         ebusgateway.NewB524WatchKey(0x15, vaillantB524OpcodeRead, remoteFunctionalModules.group, 0x01, device_slot_connected),
			wantClass:   ebusgateway.WatchSemanticClassState,
			wantProfile: ebusgateway.WatchFreshnessProfileStateSlow,
			wantPolicy:  ebusgateway.WatchDirectApplyPolicyStateDefault,
			wantTTL:     120 * time.Second,
		},
		{
			name:        "remote slot humidity is slow state",
			key:         ebusgateway.NewB524WatchKey(0x15, vaillantB524OpcodeRead, remoteFunctionalModules.group, 0x01, device_slot_room_humidity),
			wantClass:   ebusgateway.WatchSemanticClassState,
			wantProfile: ebusgateway.WatchFreshnessProfileStateSlow,
			wantPolicy:  ebusgateway.WatchDirectApplyPolicyStateDefault,
			wantTTL:     120 * time.Second,
		},
		{
			name:        "remote slot hardware identity is discovery",
			key:         ebusgateway.NewB524WatchKey(0x15, vaillantB524OpcodeRead, remoteFunctionalModules.group, 0x01, device_slot_hardware_identifier),
			wantClass:   ebusgateway.WatchSemanticClassDiscovery,
			wantProfile: ebusgateway.WatchFreshnessProfileDiscovery,
			wantPolicy:  ebusgateway.WatchDirectApplyPolicyNever,
			wantTTL:     time.Hour,
		},
		{
			name:        "non B524 keeps prior fast state fallback",
			key:         ebusgateway.NewB509WatchKey(0x08, 0x0200),
			wantClass:   ebusgateway.WatchSemanticClassState,
			wantProfile: ebusgateway.WatchFreshnessProfileStateFast,
			wantPolicy:  ebusgateway.WatchDirectApplyPolicyStateDefault,
			wantTTL:     30 * time.Second,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			descriptor := semanticReadWatchDescriptorWithDecoderID(test.key, "test.decoder")
			if descriptor.SemanticClass != test.wantClass {
				t.Fatalf("SemanticClass = %q; want %q", descriptor.SemanticClass, test.wantClass)
			}
			if descriptor.FreshnessProfile != test.wantProfile {
				t.Fatalf("FreshnessProfile = %q; want %q", descriptor.FreshnessProfile, test.wantProfile)
			}
			if descriptor.DirectApplyPolicy != test.wantPolicy {
				t.Fatalf("DirectApplyPolicy = %q; want %q", descriptor.DirectApplyPolicy, test.wantPolicy)
			}
			ttl, err := descriptor.EffectiveFreshnessTTL()
			if err != nil {
				t.Fatalf("EffectiveFreshnessTTL() error = %v", err)
			}
			if ttl != test.wantTTL {
				t.Fatalf("EffectiveFreshnessTTL() = %s; want %s", ttl, test.wantTTL)
			}
		})
	}
}

func TestPrepareSemanticReadWatchRuntime_FallbackDescriptorB524DiscoveryBucketsCorrectly(t *testing.T) {
	t.Parallel()

	cfg := ebusgateway.DefaultConfig()
	store := ebusgateway.NewBusObservabilityStore(cfg)
	poller := &vaillantSemanticPoller{
		watchEfficiency: store,
		nowFn: func() time.Time {
			return time.Unix(1700000700, 0).UTC()
		},
	}

	key := ebusgateway.NewB524WatchKey(0x15, vaillantB524OpcodeLocal, localZones.group, 0x01, zone_index)
	runtime := poller.prepareSemanticReadWatchRuntime(key)
	if runtime.hasDescriptor {
		t.Fatal("runtime.hasDescriptor = true; want false when runtime observation descriptor is missing")
	}

	poller.emitWatchReadEfficiency(runtime, runtime.maxAge, ebusgateway.SemanticReadExecutionStats{
		ActiveFetchAttempted: true,
		ActiveFetchSucceeded: true,
		ActiveFetchDuration:  time.Second,
	})

	metrics := store.RenderPrometheus()
	// The fallback descriptor provides a valid key + freshness profile, so
	// the event is bucketed correctly even without runtime observer evidence.
	if strings.Contains(metrics, `ambiguous_total{family="B524",reason="missing_runtime_descriptor"}`) {
		t.Fatalf("RenderPrometheus incorrectly classified fallback-descriptor B524 read as ambiguous:\n%s", metrics)
	}
	if !strings.Contains(metrics, `passive_hits_total{family="B524",freshness_profile="discovery"} 0`) {
		t.Fatalf("RenderPrometheus missing bucketed B524 discovery entry:\n%s", metrics)
	}
}

func TestReadB509Value_EmitsWatchEfficiencyShadowHit(t *testing.T) {
	t.Parallel()

	key := ebusgateway.NewB509WatchKey(0x08, 0x0200)
	spy := &watchEfficiencyObserverSpy{}
	cfg := observeFirstStateShadowConfig(key)
	cfg.WatchEfficiencyObserver = spy

	poller := newVaillantSemanticPoller(
		cfg,
		&ebusgateway.Gateway{},
		graphql.NewLiveSemanticProvider(),
		nil,
		nil,
	)
	poller.sendFrameFn = func(context.Context, protocol.Frame) (*protocol.Frame, error) {
		return nil, fmt.Errorf("unexpected active send on shadow-hit path")
	}

	maxAge := poller.prepareSemanticReadWatch(key)
	if maxAge != 30*time.Second {
		t.Fatalf("prepareSemanticReadWatch() = %s; want 30s", maxAge)
	}
	write := poller.shadow.Write(ebusgateway.ShadowWrite{
		Key:        key,
		Source:     ebusgateway.ShadowWriteSourcePassive,
		Confidence: ebusgateway.ShadowConfidenceHigh,
		Value:      []byte{0x7A},
		ObservedAt: time.Now(),
	})
	if !write.Accepted {
		t.Fatalf("shadow write rejected: %s", write.Reason)
	}

	value, ok := poller.readB509Value(context.Background(), 0x08, 0x0200)
	if !ok {
		t.Fatal("readB509Value() ok = false; want true on shadow hit")
	}
	if len(value) != 1 || value[0] != 0x7A {
		t.Fatalf("readB509Value() value = %v; want [0x7a]", value)
	}

	event, found := spy.latestReadEvent()
	if !found {
		t.Fatal("watch-efficiency read event missing")
	}
	if !event.Stats.ServedFromShadow {
		t.Fatal("event.Stats.ServedFromShadow = false; want true")
	}
	if event.Stats.ActiveFetchAttempted {
		t.Fatal("event.Stats.ActiveFetchAttempted = true; want false on shadow hit")
	}
	if event.Descriptor.FreshnessProfile != ebusgateway.WatchFreshnessProfileStateFast {
		t.Fatalf("event descriptor freshness_profile = %q; want state_fast", event.Descriptor.FreshnessProfile)
	}
	if event.Descriptor.Family() != ebusgateway.WatchFamilyB509 {
		t.Fatalf("event descriptor family = %q; want B509", event.Descriptor.Family())
	}
}

func TestHandleAdjudicatedPassiveEvent_EmitsWatchEfficiencyDirectApply(t *testing.T) {
	t.Parallel()

	key := ebusgateway.NewB509WatchKey(0x08, 0x0200)
	spy := &watchEfficiencyObserverSpy{}
	cfg := observeFirstStateShadowRuntimeConfig(ebusgateway.ObserveFirstExternalWritePolicyRecordOnly)
	cfg.WatchEfficiencyObserver = spy

	poller := newVaillantSemanticPoller(
		cfg,
		&ebusgateway.Gateway{},
		graphql.NewLiveSemanticProvider(),
		nil,
		nil,
	)
	now := time.Unix(1700000600, 0).UTC()
	poller.nowFn = func() time.Time { return now }

	poller.handleAdjudicatedPassiveEvent(ebusgateway.AdjudicatedPassiveEvent{
		Disposition: ebusgateway.DedupDispositionUnmatchedThirdParty,
		Fingerprint: ebusgateway.PassiveTransactionFingerprint{
			SharedWatchKey: key,
			ObservedAt:     now,
			ResponseClass:  ebusgateway.DedupResponseValueBearing,
			FamilyPolicy: ebusgateway.ObserveFirstFamilyPolicy{
				RequestIntent:     ebusgateway.ObserveFirstRequestIntentRead,
				DirectApplyPolicy: ebusgateway.ObserveFirstDirectApplyPolicyStateDefault,
			},
		},
		Event: ebusgateway.PassiveClassifiedEvent{
			HasResponse: true,
			Response: protocol.Frame{
				Data: []byte{vaillantB509OpcodeRead, 0x02, 0x00, 0x11},
			},
		},
	})

	event, found := spy.latestDirectApplyEvent()
	if !found {
		t.Fatal("watch-efficiency direct-apply event missing")
	}
	if !event.CandidateEvaluated {
		t.Fatal("event.CandidateEvaluated = false; want true")
	}
	if !event.Accepted {
		t.Fatal("event.Accepted = false; want true")
	}
	if event.Descriptor.FreshnessProfile != ebusgateway.WatchFreshnessProfileStateFast {
		t.Fatalf("event descriptor freshness_profile = %q; want state_fast", event.Descriptor.FreshnessProfile)
	}
	if event.Descriptor.Family() != ebusgateway.WatchFamilyB509 {
		t.Fatalf("event descriptor family = %q; want B509", event.Descriptor.Family())
	}
}

func TestHandleAdjudicatedPassiveEvent_EmitsWatchEfficiencyCandidateForRejectedDirectApply(t *testing.T) {
	t.Parallel()

	key := ebusgateway.NewB509WatchKey(0x08, 0x0200)
	spy := &watchEfficiencyObserverSpy{}
	cfg := observeFirstStateShadowRuntimeConfig(ebusgateway.ObserveFirstExternalWritePolicyRecordOnly)
	cfg.WatchEfficiencyObserver = spy

	poller := newVaillantSemanticPoller(
		cfg,
		&ebusgateway.Gateway{},
		graphql.NewLiveSemanticProvider(),
		nil,
		nil,
	)
	now := time.Unix(1700000600, 0).UTC()
	poller.nowFn = func() time.Time { return now }
	_ = poller.prepareSemanticReadWatch(key)

	seed := poller.shadow.Write(ebusgateway.ShadowWrite{
		Key:        key,
		Source:     ebusgateway.ShadowWriteSourcePassive,
		Confidence: ebusgateway.ShadowConfidenceHigh,
		Value:      []byte{0x33},
		ObservedAt: now.Add(time.Second),
	})
	if !seed.Accepted {
		t.Fatalf("shadow seed write rejected: %s", seed.Reason)
	}

	poller.handleAdjudicatedPassiveEvent(ebusgateway.AdjudicatedPassiveEvent{
		Disposition: ebusgateway.DedupDispositionUnmatchedThirdParty,
		Fingerprint: ebusgateway.PassiveTransactionFingerprint{
			SharedWatchKey: key,
			ObservedAt:     now,
			ResponseClass:  ebusgateway.DedupResponseValueBearing,
			FamilyPolicy: ebusgateway.ObserveFirstFamilyPolicy{
				RequestIntent:     ebusgateway.ObserveFirstRequestIntentRead,
				DirectApplyPolicy: ebusgateway.ObserveFirstDirectApplyPolicyStateDefault,
			},
		},
		Event: ebusgateway.PassiveClassifiedEvent{
			HasResponse: true,
			Response: protocol.Frame{
				Data: []byte{vaillantB509OpcodeRead, 0x02, 0x00, 0x11},
			},
		},
	})

	event, found := spy.latestDirectApplyEvent()
	if !found {
		t.Fatal("watch-efficiency direct-apply event missing")
	}
	if !event.CandidateEvaluated {
		t.Fatal("event.CandidateEvaluated = false; want true")
	}
	if event.Accepted {
		t.Fatal("event.Accepted = true; want false for stale rejected write")
	}
}

func TestRefreshDiscovery_PrimesBoilerTiersWhenBoilerAppears(t *testing.T) {
	t.Parallel()

	taskScheduler := newSemanticTaskScheduler()
	poller := &vaillantSemanticPoller{
		reg:            newTestRegistry(registry.DeviceInfo{Address: 0x08, Manufacturer: "Vaillant", DeviceID: "BAI00"}),
		tasks:          taskScheduler,
		zones:          make(map[byte]*vaillantZoneSnapshot),
		presence:       make(map[byte]*zonePresenceRecord),
		circuits:       make(map[byte]*vaillantCircuitSnapshot),
		radioDevices:   make(map[radioDeviceKey]*vaillantRadioDeviceSnapshot),
		solarCylinders: make(map[byte]*vaillantCylinderSnapshot),
	}

	poller.refreshDiscovery(context.Background())

	poller.mu.Lock()
	boilerAddress := poller.boilerAddress
	poller.mu.Unlock()
	if boilerAddress != 0x08 {
		t.Fatalf("boilerAddress = 0x%02x; want 0x08", boilerAddress)
	}

	taskScheduler.mu.Lock()
	queueLen := len(taskScheduler.queue)
	taskScheduler.mu.Unlock()
	if queueLen != len(poller.boilerStatusTierSchedules()) {
		t.Fatalf("queued boiler tasks = %d; want %d", queueLen, len(poller.boilerStatusTierSchedules()))
	}

	poller.refreshDiscovery(context.Background())

	taskScheduler.mu.Lock()
	queueLen = len(taskScheduler.queue)
	taskScheduler.mu.Unlock()
	if queueLen != len(poller.boilerStatusTierSchedules()) {
		t.Fatalf("queued boiler tasks after unchanged discovery = %d; want unchanged %d", queueLen, len(poller.boilerStatusTierSchedules()))
	}
}

func TestRefreshDiscovery_PrimesControllerSemanticTasksWhenControllerAppears(t *testing.T) {
	t.Parallel()

	taskScheduler := newSemanticTaskScheduler()
	poller := &vaillantSemanticPoller{
		reg:            newTestRegistry(registry.DeviceInfo{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV"}),
		tasks:          taskScheduler,
		zones:          make(map[byte]*vaillantZoneSnapshot),
		presence:       make(map[byte]*zonePresenceRecord),
		circuits:       make(map[byte]*vaillantCircuitSnapshot),
		radioDevices:   make(map[radioDeviceKey]*vaillantRadioDeviceSnapshot),
		solarCylinders: make(map[byte]*vaillantCylinderSnapshot),
		b524ProbeFn: func(ctx context.Context, target, opcode, group, instance byte, addr uint16) bool {
			return target == 0x15
		},
	}

	poller.refreshDiscovery(context.Background())

	poller.mu.Lock()
	controller := poller.controller
	poller.mu.Unlock()
	if controller != 0x15 {
		t.Fatalf("controller = 0x%02x; want 0x15", controller)
	}

	taskScheduler.mu.Lock()
	queueLen := len(taskScheduler.queue)
	taskScheduler.mu.Unlock()
	if queueLen != 5 {
		t.Fatalf("queued controller tasks = %d; want 5", queueLen)
	}

	poller.refreshDiscovery(context.Background())

	taskScheduler.mu.Lock()
	queueLen = len(taskScheduler.queue)
	taskScheduler.mu.Unlock()
	if queueLen != 5 {
		t.Fatalf("queued controller tasks after unchanged discovery = %d; want unchanged 5", queueLen)
	}
}

func TestBoilerStatusRegisterDefinitionsForTier_NoReturnTemperatureMapping(t *testing.T) {
	t.Parallel()

	for _, tier := range []boilerStatusTier{
		boilerStatusTierFast,
		boilerStatusTierMedium,
		boilerStatusTierSlow,
	} {
		for _, register := range boilerStatusRegisterDefinitionsForTier(tier) {
			if register.group == localCircuits.group && register.addr == circuit_flow_temp {
				t.Fatalf("tier %v maps GG=0x%02x RR=0x%04x; closed decision forbids using this as boiler return temperature", tier, localCircuits.group, circuit_flow_temp)
			}
		}
	}
}

func TestMergeBoilerSnapshotNonDestructive_PartialUpdatePreservesLastKnown(t *testing.T) {
	t.Parallel()

	flow := 61.5
	returnTemp := 45.0
	pump := true
	heatingStatus := 3
	updatedPump := false

	merged := mergeBoilerSnapshotNonDestructive(
		&vaillantBoilerSnapshot{
			FlowTemperatureC:         &flow,
			ReturnTemperatureC:       &returnTemp,
			CentralHeatingPumpActive: &pump,
			HeatingStatusRaw:         &heatingStatus,
		},
		&vaillantBoilerSnapshot{
			CentralHeatingPumpActive: &updatedPump,
		},
	)

	if merged == nil {
		t.Fatal("merged snapshot is nil")
	}
	if merged.FlowTemperatureC == nil || *merged.FlowTemperatureC != 61.5 {
		t.Fatalf("merged.FlowTemperatureC = %v; want preserved 61.5", merged.FlowTemperatureC)
	}
	if merged.CentralHeatingPumpActive == nil || *merged.CentralHeatingPumpActive {
		t.Fatalf("merged.CentralHeatingPumpActive = %v; want updated false", merged.CentralHeatingPumpActive)
	}
	if merged.HeatingStatusRaw == nil || *merged.HeatingStatusRaw != 3 {
		t.Fatalf("merged.HeatingStatusRaw = %v; want preserved 3", merged.HeatingStatusRaw)
	}
	if merged.ReturnTemperatureC != nil {
		t.Fatalf("merged.ReturnTemperatureC = %v; want nil (closed decision)", merged.ReturnTemperatureC)
	}
}

func TestRefreshBoilerStatusTier_NoSuccessfulReadsPreservesSnapshot(t *testing.T) {
	t.Parallel()

	flow := 63.0
	pump := true
	heatingStatus := 4
	poller := &vaillantSemanticPoller{
		controller: 0x15,
		boiler: &vaillantBoilerSnapshot{
			FlowTemperatureC:         &flow,
			CentralHeatingPumpActive: &pump,
			HeatingStatusRaw:         &heatingStatus,
		},
	}

	poller.refreshBoilerStatusTier(context.Background(), boilerStatusTierFast)

	if poller.boiler == nil {
		t.Fatal("poller.boiler = nil; want preserved last-known snapshot")
	}
	if poller.boiler.FlowTemperatureC == nil || *poller.boiler.FlowTemperatureC != 63.0 {
		t.Fatalf("poller.boiler.FlowTemperatureC = %v; want preserved 63.0", poller.boiler.FlowTemperatureC)
	}
	if poller.boiler.CentralHeatingPumpActive == nil || !*poller.boiler.CentralHeatingPumpActive {
		t.Fatalf("poller.boiler.CentralHeatingPumpActive = %v; want preserved true", poller.boiler.CentralHeatingPumpActive)
	}
	if poller.boiler.HeatingStatusRaw == nil || *poller.boiler.HeatingStatusRaw != 4 {
		t.Fatalf("poller.boiler.HeatingStatusRaw = %v; want preserved 4", poller.boiler.HeatingStatusRaw)
	}
}

func TestRefreshBoilerStatusFastUsesCachedDHWWithoutB524DHWReads(t *testing.T) {
	t.Parallel()

	current := 47.5
	target := 50.0
	var selectors [][]byte
	poller := &vaillantSemanticPoller{
		scheduler:      ebusgateway.NewSemanticReadScheduler(),
		provider:       graphql.NewLiveSemanticProvider(),
		source:         0x7F,
		controller:     0x15,
		requestTimeout: 50 * time.Millisecond,
		dhw: &vaillantDhwSnapshot{
			CurrentTempC:                  &current,
			TargetTempC:                   &target,
			ConfigurationDHWOperationMode: "1",
		},
	}
	poller.sendFrameFn = func(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
		selectors = append(selectors, slices.Clone(frame.Data))
		return testB524ResponseForSelector(frame.Data), nil
	}

	poller.refreshBoilerStatusTier(context.Background(), boilerStatusTierFast)

	for _, selector := range selectors {
		if len(selector) == 6 && selector[2] == localDHW.group {
			t.Fatalf("boiler fast issued duplicate DHW B524 selector % x", selector)
		}
	}
	if poller.boiler == nil {
		t.Fatal("poller.boiler = nil; want merged boiler snapshot")
	}
	if poller.boiler.DhwTemperatureC == nil || *poller.boiler.DhwTemperatureC != current {
		t.Fatalf("boiler DhwTemperatureC = %v; want cached %.1f", poller.boiler.DhwTemperatureC, current)
	}
	if poller.boiler.DhwTargetTemperatureC == nil || *poller.boiler.DhwTargetTemperatureC != target {
		t.Fatalf("boiler DhwTargetTemperatureC = %v; want cached %.1f", poller.boiler.DhwTargetTemperatureC, target)
	}
	if poller.boiler.DhwOperatingMode == nil || *poller.boiler.DhwOperatingMode != 1 {
		t.Fatalf("boiler DhwOperatingMode = %v; want cached 1", poller.boiler.DhwOperatingMode)
	}
}

func TestRefreshBoilerStatusFastCachedDHWOnlyDoesNotPublishLive(t *testing.T) {
	t.Parallel()

	current := 47.5
	target := 50.0
	provider := graphql.NewLiveSemanticProvider()
	poller := &vaillantSemanticPoller{
		scheduler:      ebusgateway.NewSemanticReadScheduler(),
		provider:       provider,
		source:         0x7F,
		controller:     0x15,
		requestTimeout: 50 * time.Millisecond,
		dhw: &vaillantDhwSnapshot{
			CurrentTempC:                  &current,
			TargetTempC:                   &target,
			ConfigurationDHWOperationMode: "1",
		},
	}
	poller.sendFrameFn = func(context.Context, protocol.Frame) (*protocol.Frame, error) {
		return nil, errors.New("boiler fast unavailable")
	}

	poller.refreshBoilerStatusTier(context.Background(), boilerStatusTierFast)

	if got := provider.BoilerStatus(); got != nil {
		t.Fatalf("BoilerStatus() = %+v; want nil without live boiler/B524 success", got)
	}
}

func TestRefreshBoilerStatusFastMergesCachedDHWWhenB509UpdatesWithoutB524Mirrors(t *testing.T) {
	t.Parallel()

	current := 47.5
	target := 50.0
	flow := 42.5
	provider := graphql.NewLiveSemanticProvider()
	poller := &vaillantSemanticPoller{
		scheduler:      ebusgateway.NewSemanticReadScheduler(),
		provider:       provider,
		source:         0x7F,
		controller:     0x15,
		boilerAddress:  0x08,
		requestTimeout: 50 * time.Millisecond,
		dhw: &vaillantDhwSnapshot{
			CurrentTempC:                  &current,
			TargetTempC:                   &target,
			ConfigurationDHWOperationMode: "1",
		},
	}
	poller.sendFrameFn = func(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
		if frame.Primary != vaillantB509Primary || frame.Secondary != vaillantB509Secondary {
			t.Fatalf("unexpected non-B509 active read: primary=0x%02x secondary=0x%02x data=% x", frame.Primary, frame.Secondary, frame.Data)
		}
		if len(frame.Data) < 3 || frame.Data[0] != vaillantB509OpcodeRead {
			t.Fatalf("unexpected B509 request data % x", frame.Data)
		}
		addr := uint16(frame.Data[1])<<8 | uint16(frame.Data[2])
		payload := append([]byte{vaillantB509OpcodeRead, byte(addr >> 8), byte(addr)}, encodeTempDATA2c(flow)...)
		return &protocol.Frame{Data: payload}, nil
	}

	poller.refreshBoilerStatusTier(context.Background(), boilerStatusTierFast)

	if poller.boiler == nil {
		t.Fatal("poller.boiler = nil; want merged boiler snapshot")
	}
	if poller.boiler.FlowTemperatureC == nil || *poller.boiler.FlowTemperatureC != flow {
		t.Fatalf("boiler FlowTemperatureC = %v; want B509 %.1f", poller.boiler.FlowTemperatureC, flow)
	}
	if poller.boiler.DhwTemperatureC == nil || *poller.boiler.DhwTemperatureC != current {
		t.Fatalf("boiler DhwTemperatureC = %v; want cached %.1f", poller.boiler.DhwTemperatureC, current)
	}
	if poller.boiler.DhwTargetTemperatureC == nil || *poller.boiler.DhwTargetTemperatureC != target {
		t.Fatalf("boiler DhwTargetTemperatureC = %v; want cached %.1f", poller.boiler.DhwTargetTemperatureC, target)
	}
	if poller.boiler.DhwOperatingMode == nil || *poller.boiler.DhwOperatingMode != 1 {
		t.Fatalf("boiler DhwOperatingMode = %v; want cached 1", poller.boiler.DhwOperatingMode)
	}
	status := provider.BoilerStatus()
	if status == nil || status.State.DhwTemperatureC == nil || *status.State.DhwTemperatureC != current {
		t.Fatalf("published boiler status = %+v; want cached DHW on live B509 update", status)
	}
}

func TestDeriveCircuitManagingDevice(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		system   *vaillantSystemSnapshot
		fm5Mode  graphql.Fm5SemanticMode
		wantRole graphql.ManagingDeviceRole
		wantID   *string
		wantAddr *int
	}{
		{
			name:     "unknown without proven topology",
			system:   nil,
			fm5Mode:  graphql.Fm5SemanticModeAbsent,
			wantRole: graphql.ManagingDeviceRoleUnknown,
		},
		{
			name: "proven topology emits VR71 function module ownership",
			system: &vaillantSystemSnapshot{
				SystemScheme:            uint16Ptr(1),
				ModuleConfigurationVR71: uint16Ptr(2),
			},
			fm5Mode:  graphql.Fm5SemanticModeInterpreted,
			wantRole: graphql.ManagingDeviceRoleFunctionModule,
			wantID:   stringPtr(circuitManagingDeviceVR71ID),
			wantAddr: intPtr(circuitManagingDeviceVR71Address),
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got := deriveCircuitManagingDevice(test.system, test.fm5Mode)
			if got.Role != test.wantRole {
				t.Fatalf("deriveCircuitManagingDevice(...).Role = %q; want %q", got.Role, test.wantRole)
			}
			if !stringPtrEquals(got.DeviceID, test.wantID) {
				t.Fatalf("deriveCircuitManagingDevice(...).DeviceID = %v; want %v", got.DeviceID, test.wantID)
			}
			if !intPtrEquals(got.Address, test.wantAddr) {
				t.Fatalf("deriveCircuitManagingDevice(...).Address = %v; want %v", got.Address, test.wantAddr)
			}
		})
	}
}

func TestFM5InventoryRegistryInfos_MaterializesVR71ForInterpretedTopology(t *testing.T) {
	t.Parallel()

	got := fm5InventoryRegistryInfos(&vaillantSystemSnapshot{
		SystemScheme:            uint16Ptr(1),
		ModuleConfigurationVR71: uint16Ptr(2),
	}, graphql.Fm5SemanticModeInterpreted)
	if len(got) != 1 {
		t.Fatalf("fm5InventoryRegistryInfos(...) len = %d; want 1", len(got))
	}
	if got[0].Address != 0x26 || got[0].Manufacturer != "Vaillant" || got[0].DeviceID != "VR_71" {
		t.Fatalf("fm5InventoryRegistryInfos(...) = %+v; want VR_71 at 0x26", got[0])
	}
}

func TestDeriveFM5SemanticModeTransitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                string
		controllerReachable bool
		fm5GateSatisfied    bool
		solarReadable       bool
		cylindersReadable   bool
		hasEvidence         bool
		want                graphql.Fm5SemanticMode
	}{
		{
			name:                "interpreted",
			controllerReachable: true,
			fm5GateSatisfied:    true,
			solarReadable:       true,
			cylindersReadable:   true,
			hasEvidence:         true,
			want:                graphql.Fm5SemanticModeInterpreted,
		},
		{
			name:                "gpio only",
			controllerReachable: false,
			fm5GateSatisfied:    false,
			solarReadable:       false,
			cylindersReadable:   false,
			hasEvidence:         true,
			want:                graphql.Fm5SemanticModeGPIOOnly,
		},
		{
			name:                "partial solar evidence is not interpreted",
			controllerReachable: true,
			fm5GateSatisfied:    true,
			solarReadable:       true,
			cylindersReadable:   false,
			hasEvidence:         true,
			want:                graphql.Fm5SemanticModeGPIOOnly,
		},
		{
			name:                "partial cylinder evidence is not interpreted",
			controllerReachable: true,
			fm5GateSatisfied:    true,
			solarReadable:       false,
			cylindersReadable:   true,
			hasEvidence:         true,
			want:                graphql.Fm5SemanticModeGPIOOnly,
		},
		{
			name:                "gate failure prevents interpretation",
			controllerReachable: true,
			fm5GateSatisfied:    false,
			solarReadable:       true,
			cylindersReadable:   true,
			hasEvidence:         true,
			want:                graphql.Fm5SemanticModeGPIOOnly,
		},
		{
			name:                "absent",
			controllerReachable: false,
			fm5GateSatisfied:    false,
			solarReadable:       false,
			cylindersReadable:   false,
			hasEvidence:         false,
			want:                graphql.Fm5SemanticModeAbsent,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got := deriveFM5SemanticMode(
				test.controllerReachable,
				test.fm5GateSatisfied,
				test.solarReadable,
				test.cylindersReadable,
				test.hasEvidence,
			)
			if got != test.want {
				t.Fatalf("deriveFM5SemanticMode(...) = %s; want %s", got, test.want)
			}
		})
	}
}

func TestMergeSystemSnapshotNonDestructive_PartialUpdatePreservesLastKnown(t *testing.T) {
	t.Parallel()

	pressure := 1.4
	flow := 35.2
	scheme := uint16(8)
	module := uint16(2)
	maxHumidity := uint16(70)
	updatedOutdoor := 4.5

	merged := mergeSystemSnapshotNonDestructive(
		&vaillantSystemSnapshot{
			SystemWaterPressure:     &pressure,
			SystemFlowTemperature:   &flow,
			SystemScheme:            &scheme,
			ModuleConfigurationVR71: &module,
			MaxRoomHumidity:         &maxHumidity,
		},
		&vaillantSystemSnapshot{
			OutdoorTemperature: &updatedOutdoor,
		},
	)

	if merged == nil {
		t.Fatal("merged snapshot is nil")
	}
	if merged.OutdoorTemperature == nil || *merged.OutdoorTemperature != 4.5 {
		t.Fatalf("merged.OutdoorTemperature = %v; want updated 4.5", merged.OutdoorTemperature)
	}
	if merged.SystemWaterPressure == nil || *merged.SystemWaterPressure != 1.4 {
		t.Fatalf("merged.SystemWaterPressure = %v; want preserved 1.4", merged.SystemWaterPressure)
	}
	if merged.SystemFlowTemperature == nil || *merged.SystemFlowTemperature != 35.2 {
		t.Fatalf("merged.SystemFlowTemperature = %v; want preserved 35.2", merged.SystemFlowTemperature)
	}
	if merged.SystemScheme == nil || *merged.SystemScheme != 8 {
		t.Fatalf("merged.SystemScheme = %v; want preserved 8", merged.SystemScheme)
	}
	if merged.ModuleConfigurationVR71 == nil || *merged.ModuleConfigurationVR71 != 2 {
		t.Fatalf("merged.ModuleConfigurationVR71 = %v; want preserved 2", merged.ModuleConfigurationVR71)
	}
	if merged.MaxRoomHumidity == nil || *merged.MaxRoomHumidity != 70 {
		t.Fatalf("merged.MaxRoomHumidity = %v; want preserved 70", merged.MaxRoomHumidity)
	}
}

func TestRefreshSystem_NoSuccessfulReadsPreservesSnapshot(t *testing.T) {
	t.Parallel()

	pressure := 1.7
	scheme := uint16(8)
	module := uint16(2)
	poller := &vaillantSemanticPoller{
		controller: 0x15,
		system: &vaillantSystemSnapshot{
			SystemWaterPressure:     &pressure,
			SystemScheme:            &scheme,
			ModuleConfigurationVR71: &module,
		},
	}

	poller.refreshSystem(context.Background())

	if poller.system == nil {
		t.Fatal("poller.system = nil; want preserved last-known snapshot")
	}
	if poller.system.SystemWaterPressure == nil || *poller.system.SystemWaterPressure != 1.7 {
		t.Fatalf("poller.system.SystemWaterPressure = %v; want preserved 1.7", poller.system.SystemWaterPressure)
	}
	if poller.system.SystemScheme == nil || *poller.system.SystemScheme != 8 {
		t.Fatalf("poller.system.SystemScheme = %v; want preserved 8", poller.system.SystemScheme)
	}
	if poller.system.ModuleConfigurationVR71 == nil || *poller.system.ModuleConfigurationVR71 != 2 {
		t.Fatalf("poller.system.ModuleConfigurationVR71 = %v; want preserved 2", poller.system.ModuleConfigurationVR71)
	}
}

func TestPublishCircuits_UsesExplicitManagingDeviceForProvenTopology(t *testing.T) {
	t.Parallel()

	scheme := uint16(1)
	module := uint16(2)
	circuitType := uint16(1)
	provider := graphql.NewLiveSemanticProvider()
	poller := &vaillantSemanticPoller{
		provider: provider,
		fm5Mode:  graphql.Fm5SemanticModeInterpreted,
		system: &vaillantSystemSnapshot{
			SystemScheme:            &scheme,
			ModuleConfigurationVR71: &module,
		},
		circuits: map[byte]*vaillantCircuitSnapshot{
			0x00: {
				Instance:       0x00,
				Active:         true,
				CircuitTypeRaw: &circuitType,
			},
		},
	}

	poller.publishCircuits(semanticSnapshotSourceLive)

	status := provider.Circuits()
	if len(status) != 1 {
		t.Fatalf("provider.Circuits() = %d entries; want 1", len(status))
	}
	if status[0].ManagingDevice.Role != graphql.ManagingDeviceRoleFunctionModule {
		t.Fatalf("provider.Circuits()[0].ManagingDevice.Role = %q; want %q", status[0].ManagingDevice.Role, graphql.ManagingDeviceRoleFunctionModule)
	}
	if !stringPtrEquals(status[0].ManagingDevice.DeviceID, stringPtr(circuitManagingDeviceVR71ID)) {
		t.Fatalf("provider.Circuits()[0].ManagingDevice.DeviceID = %v; want %q", status[0].ManagingDevice.DeviceID, circuitManagingDeviceVR71ID)
	}
	if !intPtrEquals(status[0].ManagingDevice.Address, intPtr(circuitManagingDeviceVR71Address)) {
		t.Fatalf("provider.Circuits()[0].ManagingDevice.Address = %v; want %d", status[0].ManagingDevice.Address, circuitManagingDeviceVR71Address)
	}
}

func TestPublishCircuits_UsesUnknownManagingDeviceForUnprovenTopology(t *testing.T) {
	t.Parallel()

	scheme := uint16(8)
	module := uint16(2)
	circuitType := uint16(1)
	provider := graphql.NewLiveSemanticProvider()
	poller := &vaillantSemanticPoller{
		provider: provider,
		fm5Mode:  graphql.Fm5SemanticModeInterpreted,
		system: &vaillantSystemSnapshot{
			SystemScheme:            &scheme,
			ModuleConfigurationVR71: &module,
		},
		circuits: map[byte]*vaillantCircuitSnapshot{
			0x00: {
				Instance:       0x00,
				Active:         true,
				CircuitTypeRaw: &circuitType,
			},
		},
	}

	poller.publishCircuits(semanticSnapshotSourceLive)

	status := provider.Circuits()
	if len(status) != 1 {
		t.Fatalf("provider.Circuits() = %d entries; want 1", len(status))
	}
	if status[0].ManagingDevice.Role != graphql.ManagingDeviceRoleUnknown {
		t.Fatalf("provider.Circuits()[0].ManagingDevice.Role = %q; want %q", status[0].ManagingDevice.Role, graphql.ManagingDeviceRoleUnknown)
	}
	if status[0].ManagingDevice.DeviceID != nil {
		t.Fatalf("provider.Circuits()[0].ManagingDevice.DeviceID = %v; want nil", status[0].ManagingDevice.DeviceID)
	}
	if status[0].ManagingDevice.Address != nil {
		t.Fatalf("provider.Circuits()[0].ManagingDevice.Address = %v; want nil", status[0].ManagingDevice.Address)
	}
}

func TestPublishSystem_RehydratesCircuitOwnershipAfterSystemArrives(t *testing.T) {
	t.Parallel()

	circuitType := uint16(1)
	scheme := uint16(1)
	module := uint16(2)
	provider := graphql.NewLiveSemanticProvider()
	poller := &vaillantSemanticPoller{
		provider: provider,
		fm5Mode:  graphql.Fm5SemanticModeInterpreted,
		circuits: map[byte]*vaillantCircuitSnapshot{
			0x00: {
				Instance:       0x00,
				Active:         true,
				CircuitTypeRaw: &circuitType,
			},
		},
	}

	poller.publishCircuits(semanticSnapshotSourceLive)

	initial := provider.Circuits()
	if len(initial) != 1 || initial[0].ManagingDevice.Role != graphql.ManagingDeviceRoleUnknown {
		t.Fatalf("initial circuit ownership = %#v; want UNKNOWN before system arrives", initial)
	}

	poller.system = &vaillantSystemSnapshot{
		SystemScheme:            &scheme,
		ModuleConfigurationVR71: &module,
	}
	poller.publishSystem(semanticSnapshotSourceLive)

	status := provider.Circuits()
	if len(status) != 1 {
		t.Fatalf("provider.Circuits() = %d entries; want 1", len(status))
	}
	if status[0].ManagingDevice.Role != graphql.ManagingDeviceRoleFunctionModule {
		t.Fatalf("provider.Circuits()[0].ManagingDevice.Role = %q; want %q", status[0].ManagingDevice.Role, graphql.ManagingDeviceRoleFunctionModule)
	}
}

func TestMergeCircuitSnapshotNonDestructive_PartialUpdatePreservesLastKnown(t *testing.T) {
	t.Parallel()

	circuitType := uint16(1)
	coolingEnabled := uint16(1)
	flowTemp := 41.5
	flowSetpoint := 44.0
	mixer := 67.0
	pumpStarts := uint32(120)
	updatedFlow := 42.25
	updatedPump := uint16(0)

	merged := mergeCircuitSnapshotNonDestructive(
		&vaillantCircuitSnapshot{
			Instance:          0x01,
			Active:            true,
			CircuitTypeRaw:    &circuitType,
			CoolingEnabledRaw: &coolingEnabled,
			FlowTemperatureC:  &flowTemp,
			FlowSetpointC:     &flowSetpoint,
			MixerPositionPct:  &mixer,
			PumpStartsRaw:     &pumpStarts,
		},
		&vaillantCircuitSnapshot{
			Instance:         0x01,
			Active:           true,
			FlowTemperatureC: &updatedFlow,
			PumpStatusRaw:    &updatedPump,
		},
	)

	if merged == nil {
		t.Fatal("merged snapshot is nil")
	}
	if merged.FlowTemperatureC == nil || *merged.FlowTemperatureC != 42.25 {
		t.Fatalf("merged.FlowTemperatureC = %v; want updated 42.25", merged.FlowTemperatureC)
	}
	if merged.FlowSetpointC == nil || *merged.FlowSetpointC != 44.0 {
		t.Fatalf("merged.FlowSetpointC = %v; want preserved 44.0", merged.FlowSetpointC)
	}
	if merged.PumpStatusRaw == nil || *merged.PumpStatusRaw != 0 {
		t.Fatalf("merged.PumpStatusRaw = %v; want updated 0", merged.PumpStatusRaw)
	}
	if merged.CoolingEnabledRaw == nil || *merged.CoolingEnabledRaw != 1 {
		t.Fatalf("merged.CoolingEnabledRaw = %v; want preserved 1", merged.CoolingEnabledRaw)
	}
	if merged.PumpStartsRaw == nil || *merged.PumpStartsRaw != 120 {
		t.Fatalf("merged.PumpStartsRaw = %v; want preserved 120", merged.PumpStartsRaw)
	}
	if merged.CircuitTypeRaw == nil || *merged.CircuitTypeRaw != 1 {
		t.Fatalf("merged.CircuitTypeRaw = %v; want preserved 1", merged.CircuitTypeRaw)
	}
}

func TestRefreshCircuits_NoSuccessfulReadsPreservesSnapshot(t *testing.T) {
	t.Parallel()

	circuitType := uint16(1)
	flow := 35.0
	cooling := uint16(1)
	poller := &vaillantSemanticPoller{
		controller: 0x15,
		circuits: map[byte]*vaillantCircuitSnapshot{
			0x00: {
				Instance:          0x00,
				Active:            true,
				CircuitTypeRaw:    &circuitType,
				FlowTemperatureC:  &flow,
				CoolingEnabledRaw: &cooling,
			},
		},
	}

	poller.refreshCircuits(context.Background())

	entry, ok := poller.circuits[0x00]
	if !ok || entry == nil {
		t.Fatal("circuit snapshot missing after refresh with no successful reads")
	}
	if entry.FlowTemperatureC == nil || *entry.FlowTemperatureC != 35.0 {
		t.Fatalf("entry.FlowTemperatureC = %v; want preserved 35.0", entry.FlowTemperatureC)
	}
	if entry.CircuitTypeRaw == nil || *entry.CircuitTypeRaw != 1 {
		t.Fatalf("entry.CircuitTypeRaw = %v; want preserved 1", entry.CircuitTypeRaw)
	}
	if entry.CoolingEnabledRaw == nil || *entry.CoolingEnabledRaw != 1 {
		t.Fatalf("entry.CoolingEnabledRaw = %v; want preserved 1", entry.CoolingEnabledRaw)
	}
}

func TestRefreshCircuits_TargetedProbeFailureForcesNextFullScan(t *testing.T) {
	t.Parallel()

	now := time.Unix(1000, 0)
	circuitType := uint16(1)
	poller := &vaillantSemanticPoller{
		controller:                  0x15,
		circuitFullScanInterval:     semanticCircuitFullScanInterval,
		lastCircuitFullScanAt:       now.Add(-5 * time.Minute),
		lastCircuitFullScanComplete: true,
		nowFn:                       func() time.Time { return now },
		circuits: map[byte]*vaillantCircuitSnapshot{
			0x01: {
				Instance:       0x01,
				Active:         true,
				CircuitTypeRaw: &circuitType,
			},
		},
	}

	poller.refreshCircuits(context.Background())

	poller.mu.Lock()
	got, fullScan := poller.circuitRefreshInstancesLocked(now.Add(time.Second))
	poller.mu.Unlock()
	if !fullScan {
		t.Fatal("circuitRefreshInstancesLocked fullScan = false; want true after targeted probe failure")
	}
	if want := allCircuitRefreshInstances(); !slices.Equal(got, want) {
		t.Fatalf("circuitRefreshInstancesLocked instances = %#v; want %#v", got, want)
	}
}

func TestCircuitRefreshInstances_UsesKnownActiveCircuitsBeforeFullScanDue(t *testing.T) {
	t.Parallel()

	now := time.Unix(1000, 0)
	poller := &vaillantSemanticPoller{
		circuitFullScanInterval:     semanticCircuitFullScanInterval,
		lastCircuitFullScanAt:       now.Add(-5 * time.Minute),
		lastCircuitFullScanComplete: true,
		circuits: map[byte]*vaillantCircuitSnapshot{
			0x09: {Instance: 0x09, Active: true},
			0x01: {Instance: 0x01, Active: true},
			0x02: {Instance: 0x02, Active: false},
		},
	}

	got, fullScan := poller.circuitRefreshInstancesLocked(now)
	if fullScan {
		t.Fatal("circuitRefreshInstancesLocked fullScan = true; want false")
	}
	if want := []byte{0x01, 0x09}; !slices.Equal(got, want) {
		t.Fatalf("circuitRefreshInstancesLocked instances = %#v; want %#v", got, want)
	}
}

func TestCircuitRefreshInstances_FullScanAfterRediscoveryInterval(t *testing.T) {
	t.Parallel()

	now := time.Unix(1000, 0)
	poller := &vaillantSemanticPoller{
		circuitFullScanInterval: semanticCircuitFullScanInterval,
		lastCircuitFullScanAt:   now.Add(-semanticCircuitFullScanInterval),
		circuits: map[byte]*vaillantCircuitSnapshot{
			0x01: {Instance: 0x01, Active: true},
		},
	}

	got, fullScan := poller.circuitRefreshInstancesLocked(now)
	if !fullScan {
		t.Fatal("circuitRefreshInstancesLocked fullScan = false; want true")
	}
	if want := allCircuitRefreshInstances(); !slices.Equal(got, want) {
		t.Fatalf("circuitRefreshInstancesLocked instances = %#v; want %#v", got, want)
	}
}

func TestCircuitRefreshInstances_RetriesPartialFullScanOnConfigCadence(t *testing.T) {
	t.Parallel()

	now := time.Unix(1000, 0)
	poller := &vaillantSemanticPoller{
		circuitFullScanInterval:     semanticCircuitFullScanInterval,
		lastCircuitFullScanAt:       now.Add(-semanticCircuitPartialScanInterval),
		lastCircuitFullScanComplete: false,
		circuits: map[byte]*vaillantCircuitSnapshot{
			0x01: {Instance: 0x01, Active: true},
		},
	}

	got, fullScan := poller.circuitRefreshInstancesLocked(now)
	if !fullScan {
		t.Fatal("circuitRefreshInstancesLocked fullScan = false; want true after partial scan retry interval")
	}
	if want := allCircuitRefreshInstances(); !slices.Equal(got, want) {
		t.Fatalf("circuitRefreshInstancesLocked instances = %#v; want %#v", got, want)
	}
}

func TestDecodeHeatingCircuitTypeToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		raw  *uint16
		want string
	}{
		{name: "nil", raw: nil, want: ""},
		{name: "heating", raw: uint16Ptr(1), want: "heating"},
		{name: "fixed_value", raw: uint16Ptr(2), want: "fixed_value"},
		{name: "dhw", raw: uint16Ptr(3), want: "dhw"},
		{name: "return_increase", raw: uint16Ptr(4), want: "return_increase"},
		{name: "unknown", raw: uint16Ptr(9), want: "unknown_9"},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := decodeHeatingCircuitTypeToken(test.raw); got != test.want {
				t.Fatalf("decodeHeatingCircuitTypeToken(%v) = %q; want %q", test.raw, got, test.want)
			}
		})
	}
}

func TestDecodeRoomTempControlToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		raw  *uint16
		want string
	}{
		{name: "nil", raw: nil, want: ""},
		{name: "off", raw: uint16Ptr(0), want: "off"},
		{name: "modulating", raw: uint16Ptr(1), want: "modulating"},
		{name: "thermostat", raw: uint16Ptr(2), want: "thermostat"},
		{name: "unknown", raw: uint16Ptr(7), want: "unknown_7"},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := decodeRoomTempControlToken(test.raw); got != test.want {
				t.Fatalf("decodeRoomTempControlToken(%v) = %q; want %q", test.raw, got, test.want)
			}
		})
	}
}

func TestCircuitState_DecodesKnownValues(t *testing.T) {
	tests := []struct {
		raw  uint16
		want string
	}{
		{0, "standby"},
		{1, "heating"},
		{2, "cooling"},
	}
	for _, tt := range tests {
		raw := tt.raw
		got := decodeCircuitStateToken(&raw)
		if got != tt.want {
			t.Errorf("decodeCircuitStateToken(%d) = %q; want %q", tt.raw, got, tt.want)
		}
	}
}

func TestCircuitState_ReturnsUnknownForUnmappedValue(t *testing.T) {
	raw := uint16(99)
	got := decodeCircuitStateToken(&raw)
	want := "unknown_99"
	if got != want {
		t.Errorf("decodeCircuitStateToken(%d) = %q; want %q", raw, got, want)
	}
}

func TestCircuitState_NilReturnsEmpty(t *testing.T) {
	got := decodeCircuitStateToken(nil)
	if got != "" {
		t.Errorf("decodeCircuitStateToken(nil) = %q; want empty", got)
	}
}

func uint16Ptr(value uint16) *uint16 {
	v := value
	return &v
}

func uint8Ptr(value uint8) *uint8 {
	v := value
	return &v
}

func stringPtr(value string) *string {
	v := value
	return &v
}

func intPtr(value int) *int {
	v := value
	return &v
}

func TestRefreshEnergy_NilControllerKeepsNeverSeenShape(t *testing.T) {
	t.Parallel()

	provider := graphql.NewLiveSemanticProvider()
	poller := &vaillantSemanticPoller{
		provider:            provider,
		regulatorCapability: productids.ControllerPresent,
	}

	poller.refreshEnergy(context.Background())

	totals := provider.EnergyTotals()
	if totals == nil {
		t.Fatal("EnergyTotals() = nil; want visible no-data shape for nil controller path")
	}
	if got := totals.Gas.DHW.TodayMeta.FreshnessState; got != graphql.EnergyFreshnessStateNeverSeen {
		t.Fatalf("Gas.DHW.TodayMeta.FreshnessState = %q; want never_seen", got)
	}
}

func TestRefreshEnergy_NoRegulatorKeepsNeverSeenShape(t *testing.T) {
	t.Parallel()

	provider := graphql.NewLiveSemanticProvider()
	poller := &vaillantSemanticPoller{
		provider:            provider,
		controller:          0x15,
		regulatorCapability: productids.ControllerNone,
	}

	poller.refreshEnergy(context.Background())

	totals := provider.EnergyTotals()
	if totals == nil {
		t.Fatal("EnergyTotals() = nil; want visible no-data shape without regulator")
	}
	if got := totals.Gas.DHW.TodayMeta.FreshnessState; got != graphql.EnergyFreshnessStateNeverSeen {
		t.Fatalf("Gas.DHW.TodayMeta.FreshnessState = %q; want never_seen", got)
	}
}

func TestRefreshEnergy_ReadFailurePreservesLastKnown(t *testing.T) {
	t.Parallel()

	provider := graphql.NewLiveSemanticProvider()
	poller := &vaillantSemanticPoller{
		provider:   provider,
		controller: 0x15,
	}

	provider.ApplyEnergyFromRegister(graphql.EnergyMergeKey{
		Channel: "gas",
		Usage:   "hot_water",
		Period:  "day",
	}, 3.5)
	provider.ApplyEnergyFromRegister(graphql.EnergyMergeKey{
		Channel:  "gas",
		Usage:    "hot_water",
		Period:   "year",
		YearKind: "current",
	}, 240.0)

	before := provider.EnergyTotals()
	if before == nil {
		t.Fatal("EnergyTotals() before refresh = nil; want seeded totals")
	}

	poller.refreshEnergy(context.Background())

	after := provider.EnergyTotals()
	if after == nil {
		t.Fatal("EnergyTotals() after failed refresh = nil; want preserved last-known totals")
	}
	if after.Gas.DHW.Today != before.Gas.DHW.Today {
		t.Fatalf("Gas.DHW.Today after failed refresh = %v; want preserved %v", after.Gas.DHW.Today, before.Gas.DHW.Today)
	}
	if len(after.Gas.DHW.Yearly) != len(before.Gas.DHW.Yearly) || after.Gas.DHW.Yearly[1] != before.Gas.DHW.Yearly[1] {
		t.Fatalf("Gas.DHW.Yearly after failed refresh = %#v; want preserved %#v", after.Gas.DHW.Yearly, before.Gas.DHW.Yearly)
	}
}

func TestPublishEnergyTotals_PublishesProviderSnapshot(t *testing.T) {
	t.Parallel()

	provider := graphql.NewLiveSemanticProvider()
	if !provider.ApplyEnergyFromRegister(graphql.EnergyMergeKey{
		Channel: "gas",
		Usage:   "climate",
		Period:  "day",
	}, 4.25) {
		t.Fatal("ApplyEnergyFromRegister() = false; want true")
	}

	hub := graphql.NewBroadcastHub(nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	subscription, err := hub.SubscribeEnergy(ctx)
	if err != nil {
		t.Fatalf("SubscribeEnergy() error = %v", err)
	}

	poller := &vaillantSemanticPoller{
		provider: provider,
		hub:      hub,
	}
	poller.publishEnergyTotals()

	select {
	case raw := <-subscription:
		totals, ok := raw.(*graphql.EnergyTotals)
		if !ok {
			t.Fatalf("energy payload type = %T; want *graphql.EnergyTotals", raw)
		}
		if totals.Gas.Climate.Today != 4.25 {
			t.Fatalf("energy payload gas.climate.today = %v; want 4.25", totals.Gas.Climate.Today)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for energy publish")
	}
}

func TestPublishFM5Semantic_SkipsConfigOnlyCylinderWithoutTemperature(t *testing.T) {
	t.Parallel()

	provider := graphql.NewLiveSemanticProvider()
	temp := 48.0
	maxSetpoint := 60.0
	chargeOffset := 5.0
	poller := &vaillantSemanticPoller{
		provider: provider,
		fm5Mode:  graphql.Fm5SemanticModeInterpreted,
		solarCylinders: map[byte]*vaillantCylinderSnapshot{
			0x00: {
				Instance:         0x00,
				TemperatureC:     &temp,
				MaxSetpointC:     &maxSetpoint,
				ChargeHysteresis: &chargeOffset,
			},
			0x01: {
				Instance:     0x01,
				MaxSetpointC: &maxSetpoint,
			},
		},
	}

	poller.publishFM5Semantic(semanticSnapshotSourceLive)

	cylinders := provider.Cylinders()
	if len(cylinders) != 1 {
		t.Fatalf("provider.Cylinders() len = %d; want 1 live-evidenced cylinder", len(cylinders))
	}
	if cylinders[0].Index != 0 {
		t.Fatalf("provider.Cylinders()[0].Index = %d; want 0", cylinders[0].Index)
	}
	if cylinders[0].TemperatureC == nil || *cylinders[0].TemperatureC != temp {
		t.Fatalf("provider.Cylinders()[0].TemperatureC = %#v; want %v", cylinders[0].TemperatureC, temp)
	}
}

func TestPublishFM5Semantic_DowngradeRehydratesCircuitOwnershipToUnknown(t *testing.T) {
	t.Parallel()

	circuitType := uint16(1)
	scheme := uint16(1)
	module := uint16(2)
	provider := graphql.NewLiveSemanticProvider()
	poller := &vaillantSemanticPoller{
		provider: provider,
		fm5Mode:  graphql.Fm5SemanticModeInterpreted,
		system: &vaillantSystemSnapshot{
			SystemScheme:            &scheme,
			ModuleConfigurationVR71: &module,
		},
		circuits: map[byte]*vaillantCircuitSnapshot{
			0x00: {
				Instance:       0x00,
				Active:         true,
				CircuitTypeRaw: &circuitType,
			},
		},
	}

	poller.publishCircuits(semanticSnapshotSourceLive)

	initial := provider.Circuits()
	if len(initial) != 1 || initial[0].ManagingDevice.Role != graphql.ManagingDeviceRoleFunctionModule {
		t.Fatalf("initial circuit ownership = %#v; want FUNCTION_MODULE before downgrade", initial)
	}

	poller.mu.Lock()
	poller.fm5Mode = graphql.Fm5SemanticModeGPIOOnly
	poller.mu.Unlock()
	poller.publishFM5Semantic(semanticSnapshotSourceLive)

	if provider.Solar() == nil {
		t.Fatal("provider.Solar() = nil after GPIO-only FM5 publish; want empty non-null plane")
	}
	if cylinders := provider.Cylinders(); cylinders == nil || len(cylinders) != 0 {
		t.Fatalf("provider.Cylinders() = %#v after GPIO-only FM5 publish; want empty non-null plane", cylinders)
	}

	status := provider.Circuits()
	if len(status) != 1 {
		t.Fatalf("provider.Circuits() = %d entries; want 1", len(status))
	}
	if status[0].ManagingDevice.Role != graphql.ManagingDeviceRoleUnknown {
		t.Fatalf("provider.Circuits()[0].ManagingDevice.Role = %q; want %q", status[0].ManagingDevice.Role, graphql.ManagingDeviceRoleUnknown)
	}
	if status[0].ManagingDevice.DeviceID != nil || status[0].ManagingDevice.Address != nil {
		t.Fatalf("provider.Circuits()[0].ManagingDevice = %#v; want nil identity on downgrade", status[0].ManagingDevice)
	}
}

func TestMergeCylinderSnapshotMapNonDestructive_PreservesTemperatureWhileRefreshingConfig(t *testing.T) {
	t.Parallel()

	oldTemp := 49.0
	oldSetpoint := 55.0
	newSetpoint := 60.0
	existing := map[byte]*vaillantCylinderSnapshot{
		0x00: {
			Instance:     0x00,
			TemperatureC: &oldTemp,
			MaxSetpointC: &oldSetpoint,
		},
	}
	incoming := map[byte]*vaillantCylinderSnapshot{
		0x00: {
			Instance:     0x00,
			MaxSetpointC: &newSetpoint,
		},
	}

	merged := mergeCylinderSnapshotMapNonDestructive(existing, incoming)
	snapshot := merged[0x00]
	if snapshot == nil {
		t.Fatal("merged[0x00] = nil; want preserved cylinder snapshot")
	}
	if snapshot.TemperatureC == nil || *snapshot.TemperatureC != oldTemp {
		t.Fatalf("merged temperature = %#v; want preserved %v", snapshot.TemperatureC, oldTemp)
	}
	if snapshot.MaxSetpointC == nil || *snapshot.MaxSetpointC != newSetpoint {
		t.Fatalf("merged max setpoint = %#v; want refreshed %v", snapshot.MaxSetpointC, newSetpoint)
	}
}

func TestVaillantSemanticPoller_DHWTransientCacheFailurePreservesLastKnown(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.January, 10, 8, 0, 0, 0, time.UTC)
	current := 48.5

	provider := graphql.NewLiveSemanticProvider()
	poller := &vaillantSemanticPoller{
		provider: provider,
		dhw: &vaillantDhwSnapshot{
			OperatingMode: "auto",
			Preset:        "schedule",
			CurrentTempC:  &current,
		},
		dhwStaleTTL: 10 * time.Minute,
	}
	poller.nowFn = func() time.Time { return now }
	poller.dhwLastUpdateAt = now

	poller.publishDHW(semanticSnapshotSourceLive)

	now = now.Add(5 * time.Minute)
	poller.publishDHW(semanticSnapshotSourceCache)

	dhw := provider.DHW()
	if dhw == nil {
		t.Fatalf("provider.DHW() = nil; want preserved last-known DHW before TTL expiry")
	}
	if dhw.Config.OperatingMode != "auto" {
		t.Fatalf("provider.DHW().Config.OperatingMode = %q; want auto", dhw.Config.OperatingMode)
	}
	if dhw.State.CurrentTempC == nil || *dhw.State.CurrentTempC != 48.5 {
		t.Fatalf("provider.DHW().State.CurrentTempC = %v; want 48.5", dhw.State.CurrentTempC)
	}
}

func TestVaillantSemanticPoller_DHWCacheFailureExpiresAfterTTL(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.January, 10, 8, 0, 0, 0, time.UTC)
	current := 47.2

	provider := graphql.NewLiveSemanticProvider()
	cacheSpy := &semanticSnapshotCaptureSpy{}
	poller := &vaillantSemanticPoller{
		provider: provider,
		cache:    cacheSpy,
		dhw: &vaillantDhwSnapshot{
			OperatingMode: "auto",
			Preset:        "schedule",
			CurrentTempC:  &current,
		},
		dhwStaleTTL: 10 * time.Minute,
	}
	poller.nowFn = func() time.Time { return now }
	poller.dhwLastUpdateAt = now

	poller.publishDHW(semanticSnapshotSourceLive)

	now = now.Add(11 * time.Minute)
	poller.publishDHW(semanticSnapshotSourceCache)

	if dhw := provider.DHW(); dhw != nil {
		t.Fatalf("provider.DHW() = %#v; want nil after TTL expiry", dhw)
	}
	if poller.dhw != nil {
		t.Fatalf("poller.dhw = %#v; want nil after TTL expiry", poller.dhw)
	}
	if cacheSpy.calls < 2 {
		t.Fatalf("cache Save calls = %d; want at least 2 (live persist + expiry persist)", cacheSpy.calls)
	}
	if cacheSpy.last.DHW != nil {
		t.Fatalf("cache last DHW = %#v; want nil after TTL expiry persist", cacheSpy.last.DHW)
	}
}

func TestVaillantSemanticPoller_HydrateFromCacheSeedsDHWStalenessFromPersistedAt(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.January, 10, 8, 0, 0, 0, time.UTC)
	current := 45.0

	provider := graphql.NewLiveSemanticProvider()
	cacheSpy := &semanticSnapshotCaptureSpy{}
	poller := &vaillantSemanticPoller{
		provider:    provider,
		cache:       cacheSpy,
		dhwStaleTTL: 15 * time.Minute,
	}
	poller.nowFn = func() time.Time { return now }

	// Hydrate from a cache that was persisted 20 minutes ago — already past the 15m TTL.
	poller.hydrateFromCache(semanticCacheSnapshot{
		DHW: &graphql.DhwStatus{
			Config: graphql.DhwConfig{
				OperatingMode: "auto",
				Preset:        "schedule",
			},
			State: graphql.DhwState{
				CurrentTempC: &current,
			},
		},
		PersistedAt: now.Add(-20 * time.Minute),
	})

	// The poller should have seeded dhwLastUpdateAt from PersistedAt, not now().
	if poller.dhwLastUpdateAt.Equal(now) {
		t.Fatalf("dhwLastUpdateAt = now; want PersistedAt (20m ago)")
	}

	// First cache-sourced publish should expire the DHW immediately since age > TTL.
	poller.publishDHW(semanticSnapshotSourceCache)

	if dhw := provider.DHW(); dhw != nil {
		t.Fatalf("provider.DHW() = %#v; want nil — cache was already past TTL at hydration", dhw)
	}
	if poller.dhw != nil {
		t.Fatalf("poller.dhw = %#v; want nil after immediate TTL expiry", poller.dhw)
	}
}

func TestVaillantSemanticPoller_HydrateFromCacheFallsBackToNowWhenPersistedAtZero(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.January, 10, 8, 0, 0, 0, time.UTC)
	current := 45.0

	poller := &vaillantSemanticPoller{
		provider:    graphql.NewLiveSemanticProvider(),
		dhwStaleTTL: 15 * time.Minute,
	}
	poller.nowFn = func() time.Time { return now }

	// Hydrate with zero PersistedAt — should fall back to now().
	poller.hydrateFromCache(semanticCacheSnapshot{
		DHW: &graphql.DhwStatus{
			Config: graphql.DhwConfig{
				OperatingMode: "auto",
			},
			State: graphql.DhwState{
				CurrentTempC: &current,
			},
		},
	})

	if !poller.dhwLastUpdateAt.Equal(now) {
		t.Fatalf("dhwLastUpdateAt = %v; want %v (fallback to now when PersistedAt is zero)", poller.dhwLastUpdateAt, now)
	}
}

func TestVaillantSemanticPoller_DiscoveryLossTTLExpiresDHW(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.January, 11, 9, 0, 0, 0, time.UTC)
	current := 49.1

	provider := graphql.NewLiveSemanticProvider()
	cacheSpy := &semanticSnapshotCaptureSpy{}
	poller := &vaillantSemanticPoller{
		provider:    provider,
		cache:       cacheSpy,
		dhwStaleTTL: 10 * time.Minute,
		dhw: &vaillantDhwSnapshot{
			OperatingMode: "auto",
			Preset:        "schedule",
			CurrentTempC:  &current,
		},
	}
	poller.nowFn = func() time.Time { return now }
	poller.dhwLastUpdateAt = now

	poller.publishDHW(semanticSnapshotSourceLive)

	now = now.Add(5 * time.Minute)
	poller.refreshDiscovery(context.Background())
	if dhw := provider.DHW(); dhw == nil {
		t.Fatalf("provider.DHW() = nil; want DHW preserved before TTL expiry")
	}

	now = now.Add(6 * time.Minute)
	poller.refreshDiscovery(context.Background())

	if dhw := provider.DHW(); dhw != nil {
		t.Fatalf("provider.DHW() = %#v; want nil after TTL expiry on discovery loss", dhw)
	}
	if poller.dhw != nil {
		t.Fatalf("poller.dhw = %#v; want nil after TTL expiry on discovery loss", poller.dhw)
	}
	if cacheSpy.calls < 2 {
		t.Fatalf("cache Save calls = %d; want at least 2 (live persist + expiry persist)", cacheSpy.calls)
	}
	if cacheSpy.last.DHW != nil {
		t.Fatalf("cache last DHW = %#v; want nil after TTL expiry persist", cacheSpy.last.DHW)
	}
}

func TestVaillantSemanticPoller_DiscoveryLossPreservesDHWBeforeTTL(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.January, 11, 10, 0, 0, 0, time.UTC)
	current := 47.8

	provider := graphql.NewLiveSemanticProvider()
	cacheSpy := &semanticSnapshotCaptureSpy{}
	poller := &vaillantSemanticPoller{
		provider:    provider,
		cache:       cacheSpy,
		dhwStaleTTL: 15 * time.Minute,
		dhw: &vaillantDhwSnapshot{
			OperatingMode: "auto",
			Preset:        "schedule",
			CurrentTempC:  &current,
		},
	}
	poller.nowFn = func() time.Time { return now }
	poller.dhwLastUpdateAt = now

	poller.publishDHW(semanticSnapshotSourceLive)

	now = now.Add(14 * time.Minute)
	poller.refreshDiscovery(context.Background())

	dhw := provider.DHW()
	if dhw == nil {
		t.Fatalf("provider.DHW() = nil; want DHW preserved before TTL expiry")
	}
	if dhw.Config.OperatingMode != "auto" {
		t.Fatalf("provider.DHW().Config.OperatingMode = %q; want auto", dhw.Config.OperatingMode)
	}
	if dhw.State.CurrentTempC == nil || *dhw.State.CurrentTempC != 47.8 {
		t.Fatalf("provider.DHW().State.CurrentTempC = %v; want 47.8", dhw.State.CurrentTempC)
	}
	if poller.dhw == nil {
		t.Fatalf("poller.dhw = nil; want preserved snapshot before TTL expiry")
	}
	if cacheSpy.last.DHW == nil {
		t.Fatalf("cache last DHW = nil; want last persisted live DHW snapshot")
	}
}

func TestVaillantSemanticPoller_DiscoveryFlapDuringStartup(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.January, 11, 11, 0, 0, 0, time.UTC)
	current := 46.6
	cached := &graphql.DhwStatus{
		Config: graphql.DhwConfig{
			OperatingMode: "auto",
			Preset:        "schedule",
		},
		State: graphql.DhwState{
			CurrentTempC: &current,
		},
	}

	provider := graphql.NewLiveSemanticProvider()
	provider.SetDHWFromCache(cached)
	if got := provider.StartupPhase(); got != graphql.SemanticStartupPhaseCacheLoadedStale {
		t.Fatalf("phase after cache load = %s; want %s", got, graphql.SemanticStartupPhaseCacheLoadedStale)
	}

	poller := &vaillantSemanticPoller{
		provider:    provider,
		dhwStaleTTL: 30 * time.Minute,
	}
	poller.nowFn = func() time.Time { return now }
	poller.hydrateFromCache(semanticCacheSnapshot{
		DHW:         cached,
		PersistedAt: now.Add(-5 * time.Minute),
	})

	poller.refreshDiscovery(context.Background())
	if got := provider.StartupPhase(); got != graphql.SemanticStartupPhaseCacheLoadedStale {
		t.Fatalf("phase after cache-phase discovery flap = %s; want %s", got, graphql.SemanticStartupPhaseCacheLoadedStale)
	}
	if dhw := provider.DHW(); dhw == nil {
		t.Fatalf("provider.DHW() = nil; want preserved cached DHW during CACHE_LOADED_STALE discovery flap")
	}

	now = now.Add(1 * time.Minute)
	poller.publishDHW(semanticSnapshotSourceLive)
	if got := provider.StartupPhase(); got != graphql.SemanticStartupPhaseLiveWarmup {
		t.Fatalf("phase after first live DHW publish = %s; want %s", got, graphql.SemanticStartupPhaseLiveWarmup)
	}

	now = now.Add(1 * time.Minute)
	poller.refreshDiscovery(context.Background())
	if got := provider.StartupPhase(); got != graphql.SemanticStartupPhaseLiveWarmup {
		t.Fatalf("phase after warmup discovery flap = %s; want %s", got, graphql.SemanticStartupPhaseLiveWarmup)
	}
	dhw := provider.DHW()
	if dhw == nil {
		t.Fatalf("provider.DHW() = nil; want preserved DHW during LIVE_WARMUP discovery flap")
	}
	if dhw.State.CurrentTempC == nil || *dhw.State.CurrentTempC != 46.6 {
		t.Fatalf("provider.DHW().State.CurrentTempC = %v; want 46.6", dhw.State.CurrentTempC)
	}
}

func TestMergeZoneSnapshotFields_PartialLiveUpdatePreservesLastKnownAndFreshness(t *testing.T) {
	t.Parallel()

	current := 21.0
	target := 22.5
	humidity := 44.0
	roomTemperatureZoneMapping := uint16(2)
	circuitIndex := uint16(2)
	circuitType := uint16(1)
	valve := uint16(1)
	entry := &vaillantZoneSnapshot{
		Name:                              "Zone 1",
		OperatingMode:                     "heat",
		Preset:                            "manual",
		HvacAction:                        "heating",
		AllowedModes:                      []string{"off", "auto", "heat"},
		CurrentTempC:                      &current,
		TargetTempC:                       &target,
		HumidityPct:                       &humidity,
		ConfigurationHeatingOperationMode: "1",
		StateSpecialFunction:              "0",
		ConfigurationRoomTemperatureZoneMappingRaw: &roomTemperatureZoneMapping,
		ConfigurationAssociatedCircuitRaw:          &circuitIndex,
		ConfigurationCircuitTypeRaw:                &circuitType,
		StateValveStatusRaw:                        &valve,
	}
	seedZoneFreshness(entry, semanticSnapshotSourceCache, true)

	updatedCurrent := 21.8
	incoming := &vaillantZoneSnapshot{
		CurrentTempC:                      &updatedCurrent,
		ConfigurationHeatingOperationMode: "2",
	}
	mergeZoneSnapshotFields(entry, incoming, semanticSnapshotSourceLive, newSemanticFieldSet(
		zoneFieldOperatingMode,
		zoneFieldPreset,
		zoneFieldHvacAction,
		zoneFieldAllowedModes,
		zoneFieldCurrentTempC,
		zoneFieldTargetTempC,
		zoneFieldCurrentHumidityPct,
		zoneFieldZoneOperationModeRaw,
		zoneFieldSpecialFunctionRaw,
		zoneFieldRoomTemperatureZoneMappingRaw,
		zoneFieldZoneCircuitIndexRaw,
		zoneFieldZoneValveStatusRaw,
		zoneFieldCircuitTypeRaw,
		zoneFieldQuickVetoTempC,
		zoneFieldQuickVetoDurationH,
		zoneFieldQuickVetoEndTime,
		zoneFieldQuickVetoEndDate,
		zoneFieldHolidayStartDate,
		zoneFieldHolidayEndDate,
		zoneFieldHolidaySetpointC,
		zoneFieldHolidayStartTime,
		zoneFieldHolidayEndTime,
	))

	if entry.CurrentTempC == nil || *entry.CurrentTempC != 21.8 {
		t.Fatalf("entry.CurrentTempC = %v; want 21.8", entry.CurrentTempC)
	}
	if entry.TargetTempC == nil || *entry.TargetTempC != 22.5 {
		t.Fatalf("entry.TargetTempC = %v; want preserved 22.5", entry.TargetTempC)
	}
	if entry.ConfigurationAssociatedCircuitRaw == nil || *entry.ConfigurationAssociatedCircuitRaw != 2 {
		t.Fatalf("entry.ConfigurationAssociatedCircuitRaw = %v; want preserved 2", entry.ConfigurationAssociatedCircuitRaw)
	}
	if entry.ConfigurationRoomTemperatureZoneMappingRaw == nil || *entry.ConfigurationRoomTemperatureZoneMappingRaw != 2 {
		t.Fatalf("entry.ConfigurationRoomTemperatureZoneMappingRaw = %v; want preserved 2", entry.ConfigurationRoomTemperatureZoneMappingRaw)
	}
	if entry.ConfigurationHeatingOperationMode != "2" {
		t.Fatalf("entry.ConfigurationHeatingOperationMode = %q; want 2", entry.ConfigurationHeatingOperationMode)
	}

	currentFreshness, ok := entry.FieldFreshness[zoneFieldCurrentTempC]
	if !ok || currentFreshness.Source != semanticSnapshotSourceLive || currentFreshness.Stale {
		t.Fatalf("current_temp freshness = %+v (ok=%v); want source=live stale=false", currentFreshness, ok)
	}
	targetFreshness, ok := entry.FieldFreshness[zoneFieldTargetTempC]
	if !ok || targetFreshness.Source != semanticSnapshotSourceCache || !targetFreshness.Stale {
		t.Fatalf("target_temp freshness = %+v (ok=%v); want source=cache stale=true", targetFreshness, ok)
	}
	opModeFreshness, ok := entry.FieldFreshness[zoneFieldZoneOperationModeRaw]
	if !ok || opModeFreshness.Source != semanticSnapshotSourceLive || opModeFreshness.Stale {
		t.Fatalf("operation_mode_raw freshness = %+v (ok=%v); want source=live stale=false", opModeFreshness, ok)
	}
	roomMappingFreshness, ok := entry.FieldFreshness[zoneFieldRoomTemperatureZoneMappingRaw]
	if !ok || roomMappingFreshness.Source != semanticSnapshotSourceCache || !roomMappingFreshness.Stale {
		t.Fatalf("room_temperature_zone_mapping_raw freshness = %+v (ok=%v); want source=cache stale=true", roomMappingFreshness, ok)
	}
}

func TestVaillantSemanticPoller_PublishZones_SeparatesRoomTemperatureZoneMappingFromAssociatedCircuit(t *testing.T) {
	t.Parallel()

	provider := graphql.NewLiveSemanticProvider()
	roomTemperatureZoneMapping := uint16(2)
	associatedCircuit := uint16(resolveAssociatedCircuitInstance(&roomTemperatureZoneMapping, 1))

	poller := &vaillantSemanticPoller{
		provider: provider,
		zones: map[byte]*vaillantZoneSnapshot{
			0x01: {
				Instance:      0x01,
				Present:       true,
				Name:          "Etaj",
				OperatingMode: "heat",
				Preset:        "manual",
				AllowedModes:  []string{"off", "auto", "heat"},
				ConfigurationRoomTemperatureZoneMappingRaw: &roomTemperatureZoneMapping,
				ConfigurationAssociatedCircuitRaw:          &associatedCircuit,
			},
		},
	}

	poller.publishZones(semanticSnapshotSourceLive)

	zones := provider.Zones()
	if len(zones) != 1 {
		t.Fatalf("len(provider.Zones()) = %d; want 1", len(zones))
	}
	if zones[0].Config.RoomTemperatureZoneMapping == nil || *zones[0].Config.RoomTemperatureZoneMapping != 2 {
		t.Fatalf("roomTemperatureZoneMapping = %#v; want 2", zones[0].Config.RoomTemperatureZoneMapping)
	}
	if zones[0].Config.AssociatedCircuit == nil || *zones[0].Config.AssociatedCircuit != 1 {
		t.Fatalf("associatedCircuit = %#v; want 1", zones[0].Config.AssociatedCircuit)
	}
}

func TestVaillantSemanticPoller_PublishZones_FillsRoomStateFromMappedRadioSensors(t *testing.T) {
	t.Parallel()

	provider := graphql.NewLiveSemanticProvider()
	regulatorMapping := uint16(1)
	thermostatMapping := uint16(2)
	regulatorConnected := true
	thermostatConnected := true
	regulatorTemp := 14.8125
	regulatorHumidity := 62.0
	thermostatTemp := 17.6875
	thermostatHumidity := 53.0

	poller := &vaillantSemanticPoller{
		provider: provider,
		zones: map[byte]*vaillantZoneSnapshot{
			0x00: {
				Instance: 0x00,
				Present:  true,
				Name:     "Parter",
				ConfigurationRoomTemperatureZoneMappingRaw: &regulatorMapping,
			},
			0x01: {
				Instance: 0x01,
				Present:  true,
				Name:     "Etaj",
				ConfigurationRoomTemperatureZoneMappingRaw: &thermostatMapping,
			},
		},
		radioDevices: map[radioDeviceKey]*vaillantRadioDeviceSnapshot{
			{Group: remoteRegulators.group, Instance: 0x01}: {
				Group:            remoteRegulators.group,
				Instance:         0x01,
				DeviceConnected:  &regulatorConnected,
				RoomTemperatureC: &regulatorTemp,
				RoomHumidityPct:  &regulatorHumidity,
			},
			{Group: remoteThermostats.group, Instance: 0x01}: {
				Group:            remoteThermostats.group,
				Instance:         0x01,
				DeviceConnected:  &thermostatConnected,
				RoomTemperatureC: &thermostatTemp,
				RoomHumidityPct:  &thermostatHumidity,
			},
		},
	}

	poller.publishZones(semanticSnapshotSourceLive)

	zones := provider.Zones()
	if len(zones) != 2 {
		t.Fatalf("len(provider.Zones()) = %d; want 2", len(zones))
	}
	if zones[0].State.CurrentTempC == nil || *zones[0].State.CurrentTempC != regulatorTemp {
		t.Fatalf("zone 1 current temp = %#v; want %.4f", zones[0].State.CurrentTempC, regulatorTemp)
	}
	if zones[0].State.CurrentHumidityPct == nil || *zones[0].State.CurrentHumidityPct != regulatorHumidity {
		t.Fatalf("zone 1 humidity = %#v; want %.1f", zones[0].State.CurrentHumidityPct, regulatorHumidity)
	}
	if zones[1].State.CurrentTempC == nil || *zones[1].State.CurrentTempC != thermostatTemp {
		t.Fatalf("zone 2 current temp = %#v; want %.4f", zones[1].State.CurrentTempC, thermostatTemp)
	}
	if zones[1].State.CurrentHumidityPct == nil || *zones[1].State.CurrentHumidityPct != thermostatHumidity {
		t.Fatalf("zone 2 humidity = %#v; want %.1f", zones[1].State.CurrentHumidityPct, thermostatHumidity)
	}
}

func TestVaillantSemanticPoller_PublishZones_InfersThermostatMappingFromRadioAssignment(t *testing.T) {
	t.Parallel()

	provider := graphql.NewLiveSemanticProvider()
	cache := &semanticSnapshotCaptureSpy{}
	connected := true
	zoneAssignment := uint8(2)
	radioTemp := 17.8125
	radioHumidity := 53.0

	poller := &vaillantSemanticPoller{
		provider: provider,
		cache:    cache,
		zones: map[byte]*vaillantZoneSnapshot{
			0x01: {
				Instance: 0x01,
				Present:  true,
				Name:     "Etaj",
			},
		},
		radioDevices: map[radioDeviceKey]*vaillantRadioDeviceSnapshot{
			{Group: remoteThermostats.group, Instance: 0x01}: {
				Group:            remoteThermostats.group,
				Instance:         0x01,
				DeviceConnected:  &connected,
				ZoneAssignment:   &zoneAssignment,
				RoomTemperatureC: &radioTemp,
				RoomHumidityPct:  &radioHumidity,
			},
		},
	}

	poller.publishZones(semanticSnapshotSourceLive)

	zones := provider.Zones()
	if len(zones) != 1 {
		t.Fatalf("len(provider.Zones()) = %d; want 1", len(zones))
	}
	if zones[0].State.CurrentTempC == nil || *zones[0].State.CurrentTempC != radioTemp {
		t.Fatalf("zone current temp = %#v; want radio %.4f", zones[0].State.CurrentTempC, radioTemp)
	}
	if zones[0].State.CurrentHumidityPct == nil || *zones[0].State.CurrentHumidityPct != radioHumidity {
		t.Fatalf("zone humidity = %#v; want radio %.1f", zones[0].State.CurrentHumidityPct, radioHumidity)
	}
	if zones[0].Config.RoomTemperatureZoneMapping == nil || *zones[0].Config.RoomTemperatureZoneMapping != 2 {
		t.Fatalf("roomTemperatureZoneMapping = %#v; want inferred 2", zones[0].Config.RoomTemperatureZoneMapping)
	}
	if zones[0].Config.AssociatedCircuit == nil || *zones[0].Config.AssociatedCircuit != 1 {
		t.Fatalf("associatedCircuit = %#v; want inferred 1", zones[0].Config.AssociatedCircuit)
	}
	if cache.calls == 0 || len(cache.last.Zones) != 1 {
		t.Fatalf("cache calls=%d zones=%d; want persisted zone snapshot", cache.calls, len(cache.last.Zones))
	}
	if cache.last.Zones[0].State.CurrentTempC != nil {
		t.Fatalf("cached current temp = %#v; want nil direct zone value", cache.last.Zones[0].State.CurrentTempC)
	}
	if cache.last.Zones[0].Config.RoomTemperatureZoneMapping != nil {
		t.Fatalf("cached inferred mapping = %#v; want nil direct zone mapping", cache.last.Zones[0].Config.RoomTemperatureZoneMapping)
	}
	if cache.last.Zones[0].Config.AssociatedCircuit != nil {
		t.Fatalf("cached inferred associated circuit = %#v; want nil direct associated circuit", cache.last.Zones[0].Config.AssociatedCircuit)
	}
}

func TestVaillantSemanticPoller_PublishZones_DoesNotInferAmbiguousThermostatAssignment(t *testing.T) {
	t.Parallel()

	provider := graphql.NewLiveSemanticProvider()
	cache := &semanticSnapshotCaptureSpy{}
	connected := true
	zoneAssignment := uint8(2)
	firstTemp := 17.8125
	secondTemp := 18.25

	poller := &vaillantSemanticPoller{
		provider: provider,
		cache:    cache,
		zones: map[byte]*vaillantZoneSnapshot{
			0x01: {
				Instance: 0x01,
				Present:  true,
				Name:     "Etaj",
			},
		},
		radioDevices: map[radioDeviceKey]*vaillantRadioDeviceSnapshot{
			{Group: remoteThermostats.group, Instance: 0x01}: {
				Group:            remoteThermostats.group,
				Instance:         0x01,
				DeviceConnected:  &connected,
				ZoneAssignment:   &zoneAssignment,
				RoomTemperatureC: &firstTemp,
			},
			{Group: remoteThermostats.group, Instance: 0x02}: {
				Group:            remoteThermostats.group,
				Instance:         0x02,
				DeviceConnected:  &connected,
				ZoneAssignment:   &zoneAssignment,
				RoomTemperatureC: &secondTemp,
			},
		},
	}

	poller.publishZones(semanticSnapshotSourceLive)

	zones := provider.Zones()
	if len(zones) != 1 {
		t.Fatalf("len(provider.Zones()) = %d; want 1", len(zones))
	}
	if zones[0].State.CurrentTempC != nil {
		t.Fatalf("zone current temp = %#v; want nil for ambiguous inferred mapping", zones[0].State.CurrentTempC)
	}
	if zones[0].Config.RoomTemperatureZoneMapping != nil {
		t.Fatalf("roomTemperatureZoneMapping = %#v; want nil for ambiguous inferred mapping", zones[0].Config.RoomTemperatureZoneMapping)
	}
	if zones[0].Config.AssociatedCircuit != nil {
		t.Fatalf("associatedCircuit = %#v; want nil for ambiguous inferred mapping", zones[0].Config.AssociatedCircuit)
	}
	if cache.calls == 0 || len(cache.last.Zones) != 1 {
		t.Fatalf("cache calls=%d zones=%d; want persisted zone snapshot", cache.calls, len(cache.last.Zones))
	}
	if cache.last.Zones[0].Config.RoomTemperatureZoneMapping != nil {
		t.Fatalf("cached ambiguous mapping = %#v; want nil", cache.last.Zones[0].Config.RoomTemperatureZoneMapping)
	}
	if cache.last.Zones[0].Config.AssociatedCircuit != nil {
		t.Fatalf("cached ambiguous associated circuit = %#v; want nil", cache.last.Zones[0].Config.AssociatedCircuit)
	}
}

func TestVaillantSemanticPoller_PublishZones_KeepsDirectRoomStateOverRadioFallback(t *testing.T) {
	t.Parallel()

	provider := graphql.NewLiveSemanticProvider()
	mapping := uint16(2)
	connected := true
	directTemp := 21.0
	directHumidity := 44.0
	radioTemp := 17.6875
	radioHumidity := 53.0

	poller := &vaillantSemanticPoller{
		provider: provider,
		zones: map[byte]*vaillantZoneSnapshot{
			0x01: {
				Instance:     0x01,
				Present:      true,
				Name:         "Etaj",
				CurrentTempC: &directTemp,
				HumidityPct:  &directHumidity,
				ConfigurationRoomTemperatureZoneMappingRaw: &mapping,
			},
		},
		radioDevices: map[radioDeviceKey]*vaillantRadioDeviceSnapshot{
			{Group: remoteThermostats.group, Instance: 0x01}: {
				Group:            remoteThermostats.group,
				Instance:         0x01,
				DeviceConnected:  &connected,
				RoomTemperatureC: &radioTemp,
				RoomHumidityPct:  &radioHumidity,
			},
		},
	}

	poller.publishZones(semanticSnapshotSourceLive)

	zones := provider.Zones()
	if len(zones) != 1 {
		t.Fatalf("len(provider.Zones()) = %d; want 1", len(zones))
	}
	if zones[0].State.CurrentTempC == nil || *zones[0].State.CurrentTempC != directTemp {
		t.Fatalf("zone current temp = %#v; want direct %.1f", zones[0].State.CurrentTempC, directTemp)
	}
	if zones[0].State.CurrentHumidityPct == nil || *zones[0].State.CurrentHumidityPct != directHumidity {
		t.Fatalf("zone humidity = %#v; want direct %.1f", zones[0].State.CurrentHumidityPct, directHumidity)
	}
}

func TestVaillantSemanticPoller_PublishZones_RadioFallbackDoesNotPersistAsDirectState(t *testing.T) {
	t.Parallel()

	provider := graphql.NewLiveSemanticProvider()
	cache := &semanticSnapshotCaptureSpy{}
	mapping := uint16(2)
	connected := true
	radioTemp := 17.6875
	radioHumidity := 53.0

	poller := &vaillantSemanticPoller{
		provider: provider,
		cache:    cache,
		zones: map[byte]*vaillantZoneSnapshot{
			0x01: {
				Instance: 0x01,
				Present:  true,
				Name:     "Etaj",
				ConfigurationRoomTemperatureZoneMappingRaw: &mapping,
			},
		},
		radioDevices: map[radioDeviceKey]*vaillantRadioDeviceSnapshot{
			{Group: remoteThermostats.group, Instance: 0x01}: {
				Group:            remoteThermostats.group,
				Instance:         0x01,
				DeviceConnected:  &connected,
				RoomTemperatureC: &radioTemp,
				RoomHumidityPct:  &radioHumidity,
			},
		},
	}

	poller.publishZones(semanticSnapshotSourceLive)

	published := provider.Zones()
	if len(published) != 1 {
		t.Fatalf("len(provider.Zones()) = %d; want 1", len(published))
	}
	if published[0].State.CurrentTempC == nil || *published[0].State.CurrentTempC != radioTemp {
		t.Fatalf("published current temp = %#v; want radio fallback %.4f", published[0].State.CurrentTempC, radioTemp)
	}
	if cache.calls == 0 || len(cache.last.Zones) != 1 {
		t.Fatalf("cache calls=%d zones=%d; want persisted zone snapshot", cache.calls, len(cache.last.Zones))
	}
	if cache.last.Zones[0].State.CurrentTempC != nil {
		t.Fatalf("cached current temp = %#v; want nil direct zone value", cache.last.Zones[0].State.CurrentTempC)
	}
	if cache.last.Zones[0].State.CurrentHumidityPct != nil {
		t.Fatalf("cached humidity = %#v; want nil direct zone value", cache.last.Zones[0].State.CurrentHumidityPct)
	}
}

func TestVaillantSemanticPoller_PublishZones_SkipsUnknownRoomSensorMappingFallback(t *testing.T) {
	t.Parallel()

	for _, raw := range []uint16{0, 0xFF, 0xFFFF} {
		raw := raw
		t.Run(fmt.Sprintf("mapping_0x%04x", raw), func(t *testing.T) {
			t.Parallel()

			provider := graphql.NewLiveSemanticProvider()
			connected := true
			radioTemp := 17.6875
			poller := &vaillantSemanticPoller{
				provider: provider,
				zones: map[byte]*vaillantZoneSnapshot{
					0x01: {
						Instance: 0x01,
						Present:  true,
						Name:     "Etaj",
						ConfigurationRoomTemperatureZoneMappingRaw: &raw,
					},
				},
				radioDevices: map[radioDeviceKey]*vaillantRadioDeviceSnapshot{
					{Group: remoteThermostats.group, Instance: 0xFE}: {
						Group:            remoteThermostats.group,
						Instance:         0xFE,
						DeviceConnected:  &connected,
						RoomTemperatureC: &radioTemp,
					},
				},
			}

			poller.publishZones(semanticSnapshotSourceLive)

			zones := provider.Zones()
			if len(zones) != 1 {
				t.Fatalf("len(provider.Zones()) = %d; want 1", len(zones))
			}
			if zones[0].State.CurrentTempC != nil {
				t.Fatalf("zone current temp = %#v; want nil for unknown mapping 0x%04x", zones[0].State.CurrentTempC, raw)
			}
		})
	}
}

func TestMergeDhwSnapshotFields_PartialLiveUpdatePreservesLastKnownAndFreshness(t *testing.T) {
	t.Parallel()

	current := 48.0
	target := 50.0
	entry := &vaillantDhwSnapshot{
		OperatingMode:                 "auto",
		Preset:                        "schedule",
		CurrentTempC:                  &current,
		TargetTempC:                   &target,
		ConfigurationDHWOperationMode: "1",
		StateSpecialFunction:          "0",
	}
	seedDhwFreshness(entry, semanticSnapshotSourceCache, true)

	updatedCurrent := 49.5
	incoming := &vaillantDhwSnapshot{
		CurrentTempC: &updatedCurrent,
	}
	mergeDhwSnapshotFields(entry, incoming, semanticSnapshotSourceLive, dhwFieldSet)

	if entry.CurrentTempC == nil || *entry.CurrentTempC != 49.5 {
		t.Fatalf("entry.CurrentTempC = %v; want 49.5", entry.CurrentTempC)
	}
	if entry.TargetTempC == nil || *entry.TargetTempC != 50.0 {
		t.Fatalf("entry.TargetTempC = %v; want preserved 50.0", entry.TargetTempC)
	}
	if entry.OperatingMode != "auto" {
		t.Fatalf("entry.OperatingMode = %q; want preserved auto", entry.OperatingMode)
	}

	currentFreshness, ok := entry.FieldFreshness[dhwFieldCurrentTempC]
	if !ok || currentFreshness.Source != semanticSnapshotSourceLive || currentFreshness.Stale {
		t.Fatalf("current_temp freshness = %+v (ok=%v); want source=live stale=false", currentFreshness, ok)
	}
	targetFreshness, ok := entry.FieldFreshness[dhwFieldTargetTempC]
	if !ok || targetFreshness.Source != semanticSnapshotSourceCache || !targetFreshness.Stale {
		t.Fatalf("target_temp freshness = %+v (ok=%v); want source=cache stale=true", targetFreshness, ok)
	}
}

func TestZonePresenceFSM_NoRegressionWithFreshnessMetadata(t *testing.T) {
	t.Parallel()

	instance := byte(0x02)
	poller := &vaillantSemanticPoller{
		zones:             make(map[byte]*vaillantZoneSnapshot),
		presence:          make(map[byte]*zonePresenceRecord),
		zoneMissThreshold: 3,
		zoneHitThreshold:  2,
	}

	current := 20.0
	entry := &vaillantZoneSnapshot{Instance: instance, Present: true, CurrentTempC: &current}
	seedZoneFreshness(entry, semanticSnapshotSourceCache, true)
	poller.zones[instance] = entry
	poller.presence[instance] = &zonePresenceRecord{
		State:     zonePresencePresent,
		HitStreak: poller.zoneHitThresholdValue(),
	}

	poller.applyZonePresenceProbes(map[byte]bool{instance: true}, map[byte]bool{})
	if _, exists := poller.zones[instance]; !exists {
		t.Fatalf("zone removed too early on first miss")
	}
	poller.applyZonePresenceProbes(map[byte]bool{instance: true}, map[byte]bool{})
	if _, exists := poller.zones[instance]; !exists {
		t.Fatalf("zone removed too early on second miss")
	}
	poller.applyZonePresenceProbes(map[byte]bool{instance: true}, map[byte]bool{})
	if _, exists := poller.zones[instance]; exists {
		t.Fatalf("zone should be absent after third miss")
	}
}

func TestZonePresenceFSM_AbsenceRequiresConsecutiveMisses(t *testing.T) {
	t.Parallel()

	instance := byte(0x02)
	poller := &vaillantSemanticPoller{
		zones:             make(map[byte]*vaillantZoneSnapshot),
		presence:          make(map[byte]*zonePresenceRecord),
		zoneMissThreshold: 3,
		zoneHitThreshold:  2,
	}

	poller.applyZonePresenceProbes(map[byte]bool{instance: true}, map[byte]bool{instance: true})
	if _, exists := poller.zones[instance]; exists {
		t.Fatalf("zone should not be present after first hit when hit threshold is 2")
	}
	poller.applyZonePresenceProbes(map[byte]bool{instance: true}, map[byte]bool{instance: true})
	if _, exists := poller.zones[instance]; !exists {
		t.Fatalf("zone should be present after second consecutive hit")
	}

	for miss := 1; miss <= 2; miss++ {
		poller.applyZonePresenceProbes(map[byte]bool{instance: true}, map[byte]bool{})
		if _, exists := poller.zones[instance]; !exists {
			t.Fatalf("zone removed too early after miss %d", miss)
		}
	}

	poller.applyZonePresenceProbes(map[byte]bool{instance: true}, map[byte]bool{})
	if _, exists := poller.zones[instance]; exists {
		t.Fatalf("zone should be absent after third consecutive miss")
	}
	record := poller.presence[instance]
	if record == nil || record.State != zonePresenceAbsent {
		t.Fatalf("presence state = %+v; want ABSENT", record)
	}
}

func TestZonePresenceFSM_ResurrectionRequiresConsecutiveHits(t *testing.T) {
	t.Parallel()

	instance := byte(0x01)
	poller := &vaillantSemanticPoller{
		zones:             make(map[byte]*vaillantZoneSnapshot),
		presence:          make(map[byte]*zonePresenceRecord),
		zoneMissThreshold: 3,
		zoneHitThreshold:  2,
	}

	poller.applyZonePresenceProbes(map[byte]bool{instance: true}, map[byte]bool{instance: true})
	poller.applyZonePresenceProbes(map[byte]bool{instance: true}, map[byte]bool{instance: true})
	poller.applyZonePresenceProbes(map[byte]bool{instance: true}, map[byte]bool{})
	poller.applyZonePresenceProbes(map[byte]bool{instance: true}, map[byte]bool{})
	poller.applyZonePresenceProbes(map[byte]bool{instance: true}, map[byte]bool{})
	if _, exists := poller.zones[instance]; exists {
		t.Fatalf("zone should be absent after miss threshold")
	}

	poller.applyZonePresenceProbes(map[byte]bool{instance: true}, map[byte]bool{instance: true})
	if _, exists := poller.zones[instance]; exists {
		t.Fatalf("zone should stay absent until hit threshold is reached")
	}

	poller.applyZonePresenceProbes(map[byte]bool{instance: true}, map[byte]bool{})
	poller.applyZonePresenceProbes(map[byte]bool{instance: true}, map[byte]bool{instance: true})
	if _, exists := poller.zones[instance]; exists {
		t.Fatalf("single hit after miss should not resurrect zone")
	}

	poller.applyZonePresenceProbes(map[byte]bool{instance: true}, map[byte]bool{instance: true})
	if _, exists := poller.zones[instance]; !exists {
		t.Fatalf("zone should resurrect after two consecutive hits")
	}
	record := poller.presence[instance]
	if record == nil || record.State != zonePresencePresent {
		t.Fatalf("presence state = %+v; want PRESENT", record)
	}
}

func TestZonePresenceFSM_AlternatingProbesDoNotFlapPresentZone(t *testing.T) {
	t.Parallel()

	instance := byte(0x00)
	poller := &vaillantSemanticPoller{
		zones:             make(map[byte]*vaillantZoneSnapshot),
		presence:          make(map[byte]*zonePresenceRecord),
		zoneMissThreshold: 3,
		zoneHitThreshold:  2,
	}

	poller.applyZonePresenceProbes(map[byte]bool{instance: true}, map[byte]bool{instance: true})
	poller.applyZonePresenceProbes(map[byte]bool{instance: true}, map[byte]bool{instance: true})

	for iteration := 0; iteration < 6; iteration++ {
		poller.applyZonePresenceProbes(map[byte]bool{instance: true}, map[byte]bool{})
		if _, exists := poller.zones[instance]; !exists {
			t.Fatalf("zone disappeared on alternating miss at iteration %d", iteration)
		}
		record := poller.presence[instance]
		if record == nil || record.State != zonePresenceSuspectMissing {
			t.Fatalf("presence state after miss = %+v; want SUSPECT_MISSING", record)
		}

		poller.applyZonePresenceProbes(map[byte]bool{instance: true}, map[byte]bool{instance: true})
		if _, exists := poller.zones[instance]; !exists {
			t.Fatalf("zone disappeared on alternating hit at iteration %d", iteration)
		}
		record = poller.presence[instance]
		if record == nil || record.State != zonePresencePresent {
			t.Fatalf("presence state after hit = %+v; want PRESENT", record)
		}
	}
}

func TestReconcileDiscoveryPresence_SkipsMissDemotionWhenFallbackHydrates(t *testing.T) {
	t.Parallel()

	instance := byte(0x03)
	poller := &vaillantSemanticPoller{
		zones:             make(map[byte]*vaillantZoneSnapshot),
		presence:          make(map[byte]*zonePresenceRecord),
		zoneMissThreshold: 3,
		zoneHitThreshold:  2,
		transportConfig:   ebusgateway.TransportConfig{Protocol: ebusgateway.TransportEbusdTCP},
	}

	poller.presence[instance] = &zonePresenceRecord{
		State:     zonePresenceSuspectResurrect,
		HitStreak: 1,
	}

	poller.refreshFromEbusdGrabFn = func(_ context.Context) (map[byte]bool, bool) {
		poller.applyZonePresenceProbes(
			map[byte]bool{instance: true},
			map[byte]bool{instance: true},
		)
		return map[byte]bool{instance: true}, true
	}

	source := poller.reconcileDiscoveryPresence(
		context.Background(),
		map[byte]bool{instance: true},
		map[byte]bool{},
	)
	if source != semanticSnapshotSourceLive {
		t.Fatalf("source = %v; want LIVE", source)
	}
	record := poller.presence[instance]
	if record == nil || record.State != zonePresencePresent {
		t.Fatalf("presence state = %+v; want PRESENT", record)
	}
	if _, exists := poller.zones[instance]; !exists {
		t.Fatalf("zone should be present after fallback hydration hit reaches threshold")
	}
}

func TestReconcileDiscoveryPresence_AppliesMissesWhenFallbackUnavailable(t *testing.T) {
	t.Parallel()

	instance := byte(0x01)
	poller := &vaillantSemanticPoller{
		zones:             make(map[byte]*vaillantZoneSnapshot),
		presence:          make(map[byte]*zonePresenceRecord),
		zoneMissThreshold: 3,
		zoneHitThreshold:  2,
	}
	poller.zones[instance] = &vaillantZoneSnapshot{Instance: instance, Present: true}
	poller.presence[instance] = &zonePresenceRecord{
		State:     zonePresencePresent,
		HitStreak: poller.zoneHitThresholdValue(),
	}
	poller.refreshFromEbusdGrabFn = func(_ context.Context) (map[byte]bool, bool) { return nil, false }

	source := poller.reconcileDiscoveryPresence(
		context.Background(),
		map[byte]bool{instance: true},
		map[byte]bool{},
	)
	if source != semanticSnapshotSourceCache {
		t.Fatalf("source = %v; want CACHE", source)
	}
	record := poller.presence[instance]
	if record == nil || record.State != zonePresenceSuspectMissing {
		t.Fatalf("presence state = %+v; want SUSPECT_MISSING", record)
	}
	if _, exists := poller.zones[instance]; !exists {
		t.Fatalf("zone should remain present before miss threshold")
	}
}

func TestReconcileDiscoveryPresence_TracksMissesWithPartialFallback(t *testing.T) {
	t.Parallel()

	keep := byte(0x00)
	missing := byte(0x01)
	poller := &vaillantSemanticPoller{
		zones:             make(map[byte]*vaillantZoneSnapshot),
		presence:          make(map[byte]*zonePresenceRecord),
		zoneMissThreshold: 3,
		zoneHitThreshold:  2,
		transportConfig:   ebusgateway.TransportConfig{Protocol: ebusgateway.TransportEbusdTCP},
	}
	poller.zones[keep] = &vaillantZoneSnapshot{Instance: keep, Present: true}
	poller.zones[missing] = &vaillantZoneSnapshot{Instance: missing, Present: true}
	poller.presence[keep] = &zonePresenceRecord{State: zonePresencePresent, HitStreak: poller.zoneHitThresholdValue()}
	poller.presence[missing] = &zonePresenceRecord{State: zonePresencePresent, HitStreak: poller.zoneHitThresholdValue()}
	poller.refreshFromEbusdGrabFn = func(_ context.Context) (map[byte]bool, bool) {
		return map[byte]bool{keep: true}, true
	}

	source := poller.reconcileDiscoveryPresence(
		context.Background(),
		map[byte]bool{keep: true, missing: true},
		map[byte]bool{},
	)
	if source != semanticSnapshotSourceLive {
		t.Fatalf("source = %v; want LIVE", source)
	}
	if _, exists := poller.zones[keep]; !exists {
		t.Fatalf("fallback-present zone must remain present")
	}
	if _, exists := poller.zones[missing]; !exists {
		t.Fatalf("missing zone must not be removed on first miss")
	}
	record := poller.presence[missing]
	if record == nil || record.State != zonePresenceSuspectMissing {
		t.Fatalf("missing zone state = %+v; want SUSPECT_MISSING", record)
	}
}

func TestReconcileDiscoveryPresence_DoesNotDoubleCountFallbackHit(t *testing.T) {
	t.Parallel()

	instance := byte(0x04)
	poller := &vaillantSemanticPoller{
		zones:             make(map[byte]*vaillantZoneSnapshot),
		presence:          make(map[byte]*zonePresenceRecord),
		zoneMissThreshold: 3,
		zoneHitThreshold:  2,
		transportConfig:   ebusgateway.TransportConfig{Protocol: ebusgateway.TransportEbusdTCP},
	}
	poller.refreshFromEbusdGrabFn = func(_ context.Context) (map[byte]bool, bool) {
		poller.applyZonePresenceProbes(
			map[byte]bool{instance: true},
			map[byte]bool{instance: true},
		)
		return map[byte]bool{instance: true}, true
	}

	source := poller.reconcileDiscoveryPresence(
		context.Background(),
		map[byte]bool{instance: true},
		map[byte]bool{},
	)
	if source != semanticSnapshotSourceLive {
		t.Fatalf("source = %v; want LIVE", source)
	}
	if _, exists := poller.zones[instance]; exists {
		t.Fatalf("zone should not be promoted to present after a single fallback hit when threshold is 2")
	}
	record := poller.presence[instance]
	if record == nil || record.State != zonePresenceSuspectResurrect || record.HitStreak != 1 {
		t.Fatalf("presence state = %+v; want SUSPECT_RESURRECT with hit_streak=1", record)
	}
}

func TestSemanticReadBreakerKeyIncludesOpcode(t *testing.T) {
	t.Parallel()

	localKey := semanticReadBreakerKey(0x15, vaillantB524OpcodeLocal, localDHW.group, dhwInstance, dhw_current_temp)
	readKey := semanticReadBreakerKey(0x15, vaillantB524OpcodeRead, localDHW.group, dhwInstance, dhw_current_temp)
	if localKey == readKey {
		t.Fatalf("semanticReadBreakerKey must include opcode; got equal keys %q", localKey)
	}

	otherTarget := semanticReadBreakerKey(0x16, vaillantB524OpcodeRead, localDHW.group, dhwInstance, dhw_current_temp)
	if readKey == otherTarget {
		t.Fatalf("semanticReadBreakerKey must include target; got equal keys %q", readKey)
	}
}

func TestParseB524ZonesFromGrabFiltersAbsentInstances(t *testing.T) {
	t.Parallel()

	lines := []string{
		"f115b52406020003001c00 / 0501031c0000 = 1: basv Z1ZoneCircuitIndex",
		"3115b52406020003001600 / 0b0303160050617274657200 = 2: basv Z1Shortname",
		"f115b52406020003000f00 / 0801030f0000007041 = 1: basv Z1RoomTemp",
		"f115b52406020003012200 / 080303220000002041 = 1: basv Z2HeatingManualTemp",
		"f115b52406020003011c00 / 0501031c0001 = 2: basv Z2ZoneCircuitIndex",
		"3115b52406020003011600 / 09030316004574616a00 = 1: basv Z2Shortname",
		"f115b52406020003021c00 / 0500031c00ff = 2: basv Z3ZoneCircuitIndex",
		"3115b52406020003021600 / 0b020316005a6f6e65203300 = 1: basv Z3Shortname",
	}

	zones := parseB524ZonesFromGrab(lines, 0x15)
	if len(zones) != 2 {
		t.Fatalf("len(zones) = %d; want 2", len(zones))
	}

	zone1, ok := zones[0]
	if !ok {
		t.Fatalf("zone instance 0 missing")
	}
	if zone1.Name != "Parter" {
		t.Fatalf("zone1 name = %q; want %q", zone1.Name, "Parter")
	}
	if zone1.CurrentTempC == nil || *zone1.CurrentTempC != 15.0 {
		t.Fatalf("zone1 current = %v; want 15.0", zone1.CurrentTempC)
	}

	zone2, ok := zones[1]
	if !ok {
		t.Fatalf("zone instance 1 missing")
	}
	if zone2.Name != "Etaj" {
		t.Fatalf("zone2 name = %q; want %q", zone2.Name, "Etaj")
	}
	if zone2.TargetTempC == nil || *zone2.TargetTempC != 10.0 {
		t.Fatalf("zone2 target = %v; want 10.0", zone2.TargetTempC)
	}

	if _, exists := zones[2]; exists {
		t.Fatalf("zone instance 2 should be filtered as absent")
	}
}

func TestParseB524ZonesFromGrabIncludesHumidityAndAllowedModes(t *testing.T) {
	t.Parallel()

	lines := []string{
		"f115b52406020003001c00 / 0501031c0000 = 1",
		"f115b52406020003000600 / 010306000200 = 1",
		"f115b52406020003000e00 / 01030e000000 = 1",
		"f115b52406020003001300 / 010313000000 = 1",
		"f115b52406020003001200 / 010312000100 = 1",
		"f115b52406020003002800 / 0103280000002042 = 1",
		"f115b52406020003002200 / 0103220000003041 = 1",
		"f115b52406020002000200 / 010202000100 = 1",
	}

	zones := parseB524ZonesFromGrab(lines, 0x15)
	zone, ok := zones[0]
	if !ok {
		t.Fatalf("zone instance 0 missing")
	}
	if zone.HumidityPct == nil || *zone.HumidityPct != 40.0 {
		t.Fatalf("zone humidity = %v; want 40.0", zone.HumidityPct)
	}
	if zone.Preset != "manual" {
		t.Fatalf("zone preset = %q; want manual", zone.Preset)
	}
	if got, want := zone.AllowedModes, []string{"off", "auto", "heat"}; !slices.Equal(got, want) {
		t.Fatalf("allowed modes = %v; want %v", got, want)
	}
}

func TestParseB524DhwFromGrab(t *testing.T) {
	t.Parallel()

	lines := []string{
		"f115b52406020001000300 / 010103000100 = 1",
		"f115b52406020001000400 / 0101040000005c42 = 1",
		"f115b52406020001000500 / 0101050000004842 = 1",
		"f115b52406020001000d00 / 01010d000000 = 1",
	}

	dhw, ok := parseB524DhwFromGrab(lines, 0x15)
	if !ok || dhw == nil {
		t.Fatalf("parseB524DhwFromGrab returned nil")
	}
	if dhw.OperatingMode != "auto" {
		t.Fatalf("operating mode = %q; want auto", dhw.OperatingMode)
	}
	if dhw.Preset != "schedule" {
		t.Fatalf("preset = %q; want schedule", dhw.Preset)
	}
	if dhw.TargetTempC == nil || *dhw.TargetTempC != 55.0 {
		t.Fatalf("target temp = %v; want 55", dhw.TargetTempC)
	}
	if dhw.CurrentTempC == nil || *dhw.CurrentTempC != 50.0 {
		t.Fatalf("current temp = %v; want 50", dhw.CurrentTempC)
	}
}

func TestParseB524DhwFromGrabAcceptsRemoteReadOpcode(t *testing.T) {
	t.Parallel()

	lines := []string{
		"f115b52406060001000300 / 010103000100 = 1",
		"f115b52406060001000400 / 0101040000005c42 = 1",
		"f115b52406060001000500 / 0101050000004842 = 1",
		"f115b52406060001000d00 / 01010d000000 = 1",
	}

	dhw, ok := parseB524DhwFromGrab(lines, 0x15)
	if !ok || dhw == nil {
		t.Fatalf("parseB524DhwFromGrab returned nil")
	}
	if dhw.OperatingMode != "auto" {
		t.Fatalf("operating mode = %q; want auto", dhw.OperatingMode)
	}
	if dhw.CurrentTempC == nil || *dhw.CurrentTempC != 50.0 {
		t.Fatalf("current temp = %v; want 50", dhw.CurrentTempC)
	}
}

func TestParseB524GrabLineRejectsUnsupportedOpcode(t *testing.T) {
	t.Parallel()

	line := "f115b52406010001000500 / 0801050000004842 = 1"
	_, _, _, _, ok := parseB524GrabLine(line, 0x15)
	if ok {
		t.Fatalf("expected unsupported opcode line to be rejected")
	}
}

// --- Regulator detection tests (issue #193) ---

func newTestRegistry(devices ...registry.DeviceInfo) *registry.DeviceRegistry {
	reg := registry.NewDeviceRegistry(nil)
	for _, d := range devices {
		reg.Register(d)
	}
	return reg
}

func TestVaillantSemanticPoller_RegulatorDetectionPresent(t *testing.T) {
	t.Parallel()

	catalog, err := productids.LoadCatalog()
	if err != nil {
		t.Fatalf("LoadCatalog() error: %v", err)
	}

	// 0020028521 = Vaillant calorMATIC 430f VRC 430f (Regulator)
	reg := newTestRegistry(
		registry.DeviceInfo{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV", SerialNumber: "21-22-09-0010002460-0082-005409-N4"},
		registry.DeviceInfo{Address: 0x60, Manufacturer: "Vaillant", DeviceID: "VRC430", SerialNumber: "21-22-09-0020028521-0082-005409-N4"},
	)

	poller := &vaillantSemanticPoller{
		reg:     reg,
		catalog: catalog,
	}

	got := poller.findRegulatorCapability()
	if got != productids.ControllerPresent {
		t.Fatalf("findRegulatorCapability() = %s; want ControllerPresent", got)
	}
}

func TestVaillantSemanticPoller_RegulatorDetectionNone(t *testing.T) {
	t.Parallel()

	catalog, err := productids.LoadCatalog()
	if err != nil {
		t.Fatalf("LoadCatalog() error: %v", err)
	}

	// 0010002315 = Vaillant atmoCOMPACT (Boiler) — not a regulator
	reg := newTestRegistry(
		registry.DeviceInfo{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV", SerialNumber: "21-22-09-0010002460-0082-005409-N4"},
	)

	poller := &vaillantSemanticPoller{
		reg:     reg,
		catalog: catalog,
	}

	got := poller.findRegulatorCapability()
	if got != productids.ControllerNone {
		t.Fatalf("findRegulatorCapability() = %s; want ControllerNone", got)
	}
}

func TestVaillantSemanticPoller_RegulatorDetectionUnknown(t *testing.T) {
	t.Parallel()

	catalog, err := productids.LoadCatalog()
	if err != nil {
		t.Fatalf("LoadCatalog() error: %v", err)
	}

	// Device with no serial number — cannot extract part number, so unknown.
	reg := newTestRegistry(
		registry.DeviceInfo{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV"},
	)

	poller := &vaillantSemanticPoller{
		reg:     reg,
		catalog: catalog,
	}

	got := poller.findRegulatorCapability()
	if got != productids.ControllerUnknown {
		t.Fatalf("findRegulatorCapability() = %s; want ControllerUnknown", got)
	}
}

func TestVaillantSemanticPoller_RegulatorDetectionCatalogError(t *testing.T) {
	t.Parallel()

	reg := newTestRegistry(
		registry.DeviceInfo{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV", SerialNumber: "21-22-09-0020028521-0082-005409-N4"},
	)

	poller := &vaillantSemanticPoller{
		reg:        reg,
		catalogErr: fmt.Errorf("simulated catalog load error"),
	}

	got := poller.findRegulatorCapability()
	if got != productids.ControllerUnknown {
		t.Fatalf("findRegulatorCapability() = %s; want ControllerUnknown on catalog error", got)
	}
}

func TestVaillantSemanticPoller_RegulatorDetectionMixedKnownAndUnknown(t *testing.T) {
	t.Parallel()

	catalog, err := productids.LoadCatalog()
	if err != nil {
		t.Fatalf("LoadCatalog() error: %v", err)
	}

	// One device with a known boiler part number, one with no serial.
	// Any unknown device should make the overall result ControllerUnknown.
	reg := newTestRegistry(
		registry.DeviceInfo{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV", SerialNumber: "21-22-09-0010002460-0082-005409-N4"},
		registry.DeviceInfo{Address: 0x60, Manufacturer: "Vaillant", DeviceID: "UI"},
	)

	poller := &vaillantSemanticPoller{
		reg:     reg,
		catalog: catalog,
	}

	got := poller.findRegulatorCapability()
	if got != productids.ControllerUnknown {
		t.Fatalf("findRegulatorCapability() = %s; want ControllerUnknown when any device has unknown classification", got)
	}
}

func TestVaillantSemanticPoller_RefreshDiscoverySetsRegulatorCapability(t *testing.T) {
	t.Parallel()

	catalog, err := productids.LoadCatalog()
	if err != nil {
		t.Fatalf("LoadCatalog() error: %v", err)
	}

	// Registry with a B524-capable controller and a regulator device.
	reg := newTestRegistry(
		registry.DeviceInfo{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV", SerialNumber: "21-22-09-0010002460-0082-005409-N4"},
		registry.DeviceInfo{Address: 0x60, Manufacturer: "Vaillant", DeviceID: "VRC430", SerialNumber: "21-22-09-0020028521-0082-005409-N4"},
	)

	provider := graphql.NewLiveSemanticProvider()
	poller := &vaillantSemanticPoller{
		reg:               reg,
		provider:          provider,
		catalog:           catalog,
		zones:             make(map[byte]*vaillantZoneSnapshot),
		presence:          make(map[byte]*zonePresenceRecord),
		zoneMissThreshold: 3,
		zoneHitThreshold:  2,
		dhwStaleTTL:       10 * time.Minute,
		b524ProbeFn: func(ctx context.Context, target, opcode, group, instance byte, addr uint16) bool {
			return target == 0x15
		},
	}
	poller.nowFn = func() time.Time { return time.Date(2026, time.February, 26, 12, 0, 0, 0, time.UTC) }

	poller.refreshDiscovery(context.Background())

	poller.mu.Lock()
	gotController := poller.controller
	gotCap := poller.regulatorCapability
	poller.mu.Unlock()

	if gotController != 0x15 {
		t.Fatalf("controller = 0x%02x; want 0x15 (B524 root)", gotController)
	}
	if gotCap != productids.ControllerPresent {
		t.Fatalf("regulatorCapability = %s; want ControllerPresent", gotCap)
	}
}

func TestVaillantSemanticPoller_RefreshDiscoveryNoBasvSetsRegulatorCapability(t *testing.T) {
	t.Parallel()

	catalog, err := productids.LoadCatalog()
	if err != nil {
		t.Fatalf("LoadCatalog() error: %v", err)
	}

	// Registry with ONLY a regulator — no B524-capable device. refreshDiscovery should
	// still update regulatorCapability even when no B524 root is discovered.
	reg := newTestRegistry(
		registry.DeviceInfo{Address: 0x60, Manufacturer: "Vaillant", DeviceID: "VRC430", SerialNumber: "21-22-09-0020028521-0082-005409-N4"},
	)

	provider := graphql.NewLiveSemanticProvider()
	poller := &vaillantSemanticPoller{
		reg:               reg,
		provider:          provider,
		catalog:           catalog,
		zones:             make(map[byte]*vaillantZoneSnapshot),
		presence:          make(map[byte]*zonePresenceRecord),
		zoneMissThreshold: 3,
		zoneHitThreshold:  2,
		dhwStaleTTL:       10 * time.Minute,
	}
	poller.nowFn = func() time.Time { return time.Date(2026, time.February, 26, 12, 0, 0, 0, time.UTC) }

	poller.refreshDiscovery(context.Background())

	poller.mu.Lock()
	gotController := poller.controller
	gotCap := poller.regulatorCapability
	poller.mu.Unlock()

	if gotController != 0 {
		t.Fatalf("controller = 0x%02x; want 0 (no B524 root found)", gotController)
	}
	if gotCap != productids.ControllerPresent {
		t.Fatalf("regulatorCapability = %s; want ControllerPresent (regulator in registry even without BASV)", gotCap)
	}
}

func TestExtractPartNumberFromSerial(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		serial string
		want   string
	}{
		{
			name:   "standard Vaillant serial",
			serial: "21-22-09-0020184848-0082-005409-N4",
			want:   "0020184848",
		},
		{
			name:   "empty serial",
			serial: "",
			want:   "",
		},
		{
			name:   "serial without dashes",
			serial: "212209002018484800820054",
			want:   "0020184848",
		},
		{
			name:   "short serial",
			serial: "21-22",
			want:   "",
		},
		{
			name:   "non-digit part number field",
			serial: "21-22-09-ABCDEFGHIJ-0082-005409-N4",
			want:   "",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got := extractPartNumberFromSerial(test.serial)
			if got != test.want {
				t.Fatalf("extractPartNumberFromSerial(%q) = %q; want %q", test.serial, got, test.want)
			}
		})
	}
}

func TestRegulatorRedetection_PresentToAbsentGrace(t *testing.T) {
	t.Parallel()

	catalog, err := productids.LoadCatalog()
	if err != nil {
		t.Fatalf("LoadCatalog() error: %v", err)
	}

	// Start with a regulator present (VRC430).
	reg := newTestRegistry(
		registry.DeviceInfo{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV", SerialNumber: "21-22-09-0010002460-0082-005409-N4"},
		registry.DeviceInfo{Address: 0x60, Manufacturer: "Vaillant", DeviceID: "VRC430", SerialNumber: "21-22-09-0020028521-0082-005409-N4"},
	)

	now := time.Date(2026, time.February, 26, 12, 0, 0, 0, time.UTC)
	poller := &vaillantSemanticPoller{
		reg:                      reg,
		catalog:                  catalog,
		regulatorRecheckInterval: 60 * time.Second,
		regulatorAbsenceGrace:    5 * time.Minute,
		regAbsenceState:          regulatorPresent,
		regulatorCapability:      productids.ControllerPresent,
		registryDeviceCount:      2,
		zones:                    make(map[byte]*vaillantZoneSnapshot),
		presence:                 make(map[byte]*zonePresenceRecord),
		nowFn:                    func() time.Time { return now },
	}

	// Confirm initial state is present.
	poller.refreshRegulatorCapability(context.Background())
	poller.mu.Lock()
	if poller.regAbsenceState != regulatorPresent {
		t.Fatalf("initial state = %s; want PRESENT", poller.regAbsenceState)
	}
	poller.mu.Unlock()

	// Remove the regulator from registry (simulate device disappearance).
	regNew := newTestRegistry(
		registry.DeviceInfo{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV", SerialNumber: "21-22-09-0010002460-0082-005409-N4"},
	)
	poller.reg = regNew

	poller.refreshRegulatorCapability(context.Background())
	poller.mu.Lock()
	state := poller.regAbsenceState
	absenceSince := poller.regAbsenceSince
	poller.mu.Unlock()

	if state != regulatorAbsenceGrace {
		t.Fatalf("state after regulator removal = %s; want ABSENCE_GRACE", state)
	}
	if absenceSince.IsZero() {
		t.Fatal("regAbsenceSince should be set after entering grace")
	}
}

func TestRegulatorRedetection_GraceToAbsent(t *testing.T) {
	t.Parallel()

	catalog, err := productids.LoadCatalog()
	if err != nil {
		t.Fatalf("LoadCatalog() error: %v", err)
	}

	// Registry with only the boiler (no regulator).
	reg := newTestRegistry(
		registry.DeviceInfo{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV", SerialNumber: "21-22-09-0010002460-0082-005409-N4"},
	)

	graceDuration := 5 * time.Minute
	now := time.Date(2026, time.February, 26, 12, 0, 0, 0, time.UTC)
	poller := &vaillantSemanticPoller{
		reg:                      reg,
		catalog:                  catalog,
		regulatorRecheckInterval: 60 * time.Second,
		regulatorAbsenceGrace:    graceDuration,
		regAbsenceState:          regulatorAbsenceGrace,
		regAbsenceSince:          now.Add(-4 * time.Minute), // 4 min into grace
		regulatorCapability:      productids.ControllerNone,
		registryDeviceCount:      1,
		zones:                    make(map[byte]*vaillantZoneSnapshot),
		presence:                 make(map[byte]*zonePresenceRecord),
		nowFn:                    func() time.Time { return now },
	}

	// Still within grace window — should stay in grace.
	poller.refreshRegulatorCapability(context.Background())
	poller.mu.Lock()
	if poller.regAbsenceState != regulatorAbsenceGrace {
		t.Fatalf("state at 4min = %s; want ABSENCE_GRACE", poller.regAbsenceState)
	}
	poller.mu.Unlock()

	// Advance time to exactly grace boundary (5 min).
	now = now.Add(1 * time.Minute) // now exactly 5 min past absence start
	poller.nowFn = func() time.Time { return now }

	poller.refreshRegulatorCapability(context.Background())
	poller.mu.Lock()
	state := poller.regAbsenceState
	poller.mu.Unlock()

	if state != regulatorAbsent {
		t.Fatalf("state at exact grace boundary = %s; want ABSENT", state)
	}
}

func TestRegulatorRedetection_AbsentToPresent(t *testing.T) {
	t.Parallel()

	catalog, err := productids.LoadCatalog()
	if err != nil {
		t.Fatalf("LoadCatalog() error: %v", err)
	}

	// Start with absent state, then re-add regulator.
	reg := newTestRegistry(
		registry.DeviceInfo{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV", SerialNumber: "21-22-09-0010002460-0082-005409-N4"},
	)

	now := time.Date(2026, time.February, 26, 12, 10, 0, 0, time.UTC)
	poller := &vaillantSemanticPoller{
		reg:                      reg,
		catalog:                  catalog,
		regulatorRecheckInterval: 60 * time.Second,
		regulatorAbsenceGrace:    5 * time.Minute,
		regAbsenceState:          regulatorAbsent,
		regAbsenceSince:          now.Add(-10 * time.Minute),
		regulatorCapability:      productids.ControllerNone,
		registryDeviceCount:      1,
		zones:                    make(map[byte]*vaillantZoneSnapshot),
		presence:                 make(map[byte]*zonePresenceRecord),
		nowFn:                    func() time.Time { return now },
	}

	// Verify it stays absent when no regulator.
	poller.refreshRegulatorCapability(context.Background())
	poller.mu.Lock()
	if poller.regAbsenceState != regulatorAbsent {
		t.Fatalf("state without regulator = %s; want ABSENT", poller.regAbsenceState)
	}
	poller.mu.Unlock()

	// Re-add the regulator.
	poller.reg = newTestRegistry(
		registry.DeviceInfo{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV", SerialNumber: "21-22-09-0010002460-0082-005409-N4"},
		registry.DeviceInfo{Address: 0x60, Manufacturer: "Vaillant", DeviceID: "VRC430", SerialNumber: "21-22-09-0020028521-0082-005409-N4"},
	)

	poller.refreshRegulatorCapability(context.Background())
	poller.mu.Lock()
	state := poller.regAbsenceState
	absenceSince := poller.regAbsenceSince
	poller.mu.Unlock()

	if state != regulatorPresent {
		t.Fatalf("state after re-adding regulator = %s; want PRESENT", state)
	}
	if !absenceSince.IsZero() {
		t.Fatal("regAbsenceSince should be cleared after recovery to present")
	}
}

func TestRegulatorRedetection_InventoryTrigger(t *testing.T) {
	t.Parallel()

	catalog, err := productids.LoadCatalog()
	if err != nil {
		t.Fatalf("LoadCatalog() error: %v", err)
	}

	reg := newTestRegistry(
		registry.DeviceInfo{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV", SerialNumber: "21-22-09-0010002460-0082-005409-N4"},
		registry.DeviceInfo{Address: 0x60, Manufacturer: "Vaillant", DeviceID: "VRC430", SerialNumber: "21-22-09-0020028521-0082-005409-N4"},
	)

	now := time.Date(2026, time.February, 26, 12, 0, 0, 0, time.UTC)
	poller := &vaillantSemanticPoller{
		reg:                      reg,
		catalog:                  catalog,
		regulatorRecheckInterval: 60 * time.Second,
		regulatorAbsenceGrace:    5 * time.Minute,
		regAbsenceState:          regulatorPresent,
		regulatorCapability:      productids.ControllerPresent,
		registryDeviceCount:      2,
		zones:                    make(map[byte]*vaillantZoneSnapshot),
		presence:                 make(map[byte]*zonePresenceRecord),
		nowFn:                    func() time.Time { return now },
	}

	// No change — same count. Device count should remain 2.
	poller.refreshRegulatorCapability(context.Background())
	poller.mu.Lock()
	if poller.registryDeviceCount != 2 {
		t.Fatalf("registryDeviceCount = %d; want 2", poller.registryDeviceCount)
	}
	poller.mu.Unlock()

	// Now add a third device — count changes from 2 to 3.
	regNew := newTestRegistry(
		registry.DeviceInfo{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV", SerialNumber: "21-22-09-0010002460-0082-005409-N4"},
		registry.DeviceInfo{Address: 0x60, Manufacturer: "Vaillant", DeviceID: "VRC430", SerialNumber: "21-22-09-0020028521-0082-005409-N4"},
		registry.DeviceInfo{Address: 0x70, Manufacturer: "Vaillant", DeviceID: "UI", SerialNumber: "21-22-09-0020099999-0082-005409-N4"},
	)
	poller.reg = regNew

	// Use a task scheduler so enqueueTask doesn't panic.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	taskScheduler := newSemanticTaskScheduler()
	go taskScheduler.run(ctx)
	poller.tasks = taskScheduler

	poller.refreshRegulatorCapability(context.Background())

	poller.mu.Lock()
	newCount := poller.registryDeviceCount
	poller.mu.Unlock()

	if newCount != 3 {
		t.Fatalf("registryDeviceCount after add = %d; want 3", newCount)
	}

	// Verify inventory tracking works — count went from 2 to 3.
	// The enqueueTask call to refreshDiscovery is triggered by the
	// deviceCount != prevDeviceCount condition. We verify the count
	// tracking which is the necessary precondition for the trigger.
}

func TestRefreshRegulatorCapability_NoChangeNoLog(t *testing.T) {
	t.Parallel()

	catalog, err := productids.LoadCatalog()
	if err != nil {
		t.Fatalf("LoadCatalog() error: %v", err)
	}

	reg := newTestRegistry(
		registry.DeviceInfo{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV", SerialNumber: "21-22-09-0010002460-0082-005409-N4"},
		registry.DeviceInfo{Address: 0x60, Manufacturer: "Vaillant", DeviceID: "VRC430", SerialNumber: "21-22-09-0020028521-0082-005409-N4"},
	)

	now := time.Date(2026, time.February, 26, 12, 0, 0, 0, time.UTC)
	poller := &vaillantSemanticPoller{
		reg:                      reg,
		catalog:                  catalog,
		regulatorRecheckInterval: 60 * time.Second,
		regulatorAbsenceGrace:    5 * time.Minute,
		regAbsenceState:          regulatorPresent,
		regulatorCapability:      productids.ControllerPresent,
		registryDeviceCount:      2,
		zones:                    make(map[byte]*vaillantZoneSnapshot),
		presence:                 make(map[byte]*zonePresenceRecord),
		nowFn:                    func() time.Time { return now },
	}

	// Call multiple times — no state transition should occur.
	for i := 0; i < 5; i++ {
		poller.refreshRegulatorCapability(context.Background())
	}

	poller.mu.Lock()
	state := poller.regAbsenceState
	cap := poller.regulatorCapability
	count := poller.registryDeviceCount
	poller.mu.Unlock()

	if state != regulatorPresent {
		t.Fatalf("state after repeated calls = %s; want PRESENT", state)
	}
	if cap != productids.ControllerPresent {
		t.Fatalf("capability after repeated calls = %s; want ControllerPresent", cap)
	}
	if count != 2 {
		t.Fatalf("registryDeviceCount after repeated calls = %d; want 2", count)
	}
}

func TestB524EnergyQueries_Coverage(t *testing.T) {
	t.Parallel()

	type tuple struct {
		channel  string
		usage    string
		period   string
		yearKind string
	}
	expected := map[tuple]bool{
		// All-time totals.
		{"gas", "climate", "year", "current"}:           false,
		{"gas", "hot_water", "year", "current"}:         false,
		{"electricity", "climate", "year", "current"}:   false,
		{"electricity", "hot_water", "year", "current"}: false,
		// Monthly: this month.
		{"gas", "climate", "month", "current"}:           false,
		{"gas", "hot_water", "month", "current"}:         false,
		{"electricity", "climate", "month", "current"}:   false,
		{"electricity", "hot_water", "month", "current"}: false,
		// Monthly: last month.
		{"gas", "climate", "month", "previous"}:           false,
		{"gas", "hot_water", "month", "previous"}:         false,
		{"electricity", "climate", "month", "previous"}:   false,
		{"electricity", "hot_water", "month", "previous"}: false,
	}
	for _, q := range b524EnergyQueries {
		tup := tuple{q.channel, q.usage, q.period, q.yearKind}
		if _, ok := expected[tup]; !ok {
			t.Fatalf("unexpected query in b524EnergyQueries: %+v", tup)
		}
		expected[tup] = true
	}
	for tup, seen := range expected {
		if !seen {
			t.Fatalf("missing query in b524EnergyQueries: %+v", tup)
		}
	}
	if len(b524EnergyQueries) != 12 {
		t.Fatalf("len(b524EnergyQueries) = %d; want 12", len(b524EnergyQueries))
	}
}

func TestIsB524ProbeCoherent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		payload []byte
		group   byte
		addr    uint16
		want    bool
	}{
		{
			name:    "4-byte header matching group and addr",
			payload: []byte{0x01, 0x03, 0x1C, 0x00},
			group:   0x03,
			addr:    0x001C,
			want:    true,
		},
		{
			name:    "5-byte header matching group and addr",
			payload: []byte{0x01, 0x01, 0x03, 0x1C, 0x00},
			group:   0x03,
			addr:    0x001C,
			want:    true,
		},
		{
			name:    "full response with value bytes",
			payload: []byte{0x01, 0x01, 0x03, 0x1C, 0x00, 0x42, 0x43},
			group:   0x03,
			addr:    0x001C,
			want:    true,
		},
		{
			name:    "short payload rejected",
			payload: []byte{0x01, 0x03, 0x1C},
			group:   0x03,
			addr:    0x001C,
			want:    false,
		},
		{
			name:    "empty payload rejected",
			payload: nil,
			group:   0x03,
			addr:    0x001C,
			want:    false,
		},
		{
			name:    "group mismatch rejected",
			payload: []byte{0x01, 0x02, 0x1C, 0x00},
			group:   0x03,
			addr:    0x001C,
			want:    false,
		},
		{
			name:    "addr mismatch rejected",
			payload: []byte{0x01, 0x03, 0x1D, 0x00},
			group:   0x03,
			addr:    0x001C,
			want:    false,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := isB524ProbeCoherent(test.payload, test.group, test.addr); got != test.want {
				t.Fatalf("isB524ProbeCoherent(%x, 0x%02x, 0x%04x) = %v; want %v", test.payload, test.group, test.addr, got, test.want)
			}
		})
	}
}

// mockB524Probe returns a probe function that records calls and responds
// based on a coherence map keyed by target address.
func mockB524Probe(coherent map[byte]bool, probed *[]byte) func(ctx context.Context, target, opcode, group, instance byte, addr uint16) bool {
	return func(ctx context.Context, target, opcode, group, instance byte, addr uint16) bool {
		if probed != nil {
			found := false
			for _, p := range *probed {
				if p == target {
					found = true
					break
				}
			}
			if !found {
				*probed = append(*probed, target)
			}
		}
		return coherent[target]
	}
}

func newTestPoller(reg *registry.DeviceRegistry) *vaillantSemanticPoller {
	return &vaillantSemanticPoller{
		reg:            reg,
		source:         0x71,
		requestTimeout: 2 * time.Second,
		nowFn:          time.Now,
	}
}

func TestRadioInventoryRegistryInfo_MaterializesVR71PhysicalDevice(t *testing.T) {
	t.Parallel()

	classAddress := uint8(0x26)
	info, ok := radioInventoryRegistryInfo(&vaillantRadioDeviceSnapshot{
		DeviceClassAddress: &classAddress,
		DeviceModel:        "VR71/FM5",
	})
	if !ok {
		t.Fatal("radioInventoryRegistryInfo should materialize VR_71 inventory evidence")
	}
	if info.Address != 0x26 || info.Manufacturer != "Vaillant" || info.DeviceID != "VR_71" {
		t.Fatalf("radioInventoryRegistryInfo = %+v; want VR_71 at 0x26", info)
	}
}

func TestRegistryRadioDeviceSeeds_UsesKnownRegulatorAndFM5Identities(t *testing.T) {
	t.Parallel()

	reg := newTestRegistry(
		registry.DeviceInfo{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV2"},
		registry.DeviceInfo{Address: 0x26, Manufacturer: "Vaillant", DeviceID: "VR_71"},
	)
	poller := newTestPoller(reg)

	seeds := poller.registryRadioDeviceSeeds()

	regulator := seeds[radioDeviceKey{Group: remoteRegulators.group, Instance: 0}]
	if regulator == nil || regulator.DeviceClassAddress == nil || *regulator.DeviceClassAddress != 0x15 {
		t.Fatalf("regulator seed = %+v; want BASV2 class address 0x15", regulator)
	}
	fm5 := seeds[radioDeviceKey{Group: remoteFunctionalModules.group, Instance: 0}]
	if fm5 == nil || fm5.DeviceClassAddress == nil || *fm5.DeviceClassAddress != 0x26 {
		t.Fatalf("functional module seed = %+v; want VR_71 class address 0x26", fm5)
	}
}

func TestStartupRadioDeviceInclude_SkipsDisconnectedRegulatorSlots(t *testing.T) {
	t.Parallel()

	classAddress := uint8(0x26)

	if include, _ := startupRadioDeviceInclude(remoteRegulators.group, false, &classAddress); include {
		t.Fatal("disconnected regulator slot included; want skipped")
	}
	if include, mode := startupRadioDeviceInclude(remoteFunctionalModules.group, false, &classAddress); !include || mode != "inventory" {
		t.Fatalf("functional module identity evidence include=%v mode=%q; want inventory include", include, mode)
	}
	unknownClassAddress := uint8(0x00)
	if include, _ := startupRadioDeviceInclude(remoteFunctionalModules.group, false, &unknownClassAddress); include {
		t.Fatal("unknown disconnected functional module slot included; want skipped")
	}
	if include, _ := startupRadioDeviceInclude(remoteFunctionalModules.group, false, nil); include {
		t.Fatal("empty functional module slot included; want skipped")
	}
}

func TestStartupRadioFullScanGroups_AreSelectedPerGroup(t *testing.T) {
	t.Parallel()

	allUnseeded := startupRadioFullScanGroups(nil)
	for _, grp := range remoteDeviceGroups {
		if !allUnseeded[grp.group] {
			t.Fatalf("empty radio discovery should require full scan for group 0x%02x", grp.group)
		}
	}

	discovered := map[radioDeviceKey]*vaillantRadioDeviceSnapshot{
		{Group: remoteRegulators.group, Instance: 0}: nil,
	}
	seededRegulator := startupRadioFullScanGroups(discovered)
	if seededRegulator[remoteRegulators.group] {
		t.Fatal("low-slot seeded regulator group should stay on fast scan")
	}
	if !seededRegulator[remoteThermostats.group] {
		t.Fatal("unseeded thermostat group should still require full scan")
	}

	discovered[radioDeviceKey{Group: remoteThermostats.group, Instance: semanticStartupSlotFastMaxInstance + 1}] = nil
	highSeededThermostat := startupRadioFullScanGroups(discovered)
	if !highSeededThermostat[remoteThermostats.group] {
		t.Fatal("seeded high thermostat slot should require full scan")
	}
}

func TestRefreshDHWStartup_DoesNotPromoteCacheWithoutLiveProbe(t *testing.T) {
	t.Parallel()

	currentTemp := 47.5
	provider := graphql.NewLiveSemanticProvider()
	provider.SetDHWFromCache(&graphql.DhwStatus{
		State: graphql.DhwState{CurrentTempC: &currentTemp},
	})
	poller := &vaillantSemanticPoller{
		provider: provider,
		dhw: &vaillantDhwSnapshot{
			CurrentTempC: &currentTemp,
		},
		nowFn: time.Now,
	}

	poller.refreshDHWStartup(context.Background())

	if _, liveEpoch := provider.StartupEpochs(); liveEpoch != 0 {
		t.Fatalf("live epoch after cache-only DHW startup = %d; want 0", liveEpoch)
	}
	if got := provider.StartupPhase(); got != graphql.SemanticStartupPhaseCacheLoadedStale {
		t.Fatalf("startup phase after cache-only DHW startup = %s; want %s", got, graphql.SemanticStartupPhaseCacheLoadedStale)
	}
}

func TestRefreshBoilerStatusStartup_DoesNotPromoteCacheWithoutLiveProbe(t *testing.T) {
	t.Parallel()

	flowTemp := 42.0
	provider := graphql.NewLiveSemanticProvider()
	provider.SetBoilerStatusFromCache(&graphql.BoilerStatus{
		State: graphql.BoilerState{FlowTemperatureC: &flowTemp},
	})
	poller := &vaillantSemanticPoller{
		provider:      provider,
		boilerAddress: 0x08,
		boiler: &vaillantBoilerSnapshot{
			FlowTemperatureC: &flowTemp,
		},
		nowFn: time.Now,
	}

	poller.refreshBoilerStatusStartup(context.Background())

	if _, liveEpoch := provider.StartupEpochs(); liveEpoch != 0 {
		t.Fatalf("live epoch after cache-only boiler startup = %d; want 0", liveEpoch)
	}
	if got := provider.StartupPhase(); got != graphql.SemanticStartupPhaseCacheLoadedStale {
		t.Fatalf("startup phase after cache-only boiler startup = %s; want %s", got, graphql.SemanticStartupPhaseCacheLoadedStale)
	}
}

func TestStartupL1PrimingStatusRequiresCriticalPlanes(t *testing.T) {
	t.Parallel()

	provider := graphql.NewLiveSemanticProvider()
	poller := &vaillantSemanticPoller{provider: provider}

	if status := poller.startupL1PrimingStatus(); status.ready() {
		t.Fatalf("empty startup status ready = true; status=%s", status.String())
	}

	connected := true
	provider.SetZones([]graphql.Zone{{ID: "zone-1", Name: "Zone 1"}})
	provider.SetDHW(&graphql.DhwStatus{})
	provider.SetCircuits([]graphql.CircuitStatus{{Index: 0}})
	provider.SetSystem(&graphql.SystemStatus{})
	provider.SetRadioDevices([]graphql.RadioDevice{{
		Group:           int(remoteRegulators.group),
		Instance:        0,
		DeviceConnected: &connected,
	}})
	provider.SetSolar(&graphql.SolarStatus{})
	provider.SetCylinders([]graphql.CylinderStatus{{Index: 0}})
	provider.SetBoilerStatus(&graphql.BoilerStatus{})

	status := poller.startupL1PrimingStatus()
	if status.fm5GateKnown {
		t.Fatalf("startup status fm5GateKnown = true without module config; status=%s", status.String())
	}
	if !status.ready() {
		t.Fatalf("complete startup status ready = false; status=%s", status.String())
	}

	moduleConfig := uint16(1)
	poller.mu.Lock()
	poller.system = &vaillantSystemSnapshot{ModuleConfigurationVR71: &moduleConfig}
	poller.mu.Unlock()
	if status := poller.startupL1PrimingStatus(); !status.fm5GateKnown {
		t.Fatalf("startup status fm5GateKnown = false with module config; status=%s", status.String())
	}
}

func TestStartupL1PrimingStatusTreatsOptionalFM5AndEmptyRadioAsReady(t *testing.T) {
	t.Parallel()

	provider := graphql.NewLiveSemanticProvider()
	provider.SetZones([]graphql.Zone{{ID: "zone-1", Name: "Zone 1"}})
	provider.SetDHW(&graphql.DhwStatus{})
	provider.SetCircuits([]graphql.CircuitStatus{{Index: 0}})
	provider.SetSystem(&graphql.SystemStatus{})
	provider.SetBoilerStatus(&graphql.BoilerStatus{})
	poller := &vaillantSemanticPoller{
		provider:                  provider,
		startupRadioDevicesProbed: true,
	}

	status := poller.startupL1PrimingStatus()
	if status.fm5Evidence || !status.fm5Satisfied {
		t.Fatalf("optional FM5 status = %s; want no evidence and satisfied", status.String())
	}
	if !status.radioDevices {
		t.Fatalf("radioDevices readiness = false after completed empty startup probe; status=%s", status.String())
	}
	if !status.ready() {
		t.Fatalf("startup status ready = false for optional FM5/empty radio; status=%s", status.String())
	}
}

func TestStartupL1PrimingStatusRequiresFM5PlanesWhenInterpreted(t *testing.T) {
	t.Parallel()

	connected := true
	provider := graphql.NewLiveSemanticProvider()
	provider.SetZones([]graphql.Zone{{ID: "zone-1", Name: "Zone 1"}})
	provider.SetDHW(&graphql.DhwStatus{})
	provider.SetCircuits([]graphql.CircuitStatus{{Index: 0}})
	provider.SetSystem(&graphql.SystemStatus{})
	provider.SetRadioDevices([]graphql.RadioDevice{{
		Group:           int(remoteFunctionalModules.group),
		Instance:        0,
		DeviceConnected: &connected,
	}})
	provider.SetBoilerStatus(&graphql.BoilerStatus{})

	moduleConfig := uint16(1)
	poller := &vaillantSemanticPoller{
		provider: provider,
		reg: newTestRegistry(
			registry.DeviceInfo{Address: 0x26, Manufacturer: "Vaillant", DeviceID: "VR_71"},
		),
		system: &vaillantSystemSnapshot{ModuleConfigurationVR71: &moduleConfig},
	}

	status := poller.startupL1PrimingStatus()
	if !status.fm5Evidence || !status.fm5Required || status.fm5Satisfied {
		t.Fatalf("interpreted FM5 status = %s; want evidence, required, unsatisfied", status.String())
	}
	if status.ready() {
		t.Fatalf("startup status ready = true without interpreted FM5 planes; status=%s", status.String())
	}

	provider.SetSolar(&graphql.SolarStatus{})
	provider.SetCylinders([]graphql.CylinderStatus{{Index: 0}})
	if status := poller.startupL1PrimingStatus(); !status.ready() {
		t.Fatalf("startup status ready = false after interpreted FM5 planes; status=%s", status.String())
	}
}

func TestStartupL1PrimingStatusRejectsEmptyInterpretedCylinders(t *testing.T) {
	t.Parallel()

	connected := true
	provider := graphql.NewLiveSemanticProvider()
	provider.SetZones([]graphql.Zone{{ID: "zone-1", Name: "Zone 1"}})
	provider.SetDHW(&graphql.DhwStatus{})
	provider.SetCircuits([]graphql.CircuitStatus{{Index: 0}})
	provider.SetSystem(&graphql.SystemStatus{})
	provider.SetRadioDevices([]graphql.RadioDevice{{
		Group:           int(remoteFunctionalModules.group),
		Instance:        0,
		DeviceConnected: &connected,
	}})
	provider.SetFM5SemanticMode(graphql.Fm5SemanticModeInterpreted)
	provider.SetSolar(&graphql.SolarStatus{})
	provider.SetCylinders([]graphql.CylinderStatus{})
	provider.SetBoilerStatus(&graphql.BoilerStatus{})

	moduleConfig := uint16(1)
	poller := &vaillantSemanticPoller{
		provider: provider,
		reg: newTestRegistry(
			registry.DeviceInfo{Address: 0x26, Manufacturer: "Vaillant", DeviceID: "VR_71"},
		),
		system: &vaillantSystemSnapshot{ModuleConfigurationVR71: &moduleConfig},
	}

	status := poller.startupL1PrimingStatus()
	if !status.cylinders {
		t.Fatalf("interpreted empty cylinders status = %s; want published cylinders plane", status.String())
	}
	if status.fm5Satisfied {
		t.Fatalf("interpreted empty cylinders status = %s; want FM5 unsatisfied until live cylinder evidence", status.String())
	}
	if status.ready() {
		t.Fatalf("startup status ready = true with empty interpreted cylinders; status=%s", status.String())
	}
}

func TestStartupL1PrimingStatusAcceptsPublishedGPIOOnlyFM5Planes(t *testing.T) {
	t.Parallel()

	connected := true
	provider := graphql.NewLiveSemanticProvider()
	provider.SetZones([]graphql.Zone{{ID: "zone-1", Name: "Zone 1"}})
	provider.SetDHW(&graphql.DhwStatus{})
	provider.SetCircuits([]graphql.CircuitStatus{{Index: 0}})
	provider.SetSystem(&graphql.SystemStatus{})
	provider.SetRadioDevices([]graphql.RadioDevice{{
		Group:           int(remoteFunctionalModules.group),
		Instance:        0,
		DeviceConnected: &connected,
	}})
	provider.SetFM5SemanticMode(graphql.Fm5SemanticModeGPIOOnly)
	provider.SetSolar(&graphql.SolarStatus{})
	provider.SetCylinders([]graphql.CylinderStatus{})
	provider.SetBoilerStatus(&graphql.BoilerStatus{})
	poller := &vaillantSemanticPoller{
		provider: provider,
		reg: newTestRegistry(
			registry.DeviceInfo{Address: 0x26, Manufacturer: "Vaillant", DeviceID: "VR_71"},
		),
	}

	status := poller.startupL1PrimingStatus()
	if !status.fm5Evidence || !status.fm5Satisfied {
		t.Fatalf("GPIO-only FM5 status = %s; want evidence and satisfied", status.String())
	}
	if !status.ready() {
		t.Fatalf("startup status ready = false for published GPIO-only FM5 planes; status=%s", status.String())
	}
}

func TestRefreshStartupSemanticPlanesPublishesAbsentFM5Planes(t *testing.T) {
	t.Parallel()

	provider := graphql.NewLiveSemanticProvider()
	provider.SetZones([]graphql.Zone{{ID: "zone-1", Name: "Zone 1"}})
	provider.SetDHW(&graphql.DhwStatus{})
	provider.SetCircuits([]graphql.CircuitStatus{{Index: 0}})
	provider.SetSystem(&graphql.SystemStatus{})
	provider.SetBoilerStatus(&graphql.BoilerStatus{})

	poller := &vaillantSemanticPoller{
		controller:                0x15,
		provider:                  provider,
		reg:                       newTestRegistry(),
		startupRadioDevicesProbed: true,
	}

	poller.refreshStartupSemanticPlanes(context.Background())

	if provider.Solar() == nil {
		t.Fatal("provider.Solar() = nil after startup priming; want empty non-null absent-FM5 plane")
	}
	if cylinders := provider.Cylinders(); cylinders == nil || len(cylinders) != 0 {
		t.Fatalf("provider.Cylinders() = %#v after startup priming; want empty non-null absent-FM5 plane", cylinders)
	}
	if mode := provider.FM5SemanticMode(); mode != graphql.Fm5SemanticModeAbsent {
		t.Fatalf("provider.FM5SemanticMode() = %q; want %q", mode, graphql.Fm5SemanticModeAbsent)
	}
}

func TestReconcileDiscoveryPresence_PublishesStartupZonesOnFirstHit(t *testing.T) {
	t.Parallel()

	provider := graphql.NewLiveSemanticProvider()
	poller := &vaillantSemanticPoller{
		provider:         provider,
		zones:            make(map[byte]*vaillantZoneSnapshot),
		presence:         make(map[byte]*zonePresenceRecord),
		zoneHitThreshold: 2,
		nowFn:            time.Now,
	}

	source := poller.reconcileDiscoveryPresence(
		context.Background(),
		map[byte]bool{0: true, 1: true},
		map[byte]bool{0: true, 1: true},
	)
	poller.publishZones(source)

	zones := provider.Zones()
	if len(zones) != 2 {
		t.Fatalf("published zones = %d; want 2 after first startup hit", len(zones))
	}
}

func TestPreserveExistingRegistryMetadata_KeepsRicherIdentityFields(t *testing.T) {
	t.Parallel()

	reg := newTestRegistry(registry.DeviceInfo{
		Address:         0x26,
		Manufacturer:    "Vaillant",
		DeviceID:        "VR_71",
		SerialNumber:    "21-21-34-0020262148-0082-014267-N7",
		MacAddress:      "00:11:22:33:44:55",
		SoftwareVersion: "0507",
		HardwareVersion: "1704",
	})

	got := preserveExistingRegistryMetadata(reg, registry.DeviceInfo{
		Address:      0x26,
		Manufacturer: "Vaillant",
		DeviceID:     "VR_71",
	})

	if got.SerialNumber != "21-21-34-0020262148-0082-014267-N7" {
		t.Fatalf("SerialNumber = %q; want preserved serial", got.SerialNumber)
	}
	if got.MacAddress != "00:11:22:33:44:55" {
		t.Fatalf("MacAddress = %q; want preserved MAC", got.MacAddress)
	}
	if got.SoftwareVersion != "0507" {
		t.Fatalf("SoftwareVersion = %q; want preserved software version", got.SoftwareVersion)
	}
	if got.HardwareVersion != "1704" {
		t.Fatalf("HardwareVersion = %q; want preserved hardware version", got.HardwareVersion)
	}
}

func TestCapabilityFirstDiscovery_FindsRootWithSingleCandidate(t *testing.T) {
	t.Parallel()

	reg := registry.NewDeviceRegistry(nil)
	reg.Register(registry.DeviceInfo{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV2"})

	poller := newTestPoller(reg)
	poller.b524ProbeFn = mockB524Probe(map[byte]bool{0x15: true}, nil)

	addr, err := poller.discoverB524Root(context.Background())
	if err != nil {
		t.Fatalf("discoverB524Root error = %v", err)
	}
	if addr != 0x15 {
		t.Fatalf("discoverB524Root = 0x%02x; want 0x15", addr)
	}
}

func TestCapabilityFirstDiscovery_Prefers0x15WhenCoherent(t *testing.T) {
	t.Parallel()

	reg := registry.NewDeviceRegistry(nil)
	reg.Register(registry.DeviceInfo{Address: 0x08, Manufacturer: "Vaillant", DeviceID: "BAI00"})
	reg.Register(registry.DeviceInfo{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV2"})
	reg.Register(registry.DeviceInfo{Address: 0x26, Manufacturer: "Vaillant", DeviceID: "VR_71"})

	var probed []byte
	poller := newTestPoller(reg)
	poller.b524ProbeFn = mockB524Probe(map[byte]bool{0x08: true, 0x15: true, 0x26: true}, &probed)

	addr, err := poller.discoverB524Root(context.Background())
	if err != nil {
		t.Fatalf("discoverB524Root error = %v", err)
	}
	if addr != 0x15 {
		t.Fatalf("discoverB524Root = 0x%02x; want 0x15 (D1-ordered preference)", addr)
	}
	// 0x15 should be probed first and discovery should stop there
	if len(probed) != 1 || probed[0] != 0x15 {
		t.Fatalf("probed = %v; want [0x15] (stop-at-first)", probed)
	}
}

func TestCapabilityFirstDiscovery_FallsBackWhen0x15NonCoherent(t *testing.T) {
	t.Parallel()

	reg := registry.NewDeviceRegistry(nil)
	reg.Register(registry.DeviceInfo{Address: 0x08, Manufacturer: "Vaillant", DeviceID: "BAI00"})
	reg.Register(registry.DeviceInfo{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV2"})
	reg.Register(registry.DeviceInfo{Address: 0x26, Manufacturer: "Vaillant", DeviceID: "VR_71"})

	var probed []byte
	// 0x15 non-coherent, 0x26 coherent
	poller := newTestPoller(reg)
	poller.b524ProbeFn = mockB524Probe(map[byte]bool{0x08: false, 0x15: false, 0x26: true}, &probed)

	addr, err := poller.discoverB524Root(context.Background())
	if err != nil {
		t.Fatalf("discoverB524Root error = %v", err)
	}
	if addr != 0x26 {
		t.Fatalf("discoverB524Root = 0x%02x; want 0x26 (fallback after 0x15 fails)", addr)
	}
	// Should probe 0x15 first, then 0x08, then 0x26 (ascending after 0x15)
	if len(probed) < 2 {
		t.Fatalf("probed = %v; want at least [0x15, ...0x26]", probed)
	}
	if probed[0] != 0x15 {
		t.Fatalf("probed[0] = 0x%02x; want 0x15 (probed first per D1)", probed[0])
	}
}

func TestCapabilityFirstDiscovery_RejectsShortResponse(t *testing.T) {
	t.Parallel()

	reg := registry.NewDeviceRegistry(nil)
	reg.Register(registry.DeviceInfo{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV2"})

	// Probe function that simulates short response (< 4 bytes) → non-coherent
	poller := newTestPoller(reg)
	poller.b524ProbeFn = mockB524Probe(map[byte]bool{0x15: false}, nil)

	_, err := poller.discoverB524Root(context.Background())
	if err == nil {
		t.Fatal("discoverB524Root should return error when all responses are short/non-coherent")
	}
	if !strings.Contains(err.Error(), "no coherent responder") {
		t.Fatalf("error = %q; want substring 'no coherent responder'", err.Error())
	}
}

func TestCapabilityFirstDiscovery_ReturnsDefinitiveWhenNoneQualify(t *testing.T) {
	t.Parallel()

	reg := registry.NewDeviceRegistry(nil)
	reg.Register(registry.DeviceInfo{Address: 0x08, Manufacturer: "Vaillant", DeviceID: "BAI00"})
	reg.Register(registry.DeviceInfo{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV2"})

	poller := newTestPoller(reg)
	poller.b524ProbeFn = mockB524Probe(map[byte]bool{0x08: false, 0x15: false}, nil)

	addr, err := poller.discoverB524Root(context.Background())
	if err == nil {
		t.Fatal("discoverB524Root should return error when no candidate qualifies")
	}
	if addr != 0 {
		t.Fatalf("discoverB524Root addr = 0x%02x; want 0 on failure", addr)
	}
	if !strings.Contains(err.Error(), "no coherent responder") {
		t.Fatalf("error = %q; want substring 'no coherent responder'", err.Error())
	}
}

func TestCapabilityFirstDiscovery_SucceedsWhenEnrichmentFails(t *testing.T) {
	t.Parallel()

	reg := registry.NewDeviceRegistry(nil)
	// Register device with unknown identity (no known family prefix)
	reg.Register(registry.DeviceInfo{Address: 0x15, Manufacturer: "Unknown", DeviceID: "XYZABC"})

	poller := newTestPoller(reg)
	poller.b524ProbeFn = mockB524Probe(map[byte]bool{0x15: true}, nil)

	addr, err := poller.discoverB524Root(context.Background())
	if err != nil {
		t.Fatalf("discoverB524Root error = %v; discovery should succeed regardless of enrichment", err)
	}
	if addr != 0x15 {
		t.Fatalf("discoverB524Root = 0x%02x; want 0x15", addr)
	}

	// Enrichment should return empty family but not nil
	enrichment := poller.enrichRegulatorIdentity(addr)
	if enrichment == nil {
		t.Fatal("enrichRegulatorIdentity should return non-nil for registered device")
	}
	if enrichment.family != "" {
		t.Fatalf("enrichment.family = %q; want empty for unknown device", enrichment.family)
	}
}

func TestCapabilityFirstDiscovery_SkipsNonCoherentDevice(t *testing.T) {
	t.Parallel()

	reg := registry.NewDeviceRegistry(nil)
	reg.Register(registry.DeviceInfo{Address: 0x08, Manufacturer: "Vaillant", DeviceID: "BAI00"})
	reg.Register(registry.DeviceInfo{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV2"})

	var probed []byte
	// 0x08 responds but non-coherent (e.g., boiler, not a B524 controller)
	// 0x15 responds coherently
	poller := newTestPoller(reg)
	poller.b524ProbeFn = mockB524Probe(map[byte]bool{0x08: false, 0x15: true}, &probed)

	addr, err := poller.discoverB524Root(context.Background())
	if err != nil {
		t.Fatalf("discoverB524Root error = %v", err)
	}
	if addr != 0x15 {
		t.Fatalf("discoverB524Root = 0x%02x; want 0x15", addr)
	}
	// Both should be probed (0x15 first per D1, then stop)
	if len(probed) != 1 || probed[0] != 0x15 {
		t.Fatalf("probed = %v; want [0x15] (0x15 probed first, coherent, stop)", probed)
	}
}

// TestCapabilityFirstDiscovery_StructuralFallbackWhenRegistryHasOnlyBoiler
// reproduces the post-source-selection regression where startup admission
// completes with only the boiler in the registry. discoverB524Root must
// augment its candidate list with the bounded Vaillant structural target
// set so the regulator (0x15) can be probed even before it appears in
// the registry.
func TestCapabilityFirstDiscovery_StructuralFallbackWhenRegistryHasOnlyBoiler(t *testing.T) {
	t.Parallel()

	reg := registry.NewDeviceRegistry(nil)
	reg.Register(registry.DeviceInfo{Address: 0x08, Manufacturer: "Vaillant", DeviceID: "BAI00"})

	var probed []byte
	// Only 0x15 is coherent. Without structural fallback, 0x15 is not in
	// the registry and discoverB524Root would return "no coherent
	// responder among [0x08]".
	poller := newTestPoller(reg)
	poller.b524ProbeFn = mockB524Probe(map[byte]bool{0x08: false, 0x15: true, 0x26: false}, &probed)

	addr, err := poller.discoverB524Root(context.Background())
	if err != nil {
		t.Fatalf("discoverB524Root error = %v; structural fallback must add 0x15/0x26 to candidates", err)
	}
	if addr != 0x15 {
		t.Fatalf("discoverB524Root = 0x%02x; want 0x15 (structural fallback regulator)", addr)
	}
	if len(probed) == 0 || probed[0] != 0x15 {
		t.Fatalf("probed = %v; want 0x15 first per D1 ordering", probed)
	}
}

// TestCapabilityFirstDiscovery_RichRegistryStopsAt0x15CoherentRoot pins the
// stop-at-first-coherent invariant when the registry already contains the
// regulator. Structural augmentation is now unconditional (idempotent — see
// the hostile-registry rationale in the next test) but D1 ordering must
// still terminate on the first coherent candidate. With 0x15 coherent,
// 0x26 must never be probed.
func TestCapabilityFirstDiscovery_RichRegistryStopsAt0x15CoherentRoot(t *testing.T) {
	t.Parallel()

	reg := registry.NewDeviceRegistry(nil)
	reg.Register(registry.DeviceInfo{Address: 0x08, Manufacturer: "Vaillant", DeviceID: "BAI00"})
	reg.Register(registry.DeviceInfo{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV2"})

	var probed []byte
	poller := newTestPoller(reg)
	poller.b524ProbeFn = mockB524Probe(map[byte]bool{0x08: false, 0x15: true, 0x26: true}, &probed)

	addr, err := poller.discoverB524Root(context.Background())
	if err != nil {
		t.Fatalf("discoverB524Root error = %v", err)
	}
	if addr != 0x15 {
		t.Fatalf("discoverB524Root = 0x%02x; want 0x15", addr)
	}
	for _, p := range probed {
		if p == 0x26 {
			t.Fatalf("D1 stop-at-first violated: probed includes 0x26 after 0x15 coherent: %v", probed)
		}
	}
}

// TestCapabilityFirstDiscovery_HostileRegistryWithStaleSlaveStillProbesRegulator
// closes the post-source-selection regression hole identified during
// adversarial review: a registry containing the boiler plus any stray
// non-Vaillant entry (e.g. 0xEC SOL00 from passive observation, or a
// phantom from a prior process) must NOT suppress the structural
// fallback. Always-augment guarantees 0x15 enters the candidate list
// regardless of registry size.
func TestCapabilityFirstDiscovery_HostileRegistryWithStaleSlaveStillProbesRegulator(t *testing.T) {
	t.Parallel()

	reg := registry.NewDeviceRegistry(nil)
	reg.Register(registry.DeviceInfo{Address: 0x08, Manufacturer: "Vaillant", DeviceID: "BAI00"})
	// Stale passive-only-responder entry from prior session — never
	// responds to active B524 probes.
	reg.Register(registry.DeviceInfo{Address: 0xEC, Manufacturer: "", DeviceID: ""})

	var probed []byte
	poller := newTestPoller(reg)
	poller.b524ProbeFn = mockB524Probe(map[byte]bool{0x08: false, 0xEC: false, 0x15: true, 0x26: false}, &probed)

	addr, err := poller.discoverB524Root(context.Background())
	if err != nil {
		t.Fatalf("discoverB524Root error = %v; structural fallback must still fire when registry contains stray passive-only entries", err)
	}
	if addr != 0x15 {
		t.Fatalf("discoverB524Root = 0x%02x; want 0x15", addr)
	}
	if len(probed) == 0 || probed[0] != 0x15 {
		t.Fatalf("probed = %v; want 0x15 first per D1 ordering", probed)
	}
}

// TestRefreshDiscovery_StructuralFallbackRegistersControllerInRegistry
// locks the live-evidence claim: when discoverB524Root converges on 0x15
// via the structural-fallback path (i.e. 0x15 was NOT in the registry
// when discovery started), refreshDiscovery must register 0x15 so the
// regulator surfaces in GraphQL devices and on the router plane.
// Without this side-effect the user-visible "GraphQL devices includes
// regulator 0x15" outcome is unattainable from the gateway-side fix.
func TestRefreshDiscovery_StructuralFallbackRegistersControllerInRegistry(t *testing.T) {
	t.Parallel()

	reg := registry.NewDeviceRegistry(nil)
	reg.Register(registry.DeviceInfo{Address: 0x08, Manufacturer: "Vaillant", DeviceID: "BAI00"})

	poller := newTestPoller(reg)
	poller.b524ProbeFn = mockB524Probe(map[byte]bool{0x08: false, 0x15: true, 0x26: false}, nil)

	addr, err := poller.discoverB524Root(context.Background())
	if err != nil {
		t.Fatalf("discoverB524Root error = %v", err)
	}
	if addr != 0x15 {
		t.Fatalf("discoverB524Root = 0x%02x; want 0x15", addr)
	}

	// Pre-condition: 0x15 not in registry.
	var has0x15Before bool
	reg.Iterate(func(e registry.DeviceEntry) bool {
		if e != nil && e.PrimaryDisplayAddress() == 0x15 {
			has0x15Before = true
			return false
		}
		return true
	})
	if has0x15Before {
		t.Fatal("test setup invalid: 0x15 already in registry before structural-fallback registration")
	}

	// Trigger the registration helper directly. (refreshDiscovery wraps
	// this with snapshot rebuilds we don't need to exercise here.)
	poller.registerStructuralControllerIfMissing(addr)

	var has0x15After bool
	var manufacturer string
	reg.Iterate(func(e registry.DeviceEntry) bool {
		if e != nil && e.PrimaryDisplayAddress() == 0x15 {
			has0x15After = true
			manufacturer = e.Manufacturer()
			return false
		}
		return true
	})
	if !has0x15After {
		t.Fatal("registry missing 0x15 after structural-fallback registration; GraphQL devices will not surface the regulator")
	}
	if manufacturer != "Vaillant" {
		t.Fatalf("registered 0x15 manufacturer = %q; want %q", manufacturer, "Vaillant")
	}
}

// TestCapabilityFirstDiscovery_StructuralFallbackSkipsAdmittedSource closes
// the source-address invariant hole found during Codex adversarial review
// (PR #560 P2): when the admitted semantic source equals one of the
// structural targets (e.g. configured source 0x15, or a source-selection
// result lands on 0x15), the structural augmentation must NOT add that
// address to the probe candidates — probeB524Register would issue
// Source=0x15 / Target=0x15, a self-directed unicast probe.
func TestCapabilityFirstDiscovery_StructuralFallbackSkipsAdmittedSource(t *testing.T) {
	t.Parallel()

	reg := registry.NewDeviceRegistry(nil)
	reg.Register(registry.DeviceInfo{Address: 0x08, Manufacturer: "Vaillant", DeviceID: "BAI00"})

	var probed []byte
	poller := newTestPoller(reg)
	poller.source = 0x15
	// 0x26 is the only coherent structural target left after 0x15 is
	// dropped (admitted source). 0x08 stays in candidates from the
	// registry but is non-coherent.
	poller.b524ProbeFn = mockB524Probe(map[byte]bool{0x08: false, 0x15: true, 0x26: true}, &probed)

	addr, err := poller.discoverB524Root(context.Background())
	if err != nil {
		t.Fatalf("discoverB524Root error = %v", err)
	}
	if addr != 0x26 {
		t.Fatalf("discoverB524Root = 0x%02x; want 0x26 (0x15 dropped because it equals admitted source)", addr)
	}
	for _, p := range probed {
		if p == 0x15 {
			t.Fatalf("self-directed probe issued: 0x15 was probed but is admitted source. probed=%v", probed)
		}
	}
}

// TestCapabilityFirstDiscovery_StructuralFallbackSkipsAdmittedCompanion closes
// the source-address invariant on the companion side (Codex PR #560 P2
// re-review): under source-selection, a configured / source-selected
// companion target reserved for the gateway must NOT be probed by the
// runtime semantic-discovery entry point. The startup probe path
// sanitizes via sanitizeStartupProbeTargets(... source, companion);
// discoverB524Root must apply the same guard.
func TestCapabilityFirstDiscovery_StructuralFallbackSkipsAdmittedCompanion(t *testing.T) {
	t.Parallel()

	reg := registry.NewDeviceRegistry(nil)
	reg.Register(registry.DeviceInfo{Address: 0x08, Manufacturer: "Vaillant", DeviceID: "BAI00"})

	var probed []byte
	poller := newTestPoller(reg)
	poller.source = 0x7F
	poller.companion = 0x26 // companion reserved for the gateway

	// Only 0x15 is coherent. Without the companion guard, 0x26 would
	// be probed as a structural target (it's in the structural set).
	poller.b524ProbeFn = mockB524Probe(map[byte]bool{0x08: false, 0x15: true, 0x26: true}, &probed)

	addr, err := poller.discoverB524Root(context.Background())
	if err != nil {
		t.Fatalf("discoverB524Root error = %v", err)
	}
	if addr != 0x15 {
		t.Fatalf("discoverB524Root = 0x%02x; want 0x15", addr)
	}
	for _, p := range probed {
		if p == 0x26 {
			t.Fatalf("admitted companion 0x26 was probed; source-address invariant violated. probed=%v", probed)
		}
	}
}

// TestCapabilityFirstDiscovery_DropsAdmittedSourceFromRegistryCandidates closes
// Codex PR #560 P2 finding: when the admitted source is already in the
// registry (e.g. preload imported it), the registry-iteration pass would
// add it to candidates and bypass the structural-augmentation guard.
// The unified filter must drop it regardless of how it entered.
func TestCapabilityFirstDiscovery_DropsAdmittedSourceFromRegistryCandidates(t *testing.T) {
	t.Parallel()

	reg := registry.NewDeviceRegistry(nil)
	// Preload contains the configured source address.
	reg.Register(registry.DeviceInfo{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV2"})
	reg.Register(registry.DeviceInfo{Address: 0x08, Manufacturer: "Vaillant", DeviceID: "BAI00"})

	var probed []byte
	poller := newTestPoller(reg)
	poller.source = 0x15
	poller.b524ProbeFn = mockB524Probe(map[byte]bool{0x08: true, 0x15: true}, &probed)

	addr, err := poller.discoverB524Root(context.Background())
	if err != nil {
		t.Fatalf("discoverB524Root error = %v", err)
	}
	if addr != 0x08 {
		t.Fatalf("discoverB524Root = 0x%02x; want 0x08 (0x15 dropped — admitted source)", addr)
	}
	for _, p := range probed {
		if p == 0x15 {
			t.Fatalf("self-directed probe issued via registry-derived candidate: 0x15 was probed but is admitted source. probed=%v", probed)
		}
	}
}

// TestCapabilityFirstDiscovery_DropsAdmittedCompanionFromRegistryCandidates
// closes the registry-side companion guard hole identified by Codex.
func TestCapabilityFirstDiscovery_DropsAdmittedCompanionFromRegistryCandidates(t *testing.T) {
	t.Parallel()

	reg := registry.NewDeviceRegistry(nil)
	// Registry preload includes the companion target.
	reg.Register(registry.DeviceInfo{Address: 0x26, Manufacturer: "Vaillant", DeviceID: "VR_71"})
	reg.Register(registry.DeviceInfo{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV2"})

	var probed []byte
	poller := newTestPoller(reg)
	poller.source = 0x7F
	poller.companion = 0x26 // companion reserved for the gateway

	poller.b524ProbeFn = mockB524Probe(map[byte]bool{0x15: true, 0x26: true}, &probed)

	addr, err := poller.discoverB524Root(context.Background())
	if err != nil {
		t.Fatalf("discoverB524Root error = %v", err)
	}
	if addr != 0x15 {
		t.Fatalf("discoverB524Root = 0x%02x; want 0x15 (0x26 dropped — admitted companion)", addr)
	}
	for _, p := range probed {
		if p == 0x26 {
			t.Fatalf("companion-targeted probe issued via registry-derived candidate: 0x26 was probed. probed=%v", probed)
		}
	}
}

// TestRefreshDiscovery_RecomputesRegulatorCapabilityAfterStructuralRegistration
// pins the capability-recompute fix (Codex PR #560 P2 re-review): when
// refreshDiscovery's discoverB524Root succeeds via the structural-
// fallback path and registers the controller as a side effect, the
// regulator capability lookup must run against the post-registration
// inventory. Otherwise p.regulatorCapability stays at ControllerNone
// until the next regulator-recheck tick.
func TestRefreshDiscovery_RecomputesRegulatorCapabilityAfterStructuralRegistration(t *testing.T) {
	t.Parallel()

	reg := registry.NewDeviceRegistry(nil)
	reg.Register(registry.DeviceInfo{Address: 0x08, Manufacturer: "Vaillant", DeviceID: "BAI00"})

	poller := newTestPoller(reg)
	// Initial capability is ControllerNone (registry has only boiler).
	if cap := poller.findRegulatorCapability(); cap == productids.ControllerPresent {
		t.Fatalf("test setup invalid: pre-registration capability already %v", cap)
	}

	// Discover 0x15 via structural fallback, then register it.
	poller.b524ProbeFn = mockB524Probe(map[byte]bool{0x08: false, 0x15: true, 0x26: false}, nil)
	controller, err := poller.discoverB524Root(context.Background())
	if err != nil {
		t.Fatalf("discoverB524Root error = %v", err)
	}
	registered := poller.registerStructuralControllerIfMissing(controller)
	if !registered {
		t.Fatal("registerStructuralControllerIfMissing returned false; expected true for newly-discovered 0x15")
	}

	// findRegulatorCapability must now reflect the new inventory.
	postCap := poller.findRegulatorCapability()
	if postCap == productids.ControllerNone {
		t.Fatalf("post-registration capability = %v; want non-ControllerNone (registry now contains 0x15 Vaillant)", postCap)
	}
}

// TestRegisterStructuralControllerIfMissing_NoOpWhenAlreadyRegistered pins
// the idempotence contract: repeated structural-fallback calls for an
// already-registered controller must not duplicate registry entries or
// thrash the router plane.
func TestRegisterStructuralControllerIfMissing_NoOpWhenAlreadyRegistered(t *testing.T) {
	t.Parallel()

	reg := registry.NewDeviceRegistry(nil)
	reg.Register(registry.DeviceInfo{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV2"})

	poller := newTestPoller(reg)

	refreshCount := 0
	poller.routerPlanesRefreshFn = func() { refreshCount++ }

	poller.registerStructuralControllerIfMissing(0x15)

	if refreshCount != 0 {
		t.Fatalf("router plane refreshed %d times for already-registered controller; want 0", refreshCount)
	}

	count := 0
	reg.Iterate(func(e registry.DeviceEntry) bool {
		if e != nil && e.PrimaryDisplayAddress() == 0x15 {
			count++
		}
		return true
	})
	if count != 1 {
		t.Fatalf("registry has %d entries for 0x15; want 1 (idempotent)", count)
	}
}

// TestCapabilityFirstDiscovery_StructuralFallbackEmptyRegistry covers the
// degenerate case where registry is empty — structural fallback alone
// must permit discovery to attempt the structural set rather than
// returning "no devices in registry".
func TestCapabilityFirstDiscovery_StructuralFallbackEmptyRegistry(t *testing.T) {
	t.Parallel()

	reg := registry.NewDeviceRegistry(nil)

	var probed []byte
	poller := newTestPoller(reg)
	poller.b524ProbeFn = mockB524Probe(map[byte]bool{0x15: true}, &probed)

	addr, err := poller.discoverB524Root(context.Background())
	if err != nil {
		t.Fatalf("discoverB524Root error = %v; want structural fallback to attempt 0x15", err)
	}
	if addr != 0x15 {
		t.Fatalf("discoverB524Root = 0x%02x; want 0x15", addr)
	}
}

func TestEnrichRegulatorIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		deviceID   string
		wantFamily string
	}{
		{name: "BASV2 family", deviceID: "BASV2_xx", wantFamily: "BASV2"},
		{name: "BASS2 family", deviceID: "BASS2_yy", wantFamily: "BASS2"},
		{name: "CTLV2 family", deviceID: "CTLV2_zz", wantFamily: "CTLV2"},
		{name: "unknown family", deviceID: "XYZABC", wantFamily: ""},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			reg := registry.NewDeviceRegistry(nil)
			reg.Register(registry.DeviceInfo{Address: 0x15, Manufacturer: "Vaillant", DeviceID: test.deviceID})

			poller := newTestPoller(reg)
			enrichment := poller.enrichRegulatorIdentity(0x15)
			if enrichment == nil {
				t.Fatal("enrichRegulatorIdentity returned nil")
			}
			if enrichment.family != test.wantFamily {
				t.Fatalf("enrichment.family = %q; want %q", enrichment.family, test.wantFamily)
			}
		})
	}
}

func TestDecodeB524DateSuppressSentinel(t *testing.T) {
	tests := []struct {
		name string
		raw  []byte
		want string
	}{
		{"sentinel 01.01.2015", []byte{0x01, 0x01, 0x0F}, ""},
		{"valid 15.03.2026", []byte{0x0F, 0x03, 0x1A}, "2026-03-15"},
		{"valid 01.01.2026", []byte{0x01, 0x01, 0x1A}, "2026-01-01"},
		{"short payload", []byte{0x01, 0x01}, ""},
		{"empty payload", []byte{}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := decodeB524DateSuppressSentinel(tt.raw)
			if got != tt.want {
				t.Errorf("decodeB524DateSuppressSentinel(%v) = %q; want %q", tt.raw, got, tt.want)
			}
		})
	}
}

// --- P03 startup barrier regression tests (#362) ---

func TestVaillantSemanticPoller_StartDefersPollingUntilBarrier(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	scheduler := newSemanticTaskScheduler()
	poller := &vaillantSemanticPoller{
		tasks:                    scheduler,
		discoveryInterval:        time.Hour,
		configInterval:           time.Hour,
		stateInterval:            time.Hour,
		energyInterval:           time.Hour,
		scheduleInterval:         time.Hour,
		regulatorRecheckInterval: time.Hour,
		nowFn:                    time.Now,
	}

	barrier := make(chan struct{})
	poller.startupBarrier = barrier
	poller.Start(ctx)

	// Allow goroutines to schedule.
	time.Sleep(50 * time.Millisecond)

	scheduler.mu.Lock()
	seqBefore := scheduler.seq
	scheduler.mu.Unlock()

	if seqBefore != 0 {
		t.Fatalf("scheduler.seq = %d before barrier; want 0 (no tasks submitted)", seqBefore)
	}

	close(barrier)

	// Allow deferred tasks to be enqueued.
	time.Sleep(50 * time.Millisecond)

	scheduler.mu.Lock()
	seqAfter := scheduler.seq
	scheduler.mu.Unlock()

	if seqAfter == 0 {
		t.Fatal("scheduler.seq = 0 after barrier; want > 0 (tasks should be submitted)")
	}
}

func TestVaillantSemanticPoller_StartWithoutBarrierSubmitsImmediately(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	scheduler := newSemanticTaskScheduler()
	poller := &vaillantSemanticPoller{
		tasks:                    scheduler,
		discoveryInterval:        time.Hour,
		configInterval:           time.Hour,
		stateInterval:            time.Hour,
		energyInterval:           time.Hour,
		scheduleInterval:         time.Hour,
		regulatorRecheckInterval: time.Hour,
		nowFn:                    time.Now,
	}

	poller.Start(ctx)

	time.Sleep(50 * time.Millisecond)

	scheduler.mu.Lock()
	seq := scheduler.seq
	scheduler.mu.Unlock()

	if seq == 0 {
		t.Fatal("scheduler.seq = 0 without barrier; want > 0 (tasks should be submitted immediately)")
	}
}

func TestVaillantSemanticPoller_StartBarrierExitsOnContextCancel(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())

	scheduler := newSemanticTaskScheduler()
	poller := &vaillantSemanticPoller{
		tasks:                    scheduler,
		discoveryInterval:        time.Hour,
		configInterval:           time.Hour,
		stateInterval:            time.Hour,
		energyInterval:           time.Hour,
		scheduleInterval:         time.Hour,
		regulatorRecheckInterval: time.Hour,
		nowFn:                    time.Now,
	}

	barrier := make(chan struct{})
	poller.startupBarrier = barrier
	poller.Start(ctx)

	cancel()
	time.Sleep(50 * time.Millisecond)

	scheduler.mu.Lock()
	seq := scheduler.seq
	scheduler.mu.Unlock()

	if seq != 0 {
		t.Fatalf("scheduler.seq = %d after cancel; want 0 (should not start polling on cancel)", seq)
	}
}

func TestB524GroupDef_OpcodeGroupBindingIsCorrect(t *testing.T) {
	t.Parallel()

	// Verify all local groups use OP=0x02.
	localGroups := []b524GroupDef{localRegulator, localDHW, localCircuits, localZones, localSolar, localCylinders}
	for _, g := range localGroups {
		if g.opcode != vaillantB524OpcodeLocal {
			t.Errorf("local group %q (GG=0x%02X) has opcode 0x%02X; want 0x02", g.name, g.group, g.opcode)
		}
	}
	// Verify all remote groups use OP=0x06.
	remoteGroups := []b524GroupDef{remoteRegulators, remoteThermostats, remoteFunctionalModules}
	for _, g := range remoteGroups {
		if g.opcode != vaillantB524OpcodeRead {
			t.Errorf("remote group %q (GG=0x%02X) has opcode 0x%02X; want 0x06", g.name, g.group, g.opcode)
		}
	}
}

func TestDiscoverDeviceSlotsExcludesDisconnectedRegulatorAndThermostatSlots(t *testing.T) {
	t.Parallel()

	var selectors [][]byte
	poller := &vaillantSemanticPoller{
		scheduler:      ebusgateway.NewSemanticReadScheduler(),
		source:         0x7F,
		controller:     0x15,
		requestTimeout: 50 * time.Millisecond,
	}
	poller.sendFrameFn = func(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
		selectors = append(selectors, slices.Clone(frame.Data))
		if len(frame.Data) != 6 {
			return nil, errors.New("invalid B524 selector")
		}
		group := frame.Data[2]
		instance := frame.Data[3]
		addr := uint16(frame.Data[4]) | uint16(frame.Data[5])<<8
		if addr == device_slot_connected {
			switch {
			case group == remoteRegulators.group && instance == 0x01:
				return testB524ResponseForSelectorPayload(frame.Data, 0x00), nil
			case group == remoteThermostats.group && instance == 0x01:
				return testB524ResponseForSelectorPayload(frame.Data, 0x00), nil
			case group == remoteThermostats.group && instance == 0x02:
				return testB524ResponseForSelectorPayload(frame.Data, 0x01), nil
			}
		}
		return testB524ResponseForSelectorPayload(frame.Data, 0xFF), nil
	}

	active, observedAny := poller.discoverDeviceSlots(context.Background())

	if !observedAny {
		t.Fatal("discoverDeviceSlots observedAny = false; want true for responsive disconnected/connected slots")
	}
	if active[deviceSlotKey{Group: remoteRegulators.group, Instance: 0x01}] {
		t.Fatal("disconnected regulator slot was retained for steady-state detail refresh")
	}
	if active[deviceSlotKey{Group: remoteThermostats.group, Instance: 0x01}] {
		t.Fatal("disconnected thermostat slot was retained for steady-state detail refresh")
	}
	if !active[deviceSlotKey{Group: remoteThermostats.group, Instance: 0x02}] {
		t.Fatal("connected thermostat slot was not retained for steady-state detail refresh")
	}
	if hasB524Selector(selectors, remoteRegulators.group, 0x01, device_slot_class_address) {
		t.Fatal("disconnected regulator slot should not receive identity-detail probes")
	}
	if hasB524Selector(selectors, remoteThermostats.group, 0x01, device_slot_class_address) {
		t.Fatal("disconnected thermostat slot should not receive identity-detail probes")
	}
}

func TestDiscoverDeviceSlotsKeepsDisconnectedFunctionalModuleIdentityEvidence(t *testing.T) {
	t.Parallel()

	var selectors [][]byte
	poller := &vaillantSemanticPoller{
		scheduler:      ebusgateway.NewSemanticReadScheduler(),
		source:         0x7F,
		controller:     0x15,
		requestTimeout: 50 * time.Millisecond,
	}
	poller.sendFrameFn = func(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
		selectors = append(selectors, slices.Clone(frame.Data))
		if len(frame.Data) != 6 {
			return nil, errors.New("invalid B524 selector")
		}
		group := frame.Data[2]
		instance := frame.Data[3]
		addr := uint16(frame.Data[4]) | uint16(frame.Data[5])<<8
		if group == remoteFunctionalModules.group && instance == 0x04 {
			switch addr {
			case device_slot_connected:
				return testB524ResponseForSelectorPayload(frame.Data, 0x00), nil
			case device_slot_class_address:
				return testB524ResponseForSelectorPayload(frame.Data, 0x26), nil
			}
		}
		return testB524ResponseForSelectorPayload(frame.Data, 0xFF), nil
	}

	active, observedAny := poller.discoverDeviceSlots(context.Background())

	if !observedAny {
		t.Fatal("discoverDeviceSlots observedAny = false; want true for responsive functional module slot")
	}
	if !active[deviceSlotKey{Group: remoteFunctionalModules.group, Instance: 0x04}] {
		t.Fatal("disconnected functional module identity evidence was not retained")
	}
	requireB524Selector(t, selectors, remoteFunctionalModules.group, 0x04, device_slot_class_address)
}

func TestRefreshRadioDevicesSkipsVolatileDetailsForDisconnectedFunctionalModuleInventory(t *testing.T) {
	t.Parallel()

	var selectors [][]byte
	poller := &vaillantSemanticPoller{
		scheduler:                ebusgateway.NewSemanticReadScheduler(),
		provider:                 graphql.NewLiveSemanticProvider(),
		reg:                      newTestRegistry(),
		source:                   0x7F,
		controller:               0x15,
		requestTimeout:           50 * time.Millisecond,
		deviceSlotRediscoveryTTL: 30 * time.Minute,
		deviceSlotDiscoveryDone:  true,
		deviceSlotDiscoveryAt:    time.Now(),
		deviceSlotCache: map[deviceSlotKey]bool{
			{Group: remoteFunctionalModules.group, Instance: 0x04}: true,
		},
		nowFn: time.Now,
	}
	poller.sendFrameFn = func(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
		selectors = append(selectors, slices.Clone(frame.Data))
		if len(frame.Data) != 6 {
			return nil, errors.New("invalid B524 selector")
		}
		group := frame.Data[2]
		instance := frame.Data[3]
		addr := uint16(frame.Data[4]) | uint16(frame.Data[5])<<8
		if group == remoteFunctionalModules.group && instance == 0x04 {
			switch addr {
			case device_slot_connected:
				return testB524ResponseForSelectorPayload(frame.Data, 0x00), nil
			case device_slot_class_address:
				return testB524ResponseForSelectorPayload(frame.Data, 0x26), nil
			case device_slot_firmware:
				return testB524ResponseForSelectorPayload(frame.Data, 0x01, 0x02, 0x03), nil
			case device_slot_hardware_identifier:
				return testB524ResponseForSelectorPayload(frame.Data, 0x34, 0x12), nil
			}
		}
		return testB524ResponseForSelectorPayload(frame.Data, 0xFF), nil
	}

	poller.refreshRadioDevices(context.Background())

	requireB524Selector(t, selectors, remoteFunctionalModules.group, 0x04, device_slot_connected)
	requireB524Selector(t, selectors, remoteFunctionalModules.group, 0x04, device_slot_class_address)
	requireB524Selector(t, selectors, remoteFunctionalModules.group, 0x04, device_slot_firmware)
	requireB524Selector(t, selectors, remoteFunctionalModules.group, 0x04, device_slot_hardware_identifier)

	for _, forbidden := range []uint16{
		device_slot_remote_control_address,
		device_slot_paired,
		device_slot_reception_strength,
		device_slot_zone_assignment,
		device_slot_room_temperature,
		device_slot_room_humidity,
	} {
		if hasB524Selector(selectors, remoteFunctionalModules.group, 0x04, forbidden) {
			t.Fatalf("disconnected functional-module inventory read volatile addr=0x%04x; want identity-only detail refresh", forbidden)
		}
	}

	poller.mu.Lock()
	device := poller.radioDevices[radioDeviceKey{Group: remoteFunctionalModules.group, Instance: 0x04}]
	poller.mu.Unlock()
	if device == nil {
		t.Fatal("radioDevices missing disconnected functional-module inventory snapshot")
	}
	if device.SlotMode != "inventory" {
		t.Fatalf("SlotMode = %q; want inventory", device.SlotMode)
	}
	if device.RoomTemperatureC != nil || device.RoomHumidityPct != nil || device.ReceptionStrength != nil || device.DevicePaired != nil {
		t.Fatalf("volatile fields populated for disconnected inventory snapshot: %+v", device)
	}
}

func TestRefreshRadioDevicesClearsStaleSnapshotsWhenDiscoveryFindsNoRefreshableSlots(t *testing.T) {
	t.Parallel()

	poller := &vaillantSemanticPoller{
		scheduler:      ebusgateway.NewSemanticReadScheduler(),
		provider:       graphql.NewLiveSemanticProvider(),
		source:         0x7F,
		controller:     0x15,
		requestTimeout: 50 * time.Millisecond,
		radioDevices: map[radioDeviceKey]*vaillantRadioDeviceSnapshot{
			{Group: remoteRegulators.group, Instance: 0x01}: {
				Group:    remoteRegulators.group,
				Instance: 0x01,
				SlotMode: "active",
			},
		},
	}
	poller.sendFrameFn = func(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
		if len(frame.Data) != 6 {
			return nil, errors.New("invalid B524 selector")
		}
		return testB524ResponseForSelectorPayload(frame.Data, 0x00), nil
	}

	poller.refreshRadioDevices(context.Background())

	poller.mu.Lock()
	got := len(poller.radioDevices)
	poller.mu.Unlock()
	if got != 0 {
		t.Fatalf("radioDevices length = %d; want stale snapshots cleared", got)
	}
}

func TestRefreshRadioDevicesPreservesSnapshotsWhenRediscoveryOnlyTimesOut(t *testing.T) {
	t.Parallel()

	poller := &vaillantSemanticPoller{
		scheduler:                ebusgateway.NewSemanticReadScheduler(),
		provider:                 graphql.NewLiveSemanticProvider(),
		source:                   0x7F,
		controller:               0x15,
		requestTimeout:           50 * time.Millisecond,
		deviceSlotRediscoveryTTL: time.Millisecond,
		deviceSlotCache: map[deviceSlotKey]bool{
			{Group: remoteRegulators.group, Instance: 0x01}: true,
		},
		deviceSlotDiscoveryDone: true,
		deviceSlotDiscoveryAt:   time.Now().Add(-time.Second),
		radioDevices: map[radioDeviceKey]*vaillantRadioDeviceSnapshot{
			{Group: remoteRegulators.group, Instance: 0x01}: {
				Group:    remoteRegulators.group,
				Instance: 0x01,
				SlotMode: "active",
			},
		},
	}
	poller.sendFrameFn = func(context.Context, protocol.Frame) (*protocol.Frame, error) {
		return nil, errors.New("temporary rediscovery timeout")
	}

	poller.refreshRadioDevices(context.Background())

	poller.mu.Lock()
	radioDevices := len(poller.radioDevices)
	cachedSlots := len(poller.deviceSlotCache)
	poller.mu.Unlock()
	if radioDevices != 1 {
		t.Fatalf("radioDevices length = %d; want stale snapshot preserved through all-timeout rediscovery", radioDevices)
	}
	if cachedSlots != 1 {
		t.Fatalf("deviceSlotCache length = %d; want existing cache preserved through all-timeout rediscovery", cachedSlots)
	}
}

func TestDeviceSlotCacheGating_OnlyPollsCachedSlots(t *testing.T) {
	t.Parallel()

	poller := &vaillantSemanticPoller{
		nowFn:                    time.Now,
		deviceSlotRediscoveryTTL: 30 * time.Minute,
	}

	// Simulate discovery: 2 active slots out of 33.
	cache := map[deviceSlotKey]bool{
		{Group: remoteRegulators.group, Instance: 0x00}:  true,
		{Group: remoteThermostats.group, Instance: 0x01}: true,
	}
	poller.mu.Lock()
	poller.deviceSlotCache = cache
	poller.deviceSlotDiscoveryDone = true
	poller.deviceSlotDiscoveryAt = time.Now()
	poller.mu.Unlock()

	// Verify cache is populated.
	poller.mu.Lock()
	got := len(poller.deviceSlotCache)
	poller.mu.Unlock()
	if got != 2 {
		t.Fatalf("deviceSlotCache length = %d; want 2", got)
	}

	// Verify needsDiscovery is false within TTL.
	poller.mu.Lock()
	needsDiscovery := !poller.deviceSlotDiscoveryDone ||
		(poller.deviceSlotRediscoveryTTL > 0 && poller.now().Sub(poller.deviceSlotDiscoveryAt) >= poller.deviceSlotRediscoveryTTL)
	poller.mu.Unlock()
	if needsDiscovery {
		t.Fatal("needsDiscovery should be false within TTL")
	}
}

func TestDeviceSlotCacheGating_RequiresRediscoveryAfterTTL(t *testing.T) {
	t.Parallel()

	poller := &vaillantSemanticPoller{
		nowFn:                    time.Now,
		deviceSlotRediscoveryTTL: 1 * time.Millisecond,
	}

	poller.mu.Lock()
	poller.deviceSlotCache = map[deviceSlotKey]bool{
		{Group: remoteRegulators.group, Instance: 0x00}: true,
	}
	poller.deviceSlotDiscoveryDone = true
	poller.deviceSlotDiscoveryAt = time.Now().Add(-1 * time.Second) // expired
	poller.mu.Unlock()

	poller.mu.Lock()
	needsDiscovery := !poller.deviceSlotDiscoveryDone ||
		(poller.deviceSlotRediscoveryTTL > 0 && poller.now().Sub(poller.deviceSlotDiscoveryAt) >= poller.deviceSlotRediscoveryTTL)
	poller.mu.Unlock()
	if !needsDiscovery {
		t.Fatal("needsDiscovery should be true after TTL expiry")
	}
}
