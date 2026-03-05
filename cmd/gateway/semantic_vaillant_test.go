package main

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
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

func TestBoilerStatusRegisterDefinitionsForTier_NoReturnTemperatureMapping(t *testing.T) {
	t.Parallel()

	for _, tier := range []boilerStatusTier{
		boilerStatusTierFast,
		boilerStatusTierMedium,
		boilerStatusTierSlow,
	} {
		for _, register := range boilerStatusRegisterDefinitionsForTier(tier) {
			if register.group == vaillantGroupCircuits && register.addr == uint16(0x0008) {
				t.Fatalf("tier %v maps GG=0x02 RR=0x0008; closed decision forbids using this as boiler return temperature", tier)
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

func TestDeriveVR71CircuitStartIndex(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		scheme  *uint16
		module  *uint16
		wantNil bool
		want    int
	}{
		{
			name:    "nil inputs",
			scheme:  nil,
			module:  nil,
			wantNil: true,
		},
		{
			name:    "interpreted fm5 profile",
			scheme:  uint16Ptr(8),
			module:  uint16Ptr(2),
			wantNil: false,
			want:    1,
		},
		{
			name:    "invalid scheme",
			scheme:  uint16Ptr(0),
			module:  uint16Ptr(4),
			wantNil: false,
			want:    -1,
		},
		{
			name:    "non interpreted fm5 profile",
			scheme:  uint16Ptr(8),
			module:  uint16Ptr(4),
			wantNil: false,
			want:    -1,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got := deriveVR71CircuitStartIndex(test.scheme, test.module)
			if test.wantNil {
				if got != nil {
					t.Fatalf("deriveVR71CircuitStartIndex(...) = %v; want nil", got)
				}
				return
			}
			if got == nil || *got != test.want {
				t.Fatalf("deriveVR71CircuitStartIndex(...) = %v; want %d", got, test.want)
			}
		})
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

func TestPublishSystem_DerivesVR71CircuitStartIndex(t *testing.T) {
	t.Parallel()

	scheme := uint16(8)
	module := uint16(2)
	provider := graphql.NewLiveSemanticProvider()
	poller := &vaillantSemanticPoller{
		provider: provider,
		system: &vaillantSystemSnapshot{
			SystemScheme:            &scheme,
			ModuleConfigurationVR71: &module,
		},
	}

	poller.publishSystem(semanticSnapshotSourceLive)

	status := provider.System()
	if status == nil {
		t.Fatal("provider.System() = nil; want published system status")
	}
	if status.Properties.Vr71CircuitStartIndex == nil || *status.Properties.Vr71CircuitStartIndex != 1 {
		t.Fatalf("provider.System().Properties.Vr71CircuitStartIndex = %v; want 1", status.Properties.Vr71CircuitStartIndex)
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

func TestRefreshEnergy_NilController(t *testing.T) {
	t.Parallel()

	provider := graphql.NewLiveSemanticProvider()
	poller := &vaillantSemanticPoller{
		provider:            provider,
		regulatorCapability: productids.ControllerPresent,
	}

	poller.refreshEnergy(context.Background())

	if totals := provider.EnergyTotals(); totals != nil {
		t.Fatalf("EnergyTotals() = %#v; want nil for nil controller", totals)
	}
}

func TestRefreshEnergy_NoRegulator(t *testing.T) {
	t.Parallel()

	provider := graphql.NewLiveSemanticProvider()
	poller := &vaillantSemanticPoller{
		provider:            provider,
		controller:          0x15,
		regulatorCapability: productids.ControllerNone,
	}

	poller.refreshEnergy(context.Background())

	if totals := provider.EnergyTotals(); totals != nil {
		t.Fatalf("EnergyTotals() = %#v; want nil without regulator", totals)
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
		ConfigurationAssociatedCircuitRaw: &circuitIndex,
		ConfigurationCircuitTypeRaw:       &circuitType,
		StateValveStatusRaw:               &valve,
	}
	seedZoneFreshness(entry, semanticSnapshotSourceCache, true)

	updatedCurrent := 21.8
	incoming := &vaillantZoneSnapshot{
		CurrentTempC:                      &updatedCurrent,
		ConfigurationHeatingOperationMode: "2",
	}
	mergeZoneSnapshotFields(entry, incoming, semanticSnapshotSourceLive, zoneStateFieldSet)

	if entry.CurrentTempC == nil || *entry.CurrentTempC != 21.8 {
		t.Fatalf("entry.CurrentTempC = %v; want 21.8", entry.CurrentTempC)
	}
	if entry.TargetTempC == nil || *entry.TargetTempC != 22.5 {
		t.Fatalf("entry.TargetTempC = %v; want preserved 22.5", entry.TargetTempC)
	}
	if entry.ConfigurationAssociatedCircuitRaw == nil || *entry.ConfigurationAssociatedCircuitRaw != 2 {
		t.Fatalf("entry.ConfigurationAssociatedCircuitRaw = %v; want preserved 2", entry.ConfigurationAssociatedCircuitRaw)
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

	localKey := semanticReadBreakerKey(0x15, vaillantB524OpcodeLocal, vaillantGroupDHW, dhwInstance, dhwRegCurrentTemp)
	readKey := semanticReadBreakerKey(0x15, vaillantB524OpcodeRead, vaillantGroupDHW, dhwInstance, dhwRegCurrentTemp)
	if localKey == readKey {
		t.Fatalf("semanticReadBreakerKey must include opcode; got equal keys %q", localKey)
	}

	otherTarget := semanticReadBreakerKey(0x16, vaillantB524OpcodeRead, vaillantGroupDHW, dhwInstance, dhwRegCurrentTemp)
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

	// Registry with a boiler (BASV prefix) and a regulator device.
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
	}
	poller.nowFn = func() time.Time { return time.Date(2026, time.February, 26, 12, 0, 0, 0, time.UTC) }

	poller.refreshDiscovery(context.Background())

	poller.mu.Lock()
	gotController := poller.controller
	gotCap := poller.regulatorCapability
	poller.mu.Unlock()

	if gotController != 0x15 {
		t.Fatalf("controller = 0x%02x; want 0x15 (BASV boiler)", gotController)
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

	// Registry with ONLY a regulator — no BASV boiler. refreshDiscovery should
	// still update regulatorCapability even when early-returning due to missing BASV.
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
		t.Fatalf("controller = 0x%02x; want 0 (no BASV found)", gotController)
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

	type pair struct {
		channel string
		usage   string
	}
	expected := map[pair]bool{
		{"gas", "climate"}:           false,
		{"gas", "hot_water"}:         false,
		{"electricity", "climate"}:   false,
		{"electricity", "hot_water"}: false,
	}
	for _, q := range b524EnergyQueries {
		p := pair{q.channel, q.usage}
		if _, ok := expected[p]; !ok {
			t.Fatalf("unexpected query in b524EnergyQueries: channel=%q usage=%q", q.channel, q.usage)
		}
		expected[p] = true
	}
	for p, seen := range expected {
		if !seen {
			t.Fatalf("missing query in b524EnergyQueries: channel=%q usage=%q", p.channel, p.usage)
		}
	}
	if len(b524EnergyQueries) != 4 {
		t.Fatalf("len(b524EnergyQueries) = %d; want 4", len(b524EnergyQueries))
	}
}
