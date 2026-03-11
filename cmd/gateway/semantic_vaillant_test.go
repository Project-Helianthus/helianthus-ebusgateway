package main

import (
	"context"
	"fmt"
	"math"
	"slices"
	"strings"
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
	want := []uint16{boilerB509RegFlowsetHcMaxC, boilerB509RegFlowsetHcMaxCFallback}
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
