package main

import (
	"slices"
	"testing"
)

func TestBuildB524ReadSelector(t *testing.T) {
	t.Parallel()

	got := buildB524ReadSelector(0x02, 0x03, 0x01, 0x001C)
	want := []byte{0x06, 0x02, 0x00, 0x03, 0x01, 0x1C, 0x00}
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
