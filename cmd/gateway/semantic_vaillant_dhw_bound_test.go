package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

// dhwBoundFakeBus is a fake B524 controller: it answers OP00 identifiers 6 and
// 7, acknowledges OP02 writes and answers OP02 reads with the last written
// value. It records every frame so tests can prove nothing was written.
type dhwBoundFakeBus struct {
	mu        sync.Mutex
	apiAnswer map[uint16][]byte // OP00 id -> reply payload; missing -> no answer
	store     map[uint16][]byte // OP02 addr -> value
	writes    [][]byte
	op00Reads []uint16
}

func (bus *dhwBoundFakeBus) send(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
	bus.mu.Lock()
	defer bus.mu.Unlock()
	data := frame.Data
	if len(data) == 3 && data[0] == 0x00 {
		id := uint16(data[1]) | uint16(data[2])<<8
		bus.op00Reads = append(bus.op00Reads, id)
		if reply, ok := bus.apiAnswer[id]; ok {
			return &protocol.Frame{Data: reply}, nil
		}
		return nil, errors.New("no OP00 answer")
	}
	if len(data) < 6 || data[0] != 0x02 {
		return nil, errors.New("unexpected frame")
	}
	addr := uint16(data[4]) | uint16(data[5])<<8
	switch data[1] {
	case vaillantB524OpWrite:
		bus.writes = append(bus.writes, append([]byte(nil), data...))
		bus.store[addr] = append([]byte(nil), data[6:]...)
		return &protocol.Frame{Data: []byte{0x00}}, nil
	case vaillantB524OpRead:
		value, ok := bus.store[addr]
		if !ok {
			return nil, errors.New("unwritten register")
		}
		return testB524ResponseForSelectorPayload(data, value...), nil
	}
	return nil, errors.New("unexpected B524 operation")
}

func (bus *dhwBoundFakeBus) writeCount() int {
	bus.mu.Lock()
	defer bus.mu.Unlock()
	return len(bus.writes)
}

func f32(value float64) []byte {
	out := make([]byte, 4)
	binary.LittleEndian.PutUint32(out, math.Float32bits(float32(value)))
	return out
}

func newDHWBoundPoller(t *testing.T, deviceID, software, hardware string, bus *dhwBoundFakeBus) (*vaillantSemanticPoller, *graphql.LiveSemanticProvider) {
	t.Helper()
	reg := registry.NewDeviceRegistry(nil)
	reg.Register(registry.DeviceInfo{Address: 0x15, Manufacturer: "Vaillant", DeviceID: deviceID, SoftwareVersion: software, HardwareVersion: hardware})
	provider := graphql.NewLiveSemanticProvider()
	poller := &vaillantSemanticPoller{
		scheduler:      ebusgateway.NewSemanticReadScheduler(),
		reg:            reg,
		provider:       provider,
		source:         0x71,
		controller:     0x15,
		requestTimeout: 50 * time.Millisecond,
		system:         &vaillantSystemSnapshot{Controller: 0x15},
		sendFrameFn:    bus.send,
	}
	return poller, provider
}

func apiAnswers(version, revision float64) map[uint16][]byte {
	return map[uint16][]byte{0x0006: f32(version), 0x0007: f32(revision)}
}

func TestSetSystemConfigDHWBivalenceBASV0ProfileBound(t *testing.T) {
	bus := &dhwBoundFakeBus{apiAnswer: apiAnswers(1, 1), store: map[uint16][]byte{}}
	poller, provider := newDHWBoundPoller(t, "BASV0", "0217", "8803", bus)

	for _, value := range []string{"21", "50", "-21", "20.5"} {
		result := poller.SetSystemConfig(context.Background(), "dhwBivalencePointC", value)
		if result.Success {
			t.Fatalf("BASV0 value %s accepted; want rejection", value)
		}
		if !strings.Contains(result.Error, "controller profile BASV0/SW0217/HW8803/API1.1 allows [-20, 20]") {
			t.Fatalf("BASV0 value %s error = %q; want the profile bound named", value, result.Error)
		}
	}
	if got := bus.writeCount(); got != 0 {
		t.Fatalf("bus writes after rejected BASV0 values = %d; want 0 (reject before any bus write)", got)
	}

	for _, value := range []string{"20", "-20"} {
		if result := poller.SetSystemConfig(context.Background(), "dhwBivalencePointC", value); !result.Success {
			t.Fatalf("BASV0 value %s rejected: %q", value, result.Error)
		}
	}
	if got := bus.writeCount(); got != 2 {
		t.Fatalf("bus writes for in-bound BASV0 values = %d; want 2", got)
	}

	bound := provider.System().Config.DhwBivalenceBound
	want := graphql.ConfigBound{MinC: -20, MaxC: 20, Source: graphql.ConfigBoundSourceControllerProfile, Profile: "BASV0/SW0217/HW8803/API1.1"}
	if bound == nil || *bound != want {
		t.Fatalf("published bound = %+v; want %+v", bound, want)
	}
}

func TestSetSystemConfigDHWBivalenceBASV2FallsToUnmatchedPolicy(t *testing.T) {
	// BASV2 has no seeded profile in ebusreg, so its 50 maximum is not kept: the
	// conservative unmatched policy applies and says so.
	bus := &dhwBoundFakeBus{apiAnswer: apiAnswers(1, 1), store: map[uint16][]byte{}}
	poller, provider := newDHWBoundPoller(t, "BASV2", "0507", "1704", bus)

	for _, value := range []string{"21", "50"} {
		result := poller.SetSystemConfig(context.Background(), "dhwBivalencePointC", value)
		if result.Success {
			t.Fatalf("BASV2 value %s accepted; want rejection under the unmatched policy", value)
		}
		if !strings.Contains(result.Error, "controller identity unmatched (unknown_profile): conservative policy bound [-20, 20] applies") {
			t.Fatalf("BASV2 value %s error = %q; want the unmatched policy named", value, result.Error)
		}
	}
	if got := bus.writeCount(); got != 0 {
		t.Fatalf("bus writes after rejected BASV2 values = %d; want 0", got)
	}
	if result := poller.SetSystemConfig(context.Background(), "dhwBivalencePointC", "20"); !result.Success {
		t.Fatalf("BASV2 value 20 rejected: %q", result.Error)
	}

	bound := provider.System().Config.DhwBivalenceBound
	if bound == nil || bound.Source != graphql.ConfigBoundSourceUnmatchedPolicy || bound.Reason != graphql.ConfigBoundReasonUnknownProfile || bound.MinC != -20 || bound.MaxC != 20 || bound.Profile != "" {
		t.Fatalf("published bound = %+v; want unmatched policy -20..20", bound)
	}
}

func TestSetSystemConfigDHWBivalenceAPIQualifierFailsClosed(t *testing.T) {
	cases := map[string]map[uint16][]byte{
		"no OP00 answer":           {},
		"only the version answers": {0x0006: f32(1)},
		"non-integral revision":    {0x0006: f32(1), 0x0007: f32(1.5)},
		"NaN version":              {0x0006: f32(math.NaN()), 0x0007: f32(1)},
		"negative version":         {0x0006: f32(-1), 0x0007: f32(1)},
		"wrong reply width":        {0x0006: {0x01, 0x00}, 0x0007: f32(1)},
		"other API revision":       apiAnswers(1, 2),
	}
	for name, answers := range cases {
		bus := &dhwBoundFakeBus{apiAnswer: answers, store: map[uint16][]byte{}}
		poller, provider := newDHWBoundPoller(t, "BASV0", "0217", "8803", bus)
		result := poller.SetSystemConfig(context.Background(), "dhwBivalencePointC", "21")
		if result.Success || !strings.Contains(result.Error, "unmatched (unknown_profile)") {
			t.Errorf("%s: result = %+v; want rejection under the unmatched policy", name, result)
		}
		if bus.writeCount() != 0 {
			t.Errorf("%s: bus writes = %d; want 0", name, bus.writeCount())
		}
		poller.publishSystem(semanticSnapshotSourceLive)
		if bound := provider.System().Config.DhwBivalenceBound; bound == nil || bound.Source != graphql.ConfigBoundSourceUnmatchedPolicy {
			t.Errorf("%s: published bound = %+v; want unmatched policy", name, bound)
		}
	}
}

func TestEnsureControllerAPIIdentityCachesAndSkipsWithoutIdentity(t *testing.T) {
	bus := &dhwBoundFakeBus{apiAnswer: apiAnswers(1, 1), store: map[uint16][]byte{}}
	poller, _ := newDHWBoundPoller(t, "BASV0", "0217", "8803", bus)
	poller.ensureControllerAPIIdentity(context.Background())
	poller.ensureControllerAPIIdentity(context.Background())
	if got := len(bus.op00Reads); got != 2 {
		t.Fatalf("OP00 reads = %d (%v); want one version + one revision read, cached afterwards", got, bus.op00Reads)
	}
	if id := poller.controllerB524Identity(); id.APIVersion != "1" || id.Revision != "1" || id.EID != "BASV0" {
		t.Fatalf("identity = %+v; want BASV0 with API 1.1", id)
	}

	// A failed read is not retried inside the backoff window.
	failBus := &dhwBoundFakeBus{apiAnswer: map[uint16][]byte{}, store: map[uint16][]byte{}}
	failing, _ := newDHWBoundPoller(t, "BASV0", "0217", "8803", failBus)
	failing.ensureControllerAPIIdentity(context.Background())
	failing.ensureControllerAPIIdentity(context.Background())
	if got := len(failBus.op00Reads); got != 2 {
		t.Fatalf("OP00 reads after a failure = %d; want 2 (version read, revision read, then backoff)", got)
	}

	// Without software/hardware nothing can match; no OP00 request is sent.
	bareBus := &dhwBoundFakeBus{apiAnswer: apiAnswers(1, 1), store: map[uint16][]byte{}}
	bare, _ := newDHWBoundPoller(t, "BASV0", "", "", bareBus)
	bare.ensureControllerAPIIdentity(context.Background())
	if len(bareBus.op00Reads) != 0 {
		t.Fatalf("OP00 reads without SW/HW = %v; want none", bareBus.op00Reads)
	}
}

func TestSetSystemConfigOtherSystemNumbersUnchangedAndSkipOP00(t *testing.T) {
	for _, identity := range [][3]string{{"BASV0", "0217", "8803"}, {"BASV2", "0507", "1704"}} {
		bus := &dhwBoundFakeBus{apiAnswer: apiAnswers(1, 1), store: map[uint16][]byte{}}
		poller, _ := newDHWBoundPoller(t, identity[0], identity[1], identity[2], bus)

		accepted := map[string]string{"maxRoomHumidityPct": "80", "hcBivalencePointC": "30", "hcEmergencyTempC": "80", "hwcMaxFlowTempC": "80"}
		for field, value := range accepted {
			if result := poller.SetSystemConfig(context.Background(), field, value); !result.Success {
				t.Fatalf("%s %s=%s rejected: %q", identity[0], field, value, result.Error)
			}
		}
		rejected := map[string]string{"maxRoomHumidityPct": "81", "hcBivalencePointC": "31", "hcEmergencyTempC": "81", "hwcMaxFlowTempC": "81"}
		for field, value := range rejected {
			result := poller.SetSystemConfig(context.Background(), field, value)
			if result.Success || strings.Contains(result.Error, "unmatched") || strings.Contains(result.Error, "controller profile") {
				t.Fatalf("%s %s=%s result = %+v; want a plain fixed-range rejection", identity[0], field, value, result)
			}
		}
		if got := bus.writeCount(); got != len(accepted) {
			t.Fatalf("%s writes = %d; want %d", identity[0], got, len(accepted))
		}
		if len(bus.op00Reads) != 0 {
			t.Fatalf("%s: OP00 reads for non-DHW-bivalence fields = %v; want none", identity[0], bus.op00Reads)
		}
	}
}

// TestSystemConfigBoundMCPGraphQLParity checks the active bound is the same
// value on the GraphQL provider and on the MCP adapter, serialized to the MCP
// envelope field names.
func TestSystemConfigBoundMCPGraphQLParity(t *testing.T) {
	for _, identity := range [][3]string{{"BASV0", "0217", "8803"}, {"BASV2", "0507", "1704"}} {
		bus := &dhwBoundFakeBus{apiAnswer: apiAnswers(1, 1), store: map[uint16][]byte{}}
		poller, provider := newDHWBoundPoller(t, identity[0], identity[1], identity[2], bus)
		poller.ensureControllerAPIIdentity(context.Background())
		poller.publishSystem(semanticSnapshotSourceLive)

		gql := provider.System().Config.DhwBivalenceBound
		mcpStatus := (mcpSemanticProviderAdapter{provider: provider}).System()
		if gql == nil || mcpStatus == nil || mcpStatus.Config == nil || mcpStatus.Config.DhwBivalenceBound == nil {
			t.Fatalf("%s: bound missing (graphql=%v mcp=%v)", identity[0], gql, mcpStatus)
		}
		mb := mcpStatus.Config.DhwBivalenceBound
		if mb.MinC != gql.MinC || mb.MaxC != gql.MaxC || mb.Source != gql.Source || mb.Reason != gql.Reason || mb.Profile != gql.Profile {
			t.Fatalf("%s: MCP bound %+v != GraphQL bound %+v", identity[0], mb, gql)
		}
		raw, err := json.Marshal(mcpStatus.Config)
		if err != nil {
			t.Fatal(err)
		}
		var decoded map[string]any
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatal(err)
		}
		envelope, ok := decoded["dhw_bivalence_point_bound"].(map[string]any)
		if !ok || envelope["min_c"] != float64(-20) || envelope["max_c"] != float64(20) || envelope["source"] != gql.Source {
			t.Fatalf("%s: MCP envelope bound = %#v; want dhw_bivalence_point_bound{min_c:-20,max_c:20,source:%s}", identity[0], decoded["dhw_bivalence_point_bound"], gql.Source)
		}
	}
}

func TestParseB524SystemInformationInt(t *testing.T) {
	cases := []struct {
		name    string
		payload []byte
		want    int
		ok      bool
	}{
		{"one", f32(1), 1, true},
		{"zero", f32(0), 0, true},
		{"fraction", f32(0.5), 0, false},
		{"negative", f32(-2), 0, false},
		{"infinity", f32(math.Inf(1)), 0, false},
		{"short", []byte{0x01, 0x02, 0x03}, 0, false},
		{"long", []byte{0x00, 0x00, 0x80, 0x3f, 0x00}, 0, false},
		{"empty", nil, 0, false},
	}
	for _, tc := range cases {
		got, ok := parseB524SystemInformationInt(tc.payload)
		if got != tc.want || ok != tc.ok {
			t.Errorf("%s: got (%d, %v); want (%d, %v)", tc.name, got, ok, tc.want, tc.ok)
		}
	}
}

func TestMCPConfigWriterDHWBivalenceRejectsBeforeBusWrite(t *testing.T) {
	bus := &dhwBoundFakeBus{apiAnswer: apiAnswers(1, 1), store: map[uint16][]byte{}}
	poller, _ := newDHWBoundPoller(t, "BASV0", "0217", "8803", bus)
	writer := &mcpConfigWriterAdapter{poller: poller}

	result := writer.SetSystemConfig(context.Background(), "dhwBivalencePointC", "35")
	if result.Success || !strings.Contains(result.Error, "allows [-20, 20]") {
		t.Fatalf("MCP set_config 35 on BASV0 = %+v; want rejection naming the profile bound", result)
	}
	if bus.writeCount() != 0 {
		t.Fatalf("bus writes = %d; want 0", bus.writeCount())
	}
	if result := writer.SetSystemConfig(context.Background(), "dhwBivalencePointC", "20"); !result.Success {
		t.Fatalf("MCP set_config 20 on BASV0 rejected: %q", result.Error)
	}
}
