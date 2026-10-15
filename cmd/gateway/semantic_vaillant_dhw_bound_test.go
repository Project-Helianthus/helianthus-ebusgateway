package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	"github.com/Project-Helianthus/helianthus-ebusreg/router"
	vaillantsystem "github.com/Project-Helianthus/helianthus-ebusreg/vaillant/system"
)

// dhwBoundFakeBus acknowledges OP02 writes and reads back the last written
// value. It counts all requests, including unexpected OP00 acquisition.
type dhwBoundFakeBus struct {
	mu        sync.Mutex
	requests  int
	store     map[uint16][]byte // OP02 addr -> value
	writes    [][]byte
	op00Reads []uint16
}

func (bus *dhwBoundFakeBus) send(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
	bus.mu.Lock()
	defer bus.mu.Unlock()
	bus.requests++
	data := frame.Data
	if len(data) == 3 && data[0] == 0x00 {
		id := uint16(data[1]) | uint16(data[2])<<8
		bus.op00Reads = append(bus.op00Reads, id)
		// This unqualified layout used to be accepted as API 1.1. No bound
		// validation or publication may ask for or consume it.
		return &protocol.Frame{Data: []byte{0x00, 0x00, 0x80, 0x3f}}, nil
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

func TestSetSystemConfigDHWBivalenceBASV0WithoutAPIUsesPolicy(t *testing.T) {
	bus := &dhwBoundFakeBus{store: map[uint16][]byte{}}
	poller, provider := newDHWBoundPoller(t, "BASV0", "0217", "8803", bus)

	for _, value := range []string{"51", "-21", "50.5"} {
		result := poller.SetSystemConfig(context.Background(), "dhwBivalencePointC", value)
		if result.Success {
			t.Fatalf("BASV0 value %s accepted; want rejection", value)
		}
		if !strings.Contains(result.Error, "controller identity unmatched (unknown_profile): conservative policy bound [-20, 50] applies") {
			t.Fatalf("BASV0 value %s error = %q; want the unmatched policy named", value, result.Error)
		}
	}
	if bus.requests != 0 {
		t.Fatalf("invalid values issued %d bus requests; want zero", bus.requests)
	}
	if got := bus.writeCount(); got != 0 {
		t.Fatalf("bus writes after rejected BASV0 values = %d; want 0 (reject before any bus write)", got)
	}

	for _, value := range []string{"20", "-20", "21", "50", "20.5"} {
		if result := poller.SetSystemConfig(context.Background(), "dhwBivalencePointC", value); !result.Success {
			t.Fatalf("BASV0 value %s rejected: %q", value, result.Error)
		}
	}
	if got := bus.writeCount(); got != 5 {
		t.Fatalf("bus writes for in-bound BASV0 values = %d; want 5", got)
	}

	bound := provider.System().Config.DhwBivalenceBound
	want := graphql.ConfigBound{MinC: -20, MaxC: 50, Source: graphql.ConfigBoundSourceUnmatchedPolicy, Reason: graphql.ConfigBoundReasonUnknownProfile}
	if bound == nil || *bound != want {
		t.Fatalf("published bound = %+v; want %+v", bound, want)
	}
}

func TestSetSystemConfigDHWBivalenceBASV2FallsToUnmatchedPolicy(t *testing.T) {
	// Without a seeded profile, the released unmatched policy applies.
	bus := &dhwBoundFakeBus{store: map[uint16][]byte{}}
	poller, provider := newDHWBoundPoller(t, "BASV2", "0507", "1704", bus)

	for _, value := range []string{"51", "-21"} {
		result := poller.SetSystemConfig(context.Background(), "dhwBivalencePointC", value)
		if result.Success {
			t.Fatalf("BASV2 value %s accepted; want rejection under the unmatched policy", value)
		}
		if !strings.Contains(result.Error, "controller identity unmatched (unknown_profile): conservative policy bound [-20, 50] applies") {
			t.Fatalf("BASV2 value %s error = %q; want the unmatched policy named", value, result.Error)
		}
	}
	if bus.requests != 0 {
		t.Fatalf("invalid values issued %d bus requests; want zero", bus.requests)
	}
	if got := bus.writeCount(); got != 0 {
		t.Fatalf("bus writes after rejected BASV2 values = %d; want 0", got)
	}
	for _, value := range []string{"21", "50"} {
		if result := poller.SetSystemConfig(context.Background(), "dhwBivalencePointC", value); !result.Success {
			t.Fatalf("BASV2 value %s rejected: %q", value, result.Error)
		}
	}

	bound := provider.System().Config.DhwBivalenceBound
	if bound == nil || bound.Source != graphql.ConfigBoundSourceUnmatchedPolicy || bound.Reason != graphql.ConfigBoundReasonUnknownProfile || bound.MinC != -20 || bound.MaxC != 50 || bound.Profile != "" {
		t.Fatalf("published bound = %+v; want unmatched policy -20..50", bound)
	}
}

func TestControllerB524IdentityUsesOnlyRegistry(t *testing.T) {
	bus := &dhwBoundFakeBus{store: map[uint16][]byte{}}
	poller, provider := newDHWBoundPoller(t, "BASV0", "0217", "8803", bus)
	for attempt := 0; attempt < 2; attempt++ {
		id := poller.controllerB524Identity()
		if id.EID != "BASV0" || id.SoftwareRaw != "0217" || id.HardwareRaw != "8803" || id.APIVersion != "" || id.Revision != "" {
			t.Fatalf("registry identity = %+v; want BASV0/SW0217/HW8803 without API", id)
		}
		poller.publishSystem(semanticSnapshotSourceLive)
		if bound := provider.System().Config.DhwBivalenceBound; bound == nil || bound.Source != graphql.ConfigBoundSourceUnmatchedPolicy || bound.Reason != graphql.ConfigBoundReasonUnknownProfile || bound.Profile != "" {
			t.Fatalf("published bound = %+v; want unmatched policy without a substituted profile", bound)
		}
	}
	if bus.requests != 0 {
		t.Fatalf("identity/publication issued %d bus requests; want zero", bus.requests)
	}

	// A replacement at the same address is invisible until discovery/import
	// changes registry metadata. Validation itself must not refresh identity.
	poller.reg.Register(registry.DeviceInfo{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV2", SoftwareVersion: "0507", HardwareVersion: "1704"})
	if id := poller.controllerB524Identity(); id.EID != "BASV2" || id.SoftwareRaw != "0507" || id.HardwareRaw != "1704" || id.APIVersion != "" || id.Revision != "" {
		t.Fatalf("updated registry identity = %+v; want BASV2 without API", id)
	}
	if bus.requests != 0 {
		t.Fatalf("registry identity update issued %d bus requests; want zero", bus.requests)
	}
}

func TestRefreshSystemDoesNotAcquireAPIIdentity(t *testing.T) {
	bus := &dhwBoundFakeBus{store: map[uint16][]byte{}}
	poller, _ := newDHWBoundPoller(t, "BASV0", "0217", "8803", bus)
	poller.refreshSystem(context.Background())
	if bus.requests == 0 {
		t.Fatal("refresh did not exercise the existing system poll set")
	}
	if len(bus.op00Reads) != 0 || bus.writeCount() != 0 {
		t.Fatalf("refresh added OP00 acquisition or writes: reads=%v writes=%d", bus.op00Reads, bus.writeCount())
	}
}

func TestSetSystemConfigOtherSystemNumbersUnchangedAndSkipOP00(t *testing.T) {
	for _, identity := range [][3]string{{"BASV0", "0217", "8803"}, {"BASV2", "0507", "1704"}} {
		bus := &dhwBoundFakeBus{store: map[uint16][]byte{}}
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
		bus := &dhwBoundFakeBus{store: map[uint16][]byte{}}
		poller, provider := newDHWBoundPoller(t, identity[0], identity[1], identity[2], bus)
		poller.publishSystem(semanticSnapshotSourceLive)

		gql := provider.System().Config.DhwBivalenceBound
		mcpStatus := (mcpSemanticProviderAdapter{provider: provider}).System()
		if gql == nil || mcpStatus == nil || mcpStatus.Config == nil || mcpStatus.Config.DhwBivalenceBound == nil {
			t.Fatalf("%s: bound missing (graphql=%v mcp=%v)", identity[0], gql, mcpStatus)
		}
		mb := mcpStatus.Config.DhwBivalenceBound
		if mb.MinC != gql.MinC || mb.MaxC != gql.MaxC || mb.Source != gql.Source || mb.Reason != gql.Reason || mb.Profile != gql.Profile || mb.NarrowedOnly != gql.NarrowedOnly {
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
		if !ok || envelope["min_c"] != float64(-20) || envelope["max_c"] != float64(50) || envelope["source"] != gql.Source || envelope["narrowed_only"] != gql.NarrowedOnly {
			t.Fatalf("%s: MCP envelope bound = %#v; want dhw_bivalence_point_bound{min_c:-20,max_c:50,source:%s}", identity[0], decoded["dhw_bivalence_point_bound"], gql.Source)
		}
	}
}

func TestMCPConfigWriterDHWBivalenceRejectsBeforeBusWrite(t *testing.T) {
	bus := &dhwBoundFakeBus{store: map[uint16][]byte{}}
	poller, _ := newDHWBoundPoller(t, "BASV0", "0217", "8803", bus)
	writer := &mcpConfigWriterAdapter{poller: poller}

	result := writer.SetSystemConfig(context.Background(), "dhwBivalencePointC", "51")
	if result.Success || !strings.Contains(result.Error, "conservative policy bound [-20, 50]") {
		t.Fatalf("MCP set_config 51 on BASV0 = %+v; want rejection naming the unmatched policy bound", result)
	}
	if bus.writeCount() != 0 {
		t.Fatalf("bus writes = %d; want 0", bus.writeCount())
	}
	if result := writer.SetSystemConfig(context.Background(), "dhwBivalencePointC", "50"); !result.Success {
		t.Fatalf("MCP set_config 50 on BASV0 rejected: %q", result.Error)
	}
}

// dhwBoundInvoker catches any accidental fallback to the registry invoke path.
type dhwBoundInvoker struct{ calls int }

func (invoker *dhwBoundInvoker) Invoke(context.Context, router.Plane, string, map[string]any) (any, error) {
	invoker.calls++
	return nil, errors.New("unexpected invoke")
}

func TestSystemConfigNonFiniteRejectedBeforeBusIO(t *testing.T) {
	fields := []string{"dhwBivalencePointC", "hcBivalencePointC", "hcEmergencyTempC", "hwcMaxFlowTempC", "maxRoomHumidityPct", "installerMenuCode"}
	for _, path := range []string{"GraphQL", "MCP"} {
		for _, field := range fields {
			for _, value := range []string{"NaN", "Inf", "-Inf"} {
				t.Run(path+"/"+field+"/"+value, func(t *testing.T) {
					bus := &dhwBoundFakeBus{store: map[uint16][]byte{}}
					poller, _ := newDHWBoundPoller(t, "BASV0", "0217", "8803", bus)
					invoker := &dhwBoundInvoker{}
					var success bool
					var message string
					if path == "GraphQL" {
						builder := graphql.NewBuilder(poller.reg, nil)
						builder.SetSystemConfigWriter(poller)
						builder.SetAdmittedMutationSource(0x71)
						handler, err := graphql.NewInvokeHandler(builder, poller.reg, invoker)
						if err != nil {
							t.Fatal(err)
						}
						query := fmt.Sprintf(`mutation { setSystemConfig(field: %q, value: %q) { success error } }`, field, value)
						body, err := json.Marshal(map[string]string{"query": query})
						if err != nil {
							t.Fatal(err)
						}
						req := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(string(body)))
						req.Header.Set("Content-Type", "application/json")
						rec := httptest.NewRecorder()
						handler.ServeHTTP(rec, req)
						var response struct {
							Data struct {
								Result graphql.ConfigMutationResult `json:"setSystemConfig"`
							} `json:"data"`
							Errors []any `json:"errors"`
						}
						if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil || rec.Code != http.StatusOK || len(response.Errors) != 0 {
							t.Fatalf("GraphQL response: err=%v status=%d body=%s", err, rec.Code, rec.Body.String())
						}
						success, message = response.Data.Result.Success, response.Data.Result.Error
					} else {
						result := (&mcpConfigWriterAdapter{poller: poller}).SetSystemConfig(context.Background(), field, value)
						success, message = result.Success, result.Error
					}
					if success || !strings.Contains(message, "must be finite") {
						t.Fatalf("success=%v error=%q; want explicit non-finite rejection", success, message)
					}
					if bus.writeCount() != 0 || bus.requests != 0 || invoker.calls != 0 {
						t.Fatalf("writes=%d requests=%d invokes=%d; want all zero", bus.writeCount(), bus.requests, invoker.calls)
					}
				})
			}
		}
	}
}

func TestMCPConfigBoundPreservesNarrowedOnly(t *testing.T) {
	for _, bound := range []graphql.ConfigBound{
		graphql.ResolveDHWBivalenceBound(vaillantsystem.B524ControllerIdentity{EID: "BASV0", SoftwareRaw: "0217", HardwareRaw: "8803", APIVersion: "1", Revision: "1"}),
		graphql.ResolveDHWBivalenceBound(vaillantsystem.B524ControllerIdentity{}),
	} {
		got := mcpConfigBound(&bound)
		if got.NarrowedOnly != bound.NarrowedOnly || got.Source != bound.Source || got.Profile != bound.Profile {
			t.Fatalf("MCP bound = %+v; GraphQL bound = %+v", got, bound)
		}
		raw, err := json.Marshal(got)
		if err != nil {
			t.Fatal(err)
		}
		var decoded map[string]any
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatal(err)
		}
		if decoded["narrowed_only"] != bound.NarrowedOnly {
			t.Fatalf("MCP narrowed_only = %v; want %v", decoded["narrowed_only"], bound.NarrowedOnly)
		}
	}
}
