package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	graphqlgo "github.com/graphql-go/graphql"
)

// M62 pins that circuit 0's pump status (RR=0x001B) and circuit state
// (RR=0x001E) stay on the circuit surface. Neither is a source for the boiler
// central-heating pump or the boiler heating-status diagnostic.

const m62BoilerAddress = byte(0x08)

func TestM62BoilerUnavailableLeavesBoilerFieldsUnknownAndPublishesCircuit(t *testing.T) {
	for _, boiler := range []struct {
		name    string
		address byte
	}{
		{name: "no_boiler_address", address: 0},
		{name: "boiler_reads_fail", address: m62BoilerAddress},
	} {
		for _, circuitRaw := range []uint16{1, 0} {
			name := boiler.name + "/circuit_pump_on"
			stateRaw := uint16(1)
			if circuitRaw == 0 {
				name = boiler.name + "/circuit_pump_off"
				stateRaw = 2
			}
			t.Run(name, func(t *testing.T) {
				poller := m62Poller(boiler.address, circuitRaw, stateRaw)
				var boilerReads int
				poller.sendFrameFn = func(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
					if frame.Primary != vaillantB509Primary || frame.Secondary != vaillantB509Secondary {
						t.Fatalf("unexpected non-B509 active read: % x", frame.Data)
					}
					boilerReads++
					return nil, errors.New("boiler B509 unavailable")
				}

				poller.refreshBoilerStatusTier(context.Background(), boilerStatusTierFast)
				poller.publishCircuits(semanticSnapshotSourceLive)

				if boiler.address == 0 && boilerReads != 0 {
					t.Fatalf("B509 reads = %d without a boiler address; want none", boilerReads)
				}
				if boiler.address != 0 && boilerReads == 0 {
					t.Fatal("B509 reads = 0; want the failing boiler path exercised")
				}
				m62AssertSurfaces(t, poller, nil, circuitRaw, stateRaw)
				if got := poller.circuits[0].PumpStatusRaw; got == nil || *got != circuitRaw {
					t.Fatalf("native circuit pump = %v; want preserved %d", got, circuitRaw)
				}
				if got := poller.circuits[0].CircuitStateRaw; got == nil || *got != stateRaw {
					t.Fatalf("native circuit state = %v; want preserved %d", got, stateRaw)
				}
			})
		}
	}
}

func TestM62BoilerPumpComesOnlyFromB509(t *testing.T) {
	for _, boilerByte := range []byte{0xF0, 0x01} {
		boilerActive, ok := decodeOnOff([]byte{boilerByte})
		if !ok {
			t.Fatalf("decodeOnOff(0x%02x) failed", boilerByte)
		}
		for _, circuitRaw := range []uint16{0, 1} {
			name := "boiler_off"
			if boilerActive {
				name = "boiler_on"
			}
			stateRaw := uint16(3)
			if circuitRaw == 0 {
				name += "/circuit_pump_off"
			} else {
				name += "/circuit_pump_on"
				stateRaw = 1
			}
			t.Run(name, func(t *testing.T) {
				poller := m62Poller(m62BoilerAddress, circuitRaw, stateRaw)
				var pumpReads int
				poller.sendFrameFn = func(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
					if frame.Primary != vaillantB509Primary || frame.Secondary != vaillantB509Secondary {
						t.Fatalf("unexpected non-B509 active read: % x", frame.Data)
					}
					if len(frame.Data) < 3 || frame.Data[0] != vaillantB509OpcodeRead {
						t.Fatalf("unexpected B509 request % x", frame.Data)
					}
					addr := uint16(frame.Data[1])<<8 | uint16(frame.Data[2])
					value := []byte{0x00, 0x00}
					if addr == boiler_b509_central_heating_pump {
						pumpReads++
						value = []byte{boilerByte}
					}
					return &protocol.Frame{Data: append([]byte{vaillantB509OpcodeRead, frame.Data[1], frame.Data[2]}, value...)}, nil
				}

				poller.refreshBoilerStatusTier(context.Background(), boilerStatusTierFast)
				poller.publishCircuits(semanticSnapshotSourceLive)

				if pumpReads == 0 {
					t.Fatal("boiler pump B509 read not issued")
				}
				m62AssertSurfaces(t, poller, &boilerActive, circuitRaw, stateRaw)
			})
		}
	}
}

func TestM62CachedHeatingStatusRawWithoutBoilerSourceIsDiscarded(t *testing.T) {
	for _, cachedRaw := range []int{0, 1, 3} {
		t.Run(decodeCircuitStateToken(m62Uint16(uint16(cachedRaw))), func(t *testing.T) {
			store := newSemanticCacheStore(filepath.Join(t.TempDir(), "semantic-cache.json"), t.Logf)
			dhwTemp := 48.5
			// Existing caches carry the legacy circuit-derived diagnostic.
			if err := store.writeV3(semanticCacheV3{
				SchemaVersion: semanticCacheSchemaVersionV3,
				Boiler: &semanticCacheBoiler{
					DhwTemperatureC:  &dhwTemp,
					HeatingStatusRaw: &cachedRaw,
				},
			}); err != nil {
				t.Fatal(err)
			}

			poller := m62Poller(0, 1, uint16(cachedRaw))
			poller.provider = graphql.NewLiveSemanticProvider()
			snapshot, ok := preloadSemanticCache(poller.provider, store)
			if !ok {
				t.Fatal("legacy cache was discarded wholesale")
			}
			status := poller.provider.BoilerStatus()
			if status == nil {
				t.Fatal("cached boiler status missing; want other cached fields restored")
			}
			if got := status.State.DhwTemperatureC; got == nil || *got != dhwTemp {
				t.Fatalf("cached DHW temperature = %v; want restored %.1f", got, dhwTemp)
			}
			if got := status.Diagnostics.HeatingStatusRaw; got != nil {
				t.Fatalf("cached heating status = %d; want unset without a boiler-qualified source", *got)
			}
			poller.hydrateFromCache(snapshot)
			if poller.boiler == nil || poller.boiler.HeatingStatusRaw != nil {
				t.Fatal("hydrated boiler heating status must be unset")
			}

			// A legacy value already held in memory is not written back.
			legacy := cachedRaw
			if err := store.Save(semanticCacheSnapshot{Boiler: &graphql.BoilerStatus{
				State:       graphql.BoilerState{DhwTemperatureC: &dhwTemp},
				Diagnostics: graphql.BoilerDiagnostics{HeatingStatusRaw: &legacy},
			}}); err != nil {
				t.Fatal(err)
			}
			// Inspect the persisted file directly: Load also drops the field, so
			// only the raw payload shows whether it was written.
			payload, err := os.ReadFile(store.path)
			if err != nil {
				t.Fatal(err)
			}
			var persisted semanticCacheV3
			if err := json.Unmarshal(payload, &persisted); err != nil {
				t.Fatal(err)
			}
			if persisted.Boiler == nil || persisted.Boiler.DhwTemperatureC == nil {
				t.Fatalf("persisted boiler = %+v; want other boiler fields written", persisted.Boiler)
			}
			if persisted.Boiler.HeatingStatusRaw != nil {
				t.Fatalf("persisted heating status = %d; want not written without a boiler source", *persisted.Boiler.HeatingStatusRaw)
			}
			cached, ok := store.Load()
			if !ok || cached.Boiler == nil {
				t.Fatal("saved cache did not load")
			}
			if cached.Boiler.Diagnostics.HeatingStatusRaw != nil {
				t.Fatal("cache must not persist the boiler heating status without a boiler source")
			}
		})
	}
}

func TestM62BoilerRegisterDefinitionsNeverReadCircuitPumpOrState(t *testing.T) {
	t.Parallel()

	for _, tier := range []boilerStatusTier{
		boilerStatusTierFast,
		boilerStatusTierMedium,
		boilerStatusTierSlow,
	} {
		for _, register := range boilerStatusRegisterDefinitionsForTier(tier) {
			if register.group != localCircuits.group {
				continue
			}
			switch register.addr {
			case circuit_pump_status, circuit_circuit_state:
				t.Errorf("tier %v maps circuit GG=0x%02x II=0x%02x RR=0x%04x into the boiler status; circuit registers belong to the circuit surface only",
					tier, register.group, register.instance, register.addr)
			}
		}
	}
}

func m62Uint16(v uint16) *uint16 { return &v }

func m62Poller(boilerAddress byte, circuitRaw, stateRaw uint16) *vaillantSemanticPoller {
	observedAt := time.Unix(100, 0)
	// Fresh controller flow makes the fast refresh publish a boiler status, so
	// the assertions inspect a published payload rather than a missing one.
	flow := 42.5
	return &vaillantSemanticPoller{
		scheduler:          ebusgateway.NewSemanticReadScheduler(),
		provider:           graphql.NewLiveSemanticProvider(),
		source:             0x7F,
		controller:         0x15,
		boilerAddress:      boilerAddress,
		configInterval:     5 * time.Minute,
		boilerFastInterval: 30 * time.Second,
		requestTimeout:     20 * time.Millisecond,
		nowFn:              func() time.Time { return observedAt.Add(10 * time.Second) },
		system: &vaillantSystemSnapshot{
			Controller:                  0x15,
			SystemFlowTemperature:       &flow,
			SystemFlowTemperatureLiveAt: observedAt,
		},
		circuits: map[byte]*vaillantCircuitSnapshot{
			0x00: {
				Instance:           0x00,
				Active:             true,
				Controller:         0x15,
				PumpStatusRaw:      m62Uint16(circuitRaw),
				PumpStatusLiveAt:   observedAt,
				CircuitStateRaw:    m62Uint16(stateRaw),
				CircuitStateLiveAt: observedAt,
			},
		},
	}
}

// m62AssertSurfaces checks the provider, GraphQL, MCP and Portal projections.
// wantBoilerPump nil means the boiler pump must be absent/unknown.
func m62AssertSurfaces(t *testing.T, poller *vaillantSemanticPoller, wantBoilerPump *bool, circuitRaw, stateRaw uint16) {
	t.Helper()
	wantCircuitPump := circuitRaw != 0
	wantCircuitState := decodeCircuitStateToken(&stateRaw)

	status := poller.provider.BoilerStatus()
	if status == nil {
		t.Fatal("boiler status not published")
	}
	m62AssertOptionalBool(t, "provider boiler pump", status.State.CentralHeatingPumpActive, wantBoilerPump)
	if got := status.Diagnostics.HeatingStatusRaw; got != nil {
		t.Errorf("provider boiler heating status = %d; want unset without a boiler-qualified source", *got)
	}
	circuits := poller.provider.Circuits()
	if len(circuits) != 1 || circuits[0].Index != 0 {
		t.Fatalf("provider circuits = %+v; want circuit 0", circuits)
	}
	m62AssertOptionalBool(t, "provider circuit pump", circuits[0].State.PumpActive, &wantCircuitPump)
	if circuits[0].State.CircuitState != wantCircuitState {
		t.Errorf("provider circuit state = %q; want %q", circuits[0].State.CircuitState, wantCircuitState)
	}

	// GraphQL: both alias spellings of each field.
	builder := graphql.NewBuilder(nil, nil)
	builder.SetSemanticProvider(poller.provider)
	schema, err := graphql.NewQuerySchema(builder)
	if err != nil {
		t.Fatalf("NewQuerySchema: %v", err)
	}
	result := graphqlgo.Do(graphqlgo.Params{Schema: schema, RequestString: `{
		boilerStatus {
			state { centralHeatingPumpActive central_heating_pump_active }
			diagnostics { heatingStatusRaw heating_status_raw }
		}
		circuits { index state { pumpActive pump_active circuitState } }
	}`})
	if len(result.Errors) != 0 {
		t.Fatalf("GraphQL errors: %+v", result.Errors)
	}
	data := m62JSONObject(t, result.Data)
	boilerObj := m62Object(t, data, "boilerStatus")
	stateObj := m62Object(t, boilerObj, "state")
	diagObj := m62Object(t, boilerObj, "diagnostics")
	for _, key := range []string{"centralHeatingPumpActive", "central_heating_pump_active"} {
		m62AssertJSONBool(t, "GraphQL boilerStatus.state."+key, stateObj, key, true, wantBoilerPump)
	}
	for _, key := range []string{"heatingStatusRaw", "heating_status_raw"} {
		if value, present := diagObj[key]; !present || value != nil {
			t.Errorf("GraphQL boilerStatus.diagnostics.%s = %v (present=%t); want null", key, value, present)
		}
	}
	gqlCircuits, _ := data["circuits"].([]any)
	if len(gqlCircuits) != 1 {
		t.Fatalf("GraphQL circuits = %v; want one circuit", data["circuits"])
	}
	circuitState := m62Object(t, gqlCircuits[0].(map[string]any), "state")
	for _, key := range []string{"pumpActive", "pump_active"} {
		m62AssertJSONBool(t, "GraphQL circuits[0].state."+key, circuitState, key, true, &wantCircuitPump)
	}
	if circuitState["circuitState"] != wantCircuitState {
		t.Errorf("GraphQL circuits[0].state.circuitState = %v; want %q", circuitState["circuitState"], wantCircuitState)
	}

	// MCP: omitempty pointers, so an unknown boiler field is an absent key.
	adapter := newMCPSemanticProvider(poller.provider)
	mcpBoiler := m62JSONObject(t, adapter.BoilerStatus())
	m62AssertJSONBool(t, "MCP boiler state.central_heating_pump_active", m62Object(t, mcpBoiler, "state"), "central_heating_pump_active", false, wantBoilerPump)
	if diag, ok := mcpBoiler["diagnostics"].(map[string]any); ok {
		if value, present := diag["heating_status_raw"]; present {
			t.Errorf("MCP boiler diagnostics.heating_status_raw = %v; want absent", value)
		}
	}
	mcpCircuits := adapter.Circuits()
	if len(mcpCircuits) != 1 {
		t.Fatalf("MCP circuits = %+v; want one circuit", mcpCircuits)
	}
	m62AssertJSONBool(t, "MCP circuits[0].state.pump_active", m62Object(t, m62JSONObject(t, mcpCircuits[0]), "state"), "pump_active", false, &wantCircuitPump)

	// Portal projection.
	portalBoiler := m62JSONObject(t, mapPortalBoilerStatus(status))
	m62AssertJSONBool(t, "Portal boiler state.central_heating_pump_active", m62Object(t, portalBoiler, "state"), "central_heating_pump_active", false, wantBoilerPump)
	if diag, ok := portalBoiler["diagnostics"].(map[string]any); ok {
		if value, present := diag["heating_status_raw"]; present {
			t.Errorf("Portal boiler diagnostics.heating_status_raw = %v; want absent", value)
		}
	}
	portalCircuits := mapPortalCircuits(circuits)
	if len(portalCircuits) != 1 {
		t.Fatalf("Portal circuits = %+v; want one circuit", portalCircuits)
	}
	m62AssertJSONBool(t, "Portal circuits[0].state.pump_active", m62Object(t, m62JSONObject(t, portalCircuits[0]), "state"), "pump_active", false, &wantCircuitPump)
}

func m62AssertOptionalBool(t *testing.T, label string, got, want *bool) {
	t.Helper()
	switch {
	case want == nil && got != nil:
		t.Errorf("%s = %t; want unknown", label, *got)
	case want != nil && got == nil:
		t.Errorf("%s = unknown; want %t", label, *want)
	case want != nil && *got != *want:
		t.Errorf("%s = %t; want %t", label, *got, *want)
	}
}

// m62AssertJSONBool checks a decoded JSON boolean. nullWhenUnknown selects
// GraphQL semantics (key present with null) versus omitempty (key absent).
func m62AssertJSONBool(t *testing.T, label string, object map[string]any, key string, nullWhenUnknown bool, want *bool) {
	t.Helper()
	value, present := object[key]
	if want == nil {
		if nullWhenUnknown && (!present || value != nil) {
			t.Errorf("%s = %v (present=%t); want null", label, value, present)
		}
		if !nullWhenUnknown && present {
			t.Errorf("%s = %v; want absent", label, value)
		}
		return
	}
	if got, ok := value.(bool); !ok || got != *want {
		t.Errorf("%s = %v; want %t", label, value, *want)
	}
}

func m62JSONObject(t *testing.T, value any) map[string]any {
	t.Helper()
	payload, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal %T: %v", value, err)
	}
	var out map[string]any
	if err := json.Unmarshal(payload, &out); err != nil {
		t.Fatalf("unmarshal %T: %v", value, err)
	}
	return out
}

func m62Object(t *testing.T, object map[string]any, key string) map[string]any {
	t.Helper()
	child, ok := object[key].(map[string]any)
	if !ok {
		t.Fatalf("%s = %v; want object", key, object[key])
	}
	return child
}
