package main

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
)

func TestM02ConstructorCharacterization(t *testing.T) {
	for mask := 0; mask < 8; mask++ {
		t.Run(fmt.Sprint(mask), func(t *testing.T) {
			var tcp *modbusadapter.Adapter
			var growatt *growattBMSRS485ProductionProvider
			var tesla *teslaHSCRetainedOwner
			if mask&1 != 0 {
				tcp = &modbusadapter.Adapter{}
			}
			if mask&2 != 0 {
				growatt = &growattBMSRS485ProductionProvider{}
			}
			if mask&4 != 0 {
				tesla = &teslaHSCRetainedOwner{}
			}
			constructors := []struct {
				name   string
				got    mcp.ModbusV1Provider
				owners int
			}{
				{"core", newGatewayModbusMCPProvider(tcp), mask & 1},
				{"growatt", newGatewayModbusMCPProviderWithGrowatt(tcp, growatt), mask & 3},
				{"runtimes", newGatewayModbusMCPProviderWithRuntimes(tcp, growatt, tesla), mask},
			}
			for _, c := range constructors {
				t.Run(c.name, func(t *testing.T) {
					if c.owners == 0 {
						if c.got != nil {
							t.Fatalf("typed nil became provider: %T", c.got)
						}
						return
					}
					expectedType := reflect.TypeOf(&gatewayModbusMCPProvider{})
					if c.owners&2 != 0 {
						expectedType = reflect.TypeOf(gatewayGrowattBMSMCPProvider{})
					}
					if c.owners&4 != 0 {
						expectedType = reflect.TypeOf(&gatewayModbusRuntimeProvider{})
					}
					if reflect.TypeOf(c.got) != expectedType {
						t.Fatalf("type %T, want %v", c.got, expectedType)
					}
					if c.got.(interface{ ModbusV1CoreAvailable() bool }).ModbusV1CoreAvailable() != (c.owners&1 != 0) {
						t.Fatal("core availability changed")
					}
					_, bms := c.got.(mcp.GrowattBMSRS485V202Provider)
					_, storage := c.got.(mcp.GrowattStorageSemanticProvider)
					_, evse := c.got.(mcp.TeslaGen3EVSECurrentLimitV1Provider)
					_, semanticEVSE := c.got.(mcp.TeslaGen3EVSESemanticProvider)
					if bms != (c.owners&6 != 0) || storage != bms || evse != (c.owners&4 != 0) || semanticEVSE != evse {
						t.Fatal("optional structural interfaces changed")
					}
					tools := []string{mcp.TeslaGen3EVSECurrentLimitV1GetTool, mcp.SemanticV1EVSECurrentGetTool, mcp.GrowattBMSRS485V202StatusGetTool, mcp.SemanticV1GrowattStorageCurrentGetTool, "unknown"}
					availability, ok := c.got.(interface{ ModbusV1OptionalToolAvailable(string) bool })
					if ok != (c.owners&4 != 0) {
						t.Fatal("optional availability interface changed")
					}
					if ok {
						for i, tool := range tools {
							want := i < 2 && c.owners&4 != 0 || i >= 2 && i < 4 && c.owners&2 != 0
							if availability.ModbusV1OptionalToolAvailable(tool) != want {
								t.Fatalf("tool %s", tool)
							}
						}
					}
					for _, iface := range []reflect.Type{
						reflect.TypeOf((*mcp.HuaweiSmartLoggerV1Provider)(nil)).Elem(),
						reflect.TypeOf((*mcp.HuaweiEMMAV1Provider)(nil)).Elem(),
						reflect.TypeOf((*mcp.HuaweiSDongleV1Provider)(nil)).Elem(),
						reflect.TypeOf((*mcp.GrowattProtocolIIV1Provider)(nil)).Elem(),
						reflect.TypeOf((*mcp.OutBackAXSV1Provider)(nil)).Elem(),
						reflect.TypeOf((*mcp.FroniusSunSpecV1Provider)(nil)).Elem(),
					} {
						if reflect.TypeOf(c.got).Implements(iface) {
							t.Fatalf("unexpected candidate interface %v", iface)
						}
					}
					hsc, err := c.got.(mcp.TeslaHSCV1Provider).TeslaHSCV1(context.Background())
					if err != nil || !reflect.DeepEqual(hsc, mcp.TeslaHSCV1Result{Disposition: "disabled", Compatibility: "unknown", OutboundAllowed: false}) {
						t.Fatalf("HSC: %+v %v", hsc, err)
					}
					result, err := c.got.SemanticPVCurrent(context.Background(), "non-sunspec", "synthetic")
					if err == nil || err.Error() != "semantic PV observation unavailable" || !reflect.DeepEqual(result, mcp.SemanticPVCurrentResult{}) {
						t.Fatalf("PV: %+v %v", result, err)
					}
				})
			}
		})
	}
}

func TestM02RegistryResolution(t *testing.T) {
	owners := modbusProviderOwners{tcp: &modbusadapter.Adapter{}, growatt: &growattBMSRS485ProductionProvider{}, tesla: &teslaHSCRetainedOwner{}}
	entries := modbusProviderEntries()
	registry, err := newModbusProviderRegistry(entries)
	if err != nil {
		t.Fatal(err)
	}
	if len(registry) != 8 {
		t.Fatalf("entries: %d", len(registry))
	}
	for _, entry := range entries {
		t.Run(string(entry.id), func(t *testing.T) {
			calls := 0
			original := entry.factory
			if original != nil {
				counted := registry[entry.id]
				counted.factory = func(core *gatewayModbusMCPProvider, o modbusProviderOwners) mcp.ModbusV1Provider {
					calls++
					return original(core, o)
				}
				registry[entry.id] = counted
			}
			factory, err := registry.resolve(entry.id, owners)
			if calls != 0 {
				t.Fatal("lookup invoked a factory")
			}
			if entry.state == modbusProviderWithheld {
				if original != nil || factory != nil || !errors.Is(err, errModbusProviderUnavailable) {
					t.Fatal("candidate obtained factory")
				}
				return
			}
			if err != nil || factory == nil {
				t.Fatalf("resolve: %v", err)
			}
			if provider := factory(&gatewayModbusMCPProvider{}, owners); provider == nil || calls != 1 {
				t.Fatalf("resolution count %d, provider %T", calls, provider)
			}
			if factory, err := registry.resolve(entry.id, modbusProviderOwners{}); factory != nil || !errors.Is(err, errModbusProviderUnavailable) {
				t.Fatal("absent owner resolved")
			}
			if calls != 1 {
				t.Fatal("absent lookup invoked factory")
			}
		})
	}
	// The only identity field is the entire explicit profile ID. Vary it for
	// every entry to catch fallback, prefix matching, or implicit promotion.
	for _, entry := range entries {
		for _, id := range []modbusProviderProfileID{"", entry.id + ".unknown"} {
			factory, err := registry.resolve(id, owners)
			if factory != nil || !errors.Is(err, errModbusProviderUnknown) {
				t.Fatalf("unknown %q resolved", id)
			}
		}
		duplicates := append(append([]modbusProviderEntry(nil), entries...), entry)
		rejected, err := newModbusProviderRegistry(duplicates)
		if rejected != nil || !errors.Is(err, errModbusProviderDuplicate) {
			t.Fatalf("duplicate %q accepted", entry.id)
		}
	}
}
