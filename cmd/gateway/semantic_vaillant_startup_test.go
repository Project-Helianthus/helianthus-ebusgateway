package main

import (
	"context"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

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
	if circuits := provider.Circuits(); circuits == nil || len(circuits) != 0 {
		t.Fatalf("provider.Circuits() = %#v after startup priming; want empty non-null plane", circuits)
	}
	if mode := provider.FM5SemanticMode(); mode != graphql.Fm5SemanticModeAbsent {
		t.Fatalf("provider.FM5SemanticMode() = %q; want %q", mode, graphql.Fm5SemanticModeAbsent)
	}
}
