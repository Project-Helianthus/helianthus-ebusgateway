package main

import (
	"context"
	"expvar"
	"testing"
	"time"

	"github.com/d3vi1/helianthus-ebusreg/registry"
	"github.com/d3vi1/helianthus-ebusreg/vaillant/productids"
)

func TestObservability_ZonePresenceTransitionIncrementsCounter(t *testing.T) {
	instance := byte(0x01)
	poller := &vaillantSemanticPoller{
		zones:             make(map[byte]*vaillantZoneSnapshot),
		presence:          make(map[byte]*zonePresenceRecord),
		zoneMissThreshold: 3,
		zoneHitThreshold:  2,
	}

	key := "ABSENT->SUSPECT_RESURRECT"
	before := expvarMapInt64(semanticZonePresenceTransitionsTotal, key)

	poller.applyZonePresenceProbes(
		map[byte]bool{instance: true},
		map[byte]bool{instance: true},
	)

	after := expvarMapInt64(semanticZonePresenceTransitionsTotal, key)
	if after != before+1 {
		t.Fatalf("semanticZonePresenceTransitionsTotal[%q] = %d; want %d", key, after, before+1)
	}
}

func TestObservability_RegulatorTransitionIncrementsCounter(t *testing.T) {
	catalog, err := productids.LoadCatalog()
	if err != nil {
		t.Fatalf("LoadCatalog() error: %v", err)
	}

	now := time.Date(2026, time.February, 26, 12, 0, 0, 0, time.UTC)
	poller := &vaillantSemanticPoller{
		reg: newTestRegistry(
			registry.DeviceInfo{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV", SerialNumber: "21-22-09-0010002460-0082-005409-N4"},
			registry.DeviceInfo{Address: 0x60, Manufacturer: "Vaillant", DeviceID: "VRC430", SerialNumber: "21-22-09-0020028521-0082-005409-N4"},
		),
		catalog:               catalog,
		regAbsenceState:       regulatorPresent,
		regulatorCapability:   productids.ControllerPresent,
		registryDeviceCount:   2,
		regulatorAbsenceGrace: 5 * time.Minute,
		zones:                 make(map[byte]*vaillantZoneSnapshot),
		presence:              make(map[byte]*zonePresenceRecord),
		nowFn:                 func() time.Time { return now },
	}

	key := "PRESENT->ABSENCE_GRACE"
	before := expvarMapInt64(semanticRegulatorTransitionsTotal, key)

	poller.reg = newTestRegistry(
		registry.DeviceInfo{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV", SerialNumber: "21-22-09-0010002460-0082-005409-N4"},
	)
	poller.refreshRegulatorCapability(context.Background())

	after := expvarMapInt64(semanticRegulatorTransitionsTotal, key)
	if after != before+1 {
		t.Fatalf("semanticRegulatorTransitionsTotal[%q] = %d; want %d", key, after, before+1)
	}
	if got := semanticRegulatorState.Value(); got != string(regulatorAbsenceGrace) {
		t.Fatalf("semanticRegulatorState = %q; want %q", got, regulatorAbsenceGrace)
	}
}

func TestObservability_DHWExpiry_IncrementsCounter(t *testing.T) {
	now := time.Date(2026, time.February, 26, 12, 0, 0, 0, time.UTC)
	poller := &vaillantSemanticPoller{
		dhw:             &vaillantDhwSnapshot{OperatingMode: "auto"},
		dhwStaleTTL:     10 * time.Minute,
		dhwLastUpdateAt: now.Add(-11 * time.Minute),
		nowFn:           func() time.Time { return now },
	}

	before := semanticDHWStaleExpiryTotal.Value()
	expired := poller.expireDHWIfStaleLocked(semanticSnapshotSourceCache)
	if !expired {
		t.Fatal("expireDHWIfStaleLocked() = false; want true")
	}
	after := semanticDHWStaleExpiryTotal.Value()
	if after != before+1 {
		t.Fatalf("semanticDHWStaleExpiryTotal = %d; want %d", after, before+1)
	}
}

func expvarMapInt64(m *expvar.Map, key string) int64 {
	if m == nil {
		return 0
	}
	variable := m.Get(key)
	if variable == nil {
		return 0
	}
	counter, ok := variable.(*expvar.Int)
	if !ok {
		return 0
	}
	return counter.Value()
}
