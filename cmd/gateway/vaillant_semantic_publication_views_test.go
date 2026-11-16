package main

import (
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
)

// TestVaillantZoneViewsDeriveRadioStateOnlyForMissingZoneValues pins the
// native zone views: they come in instance order, they carry the radio room
// state and the inferred room mapping only for a zone that lacks its own
// values, and they stay detached from the retained snapshots.
func TestVaillantZoneViewsDeriveRadioStateOnlyForMissingZoneValues(t *testing.T) {
	connected := true
	zoneAssignment := uint8(2)
	radioTemp := 20.5
	radioHumidity := 44.0
	ownTemp := 22.0
	ownHumidity := 51.0
	ownMapping := uint16(0)
	poller := &vaillantSemanticPoller{
		radioDevices: map[radioDeviceKey]*vaillantRadioDeviceSnapshot{
			{Group: remoteThermostats.group, Instance: 0x02}: {
				Group:            remoteThermostats.group,
				Instance:         0x02,
				DeviceConnected:  &connected,
				ZoneAssignment:   &zoneAssignment,
				RoomTemperatureC: &radioTemp,
				RoomHumidityPct:  &radioHumidity,
			},
		},
		zones: map[byte]*vaillantZoneSnapshot{
			0x01: {Instance: 0x01, Present: true, Name: "Upstairs", AllowedModes: []string{"auto"}},
			0x00: {
				Instance:     0x00,
				Present:      true,
				CurrentTempC: &ownTemp,
				HumidityPct:  &ownHumidity,
				ConfigurationRoomTemperatureZoneMappingRaw: &ownMapping,
			},
		},
	}

	poller.mu.Lock()
	views := poller.zoneViewsLocked()
	poller.mu.Unlock()

	if len(views) != 2 || views[0].instance != 0x00 || views[1].instance != 0x01 {
		t.Fatalf("zone views = %#v; want instances 0 and 1 in order", views)
	}
	if views[0].radioRoomTempC != nil || views[0].radioRoomHumidityPct != nil || views[0].inferredRoomMappingRaw != nil {
		t.Fatalf("zone with its own values derived radio state %#v", views[0])
	}
	derived := views[1]
	wantMapping := uint16(0x02) + 1
	if derived.inferredRoomMappingRaw == nil || *derived.inferredRoomMappingRaw != wantMapping {
		t.Fatalf("inferred room mapping = %v; want %d", derived.inferredRoomMappingRaw, wantMapping)
	}
	if derived.radioRoomTempC == nil || *derived.radioRoomTempC != radioTemp || derived.radioRoomHumidityPct == nil || *derived.radioRoomHumidityPct != radioHumidity {
		t.Fatalf("radio room state = %v/%v; want %v/%v", derived.radioRoomTempC, derived.radioRoomHumidityPct, radioTemp, radioHumidity)
	}

	poller.zones[0x01].Name = "Renamed"
	poller.zones[0x01].AllowedModes[0] = "off"
	*poller.zones[0x00].CurrentTempC = 30
	radioTemp = 10
	if derived.snapshot.Name != "Upstairs" || derived.snapshot.AllowedModes[0] != "auto" {
		t.Fatalf("zone view follows the retained snapshot: %#v", derived.snapshot)
	}
	if *views[0].snapshot.CurrentTempC != 22.0 || *derived.radioRoomTempC != 20.5 {
		t.Fatal("zone view shares storage with the retained snapshots")
	}

	zone := zoneFromView(derived)
	if zone.ID != "zone-2" || zone.Name != "Upstairs" {
		t.Fatalf("zone identity = %q/%q; want zone-2/Upstairs", zone.ID, zone.Name)
	}
	if zone.State.CurrentTempC == nil || *zone.State.CurrentTempC != 20.5 || zone.State.CurrentHumidityPct == nil || *zone.State.CurrentHumidityPct != radioHumidity {
		t.Fatalf("zone state = %#v; want radio room fallback", zone.State)
	}
	wantRoomMapping := decodeRoomTemperatureZoneMapping(&wantMapping)
	if zone.Config.RoomTemperatureZoneMapping == nil || wantRoomMapping == nil || *zone.Config.RoomTemperatureZoneMapping != *wantRoomMapping {
		t.Fatalf("room mapping = %v; want %v", zone.Config.RoomTemperatureZoneMapping, wantRoomMapping)
	}
	associated := uint16(resolveAssociatedCircuitInstance(&wantMapping, 0x01))
	wantAssociated := decodeAssociatedCircuit(&associated)
	if zone.Config.AssociatedCircuit == nil || wantAssociated == nil || *zone.Config.AssociatedCircuit != *wantAssociated {
		t.Fatalf("associated circuit = %v; want %v", zone.Config.AssociatedCircuit, wantAssociated)
	}

	own := zoneFromView(views[0])
	if own.Name != "Zone 1" || own.State.CurrentTempC == nil || *own.State.CurrentTempC != 22.0 {
		t.Fatalf("zone with its own values = %#v", own)
	}
}

// TestVaillantPublicationOwnerPublishesCircuitAndDhwViews drives the owner
// methods without a poller: a circuit view publishes one detached managing
// device per circuit, empty cache views never replace published values, and
// a missing hot water snapshot withdraws the published status only when the
// publication is live or expired the snapshot.
func TestVaillantPublicationOwnerPublishesCircuitAndDhwViews(t *testing.T) {
	provider := graphql.NewLiveSemanticProvider()
	owner := newVaillantSemanticPublication(provider, nil, nil)

	if owner.publishCircuitsView(vaillantCircuitsView{}, semanticSnapshotSourceLive) {
		t.Fatal("empty circuit view published without a published list")
	}
	circuitType := uint16(1)
	view := vaillantCircuitsView{
		circuits: []*vaillantCircuitSnapshot{
			{Instance: 0x00, Active: true, CircuitTypeRaw: &circuitType},
			{Instance: 0x01, Active: true, CircuitTypeRaw: &circuitType},
		},
		managedByFunctionModule: true,
	}
	if !owner.publishCircuitsView(view, semanticSnapshotSourceLive) {
		t.Fatal("circuit view not published")
	}
	circuits := provider.Circuits()
	if len(circuits) != 2 || circuits[0].Index != 0 || circuits[1].Index != 1 {
		t.Fatalf("circuits = %#v; want indexes 0 and 1", circuits)
	}
	for _, circuit := range circuits {
		if circuit.ManagingDevice.Role != graphql.ManagingDeviceRoleFunctionModule {
			t.Fatalf("managing device = %#v; want function module", circuit.ManagingDevice)
		}
	}
	if circuits[0].ManagingDevice.DeviceID == circuits[1].ManagingDevice.DeviceID {
		t.Fatal("circuits share one managing device identifier")
	}
	if owner.publishCircuitsView(vaillantCircuitsView{}, semanticSnapshotSourceCache) {
		t.Fatal("empty cache circuit view replaced published circuits")
	}
	if got := provider.Circuits(); len(got) != 2 {
		t.Fatalf("circuits after empty cache view = %#v", got)
	}

	temp := 48.5
	if !owner.publishDhwView(vaillantDhwView{snapshot: &vaillantDhwSnapshot{CurrentTempC: &temp, OperatingMode: "auto"}}, semanticSnapshotSourceLive) {
		t.Fatal("hot water view not published")
	}
	if dhw := provider.DHW(); dhw == nil || dhw.State.CurrentTempC == nil || *dhw.State.CurrentTempC != temp || dhw.Config.OperatingMode != "auto" {
		t.Fatalf("hot water = %#v", dhw)
	}
	if owner.publishDhwView(vaillantDhwView{}, semanticSnapshotSourceCache) {
		t.Fatal("cache view without a snapshot withdrew unexpired hot water")
	}
	if provider.DHW() == nil {
		t.Fatal("hot water withdrawn by a cache view without expiry")
	}
	if !owner.publishDhwView(vaillantDhwView{expired: true}, semanticSnapshotSourceCache) {
		t.Fatal("expired hot water view not published")
	}
	if dhw := provider.DHW(); dhw != nil {
		t.Fatalf("hot water after expiry = %#v; want withdrawn", dhw)
	}
}
