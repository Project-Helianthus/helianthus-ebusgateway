package main

import (
	"context"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/vaillant/systemconfig"
	"github.com/Project-Helianthus/helianthus-ebusreg/vaillant/productids"
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

// energyLedgerCall is one value applied to recordingEnergyLedger.
type energyLedgerCall struct {
	lifetime                      bool
	channel, usage, period, label string
	ref                           time.Time
	kwh                           float64
}

// recordingEnergyLedger is a native energy ledger whose reference instant
// advances by one hour on every read inside one month, so an acquisition
// that dates a window by the current instant instead of the captured one
// records a different ref.
type recordingEnergyLedger struct {
	now       time.Time
	calls     []energyLedgerCall
	reject    map[int]bool
	published int
}

func (l *recordingEnergyLedger) energyReferenceTime() time.Time {
	current := l.now
	l.now = l.now.Add(time.Hour)
	return current
}

func (l *recordingEnergyLedger) applyLifetimeEnergy(channel, usage string, kwh float64) bool {
	l.calls = append(l.calls, energyLedgerCall{lifetime: true, channel: channel, usage: usage, kwh: kwh})
	return !l.reject[len(l.calls)-1]
}

func (l *recordingEnergyLedger) applyWindowEnergy(channel, usage, period, label string, ref time.Time, kwh float64) bool {
	l.calls = append(l.calls, energyLedgerCall{channel: channel, usage: usage, period: period, label: label, ref: ref, kwh: kwh})
	return !l.reject[len(l.calls)-1]
}

func (l *recordingEnergyLedger) publishEnergyTotals() { l.published++ }

// TestAcquireEnergyAppliesNativeValuesToTheLedger drives one acquisition
// through a recording ledger and checks every native field of each applied
// value. Each field varies across the queries, and each check catches one
// production mutation:
//   - lifetime: a lifetime total applied as a window, or the reverse;
//   - channel and usage: the two arguments swapped or taken from another
//     query (every query pairs distinct channel and usage strings);
//   - period and label: the period passed as the label or the reverse, or
//     the current and previous windows exchanged;
//   - ref: a window dated by the current ledger instant instead of the
//     instant captured once at the start (the ledger clock moves per read);
//   - kwh: a reply applied to another query (every reply is distinct).
//
// A failed read applies nothing, a rejected value is not counted, and the
// totals are published once only when the ledger accepted a value.
func TestAcquireEnergyAppliesNativeValuesToTheLedger(t *testing.T) {
	start := time.Date(2030, time.April, 10, 8, 0, 0, 0, time.UTC)
	const failedQuery = 6

	ledger := &recordingEnergyLedger{now: start, reject: map[int]bool{2: true}}
	reads := 0
	acquireEnergy(ledger, func(q b524EnergyQuery, ref time.Time) (uint32, bool) {
		if ref != start {
			t.Fatalf("read %d reference = %v; want captured %v", reads, ref, start)
		}
		reads++
		return uint32(1000 + reads), reads-1 != failedQuery
	})
	if reads != len(b524EnergyQueries) {
		t.Fatalf("reads = %d; want %d", reads, len(b524EnergyQueries))
	}

	want := make([]energyLedgerCall, 0, len(b524EnergyQueries))
	for i, q := range b524EnergyQueries {
		if i == failedQuery {
			continue
		}
		call := energyLedgerCall{channel: q.channel, usage: q.usage, kwh: float64(1001 + i)}
		if q.period == "all" {
			call.lifetime = true
		} else {
			call.period, call.label, call.ref = q.period, q.yearKind, start
		}
		want = append(want, call)
	}
	if len(ledger.calls) != len(want) {
		t.Fatalf("ledger calls = %d; want %d", len(ledger.calls), len(want))
	}
	for i, got := range ledger.calls {
		if got != want[i] {
			t.Errorf("ledger call %d = %+v; want %+v", i, got, want[i])
		}
	}
	if ledger.published != 1 {
		t.Fatalf("totals published %d times; want once", ledger.published)
	}

	rejecting := &recordingEnergyLedger{now: start, reject: map[int]bool{}}
	for i := range b524EnergyQueries {
		rejecting.reject[i] = true
	}
	acquireEnergy(rejecting, func(b524EnergyQuery, time.Time) (uint32, bool) { return 1, true })
	if rejecting.published != 0 {
		t.Fatalf("totals published %d times without an accepted value", rejecting.published)
	}
}

// TestVaillantPublicationEnergyLedgerKeysProviderValues checks that the
// owner applies a lifetime total under the undated key and a window value
// under the key dated against the given instant, not the provider clock.
func TestVaillantPublicationEnergyLedgerKeysProviderValues(t *testing.T) {
	clock := time.Date(2030, time.June, 15, 12, 0, 0, 0, time.UTC)
	provider := graphql.NewLiveSemanticProvider()
	provider.SetEnergyClock(func() time.Time { return clock })
	poller := &vaillantSemanticPoller{publication: newVaillantSemanticPublication(provider, nil, nil)}
	ledger := poller.energyLedger()
	if ledger == nil {
		t.Fatal("energy ledger = nil with a provider")
	}
	if got := ledger.energyReferenceTime(); got != clock {
		t.Fatalf("reference time = %v; want provider clock %v", got, clock)
	}
	if !ledger.applyLifetimeEnergy("gas", "hot_water", 321) {
		t.Fatal("lifetime value rejected")
	}
	ref := time.Date(2030, time.May, 20, 9, 0, 0, 0, time.UTC)
	if !ledger.applyWindowEnergy("electricity", "climate", "month", "current", ref, 45) {
		t.Fatal("window value rejected")
	}
	totals := provider.EnergyTotals()
	if totals == nil {
		t.Fatal("missing totals")
	}
	if got := totals.Gas.DHW.Yearly; len(got) != 2 || got[1] != 321 {
		t.Fatalf("gas hot water yearly = %v; want lifetime 321 in the current slot", got)
	}
	// The current month of ref is May, the previous month of the June
	// provider clock; dated by the provider clock it would be June.
	if got := totals.Electric.Climate.Monthly; len(got) != 2 || got[0] != 45 || got[1] != 0 {
		t.Fatalf("electricity climate monthly = %v; want [45 0]", got)
	}

	for _, empty := range []*vaillantSemanticPoller{nil, {}, {publication: newVaillantSemanticPublication(nil, nil, nil)}} {
		if empty.energyLedger() != nil {
			t.Fatalf("energy ledger of %+v is not nil", empty)
		}
	}
}

func receivedBoilerUpdate(t *testing.T, ch chan interface{}) *graphql.BoilerStatus {
	t.Helper()
	select {
	case raw := <-ch:
		status, ok := raw.(*graphql.BoilerStatus)
		if !ok {
			t.Fatalf("boiler payload type = %T", raw)
		}
		return status
	default:
		return nil
	}
}

// TestVaillantPublicationOwnerPublishesAndWithdrawsBoilerStatus drives the
// boiler owner methods without a poller: a snapshot publishes a detached
// status once per change, the thermal withdrawal clears only the flow
// temperature, and a missing snapshot withdraws a published status without
// reporting a publication.
func TestVaillantPublicationOwnerPublishesAndWithdrawsBoilerStatus(t *testing.T) {
	provider := graphql.NewLiveSemanticProvider()
	hub := graphql.NewBroadcastHub(nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	updates, err := hub.SubscribeBoiler(ctx)
	if err != nil {
		t.Fatalf("SubscribeBoiler: %v", err)
	}
	owner := newVaillantSemanticPublication(provider, hub, nil)

	if owner.publishBoilerStatusView(nil, semanticSnapshotSourceLive) {
		t.Fatal("missing boiler snapshot reported a publication")
	}
	if provider.BoilerStatus() != nil || receivedBoilerUpdate(t, updates) != nil {
		t.Fatal("missing boiler snapshot published a status")
	}

	flow, pressure := 54.5, 1.6
	phone := "0123"
	mode := 2
	snapshot := &vaillantBoilerSnapshot{FlowTemperatureC: &flow, WaterPressureBar: &pressure, PhoneNumber: &phone, DhwOperatingMode: &mode}
	if !owner.publishBoilerStatusView(snapshot, semanticSnapshotSourceLive) {
		t.Fatal("boiler snapshot not published")
	}
	phone = "9999"
	status := provider.BoilerStatus()
	if status == nil || status.State.FlowTemperatureC == nil || *status.State.FlowTemperatureC != flow || status.State.WaterPressureBar == nil || *status.State.WaterPressureBar != pressure {
		t.Fatalf("boiler state = %#v", status)
	}
	if status.Config.PhoneNumber == nil || *status.Config.PhoneNumber != "0123" {
		t.Fatalf("phone number = %v; want detached 0123", status.Config.PhoneNumber)
	}
	if status.Config.DhwOperatingMode == nil || *status.Config.DhwOperatingMode != decodeDhwMode(mode) {
		t.Fatalf("hot water mode = %v; want %q", status.Config.DhwOperatingMode, decodeDhwMode(mode))
	}
	if receivedBoilerUpdate(t, updates) == nil {
		t.Fatal("first boiler publication pushed no update")
	}
	phone = "0123"
	if !owner.publishBoilerStatusView(snapshot, semanticSnapshotSourceLive) || receivedBoilerUpdate(t, updates) != nil {
		t.Fatal("unchanged boiler status pushed an update")
	}

	owner.withdrawBoilerFlowTemperature()
	status = provider.BoilerStatus()
	if status == nil || status.State.FlowTemperatureC != nil || status.State.WaterPressureBar == nil || *status.State.WaterPressureBar != pressure {
		t.Fatalf("boiler state after withdrawal = %#v; want only flow cleared", status)
	}
	if pushed := receivedBoilerUpdate(t, updates); pushed == nil || pushed.State.FlowTemperatureC != nil {
		t.Fatalf("withdrawal update = %#v; want a status without flow", pushed)
	}
	owner.withdrawBoilerFlowTemperature()
	if receivedBoilerUpdate(t, updates) != nil {
		t.Fatal("withdrawal without a published flow pushed an update")
	}

	if owner.publishBoilerStatusView(nil, semanticSnapshotSourceCache) {
		t.Fatal("missing cache snapshot reported a publication")
	}
	if provider.BoilerStatus() != nil {
		t.Fatal("missing snapshot left the published boiler status")
	}

	var absent *vaillantSemanticPublication
	absent.withdrawBoilerFlowTemperature()
	absent.publishRegulatorCapability(productids.ControllerPresent)
	newVaillantSemanticPublication(nil, nil, nil).withdrawBoilerFlowTemperature()
}

// TestVaillantPublicationOwnerPublishesSystemView checks that the system view
// publishes its detached snapshot with the bound it carries, and that a view
// without a snapshot withdraws the status without reporting a publication.
func TestVaillantPublicationOwnerPublishesSystemView(t *testing.T) {
	provider := graphql.NewLiveSemanticProvider()
	owner := newVaillantSemanticPublication(provider, nil, nil)

	outdoor := 7.5
	scheme := uint16(3)
	bound := systemconfig.ConfigBound{MinC: -10, MaxC: 20, Source: "profile", Profile: "synthetic", ControllerAddress: 0x15}
	view := vaillantSystemView{snapshot: &vaillantSystemSnapshot{OutdoorTemperature: &outdoor, SystemScheme: &scheme}, dhwBivalenceBound: bound}
	if !owner.publishSystemView(view, semanticSnapshotSourceLive) {
		t.Fatal("system view not published")
	}
	outdoor = 1
	view.dhwBivalenceBound.MaxC = 30
	status := provider.System()
	if status == nil || status.State.OutdoorTemperature == nil || *status.State.OutdoorTemperature != 7.5 {
		t.Fatalf("system state = %#v; want detached outdoor 7.5", status)
	}
	if status.Properties.SystemScheme == nil || *status.Properties.SystemScheme != 3 {
		t.Fatalf("system scheme = %v; want 3", status.Properties.SystemScheme)
	}
	if status.Config.DhwBivalenceBound == nil || *status.Config.DhwBivalenceBound != bound {
		t.Fatalf("bound = %#v; want %#v", status.Config.DhwBivalenceBound, bound)
	}

	if owner.publishSystemView(vaillantSystemView{}, semanticSnapshotSourceCache) {
		t.Fatal("system view without a snapshot reported a publication")
	}
	if provider.System() != nil {
		t.Fatal("system view without a snapshot left the published status")
	}
}
