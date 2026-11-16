package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
)

func TestSemanticCacheStoreLoad_MissLogsMarker(t *testing.T) {
	logs := &semanticCacheTestLogs{}
	path := filepath.Join(t.TempDir(), "semantic-cache.json")
	store := newSemanticCacheStore(path, logs.Printf)

	if _, ok := store.Load(); ok {
		t.Fatalf("Load() ok = true; want false for cache miss")
	}
	logs.requireContains(t, "semantic_cache_miss")
}

func TestSemanticCacheStoreLoad_HitLogsMarker(t *testing.T) {
	logs := &semanticCacheTestLogs{}
	path := filepath.Join(t.TempDir(), "semantic-cache.json")
	store := newSemanticCacheStore(path, logs.Printf)

	targetTemp := 22.5
	if err := store.Save(semanticCacheSnapshot{
		Zones: []graphql.Zone{
			{ID: "zone-1", Name: "Zone 1", Config: graphql.ZoneConfig{TargetTempC: &targetTemp}},
		},
	}); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	logs.Reset()
	snapshot, ok := store.Load()
	if !ok {
		t.Fatalf("Load() ok = false; want true")
	}
	if len(snapshot.Zones) != 1 {
		t.Fatalf("len(snapshot.Zones) = %d; want 1", len(snapshot.Zones))
	}
	if snapshot.Zones[0].ID != "zone-1" {
		t.Fatalf("snapshot.Zones[0].ID = %q; want zone-1", snapshot.Zones[0].ID)
	}
	logs.requireContains(t, "semantic_cache_hit")
}

func TestSemanticCacheStoreLoad_InvalidJSONLogsMarker(t *testing.T) {
	logs := &semanticCacheTestLogs{}
	path := filepath.Join(t.TempDir(), "semantic-cache.json")
	if err := os.WriteFile(path, []byte("{not-json"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	store := newSemanticCacheStore(path, logs.Printf)
	if _, ok := store.Load(); ok {
		t.Fatalf("Load() ok = true; want false for invalid payload")
	}
	logs.requireContains(t, "semantic_cache_invalid")
}

func TestSemanticCacheStoreLoad_UnknownSchemaVersionLogsInvalid(t *testing.T) {
	logs := &semanticCacheTestLogs{}
	path := filepath.Join(t.TempDir(), "semantic-cache.json")
	if err := os.WriteFile(path, []byte(`{"schema_version":99,"zones":[]}`), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	store := newSemanticCacheStore(path, logs.Printf)
	if _, ok := store.Load(); ok {
		t.Fatalf("Load() ok = true; want false for unknown schema")
	}
	logs.requireContains(t, "semantic_cache_invalid")
	logs.requireContains(t, "unknown_schema_version")
}

func TestSemanticCacheStoreLoad_DiscardsV1(t *testing.T) {
	logs := &semanticCacheTestLogs{}
	path := filepath.Join(t.TempDir(), "semantic-cache.json")
	legacyV1 := `{
  "zones": [
    {"id":"zone-2","name":"Zone 2"},
    {"id":"zone-1","name":"Zone 1","allowed_modes":["heat","auto"]}
  ],
  "dhw": {"operating_mode":"auto","preset":"schedule"}
}`
	if err := os.WriteFile(path, []byte(legacyV1), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	store := newSemanticCacheStore(path, logs.Printf)
	if _, ok := store.Load(); ok {
		t.Fatalf("Load() ok = true; want false for discarded V1 cache")
	}
	logs.requireContains(t, "semantic_cache_discarded")
}

func TestSemanticCacheStoreLoad_DiscardsV2(t *testing.T) {
	logs := &semanticCacheTestLogs{}
	path := filepath.Join(t.TempDir(), "semantic-cache.json")
	legacyV2 := `{"schema_version":2,"metadata":{},"zones":[{"id":"zone-1","name":"Zone 1"}]}`
	if err := os.WriteFile(path, []byte(legacyV2), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	store := newSemanticCacheStore(path, logs.Printf)
	if _, ok := store.Load(); ok {
		t.Fatalf("Load() ok = true; want false for discarded V2 cache")
	}
	logs.requireContains(t, "semantic_cache_discarded")
}

func TestSemanticCacheStoreSave_SortsZonesAndAllowedModes(t *testing.T) {
	logs := &semanticCacheTestLogs{}
	path := filepath.Join(t.TempDir(), "semantic-cache.json")
	store := newSemanticCacheStore(path, logs.Printf)

	if err := store.Save(semanticCacheSnapshot{
		Zones: []graphql.Zone{
			{ID: "zone-10", Name: "Zone 10", Config: graphql.ZoneConfig{AllowedModes: []string{"heat", "off"}}},
			{ID: "zone-2", Name: "Zone 2", Config: graphql.ZoneConfig{AllowedModes: []string{"off", "heat"}}},
			{ID: "zone-1", Name: "Zone 1", Config: graphql.ZoneConfig{AllowedModes: []string{"heat", "auto"}}},
		},
	}); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	var persisted semanticCacheV3
	if err := json.Unmarshal(payload, &persisted); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if len(persisted.Zones) != 3 {
		t.Fatalf("len(persisted.Zones) = %d; want 3", len(persisted.Zones))
	}
	if persisted.Zones[0].ID != "zone-1" || persisted.Zones[1].ID != "zone-2" || persisted.Zones[2].ID != "zone-10" {
		t.Fatalf("zone order = [%s %s %s]; want [zone-1 zone-2 zone-10]", persisted.Zones[0].ID, persisted.Zones[1].ID, persisted.Zones[2].ID)
	}
	if got := strings.Join(persisted.Zones[0].Config.AllowedModes, ","); got != "auto,heat" {
		t.Fatalf("allowed_modes zone-1 = %q; want auto,heat", got)
	}
	if got := strings.Join(persisted.Zones[1].Config.AllowedModes, ","); got != "heat,off" {
		t.Fatalf("allowed_modes zone-2 = %q; want heat,off", got)
	}
	if got := strings.Join(persisted.Zones[2].Config.AllowedModes, ","); got != "heat,off" {
		t.Fatalf("allowed_modes zone-10 = %q; want heat,off", got)
	}
}

func TestSemanticCacheStoreSave_WritePermissionErrorFailsSafe(t *testing.T) {
	logs := &semanticCacheTestLogs{}
	baseDir := t.TempDir()
	readonlyDir := filepath.Join(baseDir, "readonly")
	if err := os.Mkdir(readonlyDir, 0o755); err != nil {
		t.Fatalf("Mkdir() error = %v", err)
	}
	if err := os.Chmod(readonlyDir, 0o500); err != nil {
		t.Fatalf("Chmod() error = %v", err)
	}
	defer func() {
		_ = os.Chmod(readonlyDir, 0o755)
	}()

	path := filepath.Join(readonlyDir, "semantic-cache.json")
	store := newSemanticCacheStore(path, logs.Printf)
	err := store.Save(semanticCacheSnapshot{
		Zones: []graphql.Zone{{ID: "zone-1", Name: "Zone 1"}},
	})
	if err == nil {
		t.Fatalf("Save() error = nil; want permission/write failure")
	}
	logs.requireContains(t, "semantic_cache_write_error")
}

func TestSemanticCacheStoreSave_WriteFailureKeepsExistingFile(t *testing.T) {
	logs := &semanticCacheTestLogs{}
	baseDir := t.TempDir()
	path := filepath.Join(baseDir, "semantic-cache.json")
	original := []byte(`{"schema_version":2,"metadata":{"persisted_at":"2026-01-01T00:00:00Z"},"zones":[{"id":"zone-old","name":"Old"}]}` + "\n")
	if err := os.WriteFile(path, original, 0o644); err != nil {
		t.Fatalf("WriteFile(original) error = %v", err)
	}

	if err := os.Chmod(baseDir, 0o500); err != nil {
		t.Fatalf("Chmod(baseDir) error = %v", err)
	}
	defer func() {
		_ = os.Chmod(baseDir, 0o755)
	}()

	store := newSemanticCacheStore(path, logs.Printf)
	err := store.Save(semanticCacheSnapshot{
		Zones: []graphql.Zone{{ID: "zone-1", Name: "Zone 1"}},
	})
	if err == nil {
		t.Fatalf("Save() error = nil; want failure when target dir is readonly")
	}

	if err := os.Chmod(baseDir, 0o755); err != nil {
		t.Fatalf("Chmod(baseDir restore) error = %v", err)
	}
	current, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(current) error = %v", err)
	}
	if string(current) != string(original) {
		t.Fatalf("cache file changed on failed write; got %q want %q", string(current), string(original))
	}
	logs.requireContains(t, "semantic_cache_write_error")
}

func TestPreloadSemanticCache_AppliesSnapshotAsStale(t *testing.T) {
	logs := &semanticCacheTestLogs{}
	path := filepath.Join(t.TempDir(), "semantic-cache.json")
	store := newSemanticCacheStore(path, logs.Printf)

	current := 49.0
	if err := store.Save(semanticCacheSnapshot{
		Zones: []graphql.Zone{{ID: "zone-1", Name: "Zone 1", Config: graphql.ZoneConfig{OperatingMode: "heat"}}},
		DHW:   &graphql.DhwStatus{Config: graphql.DhwConfig{OperatingMode: "auto"}, State: graphql.DhwState{CurrentTempC: &current}},
	}); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	provider := graphql.NewLiveSemanticProvider()
	_, _ = preloadSemanticCache(provider, store)

	if got := provider.StartupPhase(); got != graphql.SemanticStartupPhaseCacheLoadedStale {
		t.Fatalf("StartupPhase() = %s; want %s", got, graphql.SemanticStartupPhaseCacheLoadedStale)
	}
	if zones := provider.Zones(); len(zones) != 1 || zones[0].ID != "zone-1" {
		t.Fatalf("provider.Zones() = %#v; want zone-1", zones)
	}
	if dhw := provider.DHW(); dhw == nil || dhw.Config.OperatingMode != "auto" {
		t.Fatalf("provider.DHW() = %#v; want non-nil auto", dhw)
	}
}

func TestVaillantSemanticPoller_PersistsOnlyOnLivePublish(t *testing.T) {
	cacheSpy := &semanticCachePersisterSpy{}
	provider := graphql.NewLiveSemanticProvider()
	current := 21.5
	target := 22.0
	poller := &vaillantSemanticPoller{
		publication: newVaillantSemanticPublication(provider, nil, cacheSpy),
		zones: map[byte]*vaillantZoneSnapshot{
			0x00: {
				Instance:      0x00,
				Name:          "Zone 1",
				OperatingMode: "heat",
				Preset:        "manual",
				CurrentTempC:  &current,
				TargetTempC:   &target,
			},
		},
		dhw: &vaillantDhwSnapshot{
			OperatingMode: "auto",
			Preset:        "schedule",
			CurrentTempC:  &current,
			TargetTempC:   &target,
		},
	}

	poller.publishZones(semanticSnapshotSourceCache)
	poller.publishDHW(semanticSnapshotSourceCache)
	if cacheSpy.calls != 0 {
		t.Fatalf("cache save calls after cache publish = %d; want 0", cacheSpy.calls)
	}

	poller.publishZones(semanticSnapshotSourceLive)
	if cacheSpy.calls != 1 {
		t.Fatalf("cache save calls after zones live publish = %d; want 1", cacheSpy.calls)
	}
	poller.publishDHW(semanticSnapshotSourceLive)
	if cacheSpy.calls != 2 {
		t.Fatalf("cache save calls after dhw live publish = %d; want 2", cacheSpy.calls)
	}
}

func TestVaillantSemanticPoller_CachePublishKeepsPreloadedSnapshotWhenEmpty(t *testing.T) {
	provider := graphql.NewLiveSemanticProvider()
	current := 48.2
	target := 50.0
	preloadedZones := []graphql.Zone{
		{
			ID:   "zone-1",
			Name: "Living Room",
			Config: graphql.ZoneConfig{
				OperatingMode: "heat",
				Preset:        "manual",
			},
		},
	}
	preloadedDHW := &graphql.DhwStatus{
		Config: graphql.DhwConfig{
			OperatingMode: "auto",
			Preset:        "schedule",
			TargetTempC:   &target,
		},
		State: graphql.DhwState{
			CurrentTempC: &current,
		},
	}
	provider.SetZonesFromCache(preloadedZones)
	provider.SetDHWFromCache(preloadedDHW)

	poller := &vaillantSemanticPoller{
		publication: newVaillantSemanticPublication(provider, nil, &semanticCachePersisterSpy{}),
		zones:       map[byte]*vaillantZoneSnapshot{},
		dhw:         nil,
	}

	poller.publishZones(semanticSnapshotSourceCache)
	poller.publishDHW(semanticSnapshotSourceCache)

	zones := provider.Zones()
	if len(zones) != 1 || zones[0].ID != "zone-1" {
		t.Fatalf("provider.Zones() = %#v; want preloaded zone-1 preserved", zones)
	}
	dhw := provider.DHW()
	if dhw == nil || dhw.Config.OperatingMode != "auto" {
		t.Fatalf("provider.DHW() = %#v; want preloaded DHW preserved", dhw)
	}
}

func TestVaillantSemanticPoller_HydrateFromCache_SeedsInternalState(t *testing.T) {
	current := 21.3
	target := 22.0
	dhwCurrent := 48.2
	dhwTarget := 50.0
	assocCircuit := 1
	roomTemperatureZoneMapping := 2
	snapshot := semanticCacheSnapshot{
		Zones: []graphql.Zone{
			{
				ID:   "zone-2",
				Name: "Etaj",
				State: graphql.ZoneState{
					CurrentTempC: &current,
				},
				Config: graphql.ZoneConfig{
					OperatingMode:              "heat",
					Preset:                     "manual",
					AllowedModes:               []string{"off", "auto", "heat"},
					TargetTempC:                &target,
					CircuitType:                "underfloor",
					AssociatedCircuit:          &assocCircuit,
					RoomTemperatureZoneMapping: &roomTemperatureZoneMapping,
				},
			},
		},
		DHW: &graphql.DhwStatus{
			Config: graphql.DhwConfig{
				OperatingMode: "auto",
				Preset:        "schedule",
				TargetTempC:   &dhwTarget,
			},
			State: graphql.DhwState{
				CurrentTempC: &dhwCurrent,
			},
		},
	}

	poller := &vaillantSemanticPoller{
		zones: make(map[byte]*vaillantZoneSnapshot),
	}
	poller.hydrateFromCache(cacheHydrationFromSnapshot(snapshot))

	zone := poller.zones[0x01]
	if zone == nil {
		t.Fatalf("poller.zones[0x01] = nil; want hydrated zone state")
	}
	if zone.Name != "Etaj" || zone.OperatingMode != "heat" || zone.Preset != "manual" {
		t.Fatalf("hydrated zone = %#v; unexpected core fields", zone)
	}
	if zone.ConfigurationAssociatedCircuitRaw == nil || *zone.ConfigurationAssociatedCircuitRaw != 1 {
		t.Fatalf("hydrated zone circuit index = %#v; want 1", zone.ConfigurationAssociatedCircuitRaw)
	}
	if zone.ConfigurationRoomTemperatureZoneMappingRaw == nil || *zone.ConfigurationRoomTemperatureZoneMappingRaw != 2 {
		t.Fatalf("hydrated zone room mapping = %#v; want 2", zone.ConfigurationRoomTemperatureZoneMappingRaw)
	}
	if poller.dhw == nil || poller.dhw.OperatingMode != "auto" || poller.dhw.Preset != "schedule" {
		t.Fatalf("poller.dhw = %#v; want hydrated dhw state", poller.dhw)
	}
}

type semanticCachePersisterSpy struct {
	calls int
}

func (spy *semanticCachePersisterSpy) Save(snapshot semanticCacheSnapshot) error {
	spy.calls++
	return nil
}

type semanticCacheTestLogs struct {
	entries []string
}

func (logs *semanticCacheTestLogs) Printf(format string, args ...any) {
	logs.entries = append(logs.entries, fmt.Sprintf(format, args...))
}

func (logs *semanticCacheTestLogs) Reset() {
	logs.entries = nil
}

func (logs *semanticCacheTestLogs) requireContains(t *testing.T, fragment string) {
	t.Helper()
	for _, entry := range logs.entries {
		if strings.Contains(entry, fragment) {
			return
		}
	}
	t.Fatalf("logs do not contain %q; logs=%v", fragment, logs.entries)
}

// TestPersistSemanticSnapshotLaysRetainedZoneValuesOverPublishedZones pins
// the cache persistence path: the poller hands the retained zone values to
// the publication owner, which saves the published zones with them. Each
// overlay field differs between the published and the retained zone, so a
// swapped, dropped or published-sourced field fails: current temperature,
// humidity, room temperature mapping and associated circuit. The overlay is
// keyed by the instance the zone identifier names: the retained zone at
// instance 1 overlays "zone-2" only, and "zone-1" is saved as published.
func TestPersistSemanticSnapshotLaysRetainedZoneValuesOverPublishedZones(t *testing.T) {
	publishedTemp, publishedHumidity := 18.0, 35.0
	publishedMapping, publishedCircuit := 7, 6
	published := func(id string) graphql.Zone {
		temp, humidity := publishedTemp, publishedHumidity
		mapping, circuit := publishedMapping, publishedCircuit
		return graphql.Zone{
			ID:     id,
			Name:   id,
			State:  graphql.ZoneState{CurrentTempC: &temp, CurrentHumidityPct: &humidity},
			Config: graphql.ZoneConfig{OperatingMode: "auto", RoomTemperatureZoneMapping: &mapping, AssociatedCircuit: &circuit},
		}
	}
	provider := graphql.NewLiveSemanticProvider()
	provider.SetZonesFromCache([]graphql.Zone{published("zone-1"), published("zone-2")})

	retainedTemp, retainedHumidity := 21.5, 48.0
	retainedMapping, retainedCircuit := uint16(2), uint16(1)
	cache := &semanticSnapshotCaptureSpy{}
	poller := &vaillantSemanticPoller{
		publication: newVaillantSemanticPublication(provider, nil, cache),
		zones: map[byte]*vaillantZoneSnapshot{
			0x01: {
				Instance:     0x01,
				CurrentTempC: &retainedTemp,
				HumidityPct:  &retainedHumidity,
				ConfigurationRoomTemperatureZoneMappingRaw: &retainedMapping,
				ConfigurationAssociatedCircuitRaw:          &retainedCircuit,
			},
		},
	}
	poller.persistSemanticSnapshot()

	if cache.calls != 1 || len(cache.last.Zones) != 2 {
		t.Fatalf("saves=%d zones=%d; want one save with two zones", cache.calls, len(cache.last.Zones))
	}
	untouched, overlaid := cache.last.Zones[0], cache.last.Zones[1]
	if untouched.ID != "zone-1" || overlaid.ID != "zone-2" {
		t.Fatalf("saved zone order = %q, %q; want published order", untouched.ID, overlaid.ID)
	}
	if *untouched.State.CurrentTempC != publishedTemp || *untouched.State.CurrentHumidityPct != publishedHumidity ||
		*untouched.Config.RoomTemperatureZoneMapping != publishedMapping || *untouched.Config.AssociatedCircuit != publishedCircuit {
		t.Fatalf("zone without a retained entry changed: %+v", untouched)
	}
	if got := overlaid.State.CurrentTempC; got == nil || *got != retainedTemp {
		t.Fatalf("saved current temperature = %v; want retained %.1f", got, retainedTemp)
	}
	if got := overlaid.State.CurrentHumidityPct; got == nil || *got != retainedHumidity {
		t.Fatalf("saved humidity = %v; want retained %.1f", got, retainedHumidity)
	}
	if got := overlaid.Config.RoomTemperatureZoneMapping; got == nil || *got != int(retainedMapping) {
		t.Fatalf("saved room temperature mapping = %v; want retained %d", got, retainedMapping)
	}
	if got := overlaid.Config.AssociatedCircuit; got == nil || *got != int(retainedCircuit) {
		t.Fatalf("saved associated circuit = %v; want retained %d", got, retainedCircuit)
	}
	if overlaid.State.CurrentTempC == &retainedTemp || overlaid.State.CurrentHumidityPct == &retainedHumidity {
		t.Fatal("saved zone shares a retained value")
	}
	if overlaid.Config.OperatingMode != "auto" || overlaid.Name != "zone-2" {
		t.Fatalf("saved zone lost a published field: %+v", overlaid)
	}
}

// TestCacheHydrationFromSnapshotKeysZonesAndKeepsLegacyReturn pins the
// conversion of a loaded cache into the native hydration value: zones keep
// cache order and take the instance their identifier names, falling back to
// the cache index; hot water carries the persisted time; the boiler keeps
// the cached legacy return.
func TestCacheHydrationFromSnapshotKeysZonesAndKeepsLegacyReturn(t *testing.T) {
	ret, current := 33.0, 44.0
	persisted := time.Date(2026, time.March, 1, 6, 0, 0, 0, time.FixedZone("east", 2*3600))
	hydration := cacheHydrationFromSnapshot(semanticCacheSnapshot{
		Zones: []graphql.Zone{
			{ID: "zone-3", Name: "upstairs"},
			{ID: "custom", Name: "fallback"},
		},
		DHW:         &graphql.DhwStatus{State: graphql.DhwState{CurrentTempC: &current}},
		Boiler:      &graphql.BoilerStatus{State: graphql.BoilerState{ReturnTemperatureC: &ret}},
		PersistedAt: persisted,
	})
	if len(hydration.zones) != 2 || hydration.zones[0].Instance != 0x02 || hydration.zones[1].Instance != 0x01 {
		t.Fatalf("hydrated zones = %+v; want instances 2 then fallback 1", hydration.zones)
	}
	if hydration.zones[0].Name != "upstairs" || hydration.zones[1].Name != "fallback" {
		t.Fatal("hydrated zones lost cache order")
	}
	if hydration.dhw == nil || hydration.dhw.CurrentTempC == nil || *hydration.dhw.CurrentTempC != current {
		t.Fatalf("hydrated hot water = %+v; want cached current temperature", hydration.dhw)
	}
	if !hydration.persistedAt.Equal(persisted) {
		t.Fatalf("persisted time = %v; want %v", hydration.persistedAt, persisted)
	}
	if hydration.boiler == nil || hydration.boiler.ReturnTemperatureC == nil || *hydration.boiler.ReturnTemperatureC != ret {
		t.Fatalf("hydrated boiler = %+v; want cached legacy return", hydration.boiler)
	}

	poller := &vaillantSemanticPoller{}
	poller.hydrateFromCache(hydration)
	if poller.zones[0x02] == nil || poller.zones[0x01] == nil || poller.presence[0x02] == nil {
		t.Fatal("hydration did not key zones by instance")
	}
	if !poller.dhwLastUpdateAt.Equal(persisted) || poller.dhwLastUpdateAt.Location() != time.UTC {
		t.Fatalf("hot water update time = %v; want the persisted time in UTC", poller.dhwLastUpdateAt)
	}

	empty := cacheHydrationFromSnapshot(semanticCacheSnapshot{})
	if empty.zones != nil || empty.dhw != nil || empty.boiler != nil || !empty.persistedAt.IsZero() {
		t.Fatalf("empty cache hydration = %+v; want the zero value", empty)
	}
}
