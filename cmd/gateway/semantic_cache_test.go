package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

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
			{ID: "zone-1", Name: "Zone 1", TargetTempC: &targetTemp},
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

func TestSemanticCacheStoreLoad_MigratesV1ToV2(t *testing.T) {
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
	snapshot, ok := store.Load()
	if !ok {
		t.Fatalf("Load() ok = false; want true")
	}
	if len(snapshot.Zones) != 2 {
		t.Fatalf("len(snapshot.Zones) = %d; want 2", len(snapshot.Zones))
	}
	if snapshot.Zones[0].ID != "zone-1" {
		t.Fatalf("snapshot.Zones[0].ID = %q; want zone-1 (deterministic order)", snapshot.Zones[0].ID)
	}
	if snapshot.DHW == nil || snapshot.DHW.OperatingMode != "auto" {
		t.Fatalf("snapshot.DHW = %#v; want operating_mode=auto", snapshot.DHW)
	}

	persistedPayload, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() migrated path error = %v", err)
	}
	var persisted semanticCacheV2
	if err := json.Unmarshal(persistedPayload, &persisted); err != nil {
		t.Fatalf("Unmarshal(migrated) error = %v", err)
	}
	if persisted.SchemaVersion != semanticCacheSchemaVersionV2 {
		t.Fatalf("persisted.SchemaVersion = %d; want %d", persisted.SchemaVersion, semanticCacheSchemaVersionV2)
	}
	if persisted.Metadata.MigratedFrom != semanticCacheSchemaVersionV1 {
		t.Fatalf("persisted.Metadata.MigratedFrom = %d; want %d", persisted.Metadata.MigratedFrom, semanticCacheSchemaVersionV1)
	}
	logs.requireContains(t, "semantic_cache_migrated")
	logs.requireContains(t, "semantic_cache_write")
	logs.requireContains(t, "semantic_cache_hit")
}

func TestSemanticCacheStoreSave_SortsZonesAndAllowedModes(t *testing.T) {
	logs := &semanticCacheTestLogs{}
	path := filepath.Join(t.TempDir(), "semantic-cache.json")
	store := newSemanticCacheStore(path, logs.Printf)

	if err := store.Save(semanticCacheSnapshot{
		Zones: []graphql.Zone{
			{ID: "zone-10", Name: "Zone 10", AllowedModes: []string{"heat", "off"}},
			{ID: "zone-2", Name: "Zone 2", AllowedModes: []string{"off", "heat"}},
			{ID: "zone-1", Name: "Zone 1", AllowedModes: []string{"heat", "auto"}},
		},
	}); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	var persisted semanticCacheV2
	if err := json.Unmarshal(payload, &persisted); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if len(persisted.Zones) != 3 {
		t.Fatalf("len(persisted.Zones) = %d; want 3", len(persisted.Zones))
	}
	if persisted.Zones[0].ID != "zone-1" || persisted.Zones[1].ID != "zone-2" || persisted.Zones[2].ID != "zone-10" {
		t.Fatalf("zone order = [%s %s %s]; want [zone-1 zone-2 zone-10]", persisted.Zones[0].ID, persisted.Zones[1].ID, persisted.Zones[2].ID)
	}
	if got := strings.Join(persisted.Zones[0].AllowedModes, ","); got != "auto,heat" {
		t.Fatalf("allowed_modes zone-1 = %q; want auto,heat", got)
	}
	if got := strings.Join(persisted.Zones[1].AllowedModes, ","); got != "heat,off" {
		t.Fatalf("allowed_modes zone-2 = %q; want heat,off", got)
	}
	if got := strings.Join(persisted.Zones[2].AllowedModes, ","); got != "heat,off" {
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
		Zones: []graphql.Zone{{ID: "zone-1", Name: "Zone 1", OperatingMode: "heat"}},
		DHW:   &graphql.DhwStatus{OperatingMode: "auto", CurrentTempC: &current},
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
	if dhw := provider.DHW(); dhw == nil || dhw.OperatingMode != "auto" {
		t.Fatalf("provider.DHW() = %#v; want non-nil auto", dhw)
	}
}

func TestVaillantSemanticPoller_PersistsOnlyOnLivePublish(t *testing.T) {
	cacheSpy := &semanticCachePersisterSpy{}
	provider := graphql.NewLiveSemanticProvider()
	current := 21.5
	target := 22.0
	poller := &vaillantSemanticPoller{
		provider: provider,
		cache:    cacheSpy,
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
			ID:            "zone-1",
			Name:          "Living Room",
			OperatingMode: "heat",
			Preset:        "manual",
		},
	}
	preloadedDHW := &graphql.DhwStatus{
		OperatingMode: "auto",
		Preset:        "schedule",
		CurrentTempC:  &current,
		TargetTempC:   &target,
	}
	provider.SetZonesFromCache(preloadedZones)
	provider.SetDHWFromCache(preloadedDHW)

	poller := &vaillantSemanticPoller{
		provider: provider,
		cache:    &semanticCachePersisterSpy{},
		zones:    map[byte]*vaillantZoneSnapshot{},
		dhw:      nil,
	}

	poller.publishZones(semanticSnapshotSourceCache)
	poller.publishDHW(semanticSnapshotSourceCache)

	zones := provider.Zones()
	if len(zones) != 1 || zones[0].ID != "zone-1" {
		t.Fatalf("provider.Zones() = %#v; want preloaded zone-1 preserved", zones)
	}
	dhw := provider.DHW()
	if dhw == nil || dhw.OperatingMode != "auto" {
		t.Fatalf("provider.DHW() = %#v; want preloaded DHW preserved", dhw)
	}
}

func TestVaillantSemanticPoller_HydrateFromCache_SeedsInternalState(t *testing.T) {
	current := 21.3
	target := 22.0
	dhwCurrent := 48.2
	dhwTarget := 50.0
	snapshot := semanticCacheSnapshot{
		Zones: []graphql.Zone{
			{
				ID:                  "zone-2",
				Name:                "Etaj",
				OperatingMode:       "heat",
				Preset:              "manual",
				AllowedModes:        []string{"off", "auto", "heat"},
				CurrentTempC:        &current,
				TargetTempC:         &target,
				ZoneCircuitIndexRaw: "2",
				CircuitTypeRaw:      "1",
				ZoneValveStatusRaw:  "1",
			},
		},
		DHW: &graphql.DhwStatus{
			OperatingMode:       "auto",
			Preset:              "schedule",
			CurrentTempC:        &dhwCurrent,
			TargetTempC:         &dhwTarget,
			DhwOperationModeRaw: "1",
		},
	}

	poller := &vaillantSemanticPoller{
		zones: make(map[byte]*vaillantZoneSnapshot),
	}
	poller.hydrateFromCache(snapshot)

	zone := poller.zones[0x01]
	if zone == nil {
		t.Fatalf("poller.zones[0x01] = nil; want hydrated zone state")
	}
	if zone.Name != "Etaj" || zone.OperatingMode != "heat" || zone.Preset != "manual" {
		t.Fatalf("hydrated zone = %#v; unexpected core fields", zone)
	}
	if zone.ConfigurationAssociatedCircuitRaw == nil || *zone.ConfigurationAssociatedCircuitRaw != 2 {
		t.Fatalf("hydrated zone circuit index = %#v; want 2", zone.ConfigurationAssociatedCircuitRaw)
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
