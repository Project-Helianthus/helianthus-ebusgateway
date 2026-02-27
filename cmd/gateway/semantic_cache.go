package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/d3vi1/helianthus-ebusgateway/graphql"
)

const (
	semanticCacheSchemaVersionV1 = 1
	semanticCacheSchemaVersionV2 = 2
)

type semanticCacheSnapshot struct {
	Zones       []graphql.Zone
	DHW         *graphql.DhwStatus
	Boiler      *graphql.BoilerStatus
	PersistedAt time.Time
}

type semanticCachePersister interface {
	Save(snapshot semanticCacheSnapshot) error
}

type semanticCacheStore struct {
	path string
	logf func(string, ...any)
	now  func() time.Time
}

type semanticCacheEnvelope struct {
	SchemaVersion int `json:"schema_version"`
}

type semanticCacheV1 struct {
	Zones []semanticCacheZone `json:"zones"`
	DHW   *semanticCacheDHW   `json:"dhw"`
}

type semanticCacheV2 struct {
	SchemaVersion int                    `json:"schema_version"`
	Metadata      semanticCacheMetadata  `json:"metadata"`
	Zones         []semanticCacheZone    `json:"zones,omitempty"`
	DHW           *semanticCacheDHW      `json:"dhw,omitempty"`
	Boiler        *semanticCacheBoiler   `json:"boiler,omitempty"`
}

type semanticCacheBoiler struct {
	FlowTemperatureC         *float64 `json:"flow_temperature_c,omitempty"`
	ReturnTemperatureC       *float64 `json:"return_temperature_c,omitempty"`
	CentralHeatingPumpActive *bool    `json:"central_heating_pump_active,omitempty"`
	DhwTemperatureC          *float64 `json:"dhw_temperature_c,omitempty"`
	DhwTargetTemperatureC    *float64 `json:"dhw_target_temperature_c,omitempty"`
	DhwOperatingMode         *string  `json:"dhw_operating_mode,omitempty"`
	HeatingStatusRaw         *int     `json:"heating_status_raw,omitempty"`
	DhwStatusRaw             *int     `json:"dhw_status_raw,omitempty"`
}

type semanticCacheMetadata struct {
	PersistedAt  time.Time `json:"persisted_at,omitempty"`
	MigratedFrom int       `json:"migrated_from,omitempty"`
}

type semanticCacheZone struct {
	ID                   string   `json:"id"`
	Name                 string   `json:"name"`
	OperatingMode        string   `json:"operating_mode,omitempty"`
	Preset               string   `json:"preset,omitempty"`
	HvacAction           string   `json:"hvac_action,omitempty"`
	AllowedModes         []string `json:"allowed_modes,omitempty"`
	CurrentTempC         *float64 `json:"current_temp_c,omitempty"`
	TargetTempC          *float64 `json:"target_temp_c,omitempty"`
	CurrentHumidityPct   *float64 `json:"current_humidity_pct,omitempty"`
	HeatingDemand        *float64 `json:"heating_demand,omitempty"`
	SpecialFunction      string   `json:"special_function,omitempty"`
	CircuitTypeRaw       string   `json:"circuit_type_raw,omitempty"`
	ZoneCircuitIndexRaw  string   `json:"zone_circuit_index_raw,omitempty"`
	ZoneOperationModeRaw string   `json:"zone_operation_mode_raw,omitempty"`
	ZoneValveStatusRaw   string   `json:"zone_valve_status_raw,omitempty"`
	ZoneSpecialFuncRaw   string   `json:"zone_special_function_raw,omitempty"`
}

type semanticCacheDHW struct {
	OperatingMode       string   `json:"operating_mode,omitempty"`
	Preset              string   `json:"preset,omitempty"`
	CurrentTempC        *float64 `json:"current_temp_c,omitempty"`
	TargetTempC         *float64 `json:"target_temp_c,omitempty"`
	HeatingDemand       *float64 `json:"heating_demand,omitempty"`
	SpecialFunction     string   `json:"special_function,omitempty"`
	DHWOperationModeRaw string   `json:"dhw_operation_mode_raw,omitempty"`
	DHWSpecialFuncRaw   string   `json:"dhw_special_function_raw,omitempty"`
}

func newSemanticCacheStore(path string, logf func(string, ...any)) *semanticCacheStore {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil
	}
	if logf == nil {
		logf = log.Printf
	}
	return &semanticCacheStore{
		path: path,
		logf: logf,
		now:  time.Now,
	}
}

func (store *semanticCacheStore) Load() (semanticCacheSnapshot, bool) {
	if store == nil || store.path == "" {
		return semanticCacheSnapshot{}, false
	}

	payload, err := os.ReadFile(store.path)
	if err != nil {
		if os.IsNotExist(err) {
			store.logf("semantic_cache_miss path=%q", store.path)
			return semanticCacheSnapshot{}, false
		}
		store.logf("semantic_cache_invalid path=%q reason=read_error err=%v", store.path, err)
		return semanticCacheSnapshot{}, false
	}

	var envelope semanticCacheEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		store.logf("semantic_cache_invalid path=%q reason=json_unmarshal err=%v", store.path, err)
		return semanticCacheSnapshot{}, false
	}

	switch envelope.SchemaVersion {
	case 0, semanticCacheSchemaVersionV1:
		var cacheV1 semanticCacheV1
		if err := json.Unmarshal(payload, &cacheV1); err != nil {
			store.logf("semantic_cache_invalid path=%q reason=v1_unmarshal err=%v", store.path, err)
			return semanticCacheSnapshot{}, false
		}
		cacheV2 := migrateSemanticCacheV1ToV2(cacheV1, store.now().UTC())
		store.logf("semantic_cache_migrated path=%q from=%d to=%d", store.path, semanticCacheSchemaVersionV1, semanticCacheSchemaVersionV2)
		if err := store.writeV2(cacheV2); err != nil {
			store.logf("semantic_cache_write_error path=%q reason=migration_write err=%v", store.path, err)
		}
		snapshot := semanticCacheV2ToSnapshot(cacheV2)
		store.logf("semantic_cache_hit path=%q schema_version=%d zones=%d dhw=%t", store.path, semanticCacheSchemaVersionV2, len(snapshot.Zones), snapshot.DHW != nil)
		return snapshot, true
	case semanticCacheSchemaVersionV2:
		var cacheV2 semanticCacheV2
		if err := json.Unmarshal(payload, &cacheV2); err != nil {
			store.logf("semantic_cache_invalid path=%q reason=v2_unmarshal err=%v", store.path, err)
			return semanticCacheSnapshot{}, false
		}
		snapshot := semanticCacheV2ToSnapshot(cacheV2)
		store.logf("semantic_cache_hit path=%q schema_version=%d zones=%d dhw=%t", store.path, semanticCacheSchemaVersionV2, len(snapshot.Zones), snapshot.DHW != nil)
		return snapshot, true
	default:
		store.logf("semantic_cache_invalid path=%q reason=unknown_schema_version schema_version=%d", store.path, envelope.SchemaVersion)
		return semanticCacheSnapshot{}, false
	}
}

func (store *semanticCacheStore) Save(snapshot semanticCacheSnapshot) error {
	if store == nil || store.path == "" {
		return nil
	}
	cacheV2 := semanticCacheSnapshotToV2(snapshot, store.now().UTC())
	return store.writeV2(cacheV2)
}

func (store *semanticCacheStore) writeV2(cacheV2 semanticCacheV2) error {
	if store == nil || store.path == "" {
		return nil
	}
	payload, err := json.MarshalIndent(cacheV2, "", "  ")
	if err != nil {
		store.logf("semantic_cache_write_error path=%q reason=marshal err=%v", store.path, err)
		return err
	}
	payload = append(payload, '\n')
	if err := writeSemanticCacheFileAtomic(store.path, payload); err != nil {
		store.logf("semantic_cache_write_error path=%q reason=atomic_write err=%v", store.path, err)
		return err
	}
	store.logf(
		"semantic_cache_write path=%q schema_version=%d zones=%d dhw=%t",
		store.path,
		cacheV2.SchemaVersion,
		len(cacheV2.Zones),
		cacheV2.DHW != nil,
	)
	return nil
}

func migrateSemanticCacheV1ToV2(cacheV1 semanticCacheV1, migratedAt time.Time) semanticCacheV2 {
	return semanticCacheV2{
		SchemaVersion: semanticCacheSchemaVersionV2,
		Metadata: semanticCacheMetadata{
			PersistedAt:  migratedAt,
			MigratedFrom: semanticCacheSchemaVersionV1,
		},
		Zones: normalizeSemanticCacheZones(cacheV1.Zones),
		DHW:   cloneSemanticCacheDHW(cacheV1.DHW),
	}
}

func semanticCacheV2ToSnapshot(cacheV2 semanticCacheV2) semanticCacheSnapshot {
	out := semanticCacheSnapshot{
		Zones:       make([]graphql.Zone, 0, len(cacheV2.Zones)),
		DHW:         nil,
		PersistedAt: cacheV2.Metadata.PersistedAt,
	}
	for _, zone := range normalizeSemanticCacheZones(cacheV2.Zones) {
		out.Zones = append(out.Zones, graphql.Zone{
			ID:                     zone.ID,
			Name:                   zone.Name,
			OperatingMode:          zone.OperatingMode,
			Preset:                 zone.Preset,
			HvacAction:             zone.HvacAction,
			AllowedModes:           append([]string(nil), zone.AllowedModes...),
			CurrentTempC:           cloneFloatPtr(zone.CurrentTempC),
			TargetTempC:            cloneFloatPtr(zone.TargetTempC),
			CurrentHumidityPct:     cloneFloatPtr(zone.CurrentHumidityPct),
			HeatingDemand:          cloneFloatPtr(zone.HeatingDemand),
			SpecialFunction:        zone.SpecialFunction,
			CircuitTypeRaw:         zone.CircuitTypeRaw,
			ZoneCircuitIndexRaw:    zone.ZoneCircuitIndexRaw,
			ZoneOperationModeRaw:   zone.ZoneOperationModeRaw,
			ZoneValveStatusRaw:     zone.ZoneValveStatusRaw,
			ZoneSpecialFunctionRaw: zone.ZoneSpecialFuncRaw,
		})
	}
	if cacheV2.DHW != nil {
		out.DHW = &graphql.DhwStatus{
			OperatingMode:         cacheV2.DHW.OperatingMode,
			Preset:                cacheV2.DHW.Preset,
			CurrentTempC:          cloneFloatPtr(cacheV2.DHW.CurrentTempC),
			TargetTempC:           cloneFloatPtr(cacheV2.DHW.TargetTempC),
			HeatingDemand:         cloneFloatPtr(cacheV2.DHW.HeatingDemand),
			SpecialFunction:       cacheV2.DHW.SpecialFunction,
			DhwOperationModeRaw:   cacheV2.DHW.DHWOperationModeRaw,
			DhwSpecialFunctionRaw: cacheV2.DHW.DHWSpecialFuncRaw,
		}
	}
	if cacheV2.Boiler != nil {
		out.Boiler = cacheBoilerToGraphQL(cacheV2.Boiler)
	}
	return out
}

func semanticCacheSnapshotToV2(snapshot semanticCacheSnapshot, persistedAt time.Time) semanticCacheV2 {
	normalizedSnapshot := normalizeSemanticCacheSnapshot(snapshot)
	cacheV2 := semanticCacheV2{
		SchemaVersion: semanticCacheSchemaVersionV2,
		Metadata: semanticCacheMetadata{
			PersistedAt: persistedAt,
		},
		Zones: make([]semanticCacheZone, 0, len(normalizedSnapshot.Zones)),
	}
	for _, zone := range normalizedSnapshot.Zones {
		cacheV2.Zones = append(cacheV2.Zones, semanticCacheZone{
			ID:                   zone.ID,
			Name:                 zone.Name,
			OperatingMode:        zone.OperatingMode,
			Preset:               zone.Preset,
			HvacAction:           zone.HvacAction,
			AllowedModes:         append([]string(nil), zone.AllowedModes...),
			CurrentTempC:         cloneFloatPtr(zone.CurrentTempC),
			TargetTempC:          cloneFloatPtr(zone.TargetTempC),
			CurrentHumidityPct:   cloneFloatPtr(zone.CurrentHumidityPct),
			HeatingDemand:        cloneFloatPtr(zone.HeatingDemand),
			SpecialFunction:      zone.SpecialFunction,
			CircuitTypeRaw:       zone.CircuitTypeRaw,
			ZoneCircuitIndexRaw:  zone.ZoneCircuitIndexRaw,
			ZoneOperationModeRaw: zone.ZoneOperationModeRaw,
			ZoneValveStatusRaw:   zone.ZoneValveStatusRaw,
			ZoneSpecialFuncRaw:   zone.ZoneSpecialFunctionRaw,
		})
	}
	if normalizedSnapshot.DHW != nil {
		cacheV2.DHW = &semanticCacheDHW{
			OperatingMode:       normalizedSnapshot.DHW.OperatingMode,
			Preset:              normalizedSnapshot.DHW.Preset,
			CurrentTempC:        cloneFloatPtr(normalizedSnapshot.DHW.CurrentTempC),
			TargetTempC:         cloneFloatPtr(normalizedSnapshot.DHW.TargetTempC),
			HeatingDemand:       cloneFloatPtr(normalizedSnapshot.DHW.HeatingDemand),
			SpecialFunction:     normalizedSnapshot.DHW.SpecialFunction,
			DHWOperationModeRaw: normalizedSnapshot.DHW.DhwOperationModeRaw,
			DHWSpecialFuncRaw:   normalizedSnapshot.DHW.DhwSpecialFunctionRaw,
		}
	}
	if normalizedSnapshot.Boiler != nil {
		cacheV2.Boiler = graphQLBoilerToCache(normalizedSnapshot.Boiler)
	}
	return cacheV2
}

func normalizeSemanticCacheSnapshot(snapshot semanticCacheSnapshot) semanticCacheSnapshot {
	out := semanticCacheSnapshot{
		Zones: make([]graphql.Zone, 0, len(snapshot.Zones)),
		DHW:   nil,
	}
	for _, zone := range snapshot.Zones {
		zoneCopy := zone
		zoneCopy.AllowedModes = append([]string(nil), zone.AllowedModes...)
		slices.Sort(zoneCopy.AllowedModes)
		zoneCopy.CurrentTempC = cloneFloatPtr(zone.CurrentTempC)
		zoneCopy.TargetTempC = cloneFloatPtr(zone.TargetTempC)
		zoneCopy.CurrentHumidityPct = cloneFloatPtr(zone.CurrentHumidityPct)
		zoneCopy.HeatingDemand = cloneFloatPtr(zone.HeatingDemand)
		out.Zones = append(out.Zones, zoneCopy)
	}
	slices.SortFunc(out.Zones, func(a, b graphql.Zone) int {
		if compare := compareSemanticZoneID(a.ID, b.ID); compare != 0 {
			return compare
		}
		return strings.Compare(a.Name, b.Name)
	})
	if snapshot.DHW != nil {
		out.DHW = &graphql.DhwStatus{
			OperatingMode:         snapshot.DHW.OperatingMode,
			Preset:                snapshot.DHW.Preset,
			CurrentTempC:          cloneFloatPtr(snapshot.DHW.CurrentTempC),
			TargetTempC:           cloneFloatPtr(snapshot.DHW.TargetTempC),
			HeatingDemand:         cloneFloatPtr(snapshot.DHW.HeatingDemand),
			SpecialFunction:       snapshot.DHW.SpecialFunction,
			DhwOperationModeRaw:   snapshot.DHW.DhwOperationModeRaw,
			DhwSpecialFunctionRaw: snapshot.DHW.DhwSpecialFunctionRaw,
		}
	}
	out.Boiler = snapshot.Boiler
	return out
}

func normalizeSemanticCacheZones(zones []semanticCacheZone) []semanticCacheZone {
	out := make([]semanticCacheZone, 0, len(zones))
	for _, zone := range zones {
		zoneCopy := zone
		zoneCopy.AllowedModes = append([]string(nil), zone.AllowedModes...)
		slices.Sort(zoneCopy.AllowedModes)
		zoneCopy.CurrentTempC = cloneFloatPtr(zone.CurrentTempC)
		zoneCopy.TargetTempC = cloneFloatPtr(zone.TargetTempC)
		zoneCopy.CurrentHumidityPct = cloneFloatPtr(zone.CurrentHumidityPct)
		zoneCopy.HeatingDemand = cloneFloatPtr(zone.HeatingDemand)
		out = append(out, zoneCopy)
	}
	slices.SortFunc(out, func(a, b semanticCacheZone) int {
		if compare := compareSemanticZoneID(a.ID, b.ID); compare != 0 {
			return compare
		}
		return strings.Compare(a.Name, b.Name)
	})
	return out
}

func compareSemanticZoneID(left, right string) int {
	leftOrdinal, leftOK := parseSemanticZoneOrdinal(left)
	rightOrdinal, rightOK := parseSemanticZoneOrdinal(right)
	if leftOK && rightOK {
		switch {
		case leftOrdinal < rightOrdinal:
			return -1
		case leftOrdinal > rightOrdinal:
			return 1
		}
	}
	return strings.Compare(left, right)
}

func parseSemanticZoneOrdinal(id string) (int, bool) {
	id = strings.TrimSpace(strings.ToLower(id))
	if !strings.HasPrefix(id, "zone-") {
		return 0, false
	}
	ordinalRaw := strings.TrimPrefix(id, "zone-")
	ordinal, err := strconv.Atoi(ordinalRaw)
	if err != nil || ordinal <= 0 {
		return 0, false
	}
	return ordinal, true
}

func cloneSemanticCacheDHW(status *semanticCacheDHW) *semanticCacheDHW {
	if status == nil {
		return nil
	}
	return &semanticCacheDHW{
		OperatingMode:       status.OperatingMode,
		Preset:              status.Preset,
		CurrentTempC:        cloneFloatPtr(status.CurrentTempC),
		TargetTempC:         cloneFloatPtr(status.TargetTempC),
		HeatingDemand:       cloneFloatPtr(status.HeatingDemand),
		SpecialFunction:     status.SpecialFunction,
		DHWOperationModeRaw: status.DHWOperationModeRaw,
		DHWSpecialFuncRaw:   status.DHWSpecialFuncRaw,
	}
}

func cacheBoilerToGraphQL(cache *semanticCacheBoiler) *graphql.BoilerStatus {
	if cache == nil {
		return nil
	}
	out := &graphql.BoilerStatus{
		State: graphql.BoilerState{
			FlowTemperatureC:         cloneFloatPtr(cache.FlowTemperatureC),
			ReturnTemperatureC:       cloneFloatPtr(cache.ReturnTemperatureC),
			DhwTemperatureC:          cloneFloatPtr(cache.DhwTemperatureC),
			DhwTargetTemperatureC:    cloneFloatPtr(cache.DhwTargetTemperatureC),
		},
		Diagnostics: graphql.BoilerDiagnostics{
			HeatingStatusRaw: cloneIntPtr(cache.HeatingStatusRaw),
			DhwStatusRaw:     cloneIntPtr(cache.DhwStatusRaw),
		},
	}
	if cache.CentralHeatingPumpActive != nil {
		v := *cache.CentralHeatingPumpActive
		out.State.CentralHeatingPumpActive = &v
	}
	if cache.DhwOperatingMode != nil {
		v := *cache.DhwOperatingMode
		out.Config.DhwOperatingMode = &v
	}
	return out
}

func graphQLBoilerToCache(status *graphql.BoilerStatus) *semanticCacheBoiler {
	if status == nil {
		return nil
	}
	out := &semanticCacheBoiler{
		FlowTemperatureC:    cloneFloatPtr(status.State.FlowTemperatureC),
		ReturnTemperatureC:  cloneFloatPtr(status.State.ReturnTemperatureC),
		DhwTemperatureC:     cloneFloatPtr(status.State.DhwTemperatureC),
		DhwTargetTemperatureC: cloneFloatPtr(status.State.DhwTargetTemperatureC),
		DhwOperatingMode:    cloneStringPtr(status.Config.DhwOperatingMode),
		HeatingStatusRaw:    cloneIntPtr(status.Diagnostics.HeatingStatusRaw),
		DhwStatusRaw:        cloneIntPtr(status.Diagnostics.DhwStatusRaw),
	}
	if status.State.CentralHeatingPumpActive != nil {
		v := *status.State.CentralHeatingPumpActive
		out.CentralHeatingPumpActive = &v
	}
	return out
}

func writeSemanticCacheFileAtomic(path string, payload []byte) error {
	if strings.TrimSpace(path) == "" {
		return fmt.Errorf("semantic cache path empty")
	}
	path = filepath.Clean(path)
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	tempFile, err := os.CreateTemp(dir, ".semantic-cache-*.tmp")
	if err != nil {
		return err
	}
	tempPath := tempFile.Name()
	removeTemp := true
	defer func() {
		if removeTemp {
			_ = os.Remove(tempPath)
		}
	}()

	if _, err := tempFile.Write(payload); err != nil {
		_ = tempFile.Close()
		return err
	}
	if err := tempFile.Sync(); err != nil {
		_ = tempFile.Close()
		return err
	}
	if err := tempFile.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tempPath, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tempPath, path); err != nil {
		return err
	}
	removeTemp = false
	return nil
}
