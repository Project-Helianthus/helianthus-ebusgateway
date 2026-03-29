package portal

import (
	"cmp"
	"embed"
	"encoding/json"
	"expvar"
	"fmt"
	"html/template"
	"io/fs"
	"log"
	"net/http"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

//go:embed static/*
var assets embed.FS

var staticFS = func() fs.FS {
	sub, err := fs.Sub(assets, "static")
	if err != nil {
		panic(err)
	}
	return sub
}()

var indexTemplate = template.Must(template.ParseFS(assets, "static/index.html"))

type assetHashEntry struct {
	SHA256 string `json:"sha256"`
}

type assetManifest struct {
	Assets map[string]assetHashEntry `json:"assets"`
}

var (
	portalRequestCount      = expvar.NewMap("portal_requests_total")
	portalRouteDurationMS   = expvar.NewMap("portal_route_duration_ms_total")
	portalAssetETagByTarget = loadAssetETags()
	portalStreamEventsTotal = expvar.NewMap("portal_stream_events_total")
	portalStreamDropped     = expvar.NewMap("portal_stream_dropped_total")
)

type Options struct {
	GraphQLPath         string
	SnapshotPath        string
	SubscriptionPath    string
	MCPPath             string
	GatewayVersion      string
	BuildID             string
	ListRegistry        func() []RegistryDevice
	ListSemantic        func() SemanticSnapshot
	GetBusObservability func() any
	ListProjections     func() []ProjectionDevice
	GetProjection       func(address byte, plane string) (ProjectionGraph, bool)
	ExplorerBus         ExplorerBus // nil disables explorer
	ExplorerSource      byte        // default eBUS source address (0xF0 if zero)
}

type handler struct {
	opts      Options
	files     http.Handler
	timeline  *timelineBuffer
	snapshots *snapshotStore
	sessions  *sessionStore
	explorer  *explorerStore
}

type RegistryDevice struct {
	Address      int             `json:"address"`
	Addresses    []int           `json:"addresses,omitempty"`
	Manufacturer string          `json:"manufacturer"`
	DeviceID     string          `json:"device_id"`
	DisplayName  string          `json:"display_name,omitempty"`
	Role         string          `json:"role,omitempty"`
	SerialNumber string          `json:"serial_number,omitempty"`
	Software     string          `json:"software_version"`
	Hardware     string          `json:"hardware_version"`
	Planes       []RegistryPlane `json:"planes,omitempty"`
}

type RegistryPlane struct {
	Name    string   `json:"name"`
	Methods []string `json:"methods,omitempty"`
}

type SemanticSnapshot struct {
	Zones        []SemanticZone        `json:"zones"`
	DHW          *SemanticDHW          `json:"dhw,omitempty"`
	Energy       *SemanticEnergyTotals `json:"energy_totals,omitempty"`
	BoilerStatus *SemanticBoilerStatus `json:"boiler_status,omitempty"`
	System       *SemanticSystemStatus `json:"system,omitempty"`
	Circuits     []SemanticCircuit     `json:"circuits,omitempty"`
	RadioDevices []SemanticRadioDevice `json:"radio_devices,omitempty"`
	FM5Mode      string                `json:"fm5_semantic_mode,omitempty"`
	Solar        *SemanticSolarStatus  `json:"solar,omitempty"`
	Cylinders    []SemanticCylinder    `json:"cylinders,omitempty"`
	AdapterInfo  *SemanticAdapterInfo  `json:"adapter_info,omitempty"`
	CapturedUTC  string                `json:"captured_utc"`
}

type SemanticAdapterInfo struct {
	FirmwareVersion    string   `json:"firmware_version"`
	FirmwareChecksum   string   `json:"firmware_checksum,omitempty"`
	BootloaderVersion  string   `json:"bootloader_version,omitempty"`
	BootloaderChecksum string   `json:"bootloader_checksum,omitempty"`
	HardwareID         string   `json:"hardware_id,omitempty"`
	HardwareConfig     string   `json:"hardware_config,omitempty"`
	Features           byte     `json:"features"`
	Jumpers            byte     `json:"jumpers"`
	JumperFlags        []string `json:"jumper_flags,omitempty"`
	IsWiFi             bool     `json:"is_wifi"`
	IsEthernet         bool     `json:"is_ethernet"`
	TemperatureC       *float64 `json:"temperature_c,omitempty"`
	SupplyVoltageMV    *int     `json:"supply_voltage_mv,omitempty"`
	BusVoltageMaxDV    *int     `json:"bus_voltage_max_dv,omitempty"`
	BusVoltageMinDV    *int     `json:"bus_voltage_min_dv,omitempty"`
	ResetCause         *string  `json:"reset_cause,omitempty"`
	ResetCauseCode     *int     `json:"reset_cause_code,omitempty"`
	RestartCount       *int     `json:"restart_count,omitempty"`
	WiFiRSSIDBm        *int     `json:"wifi_rssi_dbm,omitempty"`
	LastIdentityQuery  string   `json:"last_identity_query,omitempty"`
	LastTelemetryQuery string   `json:"last_telemetry_query,omitempty"`
	VersionResponseLen int      `json:"version_response_len"`
	InfoSupported      bool     `json:"info_supported"`
}

type SemanticZoneState struct {
	CurrentTempC       *float64 `json:"current_temp_c,omitempty"`
	CurrentHumidityPct *float64 `json:"current_humidity_pct,omitempty"`
	HvacAction         string   `json:"hvac_action,omitempty"`
	SpecialFunction    string   `json:"special_function,omitempty"`
	HeatingDemandPct   *float64 `json:"heating_demand_pct,omitempty"`
	ValvePositionPct   *float64 `json:"valve_position_pct,omitempty"`
}

type SemanticZoneConfig struct {
	OperatingMode              string   `json:"operating_mode,omitempty"`
	Preset                     string   `json:"preset,omitempty"`
	TargetTempC                *float64 `json:"target_temp_c,omitempty"`
	AllowedModes               []string `json:"allowed_modes,omitempty"`
	CircuitType                string   `json:"circuit_type,omitempty"`
	AssociatedCircuit          *int     `json:"associated_circuit,omitempty"`
	RoomTemperatureZoneMapping *int     `json:"room_temperature_zone_mapping,omitempty"`
}

type SemanticZone struct {
	ID     string             `json:"id"`
	Name   string             `json:"name"`
	State  SemanticZoneState  `json:"state"`
	Config SemanticZoneConfig `json:"config"`
}

type SemanticDhwState struct {
	CurrentTempC     *float64 `json:"current_temp_c,omitempty"`
	SpecialFunction  string   `json:"special_function,omitempty"`
	HeatingDemandPct *float64 `json:"heating_demand_pct,omitempty"`
}

type SemanticDhwConfig struct {
	OperatingMode string   `json:"operating_mode,omitempty"`
	Preset        string   `json:"preset,omitempty"`
	TargetTempC   *float64 `json:"target_temp_c,omitempty"`
}

type SemanticDHW struct {
	State  SemanticDhwState  `json:"state"`
	Config SemanticDhwConfig `json:"config"`
}

type SemanticEnergyTotals struct {
	Gas      SemanticEnergyChannel `json:"gas"`
	Electric SemanticEnergyChannel `json:"electric"`
	Solar    SemanticEnergyChannel `json:"solar"`
}

type SemanticEnergyChannel struct {
	DHW     SemanticEnergySeries `json:"dhw"`
	Climate SemanticEnergySeries `json:"climate"`
}

type SemanticEnergySeries struct {
	Today       float64                   `json:"today"`
	Yearly      []float64                 `json:"yearly,omitempty"`
	Monthly     []float64                 `json:"monthly,omitempty"`
	TodayMeta   SemanticEnergyPointMeta   `json:"today_meta"`
	YearlyMeta  []SemanticEnergyPointMeta `json:"yearly_meta,omitempty"`
	MonthlyMeta []SemanticEnergyPointMeta `json:"monthly_meta,omitempty"`
}

type SemanticEnergyPointMeta struct {
	FreshnessState  string  `json:"freshness_state"`
	Provenance      string  `json:"provenance"`
	LastObservedUTC string  `json:"last_observed_utc,omitempty"`
	AgeSeconds      float64 `json:"age_seconds,omitempty"`
	Stale           bool    `json:"stale"`
}

type SemanticBoilerState struct {
	FlowTemperatureC         *float64 `json:"flow_temperature_c,omitempty"`
	ReturnTemperatureC       *float64 `json:"return_temperature_c,omitempty"`
	CentralHeatingPumpActive *bool    `json:"central_heating_pump_active,omitempty"`
	WaterPressureBar         *float64 `json:"water_pressure_bar,omitempty"`
	ExternalPumpActive       *bool    `json:"external_pump_active,omitempty"`
	CirculationPumpActive    *bool    `json:"circulation_pump_active,omitempty"`
	GasValveActive           *bool    `json:"gas_valve_active,omitempty"`
	FlameActive              *bool    `json:"flame_active,omitempty"`
	DiverterValvePositionPct *float64 `json:"diverter_valve_position_pct,omitempty"`
	FanSpeedRpm              *int     `json:"fan_speed_rpm,omitempty"`
	TargetFanSpeedRpm        *int     `json:"target_fan_speed_rpm,omitempty"`
	IonisationVoltageUa      *float64 `json:"ionisation_voltage_ua,omitempty"`
	DhwWaterFlowLpm          *float64 `json:"dhw_water_flow_lpm,omitempty"`
	DhwDemandActive          *bool    `json:"dhw_demand_active,omitempty"`
	HeatingSwitchActive      *bool    `json:"heating_switch_active,omitempty"`
	StorageLoadPumpPct       *float64 `json:"storage_load_pump_pct,omitempty"`
	ModulationPct            *float64 `json:"modulation_pct,omitempty"`
	PrimaryCircuitFlowLpm    *float64 `json:"primary_circuit_flow_lpm,omitempty"`
	FlowTempDesiredC         *float64 `json:"flow_temp_desired_c,omitempty"`
	DhwTempDesiredC          *float64 `json:"dhw_temp_desired_c,omitempty"`
	StateNumber              *int     `json:"state_number,omitempty"`
	DhwTemperatureC          *float64 `json:"dhw_temperature_c,omitempty"`
	DhwTargetTemperatureC    *float64 `json:"dhw_target_temperature_c,omitempty"`
}

type SemanticBoilerConfig struct {
	DhwOperatingMode *string  `json:"dhw_operating_mode,omitempty"`
	FlowsetHcMaxC    *float64 `json:"flowset_hc_max_c,omitempty"`
	FlowsetHwcMaxC   *float64 `json:"flowset_hwc_max_c,omitempty"`
	PartloadHcKW     *float64 `json:"partload_hc_kw,omitempty"`
	PartloadHwcKW    *float64 `json:"partload_hwc_kw,omitempty"`
}

type SemanticBoilerDiagnostics struct {
	HeatingStatusRaw         *int     `json:"heating_status_raw,omitempty"`
	DhwStatusRaw             *int     `json:"dhw_status_raw,omitempty"`
	CentralHeatingHours      *float64 `json:"central_heating_hours,omitempty"`
	DhwHours                 *float64 `json:"dhw_hours,omitempty"`
	CentralHeatingStarts     *int     `json:"central_heating_starts,omitempty"`
	DhwStarts                *int     `json:"dhw_starts,omitempty"`
	PumpHours                *float64 `json:"pump_hours,omitempty"`
	FanHours                 *float64 `json:"fan_hours,omitempty"`
	DeactivationsIFC         *int     `json:"deactivations_ifc,omitempty"`
	DeactivationsTemplimiter *int     `json:"deactivations_templimiter,omitempty"`
}

type SemanticBoilerStatus struct {
	State       SemanticBoilerState       `json:"state"`
	Config      SemanticBoilerConfig      `json:"config"`
	Diagnostics SemanticBoilerDiagnostics `json:"diagnostics"`
}

type SemanticSystemState struct {
	SystemOff                    *bool    `json:"system_off,omitempty"`
	SystemWaterPressure          *float64 `json:"system_water_pressure,omitempty"`
	SystemFlowTemperature        *float64 `json:"system_flow_temperature,omitempty"`
	OutdoorTemperature           *float64 `json:"outdoor_temperature,omitempty"`
	OutdoorTemperatureAvg24h     *float64 `json:"outdoor_temperature_avg24h,omitempty"`
	MaintenanceDue               *bool    `json:"maintenance_due,omitempty"`
	HwcCylinderTemperatureTop    *float64 `json:"hwc_cylinder_temperature_top,omitempty"`
	HwcCylinderTemperatureBottom *float64 `json:"hwc_cylinder_temperature_bottom,omitempty"`
}

type SemanticSystemConfig struct {
	AdaptiveHeatingCurve         *bool    `json:"adaptive_heating_curve,omitempty"`
	AlternativePoint             *float64 `json:"alternative_point,omitempty"`
	HeatingCircuitBivalencePoint *float64 `json:"heating_circuit_bivalence_point,omitempty"`
	DhwBivalencePoint            *float64 `json:"dhw_bivalence_point,omitempty"`
	HcEmergencyTemperature       *float64 `json:"hc_emergency_temperature,omitempty"`
	HwcMaxFlowTempDesired        *float64 `json:"hwc_max_flow_temp_desired,omitempty"`
	MaxRoomHumidity              *int     `json:"max_room_humidity,omitempty"`
}

type SemanticSystemProperties struct {
	SystemScheme            *int `json:"system_scheme,omitempty"`
	ModuleConfigurationVR71 *int `json:"module_configuration_vr71,omitempty"`
}

type SemanticSystemStatus struct {
	State      SemanticSystemState      `json:"state"`
	Config     SemanticSystemConfig     `json:"config"`
	Properties SemanticSystemProperties `json:"properties"`
}

type SemanticCircuitState struct {
	PumpActive       *bool    `json:"pump_active,omitempty"`
	MixerPositionPct *float64 `json:"mixer_position_pct,omitempty"`
	FlowTemperatureC *float64 `json:"flow_temperature_c,omitempty"`
	FlowSetpointC    *float64 `json:"flow_setpoint_c,omitempty"`
	CalcFlowTempC    *float64 `json:"calc_flow_temp_c,omitempty"`
	CircuitState     string   `json:"circuit_state,omitempty"`
	Humidity         *float64 `json:"humidity,omitempty"`
	DewPoint         *float64 `json:"dew_point,omitempty"`
	PumpHours        *float64 `json:"pump_hours,omitempty"`
	PumpStarts       *int     `json:"pump_starts,omitempty"`
}

type SemanticCircuitConfig struct {
	HeatingCurve    *float64 `json:"heating_curve,omitempty"`
	FlowTempMaxC    *float64 `json:"flow_temp_max_c,omitempty"`
	FlowTempMinC    *float64 `json:"flow_temp_min_c,omitempty"`
	SummerLimitC    *float64 `json:"summer_limit_c,omitempty"`
	FrostProtC      *float64 `json:"frost_prot_c,omitempty"`
	RoomTempControl string   `json:"room_temp_control,omitempty"`
	CoolingEnabled  *bool    `json:"cooling_enabled,omitempty"`
}

type SemanticManagingDevice struct {
	Role     string  `json:"role"`
	DeviceID *string `json:"device_id,omitempty"`
	Address  *int    `json:"address,omitempty"`
}

type SemanticCircuit struct {
	Index          int                    `json:"index"`
	CircuitType    string                 `json:"circuit_type,omitempty"`
	HasMixer       bool                   `json:"has_mixer"`
	State          SemanticCircuitState   `json:"state"`
	Config         SemanticCircuitConfig  `json:"config"`
	ManagingDevice SemanticManagingDevice `json:"managing_device"`
}

type SemanticRadioDevice struct {
	Group                int      `json:"group"`
	Instance             int      `json:"instance"`
	SlotMode             string   `json:"slot_mode,omitempty"`
	DeviceConnected      *bool    `json:"device_connected,omitempty"`
	DeviceClassAddress   *int     `json:"device_class_address,omitempty"`
	DeviceModel          string   `json:"device_model,omitempty"`
	FirmwareVersion      *string  `json:"firmware_version,omitempty"`
	HardwareIdentifier   *int     `json:"hardware_identifier,omitempty"`
	RemoteControlAddress *int     `json:"remote_control_address,omitempty"`
	DevicePaired         *bool    `json:"device_paired,omitempty"`
	ReceptionStrength    *int     `json:"reception_strength,omitempty"`
	ZoneAssignment       *int     `json:"zone_assignment,omitempty"`
	RoomTemperatureC     *float64 `json:"room_temperature_c,omitempty"`
	RoomHumidityPct      *float64 `json:"room_humidity_pct,omitempty"`
}

type SemanticSolarStatus struct {
	CollectorTemperatureC *float64 `json:"collector_temperature_c,omitempty"`
	ReturnTemperatureC    *float64 `json:"return_temperature_c,omitempty"`
	PumpActive            *bool    `json:"pump_active,omitempty"`
	CurrentYield          *float64 `json:"current_yield,omitempty"`
	PumpHours             *float64 `json:"pump_hours,omitempty"`
	SolarEnabled          *bool    `json:"solar_enabled,omitempty"`
	FunctionMode          *bool    `json:"function_mode,omitempty"`
}

type SemanticCylinder struct {
	Index             int      `json:"index"`
	TemperatureC      *float64 `json:"temperature_c,omitempty"`
	MaxSetpointC      *float64 `json:"max_setpoint_c,omitempty"`
	ChargeHysteresisC *float64 `json:"charge_hysteresis_c,omitempty"`
	ChargeOffsetC     *float64 `json:"charge_offset_c,omitempty"`
}

type ProjectionDevice struct {
	Address      byte                `json:"address"`
	DeviceID     string              `json:"device_id,omitempty"`
	DisplayName  string              `json:"display_name,omitempty"`
	Manufacturer string              `json:"manufacturer,omitempty"`
	Projections  []ProjectionSummary `json:"projections"`
}

type ProjectionSummary struct {
	Plane     string `json:"plane"`
	NodeCount int    `json:"node_count"`
	EdgeCount int    `json:"edge_count"`
}

type ProjectionGraph struct {
	Address byte             `json:"address"`
	Plane   string           `json:"plane"`
	Nodes   []ProjectionNode `json:"nodes"`
	Edges   []ProjectionEdge `json:"edges"`
}

type ProjectionNode struct {
	ID            string `json:"id"`
	Path          string `json:"path"`
	CanonicalPath string `json:"canonical_path"`
}

type ProjectionEdge struct {
	ID   string `json:"id"`
	From string `json:"from"`
	To   string `json:"to"`
}

type SearchResult struct {
	Layer    string `json:"layer"`
	Kind     string `json:"kind"`
	ID       string `json:"id"`
	Title    string `json:"title"`
	Subtitle string `json:"subtitle,omitempty"`
	Address  *int   `json:"address,omitempty"`
}

type StreamEventEnvelope struct {
	At            string         `json:"at"`
	Type          string         `json:"type"`
	Layer         string         `json:"layer"`
	CorrelationID string         `json:"correlation_id"`
	Payload       map[string]any `json:"payload"`
	Provenance    StreamSource   `json:"provenance"`
}

type StreamSource struct {
	Source   string `json:"source"`
	Dropped  int    `json:"dropped"`
	Interval int    `json:"interval_ms"`
}

type ProvenanceRecord struct {
	CorrelationID string   `json:"correlation_id"`
	Layer         string   `json:"layer"`
	At            string   `json:"at"`
	Source        string   `json:"source"`
	Dropped       int      `json:"dropped"`
	IntervalMS    int      `json:"interval_ms"`
	DecodePath    []string `json:"decode_path"`
	PayloadKeys   []string `json:"payload_keys"`
	Confidence    float64  `json:"confidence"`
}

type timelineBuffer struct {
	mu      sync.Mutex
	cap     int
	entries []StreamEventEnvelope
}

type SnapshotEnvelope struct {
	ID         string         `json:"id"`
	Label      string         `json:"label,omitempty"`
	CapturedAt string         `json:"captured_at"`
	Payload    map[string]any `json:"payload"`
}

type SnapshotDiffEntry struct {
	Path   string `json:"path"`
	Change string `json:"change"`
	From   string `json:"from,omitempty"`
	To     string `json:"to,omitempty"`
}

type snapshotStore struct {
	mu           sync.Mutex
	maxSnapshots int
	seq          uint64
	items        []SnapshotEnvelope
}

type InvestigationSession struct {
	ID        string       `json:"id"`
	Name      string       `json:"name"`
	CreatedAt string       `json:"created_at"`
	UpdatedAt string       `json:"updated_at"`
	State     SessionState `json:"state"`
}

type SessionState struct {
	SearchQuery           string `json:"search_query,omitempty"`
	TimelineCorrelation   string `json:"timeline_correlation,omitempty"`
	ProvenanceCorrelation string `json:"provenance_correlation,omitempty"`
	SnapshotFromID        string `json:"snapshot_from_id,omitempty"`
	SnapshotToID          string `json:"snapshot_to_id,omitempty"`
	SelectedLayer         string `json:"selected_layer,omitempty"`
}

type sessionStore struct {
	mu          sync.Mutex
	maxSessions int
	seq         uint64
	items       []InvestigationSession
}

type IssueDraft struct {
	Title    string         `json:"title"`
	Markdown string         `json:"markdown"`
	Evidence map[string]any `json:"evidence"`
}

func NewHandler(opts Options) http.Handler {
	if opts.GraphQLPath == "" {
		opts.GraphQLPath = "/graphql"
	}
	if opts.SnapshotPath == "" {
		opts.SnapshotPath = "/snapshot"
	}
	if opts.SubscriptionPath == "" {
		opts.SubscriptionPath = "/graphql/subscriptions"
	}
	if opts.MCPPath == "" {
		opts.MCPPath = "/mcp"
	}
	if opts.GatewayVersion == "" {
		opts.GatewayVersion = "dev"
	}
	if opts.BuildID == "" {
		opts.BuildID = "unknown"
	}
	h := &handler{
		opts:      opts,
		files:     http.FileServer(http.FS(staticFS)),
		timeline:  newTimelineBuffer(1000),
		snapshots: newSnapshotStore(50),
		sessions:  newSessionStore(100),
	}
	if opts.ExplorerBus != nil {
		h.explorer = newExplorerStore(opts.ExplorerBus, opts.ExplorerSource)
	}
	return h
}

func newTimelineBuffer(capacity int) *timelineBuffer {
	if capacity <= 0 {
		capacity = 100
	}
	return &timelineBuffer{
		cap:     capacity,
		entries: make([]StreamEventEnvelope, 0, capacity),
	}
}

func (tb *timelineBuffer) add(event StreamEventEnvelope) {
	if tb == nil {
		return
	}
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if len(tb.entries) >= tb.cap {
		copy(tb.entries, tb.entries[1:])
		tb.entries[len(tb.entries)-1] = cloneStreamEvent(event)
		return
	}
	tb.entries = append(tb.entries, cloneStreamEvent(event))
}

func (tb *timelineBuffer) query(limit int, layer, correlationID string, since time.Time) []StreamEventEnvelope {
	if tb == nil {
		return []StreamEventEnvelope{}
	}
	tb.mu.Lock()
	defer tb.mu.Unlock()

	items := make([]StreamEventEnvelope, 0, limit)
	wantsLayer := strings.TrimSpace(strings.ToLower(layer)) != ""
	wantsCorrelation := strings.TrimSpace(strings.ToLower(correlationID)) != ""
	sinceActive := !since.IsZero()

	for i := len(tb.entries) - 1; i >= 0; i-- {
		item := tb.entries[i]
		if wantsLayer && !strings.EqualFold(item.Layer, layer) {
			continue
		}
		if wantsCorrelation && !strings.Contains(strings.ToLower(item.CorrelationID), strings.ToLower(correlationID)) {
			continue
		}
		if sinceActive {
			at, err := time.Parse(time.RFC3339Nano, item.At)
			if err != nil {
				at, err = time.Parse(time.RFC3339, item.At)
			}
			if err == nil && at.Before(since) {
				continue
			}
		}
		items = append(items, cloneStreamEvent(item))
		if limit > 0 && len(items) >= limit {
			break
		}
	}

	return items
}

func cloneStreamEvent(event StreamEventEnvelope) StreamEventEnvelope {
	cloned := event
	if event.Payload == nil {
		return cloned
	}
	clonedPayload := make(map[string]any, len(event.Payload))
	for key, value := range event.Payload {
		clonedPayload[key] = value
	}
	cloned.Payload = clonedPayload
	return cloned
}

func newSnapshotStore(maxSnapshots int) *snapshotStore {
	if maxSnapshots <= 0 {
		maxSnapshots = 50
	}
	return &snapshotStore{
		maxSnapshots: maxSnapshots,
		items:        make([]SnapshotEnvelope, 0, maxSnapshots),
	}
}

func (ss *snapshotStore) capture(label string, payload map[string]any) SnapshotEnvelope {
	if ss == nil {
		return SnapshotEnvelope{}
	}
	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.seq++
	snapshot := SnapshotEnvelope{
		ID:         fmt.Sprintf("snap-%d", ss.seq),
		Label:      strings.TrimSpace(label),
		CapturedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Payload:    cloneMap(payload),
	}
	if len(ss.items) >= ss.maxSnapshots {
		copy(ss.items, ss.items[1:])
		ss.items[len(ss.items)-1] = snapshot
	} else {
		ss.items = append(ss.items, snapshot)
	}
	return snapshot
}

func (ss *snapshotStore) list(limit int) []SnapshotEnvelope {
	if ss == nil {
		return []SnapshotEnvelope{}
	}
	ss.mu.Lock()
	defer ss.mu.Unlock()

	items := make([]SnapshotEnvelope, 0, limit)
	for i := len(ss.items) - 1; i >= 0; i-- {
		items = append(items, SnapshotEnvelope{
			ID:         ss.items[i].ID,
			Label:      ss.items[i].Label,
			CapturedAt: ss.items[i].CapturedAt,
			Payload:    cloneMap(ss.items[i].Payload),
		})
		if limit > 0 && len(items) >= limit {
			break
		}
	}
	return items
}

func (ss *snapshotStore) setMaxSnapshots(maxSnapshots int) int {
	if ss == nil {
		return 0
	}
	ss.mu.Lock()
	defer ss.mu.Unlock()
	if maxSnapshots < 1 {
		maxSnapshots = 1
	}
	if maxSnapshots > 500 {
		maxSnapshots = 500
	}
	ss.maxSnapshots = maxSnapshots
	for len(ss.items) > ss.maxSnapshots {
		ss.items = ss.items[1:]
	}
	return ss.maxSnapshots
}

func (ss *snapshotStore) config() (maxSnapshots int, count int) {
	if ss == nil {
		return 0, 0
	}
	ss.mu.Lock()
	defer ss.mu.Unlock()
	return ss.maxSnapshots, len(ss.items)
}

func (ss *snapshotStore) getByID(id string) (SnapshotEnvelope, bool) {
	if ss == nil {
		return SnapshotEnvelope{}, false
	}
	ss.mu.Lock()
	defer ss.mu.Unlock()
	for i := len(ss.items) - 1; i >= 0; i-- {
		if ss.items[i].ID == id {
			return SnapshotEnvelope{
				ID:         ss.items[i].ID,
				Label:      ss.items[i].Label,
				CapturedAt: ss.items[i].CapturedAt,
				Payload:    cloneMap(ss.items[i].Payload),
			}, true
		}
	}
	return SnapshotEnvelope{}, false
}

func (ss *snapshotStore) latestTwo() (SnapshotEnvelope, SnapshotEnvelope, bool) {
	if ss == nil {
		return SnapshotEnvelope{}, SnapshotEnvelope{}, false
	}
	ss.mu.Lock()
	defer ss.mu.Unlock()
	if len(ss.items) < 2 {
		return SnapshotEnvelope{}, SnapshotEnvelope{}, false
	}
	from := ss.items[len(ss.items)-2]
	to := ss.items[len(ss.items)-1]
	return SnapshotEnvelope{
			ID:         from.ID,
			Label:      from.Label,
			CapturedAt: from.CapturedAt,
			Payload:    cloneMap(from.Payload),
		}, SnapshotEnvelope{
			ID:         to.ID,
			Label:      to.Label,
			CapturedAt: to.CapturedAt,
			Payload:    cloneMap(to.Payload),
		}, true
}

func cloneMap(payload map[string]any) map[string]any {
	if payload == nil {
		return nil
	}
	cloned := make(map[string]any, len(payload))
	for key, value := range payload {
		cloned[key] = value
	}
	return cloned
}

func newSessionStore(maxSessions int) *sessionStore {
	if maxSessions <= 0 {
		maxSessions = 100
	}
	return &sessionStore{
		maxSessions: maxSessions,
		items:       make([]InvestigationSession, 0, maxSessions),
	}
}

func (ss *sessionStore) save(name string, state SessionState) InvestigationSession {
	if ss == nil {
		return InvestigationSession{}
	}
	ss.mu.Lock()
	defer ss.mu.Unlock()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	trimmedName := strings.TrimSpace(name)
	if trimmedName == "" {
		trimmedName = "investigation-session"
	}

	ss.seq++
	session := InvestigationSession{
		ID:        fmt.Sprintf("sess-%d", ss.seq),
		Name:      trimmedName,
		CreatedAt: now,
		UpdatedAt: now,
		State: SessionState{
			SearchQuery:           strings.TrimSpace(state.SearchQuery),
			TimelineCorrelation:   strings.TrimSpace(state.TimelineCorrelation),
			ProvenanceCorrelation: strings.TrimSpace(state.ProvenanceCorrelation),
			SnapshotFromID:        strings.TrimSpace(state.SnapshotFromID),
			SnapshotToID:          strings.TrimSpace(state.SnapshotToID),
			SelectedLayer:         strings.TrimSpace(state.SelectedLayer),
		},
	}

	if len(ss.items) >= ss.maxSessions {
		copy(ss.items, ss.items[1:])
		ss.items[len(ss.items)-1] = session
	} else {
		ss.items = append(ss.items, session)
	}
	return session
}

func (ss *sessionStore) list(limit int) []InvestigationSession {
	if ss == nil {
		return []InvestigationSession{}
	}
	ss.mu.Lock()
	defer ss.mu.Unlock()

	out := make([]InvestigationSession, 0, limit)
	for i := len(ss.items) - 1; i >= 0; i-- {
		item := ss.items[i]
		out = append(out, item)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out
}

func (ss *sessionStore) get(id string) (InvestigationSession, bool) {
	if ss == nil {
		return InvestigationSession{}, false
	}
	ss.mu.Lock()
	defer ss.mu.Unlock()
	for i := len(ss.items) - 1; i >= 0; i-- {
		if ss.items[i].ID == id {
			return ss.items[i], true
		}
	}
	return InvestigationSession{}, false
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r == nil {
		http.Error(w, "request missing", http.StatusBadRequest)
		return
	}
	if r.URL == nil {
		http.Error(w, "request url missing", http.StatusBadRequest)
		return
	}
	path := r.URL.Path
	if path == "" {
		path = "/"
	}
	route := classifyRoute(path)
	startedAt := time.Now()
	recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
	defer func() {
		duration := time.Since(startedAt).Milliseconds()
		portalRequestCount.Add(fmt.Sprintf("%s|%s|%d", r.Method, route, recorder.status), 1)
		portalRouteDurationMS.Add(route, duration)
		log.Printf("portal request method=%s path=%q route=%s status=%d duration_ms=%d", r.Method, path, route, recorder.status, duration)
	}()

	if strings.HasPrefix(path, "/api/v1/") {
		h.handleAPI(recorder, r, strings.TrimPrefix(path, "/api/v1/"))
		return
	}
	if path == "/" || strings.EqualFold(path, "/index.html") {
		recorder.Header().Set("Content-Type", "text/html; charset=utf-8")
		_ = indexTemplate.Execute(recorder, map[string]any{
			"GraphQLPath": h.opts.GraphQLPath,
		})
		return
	}
	if strings.HasPrefix(path, "/assets/") {
		recorder.Header().Set("Cache-Control", "no-cache")
		if applyAssetETag(recorder, r, path) {
			return
		}
	}
	h.files.ServeHTTP(recorder, r)
}

func (h *handler) handleAPI(w http.ResponseWriter, r *http.Request, path string) {
	w.Header().Set("Cache-Control", "no-store")

	// Explorer routes support POST/DELETE, must be checked before the GET-only guard.
	trimmed := strings.Trim(path, "/")
	if h.explorer != nil && h.routeExplorer(w, r, trimmed) {
		return
	}

	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	switch strings.Trim(path, "/") {
	case "health":
		writeJSON(w, http.StatusOK, map[string]any{
			"status":          "ok",
			"gateway_version": h.opts.GatewayVersion,
			"build_id":        h.opts.BuildID,
			"time_utc":        time.Now().UTC().Format(time.RFC3339),
		})
	case "bootstrap":
		streamEnabled := h.opts.ListRegistry != nil || h.opts.ListSemantic != nil || h.opts.ListProjections != nil
		writeJSON(w, http.StatusOK, map[string]any{
			"capabilities": map[string]bool{
				"registry":          h.opts.ListRegistry != nil,
				"semantic":          h.opts.ListSemantic != nil,
				"bus_observability": h.opts.GetBusObservability != nil,
				"projection":        h.opts.ListProjections != nil && h.opts.GetProjection != nil,
				"search":            h.opts.ListRegistry != nil || h.opts.ListSemantic != nil || h.opts.ListProjections != nil,
				"stream":            streamEnabled,
				"timeline":          streamEnabled,
				"provenance":        streamEnabled,
				"snapshots":         streamEnabled,
				"snapshot_diff":     streamEnabled,
				"snapshot_view":     streamEnabled,
				"sessions":          true,
				"issue_builder":     true,
				"migration":         false,
				"explorer":          h.explorer != nil,
			},
			"endpoints": map[string]string{
				"graphql":               h.opts.GraphQLPath,
				"snapshot":              h.opts.SnapshotPath,
				"subscriptions":         h.opts.SubscriptionPath,
				"mcp":                   h.opts.MCPPath,
				"bus_observability":     "/portal/api/v1/bus/observability",
				"search":                "/portal/api/v1/search",
				"stream":                "/portal/api/v1/stream",
				"timeline":              "/portal/api/v1/timeline/events",
				"provenance":            "/portal/api/v1/provenance/events",
				"snapshots":             "/portal/api/v1/snapshots",
				"capture":               "/portal/api/v1/snapshots/capture",
				"retention":             "/portal/api/v1/snapshots/retention",
				"snapshot_diff":         "/portal/api/v1/snapshots/diff",
				"snapshot_view":         "/portal/api/v1/snapshots/view",
				"sessions":              "/portal/api/v1/sessions",
				"session_save":          "/portal/api/v1/sessions/save",
				"session_load":          "/portal/api/v1/sessions/load",
				"issue_draft":           "/portal/api/v1/issues/draft",
				"issue_export":          "/portal/api/v1/issues/export",
				"explorer_scans":        "/portal/api/v1/explorer/scans",
				"explorer_scan_current": "/portal/api/v1/explorer/scans/current",
				"explorer_scan_results": "/portal/api/v1/explorer/scans/current/results",
				"explorer_scan_stream":  "/portal/api/v1/explorer/scans/current/stream",
				"explorer_read_b524":    "/portal/api/v1/explorer/read/b524",
				"explorer_read_b509":    "/portal/api/v1/explorer/read/b509",
				"explorer_read_scanid":  "/portal/api/v1/explorer/read/scanid",
			},
			"limits": map[string]any{
				"max_events_per_second": 200,
				"snapshot_retention":    "disabled_in_m0",
			},
			"ui_version": "m0",
		})
	case "registry/devices":
		h.handleRegistryDevices(w, r)
	case "semantic/snapshot":
		h.handleSemanticSnapshot(w)
	case "bus/observability":
		h.handleBusObservability(w)
	case "projection/devices":
		h.handleProjectionDevices(w, r)
	case "projection/graph":
		h.handleProjectionGraph(w, r)
	case "search":
		h.handleSearch(w, r)
	case "stream":
		h.handleStream(w, r)
	case "timeline/events":
		h.handleTimelineEvents(w, r)
	case "provenance/events":
		h.handleProvenanceEvents(w, r)
	case "snapshots":
		h.handleSnapshotsList(w, r)
	case "snapshots/capture":
		h.handleSnapshotsCapture(w, r)
	case "snapshots/retention":
		h.handleSnapshotsRetention(w, r)
	case "snapshots/view":
		h.handleSnapshotView(w, r)
	case "snapshots/diff":
		h.handleSnapshotsDiff(w, r)
	case "sessions":
		h.handleSessionsList(w, r)
	case "sessions/save":
		h.handleSessionsSave(w, r)
	case "sessions/load":
		h.handleSessionsLoad(w, r)
	case "issues/draft":
		h.handleIssueDraft(w, r)
	case "issues/export":
		h.handleIssueExport(w, r)
	default:
		http.NotFound(w, r)
	}
}

func writeJSON(w http.ResponseWriter, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(payload)
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (rec *statusRecorder) WriteHeader(code int) {
	rec.status = code
	rec.ResponseWriter.WriteHeader(code)
}

func (rec *statusRecorder) Write(payload []byte) (int, error) {
	if rec.status == 0 {
		rec.status = http.StatusOK
	}
	return rec.ResponseWriter.Write(payload)
}

func (rec *statusRecorder) Flush() {
	if flusher, ok := rec.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func classifyRoute(path string) string {
	switch {
	case strings.HasPrefix(path, "/api/v1/health"):
		return "api.health"
	case strings.HasPrefix(path, "/api/v1/bootstrap"):
		return "api.bootstrap"
	case strings.HasPrefix(path, "/api/v1/registry/devices"):
		return "api.registry.devices"
	case strings.HasPrefix(path, "/api/v1/semantic/snapshot"):
		return "api.semantic.snapshot"
	case strings.HasPrefix(path, "/api/v1/bus/observability"):
		return "api.bus.observability"
	case strings.HasPrefix(path, "/api/v1/projection/devices"):
		return "api.projection.devices"
	case strings.HasPrefix(path, "/api/v1/projection/graph"):
		return "api.projection.graph"
	case strings.HasPrefix(path, "/api/v1/search"):
		return "api.search"
	case strings.HasPrefix(path, "/api/v1/stream"):
		return "api.stream"
	case strings.HasPrefix(path, "/api/v1/timeline/events"):
		return "api.timeline.events"
	case strings.HasPrefix(path, "/api/v1/provenance/events"):
		return "api.provenance.events"
	case strings.HasPrefix(path, "/api/v1/snapshots/capture"):
		return "api.snapshots.capture"
	case strings.HasPrefix(path, "/api/v1/snapshots/retention"):
		return "api.snapshots.retention"
	case strings.HasPrefix(path, "/api/v1/snapshots/view"):
		return "api.snapshots.view"
	case strings.HasPrefix(path, "/api/v1/snapshots/diff"):
		return "api.snapshots.diff"
	case strings.HasPrefix(path, "/api/v1/snapshots"):
		return "api.snapshots.list"
	case strings.HasPrefix(path, "/api/v1/sessions/save"):
		return "api.sessions.save"
	case strings.HasPrefix(path, "/api/v1/sessions/load"):
		return "api.sessions.load"
	case strings.HasPrefix(path, "/api/v1/sessions"):
		return "api.sessions.list"
	case strings.HasPrefix(path, "/api/v1/issues/draft"):
		return "api.issues.draft"
	case strings.HasPrefix(path, "/api/v1/issues/export"):
		return "api.issues.export"
	case strings.HasPrefix(path, "/api/v1/explorer/"):
		return "api.explorer"
	case strings.HasPrefix(path, "/assets/"):
		return "assets"
	case path == "/" || strings.EqualFold(path, "/index.html"):
		return "index"
	default:
		return "static"
	}
}

func loadAssetETags() map[string]string {
	raw, err := assets.ReadFile("static/assets/manifest.json")
	if err != nil {
		return nil
	}
	var manifest assetManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return nil
	}
	etags := make(map[string]string, len(manifest.Assets))
	for name, entry := range manifest.Assets {
		hash := strings.TrimSpace(entry.SHA256)
		if hash == "" {
			continue
		}
		etags[name] = fmt.Sprintf("\"sha256-%s\"", hash)
	}
	return etags
}

func applyAssetETag(w http.ResponseWriter, r *http.Request, assetPath string) bool {
	if len(portalAssetETagByTarget) == 0 || r == nil {
		return false
	}
	name := path.Base(assetPath)
	etag, ok := portalAssetETagByTarget[name]
	if !ok {
		return false
	}
	w.Header().Set("ETag", etag)
	if strings.TrimSpace(r.Header.Get("If-None-Match")) == etag {
		w.WriteHeader(http.StatusNotModified)
		return true
	}
	return false
}

func (h *handler) handleRegistryDevices(w http.ResponseWriter, r *http.Request) {
	if h.opts.ListRegistry == nil {
		writeJSON(w, http.StatusOK, map[string]any{
			"count": 0,
			"items": []RegistryDevice{},
		})
		return
	}

	devices := h.opts.ListRegistry()
	needle := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("q")))
	limit := parseQueryLimit(r.URL.Query().Get("limit"), 200)
	if limit < 0 {
		limit = 0
	}

	filtered := make([]RegistryDevice, 0, len(devices))
	for _, device := range devices {
		if needle != "" && !matchesDeviceFilter(device, needle) {
			continue
		}
		filtered = append(filtered, device)
	}

	slices.SortFunc(filtered, func(a, b RegistryDevice) int {
		return cmp.Compare(int(a.Address), int(b.Address))
	})

	total := len(filtered)
	if limit > 0 && len(filtered) > limit {
		filtered = filtered[:limit]
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"count": total,
		"items": filtered,
	})
}

func parseQueryLimit(raw string, fallback int) int {
	if strings.TrimSpace(raw) == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	switch {
	case value <= 0:
		return 0
	case value > 1000:
		return 1000
	default:
		return value
	}
}

func matchesDeviceFilter(device RegistryDevice, needle string) bool {
	if needle == "" {
		return true
	}
	candidate := strings.ToLower(strings.Join([]string{
		device.Manufacturer,
		device.DeviceID,
		device.DisplayName,
		device.Role,
		device.SerialNumber,
		device.Software,
		device.Hardware,
	}, " "))
	if strings.Contains(candidate, needle) {
		return true
	}
	for _, plane := range device.Planes {
		if strings.Contains(strings.ToLower(plane.Name), needle) {
			return true
		}
		for _, method := range plane.Methods {
			if strings.Contains(strings.ToLower(method), needle) {
				return true
			}
		}
	}
	return false
}

func (h *handler) handleSemanticSnapshot(w http.ResponseWriter) {
	if h.opts.ListSemantic == nil {
		writeJSON(w, http.StatusOK, map[string]any{
			"zones":        []SemanticZone{},
			"captured_utc": time.Now().UTC().Format(time.RFC3339),
		})
		return
	}
	snapshot := h.opts.ListSemantic()
	if strings.TrimSpace(snapshot.CapturedUTC) == "" {
		snapshot.CapturedUTC = time.Now().UTC().Format(time.RFC3339)
	}
	if snapshot.Zones == nil {
		snapshot.Zones = []SemanticZone{}
	}
	writeJSON(w, http.StatusOK, snapshot)
}

func (h *handler) handleBusObservability(w http.ResponseWriter) {
	if h.opts.GetBusObservability == nil {
		http.Error(w, "bus observability unavailable", http.StatusServiceUnavailable)
		return
	}
	payload := h.opts.GetBusObservability()
	if payload == nil {
		http.Error(w, "bus observability unavailable", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, http.StatusOK, payload)
}

func (h *handler) handleProjectionDevices(w http.ResponseWriter, r *http.Request) {
	if h.opts.ListProjections == nil {
		writeJSON(w, http.StatusOK, map[string]any{
			"count": 0,
			"items": []ProjectionDevice{},
		})
		return
	}
	items := h.opts.ListProjections()
	needle := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("q")))
	limit := parseQueryLimit(r.URL.Query().Get("limit"), 200)

	filtered := make([]ProjectionDevice, 0, len(items))
	for _, item := range items {
		if needle != "" && !matchesProjectionFilter(item, needle) {
			continue
		}
		filtered = append(filtered, item)
	}
	slices.SortFunc(filtered, func(a, b ProjectionDevice) int {
		return cmp.Compare(int(a.Address), int(b.Address))
	})

	total := len(filtered)
	if limit > 0 && len(filtered) > limit {
		filtered = filtered[:limit]
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"count": total,
		"items": filtered,
	})
}

func (h *handler) handleProjectionGraph(w http.ResponseWriter, r *http.Request) {
	if h.opts.GetProjection == nil {
		http.NotFound(w, r)
		return
	}
	address, err := parseQueryAddress(r.URL.Query().Get("address"))
	if err != nil {
		http.Error(w, "invalid address", http.StatusBadRequest)
		return
	}
	plane := strings.TrimSpace(r.URL.Query().Get("plane"))
	if plane == "" {
		http.Error(w, "missing plane", http.StatusBadRequest)
		return
	}
	graph, ok := h.opts.GetProjection(address, plane)
	if !ok {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, http.StatusOK, graph)
}

func parseQueryAddress(raw string) (byte, error) {
	parsed, err := strconv.ParseUint(strings.TrimSpace(raw), 0, 8)
	if err != nil {
		return 0, err
	}
	return byte(parsed), nil
}

func matchesProjectionFilter(item ProjectionDevice, needle string) bool {
	text := strings.ToLower(strings.Join([]string{
		item.DeviceID,
		item.DisplayName,
		item.Manufacturer,
	}, " "))
	if strings.Contains(text, needle) {
		return true
	}
	for _, projection := range item.Projections {
		if strings.Contains(strings.ToLower(projection.Plane), needle) {
			return true
		}
	}
	return false
}

func (h *handler) handleSearch(w http.ResponseWriter, r *http.Request) {
	needle := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("q")))
	limit := parseQueryLimit(r.URL.Query().Get("limit"), 25)
	if needle == "" {
		writeJSON(w, http.StatusOK, map[string]any{
			"query": needle,
			"count": 0,
			"items": []SearchResult{},
		})
		return
	}

	results := make([]SearchResult, 0, limit)
	appendResult := func(item SearchResult) {
		if limit > 0 && len(results) >= limit {
			return
		}
		results = append(results, item)
	}

	if h.opts.ListRegistry != nil {
		devices := h.opts.ListRegistry()
		slices.SortFunc(devices, func(a, b RegistryDevice) int {
			return cmp.Compare(int(a.Address), int(b.Address))
		})
		for _, device := range devices {
			label := strings.TrimSpace(strings.Join([]string{device.Manufacturer, device.DeviceID}, " "))
			if strings.Contains(strings.ToLower(strings.Join([]string{
				device.Manufacturer,
				device.DeviceID,
				device.SerialNumber,
				device.Software,
				device.Hardware,
			}, " ")), needle) {
				address := device.Address
				appendResult(SearchResult{
					Layer:    "registry",
					Kind:     "device",
					ID:       fmt.Sprintf("reg:%02x", device.Address),
					Title:    strings.TrimSpace(label),
					Subtitle: fmt.Sprintf("addr=0x%02x", device.Address),
					Address:  &address,
				})
			}
			for _, plane := range device.Planes {
				if strings.Contains(strings.ToLower(plane.Name), needle) {
					address := device.Address
					appendResult(SearchResult{
						Layer:    "registry",
						Kind:     "plane",
						ID:       fmt.Sprintf("reg:%02x:%s", device.Address, strings.ToLower(plane.Name)),
						Title:    fmt.Sprintf("%s plane", plane.Name),
						Subtitle: fmt.Sprintf("addr=0x%02x", device.Address),
						Address:  &address,
					})
				}
				for _, method := range plane.Methods {
					if strings.Contains(strings.ToLower(method), needle) {
						address := device.Address
						appendResult(SearchResult{
							Layer:    "registry",
							Kind:     "method",
							ID:       fmt.Sprintf("reg:%02x:%s:%s", device.Address, strings.ToLower(plane.Name), strings.ToLower(method)),
							Title:    method,
							Subtitle: fmt.Sprintf("%s plane addr=0x%02x", plane.Name, device.Address),
							Address:  &address,
						})
					}
				}
			}
		}
	}

	if h.opts.ListSemantic != nil {
		snapshot := h.opts.ListSemantic()
		for _, zone := range snapshot.Zones {
			if strings.Contains(strings.ToLower(strings.Join([]string{
				zone.ID,
				zone.Name,
				zone.Config.OperatingMode,
				zone.Config.Preset,
			}, " ")), needle) {
				appendResult(SearchResult{
					Layer:    "semantic",
					Kind:     "zone",
					ID:       zone.ID,
					Title:    zone.Name,
					Subtitle: strings.TrimSpace(strings.Join([]string{zone.Config.OperatingMode, zone.Config.Preset}, " / ")),
				})
			}
		}
		if snapshot.DHW != nil && strings.Contains(strings.ToLower(strings.Join([]string{
			"dhw",
			"domestic hot water",
			snapshot.DHW.Config.OperatingMode,
			snapshot.DHW.Config.Preset,
		}, " ")), needle) {
			appendResult(SearchResult{
				Layer:    "semantic",
				Kind:     "dhw",
				ID:       "dhw",
				Title:    "Domestic Hot Water",
				Subtitle: strings.TrimSpace(strings.Join([]string{snapshot.DHW.Config.OperatingMode, snapshot.DHW.Config.Preset}, " / ")),
			})
		}
		if snapshot.Energy != nil && semanticSearchMatch(needle, "energy", "gas", "electric", "solar", "dhw", "climate") {
			appendResult(SearchResult{
				Layer:    "semantic",
				Kind:     "energy",
				ID:       "energy",
				Title:    "Energy Totals",
				Subtitle: "gas / electric / solar",
			})
		}
		if snapshot.BoilerStatus != nil && semanticSearchMatch(needle, "boiler", derefString(snapshot.BoilerStatus.Config.DhwOperatingMode)) {
			appendResult(SearchResult{
				Layer:    "semantic",
				Kind:     "boiler",
				ID:       "boiler",
				Title:    "Boiler",
				Subtitle: strings.TrimSpace(strings.Join([]string{"dhw_mode=" + defaultText(derefString(snapshot.BoilerStatus.Config.DhwOperatingMode), "n/a")}, "")),
			})
		}
		if snapshot.System != nil && semanticSearchMatch(needle, "system", semanticIntString(snapshot.System.Properties.SystemScheme), semanticIntString(snapshot.System.Properties.ModuleConfigurationVR71)) {
			appendResult(SearchResult{
				Layer:    "semantic",
				Kind:     "system",
				ID:       "system",
				Title:    "System",
				Subtitle: fmt.Sprintf("scheme=%s", defaultText(semanticIntString(snapshot.System.Properties.SystemScheme), "n/a")),
			})
		}
		for _, circuit := range snapshot.Circuits {
			if !semanticSearchMatch(needle, "circuit", semanticInt(circuit.Index), circuit.CircuitType, circuit.State.CircuitState, circuit.Config.RoomTempControl) {
				continue
			}
			appendResult(SearchResult{
				Layer:    "semantic",
				Kind:     "circuit",
				ID:       fmt.Sprintf("circuit:%d", circuit.Index),
				Title:    fmt.Sprintf("Circuit %d", circuit.Index),
				Subtitle: strings.TrimSpace(strings.Join([]string{circuit.CircuitType, circuit.State.CircuitState}, " / ")),
			})
		}
		for _, device := range snapshot.RadioDevices {
			fields := []string{
				"radio",
				device.DeviceModel,
				device.SlotMode,
				semanticIntString(device.ZoneAssignment),
				derefString(device.FirmwareVersion),
			}
			if !semanticSearchMatch(needle, fields...) {
				continue
			}
			title := strings.TrimSpace(device.DeviceModel)
			if title == "" {
				title = fmt.Sprintf("Radio %d/%d", device.Group, device.Instance)
			} else {
				title = "Radio " + title
			}
			appendResult(SearchResult{
				Layer:    "semantic",
				Kind:     "radio",
				ID:       fmt.Sprintf("radio:%d:%d", device.Group, device.Instance),
				Title:    title,
				Subtitle: fmt.Sprintf("slot=%s zone=%s", defaultText(device.SlotMode, "n/a"), defaultText(semanticIntString(device.ZoneAssignment), "n/a")),
			})
		}
		if semanticSearchMatch(needle, "fm5", snapshot.FM5Mode) {
			appendResult(SearchResult{
				Layer:    "semantic",
				Kind:     "fm5",
				ID:       "fm5",
				Title:    "FM5",
				Subtitle: defaultText(snapshot.FM5Mode, "n/a"),
			})
		}
		if snapshot.Solar != nil && semanticSearchMatch(needle, "solar", semanticBoolString(snapshot.Solar.SolarEnabled), semanticBoolString(snapshot.Solar.PumpActive)) {
			appendResult(SearchResult{
				Layer:    "semantic",
				Kind:     "solar",
				ID:       "solar",
				Title:    "Solar",
				Subtitle: fmt.Sprintf("collector=%s", defaultText(semanticFloatString(snapshot.Solar.CollectorTemperatureC, 1), "n/a")),
			})
		}
		for _, cylinder := range snapshot.Cylinders {
			if !semanticSearchMatch(needle, "cylinder", semanticInt(cylinder.Index)) {
				continue
			}
			appendResult(SearchResult{
				Layer:    "semantic",
				Kind:     "cylinder",
				ID:       fmt.Sprintf("cylinder:%d", cylinder.Index),
				Title:    fmt.Sprintf("Cylinder %d", cylinder.Index),
				Subtitle: fmt.Sprintf("temp=%s", defaultText(semanticFloatString(cylinder.TemperatureC, 1), "n/a")),
			})
		}
	}

	if h.opts.ListProjections != nil {
		items := h.opts.ListProjections()
		slices.SortFunc(items, func(a, b ProjectionDevice) int {
			return cmp.Compare(int(a.Address), int(b.Address))
		})
		for _, item := range items {
			if strings.Contains(strings.ToLower(strings.Join([]string{
				item.DeviceID,
				item.DisplayName,
				item.Manufacturer,
			}, " ")), needle) {
				title := strings.TrimSpace(item.DisplayName)
				if title == "" {
					title = strings.TrimSpace(item.DeviceID)
				}
				if title == "" {
					title = fmt.Sprintf("Projection Device 0x%02x", item.Address)
				}
				projAddr := int(item.Address)
				appendResult(SearchResult{
					Layer:    "projection",
					Kind:     "device",
					ID:       fmt.Sprintf("proj:%02x", item.Address),
					Title:    title,
					Subtitle: fmt.Sprintf("addr=0x%02x", item.Address),
					Address:  &projAddr,
				})
			}
			for _, projection := range item.Projections {
				if strings.Contains(strings.ToLower(projection.Plane), needle) {
					planeAddr := int(item.Address)
					appendResult(SearchResult{
						Layer:    "projection",
						Kind:     "plane",
						ID:       fmt.Sprintf("proj:%02x:%s", item.Address, strings.ToLower(projection.Plane)),
						Title:    projection.Plane,
						Subtitle: fmt.Sprintf("addr=0x%02x nodes=%d edges=%d", item.Address, projection.NodeCount, projection.EdgeCount),
						Address:  &planeAddr,
					})
				}
			}
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"query": needle,
		"count": len(results),
		"items": results,
	})
}

func semanticSearchMatch(needle string, fields ...string) bool {
	if needle == "" {
		return false
	}
	return strings.Contains(strings.ToLower(strings.Join(fields, " ")), needle)
}

func semanticInt(value int) string {
	return strconv.Itoa(value)
}

func semanticIntString(value *int) string {
	if value == nil {
		return ""
	}
	return strconv.Itoa(*value)
}

func semanticFloatString(value *float64, digits int) string {
	if value == nil {
		return ""
	}
	return strconv.FormatFloat(*value, 'f', digits, 64)
}

func semanticBoolString(value *bool) string {
	if value == nil {
		return ""
	}
	if *value {
		return "true"
	}
	return "false"
}

func derefString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func (h *handler) handleTimelineEvents(w http.ResponseWriter, r *http.Request) {
	if h.timeline == nil {
		writeJSON(w, http.StatusOK, map[string]any{
			"count": 0,
			"items": []StreamEventEnvelope{},
		})
		return
	}

	limit := parseQueryLimit(r.URL.Query().Get("limit"), 100)
	layer := strings.TrimSpace(r.URL.Query().Get("layer"))
	correlationID := strings.TrimSpace(r.URL.Query().Get("correlation_id"))
	sinceRaw := strings.TrimSpace(r.URL.Query().Get("since"))
	var since time.Time
	if sinceRaw != "" {
		parsed, err := time.Parse(time.RFC3339, sinceRaw)
		if err != nil {
			parsed, err = time.Parse(time.RFC3339Nano, sinceRaw)
		}
		if err != nil {
			http.Error(w, "invalid since timestamp", http.StatusBadRequest)
			return
		}
		since = parsed
	}

	items := h.timeline.query(limit, layer, correlationID, since)
	writeJSON(w, http.StatusOK, map[string]any{
		"count": len(items),
		"items": items,
	})
}

func (h *handler) handleProvenanceEvents(w http.ResponseWriter, r *http.Request) {
	if h.timeline == nil {
		writeJSON(w, http.StatusOK, map[string]any{
			"count": 0,
			"items": []ProvenanceRecord{},
		})
		return
	}
	limit := parseQueryLimit(r.URL.Query().Get("limit"), 50)
	layer := strings.TrimSpace(r.URL.Query().Get("layer"))
	correlationID := strings.TrimSpace(r.URL.Query().Get("correlation_id"))
	items := h.timeline.query(limit, layer, correlationID, time.Time{})

	records := make([]ProvenanceRecord, 0, len(items))
	for _, item := range items {
		records = append(records, toProvenanceRecord(item))
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"count": len(records),
		"items": records,
	})
}

func (h *handler) handleSnapshotsList(w http.ResponseWriter, r *http.Request) {
	if h.snapshots == nil {
		writeJSON(w, http.StatusOK, map[string]any{
			"count": 0,
			"items": []SnapshotEnvelope{},
		})
		return
	}
	limit := parseQueryLimit(r.URL.Query().Get("limit"), 20)
	items := h.snapshots.list(limit)
	maxSnapshots, count := h.snapshots.config()
	writeJSON(w, http.StatusOK, map[string]any{
		"count":         len(items),
		"stored_count":  count,
		"max_snapshots": maxSnapshots,
		"items":         items,
	})
}

func (h *handler) handleSnapshotView(w http.ResponseWriter, r *http.Request) {
	if h.snapshots == nil {
		http.Error(w, "snapshot store unavailable", http.StatusServiceUnavailable)
		return
	}
	id := strings.TrimSpace(r.URL.Query().Get("id"))
	if id == "" {
		http.Error(w, "missing id parameter", http.StatusBadRequest)
		return
	}
	snapshot, ok := h.snapshots.getByID(id)
	if !ok {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, http.StatusOK, snapshot)
}

func (h *handler) handleSnapshotsCapture(w http.ResponseWriter, r *http.Request) {
	if h.snapshots == nil {
		http.Error(w, "snapshot store unavailable", http.StatusServiceUnavailable)
		return
	}
	if h.opts.ListRegistry == nil && h.opts.ListSemantic == nil && h.opts.ListProjections == nil {
		http.Error(w, "snapshot capture unavailable", http.StatusServiceUnavailable)
		return
	}
	label := strings.TrimSpace(r.URL.Query().Get("label"))
	snapshot := h.snapshots.capture(label, h.buildSnapshotPayload())
	maxSnapshots, count := h.snapshots.config()
	writeJSON(w, http.StatusOK, map[string]any{
		"snapshot":      snapshot,
		"stored_count":  count,
		"max_snapshots": maxSnapshots,
	})
}

func (h *handler) handleSnapshotsRetention(w http.ResponseWriter, r *http.Request) {
	if h.snapshots == nil {
		http.Error(w, "snapshot store unavailable", http.StatusServiceUnavailable)
		return
	}
	if raw := strings.TrimSpace(r.URL.Query().Get("max_snapshots")); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil {
			http.Error(w, "invalid max_snapshots", http.StatusBadRequest)
			return
		}
		h.snapshots.setMaxSnapshots(value)
	}
	maxSnapshots, count := h.snapshots.config()
	writeJSON(w, http.StatusOK, map[string]any{
		"max_snapshots": maxSnapshots,
		"stored_count":  count,
	})
}

func (h *handler) handleSnapshotsDiff(w http.ResponseWriter, r *http.Request) {
	if h.snapshots == nil {
		http.Error(w, "snapshot store unavailable", http.StatusServiceUnavailable)
		return
	}
	fromID := strings.TrimSpace(r.URL.Query().Get("from_id"))
	toID := strings.TrimSpace(r.URL.Query().Get("to_id"))

	var (
		from SnapshotEnvelope
		to   SnapshotEnvelope
		ok   bool
	)
	switch {
	case fromID == "" && toID == "":
		from, to, ok = h.snapshots.latestTwo()
		if !ok {
			http.Error(w, "need at least two snapshots", http.StatusNotFound)
			return
		}
	case fromID == "" || toID == "":
		http.Error(w, "both from_id and to_id are required", http.StatusBadRequest)
		return
	default:
		from, ok = h.snapshots.getByID(fromID)
		if !ok {
			http.Error(w, "from snapshot not found", http.StatusNotFound)
			return
		}
		to, ok = h.snapshots.getByID(toID)
		if !ok {
			http.Error(w, "to snapshot not found", http.StatusNotFound)
			return
		}
	}

	limit := parseQueryLimit(r.URL.Query().Get("limit"), 200)
	diffEntries := buildSnapshotDiffEntries(from.Payload, to.Payload)
	totalChanges := len(diffEntries)
	if limit > 0 && len(diffEntries) > limit {
		diffEntries = diffEntries[:limit]
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"from_snapshot": map[string]any{
			"id":          from.ID,
			"label":       from.Label,
			"captured_at": from.CapturedAt,
		},
		"to_snapshot": map[string]any{
			"id":          to.ID,
			"label":       to.Label,
			"captured_at": to.CapturedAt,
		},
		"change_count": totalChanges,
		"count":        len(diffEntries),
		"items":        diffEntries,
	})
}

func (h *handler) handleSessionsList(w http.ResponseWriter, r *http.Request) {
	if h.sessions == nil {
		writeJSON(w, http.StatusOK, map[string]any{
			"count": 0,
			"items": []InvestigationSession{},
		})
		return
	}
	limit := parseQueryLimit(r.URL.Query().Get("limit"), 30)
	items := h.sessions.list(limit)
	writeJSON(w, http.StatusOK, map[string]any{
		"count": len(items),
		"items": items,
	})
}

func (h *handler) handleSessionsSave(w http.ResponseWriter, r *http.Request) {
	if h.sessions == nil {
		http.Error(w, "session store unavailable", http.StatusServiceUnavailable)
		return
	}
	state := SessionState{
		SearchQuery:           r.URL.Query().Get("search_query"),
		TimelineCorrelation:   r.URL.Query().Get("timeline_correlation"),
		ProvenanceCorrelation: r.URL.Query().Get("provenance_correlation"),
		SnapshotFromID:        r.URL.Query().Get("snapshot_from_id"),
		SnapshotToID:          r.URL.Query().Get("snapshot_to_id"),
		SelectedLayer:         r.URL.Query().Get("selected_layer"),
	}
	saved := h.sessions.save(r.URL.Query().Get("name"), state)
	writeJSON(w, http.StatusOK, map[string]any{
		"session": saved,
	})
}

func (h *handler) handleSessionsLoad(w http.ResponseWriter, r *http.Request) {
	if h.sessions == nil {
		http.Error(w, "session store unavailable", http.StatusServiceUnavailable)
		return
	}
	id := strings.TrimSpace(r.URL.Query().Get("id"))
	if id == "" {
		http.Error(w, "missing id", http.StatusBadRequest)
		return
	}
	session, ok := h.sessions.get(id)
	if !ok {
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"session": session,
	})
}

func (h *handler) handleIssueDraft(w http.ResponseWriter, r *http.Request) {
	draft := h.buildIssueDraft(r)
	writeJSON(w, http.StatusOK, draft)
}

func (h *handler) handleIssueExport(w http.ResponseWriter, r *http.Request) {
	draft := h.buildIssueDraft(r)
	bundle := map[string]any{
		"format_version": "helianthus-issue-bundle/v1",
		"generated_at":   time.Now().UTC().Format(time.RFC3339Nano),
		"title":          draft.Title,
		"markdown":       draft.Markdown,
		"evidence":       draft.Evidence,
		"filename_hint":  sanitizeFilename(draft.Title) + ".json",
	}
	writeJSON(w, http.StatusOK, bundle)
}

func (h *handler) buildIssueDraft(r *http.Request) IssueDraft {
	query := r.URL.Query()
	title := strings.TrimSpace(query.Get("title"))
	if title == "" {
		title = "Vaillant semantic mapping candidate"
	}
	observation := defaultText(query.Get("observation"), "Observed behavior requiring semantic mapping.")
	repro := defaultText(query.Get("reproduction_steps"), "1) Open portal 2) Capture evidence 3) Compare expected vs actual behavior")
	hypothesis := defaultText(query.Get("hypothesis"), "Potential register/method meaning inferred from correlated evidence.")
	impact := defaultText(query.Get("impact"), "Incorrect/unknown mapping blocks reliable gateway semantic extension.")
	proposal := defaultText(query.Get("proposal"), "Implement semantic mapping in gateway provider and expose through GraphQL contract.")
	ac := defaultText(query.Get("acceptance_criteria"), "1) Deterministic mapping 2) Contract fields populated 3) Tests and docs updated")
	controller := defaultText(query.Get("controller"), "unknown-controller")
	device := defaultText(query.Get("device"), "unknown-device")

	evidence := h.collectIssueEvidence()
	prettyEvidence, _ := json.MarshalIndent(evidence, "", "  ")
	markdown := strings.Join([]string{
		"# " + title,
		"",
		"## 1) Context",
		fmt.Sprintf("- gateway_version: `%s`", h.opts.GatewayVersion),
		fmt.Sprintf("- build_id: `%s`", h.opts.BuildID),
		fmt.Sprintf("- controller: `%s`", controller),
		fmt.Sprintf("- device: `%s`", device),
		"",
		"## 2) Observation & Goal",
		observation,
		"",
		"## 3) Reproduction Steps",
		repro,
		"",
		"## 4) Evidence",
		"- snapshots: included in JSON bundle",
		"- timeline/provenance: included in JSON bundle",
		"```json",
		string(prettyEvidence),
		"```",
		"",
		"## 5) Semantic Hypothesis",
		hypothesis,
		"",
		"## 6) Impact",
		impact,
		"",
		"## 7) Proposed Implementation",
		proposal,
		"",
		"## 8) Acceptance Criteria",
		ac,
		"",
	}, "\n")

	return IssueDraft{
		Title:    title,
		Markdown: markdown,
		Evidence: evidence,
	}
}

func (h *handler) collectIssueEvidence() map[string]any {
	evidence := map[string]any{
		"captured_at": time.Now().UTC().Format(time.RFC3339Nano),
	}
	if h.snapshots != nil {
		evidence["snapshots"] = h.snapshots.list(3)
	}
	if h.timeline != nil {
		events := h.timeline.query(20, "", "", time.Time{})
		evidence["timeline"] = events
		provenance := make([]ProvenanceRecord, 0, len(events))
		for _, event := range events {
			provenance = append(provenance, toProvenanceRecord(event))
		}
		evidence["provenance"] = provenance
	}
	if h.opts.ListRegistry != nil {
		evidence["registry_devices"] = h.opts.ListRegistry()
	}
	return evidence
}

func defaultText(value, fallback string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return fallback
	}
	return trimmed
}

func sanitizeFilename(value string) string {
	trimmed := strings.TrimSpace(strings.ToLower(value))
	if trimmed == "" {
		return "issue-draft"
	}
	var builder strings.Builder
	for _, ch := range trimmed {
		if (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') {
			builder.WriteRune(ch)
			continue
		}
		if ch == '-' || ch == '_' || ch == ' ' {
			builder.WriteByte('-')
		}
	}
	out := strings.Trim(builder.String(), "-")
	if out == "" {
		return "issue-draft"
	}
	return out
}

func buildSnapshotDiffEntries(fromPayload map[string]any, toPayload map[string]any) []SnapshotDiffEntry {
	fromFlat := flattenSnapshotPayload(fromPayload)
	toFlat := flattenSnapshotPayload(toPayload)

	pathsMap := make(map[string]struct{}, len(fromFlat)+len(toFlat))
	for path := range fromFlat {
		pathsMap[path] = struct{}{}
	}
	for path := range toFlat {
		pathsMap[path] = struct{}{}
	}
	paths := make([]string, 0, len(pathsMap))
	for path := range pathsMap {
		paths = append(paths, path)
	}
	slices.Sort(paths)

	entries := make([]SnapshotDiffEntry, 0, len(paths))
	for _, path := range paths {
		fromValue, fromOK := fromFlat[path]
		toValue, toOK := toFlat[path]
		switch {
		case !fromOK && toOK:
			entries = append(entries, SnapshotDiffEntry{
				Path:   path,
				Change: "added",
				To:     toValue,
			})
		case fromOK && !toOK:
			entries = append(entries, SnapshotDiffEntry{
				Path:   path,
				Change: "removed",
				From:   fromValue,
			})
		case fromOK && toOK && fromValue != toValue:
			entries = append(entries, SnapshotDiffEntry{
				Path:   path,
				Change: "changed",
				From:   fromValue,
				To:     toValue,
			})
		}
	}
	return entries
}

func flattenSnapshotPayload(payload map[string]any) map[string]string {
	normalized := normalizePayload(payload)
	out := make(map[string]string)
	flattenValue("$", normalized, out)
	return out
}

func normalizePayload(payload map[string]any) map[string]any {
	if payload == nil {
		return map[string]any{}
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return cloneMap(payload)
	}
	var normalized map[string]any
	if err := json.Unmarshal(raw, &normalized); err != nil {
		return cloneMap(payload)
	}
	return normalized
}

func flattenValue(path string, value any, out map[string]string) {
	switch typed := value.(type) {
	case map[string]any:
		if len(typed) == 0 {
			out[path] = "{}"
			return
		}
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		slices.Sort(keys)
		for _, key := range keys {
			flattenValue(path+"."+key, typed[key], out)
		}
	case []any:
		if len(typed) == 0 {
			out[path] = "[]"
			return
		}
		for idx, item := range typed {
			flattenValue(fmt.Sprintf("%s[%d]", path, idx), item, out)
		}
	default:
		encoded, err := json.Marshal(typed)
		if err != nil {
			out[path] = fmt.Sprintf("%v", typed)
			return
		}
		out[path] = string(encoded)
	}
}

func (h *handler) buildSnapshotPayload() map[string]any {
	payload := map[string]any{
		"captured_at": time.Now().UTC().Format(time.RFC3339Nano),
	}
	if h.opts.ListRegistry != nil {
		devices := h.opts.ListRegistry()
		payload["registry"] = map[string]any{
			"count": len(devices),
			"items": devices,
		}
	}
	if h.opts.ListSemantic != nil {
		semantic := h.opts.ListSemantic()
		payload["semantic"] = semantic
	}
	if h.opts.ListProjections != nil {
		projections := h.opts.ListProjections()
		payload["projection"] = map[string]any{
			"count": len(projections),
			"items": projections,
		}
	}
	if h.timeline != nil {
		events := h.timeline.query(20, "", "", time.Time{})
		provenance := make([]ProvenanceRecord, 0, len(events))
		for _, event := range events {
			provenance = append(provenance, toProvenanceRecord(event))
		}
		payload["timeline"] = map[string]any{
			"count": len(events),
			"items": events,
		}
		payload["provenance"] = map[string]any{
			"count": len(provenance),
			"items": provenance,
		}
	}
	return payload
}

func toProvenanceRecord(event StreamEventEnvelope) ProvenanceRecord {
	keys := make([]string, 0, len(event.Payload))
	for key := range event.Payload {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return ProvenanceRecord{
		CorrelationID: event.CorrelationID,
		Layer:         event.Layer,
		At:            event.At,
		Source:        event.Provenance.Source,
		Dropped:       event.Provenance.Dropped,
		IntervalMS:    event.Provenance.Interval,
		DecodePath: []string{
			fmt.Sprintf("source:%s", event.Provenance.Source),
			fmt.Sprintf("layer:%s", event.Layer),
			"gateway.portal.stream",
			"gateway.portal.timeline",
		},
		PayloadKeys: keys,
		Confidence:  0.7,
	}
}

func (h *handler) handleStream(w http.ResponseWriter, r *http.Request) {
	if h.opts.ListRegistry == nil && h.opts.ListSemantic == nil && h.opts.ListProjections == nil {
		http.Error(w, "stream unavailable", http.StatusServiceUnavailable)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	intervalMS := parseIntBounded(r.URL.Query().Get("interval_ms"), 1000, 200, 5000)
	maxEventsPerSecond := parseIntBounded(r.URL.Query().Get("max_events_per_second"), 3, 1, 30)
	maxEvents := parseIntBounded(r.URL.Query().Get("max_events"), 0, 0, 200)

	selectedLayers := parseLayerSelection(r.URL.Query().Get("layers"))

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Connection", "keep-alive")

	produceTicker := time.NewTicker(time.Duration(intervalMS) * time.Millisecond)
	defer produceTicker.Stop()
	flushTicker := time.NewTicker(time.Second / time.Duration(maxEventsPerSecond))
	defer flushTicker.Stop()
	heartbeatTicker := time.NewTicker(15 * time.Second)
	defer heartbeatTicker.Stop()

	var (
		pending    *StreamEventEnvelope
		dropped    int
		sentEvents int
	)
	for {
		select {
		case <-r.Context().Done():
			return
		case <-produceTicker.C:
			for _, event := range h.snapshotStreamEvents(selectedLayers, intervalMS) {
				if pending != nil {
					dropped++
				}
				event.Provenance.Dropped = dropped
				pending = &event
			}
		case <-flushTicker.C:
			if pending == nil {
				continue
			}
			payload, err := json.Marshal(pending)
			if err != nil {
				continue
			}
			if _, err := fmt.Fprintf(w, "event: update\ndata: %s\n\n", payload); err != nil {
				return
			}
			flusher.Flush()
			portalStreamEventsTotal.Add(pending.Layer, 1)
			if dropped > 0 {
				portalStreamDropped.Add(pending.Layer, int64(dropped))
			}
			h.timeline.add(*pending)
			dropped = 0
			pending = nil
			sentEvents++
			if maxEvents > 0 && sentEvents >= maxEvents {
				return
			}
		case <-heartbeatTicker.C:
			if _, err := fmt.Fprint(w, ": keep-alive\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func parseIntBounded(raw string, fallback, min, max int) int {
	value := fallback
	if strings.TrimSpace(raw) != "" {
		parsed, err := strconv.Atoi(raw)
		if err == nil {
			value = parsed
		}
	}
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}

func parseLayerSelection(raw string) map[string]bool {
	layers := map[string]bool{
		"registry":   true,
		"semantic":   true,
		"projection": true,
	}
	if strings.TrimSpace(raw) == "" {
		return layers
	}
	selected := map[string]bool{
		"registry":   false,
		"semantic":   false,
		"projection": false,
	}
	for _, token := range strings.Split(strings.ToLower(raw), ",") {
		key := strings.TrimSpace(token)
		if _, ok := selected[key]; ok {
			selected[key] = true
		}
	}
	return selected
}

func (h *handler) snapshotStreamEvents(layers map[string]bool, intervalMS int) []StreamEventEnvelope {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	events := make([]StreamEventEnvelope, 0, 3)

	if layers["registry"] && h.opts.ListRegistry != nil {
		devices := h.opts.ListRegistry()
		events = append(events, StreamEventEnvelope{
			At:            now,
			Type:          "snapshot",
			Layer:         "registry",
			CorrelationID: fmt.Sprintf("reg-%d", time.Now().UnixNano()),
			Payload: map[string]any{
				"device_count": len(devices),
			},
			Provenance: StreamSource{
				Source:   "poll:registry",
				Interval: intervalMS,
			},
		})
	}

	if layers["semantic"] && h.opts.ListSemantic != nil {
		snapshot := h.opts.ListSemantic()
		events = append(events, StreamEventEnvelope{
			At:            now,
			Type:          "snapshot",
			Layer:         "semantic",
			CorrelationID: fmt.Sprintf("sem-%d", time.Now().UnixNano()),
			Payload: map[string]any{
				"zones_count":   len(snapshot.Zones),
				"has_dhw":       snapshot.DHW != nil,
				"captured_utc":  snapshot.CapturedUTC,
				"has_energy":    snapshot.Energy != nil,
				"energy_series": snapshot.Energy != nil,
			},
			Provenance: StreamSource{
				Source:   "poll:semantic",
				Interval: intervalMS,
			},
		})
	}

	if layers["projection"] && h.opts.ListProjections != nil {
		items := h.opts.ListProjections()
		projectionCount := 0
		for _, item := range items {
			projectionCount += len(item.Projections)
		}
		events = append(events, StreamEventEnvelope{
			At:            now,
			Type:          "snapshot",
			Layer:         "projection",
			CorrelationID: fmt.Sprintf("proj-%d", time.Now().UnixNano()),
			Payload: map[string]any{
				"device_count":     len(items),
				"projection_count": projectionCount,
			},
			Provenance: StreamSource{
				Source:   "poll:projection",
				Interval: intervalMS,
			},
		})
	}

	return events
}
