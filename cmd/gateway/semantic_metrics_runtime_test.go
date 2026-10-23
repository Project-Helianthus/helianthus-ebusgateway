package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semanticmetrics"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/infrastructure"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/thermal"
)

// m38Series is one row of the expected semantic series inventory: the pack
// field, its exported series, the synthetic published value and the exact
// rendered sample.
type m38Series struct {
	field  semreg.DefinitionID
	name   string
	value  semreg.Value
	sample string
}

func m38Quantity(coefficient string, exponent int32, unit semreg.DefinitionID) semreg.Value {
	return semreg.Value{Kind: semreg.ValueQuantity, Quantity: &semreg.Quantity{Number: semreg.Decimal{Coefficient: coefficient, Exponent10: exponent}, Unit: unit}}
}

func m38Symbol(field semreg.DefinitionID, token string) semreg.Value {
	return semreg.Value{Kind: semreg.ValueSymbol, Symbol: &semreg.Symbol{Namespace: field, Token: token, Known: true}}
}

// m38Inventory is the complete Thermal and Infrastructure series inventory of
// the scrape surface, in exposition order.
var m38Inventory = []m38Series{
	{"thermal.action.state", "helianthus_thermal_action_state", m38Symbol("thermal.action.state", "heating"), "1"},
	{"thermal.demand.level", "helianthus_thermal_demand_ratio", m38Quantity("25", 0, "unit.percent"), "0.25"},
	{"thermal.measurement.air_temperature", "helianthus_thermal_air_temperature_celsius", m38Quantity("21", 0, "unit.celsius"), "21"},
	{"thermal.measurement.flow_rate", "helianthus_thermal_flow_cubic_meters_per_second", m38Quantity("12", 0, "unit.litre_per_minute"), "0.0002"},
	{"thermal.measurement.humidity_relative", "helianthus_thermal_relative_humidity_ratio", m38Quantity("4", 1, "unit.percent"), "0.4"},
	{"thermal.measurement.power", "helianthus_thermal_power_watts", m38Quantity("-15", 2, "unit.watt"), "-1500"},
	{"thermal.measurement.water_temperature", "helianthus_thermal_water_temperature_celsius", m38Quantity("45", 0, "unit.celsius"), "45"},
	{"thermal.mode.system", "helianthus_thermal_system_mode", m38Symbol("thermal.mode.system", "heat"), "2"},
	{"thermal.mode.zone", "helianthus_thermal_zone_mode", m38Symbol("thermal.mode.zone", "eco"), "3"},
	{"thermal.setpoint.dhw_temperature", "helianthus_thermal_dhw_setpoint_celsius", m38Quantity("5", 1, "unit.celsius"), "50"},
	{"thermal.setpoint.temperature", "helianthus_thermal_setpoint_celsius", m38Quantity("215", -1, "unit.celsius"), "21.5"},
	{"thermal.status.fault", "helianthus_thermal_fault_present", m38Symbol("thermal.status.fault", "clear"), "0"},
	{"thermal.status.operation", "helianthus_thermal_operation_active", m38Symbol("thermal.status.operation", "active"), "1"},
	{"infrastructure.ac.active_power", "helianthus_infrastructure_ac_active_power_watts", m38Quantity("-12", 2, "unit.watt"), "-1200"},
	{"infrastructure.ac.apparent_power", "helianthus_infrastructure_ac_apparent_power_volt_amperes", m38Quantity("13", 2, "unit.volt_ampere"), "1300"},
	{"infrastructure.ac.current", "helianthus_infrastructure_ac_current_amperes", m38Quantity("52", -1, "unit.ampere"), "5.2"},
	{"infrastructure.ac.frequency", "helianthus_infrastructure_ac_frequency_hertz", m38Quantity("5", 1, "unit.hertz"), "50"},
	{"infrastructure.ac.power_factor", "helianthus_infrastructure_ac_power_factor_ratio", m38Quantity("-9", -1, "unit.ratio"), "-0.9"},
	{"infrastructure.ac.reactive_power", "helianthus_infrastructure_ac_reactive_power_vars", m38Quantity("-3", 2, "unit.volt_ampere_reactive"), "-300"},
	{"infrastructure.ac.voltage", "helianthus_infrastructure_ac_voltage_volts", m38Quantity("23", 1, "unit.volt"), "230"},
	{"infrastructure.energy.export", "helianthus_infrastructure_energy_export_joules", m38Quantity("2", 0, "unit.kilowatt_hour"), "7.2e+06"},
	{"infrastructure.energy.import", "helianthus_infrastructure_energy_import_joules", m38Quantity("3", 0, "unit.kilowatt_hour"), "1.08e+07"},
	{"infrastructure.power.apparent", "helianthus_infrastructure_apparent_power_volt_amperes", m38Quantity("14", 2, "unit.volt_ampere"), "1400"},
	{"infrastructure.power.export_active", "helianthus_infrastructure_export_active_power_watts", m38Quantity("6", 2, "unit.watt"), "600"},
	{"infrastructure.power.export_reactive", "helianthus_infrastructure_export_reactive_power_vars", m38Quantity("1", 2, "unit.volt_ampere_reactive"), "100"},
	{"infrastructure.power.import_active", "helianthus_infrastructure_import_active_power_watts", m38Quantity("18", 2, "unit.watt"), "1800"},
	{"infrastructure.power.import_reactive", "helianthus_infrastructure_import_reactive_power_vars", m38Quantity("2", 2, "unit.volt_ampere_reactive"), "200"},
	{"infrastructure.status.breaker", "helianthus_infrastructure_breaker_open", m38Symbol("infrastructure.status.breaker", "closed"), "0"},
	{"infrastructure.status.connection", "helianthus_infrastructure_connection_connected", m38Symbol("infrastructure.status.connection", "connected"), "1"},
	{"infrastructure.status.fault", "helianthus_infrastructure_fault_present", m38Symbol("infrastructure.status.fault", "present"), "1"},
	{"infrastructure.status.interlock", "helianthus_infrastructure_interlock_active", m38Symbol("infrastructure.status.interlock", "clear"), "0"},
	{"infrastructure.status.readiness", "helianthus_infrastructure_ready", m38Symbol("infrastructure.status.readiness", "ready"), "1"},
}

const m38AvailabilityName = "helianthus_semantic_field_available"

// m38Slots is the explicit, collision-free slot assignment of the synthetic
// registry: one binding per pack dimension, including the highest slot.
var m38Slots = map[semreg.DefinitionID]int{
	"thermal.dimension.system": 0, "thermal.dimension.ratio": 1, "thermal.dimension.temperature": 2,
	"thermal.dimension.volumetric_flow": 3, "thermal.dimension.power": 4, "thermal.dimension.zone": 5,
	"infrastructure.dimension.phase": 0, "infrastructure.dimension.grid_connection": 1, "infrastructure.dimension.meter": 2,
	"infrastructure.dimension.circuit": 3, "infrastructure.dimension.site": 15,
}

const m38Wall = int64(1_800_000_000_000_000_000)

type m38Domain struct {
	name      string
	asset     semreg.AssetID
	binding   semreg.NativeBindingID
	metadata  semreg.PackMetadata
	validator semreg.PackValidator
}

func m38Domains() []m38Domain {
	return []m38Domain{
		{"thermal", "asset:m38-thermal", "binding:m38-thermal", thermal.Metadata(), thermal.New()},
		{"infrastructure", "asset:m38-infrastructure", "binding:m38-infrastructure", infrastructure.Metadata(), infrastructure.New()},
	}
}

func (d m38Domain) dimension(t *testing.T, field semreg.DefinitionID) semreg.DefinitionID {
	t.Helper()
	for _, f := range d.metadata.Fields {
		if f.Ref.ID == field {
			return f.Dimension.ID
		}
	}
	t.Fatalf("field %s has no %s pack metadata", field, d.name)
	return ""
}

// serviceKind names the read service of a dimension. Thermal services are
// resource kinds, so the synthetic thermal fields share the system service.
func (d m38Domain) serviceKind(dimension semreg.DefinitionID) string {
	if d.name == "thermal" {
		return "system"
	}
	return strings.TrimPrefix(string(dimension), "infrastructure.dimension.")
}

func (d m38Domain) service(dimension semreg.DefinitionID) semreg.ServiceInstanceID {
	return semreg.ServiceInstanceID("service:m38-" + d.name + "-" + d.serviceKind(dimension))
}

func (d m38Domain) ref(id string) semreg.DefinitionRef {
	return semreg.DefinitionRef{Pack: d.metadata.Pack, ID: semreg.DefinitionID(id), Version: d.metadata.Pack.Version}
}

func (d m38Domain) entries() []m38Series {
	var entries []m38Series
	for _, entry := range m38Inventory {
		if strings.HasPrefix(string(entry.field), d.name+".") {
			entries = append(entries, entry)
		}
	}
	return entries
}

func m38DimensionValue(dimension semreg.DefinitionID) []semreg.Dimension {
	text := "m38-" + string(dimension[strings.LastIndex(string(dimension), ".")+1:])
	return []semreg.Dimension{{ID: dimension, Value: semreg.Value{Kind: semreg.ValueText, Text: &text}}}
}

func m38Evidence() semreg.EvidenceRef {
	digest := sha256.Sum256([]byte("m38 synthetic native evidence"))
	return semreg.EvidenceRef{Owner: "helianthus.gateway.test", Kind: "m38.synthetic.observation", Digest: semreg.Digest(fmt.Sprintf("sha256:%x", digest)), Contract: semreg.ContractKernelV1, Access: semreg.EvidenceAccessAuthorized, Redaction: semreg.RedactionMetadataOnly}
}

func m38Times(offset int64) (semreg.TimePoint, semreg.MonotonicPoint) {
	return semreg.TimePoint{UnixNanoseconds: semreg.Int64(strconv.FormatInt(m38Wall+offset, 10)), ClockID: "clock.utc", UncertaintyNS: "0"},
		semreg.MonotonicPoint{ClockEpochID: "clock:m38-synthetic", Nanoseconds: semreg.Uint64(strconv.FormatInt(1000+offset, 10))}
}

// m38CaptureSource counts and records every hub capture of the shared provider.
type m38CaptureSource struct {
	hub    *semhub.Hub
	mu     sync.Mutex
	calls  [][]semreg.AssetID
	after  func()
	mutate func(*semhub.CoherentCapture)
	err    error
}

func (s *m38CaptureSource) Capture(ctx context.Context, ids []semreg.AssetID, now func() time.Time) (semhub.CoherentCapture, error) {
	s.mu.Lock()
	s.calls = append(s.calls, append([]semreg.AssetID(nil), ids...))
	s.mu.Unlock()
	if s.err != nil {
		return semhub.CoherentCapture{}, s.err
	}
	capture, err := s.hub.Capture(ctx, ids, now)
	if err == nil && s.mutate != nil {
		s.mutate(&capture)
	}
	if s.after != nil {
		s.after()
	}
	return capture, err
}

func (s *m38CaptureSource) captures() [][]semreg.AssetID {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][]semreg.AssetID(nil), s.calls...)
}

type m38Fixture struct {
	hub      *semhub.Hub
	handle   *semhub.Handle
	source   *m38CaptureSource
	provider *hubSemanticSnapshotProvider
	registry semanticmetrics.Registry
	store    *ebusgateway.BusObservabilityStore
	logs     []string
	logMu    sync.Mutex
}

// m38Setup publishes every inventory field through a real hub, one asset per
// domain, and configures one binding per dimension.
func m38Setup(t *testing.T) *m38Fixture {
	t.Helper()
	hub, err := semhub.New(semhub.Options{})
	if err != nil {
		t.Fatal(err)
	}
	handle, err := hub.AdmitSource(semhub.Admission{Source: "source:m38-synthetic", Driver: "m38.synthetic"})
	if err != nil {
		t.Fatal(err)
	}
	f := &m38Fixture{hub: hub, handle: handle, source: &m38CaptureSource{hub: hub}, registry: semanticmetrics.Registry{Validated: true}}
	f.provider = newGatewaySemanticSnapshotProvider(&gatewayRuntimeControl{hub: hub})
	if f.provider.hub != semanticHubCapture(hub) {
		t.Fatal("provider did not bind the composed hub")
	}
	f.provider.hub = f.source
	f.provider.now = func() time.Time { return time.Unix(0, m38Wall+int64(time.Second)) }
	for _, d := range m38Domains() {
		if _, err := hub.Declare(semhub.AssetSpec{AssetID: d.asset, SourceID: handle.Source(), Driver: "m38.synthetic", Validators: []semreg.PackValidator{d.validator}}); err != nil {
			t.Fatal(err)
		}
		f.publish(t, d, d.entries(), 0)
		bindings := map[semreg.DefinitionID]*semanticmetrics.Binding{}
		var order []semreg.DefinitionID
		for _, entry := range d.entries() {
			dimension := d.dimension(t, entry.field)
			if bindings[dimension] == nil {
				bindings[dimension] = &semanticmetrics.Binding{Domain: d.name, Slot: m38Slots[dimension], AssetID: d.asset, SourceID: handle.Source(), SourceEpochID: handle.Epoch(), BindingID: d.binding, DriverGeneration: handle.GenerationID(), ServiceInstance: d.service(dimension), Dimensions: m38DimensionValue(dimension)}
				order = append(order, dimension)
			}
			bindings[dimension].Fields = append(bindings[dimension].Fields, entry.field)
		}
		for _, dimension := range order {
			f.registry.Bindings = append(f.registry.Bindings, *bindings[dimension])
		}
	}
	f.store = ebusgateway.NewBusObservabilityStore(ebusgateway.DefaultConfig())
	f.store.SetTransportRuntimeStatus(modbusRuntimeTransportStatus(ebusgateway.ModbusTCPConfig{Enabled: true}, true))
	return f
}

// publish commits the given facts at the wall/monotonic offset. The first
// publication of an asset also declares its source, binding, services and
// read capabilities; later ones only revise facts.
func (f *m38Fixture) publish(t *testing.T, d m38Domain, entries []m38Series, offset int64) {
	t.Helper()
	tx, err := f.hub.Begin(f.handle, d.asset)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Abort()
	current, exists := tx.Current()
	wall, mono := m38Times(offset)
	evidence := m38Evidence()
	source, epoch, generation, binding := f.handle.Source(), f.handle.Epoch(), f.handle.GenerationID(), d.binding
	expected := semreg.Uint64("0")
	if exists {
		expected = current.Revisions.Semantic
	}
	sequence := tx.NextSequence()
	batch := semreg.PublicationBatch{Contract: semreg.ContractKernelV1, BatchID: semreg.BatchID("batch:m38-" + d.name + "-" + string(sequence)), AssetID: d.asset, SourceID: source, SourceEpochID: epoch, DriverGeneration: generation, Sequence: sequence, ExpectedSemanticRevision: expected, ObservedAt: wall,
		SourceUpserts: []semreg.SourceDescriptor{}, SourceRetirements: tx.SourceRetirements(), BindingUpserts: []semreg.NativeBinding{}, IdentityLinkUpserts: []semreg.IdentityLink{}, FactUpserts: []semreg.FactCandidate{}, FactWithdrawals: []semreg.CandidateID{},
		ServiceUpserts: []semreg.ServiceInstance{}, ServiceWithdrawals: []semreg.ServiceInstanceID{}, CapabilityUpserts: []semreg.CapabilityInstance{}, CapabilityWithdrawals: []semreg.CapabilityInstanceID{}, GenerationFences: []semreg.GenerationFence{}}
	if !exists {
		batch.SourceUpserts = []semreg.SourceDescriptor{{SourceID: source, SourceEpochID: epoch, ProtocolID: "m38.synthetic", ProfileID: "m38.profile", ProfileVersion: "1", RegistryEvidence: evidence, StartedAt: wall, State: semreg.SourceCurrent, Revision: "1"}}
		batch.BindingUpserts = []semreg.NativeBinding{{BindingID: binding, AssetID: d.asset, SourceID: source, SourceEpochID: epoch, DriverGeneration: generation, NativeResource: evidence, State: semreg.BindingCurrent, Revision: "1"}}
		batch.IdentityLinkUpserts = []semreg.IdentityLink{{AssetID: d.asset, BindingID: binding, State: semreg.LinkQualified, Basis: []semreg.EvidenceRef{evidence}, Revision: "1"}}
		seen := map[semreg.ServiceInstanceID]bool{}
		for _, entry := range entries {
			dimension := d.dimension(t, entry.field)
			service := d.service(dimension)
			if seen[service] {
				continue
			}
			seen[service] = true
			kind := d.serviceKind(dimension)
			batch.ServiceUpserts = append(batch.ServiceUpserts, semreg.ServiceInstance{InstanceID: service, AssetID: d.asset, Definition: d.ref(d.name + ".service." + kind), BindingID: binding, SourceEpochID: epoch, DriverGeneration: generation, Qualification: semreg.QualificationQualified, Availability: semreg.AvailabilityAvailable, Revision: "1"})
			batch.CapabilityUpserts = append(batch.CapabilityUpserts, semreg.CapabilityInstance{InstanceID: semreg.CapabilityInstanceID("capability:m38-" + d.name + "-" + kind), AssetID: d.asset, ServiceInstance: service, Definition: d.ref(d.name + ".capability.read." + kind), BindingID: binding, SourceEpochID: epoch, DriverGeneration: generation, Qualification: semreg.QualificationQualified, Availability: semreg.AvailabilityAvailable, Constraints: []semreg.TypedField{}, ActivationEvidence: []semreg.EvidenceRef{evidence}, Revision: "1"})
		}
	}
	for _, entry := range entries {
		candidateID := semreg.CandidateID("candidate:m38-" + string(entry.field))
		revision := semreg.Uint64("1")
		for _, envelope := range current.Facts {
			for _, candidate := range envelope.Candidates {
				if candidate.CandidateID == candidateID {
					n, err := strconv.ParseUint(string(candidate.Revision), 10, 64)
					if err != nil {
						t.Fatal(err)
					}
					revision = semreg.Uint64(strconv.FormatUint(n+1, 10))
				}
			}
		}
		value := entry.value
		key := semreg.FactKey{PackID: d.metadata.Pack.ID, PackVersion: d.metadata.Pack.Version, FactID: entry.field, Dimensions: m38DimensionValue(d.dimension(t, entry.field))}
		batch.FactUpserts = append(batch.FactUpserts, semreg.FactCandidate{CandidateID: candidateID, Key: key, Value: &value,
			Quality:         semreg.Quality{Assertion: semreg.AssertionObserved, Qualification: semreg.QualificationQualified, Promotion: semreg.PromotionPromoted, Validity: semreg.ValidityGood, Availability: semreg.AvailabilityAvailable, Freshness: semreg.FreshnessFresh, Reasons: []semreg.DefinitionID{}},
			Times:           semreg.Times{ReceivedAt: wall, ReceiptMonotonic: mono, EvaluatedAt: wall, EvaluateMonotonic: mono},
			FreshnessPolicy: semreg.FreshnessPolicy{PolicyID: "policy:m38-synthetic", Version: "1.0.0", FreshForNS: semreg.Uint64(strconv.FormatInt(int64(time.Minute), 10)), RetainForNS: semreg.Uint64(strconv.FormatInt(int64(5*time.Minute), 10)), MaxWallUncertaintyNS: "0"},
			BindingID:       &binding, SourceEpochID: &epoch, DriverGeneration: &generation,
			Origin:   semreg.OriginRef{OriginID: semreg.OriginID("origin:m38-" + string(entry.field)), Kind: semreg.OriginNativeObservation, SourceID: &source, SourceEpochID: &epoch, BindingID: &binding, Evidence: []semreg.EvidenceRef{evidence}},
			Evidence: []semreg.EvidenceRef{evidence}, Revision: revision})
	}
	// Publication collections are canonical: bytewise by identifier.
	sort.Slice(batch.FactUpserts, func(i, j int) bool { return batch.FactUpserts[i].CandidateID < batch.FactUpserts[j].CandidateID })
	sort.Slice(batch.ServiceUpserts, func(i, j int) bool { return batch.ServiceUpserts[i].InstanceID < batch.ServiceUpserts[j].InstanceID })
	sort.Slice(batch.CapabilityUpserts, func(i, j int) bool {
		return batch.CapabilityUpserts[i].InstanceID < batch.CapabilityUpserts[j].InstanceID
	})
	batch.BatchDigest, err = batch.ComputedDigest()
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := tx.Apply(batch, mono); err != nil {
		t.Fatalf("apply %s publication: %v", d.name, err)
	}
	if outcome, err := tx.Commit(nil); err != nil || outcome != semhub.Accepted {
		t.Fatalf("commit %s publication: %v/%v", d.name, outcome, err)
	}
}

func (f *m38Fixture) metrics(t *testing.T) *gatewaySemanticMetrics {
	t.Helper()
	metrics, err := newGatewaySemanticMetrics(f.provider, f.registry)
	if err != nil {
		t.Fatal(err)
	}
	metrics.logf = func(format string, args ...any) {
		f.logMu.Lock()
		f.logs = append(f.logs, fmt.Sprintf(format, args...))
		f.logMu.Unlock()
	}
	return metrics
}

func (f *m38Fixture) logged() []string {
	f.logMu.Lock()
	defer f.logMu.Unlock()
	return append([]string(nil), f.logs...)
}

func m38Serve(t *testing.T, handler http.Handler, request *http.Request) (*http.Response, string) {
	t.Helper()
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	response := recorder.Result()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if err := response.Body.Close(); err != nil {
		t.Fatal(err)
	}
	return response, string(body)
}

func m38Scrape(t *testing.T, handler http.Handler) string {
	t.Helper()
	response, body := m38Serve(t, handler, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if response.StatusCode != http.StatusOK {
		t.Fatalf("scrape status %d", response.StatusCode)
	}
	return body
}

// m38SemanticSection returns the exposition from the first semantic family on.
func m38SemanticSection(t *testing.T, body string) string {
	t.Helper()
	start := len(body)
	for _, entry := range m38Inventory {
		if i := strings.Index(body, "# HELP "+entry.name+" "); i >= 0 && i < start {
			start = i
		}
	}
	i := strings.Index(body, "# HELP "+m38AvailabilityName+" ")
	if i < 0 {
		t.Fatalf("scrape has no semantic availability family:\n%s", body)
	}
	if i < start {
		start = i
	}
	return body[start:]
}

func m38Slot(t *testing.T, entry m38Series) int {
	t.Helper()
	for _, d := range m38Domains() {
		if strings.HasPrefix(string(entry.field), d.name+".") {
			return m38Slots[d.dimension(t, entry.field)]
		}
	}
	t.Fatalf("unknown domain for %s", entry.field)
	return -1
}

func m38Availability(t *testing.T, entry m38Series, available int) string {
	t.Helper()
	domain := strings.SplitN(string(entry.field), ".", 2)[0]
	return fmt.Sprintf("%s{domain=%q,field=%q,slot=\"%d\"} %d", m38AvailabilityName, domain, entry.field, m38Slot(t, entry), available)
}

// m38AssertWithheld checks that every configured field reports availability
// 0 and that no semantic value sample remains.
func m38AssertWithheld(t *testing.T, body string, entries []m38Series) {
	t.Helper()
	section := m38SemanticSection(t, body)
	for _, entry := range entries {
		if strings.Contains(section, entry.name+"{") || !strings.Contains(section, m38Availability(t, entry, 0)+"\n") {
			t.Fatalf("%s was not withheld honestly:\n%s", entry.field, section)
		}
	}
	if got := strings.Count(section, m38AvailabilityName+"{"); got != len(m38Inventory) {
		t.Fatalf("availability series = %d; want one per configured field (%d)", got, len(m38Inventory))
	}
}

func m38AssertTransport(t *testing.T, body string) {
	t.Helper()
	for _, want := range []string{
		"# TYPE helianthus_transport_runtime_status gauge\n",
		`helianthus_transport_runtime_status{`,
		`protocol="modbus_tcp"`,
		"# TYPE ebus_observability_transport_info gauge\n",
		"# TYPE feature_flag_enabled gauge\n",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("scrape lost transport series %q:\n%s", want, body)
		}
	}
}

func TestM38ScrapeRouteServesTransportAndSemanticInventory(t *testing.T) {
	f := m38Setup(t)
	gateway := &ebusgateway.Gateway{Registry: registry.NewDeviceRegistry(nil)}
	query, snapshot, subscription, server, err := newHTTPControlPlaneSemanticHandlers(gateway, graphql.NewBuilder(gateway.Registry, nil), graphql.NewBroadcastHub(nil), f.provider)
	if err != nil {
		t.Fatal(err)
	}
	cfg := ebusgateway.DefaultConfig()
	plan := newHTTPControlPlaneRoutePlan(cfg, true)
	mux := http.NewServeMux()
	registerHTTPControlPlaneCoreRoutes(mux, plan, t.TempDir(), newGatewayMetricsHandler(f.store, f.metrics(t)), query, snapshot, subscription, server, http.NotFoundHandler())

	transportResponse, transportBody := m38Serve(t, f.store.MetricsHandler(), httptest.NewRequest(http.MethodGet, plan.metricsPath, nil))
	response, body := m38Serve(t, mux, httptest.NewRequest(http.MethodGet, plan.metricsPath, nil))
	if response.StatusCode != transportResponse.StatusCode || response.Header.Get("Content-Type") != transportResponse.Header.Get("Content-Type") {
		t.Fatalf("scrape status/content type = %d %q; transport handler = %d %q", response.StatusCode, response.Header.Get("Content-Type"), transportResponse.StatusCode, transportResponse.Header.Get("Content-Type"))
	}
	// The transport exposition is byte-identical and complete before the
	// semantic families start.
	if !strings.HasPrefix(body, transportBody) || !strings.HasSuffix(transportBody, "\n") {
		t.Fatalf("transport exposition changed:\n%s", body)
	}
	m38AssertTransport(t, body)
	section := body[len(transportBody):]
	if section != m38SemanticSection(t, body) {
		t.Fatalf("semantic families interleaved with transport families:\n%s", body)
	}

	var want []string
	for _, entry := range m38Inventory {
		want = append(want, "# TYPE "+entry.name+" gauge", fmt.Sprintf("%s{slot=\"%d\"} %s", entry.name, m38Slot(t, entry), entry.sample))
	}
	want = append(want, "# TYPE "+m38AvailabilityName+" gauge")
	for _, entry := range m38Inventory {
		want = append(want, m38Availability(t, entry, 1))
	}
	var got []string
	for _, line := range strings.Split(strings.TrimSuffix(section, "\n"), "\n") {
		if strings.HasPrefix(line, "# HELP ") {
			continue
		}
		got = append(got, line)
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("semantic series inventory:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	for _, entry := range m38Inventory {
		if !strings.Contains(section, "# HELP "+entry.name+" "+string(entry.field)+" (") {
			t.Fatalf("%s lacks its contract HELP line", entry.name)
		}
	}
	// No family is declared twice across the whole exposition.
	families := map[string]int{}
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "# TYPE ") {
			families[strings.Fields(line)[2]]++
		}
	}
	for name, count := range families {
		if count != 1 {
			t.Fatalf("family %s declared %d times", name, count)
		}
	}
	captures := f.source.captures()
	if len(captures) != 1 || fmt.Sprint(captures[0]) != "[asset:m38-infrastructure asset:m38-thermal]" {
		t.Fatalf("captures = %v; want one capture of the configured assets", captures)
	}
	if logs := f.logged(); len(logs) != 0 {
		t.Fatalf("healthy scrape logged %v", logs)
	}
}

func TestM38EachScrapeUsesOneCoherentCapture(t *testing.T) {
	f := m38Setup(t)
	handler := newGatewayMetricsHandler(f.store, f.metrics(t))
	domains := m38Domains()
	update := map[semreg.DefinitionID]m38Series{
		"thermal.measurement.water_temperature": {"thermal.measurement.water_temperature", "helianthus_thermal_water_temperature_celsius", m38Quantity("47", 0, "unit.celsius"), "47"},
		"infrastructure.ac.voltage":             {"infrastructure.ac.voltage", "helianthus_infrastructure_ac_voltage_volts", m38Quantity("231", 0, "unit.volt"), "231"},
	}
	published := false
	// Both assets change right after the scrape captured, before it renders.
	f.source.after = func() {
		if published {
			return
		}
		published = true
		f.publish(t, domains[0], []m38Series{update["thermal.measurement.water_temperature"]}, int64(500*time.Millisecond))
		f.publish(t, domains[1], []m38Series{update["infrastructure.ac.voltage"]}, int64(500*time.Millisecond))
	}
	sample := func(entry m38Series, value string) string {
		return fmt.Sprintf("%s{slot=\"%d\"} %s\n", entry.name, m38Slot(t, entry), value)
	}
	first := m38Scrape(t, handler)
	second := m38Scrape(t, handler)
	for field, next := range update {
		var prior m38Series
		for _, entry := range m38Inventory {
			if entry.field == field {
				prior = entry
			}
		}
		if !strings.Contains(first, sample(prior, prior.sample)) || strings.Contains(first, sample(next, next.sample)) {
			t.Fatalf("first scrape mixed in a publication committed after its capture (%s):\n%s", field, m38SemanticSection(t, first))
		}
		if !strings.Contains(second, sample(next, next.sample)) || !strings.Contains(second, m38Availability(t, next, 1)+"\n") {
			t.Fatalf("second scrape missed the new revision (%s):\n%s", field, m38SemanticSection(t, second))
		}
	}
	captures := f.source.captures()
	if len(captures) != 2 {
		t.Fatalf("captures = %d; want exactly one per scrape", len(captures))
	}
	for _, ids := range captures {
		if fmt.Sprint(ids) != "[asset:m38-infrastructure asset:m38-thermal]" {
			t.Fatalf("capture selection = %v", ids)
		}
	}
}

func TestM38UnavailableValuesAreHonest(t *testing.T) {
	tests := []struct {
		name     string
		mutate   func(*testing.T, *m38Fixture)
		captures int
		logs     int
		request  func() *http.Request
	}{
		{name: "no_process_hub", mutate: func(_ *testing.T, f *m38Fixture) {
			f.provider = newGatewaySemanticSnapshotProvider(&gatewayRuntimeControl{})
		}, logs: 1},
		{name: "capture_unavailable", mutate: func(_ *testing.T, f *m38Fixture) { f.source.err = semhub.ErrCaptureUnavailable }, captures: 2, logs: 1},
		{name: "stale_publication", mutate: func(_ *testing.T, f *m38Fixture) {
			f.provider.now = func() time.Time { return time.Unix(0, m38Wall+int64(2*time.Minute)) }
		}, captures: 2},
		{name: "retired_source_epoch", mutate: func(t *testing.T, f *m38Fixture) {
			wall, mono := m38Times(int64(500 * time.Millisecond))
			if _, err := f.hub.RetireEpoch(semhub.RetireRequest{Source: f.handle.Source(), Epoch: f.handle.Epoch(), Reason: "m38.test.retire", Evidence: []semreg.EvidenceRef{m38Evidence()}, ObservedAt: wall, Monotonic: mono}); err != nil {
				t.Fatal(err)
			}
		}, captures: 2},
		{name: "mixed_revision", mutate: func(t *testing.T, f *m38Fixture) {
			// A resealed evaluation of another facts revision than its snapshot.
			f.source.mutate = func(c *semhub.CoherentCapture) {
				evaluation := c.Assets[0].Evaluation
				evaluation.Revisions.Facts = "999"
				digest, err := evaluation.EvaluationDigestValue()
				if err != nil {
					t.Error(err)
				}
				evaluation.EvaluationDigest = digest
			}
		}, captures: 2, logs: 1},
		{name: "registry_unvalidated", mutate: func(_ *testing.T, f *m38Fixture) { f.registry.Validated = false }},
		{name: "every_slot_retired", mutate: func(_ *testing.T, f *m38Fixture) {
			for i := range f.registry.Bindings {
				f.registry.Bindings[i].Retired = true
			}
		}},
		{name: "abandoned_scrape", captures: 2, request: func() *http.Request {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			return httptest.NewRequest(http.MethodGet, "/metrics", nil).WithContext(ctx)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := m38Setup(t)
			if tt.mutate != nil {
				tt.mutate(t, f)
			}
			handler := newGatewayMetricsHandler(f.store, f.metrics(t))
			for i := 0; i < 2; i++ {
				request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
				if tt.request != nil {
					request = tt.request()
				}
				_, body := m38Serve(t, handler, request)
				m38AssertTransport(t, body)
				m38AssertWithheld(t, body, m38Inventory)
			}
			if got := len(f.source.captures()); got != tt.captures {
				t.Fatalf("captures = %d; want %d", got, tt.captures)
			}
			if logs := f.logged(); len(logs) != tt.logs {
				t.Fatalf("logs = %q; want %d entry per failure episode", logs, tt.logs)
			}
		})
	}
}

func TestM38FailedCaptureNeverReusesAnEarlierScrape(t *testing.T) {
	f := m38Setup(t)
	handler := newGatewayMetricsHandler(f.store, f.metrics(t))
	available := func(body string) bool {
		section := m38SemanticSection(t, body)
		for _, entry := range m38Inventory {
			if !strings.Contains(section, m38Availability(t, entry, 1)+"\n") {
				return false
			}
		}
		return true
	}
	if !available(m38Scrape(t, handler)) {
		t.Fatal("healthy scrape withheld values")
	}
	f.source.err = semhub.ErrCaptureUnavailable
	m38AssertWithheld(t, m38Scrape(t, handler), m38Inventory)
	m38AssertWithheld(t, m38Scrape(t, handler), m38Inventory)
	f.source.err = nil
	if !available(m38Scrape(t, handler)) {
		t.Fatal("recovered capture stayed withheld")
	}
	f.source.err = semhub.ErrCaptureUnavailable
	m38AssertWithheld(t, m38Scrape(t, handler), m38Inventory)
	// One entry per failure episode: repeated failures stay quiet and a
	// failure after recovery is reported again.
	if logs := f.logged(); len(logs) != 2 || logs[0] != logs[1] || !strings.Contains(logs[0], semhub.ErrCaptureUnavailable.Error()) {
		t.Fatalf("logs = %q", logs)
	}
	if got := len(f.source.captures()); got != 5 {
		t.Fatalf("captures = %d; want one per scrape", got)
	}
}

func TestM38ConcurrentScrapesShareOneRuntime(t *testing.T) {
	f := m38Setup(t)
	handler := newGatewayMetricsHandler(f.store, f.metrics(t))
	want := m38Scrape(t, handler)
	const scrapers, rounds = 4, 2
	bodies := make(chan string, scrapers*rounds)
	var wg sync.WaitGroup
	for i := 0; i < scrapers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < rounds; j++ {
				recorder := httptest.NewRecorder()
				handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
				bodies <- recorder.Body.String()
			}
		}()
	}
	wg.Wait()
	close(bodies)
	for body := range bodies {
		if m38SemanticSection(t, body) != m38SemanticSection(t, want) {
			t.Fatalf("concurrent scrape diverged:\n%s", m38SemanticSection(t, body))
		}
	}
	if got := len(f.source.captures()); got != 1+scrapers*rounds {
		t.Fatalf("captures = %d; want one per scrape", got)
	}
}

func TestM38RetiredSlotsLeaveSiblingsExported(t *testing.T) {
	f := m38Setup(t)
	for i := range f.registry.Bindings {
		f.registry.Bindings[i].Retired = f.registry.Bindings[i].Domain == "thermal"
	}
	body := m38Scrape(t, newGatewayMetricsHandler(f.store, f.metrics(t)))
	for _, entry := range m38Inventory {
		thermalField := strings.HasPrefix(string(entry.field), "thermal.")
		exported := strings.Contains(body, fmt.Sprintf("%s{slot=\"%d\"} %s\n", entry.name, m38Slot(t, entry), entry.sample))
		available := 1
		if thermalField {
			available = 0
		}
		if exported == thermalField || !strings.Contains(body, m38Availability(t, entry, available)+"\n") {
			t.Fatalf("%s exported=%v with retired thermal slots:\n%s", entry.field, exported, m38SemanticSection(t, body))
		}
	}
	if captures := f.source.captures(); len(captures) != 1 || fmt.Sprint(captures[0]) != "[asset:m38-infrastructure]" {
		t.Fatalf("captures = %v; retired-only assets need no hub read", captures)
	}
}

func TestM38LabelsStayBoundedWithManyHubAssets(t *testing.T) {
	f := m38Setup(t)
	for i := 0; i < 40; i++ {
		if _, err := f.hub.Declare(semhub.AssetSpec{AssetID: semreg.AssetID(fmt.Sprintf("asset:m38-opaque-%02d", i)), SourceID: f.handle.Source(), Driver: "m38.synthetic", Validators: []semreg.PackValidator{thermal.New()}}); err != nil {
			t.Fatal(err)
		}
	}
	body := m38Scrape(t, newGatewayMetricsHandler(f.store, f.metrics(t)))
	if captures := f.source.captures(); len(captures) != 1 || len(captures[0]) != 2 {
		t.Fatalf("captures = %v; only configured assets may be read", captures)
	}
	if strings.Contains(body, "m38") {
		t.Fatalf("asset, source, binding, service or dimension text leaked into the scrape:\n%s", body)
	}
	fields, names := map[string]bool{}, map[string]bool{}
	for _, entry := range m38Inventory {
		fields[string(entry.field)], names[entry.name] = true, true
	}
	sampleRE := regexp.MustCompile(`^([a-z_]+)\{([^}]*)\} \S+$`)
	labelRE := regexp.MustCompile(`([a-z_]+)="([^"]*)"`)
	series := 0
	for _, line := range strings.Split(m38SemanticSection(t, body), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		match := sampleRE.FindStringSubmatch(line)
		if match == nil {
			t.Fatalf("unexpected semantic line %q", line)
		}
		labels := map[string]string{}
		for _, label := range labelRE.FindAllStringSubmatch(match[2], -1) {
			labels[label[1]] = label[2]
		}
		slot, err := strconv.Atoi(labels["slot"])
		if err != nil || slot < 0 || slot > 15 || labels["slot"] != strconv.Itoa(slot) {
			t.Fatalf("unbounded slot in %q", line)
		}
		switch {
		case names[match[1]]:
			if len(labels) != 1 {
				t.Fatalf("value series %q carries labels beyond slot", line)
			}
		case match[1] == m38AvailabilityName:
			domain := labels["domain"]
			if len(labels) != 3 || (domain != "thermal" && domain != "infrastructure") || !fields[labels["field"]] || !strings.HasPrefix(labels["field"], domain+".") {
				t.Fatalf("availability series %q outside the bounded domain", line)
			}
		default:
			t.Fatalf("unexpected semantic series %q", line)
		}
		series++
	}
	if series != 2*len(m38Inventory) {
		t.Fatalf("semantic series = %d; want %d", series, 2*len(m38Inventory))
	}
}

func TestM38ProductionRegistryNeedsNoProbe(t *testing.T) {
	f := m38Setup(t)
	metrics, err := newGatewaySemanticMetrics(f.provider, gatewaySemanticMetricRegistry())
	if err != nil {
		t.Fatal(err)
	}
	body := m38Scrape(t, newGatewayMetricsHandler(f.store, metrics))
	m38AssertTransport(t, body)
	section := m38SemanticSection(t, body)
	lines := strings.Split(strings.TrimSuffix(section, "\n"), "\n")
	if len(lines) != 2 || !strings.HasPrefix(lines[0], "# HELP "+m38AvailabilityName+" ") || lines[1] != "# TYPE "+m38AvailabilityName+" gauge" {
		t.Fatalf("production registry must declare the family without samples:\n%s", section)
	}
	if captures := f.source.captures(); len(captures) != 0 {
		t.Fatalf("production scrape captured %v without a configured binding", captures)
	}
}

func TestM38MalformedRegistryFailsComposition(t *testing.T) {
	changes := map[string]func(*semanticmetrics.Registry){
		"slot_out_of_range": func(r *semanticmetrics.Registry) { r.Bindings[0].Slot = 16 },
		"slot_collision":    func(r *semanticmetrics.Registry) { r.Bindings[1].Slot = r.Bindings[0].Slot },
		"foreign_field": func(r *semanticmetrics.Registry) {
			r.Bindings[0].Fields = []semreg.DefinitionID{"infrastructure.ac.voltage"}
		},
		"unknown_domain": func(r *semanticmetrics.Registry) { r.Bindings[0].Domain = "pv" },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			f := m38Setup(t)
			change(&f.registry)
			if metrics, err := newGatewaySemanticMetrics(f.provider, f.registry); err == nil || metrics != nil {
				t.Fatalf("malformed registry accepted: %v", err)
			}
			if captures := f.source.captures(); len(captures) != 0 {
				t.Fatalf("composition captured %v", captures)
			}
		})
	}
}

// This AST check is retained because the shared-provider and mounted-route
// wiring of startHTTPServer is a process-bootstrap invariant that cannot be
// exercised without opening a listener and the unrelated transport lifecycle.
func TestM38StartHTTPServerSharesProviderWithScrapeRoute(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "gateway_http_server.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var body *ast.BlockStmt
	for _, declaration := range file.Decls {
		if function, ok := declaration.(*ast.FuncDecl); ok && function.Name.Name == "startHTTPServer" {
			body = function.Body
		}
	}
	if body == nil {
		t.Fatal("startHTTPServer not found")
	}
	ident := func(expression ast.Expr) string {
		if id, ok := expression.(*ast.Ident); ok {
			return id.Name
		}
		return ""
	}
	callName := func(expression ast.Expr) string {
		if call, ok := expression.(*ast.CallExpr); ok {
			return ident(call.Fun)
		}
		return ""
	}
	assigned := map[string]string{}
	calls := map[string][]*ast.CallExpr{}
	ast.Inspect(body, func(node ast.Node) bool {
		switch value := node.(type) {
		case *ast.AssignStmt:
			if len(value.Rhs) == 1 && len(value.Lhs) > 0 {
				if name := callName(value.Rhs[0]); name != "" {
					assigned[name] = ident(value.Lhs[0])
				}
			}
		case *ast.CallExpr:
			if name := ident(value.Fun); name != "" {
				calls[name] = append(calls[name], value)
			}
		}
		return true
	})
	provider, metrics := assigned["newGatewaySemanticSnapshotProvider"], assigned["newGatewaySemanticMetrics"]
	if provider == "" || metrics == "" || len(calls["newGatewaySemanticSnapshotProvider"]) != 1 || len(calls["newGatewaySemanticMetrics"]) != 1 {
		t.Fatalf("startHTTPServer must build one shared provider and one semantic metrics runtime: provider=%q metrics=%q", provider, metrics)
	}
	shared := func(name string, arg int) bool {
		if len(calls[name]) != 1 {
			return false
		}
		args := calls[name][0].Args
		if arg < 0 {
			arg += len(args)
		}
		return arg >= 0 && arg < len(args) && ident(args[arg]) == provider
	}
	if !shared("newHTTPControlPlaneSemanticHandlers", -1) || !shared("configureGatewayPortalSemanticRead", -1) || !shared("newGatewaySemanticMetrics", 0) {
		t.Fatal("GraphQL, MCP, Portal and the scrape route must read the same provider instance")
	}
	if callName(calls["newGatewaySemanticMetrics"][0].Args[1]) != "gatewaySemanticMetricRegistry" {
		t.Fatal("the scrape route must use the production binding registry")
	}
	routes := calls["registerHTTPControlPlaneCoreRoutes"]
	if len(routes) != 1 || len(routes[0].Args) < 4 || callName(routes[0].Args[3]) != "newGatewayMetricsHandler" {
		t.Fatal("the core routes must mount the composed scrape handler")
	}
	handler := routes[0].Args[3].(*ast.CallExpr)
	if len(handler.Args) != 2 || ident(handler.Args[0]) != "busObservability" || ident(handler.Args[1]) != metrics {
		t.Fatal("the scrape handler must compose the bus store with the shared semantic runtime")
	}
}

func TestM38NilStoreComposesWithoutMetricsRoute(t *testing.T) {
	f := m38Setup(t)
	if handler := newGatewayMetricsHandler(nil, f.metrics(t)); handler != nil {
		t.Fatal("handler for a nil store must be nil")
	}
	plan := newHTTPControlPlaneRoutePlan(ebusgateway.DefaultConfig(), false)
	if plan.metricsPath != "" {
		t.Fatalf("metrics path = %q without bus observability; want none", plan.metricsPath)
	}
	gateway := &ebusgateway.Gateway{Registry: registry.NewDeviceRegistry(nil)}
	query, snapshot, subscription, server, err := newHTTPControlPlaneSemanticHandlers(gateway, graphql.NewBuilder(gateway.Registry, nil), graphql.NewBroadcastHub(nil), f.provider)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	registerHTTPControlPlaneCoreRoutes(mux, plan, t.TempDir(), newGatewayMetricsHandler(nil, f.metrics(t)), query, snapshot, subscription, server, http.NotFoundHandler())
}
