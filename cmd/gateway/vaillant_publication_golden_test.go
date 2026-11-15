package main

// Issue #1480: characterization of what the Vaillant semantic poller
// publishes. The production poller built by newVaillantSemanticPoller runs
// over a real graphql.LiveSemanticProvider and graphql.BroadcastHub against
// the synthetic site of vaillant_publication_site_test.go, on a simulated
// clock. After every step the test records the provider's read views, the hub
// stream, the writer results, the MCP semantic view and every semantic cache
// payload, and compares the canonical dump with a golden under
// testdata/vaillant_publication/. The goldens are the baseline that the split
// of the poller into an acquisition owner and a publication adapter must
// reproduce unchanged.
//
// The dump names Go fields, not JSON tags, so a GraphQL or MCP schema rename
// does not move it. A provider or MCP view equal to the same view at the
// previous step is written as "<unchanged>", and a JSON value that fits one
// line stays on one line, which keeps the goldens reviewable. B555 timer
// frames take the gateway bus, so the site also answers on the wire. Each
// scenario runs twice and the two dumps must be equal
// before the golden comparison. VAILLANT_PUBLICATION_UPDATE_GOLDEN=1 rewrites
// the goldens.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	"github.com/Project-Helianthus/helianthus-ebusreg/vaillant/productids"
)

const (
	vaillantPublicationUpdateEnv = "VAILLANT_PUBLICATION_UPDATE_GOLDEN"
	vaillantPublicationDir       = "testdata/vaillant_publication"
	// vaillantPublicationStepGap is the simulated time between two steps;
	// it is longer than every read-cache lifetime, so each step reads the
	// bus again.
	vaillantPublicationStepGap = 11 * time.Minute
	// vaillantPublicationTaskLimit bounds the queued tasks one step drains.
	vaillantPublicationTaskLimit = 64
	// vaillantPublicationHubLimit is the hub's per-subscriber buffer: a
	// step that fills it could have lost an event.
	vaillantPublicationHubLimit = 16
	// vaillantPublicationUnchanged stands for a view equal to the same view
	// at the previous step.
	vaillantPublicationUnchanged = "<unchanged>"
)

// vaillantPublicationWallClockFields lists the struct fields whose value
// comes from the wall clock and cannot be driven by the poller's seams; the
// dump replaces a non-zero value with a placeholder. Every value the poller
// publishes is driven by the simulated clock, so the list is empty.
var vaillantPublicationWallClockFields = map[string]bool{}

type vaillantPublicationStream struct {
	name string
	ch   chan interface{}
}

// vaillantPublicationHarness is one run of the production poller over the
// synthetic site.
type vaillantPublicationHarness struct {
	t        *testing.T
	ctx      context.Context
	site     *vaillantPublicationSite
	provider *graphql.LiveSemanticProvider
	poller   *vaillantSemanticPoller
	streams  []vaillantPublicationStream
	mcpView  mcpSemanticProviderAdapter

	graphqlWriter     admittedGraphQLSemanticWriter
	mcpConfigWriter   admittedMCPConfigWriter
	mcpScheduleWriter admittedMCPScheduleWriter

	savesMu sync.Mutex
	saves   []any

	steps    []any
	previous map[string]string
}

// Save records every payload the poller hands to the semantic cache.
func (h *vaillantPublicationHarness) Save(snapshot semanticCacheSnapshot) error {
	h.savesMu.Lock()
	h.saves = append(h.saves, vaillantPublicationCanonical(reflect.ValueOf(snapshot)))
	h.savesMu.Unlock()
	return nil
}

func newVaillantPublicationHarness(t *testing.T, seed string) *vaillantPublicationHarness {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	site := newVaillantPublicationSite()
	gateway, err := ebusgateway.New(ctx, ebusgateway.Config{Transport: newVaillantPublicationWire(site)})
	if err != nil {
		t.Fatalf("ebusgateway.New() error = %v", err)
	}
	busCtx, stopBus := context.WithCancel(context.Background())
	busDone := make(chan struct{})
	go func() {
		defer close(busDone)
		gateway.Start(busCtx)
	}()
	t.Cleanup(func() {
		stopBus()
		<-busDone
		if err := gateway.Close(); err != nil {
			t.Errorf("gateway.Close() error = %v", err)
		}
	})
	vaillantPublicationRegisterDevices(t, gateway.Registry)

	cfg := ebusgateway.DefaultConfig()
	cfg.TransportConfig = ebusgateway.TransportConfig{Protocol: ebusgateway.TransportAdapterDirect, Network: "tcp", Address: "adapter.invalid:9999"}
	cfg.ScanSource = vaillantPublicationSource

	h := &vaillantPublicationHarness{t: t, ctx: ctx, site: site, provider: graphql.NewLiveSemanticProvider()}
	clock := site.clock
	h.provider.SetEnergyClock(clock.Now)
	var cached semanticCacheSnapshot
	cacheLoaded := false
	if seed != "" {
		cached, cacheLoaded = preloadSemanticCache(h.provider, newSemanticCacheStore(seed, t.Logf))
		if !cacheLoaded {
			t.Fatalf("semantic cache seed %s did not load", seed)
		}
	}
	hub := graphql.NewBroadcastHub(nil)
	poller := newVaillantSemanticPoller(cfg, gateway, h.provider, hub, h)
	poller.nowFn = clock.Now
	poller.tasks.now = clock.Now
	poller.scheduler.SetClock(clock.Now)
	// The constructor's shadow cache, rebuilt with the simulated clock.
	shadow := ebusgateway.NewShadowCache(ebusgateway.ShadowCacheOptions{FeatureFlags: normalizeObserveFirstFeatureFlagsForPoller(cfg), Now: clock.Now})
	poller.shadow = shadow
	poller.scheduler.SetShadowCache(shadow)
	poller.sendFrameFn = site.send
	poller.pauseFn = clock.pause
	if cacheLoaded {
		poller.hydrateFromCache(cached)
	}
	h.poller = poller

	admitted := admittedSourceProvider(func() (byte, bool) { return vaillantPublicationSource, true })
	h.graphqlWriter = admittedGraphQLSemanticWriter{boiler: poller, system: poller, schedule: poller, admitted: admitted}
	h.mcpConfigWriter = admittedMCPConfigWriter{writer: &mcpConfigWriterAdapter{poller: poller}, admitted: admitted}
	h.mcpScheduleWriter = admittedMCPScheduleWriter{writer: poller, admitted: admitted}
	view, ok := newMCPSemanticProvider(h.provider).(mcpSemanticProviderAdapter)
	if !ok {
		t.Fatal("newMCPSemanticProvider() is not the gateway's semantic adapter")
	}
	h.mcpView = view
	h.subscribe(hub)
	return h
}

// vaillantPublicationRegisterDevices registers the site's inventory: a
// regulator-class controller, a boiler-class heat source and an FM5-class
// functional module.
func vaillantPublicationRegisterDevices(t *testing.T, reg *registry.DeviceRegistry) {
	t.Helper()
	regulatorPart := controllerlessCatalogPartWith(t, "Regulator", productids.ControllerPresent)
	reg.Register(registry.DeviceInfo{Address: vaillantPublicationController, Manufacturer: "Vaillant", DeviceID: "CTLV2", SerialNumber: controllerlessSerial(regulatorPart)})
	reg.Register(registry.DeviceInfo{Address: vaillantPublicationBoiler, Manufacturer: "Vaillant", DeviceID: "BAI00"})
	reg.Register(registry.DeviceInfo{Address: vaillantPublicationModule, Manufacturer: "Vaillant", DeviceID: "FM500"})
}

func (h *vaillantPublicationHarness) subscribe(hub *graphql.BroadcastHub) {
	for _, entry := range []struct {
		name      string
		subscribe func(context.Context) (chan interface{}, error)
	}{
		{"Zones", hub.SubscribeZones},
		{"DHW", hub.SubscribeDHW},
		{"Energy", hub.SubscribeEnergy},
		{"Boiler", hub.SubscribeBoiler},
		{"RadioDevices", hub.SubscribeRadioDevices},
	} {
		ch, err := entry.subscribe(h.ctx)
		if err != nil {
			h.t.Fatalf("Subscribe%s() error = %v", entry.name, err)
		}
		h.streams = append(h.streams, vaillantPublicationStream{name: entry.name, ch: ch})
	}
}

// drainTasks runs the tasks a step enqueued, in the production queue order.
func (h *vaillantPublicationHarness) drainTasks() {
	queue := h.poller.tasks
	for ran := 0; ; ran++ {
		queue.mu.Lock()
		pending := len(queue.queue)
		queue.mu.Unlock()
		if pending == 0 {
			return
		}
		if ran >= vaillantPublicationTaskLimit {
			h.t.Fatalf("more than %d queued tasks in one step", vaillantPublicationTaskLimit)
		}
		task := queue.nextTask(h.ctx)
		if task == nil {
			h.t.Fatal("task queue stopped")
		}
		task.run(h.ctx)
		queue.taskDone(task)
	}
}

// step advances the simulated clock, runs one poller action, drains the
// tasks it enqueued and records everything published.
func (h *vaillantPublicationHarness) step(name string, action func(context.Context) any) {
	h.site.clock.Advance(vaillantPublicationStepGap)
	result := action(h.ctx)
	h.drainTasks()
	record := map[string]any{
		"Step":         name,
		"At":           h.site.clock.Now().UTC().Format(time.RFC3339Nano),
		"Provider":     h.sinceLastStep("Provider", h.providerViews()),
		"MCP":          h.sinceLastStep("MCP", h.mcpViews()),
		"Hub":          h.hubEvents(),
		"CacheSaves":   h.takeSaves(),
		"WriterResult": result,
	}
	h.steps = append(h.steps, record)
}

// sinceLastStep keeps a view in full when it differs from the same view at
// the previous step and replaces it with vaillantPublicationUnchanged
// otherwise, so the state after any step is the last full value above it.
func (h *vaillantPublicationHarness) sinceLastStep(surface string, views map[string]any) map[string]any {
	if h.previous == nil {
		h.previous = make(map[string]string)
	}
	out := make(map[string]any, len(views))
	for name, view := range views {
		encoded, err := json.Marshal(view)
		if err != nil {
			h.t.Fatalf("encode %s.%s: %v", surface, name, err)
		}
		key := surface + "." + name
		if previous, seen := h.previous[key]; seen && previous == string(encoded) {
			out[name] = vaillantPublicationUnchanged
			continue
		}
		h.previous[key] = string(encoded)
		out[name] = view
	}
	return out
}

// poll runs a refresh under the poll lock, as the task queue does.
func (h *vaillantPublicationHarness) poll(name string, refresh func(context.Context)) {
	h.step(name, func(ctx context.Context) any {
		h.poller.withPollLock(ctx, refresh)
		return nil
	})
}

func (h *vaillantPublicationHarness) providerViews() map[string]any {
	p := h.provider
	cacheEpoch, liveEpoch := p.StartupEpochs()
	registryState, registryErr := p.SemanticRegistryState()
	return map[string]any{
		"Zones":                       vaillantPublicationValue(p.Zones()),
		"DHW":                         vaillantPublicationValue(p.DHW()),
		"Circuits":                    vaillantPublicationValue(p.Circuits()),
		"BoilerStatus":                vaillantPublicationValue(p.BoilerStatus()),
		"System":                      vaillantPublicationValue(p.System()),
		"RadioDevices":                vaillantPublicationValue(p.RadioDevices()),
		"Solar":                       vaillantPublicationValue(p.Solar()),
		"Cylinders":                   vaillantPublicationValue(p.Cylinders()),
		"EnergyTotals":                vaillantPublicationValue(p.EnergyTotals()),
		"Schedules":                   vaillantPublicationValue(p.Schedules()),
		"FM5Interpretation":           vaillantPublicationValue(p.FM5Interpretation()),
		"FM5SemanticMode":             vaillantPublicationValue(p.FM5SemanticMode()),
		"VaillantRegulatorCapability": vaillantPublicationValue(p.VaillantRegulatorCapability()),
		"AdapterHardwareInfo":         vaillantPublicationValue(p.AdapterHardwareInfo()),
		"StartupPhase":                vaillantPublicationValue(p.StartupPhase()),
		"StartupEpochs":               map[string]any{"CacheEpoch": vaillantPublicationValue(cacheEpoch), "LiveEpoch": vaillantPublicationValue(liveEpoch)},
		"SemanticRegistryState":       map[string]any{"Snapshot": vaillantPublicationValue(registryState), "Error": vaillantPublicationError(registryErr)},
	}
}

func (h *vaillantPublicationHarness) mcpViews() map[string]any {
	v := h.mcpView
	views := map[string]any{
		"Zones":               vaillantPublicationValue(v.Zones()),
		"DHW":                 vaillantPublicationValue(v.DHW()),
		"Circuits":            vaillantPublicationValue(v.Circuits()),
		"BoilerStatus":        vaillantPublicationValue(v.BoilerStatus()),
		"System":              vaillantPublicationValue(v.System()),
		"RadioDevices":        vaillantPublicationValue(v.RadioDevices()),
		"Solar":               vaillantPublicationValue(v.Solar()),
		"Cylinders":           vaillantPublicationValue(v.Cylinders()),
		"EnergyTotals":        vaillantPublicationValue(v.EnergyTotals()),
		"Schedules":           vaillantPublicationValue(v.Schedules()),
		"FM5Interpretation":   vaillantPublicationValue(v.FM5Interpretation()),
		"FM5SemanticMode":     vaillantPublicationValue(v.FM5SemanticMode()),
		"AdapterHardwareInfo": vaillantPublicationValue(v.AdapterHardwareInfo()),
	}
	state, err := v.M8SemanticRegistryState()
	views["M8SemanticRegistryState"] = map[string]any{"State": vaillantPublicationValue(state), "Error": vaillantPublicationError(err)}
	return views
}

// hubEvents drains every stream. Publication never blocks, so the events of
// a step are in the buffers when the step returns.
func (h *vaillantPublicationHarness) hubEvents() map[string]any {
	out := make(map[string]any, len(h.streams))
	for _, stream := range h.streams {
		events := []any{}
	drain:
		for {
			select {
			case event, ok := <-stream.ch:
				if !ok {
					h.t.Fatalf("hub stream %s closed", stream.name)
				}
				events = append(events, vaillantPublicationValue(event))
			default:
				break drain
			}
		}
		if len(events) >= vaillantPublicationHubLimit {
			h.t.Fatalf("hub stream %s carried %d events in one step; the buffer may have dropped some", stream.name, len(events))
		}
		out[stream.name] = events
	}
	return out
}

func (h *vaillantPublicationHarness) takeSaves() []any {
	h.savesMu.Lock()
	defer h.savesMu.Unlock()
	saves := h.saves
	h.saves = nil
	if saves == nil {
		return []any{}
	}
	return saves
}

func vaillantPublicationWriteResult(result any, err error) any {
	return map[string]any{"Result": vaillantPublicationValue(result), "Error": vaillantPublicationError(err)}
}

func vaillantPublicationError(err error) any {
	if err == nil {
		return nil
	}
	return err.Error()
}

// refreshPass runs one pass of every refresh task of the poll schedule, then
// the FM5 refresh.
func (h *vaillantPublicationHarness) refreshPass(prefix string) {
	p := h.poller
	h.poll(prefix+"refresh_regulator_capability", p.refreshRegulatorCapability)
	h.poll(prefix+"refresh_discovery", p.refreshDiscovery)
	h.poll(prefix+"refresh_state", p.refreshState)
	h.poll(prefix+"refresh_config", p.refreshConfig)
	h.poll(prefix+"refresh_circuits", p.refreshCircuits)
	h.poll(prefix+"refresh_system", p.refreshSystem)
	h.poll(prefix+"refresh_energy", p.refreshEnergy)
	h.poll(prefix+"refresh_schedules", p.refreshSchedules)
	h.poll(prefix+"refresh_boiler_fast", p.boilerStatusTierTask(boilerStatusTierFast))
	h.poll(prefix+"refresh_boiler_medium", p.boilerStatusTierTask(boilerStatusTierMedium))
	h.poll(prefix+"refresh_boiler_slow", p.boilerStatusTierTask(boilerStatusTierSlow))
	h.poll(prefix+"refresh_radio_devices", p.refreshRadioDevices)
	h.poll(prefix+"refresh_fm5", p.refreshFM5Semantic)
}

func (h *vaillantPublicationHarness) writes() {
	temperature := 20.5
	h.step("write_boiler_config", func(ctx context.Context) any {
		return vaillantPublicationValue(h.graphqlWriter.SetBoilerConfig(ctx, "maximum_heating_flow_temperature_setpoint", "70"))
	})
	h.step("write_system_config", func(ctx context.Context) any {
		return vaillantPublicationValue(h.mcpConfigWriter.SetSystemConfig(ctx, "room_humidity_limit_maximum", "55"))
	})
	h.step("write_zone_time_program", func(ctx context.Context) any {
		slots := []mcp.TimeProgramSlot{{StartHour: 5, StartMinute: 30, EndHour: 21, EndMinute: 0, TemperatureC: &temperature}}
		result := vaillantPublicationWriteResult(h.mcpScheduleWriter.SetZoneTimeProgram(ctx, 0, 2, slots))
		h.awaitProgramRefresh(0, "heating", 2, slots[0])
		return result
	})
	h.step("write_dhw_time_program", func(ctx context.Context) any {
		slots := []mcp.TimeProgramSlot{{StartHour: 7, StartMinute: 0, EndHour: 9, EndMinute: 30}}
		result := vaillantPublicationWriteResult(h.graphqlWriter.SetDhwTimeProgram(ctx, 6, slots))
		h.awaitProgramRefresh(255, "dhw", 6, slots[0])
		return result
	})
}

// vaillantPublicationRefreshWait bounds, in wall-clock time, the wait for
// the program refresh a time-program write starts.
const vaillantPublicationRefreshWait = 10 * time.Second

// awaitProgramRefresh waits until the provider publishes weekday of the
// program with written as its only slot. A time-program write returns
// before the poller re-reads the written program on a goroutine of its own;
// waiting for that publication keeps the frames, the simulated clock and
// the recorded views of the step deterministic.
func (h *vaillantPublicationHarness) awaitProgramRefresh(zone int, hc string, weekday int, written mcp.TimeProgramSlot) {
	h.t.Helper()
	deadline := time.Now().Add(vaillantPublicationRefreshWait)
	for {
		if status := h.provider.Schedules(); status != nil {
			for _, program := range status.Programs {
				if program.Zone != zone || program.HC != hc || weekday >= len(program.Days) {
					continue
				}
				slots := program.Days[weekday].Slots
				if len(slots) == 1 && slots[0].StartHour == written.StartHour && slots[0].StartMinute == written.StartMinute &&
					slots[0].EndHour == written.EndHour && slots[0].EndMinute == written.EndMinute {
					return
				}
			}
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("program zone=%d hc=%s weekday=%d was not refreshed after the write", zone, hc, weekday)
		}
		time.Sleep(time.Millisecond)
	}
}

func (h *vaillantPublicationHarness) dump(scenario string) []byte {
	var out bytes.Buffer
	if err := vaillantPublicationRender(&out, map[string]any{"Scenario": scenario, "Steps": h.steps}, ""); err != nil {
		h.t.Fatalf("encode %s dump: %v", scenario, err)
	}
	out.WriteByte('\n')
	return out.Bytes()
}

// vaillantPublicationLineWidth bounds the one-line form of a JSON value.
const vaillantPublicationLineWidth = 120

func vaillantPublicationCompact(value any) ([]byte, error) {
	var out bytes.Buffer
	encoder := json.NewEncoder(&out)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(out.Bytes(), []byte("\n")), nil
}

// vaillantPublicationRender writes value as JSON with sorted keys: a value
// whose one-line form fits the line width stays on one line, every other
// object or array puts one member per line.
func vaillantPublicationRender(out *bytes.Buffer, value any, indent string) error {
	compact, err := vaillantPublicationCompact(value)
	if err != nil {
		return err
	}
	if len(indent)+len(compact) <= vaillantPublicationLineWidth {
		out.Write(compact)
		return nil
	}
	inner := indent + " "
	switch typed := value.(type) {
	case map[string]any:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		out.WriteString("{\n")
		for i, key := range keys {
			name, _ := vaillantPublicationCompact(key)
			out.WriteString(inner)
			out.Write(name)
			out.WriteString(": ")
			if err := vaillantPublicationRender(out, typed[key], inner); err != nil {
				return err
			}
			if i < len(keys)-1 {
				out.WriteByte(',')
			}
			out.WriteByte('\n')
		}
		out.WriteString(indent + "}")
	case []any:
		out.WriteString("[\n")
		for i, item := range typed {
			out.WriteString(inner)
			if err := vaillantPublicationRender(out, item, inner); err != nil {
				return err
			}
			if i < len(typed)-1 {
				out.WriteByte(',')
			}
			out.WriteByte('\n')
		}
		out.WriteString(indent + "]")
	default:
		out.Write(compact)
	}
	return nil
}

// runVaillantPublicationFullSite: startup semantic planes, one pass of each
// refresh task, then the configuration and time-program writes.
func runVaillantPublicationFullSite(t *testing.T) []byte {
	h := newVaillantPublicationHarness(t, "")
	h.poll("startup_discovery", h.poller.refreshDiscovery)
	h.refreshPass("")
	h.writes()
	return h.dump("full_site")
}

// vaillantPublicationFailingB524 and vaillantPublicationFailingB509 are the
// reads the site stops answering in the cache-hydrated scenario.
var (
	vaillantPublicationFailingB524 = []vaillantPublicationSelector{
		{opcode: vaillantB524OpcodeLocal, group: localZones.group, instance: 0x01, addr: zone_current_temp},
		{opcode: vaillantB524OpcodeLocal, group: localZones.group, instance: 0x01, addr: zone_current_humidity},
		{opcode: vaillantB524OpcodeLocal, group: localDHW.group, instance: dhwInstance, addr: dhw_current_temp},
		{opcode: vaillantB524OpcodeLocal, group: localRegulator.group, instance: regulatorInstance, addr: system_outdoor_temperature},
		{opcode: vaillantB524OpcodeLocal, group: localRegulator.group, instance: regulatorInstance, addr: energy_fuel_sum_hc_this_month},
		{opcode: vaillantB524OpcodeLocal, group: localCircuits.group, instance: 0x01, addr: circuit_flow_temp},
		{opcode: vaillantB524OpcodeLocal, group: localSolar.group, instance: solarInstance, addr: solar_collector_temp},
		{opcode: vaillantB524OpcodeLocal, group: localCylinders.group, instance: 0x01, addr: cylinder_temperature},
		{opcode: vaillantB524OpcodeRead, group: remoteThermostats.group, instance: 0x01, addr: device_slot_room_temperature},
	}
	vaillantPublicationFailingB509 = []uint16{boiler_b509_flow_temperature, boiler_b509_water_pressure}
)

// runVaillantPublicationCacheHydrated: hydrate from the seed, run one
// successful pass, then a pass while a fixed subset of reads fails.
func runVaillantPublicationCacheHydrated(t *testing.T) []byte {
	h := newVaillantPublicationHarness(t, filepath.Join(vaillantPublicationDir, "semantic_cache_seed.json"))
	h.step("cache_hydrated", func(context.Context) any { return nil })
	// A successful pass first gives every surface a last-known value; the
	// failing pass then shows which of them survive.
	h.poll("startup_discovery", h.poller.refreshDiscovery)
	h.refreshPass("")
	h.site.failReads(vaillantPublicationFailingB524, vaillantPublicationFailingB509)
	h.poll("failing_startup_discovery", h.poller.refreshDiscovery)
	h.refreshPass("failing_")
	return h.dump("cache_hydrated_partial_reads")
}

func TestVaillantPublicationGolden(t *testing.T) {
	for _, scenario := range []struct {
		golden string
		run    func(*testing.T) []byte
	}{
		{"full_site.golden.json", runVaillantPublicationFullSite},
		{"cache_hydrated_partial_reads.golden.json", runVaillantPublicationCacheHydrated},
	} {
		t.Run(scenario.golden, func(t *testing.T) {
			first := scenario.run(t)
			second := scenario.run(t)
			if !bytes.Equal(first, second) {
				t.Fatalf("two runs of the scenario differ:\n%s", vaillantPublicationFirstDifference(first, second))
			}
			path := filepath.Join(vaillantPublicationDir, scenario.golden)
			if os.Getenv(vaillantPublicationUpdateEnv) == "1" {
				if err := os.WriteFile(path, first, 0o644); err != nil {
					t.Fatalf("write golden: %v", err)
				}
				return
			}
			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read golden (%s=1 writes it): %v", vaillantPublicationUpdateEnv, err)
			}
			if !bytes.Equal(first, want) {
				t.Fatalf("published output differs from %s:\n%s", path, vaillantPublicationFirstDifference(want, first))
			}
		})
	}
}

func vaillantPublicationFirstDifference(want, got []byte) string {
	wantLines := bytes.Split(want, []byte("\n"))
	gotLines := bytes.Split(got, []byte("\n"))
	for i := 0; i < len(wantLines) || i < len(gotLines); i++ {
		var w, g []byte
		if i < len(wantLines) {
			w = wantLines[i]
		}
		if i < len(gotLines) {
			g = gotLines[i]
		}
		if !bytes.Equal(w, g) {
			return fmt.Sprintf("line %d:\n- %s\n+ %s", i+1, w, g)
		}
	}
	return "equal"
}

func vaillantPublicationValue(value any) any {
	return vaillantPublicationCanonical(reflect.ValueOf(value))
}

var vaillantPublicationTimeType = reflect.TypeOf(time.Time{})

// vaillantPublicationCanonical converts a value into the dump: exported Go
// field names, sorted map keys, null for nil pointers, interfaces, maps and
// slices, and floats in shortest round-trip form.
func vaillantPublicationCanonical(v reflect.Value) any {
	if !v.IsValid() {
		return nil
	}
	if v.Type() == vaillantPublicationTimeType {
		return v.Interface().(time.Time).UTC().Format(time.RFC3339Nano)
	}
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface:
		if v.IsNil() {
			return nil
		}
		return vaillantPublicationCanonical(v.Elem())
	case reflect.Struct:
		out := make(map[string]any, v.NumField())
		for i := 0; i < v.NumField(); i++ {
			field := v.Type().Field(i)
			if !field.IsExported() {
				continue
			}
			if vaillantPublicationWallClockFields[field.Name] && !v.Field(i).IsZero() {
				out[field.Name] = "<wall-clock>"
				continue
			}
			out[field.Name] = vaillantPublicationCanonical(v.Field(i))
		}
		return out
	case reflect.Map:
		if v.IsNil() {
			return nil
		}
		out := make(map[string]any, v.Len())
		keys := v.MapKeys()
		sort.Slice(keys, func(i, j int) bool { return fmt.Sprint(keys[i].Interface()) < fmt.Sprint(keys[j].Interface()) })
		for _, key := range keys {
			out[fmt.Sprint(key.Interface())] = vaillantPublicationCanonical(v.MapIndex(key))
		}
		return out
	case reflect.Slice, reflect.Array:
		if v.Kind() == reflect.Slice && v.IsNil() {
			return nil
		}
		out := make([]any, v.Len())
		for i := range out {
			out[i] = vaillantPublicationCanonical(v.Index(i))
		}
		return out
	case reflect.Float32, reflect.Float64:
		f := v.Float()
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return strconv.FormatFloat(f, 'g', -1, 64)
		}
		return json.Number(strconv.FormatFloat(f, 'g', -1, 64))
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return json.Number(strconv.FormatInt(v.Int(), 10))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return json.Number(strconv.FormatUint(v.Uint(), 10))
	case reflect.Bool:
		return v.Bool()
	case reflect.String:
		return v.String()
	default:
		return fmt.Sprintf("<%s>", v.Kind())
	}
}
