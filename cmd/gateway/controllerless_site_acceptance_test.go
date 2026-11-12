package main

// Issue #1469: offline acceptance of the site profile in which the eBUS carries
// a heat source and the vendor's internet module but never a system controller.
//
// The fixture is synthetic. Device classes, target addresses and the frame
// shapes it answers come from the public protocol documentation in
// helianthus-docs-ebus: protocols/ebus-services/ebus-service-07h.md (07 04
// identification), protocols/vaillant/ebus-vaillant-b509.md (B509 register
// reads and the 24..27 serial windows) and protocols/vaillant/ebus-vaillant.md
// (device-class table: heat source target 0x08 without B524, internet module
// target 0xF6 with B524 as initiator only). Serial numbers are synthetic: zero
// date, plant and sequence fields around a public product-catalogue part number
// that the test selects by catalogue role at run time. No identifier of a real
// installation is used.
//
// The scenario runs the production startup scan loop, the production registry
// scan, the production semantic poller constructor and its production poll
// schedule with default configuration. The replaced parts are the seams the
// existing startup and semantic tests already use:
//   - frames reach the synthetic bus through the scan bus, sendFrameFn and the
//     B524 probe seams; the gateway's own protocol bus runs over a counting
//     loopback that must stay silent for the whole run;
//   - polling time is simulated: the poller clock and the task-queue clock
//     follow one simulated clock, every recurring task is enqueued at the
//     instants its production ticker would fire, and the task queue is then
//     drained. On-wire duration is not modelled, so a frame takes zero
//     simulated time. The B509 read cache and the startup scan pacing keep the
//     wall clock, so heat-source read counts are not asserted exactly;
//   - simulated polling time 0 is the moment the semantic barrier opens. The
//     boot-live timer counts from process start, which is earlier, so the
//     scenario applies it at the latest instant it could fire;
//   - the passive observation path is not simulated. The internet module is
//     identified through the per-address identity probe that the passive
//     address-table inserter calls when it sees the module's traffic.
//
// Expected results, asserted below for every variant unless stated:
//   - startup, automatic source (add-on default): one scan pass; semantic
//     polling is released by the active probe, not by a controller. Explicit
//     source: two passes, then the loop gives up root confirmation and reports
//     admission failure while still releasing semantic polling;
//   - phases: a boiler-class heat source reaches LIVE_READY during the first
//     boiler priming pass at polling start, from the boiler stream alone. A
//     heat-pump-class heat source publishes no stream and the boot-live timer
//     moves BOOT_INIT to DEGRADED;
//   - no B524 register read, B555 read or write frame is ever sent. The only
//     controller-class traffic is B524 root discovery: one probe per candidate
//     (0x15 first, then the registry and structural targets) on every state
//     tick (60 s), config tick (5 min) and discovery tick (10 min), never more
//     than the per-instant plan below and never a tight loop;
//   - published: boiler status from the heat source only, an ABSENT FM5 mode,
//     empty zone, circuit, radio and cylinder lists, an all-null solar plane and
//     the zero/provenance-none energy projection; nothing for DHW, system or
//     schedules; no controller-owned boiler mirror field;
//   - absence: vaillant_regulator_capability is NONE from the first discovery
//     (UNKNOWN when an identified module has no readable serial), identical in
//     GraphQL and MCP; the absence state goes PRESENT -> ABSENCE_GRACE at the
//     first recheck (60 s) -> ABSENT five minutes later (360 s).
//
// Nothing here writes to a bus or answers as a controller: the synthetic bus
// leaves every B524 request unanswered and the test fails on any write frame.

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	"github.com/Project-Helianthus/helianthus-ebusreg/vaillant/productids"
	graphqlgo "github.com/graphql-go/graphql"
)

const (
	controllerlessSource         = byte(0xF0)
	controllerlessCompanion      = byte(0xF5)
	controllerlessHeatSource     = byte(0x08)
	controllerlessHeatSourceInit = byte(0x03)
	controllerlessRegulator      = byte(0x15)
	controllerlessModuleSlot     = byte(0x26)
	controllerlessInternetModule = byte(0xF6)
	controllerlessHorizon        = 30 * time.Minute
	// controllerlessTaskLimit bounds the tasks one simulated instant may run.
	// Twelve recurring families exist; a queue that keeps refilling itself
	// within one instant is a tight loop.
	controllerlessTaskLimit = 64
)

// Frame classes the synthetic bus records.
const (
	controllerlessIdentify     = "identification_07_04"
	controllerlessSerialWindow = "b509_serial_window"
	controllerlessB509Read     = "b509_register_read"
	controllerlessB524Frame    = "b524_frame"
	controllerlessWriteFrame   = "write_frame"
	controllerlessOtherFrame   = "other_frame"
	controllerlessRootProbe    = "b524_root_probe"
	controllerlessStartupProbe = "b524_root_probe_startup"
)

type controllerlessClock struct {
	mu     sync.Mutex
	base   time.Time
	offset time.Duration
}

func newControllerlessClock() *controllerlessClock {
	return &controllerlessClock{base: time.Date(2001, time.January, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *controllerlessClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.base.Add(c.offset)
}

func (c *controllerlessClock) elapsed() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.offset
}

func (c *controllerlessClock) set(offset time.Duration) {
	c.mu.Lock()
	c.offset = offset
	c.mu.Unlock()
}

type controllerlessDevice struct {
	deviceID    string // five ASCII characters returned by 07 04
	serialRaw   string // 28 characters for the 24..27 windows; empty leaves them unanswered
	answersB509 bool   // answers B509 0D register reads
}

type controllerlessEvent struct {
	at       time.Duration
	kind     string
	target   byte
	register uint16
}

// controllerlessBus is the synthetic eBUS: present devices answer the frame
// shapes listed above, every other address is silent (no acknowledgement, the
// timeout class the protocol bus reports), and no device answers B524.
type controllerlessBus struct {
	mu      sync.Mutex
	clock   *controllerlessClock
	devices map[byte]controllerlessDevice
	events  []controllerlessEvent
}

// controllerlessHeatSourceRegisters holds synthetic B509 values for the
// registers whose published value the scenario checks. Every other register
// the gateway reads answers with two zero bytes.
var controllerlessHeatSourceRegisters = map[uint16][]byte{
	boiler_b509_water_pressure:     {0x80, 0x01}, // DATA2b 1.5 bar
	boiler_b509_flame_active:       {0xF0},       // off
	boiler_b509_flowset_hc_max_c:   {0xB0, 0x04}, // DATA2c 75 °C
	boiler_b509_dhw_temp_desired_c: {0x20, 0x03}, // DATA2c 50 °C
	boiler_b509_phone_number:       {0xFF},       // no BCD digit
}

func newControllerlessBus() *controllerlessBus {
	return &controllerlessBus{clock: newControllerlessClock(), devices: map[byte]controllerlessDevice{}}
}

func controllerlessFrameKind(frame protocol.Frame) string {
	switch {
	case frame.Primary == 0x07 && frame.Secondary == 0x04:
		return controllerlessIdentify
	case frame.Primary == vaillantB509Primary && frame.Secondary == vaillantB509Secondary && len(frame.Data) > 0:
		switch op := frame.Data[0]; {
		case op >= 0x24 && op <= 0x27 && len(frame.Data) == 1:
			return controllerlessSerialWindow
		case op == vaillantB509OpcodeRead && len(frame.Data) == 3:
			return controllerlessB509Read
		case op == vaillantB509OpcodeWrite:
			return controllerlessWriteFrame
		}
	case frame.Primary == vaillantExtRegisterPrimary && frame.Secondary == vaillantExtRegisterSecondary:
		if len(frame.Data) > 1 && frame.Data[1] == vaillantB524OpWrite {
			return controllerlessWriteFrame
		}
		return controllerlessB524Frame
	}
	return controllerlessOtherFrame
}

func (b *controllerlessBus) record(event controllerlessEvent) {
	event.at = b.clock.elapsed()
	b.mu.Lock()
	b.events = append(b.events, event)
	b.mu.Unlock()
}

func (b *controllerlessBus) Send(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
	kind := controllerlessFrameKind(frame)
	event := controllerlessEvent{kind: kind, target: frame.Target}
	if kind == controllerlessB509Read {
		event.register = uint16(frame.Data[1])<<8 | uint16(frame.Data[2])
	}
	b.record(event)
	silent := fmt.Errorf("synthetic bus: no answer from 0x%02X: %w", frame.Target, ebuserrors.ErrTimeout)
	b.mu.Lock()
	device, present := b.devices[frame.Target]
	b.mu.Unlock()
	if !present {
		return nil, silent
	}
	reply := func(data []byte) *protocol.Frame {
		return &protocol.Frame{Source: frame.Target, Target: frame.Source, Primary: frame.Primary, Secondary: frame.Secondary, Data: data}
	}
	switch kind {
	case controllerlessIdentify:
		payload := append([]byte{0xB5}, []byte(device.deviceID)...)
		return reply(append(payload, 0x01, 0x00, 0x01, 0x00)), nil
	case controllerlessSerialWindow:
		if device.serialRaw == "" {
			return nil, silent
		}
		padded := device.serialRaw + strings.Repeat(" ", 32-len(device.serialRaw))
		start := int(frame.Data[0]-0x24) * 8
		return reply(append([]byte{0x00}, padded[start:start+8]...)), nil
	case controllerlessB509Read:
		if !device.answersB509 {
			return nil, silent
		}
		value, ok := controllerlessHeatSourceRegisters[event.register]
		if !ok {
			value = []byte{0x00, 0x00}
		}
		return reply(append([]byte{vaillantB509OpcodeRead, frame.Data[1], frame.Data[2]}, value...)), nil
	}
	return nil, silent
}

// semanticRootProbe and startupRootProbe stand in for one production B524
// root probe call each. No device on this bus answers B524, so every probe is
// incoherent. Against a silent address the production probe makes at most
// three bus attempts per call (probeB524Register).
func (b *controllerlessBus) semanticRootProbe(_ context.Context, target, _, _, _ byte, _ uint16) bool {
	b.record(controllerlessEvent{kind: controllerlessRootProbe, target: target})
	return false
}

func (b *controllerlessBus) startupRootProbe(_ context.Context, target, _, _, _ byte, _ uint16) bool {
	b.record(controllerlessEvent{kind: controllerlessStartupProbe, target: target})
	return false
}

func (b *controllerlessBus) addDevice(target byte, device controllerlessDevice) {
	b.mu.Lock()
	b.devices[target] = device
	b.mu.Unlock()
}

func (b *controllerlessBus) eventsOf(kind string) []controllerlessEvent {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []controllerlessEvent
	for _, event := range b.events {
		if event.kind == kind {
			out = append(out, event)
		}
	}
	return out
}

func (b *controllerlessBus) countByTarget(kind string) map[byte]int {
	counts := make(map[byte]int)
	for _, event := range b.eventsOf(kind) {
		counts[event.target]++
	}
	return counts
}

func (b *controllerlessBus) probesByInstant() map[time.Duration]int {
	out := make(map[time.Duration]int)
	for _, event := range b.eventsOf(controllerlessRootProbe) {
		out[event.at]++
	}
	return out
}

// controllerlessCatalogPart selects a ten-digit part number that the public
// product catalogue classifies with role and as a non-controller.
func controllerlessCatalogPart(t *testing.T, role string) string {
	t.Helper()
	catalog, err := productids.LoadCatalog()
	if err != nil {
		t.Fatalf("LoadCatalog() error = %v", err)
	}
	for _, record := range catalog.All {
		part := strings.TrimSpace(record.PartNumber)
		if !strings.EqualFold(strings.TrimSpace(record.Role), role) || len(part) != 10 || !isAllDigits(part) {
			continue
		}
		if catalog.ControllerCapability(part) == productids.ControllerNone {
			return part
		}
	}
	t.Fatalf("product catalogue has no non-controller %q part number", role)
	return ""
}

// controllerlessSerialRaw builds the synthetic 28-character serial the 24..27
// windows return: zero date, plant and sequence fields around part.
func controllerlessSerialRaw(part string) string {
	return "000000" + part + "0000000000" + "N0"
}

// controllerlessSerial is the registry form of controllerlessSerialRaw.
func controllerlessSerial(part string) string {
	return "00-00-00-" + part + "-0000-000000-N0"
}

// controllerlessSilentTransport counts writes to the gateway's own protocol
// bus. Every frame of this scenario must take a fixture seam instead.
type controllerlessSilentTransport struct {
	*transport.Loopback
	writes atomic.Int64
}

func (t *controllerlessSilentTransport) Write(payload []byte) (int, error) {
	t.writes.Add(1)
	return t.Loopback.Write(payload)
}

type controllerlessStartup struct {
	gateway             *ebusgateway.Gateway
	silent              *controllerlessSilentTransport
	cfg                 ebusgateway.Config
	passes              int
	activeProbePassed   bool
	admissionFailed     bool
	identityEnrichCalls int
	delayedRetryCalls   int
}

// runControllerlessStartupScan runs the production startup scan loop in the
// add-on default admission (adapter-direct, automatic source with its
// companion) or with an explicitly configured source, over the synthetic bus.
// The seams stay installed until the test ends so the poller's per-address
// identity probe reaches the same bus.
func runControllerlessStartupScan(t *testing.T, bus *controllerlessBus, explicitSource bool) *controllerlessStartup {
	t.Helper()
	silent := &controllerlessSilentTransport{Loopback: transport.NewLoopback()}
	gateway, err := ebusgateway.New(context.Background(), ebusgateway.Config{Transport: silent})
	if err != nil {
		t.Fatalf("ebusgateway.New() error = %v", err)
	}
	busCtx, stopBus := context.WithCancel(context.Background())
	go gateway.Start(busCtx)
	t.Cleanup(func() {
		stopBus()
		if err := gateway.Close(); err != nil {
			t.Errorf("gateway.Close() error = %v", err)
		}
	})

	cfg := ebusgateway.DefaultConfig()
	cfg.ScanOnStart = true
	// Pass spacing only; the default is 30 s and the scenario counts passes.
	cfg.ScanInterval = 10 * time.Millisecond
	cfg.TransportConfig = ebusgateway.TransportConfig{Protocol: ebusgateway.TransportAdapterDirect, Network: "tcp", Address: "adapter.invalid:9999"}
	cfg.ScanSource = controllerlessSource
	selection := protocol.SourceAddressSelection{Source: controllerlessSource}
	if explicitSource {
		source := controllerlessSource
		cfg.StartupSource.Source = &source
	} else {
		cfg.StartupCompanionTarget = controllerlessCompanion
		selection.Companion = controllerlessCompanion
	}
	// No passive warm-up evidence: the structural targets are probed.
	cfg.StartupProbeTargets = startupProbeTargetsForSelection(selection)

	startup := &controllerlessStartup{gateway: gateway, silent: silent, cfg: cfg}
	var mu sync.Mutex
	savedScan, savedDirected := registryScanFn, registryScanDirectedFn
	savedProbe, savedExit := startupScanB524ProbeFn, startupScanLoopExitFn
	savedIdentity, savedSerials := enrichVaillantIdentityFn, enrichSerialsFromEbusdFn
	savedB524Identity, savedDelayed := enrichB524InterfaceIdentityFn, postStartupIdentityRetryFn
	savedTargets, savedInfos := ebusdScanResultTargetsFn, ebusdScanResultInfosFn
	t.Cleanup(func() {
		registryScanFn, registryScanDirectedFn = savedScan, savedDirected
		startupScanB524ProbeFn, startupScanLoopExitFn = savedProbe, savedExit
		enrichVaillantIdentityFn, enrichSerialsFromEbusdFn = savedIdentity, savedSerials
		enrichB524InterfaceIdentityFn, postStartupIdentityRetryFn = savedB524Identity, savedDelayed
		ebusdScanResultTargetsFn, ebusdScanResultInfosFn = savedTargets, savedInfos
	})

	// The production directed scan runs unchanged; only the bus beneath the
	// loop's counting scan bus is the synthetic one.
	registryScanDirectedFn = func(ctx context.Context, scanBus registry.ScanBus, reg *registry.DeviceRegistry, source byte, targets []byte) ([]registry.DeviceEntry, error) {
		if stats, ok := scanBus.(*statsBus); ok {
			mu.Lock()
			startup.passes++
			mu.Unlock()
			stats.bus = &timeoutBus{bus: bus, timeout: cfg.ScanRequestTimeout}
			return registry.ScanDirected(ctx, stats, reg, source, targets)
		}
		return registry.ScanDirected(ctx, bus, reg, source, targets)
	}
	registryScanFn = func(context.Context, registry.ScanBus, *registry.DeviceRegistry, byte, []byte) ([]registry.DeviceEntry, error) {
		t.Error("adapter-direct admission must use the directed scan")
		return nil, nil
	}
	ebusdScanResultTargetsFn = func(context.Context, ebusgateway.TransportConfig) ([]byte, error) {
		t.Error("ebusd scan result queried on an adapter-direct transport")
		return nil, nil
	}
	ebusdScanResultInfosFn = func(context.Context, ebusgateway.TransportConfig) ([]registry.DeviceInfo, error) {
		t.Error("ebusd scan result queried on an adapter-direct transport")
		return nil, nil
	}
	startupScanB524ProbeFn = bus.startupRootProbe
	// Identity enrichment re-reads the same 24..27 windows the scan already
	// read; with this fixture its outcome is the scan's, so only calls count.
	enrichVaillantIdentityFn = func(context.Context, *ebusgateway.Gateway, ebusgateway.Config) {
		mu.Lock()
		startup.identityEnrichCalls++
		mu.Unlock()
	}
	enrichSerialsFromEbusdFn = func(context.Context, *registry.DeviceRegistry, ebusgateway.TransportConfig) {
		t.Error("ebusd serial enrichment on an adapter-direct transport")
	}
	enrichB524InterfaceIdentityFn = func(ctx context.Context, reg *registry.DeviceRegistry, _ registry.ScanBus, source, companion byte) {
		enrichB524InterfaceIdentity(ctx, reg, bus, source, companion)
	}
	postStartupIdentityRetryFn = func(context.Context, *ebusgateway.Gateway, *graphql.Builder, ebusgateway.Config, *ebusgateway.TransportConfig) {
		mu.Lock()
		startup.delayedRetryCalls++
		mu.Unlock()
	}
	startupScanLoopExitFn = nil

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	signals := startDiscoveryScanLoop(ctx, cfg, gateway, nil)
	for _, wait := range []struct {
		name string
		ch   <-chan struct{}
	}{{"semantic bootstrap", signals.semanticBootstrapReady}, {"scan loop exit", signals.done}} {
		select {
		case <-wait.ch:
		case <-time.After(30 * time.Second):
			t.Fatalf("%s not signalled: the startup scan never finished on the controller-less bus", wait.name)
		}
	}
	select {
	case <-signals.activeProbePassed:
		startup.activeProbePassed = true
	default:
	}
	select {
	case <-signals.admissionFailed:
		startup.admissionFailed = true
	default:
	}
	mu.Lock()
	defer mu.Unlock()
	return startup
}

type controllerlessObservation struct {
	at    time.Duration
	value string
}

type controllerlessRun struct {
	provider           *graphql.LiveSemanticProvider
	phases             []controllerlessObservation
	absence            []controllerlessObservation
	capability         []controllerlessObservation
	recurringFamilies  int
	maxTasksPerInstant int
	phaseAtBootTimeout graphql.SemanticStartupPhase
}

func (r *controllerlessRun) note(series *[]controllerlessObservation, at time.Duration, value string) {
	if n := len(*series); n > 0 && (*series)[n-1].value == value {
		return
	}
	*series = append(*series, controllerlessObservation{at: at, value: value})
}

// runControllerlessPolling starts the production poller over the startup
// registry and drives its production schedule over simulated time. A script
// entry runs at its instant before the tickers due at the same instant.
func runControllerlessPolling(t *testing.T, startup *controllerlessStartup, bus *controllerlessBus, script map[time.Duration]func(*vaillantSemanticPoller)) *controllerlessRun {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clock := bus.clock
	clock.set(0)

	provider := graphql.NewLiveSemanticProvider()
	poller := newVaillantSemanticPoller(startup.cfg, startup.gateway, provider, nil, nil)
	poller.nowFn = clock.Now
	poller.tasks.now = clock.Now
	poller.sendFrameFn = bus.Send
	poller.b524ProbeFn = bus.semanticRootProbe
	run := &controllerlessRun{provider: provider}

	observe := func() {
		at := clock.elapsed()
		run.note(&run.phases, at, string(provider.StartupPhase()))
		poller.mu.Lock()
		state := poller.regAbsenceState
		poller.mu.Unlock()
		run.note(&run.absence, at, string(state))
		run.note(&run.capability, at, string(provider.VaillantRegulatorCapability()))
	}
	drain := func() {
		queue := poller.tasks
		ran := 0
		for {
			queue.mu.Lock()
			pending := len(queue.queue)
			queue.mu.Unlock()
			if pending == 0 {
				break
			}
			task := queue.nextTask(ctx)
			if task == nil {
				t.Fatal("task queue stopped during the simulation")
			}
			task.run(ctx)
			queue.taskDone(task)
			ran++
			observe()
			if ran > controllerlessTaskLimit {
				t.Fatalf("tight loop: more than %d tasks at %s", controllerlessTaskLimit, clock.elapsed())
			}
		}
		if ran > run.maxTasksPerInstant {
			run.maxTasksPerInstant = ran
		}
	}

	steps := map[time.Duration]func(*vaillantSemanticPoller){}
	for at, step := range script {
		steps[at] = step
	}
	bootTimeout := startup.cfg.BootLiveTimeout
	scripted := steps[bootTimeout]
	// The production boot timer moves any phase other than LIVE_READY to
	// DEGRADED when it fires (graphql.StartBootFSM); apply it at the latest
	// instant it can fire relative to polling start.
	steps[bootTimeout] = func(p *vaillantSemanticPoller) {
		if scripted != nil {
			scripted(p)
		}
		run.phaseAtBootTimeout = provider.StartupPhase()
		if run.phaseAtBootTimeout == graphql.SemanticStartupPhaseLiveReady {
			return
		}
		provider.StartBootFSM(ctx, time.Nanosecond, func(string, ...any) {})
		deadline := time.Now().Add(5 * time.Second)
		for provider.StartupPhase() != graphql.SemanticStartupPhaseDegraded {
			if time.Now().After(deadline) {
				t.Fatalf("boot-live timer did not move phase %s to DEGRADED", provider.StartupPhase())
			}
			time.Sleep(time.Millisecond)
		}
	}

	observe()
	schedule := poller.pollSchedule()
	run.recurringFamilies = len(schedule.recurring)
	for _, task := range schedule.initial {
		poller.enqueueTask(task.key, task.priority, task.refresh)
	}
	drain()

	next := make([]time.Duration, len(schedule.recurring))
	for i, task := range schedule.recurring {
		if task.interval <= 0 {
			t.Fatalf("recurring task %s has no interval", task.key)
		}
		next[i] = task.interval
	}
	var stepTimes []time.Duration
	for at := range steps {
		stepTimes = append(stepTimes, at)
	}
	sort.Slice(stepTimes, func(i, j int) bool { return stepTimes[i] < stepTimes[j] })

	for {
		at := controllerlessHorizon + 1
		for _, due := range next {
			if due < at {
				at = due
			}
		}
		if len(stepTimes) > 0 && stepTimes[0] < at {
			at = stepTimes[0]
		}
		if at > controllerlessHorizon {
			break
		}
		clock.set(at)
		if len(stepTimes) > 0 && stepTimes[0] == at {
			steps[at](poller)
			stepTimes = stepTimes[1:]
			observe()
		}
		for i, task := range schedule.recurring {
			if next[i] == at {
				poller.enqueueTask(task.key, task.priority, task.refresh)
				next[i] += task.interval
			}
		}
		drain()
	}
	return run
}

// controllerlessDiscoveryPlan is the expected number of root probes per
// instant: one discovery pass at polling start, then one per state tick
// (60 s), one more per config tick (5 min) and one more per discovery tick
// (10 min), each probing every candidate once; extra adds event-driven probes.
func controllerlessDiscoveryPlan(candidates func(minute int) int, extra map[int]int) map[time.Duration]int {
	plan := map[time.Duration]int{0: candidates(0)}
	for minute := 1; minute <= int(controllerlessHorizon/time.Minute); minute++ {
		passes := 1
		if minute%5 == 0 {
			passes++
		}
		if minute%10 == 0 {
			passes++
		}
		plan[time.Duration(minute)*time.Minute] = passes*candidates(minute) + extra[minute]
	}
	return plan
}

func controllerlessGraphQL(t *testing.T, provider *graphql.LiveSemanticProvider) map[string]any {
	t.Helper()
	builder := graphql.NewBuilder(nil, nil)
	builder.SetSemanticProvider(provider)
	schema, err := graphql.NewQuerySchema(builder)
	if err != nil {
		t.Fatalf("NewQuerySchema() error = %v", err)
	}
	result := graphqlgo.Do(graphqlgo.Params{Schema: schema, RequestString: `{
		vaillant_regulator_capability
		zones { __typename }
		dhw { __typename }
		circuits { __typename }
		radio_devices { __typename }
		system { __typename }
		schedules { __typename }
		cylinders { __typename }
		fm5_semantic_mode
		solar { collector_temperature_c return_temperature_c pump_active current_yield pump_hours solar_enabled function_mode }
		energy_totals {
			gas { dhw { today yearly monthly today_meta { provenance last_observed_utc } } climate { today yearly monthly } }
			electric { dhw { today yearly monthly } climate { today yearly monthly } }
			solar { dhw { today yearly monthly } climate { today yearly monthly } }
		}
		boiler_status {
			state { water_pressure_bar flame_active flow_temperature_c dhw_temperature_c dhw_target_temperature_c dhw_temp_desired_c }
			config { flowset_hc_max_c dhw_operating_mode }
		}
	}`})
	if len(result.Errors) != 0 {
		t.Fatalf("GraphQL errors = %v", result.Errors)
	}
	return result.Data.(map[string]any)
}

func controllerlessMCPCapability(t *testing.T, provider *graphql.LiveSemanticProvider) any {
	t.Helper()
	server, err := mcp.NewServer(emptyMCPRegistry{}, nil)
	if err != nil {
		t.Fatalf("mcp.NewServer() error = %v", err)
	}
	server.SetStatusProvider(newMCPRuntimeStatusProvider(provider, nil))
	return mcpCallToolEnvelope(t, server.Handler(), "ebus_v1_runtime_status_get", `{}`)["data"].(map[string]any)["vaillant_regulator_capability"]
}

func controllerlessPath(data map[string]any, path ...string) any {
	var current any = data
	for _, key := range path {
		object, ok := current.(map[string]any)
		if !ok {
			return fmt.Sprintf("<no object at %q>", key)
		}
		current = object[key]
	}
	return current
}

func controllerlessRegistryEntries(reg *registry.DeviceRegistry) map[byte]registry.DeviceEntrySnapshot {
	entries := make(map[byte]registry.DeviceEntrySnapshot)
	reg.IterateSnapshots(func(snap registry.DeviceEntrySnapshot) bool {
		entries[snap.PrimaryAddress] = snap
		return true
	})
	return entries
}

func controllerlessExpectSeries(t *testing.T, name string, got []controllerlessObservation, want []controllerlessObservation) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s = %v; want %v", name, got, want)
	}
}

// controllerlessExpectCommon asserts the results every variant shares.
func controllerlessExpectCommon(t *testing.T, bus *controllerlessBus, startup *controllerlessStartup, run *controllerlessRun, wantCapability string) map[string]any {
	t.Helper()
	for _, kind := range []string{controllerlessB524Frame, controllerlessWriteFrame, controllerlessOtherFrame} {
		if got := bus.countByTarget(kind); len(got) != 0 {
			t.Errorf("%s frames = %v; want none on a controller-less bus", kind, got)
		}
	}
	if writes := startup.silent.writes.Load(); writes != 0 {
		t.Errorf("gateway protocol bus writes = %d; want 0 (every frame takes a fixture seam)", writes)
	}
	for _, read := range bus.eventsOf(controllerlessB509Read) {
		if read.target != controllerlessHeatSource {
			t.Errorf("B509 register read to 0x%02X at %s; want only the heat source", read.target, read.at)
		}
	}
	controllerlessExpectSeries(t, "absence states", run.absence, []controllerlessObservation{
		{at: 0, value: string(regulatorPresent)},
		{at: time.Minute, value: string(regulatorAbsenceGrace)},
		{at: 6 * time.Minute, value: string(regulatorAbsent)},
	})
	if got := semanticRegulatorState.Value(); got != string(regulatorAbsent) {
		t.Errorf("semantic_regulator_state = %q; want %q", got, regulatorAbsent)
	}
	if run.maxTasksPerInstant != run.recurringFamilies {
		t.Errorf("most tasks run at one instant = %d; want %d, every recurring family due once", run.maxTasksPerInstant, run.recurringFamilies)
	}
	probes := bus.eventsOf(controllerlessRootProbe)
	if len(probes) < 3 || probes[0].target != controllerlessRegulator || probes[1].target != controllerlessHeatSource || probes[2].target != controllerlessModuleSlot {
		t.Errorf("first discovery pass probes %v; want 0x15, then 0x08 and 0x26", probes)
	}

	view := controllerlessGraphQL(t, run.provider)
	if got := view["vaillant_regulator_capability"]; got != wantCapability {
		t.Errorf("GraphQL vaillant_regulator_capability = %v; want %s", got, wantCapability)
	}
	if got := controllerlessMCPCapability(t, run.provider); got != wantCapability {
		t.Errorf("MCP vaillant_regulator_capability = %v; want %s", got, wantCapability)
	}
	for _, plane := range []string{"zones", "circuits", "radio_devices", "cylinders"} {
		if got, ok := view[plane].([]any); !ok || len(got) != 0 {
			t.Errorf("GraphQL %s = %#v; want an empty list", plane, view[plane])
		}
	}
	for _, plane := range []string{"dhw", "system", "schedules"} {
		if got := view[plane]; got != nil {
			t.Errorf("GraphQL %s = %#v; want null without a controller", plane, got)
		}
	}
	if got := view["fm5_semantic_mode"]; got != string(graphql.Fm5SemanticModeAbsent) {
		t.Errorf("GraphQL fm5_semantic_mode = %v; want %s", got, graphql.Fm5SemanticModeAbsent)
	}
	solar, ok := view["solar"].(map[string]any)
	if !ok {
		t.Errorf("GraphQL solar = %#v; want the absent-FM5 placeholder", view["solar"])
	}
	for field, value := range solar {
		if value != nil {
			t.Errorf("GraphQL solar.%s = %v; want null without a controller", field, value)
		}
	}
	for _, channel := range []string{"gas", "electric", "solar"} {
		for _, usage := range []string{"dhw", "climate"} {
			if got := controllerlessPath(view, "energy_totals", channel, usage, "today"); got != 0.0 {
				t.Errorf("energy_totals.%s.%s.today = %v; want the 0 placeholder", channel, usage, got)
			}
			for _, series := range []string{"yearly", "monthly"} {
				if got, ok := controllerlessPath(view, "energy_totals", channel, usage, series).([]any); !ok || len(got) != 0 {
					t.Errorf("energy_totals.%s.%s.%s = %#v; want empty", channel, usage, series, got)
				}
			}
		}
	}
	if got := controllerlessPath(view, "energy_totals", "gas", "dhw", "today_meta", "provenance"); got != "none" {
		t.Errorf("energy_totals today provenance = %v; want none (no observation)", got)
	}
	if got := controllerlessPath(view, "energy_totals", "gas", "dhw", "today_meta", "last_observed_utc"); got != nil {
		t.Errorf("energy_totals today last_observed_utc = %v; want null", got)
	}
	return view
}

// controllerlessExpectBoilerStream asserts that the boiler stream carries the
// heat source's own B509 values and no controller-owned mirror field.
func controllerlessExpectBoilerStream(t *testing.T, bus *controllerlessBus, view map[string]any) {
	t.Helper()
	pressure, _ := decodeDATA2b(controllerlessHeatSourceRegisters[boiler_b509_water_pressure])
	flowset, _ := decodeDATA2c(controllerlessHeatSourceRegisters[boiler_b509_flowset_hc_max_c])
	dhwDesired, _ := decodeDATA2c(controllerlessHeatSourceRegisters[boiler_b509_dhw_temp_desired_c])
	for path, want := range map[string]any{
		"state.water_pressure_bar":       pressure,
		"state.flame_active":             false,
		"state.dhw_temp_desired_c":       dhwDesired,
		"config.flowset_hc_max_c":        flowset,
		"state.flow_temperature_c":       nil, // Hypothesis mapping: no qualified binding in production
		"state.dhw_temperature_c":        nil, // controller DHW mirror
		"state.dhw_target_temperature_c": nil, // controller DHW mirror
		"config.dhw_operating_mode":      nil, // controller DHW mirror
	} {
		got := controllerlessPath(view, append([]string{"boiler_status"}, strings.Split(path, ".")...)...)
		if got != want {
			t.Errorf("boiler_status.%s = %v; want %v", path, got, want)
		}
	}
	read := map[uint16]bool{}
	for _, event := range bus.eventsOf(controllerlessB509Read) {
		read[event.register] = true
	}
	// One register of each boiler tier: fast, medium, slow.
	for _, register := range []uint16{boiler_b509_water_pressure, boiler_b509_flowset_hc_max_c, boiler_b509_central_heating_hours} {
		if !read[register] {
			t.Errorf("heat source register 0x%04X never read; want every boiler tier polled", register)
		}
	}
	t.Logf("distinct heat source B509 registers read: %d", len(read))
}

func TestControllerlessSiteStartupWithoutController(t *testing.T) {
	boilerPart := controllerlessCatalogPart(t, "Boiler")
	for _, tc := range []struct {
		name            string
		explicitSource  bool
		wantPasses      int
		wantActive      bool
		wantFailed      bool
		wantIdentify    map[byte]int
		wantSerialReads map[byte]int
		wantRootProbes  map[byte]int
	}{
		{
			// Source selection admits on the first pass on which any device
			// answers; the silent controller and module-slot addresses are
			// retried by the registry scan three more times within that pass.
			name: "automatic source", wantPasses: 1, wantActive: true,
			wantIdentify:    map[byte]int{controllerlessHeatSource: 1, controllerlessRegulator: 4, controllerlessModuleSlot: 4},
			wantSerialReads: map[byte]int{controllerlessHeatSource: 4},
			wantRootProbes:  map[byte]int{controllerlessHeatSource: 1},
		},
		{
			// Without source selection the loop waits for a coherent B524 root,
			// gives up after two passes and releases semantic polling while
			// reporting admission failure.
			name: "explicit source", explicitSource: true, wantPasses: 2, wantFailed: true,
			wantIdentify:    map[byte]int{controllerlessHeatSource: 2, controllerlessRegulator: 8, controllerlessModuleSlot: 8},
			wantSerialReads: map[byte]int{controllerlessHeatSource: 8},
			wantRootProbes:  map[byte]int{controllerlessHeatSource: 2},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bus := newControllerlessBus()
			bus.addDevice(controllerlessHeatSource, controllerlessDevice{deviceID: "BAI00", serialRaw: controllerlessSerialRaw(boilerPart), answersB509: true})
			startup := runControllerlessStartupScan(t, bus, tc.explicitSource)

			if startup.passes != tc.wantPasses {
				t.Errorf("scan passes before semantic polling = %d; want %d", startup.passes, tc.wantPasses)
			}
			if startup.activeProbePassed != tc.wantActive || startup.admissionFailed != tc.wantFailed {
				t.Errorf("active probe passed = %v, admission failed = %v; want %v, %v", startup.activeProbePassed, startup.admissionFailed, tc.wantActive, tc.wantFailed)
			}
			for kind, want := range map[string]map[byte]int{
				controllerlessIdentify:     tc.wantIdentify,
				controllerlessSerialWindow: tc.wantSerialReads,
				controllerlessStartupProbe: tc.wantRootProbes,
			} {
				if got := bus.countByTarget(kind); !reflect.DeepEqual(got, want) {
					t.Errorf("%s attempts by target = %v; want %v", kind, got, want)
				}
			}
			for _, kind := range []string{controllerlessB509Read, controllerlessB524Frame, controllerlessWriteFrame, controllerlessOtherFrame, controllerlessRootProbe} {
				if got := bus.countByTarget(kind); len(got) != 0 {
					t.Errorf("%s during startup = %v; want none", kind, got)
				}
			}
			if startup.identityEnrichCalls != tc.wantPasses || startup.delayedRetryCalls != 0 {
				t.Errorf("identity enrichment calls = %d, delayed retries = %d; want %d, 0", startup.identityEnrichCalls, startup.delayedRetryCalls, tc.wantPasses)
			}
			if writes := startup.silent.writes.Load(); writes != 0 {
				t.Errorf("gateway protocol bus writes = %d; want 0", writes)
			}
			entries := controllerlessRegistryEntries(startup.gateway.Registry)
			heatSource, ok := entries[controllerlessHeatSource]
			if len(entries) != 1 || !ok {
				t.Fatalf("registry = %v; want only the heat source", entries)
			}
			if !reflect.DeepEqual(heatSource.Addresses, []byte{controllerlessHeatSource, controllerlessHeatSourceInit}) ||
				heatSource.Manufacturer != "Vaillant" || heatSource.DeviceID != "BAI00" || heatSource.SerialNumber != controllerlessSerial(boilerPart) {
				t.Errorf("heat source entry = addresses %v, %q, %q, %q; want [8 3], Vaillant, BAI00, the synthetic serial",
					heatSource.Addresses, heatSource.Manufacturer, heatSource.DeviceID, heatSource.SerialNumber)
			}
		})
	}
}

func TestControllerlessSitePollingOverSimulatedTime(t *testing.T) {
	boilerPart := controllerlessCatalogPart(t, "Boiler")
	heatPumpPart := controllerlessCatalogPart(t, "Heat Pump")
	modulePart := controllerlessCatalogPart(t, "Gateway")
	threeCandidates := func(int) int { return 3 }
	moduleFromMinuteThree := func(minute int) int {
		if minute < 3 {
			return 3
		}
		return 4
	}
	liveReadyAtStart := []controllerlessObservation{
		{at: 0, value: string(graphql.SemanticStartupPhaseBootInit)},
		{at: 0, value: string(graphql.SemanticStartupPhaseLiveWarmup)},
		{at: 0, value: string(graphql.SemanticStartupPhaseLiveReady)},
	}
	noneFromFirstDiscovery := []controllerlessObservation{
		{at: 0, value: string(graphql.VaillantRegulatorCapabilityUnknown)},
		{at: 0, value: string(graphql.VaillantRegulatorCapabilityNone)},
	}

	for _, tc := range []struct {
		name           string
		heatSourceID   string
		heatSourcePart string
		boilerClass    bool
		// moduleSerial is used when the internet module is identified at two
		// minutes; "-" identifies it without a readable serial.
		moduleSerial     string
		wantPhases       []controllerlessObservation
		wantBootPhase    graphql.SemanticStartupPhase
		wantCapability   []controllerlessObservation
		wantRootByTarget map[byte]int
		wantRootPlan     map[time.Duration]int
	}{
		{
			name: "boiler-class heat source", heatSourceID: "BAI00", heatSourcePart: boilerPart, boilerClass: true,
			wantPhases: liveReadyAtStart, wantBootPhase: graphql.SemanticStartupPhaseLiveReady, wantCapability: noneFromFirstDiscovery,
			// 1 + 30 state + 6 config + 3 discovery passes, three candidates each.
			wantRootByTarget: map[byte]int{controllerlessRegulator: 40, controllerlessHeatSource: 40, controllerlessModuleSlot: 40},
			wantRootPlan:     controllerlessDiscoveryPlan(threeCandidates, nil),
		},
		{
			// The module joins the candidates; the inventory change at its
			// identification adds one discovery pass at two minutes.
			name: "boiler-class heat source and internet module", heatSourceID: "BAI00", heatSourcePart: boilerPart, boilerClass: true,
			moduleSerial: controllerlessSerialRaw(modulePart),
			wantPhases:   liveReadyAtStart, wantBootPhase: graphql.SemanticStartupPhaseLiveReady, wantCapability: noneFromFirstDiscovery,
			wantRootByTarget: map[byte]int{controllerlessRegulator: 41, controllerlessHeatSource: 41, controllerlessModuleSlot: 41, controllerlessInternetModule: 38},
			wantRootPlan:     controllerlessDiscoveryPlan(moduleFromMinuteThree, map[int]int{2: 4}),
		},
		{
			// An identified Vaillant device without a catalogued serial keeps
			// the public result UNKNOWN; the absence state still settles.
			name: "boiler-class heat source and internet module without serial", heatSourceID: "BAI00", heatSourcePart: boilerPart, boilerClass: true,
			moduleSerial: "-",
			wantPhases:   liveReadyAtStart, wantBootPhase: graphql.SemanticStartupPhaseLiveReady,
			wantCapability: append(append([]controllerlessObservation(nil), noneFromFirstDiscovery...),
				controllerlessObservation{at: 2 * time.Minute, value: string(graphql.VaillantRegulatorCapabilityUnknown)}),
			wantRootByTarget: map[byte]int{controllerlessRegulator: 41, controllerlessHeatSource: 41, controllerlessModuleSlot: 41, controllerlessInternetModule: 38},
			wantRootPlan:     controllerlessDiscoveryPlan(moduleFromMinuteThree, map[int]int{2: 4}),
		},
		{
			// The poller reads boiler status only from boiler-class identities;
			// a heat-pump-class heat source therefore publishes no live stream.
			name: "heat-pump-class heat source", heatSourceID: "EHP00", heatSourcePart: heatPumpPart,
			wantPhases: []controllerlessObservation{
				{at: 0, value: string(graphql.SemanticStartupPhaseBootInit)},
				{at: 2 * time.Minute, value: string(graphql.SemanticStartupPhaseDegraded)},
			},
			wantBootPhase:    graphql.SemanticStartupPhaseBootInit,
			wantCapability:   noneFromFirstDiscovery,
			wantRootByTarget: map[byte]int{controllerlessRegulator: 40, controllerlessHeatSource: 40, controllerlessModuleSlot: 40},
			wantRootPlan:     controllerlessDiscoveryPlan(threeCandidates, nil),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bus := newControllerlessBus()
			bus.addDevice(controllerlessHeatSource, controllerlessDevice{deviceID: tc.heatSourceID, serialRaw: controllerlessSerialRaw(tc.heatSourcePart), answersB509: tc.boilerClass})
			startup := runControllerlessStartupScan(t, bus, false)
			if startup.passes != 1 || !startup.activeProbePassed {
				t.Fatalf("startup passes = %d, active probe passed = %v; want 1, true", startup.passes, startup.activeProbePassed)
			}

			var script map[time.Duration]func(*vaillantSemanticPoller)
			if tc.moduleSerial != "" {
				serial := tc.moduleSerial
				if serial == "-" {
					serial = ""
				}
				script = map[time.Duration]func(*vaillantSemanticPoller){2 * time.Minute: func(p *vaillantSemanticPoller) {
					bus.addDevice(controllerlessInternetModule, controllerlessDevice{deviceID: "NETX3", serialRaw: serial})
					p.EnqueueAddressIdentityProbe(controllerlessInternetModule)
				}}
			}
			run := runControllerlessPolling(t, startup, bus, script)

			controllerlessExpectSeries(t, "startup phases", run.phases, tc.wantPhases)
			if run.phaseAtBootTimeout != tc.wantBootPhase {
				t.Errorf("phase when the boot-live timer fires = %s; want %s", run.phaseAtBootTimeout, tc.wantBootPhase)
			}
			controllerlessExpectSeries(t, "regulator capability", run.capability, tc.wantCapability)
			if got := bus.countByTarget(controllerlessRootProbe); !reflect.DeepEqual(got, tc.wantRootByTarget) {
				t.Errorf("B524 root probes by target over %s = %v; want %v", controllerlessHorizon, got, tc.wantRootByTarget)
			}
			if got := bus.probesByInstant(); !reflect.DeepEqual(got, tc.wantRootPlan) {
				t.Errorf("B524 root probes per instant = %v; want %v", got, tc.wantRootPlan)
			}
			wantCapability := tc.wantCapability[len(tc.wantCapability)-1].value
			view := controllerlessExpectCommon(t, bus, startup, run, wantCapability)

			if tc.boilerClass {
				controllerlessExpectBoilerStream(t, bus, view)
			} else {
				if got := view["boiler_status"]; got != nil {
					t.Errorf("GraphQL boiler_status = %#v; want null for a heat-pump-class heat source", got)
				}
				if got := bus.countByTarget(controllerlessB509Read); len(got) != 0 {
					t.Errorf("B509 register reads = %v; want none for a heat-pump-class heat source", got)
				}
			}

			if tc.moduleSerial != "" {
				wantSerialWindows := 4
				wantSerial := controllerlessSerial(modulePart)
				if tc.moduleSerial == "-" {
					wantSerialWindows, wantSerial = 1, ""
				}
				module, ok := controllerlessRegistryEntries(startup.gateway.Registry)[controllerlessInternetModule]
				if !ok || module.Manufacturer != "Vaillant" || module.DeviceID != "NETX3" || module.SerialNumber != wantSerial {
					t.Errorf("internet module entry = %+v (present %v); want Vaillant NETX3 with serial %q", module, ok, wantSerial)
				}
				if got := bus.countByTarget(controllerlessIdentify)[controllerlessInternetModule]; got != 1 {
					t.Errorf("internet module identification attempts = %d; want 1", got)
				}
				if got := bus.countByTarget(controllerlessSerialWindow)[controllerlessInternetModule]; got != wantSerialWindows {
					t.Errorf("internet module serial window reads = %d; want %d", got, wantSerialWindows)
				}
			}
		})
	}
}
