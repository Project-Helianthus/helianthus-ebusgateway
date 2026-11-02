package main

// M57 (issue #1348): offline acceptance of the composed drivers through the
// shared northbound path.
//
// Every test runs the real post-G5 lifecycle entrypoint (run, which is
// runGatewayLifecycle) with the production HTTP control plane, runtime control
// service, managed sidecar registry, semantic hub, shared snapshot provider and
// semantic operation route. The replaced parts are:
//   - ebus.primary dials an in-process net.Pipe (the G5 rctl fixture); its
//     poller, discovery and background scans are stubbed, so the Thermal value
//     is a synthetic batch published through its admitted generation's handle;
//   - growatt.bms.rs485 is the real managed driver and Storage publication over
//     the existing in-memory RTU register fake;
//   - modbus.sunspec and eebus.primary are injected managed factories; the
//     SunSpec one publishes synthetic PV batches through its own hub admission;
//   - the control-plane listener is in memory, the owner-only eeBUS operator
//     socket is an inert closer and the release checker is disabled;
//   - the semantic operation route receives a fake intent owner and executor
//     lookup; its capture, kernel and router are production.
//
// No socket is bound (the M2M and adapter-proxy listeners stay unconfigured),
// no device is opened and no bus traffic leaves the process. The evidence covers this bounded software chain only: it does not
// establish family admission, legacy removal, candidate, hardware or release
// readiness.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/releasecheck"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semanticoperations"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semanticoperations/semoptest"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semanticview"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	modbus "github.com/Project-Helianthus/helianthus-modbus"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/operation"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/pv"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/thermal"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/projection"
)

const (
	m57PVAsset      semreg.AssetID      = "asset:m57-pv-inverter"
	m57ThermalAsset semreg.AssetID      = "asset:m57-thermal-system"
	m57BaseURL                          = "http://gateway.m57.invalid"
	m57ModeField    semreg.DefinitionID = "thermal.mode.system"
	m57SetMode      semreg.DefinitionID = "thermal.operation.set_mode"
	m57PowerField   semreg.DefinitionID = "pv.ac.aggregate_active_power"
	m57TempField    semreg.DefinitionID = "pv.temperature.inverter"
)

var m57Drivers = []runtimecontrol.DriverID{managedEEBusID, primaryEBusDriverID, managedGrowattID, managedSunSpecID}

// --- in-memory transports -------------------------------------------------

// m57Listener is the control-plane listener the production server accepts on.
// Each client dial hands the server one end of a net.Pipe.
type m57Listener struct {
	conns    chan net.Conn
	done     chan struct{}
	once     sync.Once
	accepted atomic.Int32
}

func newM57Listener() *m57Listener {
	return &m57Listener{conns: make(chan net.Conn), done: make(chan struct{})}
}

func (l *m57Listener) Accept() (net.Conn, error) {
	select {
	case conn := <-l.conns:
		l.accepted.Add(1)
		return conn, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

func (l *m57Listener) Close() error {
	l.once.Do(func() { close(l.done) })
	return nil
}

func (l *m57Listener) Addr() net.Addr { return m57Addr{} }

func (l *m57Listener) closed() bool {
	select {
	case <-l.done:
		return true
	default:
		return false
	}
}

func (l *m57Listener) dial(ctx context.Context, _, _ string) (net.Conn, error) {
	client, server := net.Pipe()
	select {
	case l.conns <- server:
		return client, nil
	case <-l.done:
	case <-ctx.Done():
	}
	_, _ = client.Close(), server.Close()
	return nil, net.ErrClosed
}

type m57Addr struct{}

func (m57Addr) Network() string { return "memory" }
func (m57Addr) String() string  { return "m57-control-plane" }

// m57Closer stands in for the owner-only eeBUS operator socket.
type m57Closer struct{ starts, closes atomic.Int32 }

func (c *m57Closer) Close() error { c.closes.Add(1); return nil }

// m57GrowattEndpoint is the existing register fake with receipt instants on
// the process clock, so the production provider (which evaluates at the wall
// clock) sees current, monotonic native evidence across generations.
type m57GrowattEndpoint struct {
	*growattEndpointFake
	start time.Time
}

func (e *m57GrowattEndpoint) Read(ctx context.Context, unit byte, request modbus.ReadRegistersRequest) (modbus.ReadRegistersResponse, modbus.RTUReadEvidence, error) {
	response, evidence, err := e.growattEndpointFake.Read(ctx, unit, request)
	if err == nil {
		at := time.Now()
		evidence.ReceiptWall, evidence.ReceivedAt = at.UTC(), at.Sub(e.start)
	}
	return response, evidence, err
}

func (e *m57GrowattEndpoint) closes() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.closed
}

// --- synthetic publications -------------------------------------------------

func m57ID(kind string, parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return kind + ":m57-" + hex.EncodeToString(sum[:12])
}

func m57Evidence(label string) semreg.EvidenceRef {
	sum := sha256.Sum256([]byte("m57 synthetic evidence\x00" + label))
	return semreg.EvidenceRef{Owner: "helianthus.gateway.test", Kind: "m57.synthetic.observation", Digest: semreg.Digest("sha256:" + hex.EncodeToString(sum[:])),
		Contract: semreg.ContractKernelV1, Access: semreg.EvidenceAccessPublic, Redaction: semreg.RedactionNone}
}

func m57Symbol(namespace semreg.DefinitionID, token string) semreg.Value {
	return semreg.Value{Kind: semreg.ValueSymbol, Symbol: &semreg.Symbol{Namespace: namespace, Token: token, Known: true}}
}

// m57Quantity returns an integer quantity as a canonical SemReg decimal.
func m57Quantity(value uint64, unit semreg.DefinitionID) semreg.Value {
	exponent := int32(0)
	for value != 0 && value%10 == 0 {
		value, exponent = value/10, exponent+1
	}
	return semreg.Value{Kind: semreg.ValueQuantity, Quantity: &semreg.Quantity{Number: semreg.Decimal{Coefficient: strconv.FormatUint(value, 10), Exponent10: exponent}, Unit: unit}}
}

type m57Fact struct {
	field    semreg.DefinitionID
	value    semreg.Value
	withheld bool
}

type m57Capability struct {
	definition, service semreg.DefinitionID
	constraints         []semreg.TypedField
}

// m57Publication commits pack-defined facts through the hub transaction
// boundary every managed publisher uses. Binding, service and capability
// identities are minted per source epoch and generation, like the Growatt
// publication, so a successor generation never reuses a retired route.
type m57Publication struct {
	metadata     semreg.PackMetadata
	validator    semreg.PackValidator
	asset        semreg.AssetID
	protocol     string
	services     []semreg.DefinitionID
	capabilities []m57Capability
	clockEpoch   semreg.ClockEpochID
	start        time.Time
}

func (p *m57Publication) now() (semreg.TimePoint, semreg.MonotonicPoint) {
	at := time.Now()
	return semoptest.TimePoint(at), semreg.MonotonicPoint{ClockEpochID: p.clockEpoch, Nanoseconds: semreg.Uint64(strconv.FormatInt(at.Sub(p.start).Nanoseconds()+1, 10))}
}

func (p *m57Publication) ref(id semreg.DefinitionID) semreg.DefinitionRef {
	return semreg.DefinitionRef{Pack: p.metadata.Pack, ID: id, Version: p.metadata.Pack.Version}
}

func (p *m57Publication) dimension(field semreg.DefinitionID) semreg.DefinitionID {
	for _, f := range p.metadata.Fields {
		if f.Ref.ID == field {
			return f.Dimension.ID
		}
	}
	return ""
}

func (p *m57Publication) key(field semreg.DefinitionID) semreg.FactKey {
	dimension := p.dimension(field)
	text := "m57-" + string(dimension[strings.LastIndex(string(dimension), ".")+1:])
	return semreg.FactKey{PackID: p.metadata.Pack.ID, PackVersion: p.metadata.Pack.Version, FactID: field, Dimensions: []semreg.Dimension{{ID: dimension, Value: semreg.Value{Kind: semreg.ValueText, Text: &text}}}}
}

func (p *m57Publication) declare(hub *semhub.Hub, handle *semhub.Handle, driver runtimecontrol.DriverID) error {
	_, err := hub.Declare(semhub.AssetSpec{AssetID: p.asset, SourceID: handle.Source(), Driver: string(driver), Validators: []semreg.PackValidator{p.validator}})
	return err
}

func (p *m57Publication) binding(handle *semhub.Handle) semreg.NativeBindingID {
	return semreg.NativeBindingID(m57ID("binding", string(p.asset), string(handle.Epoch()), string(handle.GenerationID())))
}

func (p *m57Publication) capabilityID(handle *semhub.Handle, definition semreg.DefinitionID) semreg.CapabilityInstanceID {
	return semreg.CapabilityInstanceID(m57ID("capability", string(p.binding(handle)), string(definition)))
}

func (p *m57Publication) serviceID(handle *semhub.Handle, definition semreg.DefinitionID) semreg.ServiceInstanceID {
	return semreg.ServiceInstanceID(m57ID("service", string(p.binding(handle)), string(definition)))
}

func (p *m57Publication) publish(hub *semhub.Hub, handle *semhub.Handle, facts []m57Fact) error {
	if hub == nil || handle == nil {
		return errors.New("m57: publication without hub or handle")
	}
	tx, err := hub.Begin(handle, p.asset)
	if err != nil {
		return err
	}
	defer tx.Abort()
	// An unpublished asset has the zero snapshot as its current publication.
	current, exists := tx.Current()
	sequence := tx.NextSequence()
	wall, mono := p.now()
	source, epoch, generation, binding := handle.Source(), handle.Epoch(), handle.GenerationID(), p.binding(handle)
	evidence := m57Evidence(string(p.asset) + "\x00" + string(binding))
	expected := semreg.Uint64("0")
	if exists {
		expected = current.Revisions.Semantic
	}
	batch := semreg.PublicationBatch{Contract: semreg.ContractKernelV1, BatchID: semreg.BatchID(m57ID("batch", string(p.asset), string(sequence))), AssetID: p.asset, SourceID: source, SourceEpochID: epoch, DriverGeneration: generation, Sequence: sequence, ExpectedSemanticRevision: expected, ObservedAt: wall,
		SourceUpserts: []semreg.SourceDescriptor{}, SourceRetirements: tx.SourceRetirements(), BindingUpserts: []semreg.NativeBinding{}, IdentityLinkUpserts: []semreg.IdentityLink{},
		FactUpserts: []semreg.FactCandidate{}, FactWithdrawals: []semreg.CandidateID{}, ServiceUpserts: []semreg.ServiceInstance{}, ServiceWithdrawals: []semreg.ServiceInstanceID{},
		CapabilityUpserts: []semreg.CapabilityInstance{}, CapabilityWithdrawals: []semreg.CapabilityInstanceID{}, GenerationFences: []semreg.GenerationFence{}}
	// The first publication of an epoch declares its source; the first of a
	// generation declares its binding, services and capabilities. Later ones
	// only revise facts.
	sourceKnown := false
	for _, known := range current.Sources {
		sourceKnown = sourceKnown || known.SourceID == source && known.SourceEpochID == epoch
	}
	if !sourceKnown {
		batch.SourceUpserts = append(batch.SourceUpserts, semreg.SourceDescriptor{SourceID: source, SourceEpochID: epoch, ProtocolID: semreg.DefinitionID(p.protocol), ProfileID: "m57.synthetic.profile", ProfileVersion: "1", RegistryEvidence: evidence, StartedAt: wall, State: semreg.SourceCurrent, Revision: "1"})
	}
	bindingKnown := false
	for _, known := range current.Bindings {
		bindingKnown = bindingKnown || known.BindingID == binding
	}
	if !bindingKnown {
		batch.BindingUpserts = append(batch.BindingUpserts, semreg.NativeBinding{BindingID: binding, AssetID: p.asset, SourceID: source, SourceEpochID: epoch, DriverGeneration: generation, NativeResource: evidence, State: semreg.BindingCurrent, Revision: "1"})
		batch.IdentityLinkUpserts = append(batch.IdentityLinkUpserts, semreg.IdentityLink{AssetID: p.asset, BindingID: binding, State: semreg.LinkQualified, Basis: []semreg.EvidenceRef{evidence}, Revision: "1"})
		for _, service := range p.services {
			batch.ServiceUpserts = append(batch.ServiceUpserts, semreg.ServiceInstance{InstanceID: p.serviceID(handle, service), AssetID: p.asset, Definition: p.ref(service), BindingID: binding, SourceEpochID: epoch, DriverGeneration: generation,
				Qualification: semreg.QualificationQualified, Availability: semreg.AvailabilityAvailable, Revision: "1"})
		}
		for _, capability := range p.capabilities {
			batch.CapabilityUpserts = append(batch.CapabilityUpserts, semreg.CapabilityInstance{InstanceID: p.capabilityID(handle, capability.definition), AssetID: p.asset, ServiceInstance: p.serviceID(handle, capability.service), Definition: p.ref(capability.definition),
				BindingID: binding, SourceEpochID: epoch, DriverGeneration: generation, Qualification: semreg.QualificationQualified, Availability: semreg.AvailabilityAvailable,
				Constraints: append([]semreg.TypedField{}, capability.constraints...), ActivationEvidence: []semreg.EvidenceRef{evidence}, Revision: "1"})
		}
	}
	for _, fact := range facts {
		quality := semreg.Quality{Assertion: semreg.AssertionObserved, Qualification: semreg.QualificationQualified, Promotion: semreg.PromotionPromoted, Validity: semreg.ValidityGood, Availability: semreg.AvailabilityAvailable, Freshness: semreg.FreshnessFresh, Reasons: []semreg.DefinitionID{}}
		if fact.withheld {
			// The native owner keeps its last value but marks it unavailable:
			// the publication retains it as evidence and every read withholds it.
			quality.Promotion, quality.Availability = semreg.PromotionUnpromoted, semreg.AvailabilityUnavailable
		}
		candidateID := semreg.CandidateID(m57ID("candidate", string(binding), string(fact.field)))
		revision := uint64(1)
		for _, envelope := range current.Facts {
			for _, candidate := range envelope.Candidates {
				if candidate.CandidateID == candidateID {
					n, err := strconv.ParseUint(string(candidate.Revision), 10, 64)
					if err != nil {
						return err
					}
					revision = n + 1
				}
			}
		}
		value := fact.value
		bindingID, epochID, generationID, sourceID := binding, epoch, generation, source
		batch.FactUpserts = append(batch.FactUpserts, semreg.FactCandidate{CandidateID: candidateID, Key: p.key(fact.field), Value: &value, Quality: quality,
			Times:           semreg.Times{ReceivedAt: wall, ReceiptMonotonic: mono, EvaluatedAt: wall, EvaluateMonotonic: mono},
			FreshnessPolicy: semreg.FreshnessPolicy{PolicyID: "policy:m57-synthetic", Version: "1.0.0", FreshForNS: semreg.Uint64(strconv.FormatInt(int64(time.Hour), 10)), RetainForNS: semreg.Uint64(strconv.FormatInt(int64(2*time.Hour), 10)), MaxWallUncertaintyNS: "0"},
			BindingID:       &bindingID, SourceEpochID: &epochID, DriverGeneration: &generationID,
			Origin:   semreg.OriginRef{OriginID: semreg.OriginID(m57ID("origin", string(binding), string(fact.field))), Kind: semreg.OriginNativeObservation, SourceID: &sourceID, SourceEpochID: &epochID, BindingID: &bindingID, Evidence: []semreg.EvidenceRef{evidence}},
			Evidence: []semreg.EvidenceRef{evidence}, Revision: semreg.Uint64(strconv.FormatUint(revision, 10))})
	}
	sort.Slice(batch.FactUpserts, func(i, j int) bool { return batch.FactUpserts[i].CandidateID < batch.FactUpserts[j].CandidateID })
	sort.Slice(batch.ServiceUpserts, func(i, j int) bool { return batch.ServiceUpserts[i].InstanceID < batch.ServiceUpserts[j].InstanceID })
	sort.Slice(batch.CapabilityUpserts, func(i, j int) bool {
		return batch.CapabilityUpserts[i].InstanceID < batch.CapabilityUpserts[j].InstanceID
	})
	if batch.BatchDigest, err = batch.ComputedDigest(); err != nil {
		return err
	}
	if _, _, err := tx.Apply(batch, mono); err != nil {
		return err
	}
	outcome, err := tx.Commit(nil)
	if err == nil && outcome != semhub.Accepted {
		err = fmt.Errorf("m57: publication outcome %v", outcome)
	}
	return err
}

func (p *m57Publication) fence(hub *semhub.Hub, handle *semhub.Handle) error {
	wall, mono := p.now()
	_, err := hub.Fence(semhub.FenceRequest{Source: handle.Source(), Epoch: handle.Epoch(), Generation: handle.Generation(), Reason: "lifecycle.generation_replaced",
		Evidence: []semreg.EvidenceRef{m57Evidence("fence\x00" + string(handle.Epoch()) + "\x00" + string(handle.GenerationID()))}, ObservedAt: wall, Monotonic: mono})
	return err
}

func m57PVPublication() *m57Publication {
	return &m57Publication{metadata: pv.Metadata(), validator: pv.New(), asset: m57PVAsset, protocol: "modbus_tcp", clockEpoch: "clock:m57-pv", start: time.Now(),
		services:     []semreg.DefinitionID{"pv.service.inverter"},
		capabilities: []m57Capability{{definition: "pv.capability.read.inverter", service: "pv.service.inverter"}}}
}

// m57PVFacts is one already-defined PV value per generation and an explicit
// withheld value. A late publication carries a sentinel that must never appear.
func m57PVFacts(generation uint64, late bool) []m57Fact {
	power := m57Quantity(1500+100*(generation-1), "unit.watt")
	if late {
		power = m57Quantity(9999, "unit.watt")
	}
	return []m57Fact{{field: m57PowerField, value: power}, {field: m57TempField, value: m57Quantity(41, "unit.celsius"), withheld: true}}
}

func m57ThermalPublication() *m57Publication {
	return &m57Publication{metadata: thermal.Metadata(), validator: thermal.New(), asset: m57ThermalAsset, protocol: "ebus.native", clockEpoch: "clock:m57-thermal", start: time.Now(),
		services:     []semreg.DefinitionID{"thermal.service.system"},
		capabilities: []m57Capability{{definition: "thermal.capability.set_mode", service: "thermal.service.system", constraints: []semreg.TypedField{{ID: m57ModeField, Value: m57Symbol(m57ModeField, "heat")}}}}}
}

func m57Mode(token string) []m57Fact {
	return []m57Fact{{field: m57ModeField, value: m57Symbol(m57ModeField, token)}}
}

// --- injected managed sidecar factory ----------------------------------------

// m57Sidecar is a managed factory runtime. With a publication it admits its
// registered source on the process hub before it reports a generation,
// publishes once per generation and, as the family owner, fences its hub
// generation when the composition stops it; its per-generation worker races
// withdrawal with one late publication that must be refused.
type m57Sidecar struct {
	id           runtimecontrol.DriverID
	publication  *m57Publication
	constructErr error
	// leaseErr refuses the shell lease after a successful native start, so the
	// composition itself must stop the generation it started.
	leaseErr error
	hub      atomic.Pointer[semhub.Hub]

	mu     sync.Mutex
	source semreg.SourceID
	handle *semhub.Handle
	cancel context.CancelFunc
	tasks  sync.WaitGroup
	keys   []runtimecontrol.LifecycleCorrelation

	lateMu sync.Mutex
	late   []error

	constructs, starts, stops, workers atomic.Int32
	// withdrawals counts the composition's FenceWithdrawal calls; unwithdrawn
	// counts Stop calls of a running generation that no withdrawal preceded.
	withdrawals, unwithdrawn atomic.Int32
	running                  atomic.Bool
	generation               atomic.Uint64
}

func (s *m57Sidecar) adapter() runtimecontrol.LifecycleAdapter {
	return runtimecontrol.NewAdapter(func(_ context.Context, c runtimecontrol.Construction) (*m57Sidecar, error) {
		s.constructs.Add(1)
		if s.constructErr != nil {
			return nil, s.constructErr
		}
		if c.DriverID != s.id || len(c.Sources) != 1 {
			return nil, errors.New("m57: construction lost the frozen descriptor identity")
		}
		s.mu.Lock()
		s.source = semreg.SourceID(c.Sources[0])
		s.mu.Unlock()
		return s, nil
	})
}

func (s *m57Sidecar) BindLifecycle(c runtimecontrol.LifecycleCorrelation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.keys = append(s.keys, c)
	return nil
}

func (s *m57Sidecar) Start(context.Context) (uint64, error) {
	s.starts.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	generation := s.generation.Load() + 1
	var handle *semhub.Handle
	if s.publication != nil {
		hub := s.hub.Load()
		if hub == nil {
			return 0, errors.New("m57: process hub not composed")
		}
		var err error
		if s.handle == nil {
			if handle, err = hub.AdmitSource(semhub.Admission{Source: s.source, Driver: string(s.id)}); err == nil {
				err = s.publication.declare(hub, handle, s.id)
			}
		} else {
			handle, err = hub.Activate(s.handle, s.handle.Generation()+1)
		}
		if err == nil {
			err = s.publication.publish(hub, handle, m57PVFacts(generation, false))
		}
		if err != nil {
			return 0, err
		}
		s.handle = handle
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.tasks.Add(1)
	s.workers.Add(1)
	go s.work(ctx, handle, generation)
	s.running.Store(true)
	s.generation.Store(generation)
	return generation, nil
}

func (s *m57Sidecar) work(ctx context.Context, handle *semhub.Handle, generation uint64) {
	defer s.tasks.Done()
	defer s.workers.Add(-1)
	<-ctx.Done()
	if handle == nil {
		return
	}
	// A refresh racing withdrawal: it runs only after Stop fenced this
	// generation, so the hub must refuse it.
	err := s.publication.publish(s.hub.Load(), handle, m57PVFacts(generation, true))
	s.lateMu.Lock()
	s.late = append(s.late, err)
	s.lateMu.Unlock()
}

// FenceWithdrawal is the composition's withdrawal signal; it must precede the
// native stop of every running generation.
func (s *m57Sidecar) FenceWithdrawal() { s.withdrawals.Add(1) }

func (s *m57Sidecar) prepareManagedLease(context.Context, uint64, *managedSidecarLease) error {
	return s.leaseErr
}

func (s *m57Sidecar) Stop(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.running.Load() {
		return nil
	}
	if s.withdrawals.Load() <= s.stops.Load() {
		s.unwithdrawn.Add(1)
	}
	if s.handle != nil {
		if err := s.publication.fence(s.hub.Load(), s.handle); err != nil {
			return err
		}
	}
	s.cancel()
	joined := make(chan struct{})
	go func() { s.tasks.Wait(); close(joined) }()
	select {
	case <-joined:
	case <-ctx.Done():
		return ctx.Err()
	}
	s.running.Store(false)
	s.stops.Add(1)
	return nil
}

func (s *m57Sidecar) Replace(ctx context.Context) (uint64, error) {
	if err := s.Stop(ctx); err != nil {
		return 0, err
	}
	return s.Start(ctx)
}

func (s *m57Sidecar) Generation() uint64    { return s.generation.Load() }
func (s *m57Sidecar) Revision() uint64      { return s.generation.Load() }
func (*m57Sidecar) SafetyQuarantined() bool { return false }

func (s *m57Sidecar) lateResults() []error {
	s.lateMu.Lock()
	defer s.lateMu.Unlock()
	return append([]error(nil), s.late...)
}

func (s *m57Sidecar) currentHandle() *semhub.Handle {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.handle
}

// m57CurrentSource returns the one current source descriptor of a
// publication; retired epochs remain beside it as history.
func m57CurrentSource(snapshot semreg.Snapshot) (semreg.SourceDescriptor, bool) {
	var found []semreg.SourceDescriptor
	for _, source := range snapshot.Sources {
		if source.State == semreg.SourceCurrent {
			found = append(found, source)
		}
	}
	if len(found) != 1 {
		return semreg.SourceDescriptor{}, false
	}
	return found[0], true
}

// --- fake downward route ------------------------------------------------------

// m57Owner is a fake runtime intent owner for the Thermal set_mode operation:
// it completes only the members a request does not carry.
type m57Owner struct{ calls atomic.Int32 }

func (o *m57Owner) CompleteIntent(_ context.Context, draft operation.Intent, budget time.Time) (operation.Intent, operation.AuthorityResolver, error) {
	n := strconv.Itoa(int(o.calls.Add(1)))
	pack := thermal.Metadata().Pack
	intent := draft
	intent.IntentID = semreg.IntentID("intent:m57-" + n)
	intent.IdempotencyKey = semreg.IdempotencyKey("idempotency:m57-" + n)
	intent.RequiredCapability = operation.CapabilityRequirement{Pack: pack, DefinitionID: "thermal.capability.set_mode", Versions: semreg.VersionRange{Minimum: "1.0.0", MaximumExclusive: "2.0.0"}, InstanceID: draft.RequiredCapability.InstanceID}
	expected := semreg.Value{}
	if len(draft.Arguments) == 1 {
		expected = draft.Arguments[0].Value
	}
	intent.ExpectedEffect = operation.ExpectedEffect{Rule: semreg.DefinitionRef{Pack: pack, ID: "thermal.effect.set_mode", Version: pack.Version}, Fact: m57ThermalPublication().key(m57ModeField), Operator: semreg.PredicateEqual, Expected: expected}
	intent.Authority = m57Evidence("authority")
	now := time.Now()
	intent.Causal = semreg.CausalContext{Origin: semreg.OriginRef{OriginID: "origin:m57-operator", Kind: semreg.OriginOperator, Evidence: []semreg.EvidenceRef{m57Evidence("operator")}},
		CorrelationID: semreg.CorrelationID("correlation:m57-" + n), MaxHops: 8, FirstSeenAt: semoptest.TimePoint(now), ExpiresAt: semoptest.TimePoint(now.Add(2 * time.Minute)), Path: []semreg.TargetID{}}
	intent.Deadline = semoptest.TimePoint(budget)
	return intent, operation.AuthorityResolverFunc(func(operation.Intent, operation.Route, semreg.EvaluationContext) error { return nil }), nil
}

// m57Executor is the injected executor of one admitted Thermal route. After
// release it has the native owner republish the requested mode through the
// currently admitted generation and reads it back through the same shared
// capture the route used, claiming confirmation.
type m57Executor struct {
	snapshots semanticoperations.SnapshotCapture
	republish func() error
	hold      bool
	// kernel, when set, is the production route kernel: the executor submits
	// its record to the admission once itself, keeping the kernel's verdict.
	kernel      *operation.Kernel
	verdict     error
	dispatched  chan semanticoperations.Dispatch
	release     chan struct{}
	releaseOnce sync.Once
	done        chan struct{}
	calls       atomic.Int32
}

// unhold lets a holding executor complete.
func (e *m57Executor) unhold() { e.releaseOnce.Do(func() { close(e.release) }) }

func newM57Executor(snapshots semanticoperations.SnapshotCapture, republish func() error, hold bool) *m57Executor {
	return &m57Executor{snapshots: snapshots, republish: republish, hold: hold, dispatched: make(chan semanticoperations.Dispatch, 1), release: make(chan struct{}), done: make(chan struct{})}
}

func (e *m57Executor) Execute(_ context.Context, dispatch semanticoperations.Dispatch) (semanticoperations.Completion, error) {
	e.calls.Add(1)
	defer close(e.done)
	e.dispatched <- dispatch
	if e.hold {
		// The native owner completes when it completes: a late completion
		// does not stop at the router's bounded wait.
		<-e.release
	}
	// The simulated native write completes at the admitted instant; the
	// owner's observation of its effect must be strictly later.
	admitted, err := strconv.ParseInt(string(dispatch.AdmittedAt.UnixNanoseconds), 10, 64)
	if err != nil {
		return semanticoperations.Completion{}, err
	}
	for time.Now().UnixNano() <= admitted {
		time.Sleep(time.Microsecond)
	}
	if err := e.republish(); err != nil {
		return semanticoperations.Completion{}, err
	}
	_, inputs, err := e.snapshots.Capture(context.Background(), []semreg.AssetID{dispatch.Intent.AssetID})
	if err != nil || len(inputs) != 1 || inputs[0].Snapshot == nil || inputs[0].Evaluation == nil {
		return semanticoperations.Completion{}, fmt.Errorf("m57: readback capture: %v", err)
	}
	// Read back the candidate the owner just published through its current
	// binding; a retired generation's candidate may remain beside it.
	snapshot := *inputs[0].Snapshot
	current := map[semreg.NativeBindingID]bool{}
	for _, binding := range snapshot.Bindings {
		current[binding.BindingID] = binding.State == semreg.BindingCurrent
	}
	var candidate *semreg.FactCandidate
	for _, envelope := range snapshot.Facts {
		for i := range envelope.Candidates {
			if c := &envelope.Candidates[i]; reflect.DeepEqual(envelope.Key, dispatch.Intent.ExpectedEffect.Fact) && c.BindingID != nil && current[*c.BindingID] {
				candidate = c
			}
		}
	}
	source, sourceOK := m57CurrentSource(snapshot)
	if candidate == nil || candidate.BindingID == nil || candidate.SourceEpochID == nil || candidate.DriverGeneration == nil || !sourceOK {
		return semanticoperations.Completion{}, errors.New("m57: readback candidate missing")
	}
	completed := semreg.EvaluationContext{EvaluatedAt: dispatch.AdmittedAt, EvaluateMonotonic: semreg.MonotonicPoint{ClockEpochID: "clock:m57-executor", Nanoseconds: "1"}}
	record := dispatch.ExecutionRecord()
	record.Dispatch = &operation.DispatchEvidence{AttemptID: "attempt:m57-1", Started: completed, Completed: &completed, Delivery: operation.DeliverySent, PossibleSideEffect: true, Evidence: []semreg.EvidenceRef{m57Evidence("dispatch")}}
	record.Acknowledgement = &operation.Acknowledgement{State: operation.AckAccepted, At: dispatch.AdmittedAt, Evidence: []semreg.EvidenceRef{m57Evidence("ack")}}
	record.Readback = &operation.Readback{SnapshotID: snapshot.SnapshotID, Revisions: snapshot.Revisions, CandidateID: candidate.CandidateID, CandidateRevision: candidate.Revision,
		BindingID: *candidate.BindingID, SourceID: source.SourceID, SourceEpochID: *candidate.SourceEpochID, DriverGeneration: *candidate.DriverGeneration,
		Relation: operation.ReadbackConfirms, Evaluation: inputs[0].Evaluation.Context, Evidence: []semreg.EvidenceRef{m57Evidence("readback")}}
	record.Outcome = operation.OutcomeApplied
	record.OutcomeEvidence = []semreg.EvidenceRef{m57Evidence("outcome")}
	if e.kernel != nil {
		// Identical intent bytes resolve to the router's admission; a refused
		// record leaves it unchanged for the router's own submission. Any other
		// admission result is reported without its SemReg identifier.
		admission, err := e.kernel.Admit(snapshot, inputs[0].Evaluation.Context, dispatch.Intent, operation.AuthorityResolverFunc(func(operation.Intent, operation.Route, semreg.EvaluationContext) error { return nil }))
		switch at, _, _ := admission.AdmittedAt(); {
		case err != nil:
			e.verdict = fmt.Errorf("m57: admission lookup failed: %v", err)
		case at != dispatch.AdmittedAt:
			e.verdict = errors.New("m57: admission lookup returned another admission")
		default:
			_, e.verdict = e.kernel.Record(admission, record, &snapshot)
		}
	}
	return semanticoperations.Completion{Record: &record, ReadbackSnapshot: &snapshot}, nil
}

// m57Lookup holds the executor injected for each exact admitted Thermal
// set_mode route; the latest injection for a route wins.
type m57Lookup struct {
	mu        sync.Mutex
	executors map[operation.Route]semanticoperations.Executor
	calls     atomic.Int32
}

func (l *m57Lookup) inject(route operation.Route, executor semanticoperations.Executor) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.executors == nil {
		l.executors = map[operation.Route]semanticoperations.Executor{}
	}
	l.executors[route] = executor
}

func (l *m57Lookup) Lookup(route operation.Route, kind semreg.DefinitionRef) (semanticoperations.Executor, bool) {
	l.calls.Add(1)
	l.mu.Lock()
	defer l.mu.Unlock()
	executor, ok := l.executors[route]
	return executor, ok && kind == m57ThermalPublication().ref(m57SetMode)
}

// --- process harness ------------------------------------------------------------

type m57Options struct {
	sunspecLeaseErr   error
	eebusConstructErr error
}

// m57Process is one runGatewayLifecycle incarnation behind the production
// HTTP control plane.
type m57Process struct {
	t        *testing.T
	env      *rctlEnv
	cfg      ebusgateway.Config
	cancel   context.CancelFunc
	done     chan error
	control  *gatewayRuntimeControl
	listener *m57Listener
	client   *http.Client
	operator *m57Closer
	sunspec  *m57Sidecar
	eebus    *m57Sidecar
	owner    *m57Owner
	lookup   *m57Lookup
	thermal  *m57Publication

	mu      sync.Mutex
	listens []string
	growatt []*m57GrowattEndpoint
	route   []semanticoperations.Config
}

// m57SaveSeams returns the restore function of every package seam m57Start
// replaces.
func m57SaveSeams() func() {
	release, polling, discovery, background, httpServer := startGatewayReleaseChecker, startVaillantSemanticPollingFn, startDiscoveryScanLoopFn, startBackgroundFullScanFn, startHTTPServerFn
	options, started, descriptors, growatt := gatewayRuntimeControlOptionsFor, gatewayRuntimeControlStarted, gatewayManagedSidecarDescriptorsFor, openGrowattBMSRTUEndpoint
	listen, operator, route := listenHTTPControlPlane, startEEBusOperatorEndpoint, gatewaySemanticOperationConfigFor
	return func() {
		startGatewayReleaseChecker, startVaillantSemanticPollingFn, startDiscoveryScanLoopFn, startBackgroundFullScanFn, startHTTPServerFn = release, polling, discovery, background, httpServer
		gatewayRuntimeControlOptionsFor, gatewayRuntimeControlStarted, gatewayManagedSidecarDescriptorsFor, openGrowattBMSRTUEndpoint = options, started, descriptors, growatt
		listenHTTPControlPlane, startEEBusOperatorEndpoint, gatewaySemanticOperationConfigFor = listen, operator, route
	}
}

// m57Start runs one process with fake endpoints and factories and returns once
// runtime control is composed and the production control plane is listening.
func m57Start(t *testing.T, opts m57Options) *m57Process {
	t.Helper()
	env := newRCtlEnv(t)
	p := &m57Process{t: t, env: env, done: make(chan error, 1), listener: newM57Listener(), operator: &m57Closer{}, owner: &m57Owner{}, lookup: &m57Lookup{}, thermal: m57ThermalPublication(),
		sunspec: &m57Sidecar{id: managedSunSpecID, publication: m57PVPublication(), leaseErr: opts.sunspecLeaseErr},
		eebus:   &m57Sidecar{id: managedEEBusID, constructErr: opts.eebusConstructErr}}
	p.client = &http.Client{Timeout: rctlBound, Transport: &http.Transport{DialContext: p.listener.dial, DisableKeepAlives: true}}

	t.Cleanup(m57SaveSeams())

	startGatewayReleaseChecker = func(*releasecheck.Checker, context.Context) {}
	// No eBUS poller, scan or probe traffic: the only eBUS endpoint activity is
	// the managed connection over the in-process pipe.
	startVaillantSemanticPollingFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.LiveSemanticProvider, *graphql.BroadcastHub, <-chan struct{}) *vaillantSemanticPoller {
		env.event("semantic_poller")
		return nil
	}
	startDiscoveryScanLoopFn = func(ctx context.Context, _ ebusgateway.Config, _ *ebusgateway.Gateway, _ *graphql.Builder, _ activeTxnClassifier) startupScanSignals {
		n := env.discovery.Add(1)
		env.event("discovery %d", n)
		passed, first, loopDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
		close(passed)
		close(first)
		go func() {
			<-ctx.Done()
			env.event("discovery_exit %d", n)
			close(loopDone)
		}()
		return startupScanSignals{firstPassDone: first, activeProbePassed: passed, done: loopDone}
	}
	startBackgroundFullScanFn = func(ctx context.Context, _ ebusgateway.Config, _ *ebusgateway.Gateway, _ *graphql.Builder, _ <-chan struct{}) {
		env.background.Add(1)
		<-ctx.Done()
		env.background.Add(-1)
	}
	startHTTPServerFn = nil
	gatewayRuntimeControlOptionsFor = func(o gatewayRuntimeControlOptions) gatewayRuntimeControlOptions {
		p.sunspec.hub.Store(o.Hub)
		p.eebus.hub.Store(o.Hub)
		return o
	}
	controlReady := make(chan *gatewayRuntimeControl, 1)
	gatewayRuntimeControlStarted = func(control *gatewayRuntimeControl) { controlReady <- control }
	// Replace only the inert SunSpec and eeBUS adapters; Growatt keeps its
	// production factory. Dispositions stay those of the configuration.
	gatewayManagedSidecarDescriptorsFor = func(descriptors []runtimecontrol.Descriptor) []runtimecontrol.Descriptor {
		for i := range descriptors {
			switch descriptors[i].DriverID {
			case managedSunSpecID:
				descriptors[i].Adapter = p.sunspec.adapter()
			case managedEEBusID:
				descriptors[i].Adapter = p.eebus.adapter()
			}
		}
		return descriptors
	}
	growattClock := time.Now()
	openGrowattBMSRTUEndpoint = func(modbus.RTUProductionConfig) (growattBMSRTUEndpoint, error) {
		p.mu.Lock()
		defer p.mu.Unlock()
		endpoint := &m57GrowattEndpoint{growattEndpointFake: &growattEndpointFake{words: growattBMSProductionWords(), failAt: -1, mismatch: -1, generation: uint64(len(p.growatt) + 1)}, start: growattClock}
		p.growatt = append(p.growatt, endpoint)
		return endpoint, nil
	}
	listened := make(chan struct{}, 1)
	listenHTTPControlPlane = func(network, address string) (net.Listener, error) {
		p.mu.Lock()
		p.listens = append(p.listens, network+" "+address)
		p.mu.Unlock()
		listened <- struct{}{}
		return p.listener, nil
	}
	startEEBusOperatorEndpoint = func(*mcp.Server, context.Context, string) (io.Closer, error) {
		p.operator.starts.Add(1)
		return p.operator, nil
	}
	// The production route configuration is recorded unchanged; the fake intent
	// owner and executor lookup are injected for the downward route.
	gatewaySemanticOperationConfigFor = func(config semanticoperations.Config) semanticoperations.Config {
		p.mu.Lock()
		p.route = append(p.route, config)
		p.mu.Unlock()
		config.Intents, config.Executors = p.owner, p.lookup
		return config
	}

	cfg := env.config()
	cfg.HTTPAddr = "127.0.0.1:0"
	cfg.MDNSAdvertise = false
	cfg.PortalConfigRoot = t.TempDir()
	cfg.ModbusTCPConfig = ebusgateway.ModbusTCPConfig{Enabled: true, Endpoint: "tcp://fixture.invalid:502", DialTimeout: time.Second, GrowattBMSRS485: growattProductionConfig()}
	cfg.EEBusConfig.Enabled = true
	cfg.EEBusConfig.StateRoot = t.TempDir()
	if !cfg.M2MGraphQL.Disabled() || cfg.ProxyListenAddr != "" {
		t.Fatal("fixture configuration would open a listener outside the control-plane seam")
	}
	p.cfg = cfg

	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	go func() { p.done <- run(ctx, cfg) }()
	t.Cleanup(p.stop)
	// Runtime control is composed before the HTTP shell starts serving.
	select {
	case p.control = <-controlReady:
	case err := <-p.done:
		t.Fatalf("run returned before runtime control composition: %v", err)
	case <-time.After(rctlBound):
		t.Fatal("runtime control was not composed")
	}
	select {
	case <-listened:
	case err := <-p.done:
		t.Fatalf("run returned before the control plane served: %v", err)
	case <-time.After(rctlBound):
		t.Fatal("control plane did not start")
	}
	return p
}

func (p *m57Process) stop() {
	if p.cancel == nil {
		return
	}
	p.cancel()
	p.cancel = nil
	select {
	case err := <-p.done:
		if err != nil {
			p.t.Errorf("run returned %v on process shutdown; want nil", err)
		}
	case <-time.After(3 * rctlBound):
		p.t.Error("run did not return after process shutdown")
	}
}

func (p *m57Process) send(method, path string, body []byte) (int, []byte, error) {
	request, err := http.NewRequest(method, m57BaseURL+path, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := p.client.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = response.Body.Close() }()
	raw, err := io.ReadAll(response.Body)
	return response.StatusCode, raw, err
}

func (p *m57Process) request(method, path string, body []byte) []byte {
	p.t.Helper()
	status, raw, err := p.send(method, path, body)
	if err != nil || status != http.StatusOK {
		p.t.Fatalf("%s %s: status=%d err=%v body=%s", method, path, status, err, raw)
	}
	return raw
}

func (p *m57Process) tool(name, args string) rctlToolResult {
	p.t.Helper()
	result, rpcErr := rctlDecodeRPC(p.t, p.request(http.MethodPost, p.cfg.MCPPath, []byte(rctlRPCBody("tools/call", rctlCallParams(name, args)))))
	if rpcErr != nil {
		p.t.Fatalf("%s: JSON-RPC error %+v", name, rpcErr)
	}
	return rctlDecodeTool(p.t, result)
}

// drivers reads the ordinary MCP lifecycle list.
func (p *m57Process) drivers() map[runtimecontrol.DriverID]rctlWireDriver {
	p.t.Helper()
	result := p.tool(mcp.RuntimeControlListTool, `{}`)
	var list rctlWireList
	if result.isError || json.Unmarshal(result.data, &list) != nil {
		p.t.Fatalf("lifecycle list unavailable: %+v", result)
	}
	drivers := map[runtimecontrol.DriverID]rctlWireDriver{}
	for _, driver := range list.Drivers {
		if _, duplicate := drivers[runtimecontrol.DriverID(driver.DriverID)]; duplicate {
			p.t.Fatalf("%s listed twice", driver.DriverID)
		}
		drivers[runtimecontrol.DriverID(driver.DriverID)] = driver
	}
	return drivers
}

// waitObserved polls the ordinary MCP lifecycle list until every named driver
// reports its wanted observed state.
func (p *m57Process) waitObserved(want map[runtimecontrol.DriverID]drivermanager.ObservedState) map[runtimecontrol.DriverID]rctlWireDriver {
	p.t.Helper()
	deadline := time.Now().Add(rctlBound)
	for {
		drivers := p.drivers()
		ready := len(drivers) == len(m57Drivers)
		for id, state := range want {
			ready = ready && drivers[id].ObservedState == string(state)
		}
		if ready {
			return drivers
		}
		if time.Now().After(deadline) {
			p.t.Fatalf("lifecycle list %+v; want %v", drivers, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// decide submits one ordinary MCP lifecycle mutation at the read-back desired
// revision, retrying only retryable refusals within the bound.
func (p *m57Process) decide(tool string, id runtimecontrol.DriverID) {
	p.t.Helper()
	deadline := time.Now().Add(rctlBound)
	for {
		result := p.tool(tool, rctlMutationArgs(id, p.drivers()[id].DesiredRevision))
		if !result.isError {
			return
		}
		if !result.retriable || time.Now().After(deadline) {
			p.t.Fatalf("%s %s: %s %s", tool, id, result.code, result.message)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// read captures the same assets through GraphQL semanticSnapshot and MCP
// semantic.v1.snapshot.get and requires identical identity, revision, value
// and loss. It returns the MCP view.
func (p *m57Process) read(ids ...semreg.AssetID) semanticview.View {
	p.t.Helper()
	quoted := make([]string, len(ids))
	for i, id := range ids {
		quoted[i] = strconv.Quote(string(id))
	}
	query, err := json.Marshal(map[string]string{"query": `{ semanticSnapshot(assetIds:[` + strings.Join(quoted, ",") + `])` + m11Selection(reflect.TypeOf(semanticview.View{})) + ` }`})
	if err != nil {
		p.t.Fatal(err)
	}
	var gql struct {
		Data struct {
			SemanticSnapshot map[string]any `json:"semanticSnapshot"`
		} `json:"data"`
		Errors []any `json:"errors"`
	}
	if err := json.Unmarshal(p.request(http.MethodPost, p.cfg.GraphQLPath, query), &gql); err != nil || gql.Errors != nil {
		p.t.Fatalf("GraphQL semanticSnapshot: %v %v", err, gql.Errors)
	}
	args, err := json.Marshal(map[string]any{"asset_ids": ids})
	if err != nil {
		p.t.Fatal(err)
	}
	result := p.tool(mcp.SemanticSnapshotTool, string(args))
	if result.isError {
		p.t.Fatalf("MCP semantic snapshot: %s %s", result.code, result.message)
	}
	var view semanticview.View
	var raw map[string]any
	if json.Unmarshal(result.data, &view) != nil || json.Unmarshal(result.data, &raw) != nil {
		p.t.Fatalf("MCP semantic snapshot data: %s", result.data)
	}
	graphQLAssets, _ := m11Normalize(gql.Data.SemanticSnapshot).(map[string]any)["assets"].([]any)
	mcpAssets, _ := m11Normalize(raw).(map[string]any)["assets"].([]any)
	if len(graphQLAssets) != len(ids) || len(mcpAssets) != len(ids) || len(view.Assets) != len(ids) {
		p.t.Fatalf("selection lost assets: GraphQL=%d MCP=%d want %d", len(graphQLAssets), len(mcpAssets), len(ids))
	}
	for i := range mcpAssets {
		left, right := graphQLAssets[i].(map[string]any), mcpAssets[i].(map[string]any)
		// Each surface captures once at its own instant, so evaluation clocks
		// differ; the evaluated publication tuple must not.
		for _, member := range []string{"assetId", "unavailableReason", "snapshot", "projection", "selections"} {
			if !reflect.DeepEqual(left[member], right[member]) {
				p.t.Fatalf("%v: GraphQL and MCP disagree on %s:\n%v\n%v", right["assetId"], member, left[member], right[member])
			}
		}
		for _, member := range []string{"snapshotId", "revisions"} {
			l, _ := left["evaluation"].(map[string]any)
			r, _ := right["evaluation"].(map[string]any)
			if !reflect.DeepEqual(l[member], r[member]) {
				p.t.Fatalf("%v: GraphQL and MCP evaluate different publications", right["assetId"])
			}
		}
	}
	return view
}

func m57Asset(t *testing.T, view semanticview.View, id semreg.AssetID) semanticview.Asset {
	t.Helper()
	for _, asset := range view.Assets {
		if asset.AssetID == id {
			return asset
		}
	}
	t.Fatalf("asset %s missing from %+v", id, view)
	return semanticview.Asset{}
}

func m57Disposition(t *testing.T, asset semanticview.Asset, field semreg.DefinitionID) projection.ProjectionDisposition {
	t.Helper()
	if asset.Projection == nil {
		t.Fatalf("%s has no projection (reason %q)", asset.AssetID, asset.UnavailableReason)
	}
	for _, d := range asset.Projection.Dispositions {
		if d.Kind == projection.ItemFact && d.ItemID == field {
			return d
		}
	}
	t.Fatalf("%s has no %s disposition", asset.AssetID, field)
	return projection.ProjectionDisposition{}
}

func m57Value(t *testing.T, asset semanticview.Asset, field semreg.DefinitionID) semreg.Value {
	t.Helper()
	for _, envelope := range asset.Snapshot.Facts {
		if envelope.Key.FactID == field && len(envelope.Candidates) == 1 && envelope.Candidates[0].Value != nil {
			return *envelope.Candidates[0].Value
		}
	}
	t.Fatalf("%s has no single %s candidate", asset.AssetID, field)
	return semreg.Value{}
}

// requireAdmitted checks that a publication is bound to the source registered
// for its driver and to the hub's current admission of that source.
func (p *m57Process) requireAdmitted(asset semanticview.Asset, driver runtimecontrol.DriverID) {
	p.t.Helper()
	binding, err := p.control.Registry().Binding(driver)
	if err != nil || len(binding.Construction.Sources) != 1 {
		p.t.Fatalf("%s registration: %v", driver, err)
	}
	source := semreg.SourceID(binding.Construction.Sources[0])
	info, ok := p.control.hub.Source(source)
	if asset.Snapshot == nil || !ok || !info.Live {
		p.t.Fatalf("%s: publication %v, hub source %+v", asset.AssetID, asset.UnavailableReason, info)
	}
	generation := semreg.Uint64(strconv.FormatUint(info.Generation, 10))
	if current, ok := m57CurrentSource(*asset.Snapshot); !ok || current.SourceID != source || current.SourceEpochID != info.Epoch {
		p.t.Fatalf("%s: sources %+v; registered %s at %s", asset.AssetID, asset.Snapshot.Sources, source, info.Epoch)
	}
	for _, b := range asset.Snapshot.Bindings {
		if b.State == semreg.BindingCurrent && (b.SourceID != source || b.SourceEpochID != info.Epoch || b.DriverGeneration != generation) {
			p.t.Fatalf("%s: current binding %+v; hub admits %s/%s/%d", asset.AssetID, b, source, info.Epoch, info.Generation)
		}
	}
}

func (p *m57Process) ebusHandle() *semhub.Handle {
	p.t.Helper()
	handle, err := p.control.bridge.PublicationHandle(p.control.bridge.View().Key)
	if err != nil {
		p.t.Fatalf("ebus.primary publication handle: %v", err)
	}
	return handle
}

// m57OperationRequest is a Thermal set_mode request whose every expectation
// comes from the northbound read, with the route that read names.
type m57OperationRequest struct {
	body  []byte
	route operation.Route
}

func m57SetModeRequest(t *testing.T, asset semanticview.Asset, token string) m57OperationRequest {
	t.Helper()
	if asset.Snapshot == nil {
		t.Fatalf("%s unreadable: %s", asset.AssetID, asset.UnavailableReason)
	}
	snapshot := asset.Snapshot
	current := map[semreg.NativeBindingID]bool{}
	for _, binding := range snapshot.Bindings {
		current[binding.BindingID] = binding.State == semreg.BindingCurrent
	}
	// A retired generation's capability may remain beside the current one.
	var capability *semreg.CapabilityInstance
	for i := range snapshot.Capabilities {
		if c := &snapshot.Capabilities[i]; c.Definition.ID == "thermal.capability.set_mode" && current[c.BindingID] {
			capability = c
		}
	}
	source, ok := m57CurrentSource(*snapshot)
	if capability == nil || !ok {
		t.Fatalf("%s publishes no set_mode route", asset.AssetID)
	}
	kind := m57ThermalPublication().ref(m57SetMode)
	input := map[string]any{
		"assetId": asset.AssetID, "bindingId": capability.BindingID, "capabilityInstanceId": capability.InstanceID,
		"operation":                map[string]any{"pack": map[string]any{"id": kind.Pack.ID, "version": kind.Pack.Version}, "id": kind.ID, "version": kind.Version},
		"arguments":                []any{map[string]any{"id": m57ModeField, "value": map[string]any{"kind": "symbol", "symbol": map[string]any{"namespace": m57ModeField, "token": token, "known": true}}}},
		"expectedSemanticRevision": snapshot.Revisions.Semantic, "expectedCapabilityRevision": snapshot.Revisions.Capabilities, "expectedCapabilityInstanceRevision": capability.Revision,
		"expectedSourceEpochId": capability.SourceEpochID, "expectedDriverGeneration": capability.DriverGeneration, "preconditions": []any{}, "timeoutMs": 5000,
	}
	body, err := json.Marshal(map[string]any{"query": `mutation($input: SemanticOperationInput!) { semanticOperation(input: $input) { status kernelOutcome delivery reason } }`, "variables": map[string]any{"input": input}})
	if err != nil {
		t.Fatal(err)
	}
	return m57OperationRequest{body: body, route: operation.Route{CapabilityInstance: capability.InstanceID, ServiceInstance: capability.ServiceInstance, BindingID: capability.BindingID,
		SourceID: source.SourceID, SourceEpochID: capability.SourceEpochID, DriverGeneration: capability.DriverGeneration}}
}

// operate posts one semanticOperation mutation to the production GraphQL
// route. It is safe to call from any goroutine.
func (p *m57Process) operate(request m57OperationRequest) (map[string]any, error) {
	status, raw, err := p.send(http.MethodPost, p.cfg.GraphQLPath, request.body)
	if err != nil || status != http.StatusOK {
		return nil, fmt.Errorf("semanticOperation: status=%d err=%v body=%s", status, err, raw)
	}
	var response struct {
		Data struct {
			Result map[string]any `json:"semanticOperation"`
		} `json:"data"`
		Errors []any `json:"errors"`
	}
	if err := json.Unmarshal(raw, &response); err != nil || response.Errors != nil || response.Data.Result == nil {
		return nil, fmt.Errorf("semanticOperation: %v %s", err, raw)
	}
	return response.Data.Result, nil
}

func m57RequireResult(t *testing.T, what string, result map[string]any, err error, status, kernel, delivery, reason string) {
	t.Helper()
	want := map[string]any{"status": status, "kernelOutcome": nil, "delivery": nil, "reason": nil}
	for member, value := range map[string]string{"kernelOutcome": kernel, "delivery": delivery, "reason": reason} {
		if value != "" {
			want[member] = value
		}
	}
	if err != nil || !reflect.DeepEqual(result, want) {
		t.Fatalf("%s: %v %v; want %v", what, result, err, want)
	}
}

// portalDrivers reads the Portal configuration driver rows.
func (p *m57Process) portalDrivers() map[string]map[string]any {
	p.t.Helper()
	var view struct {
		State struct {
			Drivers []map[string]any `json:"drivers"`
		} `json:"state"`
	}
	if err := json.Unmarshal(p.request(http.MethodGet, p.cfg.PortalPath+"/api/v1/config", nil), &view); err != nil {
		p.t.Fatal(err)
	}
	rows := map[string]map[string]any{}
	for _, row := range view.State.Drivers {
		rows[fmt.Sprint(row["driver_id"])] = row
	}
	return rows
}

func (p *m57Process) requirePortalRow(id runtimecontrol.DriverID, status, reason string, effective bool) {
	p.t.Helper()
	row := p.portalDrivers()[string(id)]
	if row["status"] != status || row["reason"] != reason || row["effective"] != effective {
		p.t.Fatalf("Portal %s row %v; want %s/%s effective=%t", id, row, status, reason, effective)
	}
}

func (p *m57Process) routeConfig() semanticoperations.Config {
	p.t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.route) != 1 {
		p.t.Fatalf("semantic operation route composed %d times", len(p.route))
	}
	return p.route[0]
}

// stopAndRequireJoined stops the process and checks that every task the
// composition started has joined and nothing remains admitted or open.
func (p *m57Process) stopAndRequireJoined() {
	p.t.Helper()
	hub := p.control.hub
	p.stop()
	for _, s := range []*m57Sidecar{p.sunspec, p.eebus} {
		if s.running.Load() || s.workers.Load() != 0 {
			p.t.Errorf("%s after shutdown: running=%t workers=%d", s.id, s.running.Load(), s.workers.Load())
		}
		if started := s.starts.Load(); s.stops.Load() != started {
			p.t.Errorf("%s: %d generations started, %d joined", s.id, started, s.stops.Load())
		}
		// A generation whose lease was refused was never exposed, so the
		// composition stops it directly; every exposed one is withdrawn first.
		if s.leaseErr == nil && s.unwithdrawn.Load() != 0 {
			p.t.Errorf("%s: %d native stops without a preceding composition withdrawal", s.id, s.unwithdrawn.Load())
		}
	}
	p.mu.Lock()
	endpoints := append([]*m57GrowattEndpoint(nil), p.growatt...)
	p.mu.Unlock()
	for i, endpoint := range endpoints {
		if endpoint.closes() != 1 {
			p.t.Errorf("Growatt endpoint %d closed %d times", i+1, endpoint.closes())
		}
	}
	if p.env.background.Load() != 0 {
		p.t.Errorf("eBUS background tasks still running: %d", p.env.background.Load())
	}
	discoveries := int(p.env.discovery.Load())
	exits := 0
	for _, event := range p.env.events() {
		if strings.HasPrefix(event, "discovery_exit ") {
			exits++
		}
	}
	if exits != discoveries {
		p.t.Errorf("eBUS discovery tasks: %d started, %d joined", discoveries, exits)
	}
	// Every request was served by the production server on this listener. The
	// server closes the operator endpoint from its own goroutines on shutdown.
	m06Wait(p.t, func() bool { return p.operator.closes.Load() > 0 })
	if !p.listener.closed() || p.listener.accepted.Load() == 0 || p.operator.starts.Load() != 1 {
		p.t.Errorf("control plane endpoints after shutdown: listener closed=%t accepted=%d operator starts=%d closes=%d", p.listener.closed(), p.listener.accepted.Load(), p.operator.starts.Load(), p.operator.closes.Load())
	}
	// Managed sidecars fence their generation on stop. Process exit preserves
	// the eBUS epoch for the next incarnation, so its gate is the bridge: no
	// publication handle is issued once the generation is closed.
	for _, source := range hub.Sources() {
		if source.Live && source.Driver != string(primaryEBusDriverID) {
			p.t.Errorf("source %s (%s) still admitted after shutdown", source.Source, source.Driver)
		}
	}
	if handle, err := p.control.bridge.PublicationHandle(p.control.bridge.View().Key); err == nil || handle != nil {
		p.t.Error("eBUS bridge issued a publication handle after shutdown")
	}
}

// --- tests ------------------------------------------------------------------------

// Registration, publication and every northbound reader agree: the four
// configured drivers run under one registry; each available pack publishes an
// already-defined value through its driver's admitted source; GraphQL and MCP
// read the same publication, revision, value and loss through the shared
// provider; Portal advertises that same read and reports the lifecycle the
// MCP tools report; the scrape route withholds semantic samples.
func TestM57ComposedDriversShareOneNorthboundPath(t *testing.T) {
	p := m57Start(t, m57Options{})
	running := map[runtimecontrol.DriverID]drivermanager.ObservedState{}
	for _, id := range m57Drivers {
		running[id] = drivermanager.ObservedRunning
	}
	drivers := p.waitObserved(running)
	registered := map[runtimecontrol.DriverID]bool{}
	for _, d := range p.control.Registry().List().Drivers {
		registered[d.DriverID] = true
	}
	for _, id := range m57Drivers {
		if !registered[id] || !drivers[id].Enabled {
			t.Fatalf("%s: registered=%t lifecycle=%+v", id, registered[id], drivers[id])
		}
	}
	p.mu.Lock()
	listens := append([]string(nil), p.listens...)
	p.mu.Unlock()
	if !reflect.DeepEqual(listens, []string{"tcp " + p.cfg.HTTPAddr}) || p.operator.starts.Load() != 1 {
		t.Fatalf("production endpoints requested: listen=%v operator=%d", listens, p.operator.starts.Load())
	}

	// The route reads through the HTTP shared provider of this process hub and
	// production installs no intent owner and the default executor lookup.
	route := p.routeConfig()
	shared, ok := route.Snapshots.(*hubSemanticSnapshotProvider)
	if !ok || shared.hub != semanticHubCapture(p.control.hub) || route.Kernel == nil || route.Intents != nil || !reflect.DeepEqual(route.Executors, semanticoperations.NoExecutors()) {
		t.Fatalf("production route configuration: %+v", route)
	}

	// One already-defined value per available pack: Thermal through the
	// admitted eBUS generation, Storage through the real Growatt driver on an
	// ordinary MCP read, PV (with one explicitly withheld value) through the
	// SunSpec managed start.
	ebus := p.ebusHandle()
	if err := p.thermal.declare(p.control.hub, ebus, primaryEBusDriverID); err != nil {
		t.Fatal(err)
	}
	if err := p.thermal.publish(p.control.hub, ebus, m57Mode("off")); err != nil {
		t.Fatal(err)
	}
	if result := p.tool(mcp.SemanticV1GrowattStorageCurrentGetTool, `{}`); result.isError {
		t.Fatalf("Growatt storage read: %s %s", result.code, result.message)
	}
	storageAsset := semreg.AssetID(p.cfg.ModbusTCPConfig.GrowattBMSRS485.AssetID)
	view := p.read(m57ThermalAsset, storageAsset, m57PVAsset)

	thermalView := m57Asset(t, view, m57ThermalAsset)
	p.requireAdmitted(thermalView, primaryEBusDriverID)
	if d := m57Disposition(t, thermalView, m57ModeField); d.Outcome != projection.ProjectionExact || !reflect.DeepEqual(m57Value(t, thermalView, m57ModeField), m57Symbol(m57ModeField, "off")) {
		t.Fatalf("Thermal mode %+v", d)
	}
	storageView := m57Asset(t, view, storageAsset)
	p.requireAdmitted(storageView, managedGrowattID)
	exact := 0
	for _, d := range storageView.Projection.Dispositions {
		if d.Kind == projection.ItemFact && d.Outcome == projection.ProjectionExact {
			exact++
		}
	}
	if exact == 0 {
		t.Fatalf("Storage published no exact value: %+v", storageView.Projection.Dispositions)
	}
	pvView := m57Asset(t, view, m57PVAsset)
	p.requireAdmitted(pvView, managedSunSpecID)
	if d := m57Disposition(t, pvView, m57PowerField); d.Outcome != projection.ProjectionExact || !reflect.DeepEqual(m57Value(t, pvView, m57PowerField), m57Quantity(1500, "unit.watt")) {
		t.Fatalf("PV power %+v", d)
	}
	// The withheld value stays in the publication as native evidence, while
	// both surfaces withhold it with the same reason.
	if d := m57Disposition(t, pvView, m57TempField); d.Outcome != projection.ProjectionWithheld || d.Reason == nil || *d.Reason != "semantic.read.reason.fact_unavailable" {
		t.Fatalf("PV withheld value %+v", d)
	}

	// Portal mounts the shared read on the same GraphQL route and reports the
	// lifecycle the ordinary MCP list reports.
	var bootstrap struct {
		Endpoints map[string]any `json:"endpoints"`
		Read      struct {
			Operation   string   `json:"snapshot_operation"`
			Unavailable []string `json:"unavailable_operations"`
		} `json:"portal_read"`
	}
	if err := json.Unmarshal(p.request(http.MethodGet, p.cfg.PortalPath+"/api/v1/bootstrap", nil), &bootstrap); err != nil {
		t.Fatal(err)
	}
	if bootstrap.Read.Operation != "semanticSnapshot" || len(bootstrap.Read.Unavailable) != 0 || bootstrap.Endpoints["graphql"] != p.cfg.GraphQLPath {
		t.Fatalf("Portal shared read: %+v endpoints=%v", bootstrap.Read, bootstrap.Endpoints)
	}
	drivers = p.drivers()
	rows := p.portalDrivers()
	for _, id := range m57Drivers {
		isRunning := drivers[id].Enabled && drivers[id].ObservedState == string(drivermanager.ObservedRunning)
		if row := rows[string(id)]; !isRunning || row["status"] != "effective" || row["reason"] != "running" || row["effective"] != isRunning {
			t.Fatalf("Portal %s row %v; MCP lifecycle %+v", id, row, drivers[id])
		}
	}

	// The scrape route serves the transport exposition and declares the
	// semantic availability family; with no durable slot registry it withholds
	// every semantic sample rather than exporting an unregistered value.
	metrics := string(p.request(http.MethodGet, p.cfg.MetricsPath, nil))
	transport, availability := strings.Index(metrics, "# TYPE helianthus_transport_runtime_status gauge"), strings.Index(metrics, "# TYPE helianthus_semantic_field_available gauge")
	if transport < 0 || availability < transport || strings.Contains(metrics, "helianthus_semantic_field_available{") || strings.Contains(metrics, "helianthus_thermal_") {
		t.Fatalf("scrape route sections:\n%s", metrics)
	}

	p.stopAndRequireJoined()
}

// The ordinary MCP lifecycle tools replace generations end to end: the
// composition withdraws each exposed generation before its native stop, a
// stopped driver's loss reads identically on both surfaces and Portal, a
// publication racing withdrawal and any later use of a retired handle are
// refused, and each successor publishes under its own admitted generation.
func TestM57LifecycleToolsReplaceGenerationsAndRefuseLatePublication(t *testing.T) {
	p := m57Start(t, m57Options{})
	running := map[runtimecontrol.DriverID]drivermanager.ObservedState{}
	for _, id := range m57Drivers {
		running[id] = drivermanager.ObservedRunning
	}
	p.waitObserved(running)
	hub := p.control.hub
	retired := p.sunspec.currentHandle()

	p.decide(mcp.RuntimeControlStopTool, managedSunSpecID)
	p.waitObserved(map[runtimecontrol.DriverID]drivermanager.ObservedState{managedSunSpecID: drivermanager.ObservedStopped})
	if loss := m57Asset(t, p.read(m57PVAsset), m57PVAsset); loss.UnavailableReason != "semantic.read.reason.source_unavailable" || loss.Snapshot != nil {
		t.Fatalf("stopped SunSpec read %+v", loss)
	}
	p.requirePortalRow(managedSunSpecID, "withheld", "lifecycle_stopped", false)
	if late := p.sunspec.lateResults(); len(late) != 1 || semreg.ErrorIdentifier(late[0]) != semreg.StaleDriverGeneration {
		t.Fatalf("publication racing withdrawal: %v; want one %s refusal", late, semreg.StaleDriverGeneration)
	}

	p.decide(mcp.RuntimeControlStartTool, managedSunSpecID)
	p.waitObserved(map[runtimecontrol.DriverID]drivermanager.ObservedState{managedSunSpecID: drivermanager.ObservedRunning})
	successor := m57Asset(t, p.read(m57PVAsset), m57PVAsset)
	p.requireAdmitted(successor, managedSunSpecID)
	if !reflect.DeepEqual(m57Value(t, successor, m57PowerField), m57Quantity(1600, "unit.watt")) {
		t.Fatalf("successor PV value %+v", m57Value(t, successor, m57PowerField))
	}
	if err := p.sunspec.publication.publish(hub, retired, m57PVFacts(1, true)); semreg.ErrorIdentifier(err) != semreg.StaleDriverGeneration {
		t.Fatalf("retired SunSpec generation after its successor started: %v", err)
	}
	encoded, err := json.Marshal(p.read(m57PVAsset))
	if err != nil || bytes.Contains(encoded, []byte(`"coefficient":"9999"`)) {
		t.Fatalf("a late publication reached a reader: %v", err)
	}
	p.sunspec.mu.Lock()
	keys := append([]runtimecontrol.LifecycleCorrelation(nil), p.sunspec.keys...)
	p.sunspec.mu.Unlock()
	if len(keys) != 2 || keys[0].Epoch == keys[1].Epoch || keys[0].DriverID != managedSunSpecID || keys[0].SourceID != keys[1].SourceID {
		t.Fatalf("lifecycle correlations %+v; want a new epoch for START after STOP", keys)
	}

	// The real Growatt driver fences its generation and activates the next on
	// RESTART; until the successor publishes, both surfaces withhold the
	// predecessor's publication.
	storageAsset := semreg.AssetID(p.cfg.ModbusTCPConfig.GrowattBMSRS485.AssetID)
	if result := p.tool(mcp.SemanticV1GrowattStorageCurrentGetTool, `{}`); result.isError {
		t.Fatalf("Growatt storage read: %s %s", result.code, result.message)
	}
	p.requireAdmitted(m57Asset(t, p.read(storageAsset), storageAsset), managedGrowattID)
	source := semreg.SourceID(p.cfg.ModbusTCPConfig.GrowattBMSRS485.SourceID)
	p.decide(mcp.RuntimeControlRestartTool, managedGrowattID)
	m06Wait(t, func() bool {
		info, ok := hub.Source(source)
		p.mu.Lock()
		opened := len(p.growatt)
		p.mu.Unlock()
		return ok && info.Live && info.Generation == 2 && opened == 2
	})
	p.waitObserved(map[runtimecontrol.DriverID]drivermanager.ObservedState{managedGrowattID: drivermanager.ObservedRunning})
	if withheld := m57Asset(t, p.read(storageAsset), storageAsset); withheld.UnavailableReason != "semantic.read.reason.publication_withheld" || withheld.Snapshot != nil {
		t.Fatalf("predecessor Storage publication exposed after restart: %+v", withheld)
	}
	// The successor's native read path is admitted. Its Storage publication is
	// asserted by the TestM61 fixtures, not here.
	if result := p.tool(mcp.GrowattBMSRS485V202StatusGetTool, `{}`); result.isError {
		t.Fatalf("Growatt native read after restart: %s %s", result.code, result.message)
	}
	p.mu.Lock()
	endpoints := append([]*m57GrowattEndpoint(nil), p.growatt...)
	p.mu.Unlock()
	if len(endpoints) != 2 || endpoints[0].closes() != 1 {
		t.Fatalf("Growatt endpoints opened=%d; want two with the retired one closed once", len(endpoints))
	}

	p.stopAndRequireJoined()
}

// The fake downward route runs through the production GraphQL route, shared
// provider and built-in SemReg kernel. Within one generation the injected
// executor's readback confirms; across an ordinary MCP RESTART the same
// completion is never confirmed because the kernel refuses evidence naming the
// successor epoch, a retired-generation publication is refused, and a request
// naming the retired generation is not sent.
func TestM57RestartRejectsLateOperationCompletion(t *testing.T) {
	p := m57Start(t, m57Options{})
	running := map[runtimecontrol.DriverID]drivermanager.ObservedState{}
	for _, id := range m57Drivers {
		running[id] = drivermanager.ObservedRunning
	}
	p.waitObserved(running)
	hub := p.control.hub
	shared := p.routeConfig().Snapshots
	retired := p.ebusHandle()
	if err := p.thermal.declare(hub, retired, primaryEBusDriverID); err != nil {
		t.Fatal(err)
	}
	if err := p.thermal.publish(hub, retired, m57Mode("off")); err != nil {
		t.Fatal(err)
	}
	// The native owner republishes the requested mode through whichever
	// generation is admitted when it completes.
	republish := func() error {
		handle, err := p.control.bridge.PublicationHandle(p.control.bridge.View().Key)
		if err != nil {
			return err
		}
		return p.thermal.publish(hub, handle, m57Mode("heat"))
	}

	first := m57SetModeRequest(t, m57Asset(t, p.read(m57ThermalAsset), m57ThermalAsset), "heat")
	confirming := newM57Executor(shared, republish, false)
	p.lookup.inject(first.route, confirming)
	result, err := p.operate(first)
	m57RequireResult(t, "same-generation completion", result, err, "readback_confirmed", "applied", "sent", "")
	if confirming.calls.Load() != 1 {
		t.Fatalf("executor calls=%d", confirming.calls.Load())
	}

	inFlight := m57SetModeRequest(t, m57Asset(t, p.read(m57ThermalAsset), m57ThermalAsset), "heat")
	if inFlight.route != first.route {
		t.Fatal("route changed without a lifecycle transition")
	}
	late := newM57Executor(shared, republish, true)
	late.kernel = p.routeConfig().Kernel
	t.Cleanup(late.unhold)
	p.lookup.inject(inFlight.route, late)
	type outcome struct {
		result map[string]any
		err    error
	}
	results := make(chan outcome, 1)
	go func() {
		result, err := p.operate(inFlight)
		results <- outcome{result, err}
	}()
	select {
	case <-late.dispatched:
	case <-time.After(rctlBound):
		t.Fatal("operation was not dispatched")
	}
	p.decide(mcp.RuntimeControlRestartTool, primaryEBusDriverID)
	m06Wait(t, func() bool {
		handle, err := p.control.bridge.PublicationHandle(p.control.bridge.View().Key)
		return err == nil && handle.Epoch() != retired.Epoch()
	})
	if err := p.thermal.publish(hub, retired, m57Mode("auto")); semreg.ErrorIdentifier(err) != semreg.StaleSourceEpoch {
		t.Fatalf("retired eBUS generation after restart: %v", err)
	}
	late.unhold()
	completed := <-results
	// The router reports the kernel's refusal of the late evidence, or its own
	// bounded wait when the restart outlasted the request timeout; never a
	// confirmation.
	if reason := fmt.Sprint(completed.result["reason"]); completed.err != nil || completed.result["status"] != "unknown" ||
		completed.result["kernelOutcome"] != nil || completed.result["delivery"] != nil ||
		reason != semanticoperations.ReasonExecutionEvidenceInvalid && reason != semanticoperations.ReasonEvidenceTimeout {
		t.Fatalf("late prior-generation completion: %v %v", completed.result, completed.err)
	}
	<-late.done
	// The kernel refuses the evidence because its readback names the
	// successor epoch, not the admitted route's.
	if semreg.ErrorIdentifier(late.verdict) != semreg.StaleSourceEpoch {
		t.Fatalf("late completion verdict %v; want %s", late.verdict, semreg.StaleSourceEpoch)
	}

	successor := m57Asset(t, p.read(m57ThermalAsset), m57ThermalAsset)
	p.requireAdmitted(successor, primaryEBusDriverID)
	if current, _ := m57CurrentSource(*successor.Snapshot); current.SourceEpochID == retired.Epoch() || !reflect.DeepEqual(m57Value(t, successor, m57ModeField), m57Symbol(m57ModeField, "heat")) {
		t.Fatalf("successor Thermal publication %+v", successor.Snapshot.Sources)
	}
	calls := p.lookup.calls.Load()
	result, err = p.operate(inFlight)
	m57RequireResult(t, "request naming the retired generation", result, err, "failed", "rejected", "not_sent", string(semreg.StaleSourceEpoch))
	if p.lookup.calls.Load() != calls || late.calls.Load() != 1 {
		t.Fatal("a retired-generation request reached executor lookup")
	}
	// Successor-generation admission is asserted by
	// TestM65RestartedDriverAdmitsOperationsOnItsSuccessor.

	p.stopAndRequireJoined()
}

// m65Shape is one managed-driver restart as the shared hub sees it. Each shape
// changes exactly one lifecycle axis of the route: the eBUS restart retires its
// source epoch, while the SunSpec restart keeps the epoch and fences the prior
// driver generation. The source ID is the same on both sides of a restart.
type m65Shape struct {
	name   string
	driver runtimecontrol.DriverID
	// handle returns the publication handle the driver currently holds.
	handle func(*m57Process) (*semhub.Handle, error)
	// succeeds reports whether next is the admitted successor of retired.
	succeeds func(retired, next *semhub.Handle) bool
	// stale is the identifier the kernel refuses a retired-lifecycle request with.
	stale semreg.ErrorID
}

var m65Shapes = []m65Shape{
	{
		name: "successor source epoch", driver: primaryEBusDriverID, stale: semreg.StaleSourceEpoch,
		handle: func(p *m57Process) (*semhub.Handle, error) {
			return p.control.bridge.PublicationHandle(p.control.bridge.View().Key)
		},
		succeeds: func(retired, next *semhub.Handle) bool { return next.Epoch() != retired.Epoch() },
	},
	{
		name: "successor driver generation", driver: managedSunSpecID, stale: semreg.StaleDriverGeneration,
		handle: func(p *m57Process) (*semhub.Handle, error) {
			if handle := p.sunspec.currentHandle(); handle != nil {
				return handle, nil
			}
			return nil, errors.New("SunSpec holds no publication handle")
		},
		succeeds: func(retired, next *semhub.Handle) bool {
			return next.Epoch() == retired.Epoch() && next.Generation() == retired.Generation()+1
		},
	},
}

// with returns the request with one member of its mutation input replaced.
func (r m57OperationRequest) with(t *testing.T, member string, value any) m57OperationRequest {
	t.Helper()
	var envelope struct {
		Query     string `json:"query"`
		Variables struct {
			Input map[string]any `json:"input"`
		} `json:"variables"`
	}
	if err := json.Unmarshal(r.body, &envelope); err != nil || envelope.Variables.Input[member] == nil {
		t.Fatalf("request has no %s member: %v", member, err)
	}
	envelope.Variables.Input[member] = value
	body, err := json.Marshal(map[string]any{"query": envelope.Query, "variables": envelope.Variables})
	if err != nil {
		t.Fatal(err)
	}
	r.body = body
	return r
}

// After a managed driver restarts, the shared snapshot holds the retired
// route's withdrawn capability beside the successor's current one. The
// successor's operation is admitted, dispatched and confirmed through the
// production GraphQL route, shared provider and built-in kernel, while every
// request naming a retired route member is refused without reaching executor
// lookup. Before the SemReg fix every request for the successor route was
// refused with capability_unavailable.
//
// The fixture's intent owner and executor are the M57 fakes: no production
// intent owner exists yet, so admission is asserted at the kernel boundary the
// gateway route uses. The Growatt Storage publication declares no capability,
// so the vehicles are the two managed drivers that publish one: the real
// ebus.primary restart and the injected SunSpec managed fake, both restarted
// through the ordinary MCP lifecycle tool, with the Thermal set_mode route M57
// already drives published through each driver's admitted source.
//
// Route members that differ between the retired and successor routes, and the
// request that carries each retired value alone:
//   - source epoch (eBUS shape): refused as stale_source_epoch;
//   - driver generation (SunSpec shape): refused as stale_driver_generation;
//   - capability instance: refused with the shape's stale identifier;
//   - binding: admitted, then refused by the router as binding_mismatch;
//   - service instance: not a request member; it differs between the routes and
//     is checked on the admitted route and guard claims.
//
// The source ID is held fixed because a restart preserves source identity.
func TestM65RestartedDriverAdmitsOperationsOnItsSuccessor(t *testing.T) {
	for _, shape := range m65Shapes {
		t.Run(shape.name, func(t *testing.T) {
			p := m57Start(t, m57Options{})
			running := map[runtimecontrol.DriverID]drivermanager.ObservedState{}
			for _, id := range m57Drivers {
				running[id] = drivermanager.ObservedRunning
			}
			p.waitObserved(running)
			hub := p.control.hub
			shared := p.routeConfig().Snapshots
			retired, err := shape.handle(p)
			if err != nil {
				t.Fatal(err)
			}
			if err := p.thermal.declare(hub, retired, shape.driver); err != nil {
				t.Fatal(err)
			}
			if err := p.thermal.publish(hub, retired, m57Mode("off")); err != nil {
				t.Fatal(err)
			}
			// The native owner republishes the requested mode through the
			// generation the driver holds when it completes.
			republish := func() error {
				handle, err := shape.handle(p)
				if err != nil {
					return err
				}
				return p.thermal.publish(hub, handle, m57Mode("heat"))
			}

			// The retired generation's operation is admitted while it is current.
			before := m57SetModeRequest(t, m57Asset(t, p.read(m57ThermalAsset), m57ThermalAsset), "heat")
			beforeExecutor := newM57Executor(shared, republish, false)
			p.lookup.inject(before.route, beforeExecutor)
			result, err := p.operate(before)
			m57RequireResult(t, "operation before the restart", result, err, "readback_confirmed", "applied", "sent", "")
			if beforeExecutor.calls.Load() != 1 {
				t.Fatalf("executor calls before the restart = %d", beforeExecutor.calls.Load())
			}

			p.decide(mcp.RuntimeControlRestartTool, shape.driver)
			var successor *semhub.Handle
			m06Wait(t, func() bool {
				next, err := shape.handle(p)
				if err != nil || !shape.succeeds(retired, next) {
					return false
				}
				successor = next
				return true
			})
			p.waitObserved(running)
			if err := p.thermal.publish(hub, successor, m57Mode("off")); err != nil {
				t.Fatal(err)
			}

			// The snapshot every post-restart request is admitted against holds
			// the retired capability as a withdrawn tombstone beside the
			// successor's, which is the shape that SemReg once refused.
			view := m57Asset(t, p.read(m57ThermalAsset), m57ThermalAsset)
			p.requireAdmitted(view, shape.driver)
			tombstone := false
			for _, capability := range view.Snapshot.Capabilities {
				tombstone = tombstone || capability.InstanceID == before.route.CapabilityInstance && capability.Availability == semreg.AvailabilityWithdrawn
			}
			if !tombstone || len(view.Snapshot.Capabilities) != 2 {
				t.Fatalf("restart did not leave the retired capability as a tombstone: %+v", view.Snapshot.Capabilities)
			}

			// The successor's route is minted from the admitted handle and
			// shares no route member with the retired one except the source.
			want := operation.Route{CapabilityInstance: p.thermal.capabilityID(successor, "thermal.capability.set_mode"), ServiceInstance: p.thermal.serviceID(successor, "thermal.service.system"),
				BindingID: p.thermal.binding(successor), SourceID: successor.Source(), SourceEpochID: successor.Epoch(), DriverGeneration: successor.GenerationID()}
			request := m57SetModeRequest(t, view, "heat")
			epochChanged, generationChanged := before.route.SourceEpochID != want.SourceEpochID, before.route.DriverGeneration != want.DriverGeneration
			if request.route != want || before.route.SourceID != want.SourceID || before.route.CapabilityInstance == want.CapabilityInstance ||
				before.route.ServiceInstance == want.ServiceInstance || before.route.BindingID == want.BindingID ||
				epochChanged != (shape.stale == semreg.StaleSourceEpoch) || generationChanged != (shape.stale == semreg.StaleDriverGeneration) {
				t.Fatalf("retired route %+v, successor route %+v; want %+v with only the source shared and the %s axis changed", before.route, request.route, want, shape.stale)
			}

			// The successor's operation is admitted on its own route only.
			executor := newM57Executor(shared, republish, false)
			p.lookup.inject(request.route, executor)
			result, err = p.operate(request)
			m57RequireResult(t, "operation on the successor", result, err, "readback_confirmed", "applied", "sent", "")
			if executor.calls.Load() != 1 {
				t.Fatalf("successor executor calls = %d", executor.calls.Load())
			}
			dispatch := <-executor.dispatched
			wantGuard := operation.GuardClaims{AssetID: m57ThermalAsset, ExpectedCapabilityRevision: view.Snapshot.Revisions.Capabilities, CapabilityInstance: want.CapabilityInstance, ExpectedCapabilityInstanceRevision: "1",
				ServiceInstance: want.ServiceInstance, BindingID: want.BindingID, SourceID: want.SourceID, SourceEpochID: want.SourceEpochID, DriverGeneration: want.DriverGeneration}
			if dispatch.Route != want || dispatch.Guard != wantGuard {
				t.Fatalf("admitted route %+v guard %+v; want %+v %+v", dispatch.Route, dispatch.Guard, want, wantGuard)
			}

			// Every request naming a retired route member stays refused before
			// executor lookup, whether it carries every retired member or only one.
			// The successor's operation moved the revisions, so each single-member
			// request is rebuilt from a fresh read and differs from the successor's
			// current request in that member alone.
			current := m57SetModeRequest(t, m57Asset(t, p.read(m57ThermalAsset), m57ThermalAsset), "heat")
			if current.route != want {
				t.Fatalf("successor route after its operation %+v; want %+v", current.route, want)
			}
			type refusal struct {
				name    string
				request m57OperationRequest
				reason  string
			}
			refusals := []refusal{
				{"request naming the retired route", before, string(shape.stale)},
				{"retired capability instance", current.with(t, "capabilityInstanceId", before.route.CapabilityInstance), string(shape.stale)},
				{"retired binding", current.with(t, "bindingId", before.route.BindingID), semanticoperations.ReasonBindingMismatch},
			}
			if epochChanged {
				refusals = append(refusals, refusal{"retired source epoch", current.with(t, "expectedSourceEpochId", before.route.SourceEpochID), string(semreg.StaleSourceEpoch)})
			}
			if generationChanged {
				refusals = append(refusals, refusal{"retired driver generation", current.with(t, "expectedDriverGeneration", before.route.DriverGeneration), string(semreg.StaleDriverGeneration)})
			}
			calls := p.lookup.calls.Load()
			for _, refused := range refusals {
				result, err := p.operate(refused.request)
				m57RequireResult(t, refused.name, result, err, "failed", "rejected", "not_sent", refused.reason)
			}
			if p.lookup.calls.Load() != calls || beforeExecutor.calls.Load() != 1 || executor.calls.Load() != 1 {
				t.Fatalf("a retired-route request reached executor lookup: lookups %d->%d", calls, p.lookup.calls.Load())
			}

			p.stopAndRequireJoined()
		})
	}
}

// A sidecar whose construction is refused stays withheld and unconstructed.
// A sidecar whose shell lease is refused after its native start is stopped by
// the composition itself: its worker joins, its hub generation is fenced and
// its publication is never readable. Both read as failed on the MCP list and
// in Portal while the shell and sibling drivers keep serving.
func TestM57FailedAndWithheldSidecarsArePresentedTruthfully(t *testing.T) {
	p := m57Start(t, m57Options{sunspecLeaseErr: errors.New("synthetic SunSpec lease refused"), eebusConstructErr: errors.New("synthetic eeBUS construction refused")})
	drivers := p.waitObserved(map[runtimecontrol.DriverID]drivermanager.ObservedState{
		primaryEBusDriverID: drivermanager.ObservedRunning, managedGrowattID: drivermanager.ObservedRunning,
		managedSunSpecID: drivermanager.ObservedFailed, managedEEBusID: drivermanager.ObservedFailed,
	})
	for _, id := range []runtimecontrol.DriverID{managedSunSpecID, managedEEBusID} {
		d := drivers[id]
		if !d.Enabled || d.DesiredState != string(runtimecontrol.DesiredRunning) || d.Reason == "" || d.Reason == string(drivermanager.ReasonNone) {
			t.Fatalf("%s lifecycle %+v", id, d)
		}
		p.requirePortalRow(id, "error", strings.ToLower(d.Reason), false)
	}
	if p.eebus.constructs.Load() != 1 || p.eebus.starts.Load() != 0 {
		t.Fatalf("withheld eeBUS constructs=%d starts=%d", p.eebus.constructs.Load(), p.eebus.starts.Load())
	}
	// The refused SunSpec generation was started, then stopped and joined by
	// the composition; the publication it made before the refusal is fenced.
	if p.sunspec.starts.Load() != 1 || p.sunspec.stops.Load() != 1 || p.sunspec.workers.Load() != 0 || p.sunspec.running.Load() {
		t.Fatalf("refused SunSpec generation starts=%d stops=%d workers=%d", p.sunspec.starts.Load(), p.sunspec.stops.Load(), p.sunspec.workers.Load())
	}
	if late := p.sunspec.lateResults(); len(late) != 1 || semreg.ErrorIdentifier(late[0]) != semreg.StaleDriverGeneration {
		t.Fatalf("refused generation's racing publication: %v", late)
	}
	if pvView := m57Asset(t, p.read(m57PVAsset), m57PVAsset); pvView.UnavailableReason != "semantic.read.reason.source_unavailable" || pvView.Snapshot != nil {
		t.Fatalf("refused SunSpec generation readable: %+v", pvView)
	}

	// Siblings keep publishing and the shell keeps reading.
	p.requirePortalRow(primaryEBusDriverID, "effective", "running", true)
	p.requirePortalRow(managedGrowattID, "effective", "running", true)
	if result := p.tool(mcp.SemanticV1GrowattStorageCurrentGetTool, `{}`); result.isError {
		t.Fatalf("Growatt storage read: %s %s", result.code, result.message)
	}
	ebus := p.ebusHandle()
	if err := p.thermal.declare(p.control.hub, ebus, primaryEBusDriverID); err != nil {
		t.Fatal(err)
	}
	if err := p.thermal.publish(p.control.hub, ebus, m57Mode("off")); err != nil {
		t.Fatal(err)
	}
	storageAsset := semreg.AssetID(p.cfg.ModbusTCPConfig.GrowattBMSRS485.AssetID)
	view := p.read(m57ThermalAsset, storageAsset)
	p.requireAdmitted(m57Asset(t, view, m57ThermalAsset), primaryEBusDriverID)
	p.requireAdmitted(m57Asset(t, view, storageAsset), managedGrowattID)

	p.stopAndRequireJoined()
}
