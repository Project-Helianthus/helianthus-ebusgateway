package mcp

// This file is deliberately an injected-record semantic boundary.  It does
// not know how to open a Tesla endpoint, build a FC100 request, or execute an
// operation.  The native current-limit tool remains the only native surface.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/evse"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/projection"
)

const SemanticV1EVSECurrentGetTool = "semantic.v1.evse.current.get"

var ErrTeslaGen3EVSESemanticUnavailable = errors.New("tesla Gen3 EVSE semantic publication unavailable")

const teslaGen3EVSEMaxUnixNano = int64(^uint64(0) >> 1)

var (
	teslaGen3EVSEMinUnixNanoTime = time.Unix(0, -teslaGen3EVSEMaxUnixNano-1).UTC()
	teslaGen3EVSEMaxUnixNanoTime = time.Unix(0, teslaGen3EVSEMaxUnixNano).UTC()
)

// TeslaGen3EVSESemanticConfig deliberately makes every identity and lifecycle
// axis explicit.  Native payloads cannot manufacture an asset identity.
type TeslaGen3EVSESemanticConfig struct {
	AssetID, SourceID, EVSEID, ConnectorID, SourceEpoch, ClockEpoch string
	DriverGeneration                                                uint64
}

// TeslaGen3EVSESemanticEvidence is supplied by the injection owner.  It is
// metadata only: payloads stay in the accepted registry records.
type TeslaGen3EVSESemanticEvidence struct {
	ObservationID        string
	ObservedAt           time.Time
	EvaluatedAt          time.Time
	MonotonicNS          int64
	EvaluatedMonotonicNS int64
	Sequence             uint64
}

type teslaGen3EVSEPersistentEvidence struct {
	OperationVersion     string                        `json:"operation_version"`
	MaxOutputCurrentAmps uint32                        `json:"max_output_current_amps"`
	RequestPayload       []byte                        `json:"request_payload"`
	TerminalPayload      []byte                        `json:"terminal_payload"`
	Lifecycle            TeslaGen3EVSESemanticEvidence `json:"lifecycle"`
}

type teslaGen3EVSEProvisionalEvidence struct {
	OperationVersion        string                        `json:"operation_version"`
	LimitCurrentMaxAmps     uint32                        `json:"limit_current_max_amps"`
	LimitTimeoutSeconds     uint32                        `json:"limit_timeout_s"`
	InhibitCharging         bool                          `json:"inhibit_charging"`
	SetRequestPayload       []byte                        `json:"set_request_payload"`
	AckPayload              []byte                        `json:"ack_payload"`
	ReadbackRequestPayload  []byte                        `json:"readback_request_payload"`
	ReadbackTerminalPayload []byte                        `json:"readback_terminal_payload"`
	Lifecycle               TeslaGen3EVSESemanticEvidence `json:"lifecycle"`
}

type teslaGen3EVSEPublicationEvidence struct {
	Persistent  teslaGen3EVSEPersistentEvidence   `json:"persistent"`
	Provisional *teslaGen3EVSEProvisionalEvidence `json:"provisional,omitempty"`
}

// TeslaGen3EVSESemanticProvider is read-only.  It provides one detached,
// evaluated SemReg view and has no operation or transport method.
type TeslaGen3EVSESemanticProvider interface {
	TeslaGen3EVSESemanticCurrent(context.Context) (any, error)
}

// SemanticEVSECurrent is a detached, evaluated EVSE SemReg tuple. It is the
// protocol-neutral read contract used by passive output bindings; it carries
// neither native evidence bytes nor operation authority.
type SemanticEVSECurrent struct {
	Snapshot   semreg.Snapshot
	Evaluation semreg.EvaluationView
	Selections []semreg.Selection
	Projection projection.ProjectionReport
}

// SemanticEVSEPrometheusProvider is the deliberately narrow read seam for a
// Prometheus binding. Implementations must evaluate an already accepted
// publication at the supplied scrape instant and must not acquire, publish, or
// mutate lifecycle state.
type SemanticEVSEPrometheusProvider interface {
	SemanticEVSECurrentAt(time.Time) (SemanticEVSECurrent, bool)
}

type TeslaGen3EVSESemanticPublication struct {
	mu                 sync.RWMutex
	cfg                TeslaGen3EVSESemanticConfig
	kernel             *semreg.PublicationKernel
	current            semreg.Snapshot
	manifest           projection.ProjectionManifest
	requested          []projection.RequestedItem
	dispositions       []projection.ProjectionDisposition
	evaluatedAt        time.Time
	evaluatedMonotonic semreg.MonotonicPoint
	lastReadAt         time.Time
	lastReadMonotonic  semreg.MonotonicPoint
	publishedReadClock uint64
	lastReadClock      uint64
	allocatedExpiresAt *semreg.MonotonicPoint
	scrapeEpoch        time.Time
	// prometheusMonotonic is a read-only output lifecycle floor. It never
	// changes the immutable SemReg publication or its revision, but prevents an
	// older scrape context from making an already stale fact fresh again.
	prometheusMonotonic semreg.MonotonicPoint
	sequence            uint64
	lastInputDigest     semreg.Digest
	candidateHighWater  map[semreg.CandidateID]semreg.Uint64
	now                 func() time.Time
	readClock           func() (uint64, error)
}

func NewTeslaGen3EVSESemanticPublication(cfg TeslaGen3EVSESemanticConfig) (*TeslaGen3EVSESemanticPublication, error) {
	for _, value := range []string{cfg.AssetID, cfg.SourceID, cfg.EVSEID, cfg.ConnectorID, cfg.SourceEpoch, cfg.ClockEpoch} {
		if strings.TrimSpace(value) == "" || value != strings.TrimSpace(value) {
			return nil, errors.New("tesla Gen3 EVSE semantic identity is incomplete")
		}
	}
	if cfg.AssetID == cfg.SourceID || semreg.AssetID(cfg.AssetID).Validate() != nil || semreg.SourceID(cfg.SourceID).Validate() != nil || cfg.DriverGeneration == 0 {
		return nil, errors.New("tesla Gen3 EVSE semantic identity is invalid")
	}
	kernel, err := semreg.NewPublicationKernel(semreg.AssetID(cfg.AssetID), evse.New())
	if err != nil {
		return nil, err
	}
	origin := time.Now()
	readClock := func() (uint64, error) {
		elapsed := time.Since(origin)
		if elapsed < 0 {
			return 0, errors.New("tesla Gen3 EVSE read monotonic clock regressed")
		}
		return uint64(elapsed.Nanoseconds()), nil
	}
	return &TeslaGen3EVSESemanticPublication{cfg: cfg, kernel: kernel, candidateHighWater: make(map[semreg.CandidateID]semreg.Uint64), now: time.Now, readClock: readClock}, nil
}

// Publish atomically maps one accepted WC3 24.44.3 record bundle.  A rejected
// bundle never advances the kernel or replaces the retained public snapshot.
func (p *TeslaGen3EVSESemanticPublication) Publish(source TeslaGen3EVSECurrentLimitV1Source, evidence TeslaGen3EVSESemanticEvidence) error {
	if p == nil {
		return ErrTeslaGen3EVSESemanticUnavailable
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.kernel == nil {
		return ErrTeslaGen3EVSESemanticUnavailable
	}
	if err := p.validate(source, evidence); err != nil {
		return err
	}
	inputDigest := p.inputDigest(source, evidence)
	if evidence.Sequence < p.sequence {
		return errors.New("tesla Gen3 EVSE semantic replay or collision")
	}
	if evidence.Sequence == p.sequence {
		if inputDigest == p.lastInputDigest {
			// The accepted input is already represented by the immutable kernel
			// snapshot.  Do not read a clock or construct a second batch: UP-02
			// requires this retry to be a no-op.
			return nil
		}
		return errors.New("tesla Gen3 EVSE semantic replay or collision")
	}
	if (p.sequence == 0 && evidence.Sequence != 1) || (p.sequence != 0 && (p.sequence == ^uint64(0) || evidence.Sequence != p.sequence+1)) {
		return errors.New("tesla Gen3 EVSE semantic sequence gap")
	}
	readClock, err := p.readClock()
	if err != nil {
		return err
	}
	scrapeEpoch := p.now()
	if scrapeEpoch.IsZero() {
		return errors.New("tesla Gen3 EVSE semantic scrape clock is unavailable")
	}
	if p.sequence != 0 && readClock < p.lastReadClock {
		return errors.New("tesla Gen3 EVSE read monotonic clock regressed")
	}
	receiptMono := semreg.MonotonicPoint{ClockEpochID: semreg.ClockEpochID(p.cfg.ClockEpoch), Nanoseconds: semreg.Uint64(strconv.FormatInt(evidence.MonotonicNS, 10))}
	evaluationMono, err := teslaGen3EVSEEvaluationMonotonic(receiptMono, evidence)
	if err != nil {
		return err
	}
	prometheusMono, err := teslaGen3EVSEPublicationScrapeMonotonic(evaluationMono, evidence.EvaluatedAt, scrapeEpoch)
	if err != nil {
		return err
	}
	if p.sequence != 0 {
		for _, floor := range []semreg.MonotonicPoint{p.evaluatedMonotonic, p.lastReadMonotonic} {
			if err := teslaGen3EVSEPublicationMonotonicNotBefore(receiptMono, floor); err != nil {
				return err
			}
			if err := teslaGen3EVSEPublicationMonotonicNotBefore(evaluationMono, floor); err != nil {
				return err
			}
		}
	}
	staged, err := p.kernel.Fork()
	if err != nil {
		return err
	}
	_, _, exists := staged.Current()
	batch, manifest, requested, dispositions, err := p.batch(source, evidence, p.current, exists, receiptMono, evaluationMono)
	if err != nil {
		return err
	}
	snapshot, _, err := staged.Apply(batch, evaluationMono)
	if err != nil {
		return err
	}
	expiresAt := teslaGen3EVSEAllocatedExpiry(source.Provisional, evidence, receiptMono)
	_, err = p.publicAt(snapshot, manifest, requested, dispositions, evidence.EvaluatedAt, evaluationMono, expiresAt)
	if err != nil {
		return err
	}
	highWater, err := teslaGen3EVSECandidateHighWater(p.candidateHighWater, batch.FactUpserts)
	if err != nil {
		return err
	}
	p.kernel, p.current, p.manifest, p.requested, p.dispositions, p.candidateHighWater = staged, snapshot, manifest, append([]projection.RequestedItem(nil), requested...), append([]projection.ProjectionDisposition(nil), dispositions...), highWater
	p.evaluatedAt, p.evaluatedMonotonic, p.lastReadAt, p.lastReadMonotonic, p.publishedReadClock, p.lastReadClock, p.allocatedExpiresAt, p.scrapeEpoch, p.prometheusMonotonic, p.sequence, p.lastInputDigest = evidence.EvaluatedAt, evaluationMono, evidence.EvaluatedAt, evaluationMono, readClock, readClock, expiresAt, scrapeEpoch, prometheusMono, evidence.Sequence, inputDigest
	return nil
}

// SemanticEVSECurrentAt reevaluates one detached accepted publication at the
// caller's single scrape instant. It deliberately does not call now, readClock,
// a provider, or Publish, so a Prometheus scrape cannot perform native I/O or
// advance publication/read state. A scrape captured immediately before a
// concurrent publication uses that publication's sealed scrape floor once.
func (p *TeslaGen3EVSESemanticPublication) SemanticEVSECurrentAt(at time.Time) (SemanticEVSECurrent, bool) {
	if p == nil || at.IsZero() {
		return SemanticEVSECurrent{}, false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.sequence == 0 || p.scrapeEpoch.IsZero() {
		return SemanticEVSECurrent{}, false
	}
	// The retained snapshot includes nested slices and pointers. Copy it while
	// holding the publication mutex so an output consumer cannot mutate the
	// accepted lifecycle through a returned SemanticEVSECurrent value.
	snapshot, err := teslaGen3EVSECloneSnapshot(p.current)
	if err != nil {
		return SemanticEVSECurrent{}, false
	}
	manifest := p.manifest
	requested := append([]projection.RequestedItem(nil), p.requested...)
	dispositions := append([]projection.ProjectionDisposition(nil), p.dispositions...)
	evaluatedAt := p.evaluatedAt
	evaluatedMonotonic := p.evaluatedMonotonic
	scrapeEpoch := p.scrapeEpoch
	var allocatedExpiresAt *semreg.MonotonicPoint
	if p.allocatedExpiresAt != nil {
		copy := *p.allocatedExpiresAt
		allocatedExpiresAt = &copy
	}

	// Preserve the original monotonic coordinate when it is available. A
	// pre-publication scrape cannot evaluate this newer immutable snapshot at a
	// negative age, so the one allowed floor retry is its sealed scrape instant.
	elapsed := at.Sub(scrapeEpoch)
	if elapsed < 0 {
		elapsed = 0
	}
	mono, err := teslaGen3EVSEReadMonotonic(evaluatedMonotonic, elapsed)
	if err != nil {
		return SemanticEVSECurrent{}, false
	}
	mono, err = teslaGen3EVSEAtLeastMonotonic(mono, p.prometheusMonotonic)
	if err != nil {
		return SemanticEVSECurrent{}, false
	}
	wall := at
	if wall.Before(evaluatedAt) {
		wall = evaluatedAt
	}
	current, err := teslaGen3EVSEPublicCurrentAt(snapshot, manifest, requested, dispositions, wall, mono, allocatedExpiresAt)
	if err != nil {
		return SemanticEVSECurrent{}, false
	}
	// The output floor is deliberately separate from lastReadMonotonic. It
	// serializes only Prometheus contexts and does not fence a later accepted
	// native publication with a new lifecycle generation.
	p.prometheusMonotonic = mono
	return current, true
}

// teslaGen3EVSECloneSnapshot follows the SemReg publication kernel's detached
// snapshot boundary. The accepted snapshot is already valid; retain the error
// path so a future non-serializable addition fails the passive scrape closed.
func teslaGen3EVSECloneSnapshot(snapshot semreg.Snapshot) (semreg.Snapshot, error) {
	raw, err := json.Marshal(snapshot)
	if err != nil {
		return semreg.Snapshot{}, err
	}
	var clone semreg.Snapshot
	if err := json.Unmarshal(raw, &clone); err != nil {
		return semreg.Snapshot{}, err
	}
	if err := clone.Validate(); err != nil {
		return semreg.Snapshot{}, err
	}
	return clone, nil
}

func (p *TeslaGen3EVSESemanticPublication) TeslaGen3EVSESemanticCurrent(context.Context) (any, error) {
	if p == nil {
		return nil, ErrTeslaGen3EVSESemanticUnavailable
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.sequence == 0 {
		return nil, ErrTeslaGen3EVSESemanticUnavailable
	}
	now := p.now()
	if now.Before(p.evaluatedAt) {
		// Wall-clock corrections cannot make a retained publication appear to
		// predate its own receipt.  Keep the sealed evaluation point until the
		// injected clock advances again; monotonic progress remains authoritative.
		now = p.evaluatedAt
	}
	if now.Before(p.lastReadAt) {
		// An external retained-record owner can provide wall-only timestamps.
		// Once a read has exposed an age, a wall-clock rollback must not make
		// the same provisional allocation younger without new native evidence.
		now = p.lastReadAt
	}
	readClock, err := p.readClock()
	if err != nil {
		return nil, err
	}
	if readClock < p.lastReadClock {
		return nil, errors.New("tesla Gen3 EVSE read monotonic clock regressed")
	}
	mono, err := p.readMonotonic(readClock)
	if err != nil {
		return nil, err
	}
	mono, err = teslaGen3EVSEAtLeastMonotonic(mono, p.lastReadMonotonic)
	if err != nil {
		return nil, err
	}
	// Re-evaluation applies the SemReg lifecycle to the immutable snapshot.
	// It performs no provider call and does not fabricate a replacement batch.
	view, err := p.publicAt(p.current, p.manifest, p.requested, p.dispositions, now, mono, p.allocatedExpiresAt)
	if err != nil {
		return nil, err
	}
	p.lastReadAt, p.lastReadMonotonic, p.lastReadClock = now, mono, readClock
	return view, nil
}

func (p *TeslaGen3EVSESemanticPublication) validate(s TeslaGen3EVSECurrentLimitV1Source, e TeslaGen3EVSESemanticEvidence) error {
	if e.ObservationID == "" || e.ObservedAt.IsZero() || e.EvaluatedAt.IsZero() || e.EvaluatedAt.Before(e.ObservedAt) || e.MonotonicNS < 0 || e.EvaluatedMonotonicNS < 0 || e.Sequence == 0 {
		return errors.New("tesla Gen3 EVSE semantic lifecycle is invalid")
	}
	if _, err := json.Marshal(e); err != nil {
		return errors.New("tesla Gen3 EVSE semantic lifecycle is not serializable")
	}
	if e.ObservedAt.Before(teslaGen3EVSEMinUnixNanoTime) || e.ObservedAt.After(teslaGen3EVSEMaxUnixNanoTime) || e.EvaluatedAt.Before(teslaGen3EVSEMinUnixNanoTime) || e.EvaluatedAt.After(teslaGen3EVSEMaxUnixNanoTime) {
		return errors.New("tesla Gen3 EVSE semantic lifecycle time is out of range")
	}
	if e.EvaluatedMonotonicNS == 0 && e.EvaluatedAt.Equal(e.ObservedAt) {
		e.EvaluatedMonotonicNS = e.MonotonicNS
	}
	if e.EvaluatedMonotonicNS == 0 && !e.EvaluatedAt.Equal(e.ObservedAt) {
		return errors.New("tesla Gen3 EVSE delayed evaluation monotonic clock is required")
	}
	if e.EvaluatedMonotonicNS < e.MonotonicNS {
		return errors.New("tesla Gen3 EVSE evaluation monotonic clock regressed")
	}
	if s.Persistent == nil || s.Persistent.OperationVersion() != modbusreg.TeslaGen3CurrentLimitOperationVersion24443 || len(s.Persistent.RequestPayload()) == 0 || len(s.Persistent.TerminalPayload()) == 0 {
		return errors.New("tesla Gen3 EVSE persistent evidence is invalid")
	}
	return nil
}

func (p *TeslaGen3EVSESemanticPublication) batch(s TeslaGen3EVSECurrentLimitV1Source, e TeslaGen3EVSESemanticEvidence, current semreg.Snapshot, exists bool, receiptMono, evaluationMono semreg.MonotonicPoint) (semreg.PublicationBatch, projection.ProjectionManifest, []projection.RequestedItem, []projection.ProjectionDisposition, error) {
	asset, source := semreg.AssetID(p.cfg.AssetID), semreg.SourceID(p.cfg.SourceID)
	epoch, generation := semreg.SourceEpochID(p.cfg.SourceEpoch), semreg.Uint64(strconv.FormatUint(p.cfg.DriverGeneration, 10))
	seq := semreg.Uint64(strconv.FormatUint(e.Sequence, 10))
	binding := semreg.NativeBindingID("binding:tesla-wc3:" + evseHash(p.cfg.AssetID, p.cfg.SourceID)[:32])
	receivedAt, evaluatedAt := evseWall(e.ObservedAt), evseWall(e.EvaluatedAt)
	persistentRecord := teslaGen3EVSEPersistentRecord(s.Persistent, e)
	provisionalRecord := teslaGen3EVSEProvisionalRecord(s.Provisional, e)
	evidence := evseEvidence("native.tesla.wc3.current_limit", teslaGen3EVSEPublicationEvidence{Persistent: persistentRecord, Provisional: provisionalRecord})
	activationEvidence := []semreg.EvidenceRef{evseEvidence("native.tesla.wc3.current_limit.persistent", persistentRecord)}
	registry := evseDigestEvidence("registry.tesla.wc3_24_44_3", modbusreg.TeslaGen3CurrentLimitOperationVersion24443)
	// MappingRevision is the kernel's monotonic label; the immutable accepted
	// docs commit is retained in the registry evidence digest below.
	manifest := projection.ProjectionManifest{TargetID: "target:gateway-semantic-evse", TargetVersion: "1.0.0", KernelVersion: semreg.ContractKernelV1, PackVersions: []semreg.PackRef{{ID: "helianthus.pack.evse", Version: "1.0.0"}}, MappingRevision: "1"}
	configured := p.candidate("evse.limit.configured_current", "evse.dimension.evse", p.cfg.EVSEID, s.Persistent.MaxOutputCurrentAmps(), 60*time.Second, 300*time.Second, current, binding, source, epoch, generation, evidence, receivedAt, receiptMono, evaluatedAt, evaluationMono)
	requested := []projection.RequestedItem{{Kind: projection.ItemFact, ItemID: "evse.limit.configured_current"}, {Kind: projection.ItemFact, ItemID: "evse.limit.allocated_current"}}
	dispositions := []projection.ProjectionDisposition{{Kind: projection.ItemFact, ItemID: "evse.limit.configured_current", Outcome: projection.ProjectionExact, SourceKeys: []semreg.FactKey{configured.Key}, Loss: []projection.LossDetail{}}}
	facts, withdrawals := []semreg.FactCandidate{configured}, []semreg.CandidateID{}
	if provisional, reason := p.provisional(s.Provisional, e); provisional != nil {
		activationEvidence = append(activationEvidence, evseEvidence("native.tesla.wc3.current_limit.provisional", *provisionalRecord))
		timeout := time.Duration(s.Provisional.LimitTimeoutSeconds()) * time.Second
		allocated := p.candidate("evse.limit.allocated_current", "evse.dimension.connector", p.cfg.ConnectorID, *provisional, timeout, timeout+time.Nanosecond, current, binding, source, epoch, generation, evidence, receivedAt, receiptMono, evaluatedAt, evaluationMono)
		facts = append(facts, allocated)
		dispositions = append(dispositions, projection.ProjectionDisposition{Kind: projection.ItemFact, ItemID: "evse.limit.allocated_current", Outcome: projection.ProjectionExact, SourceKeys: []semreg.FactKey{allocated.Key}, Loss: []projection.LossDetail{}})
	} else {
		if allocatedID, ok := teslaGen3EVSECandidateID(p.cfg.AssetID, binding, "evse.limit.allocated_current"); ok && teslaGen3EVSESnapshotHasCandidate(current, allocatedID) {
			withdrawals = append(withdrawals, allocatedID)
		}
		r := semreg.DefinitionID("withheld_provisional_" + reason)
		dispositions = append(dispositions, projection.ProjectionDisposition{Kind: projection.ItemFact, ItemID: "evse.limit.allocated_current", Outcome: projection.ProjectionWithheld, Reason: &r, SourceKeys: []semreg.FactKey{}, Loss: []projection.LossDetail{{Kind: projection.LossPolicy, SourceItems: []semreg.DefinitionID{"native.tesla.wc3.provisional_current_limit"}, Description: "allocated current withheld: " + reason}}})
	}
	sort.Slice(facts, func(i, j int) bool { return facts[i].CandidateID < facts[j].CandidateID })
	sort.Slice(requested, func(i, j int) bool { return requested[i].ItemID < requested[j].ItemID })
	sort.Slice(dispositions, func(i, j int) bool { return dispositions[i].ItemID < dispositions[j].ItemID })
	expected := semreg.Uint64("0")
	if exists {
		expected = current.Revisions.Semantic
	}
	batch := semreg.PublicationBatch{Contract: semreg.ContractKernelV1, BatchID: semreg.BatchID("batch:tesla-wc3:" + evseHash(e.ObservationID)[:32]), AssetID: asset, SourceID: source, SourceEpochID: epoch, DriverGeneration: generation, Sequence: seq, ExpectedSemanticRevision: expected, ObservedAt: receivedAt,
		SourceUpserts:       []semreg.SourceDescriptor{{SourceID: source, SourceEpochID: epoch, ProtocolID: "modbus_fc100", ProfileID: "tesla.wc3.24_44_3.current_limit", ProfileVersion: "24.44.3", RegistryEvidence: registry, StartedAt: receivedAt, State: semreg.SourceCurrent, Revision: seq}},
		BindingUpserts:      []semreg.NativeBinding{{BindingID: binding, AssetID: asset, SourceID: source, SourceEpochID: epoch, DriverGeneration: generation, NativeResource: registry, State: semreg.BindingCurrent, Revision: seq}},
		IdentityLinkUpserts: []semreg.IdentityLink{{AssetID: asset, BindingID: binding, State: semreg.LinkQualified, Basis: []semreg.EvidenceRef{evidence}, Revision: seq}}, FactUpserts: facts,
		ServiceUpserts:    []semreg.ServiceInstance{p.service("evse.service.evse", p.cfg.EVSEID, binding, asset, epoch, generation, seq), p.service("evse.service.connector", p.cfg.ConnectorID, binding, asset, epoch, generation, seq)},
		CapabilityUpserts: []semreg.CapabilityInstance{p.capability("evse.capability.read.evse", "evse.service.evse", binding, asset, epoch, generation, seq, activationEvidence), p.capability("evse.capability.read.connector", "evse.service.connector", binding, asset, epoch, generation, seq, activationEvidence)},
		SourceRetirements: []semreg.SourceEpochID{}, FactWithdrawals: withdrawals, ServiceWithdrawals: []semreg.ServiceInstanceID{}, CapabilityWithdrawals: []semreg.CapabilityInstanceID{}, GenerationFences: []semreg.GenerationFence{}}
	sort.Slice(batch.ServiceUpserts, func(i, j int) bool { return batch.ServiceUpserts[i].InstanceID < batch.ServiceUpserts[j].InstanceID })
	sort.Slice(batch.CapabilityUpserts, func(i, j int) bool {
		return batch.CapabilityUpserts[i].InstanceID < batch.CapabilityUpserts[j].InstanceID
	})
	if exists {
		batch.SourceUpserts, batch.BindingUpserts, batch.IdentityLinkUpserts, batch.ServiceUpserts = []semreg.SourceDescriptor{}, []semreg.NativeBinding{}, []semreg.IdentityLink{}, []semreg.ServiceInstance{}
	}
	digest, err := batch.ComputedDigest()
	if err != nil {
		return semreg.PublicationBatch{}, manifest, nil, nil, err
	}
	batch.BatchDigest = digest
	if err := batch.Validate(); err != nil {
		return semreg.PublicationBatch{}, manifest, nil, nil, fmt.Errorf("tesla Gen3 EVSE semantic batch: %w", err)
	}
	return batch, manifest, requested, dispositions, nil
}

func (p *TeslaGen3EVSESemanticPublication) provisional(v *modbusreg.TeslaGen3ProvisionalCurrentLimit, e TeslaGen3EVSESemanticEvidence) (*uint32, string) {
	if v == nil {
		return nil, "missing"
	}
	if v.OperationVersion() != modbusreg.TeslaGen3CurrentLimitOperationVersion24443 || len(v.SetRequestPayload()) == 0 || len(v.AckPayload()) == 0 || len(v.ReadbackRequestPayload()) == 0 || len(v.ReadbackTerminalPayload()) == 0 {
		return nil, "malformed_or_correlation_mismatched"
	}
	if v.InhibitCharging() {
		return nil, "inhibited"
	}
	timeout := v.LimitTimeoutSeconds()
	if timeout == 0 {
		return nil, "zero_timeout"
	}
	if timeout > 86399 {
		return nil, "timeout_out_of_range"
	}
	evaluated := e.EvaluatedMonotonicNS
	if evaluated == 0 && e.EvaluatedAt.Equal(e.ObservedAt) {
		evaluated = e.MonotonicNS
	}
	if evaluated < e.MonotonicNS || uint64(evaluated-e.MonotonicNS) >= uint64(timeout)*uint64(time.Second) {
		return nil, "expired"
	}
	value := v.LimitCurrentMaxAmps()
	return &value, ""
}

func teslaGen3EVSEPersistentRecord(value *modbusreg.TeslaGen3PersistentCurrentLimit, lifecycle TeslaGen3EVSESemanticEvidence) teslaGen3EVSEPersistentEvidence {
	return teslaGen3EVSEPersistentEvidence{
		OperationVersion:     value.OperationVersion(),
		MaxOutputCurrentAmps: value.MaxOutputCurrentAmps(),
		RequestPayload:       append([]byte(nil), value.RequestPayload()...),
		TerminalPayload:      append([]byte(nil), value.TerminalPayload()...),
		Lifecycle:            lifecycle,
	}
}

func teslaGen3EVSEProvisionalRecord(value *modbusreg.TeslaGen3ProvisionalCurrentLimit, lifecycle TeslaGen3EVSESemanticEvidence) *teslaGen3EVSEProvisionalEvidence {
	if value == nil {
		return nil
	}
	return &teslaGen3EVSEProvisionalEvidence{
		OperationVersion:        value.OperationVersion(),
		LimitCurrentMaxAmps:     value.LimitCurrentMaxAmps(),
		LimitTimeoutSeconds:     value.LimitTimeoutSeconds(),
		InhibitCharging:         value.InhibitCharging(),
		SetRequestPayload:       append([]byte(nil), value.SetRequestPayload()...),
		AckPayload:              append([]byte(nil), value.AckPayload()...),
		ReadbackRequestPayload:  append([]byte(nil), value.ReadbackRequestPayload()...),
		ReadbackTerminalPayload: append([]byte(nil), value.ReadbackTerminalPayload()...),
		Lifecycle:               lifecycle,
	}
}

func (p *TeslaGen3EVSESemanticPublication) candidate(id, dimension, value string, amps uint32, freshFor, retainFor time.Duration, current semreg.Snapshot, binding semreg.NativeBindingID, source semreg.SourceID, epoch semreg.SourceEpochID, generation semreg.Uint64, evidence semreg.EvidenceRef, receivedAt semreg.TimePoint, receiptMono semreg.MonotonicPoint, evaluatedAt semreg.TimePoint, evaluationMono semreg.MonotonicPoint) semreg.FactCandidate {
	key := semreg.FactKey{PackID: "helianthus.pack.evse", PackVersion: "1.0.0", FactID: semreg.DefinitionID(id), Dimensions: []semreg.Dimension{{ID: semreg.DefinitionID(dimension), Value: semreg.Value{Kind: semreg.ValueText, Text: &value}}}}
	v := semreg.Value{Kind: semreg.ValueQuantity, Quantity: &semreg.Quantity{Number: semreg.Decimal{Coefficient: strconv.FormatUint(uint64(amps), 10), Exponent10: 0}, Unit: "unit.ampere"}}
	h := evseHash(p.cfg.AssetID, string(binding), id)
	candidateID := semreg.CandidateID("candidate:tesla-wc3:" + h[:32])
	return semreg.FactCandidate{CandidateID: candidateID, Key: key, Value: &v, Quality: semreg.Quality{Assertion: semreg.AssertionObserved, Qualification: semreg.QualificationQualified, Promotion: semreg.PromotionPromoted, Validity: semreg.ValidityGood, Availability: semreg.AvailabilityAvailable, Freshness: semreg.FreshnessFresh, Reasons: []semreg.DefinitionID{}}, Times: semreg.Times{ReceivedAt: receivedAt, ReceiptMonotonic: receiptMono, EvaluatedAt: evaluatedAt, EvaluateMonotonic: evaluationMono}, FreshnessPolicy: semreg.FreshnessPolicy{PolicyID: "policy:tesla-wc3-native-receipt", Version: "1.0.0", FreshForNS: semreg.Uint64(strconv.FormatInt(freshFor.Nanoseconds(), 10)), RetainForNS: semreg.Uint64(strconv.FormatInt(retainFor.Nanoseconds(), 10)), MaxWallUncertaintyNS: "0"}, BindingID: &binding, SourceEpochID: &epoch, DriverGeneration: &generation, Origin: semreg.OriginRef{OriginID: semreg.OriginID("origin:tesla-wc3:" + h[:32]), Kind: semreg.OriginNativeObservation, SourceID: &source, SourceEpochID: &epoch, BindingID: &binding, Evidence: []semreg.EvidenceRef{evidence}}, Evidence: []semreg.EvidenceRef{evidence}, Revision: p.nextCandidateRevision(current, candidateID)}
}

func (p *TeslaGen3EVSESemanticPublication) service(id, dimension string, binding semreg.NativeBindingID, asset semreg.AssetID, epoch semreg.SourceEpochID, generation, revision semreg.Uint64) semreg.ServiceInstance {
	return semreg.ServiceInstance{InstanceID: semreg.ServiceInstanceID("service:tesla-wc3:" + evseHash(p.cfg.AssetID, id)[:32]), AssetID: asset, Definition: semreg.DefinitionRef{Pack: semreg.PackRef{ID: "helianthus.pack.evse", Version: "1.0.0"}, ID: semreg.DefinitionID(id), Version: "1.0.0"}, BindingID: binding, SourceEpochID: epoch, DriverGeneration: generation, Qualification: semreg.QualificationQualified, Availability: semreg.AvailabilityAvailable, Revision: revision}
}
func (p *TeslaGen3EVSESemanticPublication) capability(id, service string, binding semreg.NativeBindingID, asset semreg.AssetID, epoch semreg.SourceEpochID, generation, revision semreg.Uint64, activationEvidence []semreg.EvidenceRef) semreg.CapabilityInstance {
	return semreg.CapabilityInstance{InstanceID: semreg.CapabilityInstanceID("capability:tesla-wc3:" + evseHash(p.cfg.AssetID, id)[:32]), AssetID: asset, ServiceInstance: semreg.ServiceInstanceID("service:tesla-wc3:" + evseHash(p.cfg.AssetID, service)[:32]), Definition: semreg.DefinitionRef{Pack: semreg.PackRef{ID: "helianthus.pack.evse", Version: "1.0.0"}, ID: semreg.DefinitionID(id), Version: "1.0.0"}, BindingID: binding, SourceEpochID: epoch, DriverGeneration: generation, Qualification: semreg.QualificationQualified, Availability: semreg.AvailabilityAvailable, Constraints: []semreg.TypedField{}, ActivationEvidence: append([]semreg.EvidenceRef(nil), activationEvidence...), Revision: revision}
}

func (p *TeslaGen3EVSESemanticPublication) publicAt(snapshot semreg.Snapshot, manifest projection.ProjectionManifest, requested []projection.RequestedItem, dispositions []projection.ProjectionDisposition, evaluated time.Time, mono semreg.MonotonicPoint, allocatedExpiresAt *semreg.MonotonicPoint) (json.RawMessage, error) {
	current, err := teslaGen3EVSEPublicCurrentAt(snapshot, manifest, requested, dispositions, evaluated, mono, allocatedExpiresAt)
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(map[string]any{"snapshot": current.Snapshot, "evaluation": current.Evaluation, "selections": current.Selections, "projection": current.Projection})
	return json.RawMessage(b), err
}

func teslaGen3EVSEPublicCurrentAt(snapshot semreg.Snapshot, manifest projection.ProjectionManifest, requested []projection.RequestedItem, dispositions []projection.ProjectionDisposition, evaluated time.Time, mono semreg.MonotonicPoint, allocatedExpiresAt *semreg.MonotonicPoint) (SemanticEVSECurrent, error) {
	evaluation, err := semreg.EvaluateSnapshot(snapshot, semreg.EvaluationContext{EvaluatedAt: evseWall(evaluated), EvaluateMonotonic: mono})
	if err != nil {
		return SemanticEVSECurrent{}, err
	}
	publicDispositions := append([]projection.ProjectionDisposition(nil), dispositions...)
	if allocatedExpiresAt != nil && teslaGen3EVSEMonotonicAtOrAfter(mono, *allocatedExpiresAt) {
		for index := range publicDispositions {
			if publicDispositions[index].ItemID != "evse.limit.allocated_current" {
				continue
			}
			reason := semreg.DefinitionID("withheld_provisional_expired")
			publicDispositions[index] = projection.ProjectionDisposition{Kind: projection.ItemFact, ItemID: "evse.limit.allocated_current", Outcome: projection.ProjectionWithheld, Reason: &reason, SourceKeys: []semreg.FactKey{}, Loss: []projection.LossDetail{{Kind: projection.LossPolicy, SourceItems: []semreg.DefinitionID{"native.tesla.wc3.provisional_current_limit"}, Description: "allocated current withheld: expired"}}}
		}
	}
	report, err := projection.Project(snapshot, manifest, requested, publicDispositions, nil)
	if err != nil {
		return SemanticEVSECurrent{}, err
	}
	return SemanticEVSECurrent{Snapshot: snapshot, Evaluation: evaluation, Selections: []semreg.Selection{}, Projection: report}, nil
}

func teslaGen3EVSEReadMonotonic(base semreg.MonotonicPoint, elapsed time.Duration) (semreg.MonotonicPoint, error) {
	if elapsed < 0 {
		return semreg.MonotonicPoint{}, errors.New("tesla Gen3 EVSE scrape monotonic clock regressed")
	}
	baseNS, err := strconv.ParseUint(string(base.Nanoseconds), 10, 64)
	if err != nil {
		return semreg.MonotonicPoint{}, errors.New("tesla Gen3 EVSE publication monotonic clock is invalid")
	}
	delta := uint64(elapsed)
	if baseNS > ^uint64(0)-delta {
		return semreg.MonotonicPoint{}, errors.New("tesla Gen3 EVSE scrape monotonic clock overflows")
	}
	return semreg.MonotonicPoint{ClockEpochID: base.ClockEpochID, Nanoseconds: semreg.Uint64(strconv.FormatUint(baseNS+delta, 10))}, nil
}

// teslaGen3EVSEPublicationScrapeMonotonic carries immutable evidence age into
// an immediate scrape after delayed publication. A backward publication wall
// coordinate stays at the evidence floor and cannot make the record younger.
func teslaGen3EVSEPublicationScrapeMonotonic(evaluation semreg.MonotonicPoint, evaluatedAt, publishedAt time.Time) (semreg.MonotonicPoint, error) {
	if publishedAt.Before(evaluatedAt) {
		return evaluation, nil
	}
	return teslaGen3EVSEReadMonotonic(evaluation, publishedAt.Sub(evaluatedAt))
}

func (p *TeslaGen3EVSESemanticPublication) readMonotonic(readClock uint64) (semreg.MonotonicPoint, error) {
	if readClock < p.publishedReadClock {
		return semreg.MonotonicPoint{}, errors.New("tesla Gen3 EVSE read monotonic clock regressed")
	}
	base, err := strconv.ParseUint(string(p.evaluatedMonotonic.Nanoseconds), 10, 64)
	if err != nil {
		return semreg.MonotonicPoint{}, errors.New("tesla Gen3 EVSE publication monotonic clock is invalid")
	}
	delta := readClock - p.publishedReadClock
	if base > ^uint64(0)-delta {
		return semreg.MonotonicPoint{}, errors.New("tesla Gen3 EVSE read monotonic clock overflows")
	}
	return semreg.MonotonicPoint{ClockEpochID: p.evaluatedMonotonic.ClockEpochID, Nanoseconds: semreg.Uint64(strconv.FormatUint(base+delta, 10))}, nil
}

func teslaGen3EVSEEvaluationMonotonic(receipt semreg.MonotonicPoint, evidence TeslaGen3EVSESemanticEvidence) (semreg.MonotonicPoint, error) {
	base, err := strconv.ParseUint(string(receipt.Nanoseconds), 10, 64)
	if err != nil {
		return semreg.MonotonicPoint{}, errors.New("tesla Gen3 EVSE receipt monotonic clock is invalid")
	}
	evaluated := evidence.EvaluatedMonotonicNS
	if evaluated == 0 && evidence.EvaluatedAt.Equal(evidence.ObservedAt) {
		evaluated = evidence.MonotonicNS
	}
	if evaluated < evidence.MonotonicNS {
		return semreg.MonotonicPoint{}, errors.New("tesla Gen3 EVSE evaluation monotonic clock regressed")
	}
	if uint64(evaluated) < uint64(base) {
		return semreg.MonotonicPoint{}, errors.New("tesla Gen3 EVSE evaluation monotonic clock regressed")
	}
	return semreg.MonotonicPoint{ClockEpochID: receipt.ClockEpochID, Nanoseconds: semreg.Uint64(strconv.FormatInt(evaluated, 10))}, nil
}

func teslaGen3EVSEAtLeastMonotonic(candidate, floor semreg.MonotonicPoint) (semreg.MonotonicPoint, error) {
	if floor.ClockEpochID == "" {
		return candidate, nil
	}
	if candidate.ClockEpochID != floor.ClockEpochID {
		return semreg.MonotonicPoint{}, errors.New("tesla Gen3 EVSE read monotonic clock epoch changed")
	}
	candidateNS, err := strconv.ParseUint(string(candidate.Nanoseconds), 10, 64)
	if err != nil {
		return semreg.MonotonicPoint{}, errors.New("tesla Gen3 EVSE read monotonic clock is invalid")
	}
	floorNS, err := strconv.ParseUint(string(floor.Nanoseconds), 10, 64)
	if err != nil {
		return semreg.MonotonicPoint{}, errors.New("tesla Gen3 EVSE retained monotonic clock is invalid")
	}
	if candidateNS < floorNS {
		return floor, nil
	}
	return candidate, nil
}

func teslaGen3EVSEPublicationMonotonicNotBefore(value, floor semreg.MonotonicPoint) error {
	if floor.ClockEpochID == "" {
		return nil
	}
	if value.ClockEpochID != floor.ClockEpochID {
		return errors.New("tesla Gen3 EVSE publication monotonic clock epoch changed")
	}
	valueNS, err := strconv.ParseUint(string(value.Nanoseconds), 10, 64)
	if err != nil {
		return errors.New("tesla Gen3 EVSE publication monotonic clock is invalid")
	}
	floorNS, err := strconv.ParseUint(string(floor.Nanoseconds), 10, 64)
	if err != nil {
		return errors.New("tesla Gen3 EVSE retained publication monotonic clock is invalid")
	}
	if valueNS < floorNS {
		return errors.New("tesla Gen3 EVSE publication monotonic clock regressed")
	}
	return nil
}

func teslaGen3EVSECandidateID(asset string, binding semreg.NativeBindingID, fact string) (semreg.CandidateID, bool) {
	h := evseHash(asset, string(binding), fact)
	if len(h) < 32 {
		return "", false
	}
	return semreg.CandidateID("candidate:tesla-wc3:" + h[:32]), true
}

func teslaGen3EVSESnapshotHasCandidate(snapshot semreg.Snapshot, id semreg.CandidateID) bool {
	for _, envelope := range snapshot.Facts {
		for _, candidate := range envelope.Candidates {
			if candidate.CandidateID == id {
				return true
			}
		}
	}
	return false
}

func teslaGen3EVSEAllocatedExpiry(v *modbusreg.TeslaGen3ProvisionalCurrentLimit, evidence TeslaGen3EVSESemanticEvidence, receipt semreg.MonotonicPoint) *semreg.MonotonicPoint {
	if v == nil || v.OperationVersion() != modbusreg.TeslaGen3CurrentLimitOperationVersion24443 || len(v.SetRequestPayload()) == 0 || len(v.AckPayload()) == 0 || len(v.ReadbackRequestPayload()) == 0 || len(v.ReadbackTerminalPayload()) == 0 || v.InhibitCharging() || v.LimitTimeoutSeconds() == 0 || v.LimitTimeoutSeconds() > 86399 {
		return nil
	}
	evaluated := evidence.EvaluatedMonotonicNS
	if evaluated == 0 && evidence.EvaluatedAt.Equal(evidence.ObservedAt) {
		evaluated = evidence.MonotonicNS
	}
	if evaluated < evidence.MonotonicNS {
		return nil
	}
	base, err := strconv.ParseUint(string(receipt.Nanoseconds), 10, 64)
	if err != nil {
		return nil
	}
	timeout := uint64(v.LimitTimeoutSeconds()) * uint64(time.Second)
	if base > ^uint64(0)-timeout || uint64(evaluated) >= base+timeout {
		return nil
	}
	expires := semreg.MonotonicPoint{ClockEpochID: receipt.ClockEpochID, Nanoseconds: semreg.Uint64(strconv.FormatUint(base+timeout, 10))}
	return &expires
}

func teslaGen3EVSEMonotonicAtOrAfter(value, boundary semreg.MonotonicPoint) bool {
	if value.ClockEpochID != boundary.ClockEpochID {
		return true
	}
	current, currentErr := strconv.ParseUint(string(value.Nanoseconds), 10, 64)
	expires, expiryErr := strconv.ParseUint(string(boundary.Nanoseconds), 10, 64)
	return currentErr != nil || expiryErr != nil || current >= expires
}

func (p *TeslaGen3EVSESemanticPublication) inputDigest(source TeslaGen3EVSECurrentLimitV1Source, evidence TeslaGen3EVSESemanticEvidence) semreg.Digest {
	return evseEvidence("native.tesla.wc3.current_limit.publication_input", struct {
		DriverGeneration uint64                            `json:"driver_generation"`
		Persistent       teslaGen3EVSEPersistentEvidence   `json:"persistent"`
		Provisional      *teslaGen3EVSEProvisionalEvidence `json:"provisional,omitempty"`
	}{DriverGeneration: p.cfg.DriverGeneration, Persistent: teslaGen3EVSEPersistentRecord(source.Persistent, evidence), Provisional: teslaGen3EVSEProvisionalRecord(source.Provisional, evidence)}).Digest
}

func (p *TeslaGen3EVSESemanticPublication) nextCandidateRevision(snapshot semreg.Snapshot, id semreg.CandidateID) semreg.Uint64 {
	highest := uint64(0)
	if retained, ok := p.candidateHighWater[id]; ok {
		value, err := strconv.ParseUint(string(retained), 10, 64)
		if err != nil {
			return ""
		}
		highest = value
	}
	for _, envelope := range snapshot.Facts {
		for _, candidate := range envelope.Candidates {
			if candidate.CandidateID != id {
				continue
			}
			revision, err := strconv.ParseUint(string(candidate.Revision), 10, 64)
			if err != nil || revision == ^uint64(0) {
				return ""
			}
			if revision > highest {
				highest = revision
			}
		}
	}
	if highest == ^uint64(0) {
		return ""
	}
	return semreg.Uint64(strconv.FormatUint(highest+1, 10))
}

func teslaGen3EVSECandidateHighWater(current map[semreg.CandidateID]semreg.Uint64, candidates []semreg.FactCandidate) (map[semreg.CandidateID]semreg.Uint64, error) {
	next := make(map[semreg.CandidateID]semreg.Uint64, len(current)+len(candidates))
	for id, revision := range current {
		next[id] = revision
	}
	for _, candidate := range candidates {
		revision, err := strconv.ParseUint(string(candidate.Revision), 10, 64)
		if err != nil || revision == 0 {
			return nil, errors.New("tesla Gen3 EVSE candidate revision is invalid")
		}
		if retained, ok := next[candidate.CandidateID]; ok {
			retainedRevision, err := strconv.ParseUint(string(retained), 10, 64)
			if err != nil || revision < retainedRevision {
				return nil, errors.New("tesla Gen3 EVSE candidate revision regressed")
			}
		}
		next[candidate.CandidateID] = candidate.Revision
	}
	return next, nil
}
func evseWall(t time.Time) semreg.TimePoint {
	return semreg.TimePoint{UnixNanoseconds: semreg.Int64(strconv.FormatInt(t.UTC().UnixNano(), 10)), ClockID: "wall.utc", UncertaintyNS: "0"}
}
func evseHash(parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:])
}
func evseEvidence(kind semreg.DefinitionID, v any) semreg.EvidenceRef {
	b, _ := json.Marshal(v)
	return evseDigestEvidence(kind, string(b))
}
func evseDigestEvidence(kind semreg.DefinitionID, v string) semreg.EvidenceRef {
	return semreg.EvidenceRef{Owner: "helianthus.pack.evse", Kind: kind, Digest: semreg.Digest("sha256:" + evseHash(v)), Contract: semreg.ContractKernelV1, Access: semreg.EvidenceAccessAuthorized, Redaction: semreg.RedactionMetadataOnly}
}

var teslaGen3EVSESemanticProviders = struct {
	sync.RWMutex
	byServer map[*Server]TeslaGen3EVSESemanticProvider
}{byServer: make(map[*Server]TeslaGen3EVSESemanticProvider)}

func registerTeslaGen3EVSESemanticTool(server *Server, provider ModbusV1Provider) {
	p, ok := provider.(TeslaGen3EVSESemanticProvider)
	if !ok || p == nil {
		return
	}
	teslaGen3EVSESemanticProviders.Lock()
	teslaGen3EVSESemanticProviders.byServer[server] = p
	teslaGen3EVSESemanticProviders.Unlock()
	server.tools = append(server.tools, Tool{Name: SemanticV1EVSECurrentGetTool, Description: "Get one evaluated Tesla Gen3 EVSE SemReg current projection.", InputSchema: map[string]any{"type": "object", "properties": map[string]any{}, "additionalProperties": false}})
}
func (server *Server) handleTeslaGen3EVSESemanticCall(ctx context.Context, name string, args map[string]any) (map[string]any, bool) {
	if name != SemanticV1EVSECurrentGetTool {
		return nil, false
	}
	if len(args) != 0 {
		return callToolResultText(mustJSON(newModbusV1Envelope(nil, errors.New("invalid Tesla Gen3 EVSE semantic arguments"), false, "EVALUATED_SEMREG_PUBLICATION", "")), true), true
	}
	teslaGen3EVSESemanticProviders.RLock()
	p := teslaGen3EVSESemanticProviders.byServer[server]
	teslaGen3EVSESemanticProviders.RUnlock()
	if p == nil {
		return callToolResultText(mustJSON(newModbusV1Envelope(nil, ErrTeslaGen3EVSESemanticUnavailable, false, "EVALUATED_SEMREG_PUBLICATION", "")), true), true
	}
	d, e := p.TeslaGen3EVSESemanticCurrent(ctx)
	if e != nil {
		return callToolResultText(mustJSON(newModbusV1Envelope(nil, e, true, "EVALUATED_SEMREG_PUBLICATION", "")), true), true
	}
	timestamp, e := growattStorageEvaluatedTimestamp(d)
	if e != nil {
		return callToolResultText(mustJSON(newModbusV1Envelope(nil, e, true, "EVALUATED_SEMREG_PUBLICATION", "")), true), true
	}
	return callToolResultText(mustJSON(newModbusV1Envelope(d, nil, true, "EVALUATED_SEMREG_PUBLICATION", timestamp)), false), true
}

var _ = fmt.Sprintf
