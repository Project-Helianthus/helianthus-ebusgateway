package modbusadapter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"sync"
	"time"

	modbus "github.com/Project-Helianthus/helianthus-modbus"
	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/projection"
)

// Endpoint is the public Modbus TCP runtime surface used by the gateway.
// The gateway adapter deliberately adds no scheduler or provenance model.
type Endpoint interface {
	OpenConnection(net.Conn) (modbus.TCPConnectionHandle, error)
	EnqueueRead(modbus.TCPReadPlan) (modbus.TCPRequestHandle, error)
	Dispatch() (modbus.TCPDispatch, bool)
	Write(context.Context, modbus.TCPDispatch) (modbus.OwnerTransition, error)
	Read(context.Context, modbus.TCPConnectionHandle) (modbus.TCPReadBatch, error)
	Cancel(modbus.TCPRequestHandle) error
	Snapshot() modbus.TCPEndpointSnapshot
	Close() error
}

// Factory constructs the single endpoint owned by one adapter instance.
type Factory func(modbus.TCPEndpointConfig) (Endpoint, error)

// Dialer is injectable so integration tests can use an isolated fake peer.
type Dialer func(context.Context, string, string) (net.Conn, error)

// Config contains the already-validated bounded runtime configuration.
type Config struct {
	Enabled     bool
	Endpoint    modbus.TCPEndpointConfig
	DialTimeout time.Duration
}

// ReadPlan is a gateway-side request without socket ownership. The adapter
// injects its current opaque connection handle and preserves every other field.
type ReadPlan struct {
	UnitID             byte
	AuthorizationScope string
	PollGeneration     uint64
	DeadlineIdentity   uint64
	Timeout            time.Duration
	Reads              []modbus.TCPLogicalRead
}

// Adapter owns exactly one endpoint and its active connection generation.
type Adapter struct {
	endpoint   Endpoint
	connection modbus.TCPConnectionHandle
	source     *modbus.RuntimeAcquisitionSource
	config     Config
	dial       Dialer

	closeOnce       sync.Once
	closeErr        error
	executeMu       sync.Mutex
	connectionMu    sync.RWMutex
	closed          bool
	lastRequest     modbus.TCPRequestHandle
	profileMu       sync.RWMutex
	profiles        map[string]ProfileObservationRecord
	qualifications  map[string]sunSpecQualificationRecord
	refreshEvidence map[string]sunSpecQualificationRecord
	refreshOrder    []string
	semanticPV      *pvPublicationCore
	pvSourceEpoch   semreg.SourceEpochID
	startedWall     time.Time
	startedMono     time.Time
	wallNow         func() time.Time
	monotonicNow    func() time.Time
}

const maxRetainedProfileObservations = 32

// Refresh evidence has its own bounded lifecycle. Terminal qualification
// observations retain their existing capacity and identity contract.
const maxRetainedSunSpecRefreshEvidence = 32

// ProfileObservationRecord retains one exact registry-owned observation and
// the evidence labels supplied by its future detector/poller owner.
type ProfileObservationRecord struct {
	Observation        modbusreg.Observation
	DetectionEvidence  []string
	ActivationEvidence []string
}

type sunSpecQualificationRecord struct {
	observation modbusreg.SunSpecQualificationObservation
	encoded     []byte
}

// SemanticPVCurrent is the immutable, evaluated SemReg projection shared by
// every enabled consumer. It contains no transport operation authority.
type SemanticPVCurrent struct {
	Snapshot   semreg.Snapshot
	Canonical  []byte
	Evaluation semreg.EvaluationView
	Selections []semreg.Selection
	Projection projection.ProjectionReport
}

// Start constructs and connects one endpoint. Disabled configuration is inert.
func Start(
	ctx context.Context,
	config Config,
	dial Dialer,
	factory Factory,
) (*Adapter, error) {
	if !config.Enabled {
		return nil, nil
	}
	if ctx == nil || dial == nil || factory == nil || config.DialTimeout <= 0 {
		return nil, errors.New("enabled Modbus TCP adapter configuration is incomplete")
	}
	semanticPV, err := newPVPublicationCore()
	if err != nil {
		return nil, fmt.Errorf("construct SemReg PV publication: %w", err)
	}
	address, err := dialAddress(config.Endpoint.Endpoint)
	if err != nil {
		return nil, err
	}
	endpoint, err := factory(config.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("construct Modbus TCP endpoint: %w", err)
	}
	if endpoint == nil {
		return nil, errors.New("construct Modbus TCP endpoint: factory returned nil")
	}

	dialCtx, cancel := context.WithTimeout(ctx, config.DialTimeout)
	defer cancel()
	connection, err := dial(dialCtx, "tcp", address)
	if err != nil {
		return nil, errors.Join(
			fmt.Errorf("dial Modbus TCP endpoint: %w", err),
			endpoint.Close(),
		)
	}
	handle, err := endpoint.OpenConnection(connection)
	if err != nil {
		return nil, errors.Join(
			fmt.Errorf("open Modbus TCP endpoint: %w", err),
			connection.Close(),
			endpoint.Close(),
		)
	}
	processStarted := time.Now()
	startedWall := processStarted.UTC()
	epochHash := pvCoreHash("semantic-pv-source-epoch", []byte(config.Endpoint.Endpoint+"\x00"+startedWall.Format(time.RFC3339Nano)))
	return &Adapter{
		endpoint:        endpoint,
		connection:      handle,
		source:          config.Endpoint.RuntimeAcquisitionSource,
		config:          config,
		dial:            dial,
		profiles:        make(map[string]ProfileObservationRecord),
		qualifications:  make(map[string]sunSpecQualificationRecord),
		refreshEvidence: make(map[string]sunSpecQualificationRecord),
		semanticPV:      semanticPV,
		pvSourceEpoch:   semreg.SourceEpochID("source-epoch:semantic-pv:" + epochHash[:32]),
		startedWall:     startedWall,
		startedMono:     processStarted,
		wallNow:         time.Now,
		monotonicNow:    time.Now,
	}, nil
}

// ValidateTCPEndpoint applies the endpoint grammar used by Start without
// opening a connection.
func ValidateTCPEndpoint(endpoint string) error {
	_, err := dialAddress(endpoint)
	return err
}

func dialAddress(endpoint string) (string, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme != "tcp" || parsed.Host == "" ||
		parsed.User != nil || parsed.Path != "" || parsed.RawQuery != "" ||
		parsed.Fragment != "" {
		return "", errors.New("invalid Modbus TCP endpoint")
	}
	if _, _, err := net.SplitHostPort(parsed.Host); err != nil {
		return "", errors.New("invalid Modbus TCP endpoint")
	}
	return parsed.Host, nil
}

// EnqueueRead admits a logical request without exposing connection ownership.
func (adapter *Adapter) EnqueueRead(plan ReadPlan) (modbus.TCPRequestHandle, error) {
	if adapter == nil || adapter.endpoint == nil {
		return modbus.TCPRequestHandle{}, errors.New("modbus TCP adapter unavailable")
	}
	adapter.connectionMu.Lock()
	defer adapter.connectionMu.Unlock()
	if adapter.closed {
		return modbus.TCPRequestHandle{}, errors.New("modbus TCP adapter is closed")
	}
	handle, err := adapter.endpoint.EnqueueRead(modbus.TCPReadPlan{
		Connection:         adapter.connection,
		UnitID:             plan.UnitID,
		AuthorizationScope: plan.AuthorizationScope,
		PollGeneration:     plan.PollGeneration,
		DeadlineIdentity:   plan.DeadlineIdentity,
		Timeout:            plan.Timeout,
		Reads:              append([]modbus.TCPLogicalRead(nil), plan.Reads...),
	})
	if err == nil {
		adapter.lastRequest = handle
	}
	return handle, err
}

// Dispatch delegates deterministic fair service to helianthus-modbus.
func (adapter *Adapter) Dispatch() (modbus.TCPDispatch, bool) {
	if adapter == nil || adapter.endpoint == nil {
		return modbus.TCPDispatch{}, false
	}
	return adapter.endpoint.Dispatch()
}

// Write crosses the public runtime's owned transport boundary.
func (adapter *Adapter) Write(
	ctx context.Context,
	dispatch modbus.TCPDispatch,
) (modbus.OwnerTransition, error) {
	if adapter == nil || adapter.endpoint == nil {
		return modbus.OwnerTransition{}, errors.New("modbus TCP adapter unavailable")
	}
	return adapter.endpoint.Write(ctx, dispatch)
}

// Read returns the public runtime batch unchanged.
func (adapter *Adapter) Read(ctx context.Context) (modbus.TCPReadBatch, error) {
	if adapter == nil || adapter.endpoint == nil {
		return modbus.TCPReadBatch{}, errors.New("modbus TCP adapter unavailable")
	}
	adapter.connectionMu.RLock()
	defer adapter.connectionMu.RUnlock()
	if adapter.closed {
		return modbus.TCPReadBatch{}, errors.New("modbus TCP adapter is closed")
	}
	return adapter.endpoint.Read(ctx, adapter.connection)
}

type reconnectEndpoint interface {
	CloseConnection(modbus.TCPConnectionHandle) error
	WaitReconnect(context.Context, modbus.TCPRequestHandle, modbus.DelayWaiter) error
}

type contextDelayWaiter struct{}

func (contextDelayWaiter) Wait(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// Reconnect retires the old endpoint-owned generation before a fresh dial.
// It never moves an admitted request across generations; endpoint-owned
// backoff is consumed only as recovery authorization for the next poll.
func (adapter *Adapter) Reconnect(ctx context.Context) error {
	if adapter == nil || adapter.endpoint == nil || ctx == nil {
		return errors.New("modbus TCP adapter unavailable")
	}
	adapter.executeMu.Lock()
	defer adapter.executeMu.Unlock()
	_, err := adapter.reconnectLocked(ctx, false)
	return err
}

// reconnectLocked checks owner recovery state and replaces the connection while
// executeMu is held. requireOwnerAuthorization makes the check and replacement
// one atomic adapter operation for caller-triggered recovery.
func (adapter *Adapter) reconnectLocked(ctx context.Context, requireOwnerAuthorization bool) (bool, error) {
	reconnector, ok := adapter.endpoint.(reconnectEndpoint)
	if !ok {
		return false, errors.New("modbus TCP endpoint does not support reconnect")
	}
	adapter.connectionMu.Lock()
	defer adapter.connectionMu.Unlock()
	if adapter.closed {
		return false, errors.New("modbus TCP adapter is closed")
	}
	oldConnection, request := adapter.connection, adapter.lastRequest
	beforeRetirement := adapter.endpoint.Snapshot()
	if requireOwnerAuthorization && !beforeRetirement.ReconnectRequired {
		return false, nil
	}
	if err := reconnector.CloseConnection(oldConnection); err != nil && !beforeRetirement.ReconnectRequired {
		return false, fmt.Errorf("retire Modbus TCP connection: %w", err)
	}
	adapter.lastRequest = modbus.TCPRequestHandle{}
	// A failed socket may already have retired its handle. Only the endpoint's
	// public state authorizes consuming request-bound reconnect backoff.
	if request.RequestID() != 0 && beforeRetirement.ReconnectRequired {
		if err := reconnector.WaitReconnect(ctx, request, contextDelayWaiter{}); err != nil {
			return false, fmt.Errorf("wait endpoint reconnect backoff: %w", err)
		}
	}
	address, err := dialAddress(adapter.config.Endpoint.Endpoint)
	if err != nil {
		return false, err
	}
	dialCtx, cancel := context.WithTimeout(ctx, adapter.config.DialTimeout)
	defer cancel()
	connection, err := adapter.dial(dialCtx, "tcp", address)
	if err != nil {
		return false, fmt.Errorf("redial Modbus TCP endpoint: %w", err)
	}
	handle, err := adapter.endpoint.OpenConnection(connection)
	if err != nil {
		return false, errors.Join(fmt.Errorf("reopen Modbus TCP endpoint: %w", err), connection.Close())
	}
	adapter.connection = handle
	return true, nil
}

// ExecuteRead serializes one bounded request through the endpoint owner. It
// preserves the runtime batch unchanged and never interprets register values.
func (adapter *Adapter) ExecuteRead(ctx context.Context, plan ReadPlan) (modbus.TCPReadBatch, error) {
	if adapter == nil || adapter.endpoint == nil {
		return modbus.TCPReadBatch{}, errors.New("modbus TCP adapter unavailable")
	}
	adapter.executeMu.Lock()
	defer adapter.executeMu.Unlock()
	return adapter.executeReadLocked(ctx, plan)
}

// ExecuteReadWithReconnect owns one bounded read operation across a retryable
// transport failure. The owner check, connection replacement, and single retry
// remain serialized with every other adapter execution.
func (adapter *Adapter) ExecuteReadWithReconnect(ctx context.Context, plan ReadPlan) (modbus.TCPReadBatch, error) {
	if adapter == nil || adapter.endpoint == nil {
		return modbus.TCPReadBatch{}, errors.New("modbus TCP adapter unavailable")
	}
	adapter.executeMu.Lock()
	defer adapter.executeMu.Unlock()
	batch, err := adapter.executeReadLocked(ctx, plan)
	if err == nil {
		return batch, nil
	}
	reconnected, reconnectErr := adapter.reconnectLocked(ctx, true)
	if reconnectErr != nil {
		return modbus.TCPReadBatch{}, reconnectErr
	}
	if !reconnected {
		return modbus.TCPReadBatch{}, err
	}
	return adapter.executeReadLocked(ctx, plan)
}

func (adapter *Adapter) executeReadLocked(ctx context.Context, plan ReadPlan) (modbus.TCPReadBatch, error) {
	handle, err := adapter.EnqueueRead(plan)
	if err != nil {
		return modbus.TCPReadBatch{}, err
	}
	dispatch, ok := adapter.Dispatch()
	if !ok || dispatch.RequestID() != handle.RequestID() {
		_ = adapter.Cancel(handle)
		return modbus.TCPReadBatch{}, errors.New("modbus TCP endpoint did not dispatch the admitted request")
	}
	if _, err := adapter.Write(ctx, dispatch); err != nil {
		return modbus.TCPReadBatch{}, err
	}
	return adapter.Read(ctx)
}

// RecordProfileObservation retains a bounded exact replay record. Profile
// decoding and evidence production remain owned outside the adapter.
func (adapter *Adapter) RecordProfileObservation(record ProfileObservationRecord) error {
	if adapter == nil {
		return errors.New("modbus TCP adapter unavailable")
	}
	spec := record.Observation.Spec()
	if spec.ProfileID == "" || spec.SampleID == "" {
		return errors.New("profile observation identity is incomplete")
	}
	key := spec.ProfileID + "\x00" + spec.SampleID
	adapter.profileMu.Lock()
	defer adapter.profileMu.Unlock()
	if _, exists := adapter.profiles[key]; !exists && len(adapter.profiles)+len(adapter.qualifications) >= maxRetainedProfileObservations {
		return errors.New("profile observation retention limit reached")
	}
	record.DetectionEvidence = append([]string(nil), record.DetectionEvidence...)
	record.ActivationEvidence = append([]string(nil), record.ActivationEvidence...)
	adapter.profiles[key] = record
	return nil
}

// RecordSunSpecQualificationObservation first proves deterministic
// serialization, then retains one immutable terminal registry observation.
// The shared bound covers legacy profile observations as well.
func (adapter *Adapter) RecordSunSpecQualificationObservation(observation modbusreg.SunSpecQualificationObservation) error {
	if adapter == nil {
		return errors.New("modbus TCP adapter unavailable")
	}
	adapter.connectionMu.RLock()
	defer adapter.connectionMu.RUnlock()
	if adapter.closed {
		return errors.New("modbus TCP adapter is closed")
	}
	encoded, err := json.Marshal(observation)
	if err != nil {
		return fmt.Errorf("serialize SunSpec qualification observation: %w", err)
	}
	capability, sampleID := observation.Capability().ProfileID(), observation.SampleID()
	if capability == "" || sampleID == "" {
		return errors.New("SunSpec qualification observation identity is incomplete")
	}
	key := capability + "\x00" + sampleID
	adapter.profileMu.Lock()
	defer adapter.profileMu.Unlock()
	if existing, exists := adapter.qualifications[key]; exists {
		if !bytes.Equal(existing.encoded, encoded) {
			return errors.New("SunSpec qualification observation identity collision")
		}
		return nil
	}
	if len(adapter.profiles)+len(adapter.qualifications) >= maxRetainedProfileObservations {
		return errors.New("profile observation retention limit reached")
	}
	if err := adapter.publishSemanticPV(observation); err != nil {
		return err
	}
	adapter.qualifications[key] = sunSpecQualificationRecord{
		observation: observation, encoded: append([]byte(nil), encoded...),
	}
	return nil
}

// PublishSunSpecCurrent replaces only the semantic current slot for an asset.
// It neither retains nor mutates terminal qualification evidence.
func (adapter *Adapter) PublishSunSpecCurrent(observation modbusreg.SunSpecQualificationObservation) error {
	if adapter == nil {
		return errors.New("modbus TCP adapter unavailable")
	}
	adapter.connectionMu.RLock()
	defer adapter.connectionMu.RUnlock()
	if adapter.closed {
		return errors.New("modbus TCP adapter is closed")
	}
	adapter.profileMu.Lock()
	defer adapter.profileMu.Unlock()
	return adapter.publishSemanticPV(observation)
}

// RecordSunSpecCurrentObservation retains immutable native evidence for a
// publishable refresh. It commits the evidence only with a successful SemReg
// publication and evicts the oldest refresh evidence deterministically.
func (adapter *Adapter) RecordSunSpecCurrentObservation(observation modbusreg.SunSpecQualificationObservation) error {
	if adapter == nil {
		return errors.New("modbus TCP adapter unavailable")
	}
	adapter.connectionMu.RLock()
	defer adapter.connectionMu.RUnlock()
	if adapter.closed {
		return errors.New("modbus TCP adapter is closed")
	}
	encoded, err := json.Marshal(observation)
	if err != nil {
		return fmt.Errorf("serialize SunSpec current observation: %w", err)
	}
	capability, sampleID := observation.Capability().ProfileID(), observation.SampleID()
	if capability == "" || sampleID == "" {
		return errors.New("SunSpec current observation identity is incomplete")
	}
	key := capability + "\x00" + sampleID
	adapter.profileMu.Lock()
	defer adapter.profileMu.Unlock()
	if existing, ok := adapter.refreshEvidence[key]; ok {
		if !bytes.Equal(existing.encoded, encoded) {
			return errors.New("SunSpec current observation identity collision")
		}
		return adapter.publishSemanticPV(observation)
	}
	var evictedKey string
	var evicted sunSpecQualificationRecord
	if len(adapter.refreshOrder) == maxRetainedSunSpecRefreshEvidence {
		referenced, err := adapter.currentSunSpecObservationDigestsLocked(observation)
		if err != nil {
			return err
		}
		for _, candidate := range adapter.refreshOrder {
			record := adapter.refreshEvidence[candidate]
			if !referenced[sunSpecObservationDigest(record.encoded)] {
				evictedKey, evicted = candidate, record
				break
			}
		}
		if evictedKey == "" {
			return errors.New("SunSpec current evidence retention capacity would exceed bound")
		}
		delete(adapter.refreshEvidence, evictedKey)
		adapter.refreshOrder = removeSunSpecRefreshKey(adapter.refreshOrder, evictedKey)
	}
	adapter.refreshEvidence[key] = sunSpecQualificationRecord{
		observation: observation,
		encoded:     append([]byte(nil), encoded...),
	}
	adapter.refreshOrder = append(adapter.refreshOrder, key)
	if err := adapter.publishSemanticPV(observation); err != nil {
		delete(adapter.refreshEvidence, key)
		adapter.refreshOrder = adapter.refreshOrder[:len(adapter.refreshOrder)-1]
		if evictedKey != "" {
			adapter.refreshEvidence[evictedKey] = evicted
			adapter.refreshOrder = append([]string{evictedKey}, adapter.refreshOrder...)
		}
		return err
	}
	// A successful replacement can retire old candidate evidence. Pruning only
	// after commit preserves every digest referenced by the newly public view.
	if referenced, err := adapter.currentSunSpecObservationDigestsLocked(observation); err == nil {
		adapter.pruneSunSpecRefreshEvidenceLocked(referenced)
	}
	return nil
}

func sunSpecObservationDigest(encoded []byte) string {
	return "sha256:" + pvCoreHash("pv-observation", encoded)
}

func removeSunSpecRefreshKey(order []string, key string) []string {
	for index, candidate := range order {
		if candidate == key {
			copy(order[index:], order[index+1:])
			order[len(order)-1] = ""
			return order[:len(order)-1]
		}
	}
	return order
}

func (adapter *Adapter) pruneSunSpecRefreshEvidenceLocked(referenced map[string]bool) {
	kept := adapter.refreshOrder[:0]
	for _, key := range adapter.refreshOrder {
		record := adapter.refreshEvidence[key]
		if referenced[sunSpecObservationDigest(record.encoded)] {
			kept = append(kept, key)
		} else {
			delete(adapter.refreshEvidence, key)
		}
	}
	adapter.refreshOrder = kept
}

func (adapter *Adapter) currentSunSpecObservationDigestsLocked(observation modbusreg.SunSpecQualificationObservation) (map[string]bool, error) {
	identity, _, err := resolvePVPublicationIdentity(observation)
	if err != nil {
		return nil, err
	}
	view, err := adapter.semanticPV.currentForValidation(semreg.AssetID("pv-asset-" + pvCoreRawHash(identity)[:32]))
	if err != nil {
		return map[string]bool{}, nil
	}
	refs := make(map[string]bool)
	collect := func(evidence []semreg.EvidenceRef) {
		for _, ref := range evidence {
			if ref.Kind == "sunspec.qualification_observation" {
				refs[string(ref.Digest)] = true
			}
		}
	}
	for _, envelope := range view.snapshot.Facts {
		for _, candidate := range envelope.Candidates {
			collect(candidate.Evidence)
			collect(candidate.Origin.Evidence)
		}
	}
	for _, retained := range view.snapshot.Retained {
		collect(retained.Candidate.Evidence)
		collect(retained.Candidate.Origin.Evidence)
	}
	for _, capability := range view.snapshot.Capabilities {
		collect(capability.ActivationEvidence)
	}
	for _, fence := range view.snapshot.Fences {
		collect(fence.Evidence)
	}
	return refs, nil
}

func (adapter *Adapter) publishSemanticPV(observation modbusreg.SunSpecQualificationObservation) error {
	if adapter.semanticPV == nil {
		return errors.New("SemReg PV publication unavailable")
	}
	context, err := adapter.semanticPVReadContext()
	if err != nil {
		return err
	}
	lifecycle := pvPublicationLifecycle{
		sourceEpochID: adapter.pvSourceEpoch, driverGeneration: "1",
		sourceStartedAt: pvPublicationWall(adapter.startedWall), receivedAt: context.EvaluatedAt,
		receiptMonotonic: context.EvaluateMonotonic, evaluatedAt: context.EvaluatedAt,
		evaluateMonotonic: context.EvaluateMonotonic,
	}
	draft, err := buildPVPublicationDraft(observation, lifecycle)
	if err != nil {
		return fmt.Errorf("build SemReg PV publication: %w", err)
	}
	if _, err := adapter.semanticPV.ingest(draft); err != nil {
		return fmt.Errorf("publish SemReg PV observation: %w", err)
	}
	return nil
}

// SemanticPVCurrentByAsset returns the one evaluated SemReg projection. It
// never triggers Modbus I/O, republishes, or exposes write authority.
func (adapter *Adapter) SemanticPVCurrentByAsset(assetRef string) (SemanticPVCurrent, bool) {
	if adapter == nil || assetRef == "" || adapter.semanticPV == nil {
		return SemanticPVCurrent{}, false
	}
	context, err := adapter.semanticPVReadContext()
	if err != nil {
		return SemanticPVCurrent{}, false
	}
	view, err := adapter.semanticPV.publicViewAt(semreg.AssetID(assetRef), context)
	if err != nil {
		return SemanticPVCurrent{}, false
	}
	return SemanticPVCurrent{Snapshot: view.snapshot, Canonical: view.canonical, Evaluation: view.evaluation, Selections: view.selections, Projection: view.projection}, true
}

func (adapter *Adapter) semanticPVReadContext() (semreg.EvaluationContext, error) {
	if adapter == nil || adapter.startedWall.IsZero() || adapter.startedMono.IsZero() {
		return semreg.EvaluationContext{}, errors.New("SemReg PV publication clock is unavailable")
	}
	wallClock, monotonicClock := time.Now, time.Now
	if adapter.wallNow != nil {
		wallClock = adapter.wallNow
	}
	if adapter.monotonicNow != nil {
		monotonicClock = adapter.monotonicNow
	}
	wall := wallClock().UTC()
	if wall.Before(adapter.startedWall) {
		wall = adapter.startedWall
	}
	elapsed := monotonicClock().Sub(adapter.startedMono)
	if elapsed < 0 {
		return semreg.EvaluationContext{}, errors.New("SemReg PV publication clock is invalid")
	}
	return semreg.EvaluationContext{EvaluatedAt: pvPublicationWall(wall), EvaluateMonotonic: pvPublicationMonotonic(elapsed)}, nil
}

func (adapter *Adapter) SemanticPVCurrent(profileID, sampleID string) (SemanticPVCurrent, bool) {
	if adapter == nil || profileID != modbusreg.SunSpecThreePhaseMonitoringCapabilityID {
		return SemanticPVCurrent{}, false
	}
	observation, _, ok := adapter.SunSpecQualificationObservation(profileID, sampleID)
	if !ok {
		return SemanticPVCurrent{}, false
	}
	identity, _, err := resolvePVPublicationIdentity(observation)
	if err != nil {
		return SemanticPVCurrent{}, false
	}
	return adapter.SemanticPVCurrentByAsset("pv-asset-" + pvCoreRawHash(identity)[:32])
}

// ProfileObservation returns one immutable retained sample by exact identity.
func (adapter *Adapter) ProfileObservation(profileID, sampleID string) (ProfileObservationRecord, bool) {
	if adapter == nil {
		return ProfileObservationRecord{}, false
	}
	adapter.profileMu.RLock()
	defer adapter.profileMu.RUnlock()
	record, ok := adapter.profiles[profileID+"\x00"+sampleID]
	if !ok {
		return ProfileObservationRecord{}, false
	}
	record.DetectionEvidence = append([]string(nil), record.DetectionEvidence...)
	record.ActivationEvidence = append([]string(nil), record.ActivationEvidence...)
	return record, true
}

// SunSpecQualificationObservation returns a detached immutable terminal
// qualification and the serialization proven before it was retained.
func (adapter *Adapter) SunSpecQualificationObservation(profileID, sampleID string) (modbusreg.SunSpecQualificationObservation, []byte, bool) {
	if adapter == nil {
		return modbusreg.SunSpecQualificationObservation{}, nil, false
	}
	adapter.profileMu.RLock()
	defer adapter.profileMu.RUnlock()
	record, ok := adapter.qualifications[profileID+"\x00"+sampleID]
	if !ok {
		record, ok = adapter.refreshEvidence[profileID+"\x00"+sampleID]
	}
	if !ok {
		return modbusreg.SunSpecQualificationObservation{}, nil, false
	}
	return record.observation, append([]byte(nil), record.encoded...), true
}

// Cancel delegates cancellation to the endpoint owner.
func (adapter *Adapter) Cancel(handle modbus.TCPRequestHandle) error {
	if adapter == nil || adapter.endpoint == nil {
		return errors.New("modbus TCP adapter unavailable")
	}
	return adapter.endpoint.Cancel(handle)
}

// Snapshot exposes only the public bounded endpoint health snapshot.
func (adapter *Adapter) Snapshot() modbus.TCPEndpointSnapshot {
	if adapter == nil || adapter.endpoint == nil {
		return modbus.TCPEndpointSnapshot{}
	}
	return adapter.endpoint.Snapshot()
}

// RuntimeAcquisitionSource returns the source owner attached to endpoint views.
func (adapter *Adapter) RuntimeAcquisitionSource() *modbus.RuntimeAcquisitionSource {
	if adapter == nil {
		return nil
	}
	return adapter.source
}

// Close retires all work and the socket exactly once.
func (adapter *Adapter) Close() error {
	if adapter == nil || adapter.endpoint == nil {
		return nil
	}
	adapter.closeOnce.Do(func() {
		adapter.connectionMu.Lock()
		adapter.closed = true
		adapter.connectionMu.Unlock()
		adapter.closeErr = adapter.endpoint.Close()
	})
	return adapter.closeErr
}
