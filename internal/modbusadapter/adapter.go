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

	pv "github.com/Project-Helianthus/helianthus-ebusreg/pv"
	modbus "github.com/Project-Helianthus/helianthus-modbus"
	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
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

	closeOnce      sync.Once
	closeErr       error
	executeMu      sync.Mutex
	connectionMu   sync.RWMutex
	closed         bool
	lastRequest    modbus.TCPRequestHandle
	profileMu      sync.RWMutex
	profiles       map[string]ProfileObservationRecord
	qualifications map[string]sunSpecQualificationRecord
	currentPV      map[string]canonicalPVCurrentRecord
	canonicalPV    *CanonicalPVMapper
	started        time.Time
}

const maxRetainedProfileObservations = 32

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
	canonical   pv.Snapshot
	producedAt  time.Time
}

// canonicalPVCurrentRecord is the replaceable semantic publication for one
// asset. It is deliberately independent from retained qualification evidence.
type canonicalPVCurrentRecord struct {
	canonical  pv.Snapshot
	producedAt time.Time
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
	canonicalMapper, err := NewCanonicalPVMapper()
	if err != nil {
		return nil, fmt.Errorf("construct canonical PV mapper: %w", err)
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
	return &Adapter{
		endpoint:       endpoint,
		connection:     handle,
		source:         config.Endpoint.RuntimeAcquisitionSource,
		config:         config,
		dial:           dial,
		profiles:       make(map[string]ProfileObservationRecord),
		qualifications: make(map[string]sunSpecQualificationRecord),
		currentPV:      make(map[string]canonicalPVCurrentRecord),
		canonicalPV:    canonicalMapper,
		started:        time.Now(),
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
	evaluated := time.Since(adapter.started)
	if evaluated < 0 || adapter.canonicalPV == nil {
		return errors.New("canonical PV mapper unavailable")
	}
	canonical, err := adapter.canonicalPV.Map(observation, encoded, pv.MonotonicNanos(evaluated.Nanoseconds()))
	if err != nil {
		return fmt.Errorf("map canonical PV observation: %w", err)
	}
	producedAt := time.Now().UTC()
	adapter.qualifications[key] = sunSpecQualificationRecord{
		observation: observation, encoded: append([]byte(nil), encoded...), canonical: cloneCanonicalPVSnapshot(canonical), producedAt: producedAt,
	}
	adapter.currentPV[canonical.AssetRef] = canonicalPVCurrentRecord{
		canonical: cloneCanonicalPVSnapshot(canonical), producedAt: producedAt,
	}
	return nil
}

// PublishSunSpecCurrent replaces only the semantic current slot for an asset.
// It neither retains nor mutates terminal qualification evidence.
func (adapter *Adapter) PublishSunSpecCurrent(observation modbusreg.SunSpecQualificationObservation) error {
	if adapter == nil {
		return errors.New("modbus TCP adapter unavailable")
	}
	encoded, err := json.Marshal(observation)
	if err != nil {
		return fmt.Errorf("serialize current SunSpec observation: %w", err)
	}
	evaluated := time.Since(adapter.started)
	if evaluated < 0 || adapter.canonicalPV == nil {
		return errors.New("canonical PV mapper unavailable")
	}
	canonical, err := adapter.canonicalPV.Map(observation, encoded, pv.MonotonicNanos(evaluated.Nanoseconds()))
	if err != nil {
		return fmt.Errorf("map current canonical PV observation: %w", err)
	}
	adapter.profileMu.Lock()
	defer adapter.profileMu.Unlock()
	if adapter.currentPV == nil {
		adapter.currentPV = make(map[string]canonicalPVCurrentRecord)
	}
	adapter.currentPV[canonical.AssetRef] = canonicalPVCurrentRecord{
		canonical: cloneCanonicalPVSnapshot(canonical), producedAt: time.Now().UTC(),
	}
	return nil
}

func (adapter *Adapter) CanonicalPVSnapshot(profileID, sampleID string) (pv.Snapshot, time.Time, bool) {
	if adapter == nil {
		return pv.Snapshot{}, time.Time{}, false
	}
	adapter.profileMu.RLock()
	defer adapter.profileMu.RUnlock()
	record, ok := adapter.qualifications[profileID+"\x00"+sampleID]
	if !ok {
		return pv.Snapshot{}, time.Time{}, false
	}
	return cloneCanonicalPVSnapshot(record.canonical), record.producedAt, true
}

// CanonicalPVSnapshotByAsset returns the retained canonical snapshot selected
// by its public asset reference. It deliberately exposes no source transport
// or qualification identity and always returns a detached value.
func (adapter *Adapter) CanonicalPVSnapshotByAsset(assetRef string) (pv.Snapshot, time.Time, bool) {
	if adapter == nil || assetRef == "" {
		return pv.Snapshot{}, time.Time{}, false
	}
	adapter.profileMu.RLock()
	defer adapter.profileMu.RUnlock()
	var match *sunSpecQualificationRecord
	for _, record := range adapter.qualifications {
		if record.canonical.AssetRef == assetRef {
			if match != nil {
				return pv.Snapshot{}, time.Time{}, false
			}
			copy := record
			match = &copy
		}
	}
	if match != nil {
		current, currentOK := adapter.currentPV[assetRef]
		if !currentOK {
			return cloneCanonicalPVSnapshot(match.canonical), match.producedAt, true
		}
		if adapter.canonicalPV == nil {
			return cloneCanonicalPVSnapshot(current.canonical), current.producedAt, true
		}
		evaluated := time.Since(adapter.started)
		if evaluated < 0 {
			return pv.Snapshot{}, time.Time{}, false
		}
		snapshot, err := adapter.canonicalPV.Snapshot(assetRef, pv.MonotonicNanos(evaluated.Nanoseconds()))
		if err != nil {
			return pv.Snapshot{}, time.Time{}, false
		}
		return cloneCanonicalPVSnapshot(snapshot), current.producedAt, true
	}
	return pv.Snapshot{}, time.Time{}, false
}

func cloneCanonicalPVSnapshot(source pv.Snapshot) pv.Snapshot {
	clone := source
	clone.Facts = make(map[pv.FactKey]pv.Fact, len(source.Facts))
	for key, fact := range source.Facts {
		if fact.Value.Decimal != nil {
			value := *fact.Value.Decimal
			fact.Value.Decimal = &value
		}
		fact.Value.Symbols = append([]string(nil), fact.Value.Symbols...)
		if fact.Continuity != nil {
			continuity := *fact.Continuity
			if continuity.Delta != nil {
				value := *continuity.Delta
				continuity.Delta = &value
			}
			if continuity.Modulus != nil {
				value := *continuity.Modulus
				continuity.Modulus = &value
			}
			fact.Continuity = &continuity
		}
		clone.Facts[key] = fact
	}
	clone.Origins = make(map[pv.Digest]pv.Provenance, len(source.Origins))
	for key, value := range source.Origins {
		clone.Origins[key] = value
	}
	clone.RequestedOutputs = append([]pv.RequestedOutput(nil), source.RequestedOutputs...)
	clone.ProjectionReport = make([]pv.Projection, len(source.ProjectionReport))
	for index, projection := range source.ProjectionReport {
		if projection.Dimensions != nil {
			dimensions := *projection.Dimensions
			projection.Dimensions = &dimensions
		}
		clone.ProjectionReport[index] = projection
	}
	return clone
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
