package modbusadapter

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"sync"
	"time"

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

	closeOnce sync.Once
	closeErr  error
	executeMu sync.Mutex
	profileMu sync.RWMutex
	profiles  map[string]ProfileObservationRecord
}

const maxRetainedProfileObservations = 32

// ProfileObservationRecord retains one exact registry-owned observation and
// the evidence labels supplied by its future detector/poller owner.
type ProfileObservationRecord struct {
	Observation        modbusreg.Observation
	DetectionEvidence  []string
	ActivationEvidence []string
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
		endpoint:   endpoint,
		connection: handle,
		source:     config.Endpoint.RuntimeAcquisitionSource,
		profiles:   make(map[string]ProfileObservationRecord),
	}, nil
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
	return adapter.endpoint.EnqueueRead(modbus.TCPReadPlan{
		Connection:         adapter.connection,
		UnitID:             plan.UnitID,
		AuthorizationScope: plan.AuthorizationScope,
		PollGeneration:     plan.PollGeneration,
		DeadlineIdentity:   plan.DeadlineIdentity,
		Timeout:            plan.Timeout,
		Reads:              append([]modbus.TCPLogicalRead(nil), plan.Reads...),
	})
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
	return adapter.endpoint.Read(ctx, adapter.connection)
}

// ExecuteRead serializes one bounded request through the endpoint owner. It
// preserves the runtime batch unchanged and never interprets register values.
func (adapter *Adapter) ExecuteRead(ctx context.Context, plan ReadPlan) (modbus.TCPReadBatch, error) {
	if adapter == nil || adapter.endpoint == nil {
		return modbus.TCPReadBatch{}, errors.New("modbus TCP adapter unavailable")
	}
	adapter.executeMu.Lock()
	defer adapter.executeMu.Unlock()
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
	if _, exists := adapter.profiles[key]; !exists && len(adapter.profiles) >= maxRetainedProfileObservations {
		return errors.New("profile observation retention limit reached")
	}
	record.DetectionEvidence = append([]string(nil), record.DetectionEvidence...)
	record.ActivationEvidence = append([]string(nil), record.ActivationEvidence...)
	adapter.profiles[key] = record
	return nil
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
		adapter.closeErr = adapter.endpoint.Close()
	})
	return adapter.closeErr
}
