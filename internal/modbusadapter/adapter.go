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
		return modbus.TCPRequestHandle{}, errors.New("Modbus TCP adapter unavailable")
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
		return modbus.OwnerTransition{}, errors.New("Modbus TCP adapter unavailable")
	}
	return adapter.endpoint.Write(ctx, dispatch)
}

// Read returns the public runtime batch unchanged.
func (adapter *Adapter) Read(ctx context.Context) (modbus.TCPReadBatch, error) {
	if adapter == nil || adapter.endpoint == nil {
		return modbus.TCPReadBatch{}, errors.New("Modbus TCP adapter unavailable")
	}
	return adapter.endpoint.Read(ctx, adapter.connection)
}

// Cancel delegates cancellation to the endpoint owner.
func (adapter *Adapter) Cancel(handle modbus.TCPRequestHandle) error {
	if adapter == nil || adapter.endpoint == nil {
		return errors.New("Modbus TCP adapter unavailable")
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
