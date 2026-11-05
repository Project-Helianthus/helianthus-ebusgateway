package modbusadapter

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"time"

	modbus "github.com/Project-Helianthus/helianthus-modbus"
	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
)

// ReadOnlySession binds the registry's two read operations to one connected
// endpoint generation. It grants no raw dispatch or write authority. The
// managed adapter retains sole ownership of closing the endpoint.
type ReadOnlySession struct {
	adapter    *Adapter
	generation uint64
	sequence   atomic.Uint64
	closed     atomic.Bool
}

func NewReadOnlySession(adapter *Adapter) (*ReadOnlySession, error) {
	if adapter == nil {
		return nil, errors.New("read-only session unavailable")
	}
	adapter.connectionMu.RLock()
	defer adapter.connectionMu.RUnlock()
	if adapter.closed {
		return nil, errors.New("read-only session closed")
	}
	return &ReadOnlySession{adapter: adapter, generation: adapter.connection.Generation()}, nil
}

func (s *ReadOnlySession) Close() error { s.closed.Store(true); return nil }

func (s *ReadOnlySession) check() error {
	if s == nil || s.adapter == nil || s.closed.Load() {
		return errors.New("read-only session closed")
	}
	s.adapter.connectionMu.RLock()
	defer s.adapter.connectionMu.RUnlock()
	if s.adapter.closed || s.adapter.connection.Generation() != s.generation {
		return errors.New("read-only session generation fenced")
	}
	return nil
}

func sessionTimeout(ctx context.Context) time.Duration {
	timeout := 5 * time.Second
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < timeout {
		timeout = time.Until(deadline)
	}
	return timeout
}

func (s *ReadOnlySession) ReadHolding(ctx context.Context, unit byte, request modbus.ReadRegistersRequest) (modbus.ReadRegistersResponse, error) {
	if request.Function() != modbus.FunctionReadHoldingRegisters || request.Quantity() > 123 {
		return modbus.ReadRegistersResponse{}, errors.New("read-only session admits FC03 only")
	}
	if err := s.check(); err != nil {
		return modbus.ReadRegistersResponse{}, err
	}
	if err := s.adapter.executeMu.Lock(ctx); err != nil {
		return modbus.ReadRegistersResponse{}, err
	}
	defer s.adapter.executeMu.Unlock()
	if err := s.check(); err != nil {
		return modbus.ReadRegistersResponse{}, err
	}
	n := s.sequence.Add(1)
	batch, err := s.adapter.executeReadLocked(ctx, ReadPlan{UnitID: unit, AuthorizationScope: "huawei.readonly", PollGeneration: s.generation, DeadlineIdentity: n, Timeout: sessionTimeout(ctx), Reads: []modbus.TCPLogicalRead{{LogicalViewID: n, Request: request}}})
	if err != nil {
		return modbus.ReadRegistersResponse{}, sessionReadError(err)
	}
	for {
		if err := ctx.Err(); err != nil {
			return modbus.ReadRegistersResponse{}, err
		}
		for _, response := range batch.Responses {
			p := response.Provenance()
			if response.Deliverable() && p.UnitID == unit && p.RequestedFunction == request.Function() && p.Offset == request.Offset() && p.Quantity == request.Quantity() {
				raw := response.Bytes()
				if len(raw) >= 7 {
					return modbus.DecodeReadRegistersResponse(request, raw[7:])
				}
			}
		}
		// The endpoint may first return a quarantined late response or a
		// tombstone transition. Continue the same bounded read until this
		// request's deliverable response arrives; do not call it malformed.
		batch, err = s.adapter.Read(ctx)
		if err != nil {
			return modbus.ReadRegistersResponse{}, sessionReadError(err)
		}
	}
}

func (s *ReadOnlySession) ReadDeviceIdentification(ctx context.Context, unit byte, input modbusreg.HuaweiIdentificationRequest) (modbusreg.HuaweiIdentificationPage, error) {
	if input.ReadDeviceIDCode != 1 && input.ReadDeviceIDCode != 3 {
		return modbusreg.HuaweiIdentificationPage{}, errors.New("read-only identification code refused")
	}
	if err := s.check(); err != nil {
		return modbusreg.HuaweiIdentificationPage{}, err
	}
	a := s.adapter
	if err := a.executeMu.Lock(ctx); err != nil {
		return modbusreg.HuaweiIdentificationPage{}, err
	}
	defer a.executeMu.Unlock()
	if err := s.check(); err != nil {
		return modbusreg.HuaweiIdentificationPage{}, err
	}
	endpoint, ok := a.endpoint.(interface {
		EnqueueDeviceID(modbus.TCPDeviceIDPlan) (modbus.TCPRequestHandle, error)
	})
	if !ok {
		return modbusreg.HuaweiIdentificationPage{}, errors.New("endpoint identification unavailable")
	}
	if pacing := a.config.Pacing; pacing != nil {
		if err := a.awaitPacingLocked(ctx, *pacing); err != nil {
			return modbusreg.HuaweiIdentificationPage{}, err
		}
		defer func() { a.completedAt = time.Now() }()
	}
	request, err := modbus.NewDeviceIDRequest(modbus.DeviceIDAccess(input.ReadDeviceIDCode), input.ObjectID)
	if input.ReadDeviceIDCode == 3 {
		request, err = modbus.NewExtendedDeviceIDStreamRequest(input.ObjectID)
	}
	if err != nil {
		return modbusreg.HuaweiIdentificationPage{}, err
	}
	// Exactly one page per provider call: continuation is owned by the native
	// provider, including its inter-request gap and anomaly admission rules.
	limits := modbus.DefaultDeviceIDLimits()
	limits.MaxSegments = 1
	n := s.sequence.Add(1)
	handle, err := endpoint.EnqueueDeviceID(modbus.TCPDeviceIDPlan{Connection: a.connection, UnitID: unit, AuthorizationScope: "huawei.readonly", PollGeneration: s.generation, DeadlineIdentity: n, Timeout: sessionTimeout(ctx), Request: request, Limits: limits, Decoding: modbus.DeviceIDDecodingTolerant})
	if err != nil {
		return modbusreg.HuaweiIdentificationPage{}, err
	}
	a.connectionMu.Lock()
	a.lastRequest = handle
	a.connectionMu.Unlock()
	dispatch, ok := a.Dispatch()
	if !ok || dispatch.RequestID() != handle.RequestID() {
		_ = a.Cancel(handle)
		return modbusreg.HuaweiIdentificationPage{}, errors.New("identification dispatch refused")
	}
	if _, err = a.Write(ctx, dispatch); err != nil {
		return modbusreg.HuaweiIdentificationPage{}, err
	}
	batch, err := a.Read(ctx)
	if err != nil {
		return modbusreg.HuaweiIdentificationPage{}, sessionReadError(err)
	}
	for {
		if err := ctx.Err(); err != nil {
			return modbusreg.HuaweiIdentificationPage{}, err
		}
		for _, response := range batch.Responses {
			if response.Provenance().UnitID != unit || !response.Deliverable() {
				continue
			}
			if page, ok := response.DeviceIDPage(); ok {
				out := modbusreg.HuaweiIdentificationPage{ReadDeviceIDCode: uint8(page.AccessCode), Conformity: uint8(page.Conformity), ResponseBytes: len(page.PDU), DeclaredObjects: page.DeclaredObjects, More: page.MoreFollows(), NextObjectID: page.NextObjectID}
				for _, object := range page.Objects {
					out.Objects = append(out.Objects, modbusreg.HuaweiGatewayInventoryObject{ObjectID: object.ID, Value: append([]byte(nil), object.Value...)})
				}
				return out, nil
			}
			raw := response.Bytes()
			if len(raw) >= 7 {
				_, decodeErr := modbus.DecodeDeviceIDPage(request, raw[7:])
				if decodeErr != nil {
					return modbusreg.HuaweiIdentificationPage{}, decodeErr
				}
			}
		}
		batch, err = a.Read(ctx)
		if err != nil {
			return modbusreg.HuaweiIdentificationPage{}, sessionReadError(err)
		}
	}
}

var _ modbusreg.HuaweiProviderSession = (*ReadOnlySession)(nil)

func sessionReadError(err error) error {
	var timeout net.Error
	if errors.As(err, &timeout) && timeout.Timeout() {
		return errors.Join(context.DeadlineExceeded, err)
	}
	return err
}
