package main

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter"
	modbus "github.com/Project-Helianthus/helianthus-modbus"
)

type modbusRunLifecycleEndpoint struct {
	closeCalls int
	closeErr   error
}

func (*modbusRunLifecycleEndpoint) OpenConnection(net.Conn) (modbus.TCPConnectionHandle, error) {
	return modbus.TCPConnectionHandle{}, nil
}

func (*modbusRunLifecycleEndpoint) EnqueueRead(modbus.TCPReadPlan) (modbus.TCPRequestHandle, error) {
	return modbus.TCPRequestHandle{}, nil
}

func (*modbusRunLifecycleEndpoint) Dispatch() (modbus.TCPDispatch, bool) {
	return modbus.TCPDispatch{}, false
}

func (*modbusRunLifecycleEndpoint) Write(context.Context, modbus.TCPDispatch) (modbus.OwnerTransition, error) {
	return modbus.OwnerTransition{}, nil
}

func (*modbusRunLifecycleEndpoint) Read(context.Context, modbus.TCPConnectionHandle) (modbus.TCPReadBatch, error) {
	return modbus.TCPReadBatch{}, nil
}

func (*modbusRunLifecycleEndpoint) Cancel(modbus.TCPRequestHandle) error { return nil }

func (*modbusRunLifecycleEndpoint) Snapshot() modbus.TCPEndpointSnapshot {
	return modbus.TCPEndpointSnapshot{}
}

func (endpoint *modbusRunLifecycleEndpoint) Close() error {
	endpoint.closeCalls++
	return endpoint.closeErr
}

func TestRunClosesModbusSidecarOnceAndJoinsLaterEEBusFailure(t *testing.T) {
	originalDial := dialModbusEndpointFn
	originalFactory := newModbusEndpointFn
	originalResolver := resolveEEBusInterfaceAddressesFn
	t.Cleanup(func() {
		dialModbusEndpointFn = originalDial
		newModbusEndpointFn = originalFactory
		resolveEEBusInterfaceAddressesFn = originalResolver
	})

	client, peer := net.Pipe()
	t.Cleanup(func() { _ = peer.Close() })
	endpoint := &modbusRunLifecycleEndpoint{closeErr: errors.New("modbus shutdown failed")}
	dialModbusEndpointFn = func(context.Context, string, string) (net.Conn, error) {
		return client, nil
	}
	newModbusEndpointFn = func(modbus.TCPEndpointConfig) (modbusadapter.Endpoint, error) {
		return endpoint, nil
	}
	laterErr := errors.New("eebus interface resolution failed")
	resolveEEBusInterfaceAddressesFn = func(string) ([]netip.Addr, error) {
		return nil, laterErr
	}

	cfg := ebusgateway.DefaultConfig()
	cfg.ModbusTCPConfig = ebusgateway.ModbusTCPConfig{
		Enabled:     true,
		Endpoint:    "tcp://127.0.0.1:1502",
		DialTimeout: time.Second,
	}
	cfg.EEBusConfig = msp05bEnabledConfig()

	err := run(context.Background(), cfg)
	if !errors.Is(err, laterErr) || !errors.Is(err, endpoint.closeErr) {
		t.Fatalf("run error = %v; want later eeBUS and Modbus shutdown causes", err)
	}
	if endpoint.closeCalls != 1 {
		t.Fatalf("Modbus endpoint close calls = %d; want 1", endpoint.closeCalls)
	}
}
