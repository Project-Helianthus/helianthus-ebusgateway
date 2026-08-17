package main

import (
	"bytes"
	"context"
	"errors"
	"log"
	"net"
	"net/netip"
	"strings"
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

func TestRunKeepsModbusStartupFailureProtocolLocal(t *testing.T) {
	originalDial := dialModbusEndpointFn
	originalResolver := resolveEEBusInterfaceAddressesFn
	originalLogWriter := log.Writer()
	originalLogFlags := log.Flags()
	t.Cleanup(func() {
		dialModbusEndpointFn = originalDial
		resolveEEBusInterfaceAddressesFn = originalResolver
		log.SetOutput(originalLogWriter)
		log.SetFlags(originalLogFlags)
	})

	endpoint := "tcp://192.0.2.44:1502"
	dialErr := errors.New("dial " + endpoint + ": unavailable")
	dialModbusEndpointFn = func(context.Context, string, string) (net.Conn, error) {
		return nil, dialErr
	}
	laterErr := errors.New("eebus interface resolution failed")
	resolveEEBusInterfaceAddressesFn = func(string) ([]netip.Addr, error) {
		return nil, laterErr
	}
	var logs bytes.Buffer
	log.SetOutput(&logs)
	log.SetFlags(0)

	cfg := ebusgateway.DefaultConfig()
	cfg.ModbusTCPConfig = ebusgateway.ModbusTCPConfig{
		Enabled:     true,
		Endpoint:    endpoint,
		DialTimeout: time.Second,
	}
	cfg.EEBusConfig = msp05bEnabledConfig()

	err := run(context.Background(), cfg)
	if !errors.Is(err, laterErr) {
		t.Fatalf("run error = %v; want later eeBUS error", err)
	}
	if errors.Is(err, dialErr) {
		t.Fatalf("run error retained protocol-local Modbus startup failure: %v", err)
	}
	if got := logs.String(); !strings.Contains(got, "Modbus TCP unavailable; continuing without Modbus") {
		t.Fatalf("gateway log = %q; want protocol-local unavailability diagnostic", got)
	} else if strings.Contains(got, endpoint) || strings.Contains(got, "192.0.2.44") {
		t.Fatalf("gateway log disclosed Modbus endpoint: %q", got)
	}
}

func TestRunMarksAdapterDirectStartupFailureForOwnedRedaction(t *testing.T) {
	originalDial := dialModbusEndpointFn
	originalFactory := newModbusEndpointFn
	t.Cleanup(func() {
		dialModbusEndpointFn = originalDial
		newModbusEndpointFn = originalFactory
	})

	client, peer := net.Pipe()
	t.Cleanup(func() { _ = peer.Close() })
	endpoint := &modbusRunLifecycleEndpoint{}
	dialModbusEndpointFn = func(context.Context, string, string) (net.Conn, error) {
		return client, nil
	}
	newModbusEndpointFn = func(modbus.TCPEndpointConfig) (modbusadapter.Endpoint, error) {
		return endpoint, nil
	}

	cfg := ebusgateway.DefaultConfig()
	cfg.ModbusTCPConfig = ebusgateway.ModbusTCPConfig{
		Enabled:     true,
		Endpoint:    "tcp://127.0.0.1:1502",
		DialTimeout: time.Second,
	}
	cfg.TransportConfig.Protocol = ebusgateway.TransportAdapterDirect
	cfg.TransportConfig.Network = "tcp"
	cfg.TransportConfig.Address = ""

	err := run(context.Background(), cfg)
	if err == nil {
		t.Fatal("run accepted adapter-direct without an address")
	}
	if got := endpointOwnerOf(err); got != endpointOwnerAdapterDirect {
		t.Fatalf("startup error owner = %v; want adapter-direct; error=%v", got, err)
	}
}
