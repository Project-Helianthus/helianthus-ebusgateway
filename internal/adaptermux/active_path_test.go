package adaptermux

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// --- mock transport with InfoRequester + ArbitrationSendsSource + Reconnectable ---

type mockInfoTransport struct {
	mu          sync.Mutex
	readCh      chan byte
	closed      bool
	infoCalls   []transport.AdapterInfoID
	infoResp    []byte
	infoErr     error
	arbSends    bool
	reconnected bool
	reconnErr   error
}

func newMockInfoTransport() *mockInfoTransport {
	return &mockInfoTransport{
		readCh:   make(chan byte, 256),
		infoResp: []byte{0x03, 0x01}, // default: version 3, features 1
	}
}

func (m *mockInfoTransport) ReadByte() (byte, error) {
	b, ok := <-m.readCh
	if !ok {
		return 0, io.EOF
	}
	return b, nil
}

func (m *mockInfoTransport) Write(p []byte) (int, error) { return len(p), nil }

func (m *mockInfoTransport) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.closed {
		m.closed = true
		close(m.readCh)
	}
	return nil
}

func (m *mockInfoTransport) RequestInfo(id transport.AdapterInfoID) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.infoCalls = append(m.infoCalls, id)
	if m.infoErr != nil {
		return nil, m.infoErr
	}
	resp := make([]byte, len(m.infoResp))
	copy(resp, m.infoResp)
	return resp, nil
}

func (m *mockInfoTransport) ArbitrationSendsSource() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.arbSends
}

func (m *mockInfoTransport) Reconnect() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reconnected = true
	return m.reconnErr
}

func (m *mockInfoTransport) getInfoCalls() []transport.AdapterInfoID {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]transport.AdapterInfoID, len(m.infoCalls))
	copy(result, m.infoCalls)
	return result
}

func (m *mockInfoTransport) wasReconnected() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.reconnected
}

// --- plain transport (no optional interfaces) ---

type plainTransport struct {
	readCh chan byte
	closed bool
	mu     sync.Mutex
}

func newPlainTransport() *plainTransport {
	return &plainTransport{readCh: make(chan byte, 256)}
}

func (p *plainTransport) ReadByte() (byte, error) {
	b, ok := <-p.readCh
	if !ok {
		return 0, io.EOF
	}
	return b, nil
}

func (p *plainTransport) Write(data []byte) (int, error) { return len(data), nil }

func (p *plainTransport) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.closed {
		p.closed = true
		close(p.readCh)
	}
	return nil
}

// --- helper to create a Mux with injected upstream ---

func newTestMuxWithUpstream(t *testing.T, upstream transport.RawTransport) *Mux {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	mux := New(Config{
		Protocol:    "enh",
		Network:     "tcp",
		Address:     "127.0.0.1:0",
		ReadTimeout: 200 * time.Millisecond,
	})
	mux.ctx, mux.cancel = ctx, cancel

	mux.connMu.Lock()
	mux.upstream = upstream
	mux.connMu.Unlock()

	return mux
}

// --- InfoRequester tests ---

func TestActiveTransport_RequestInfo_DelegatesToUpstream(t *testing.T) {
	mock := newMockInfoTransport()
	mock.infoResp = []byte{0x03, 0x01, 0xAB, 0xCD, 0x08}
	mux := newTestMuxWithUpstream(t, mock)
	at := mux.ActiveTransport()

	infoReq, ok := at.(transport.InfoRequester)
	if !ok {
		t.Fatal("ActiveTransport does not implement InfoRequester")
	}

	data, err := infoReq.RequestInfo(transport.AdapterInfoVersion)
	if err != nil {
		t.Fatalf("RequestInfo returned error: %v", err)
	}
	if len(data) != 5 || data[0] != 0x03 || data[1] != 0x01 {
		t.Fatalf("unexpected response: %x", data)
	}

	calls := mock.getInfoCalls()
	if len(calls) != 1 || calls[0] != transport.AdapterInfoVersion {
		t.Fatalf("expected 1 call with AdapterInfoVersion, got %v", calls)
	}
}

func TestActiveTransport_RequestInfo_PropagatesError(t *testing.T) {
	mock := newMockInfoTransport()
	mock.infoErr = errors.New("adapter busy")
	mux := newTestMuxWithUpstream(t, mock)
	at := mux.ActiveTransport()

	infoReq := at.(transport.InfoRequester)
	_, err := infoReq.RequestInfo(transport.AdapterInfoTemperature)
	if err == nil || err.Error() != "adapter busy" {
		t.Fatalf("expected 'adapter busy' error, got: %v", err)
	}
}

func TestActiveTransport_RequestInfo_NilUpstream(t *testing.T) {
	mux := newTestMuxWithUpstream(t, nil)

	// Explicitly nil upstream.
	mux.connMu.Lock()
	mux.upstream = nil
	mux.connMu.Unlock()

	at := mux.ActiveTransport()
	infoReq := at.(transport.InfoRequester)
	_, err := infoReq.RequestInfo(transport.AdapterInfoVersion)
	if err == nil {
		t.Fatal("expected error for nil upstream")
	}
	if err.Error() != "adaptermux: not connected" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestActiveTransport_RequestInfo_UnsupportedUpstream(t *testing.T) {
	plain := newPlainTransport()
	mux := newTestMuxWithUpstream(t, plain)
	at := mux.ActiveTransport()

	infoReq := at.(transport.InfoRequester)
	_, err := infoReq.RequestInfo(transport.AdapterInfoVersion)
	if err == nil {
		t.Fatal("expected error for plain transport")
	}
	if err.Error() != "adaptermux: upstream does not support INFO" {
		t.Fatalf("unexpected error: %v", err)
	}
}

// --- ArbitrationSendsSource tests ---

func TestActiveTransport_ArbitrationSendsSource_True(t *testing.T) {
	mock := newMockInfoTransport()
	mock.arbSends = true
	mux := newTestMuxWithUpstream(t, mock)
	at := mux.ActiveTransport()

	checker, ok := at.(interface{ ArbitrationSendsSource() bool })
	if !ok {
		t.Fatal("ActiveTransport does not implement ArbitrationSendsSource")
	}
	if !checker.ArbitrationSendsSource() {
		t.Fatal("expected ArbitrationSendsSource() = true")
	}
}

func TestActiveTransport_ArbitrationSendsSource_False(t *testing.T) {
	mock := newMockInfoTransport()
	mock.arbSends = false
	mux := newTestMuxWithUpstream(t, mock)
	at := mux.ActiveTransport()

	checker := at.(interface{ ArbitrationSendsSource() bool })
	if checker.ArbitrationSendsSource() {
		t.Fatal("expected ArbitrationSendsSource() = false")
	}
}

func TestActiveTransport_ArbitrationSendsSource_PlainUpstream(t *testing.T) {
	plain := newPlainTransport()
	mux := newTestMuxWithUpstream(t, plain)
	at := mux.ActiveTransport()

	checker := at.(interface{ ArbitrationSendsSource() bool })
	if checker.ArbitrationSendsSource() {
		t.Fatal("expected ArbitrationSendsSource() = false for plain transport")
	}
}

func TestActiveTransport_ArbitrationSendsSource_NilUpstream(t *testing.T) {
	mux := newTestMuxWithUpstream(t, nil)
	mux.connMu.Lock()
	mux.upstream = nil
	mux.connMu.Unlock()
	at := mux.ActiveTransport()

	checker := at.(interface{ ArbitrationSendsSource() bool })
	if checker.ArbitrationSendsSource() {
		t.Fatal("expected ArbitrationSendsSource() = false for nil upstream")
	}
}

// --- Reconnectable tests ---

func TestActiveTransport_Reconnect_DelegatesToUpstream(t *testing.T) {
	mock := newMockInfoTransport()
	mux := newTestMuxWithUpstream(t, mock)
	at := mux.ActiveTransport()

	reconnectable, ok := at.(transport.Reconnectable)
	if !ok {
		t.Fatal("ActiveTransport does not implement Reconnectable")
	}
	if err := reconnectable.Reconnect(); err != nil {
		t.Fatalf("Reconnect returned error: %v", err)
	}
	if !mock.wasReconnected() {
		t.Fatal("Reconnect did not delegate to upstream")
	}
}

func TestActiveTransport_Reconnect_PropagatesError(t *testing.T) {
	mock := newMockInfoTransport()
	mock.reconnErr = errors.New("reconnect failed")
	mux := newTestMuxWithUpstream(t, mock)
	at := mux.ActiveTransport()

	reconnectable := at.(transport.Reconnectable)
	err := reconnectable.Reconnect()
	if err == nil || err.Error() != "reconnect failed" {
		t.Fatalf("expected 'reconnect failed' error, got: %v", err)
	}
}

func TestActiveTransport_Reconnect_NilUpstream(t *testing.T) {
	mux := newTestMuxWithUpstream(t, nil)
	mux.connMu.Lock()
	mux.upstream = nil
	mux.connMu.Unlock()
	at := mux.ActiveTransport()

	reconnectable := at.(transport.Reconnectable)
	err := reconnectable.Reconnect()
	if err == nil {
		t.Fatal("expected error for nil upstream")
	}
	if err.Error() != "adaptermux: not connected" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestActiveTransport_Reconnect_UnsupportedUpstream(t *testing.T) {
	plain := newPlainTransport()
	mux := newTestMuxWithUpstream(t, plain)
	at := mux.ActiveTransport()

	reconnectable := at.(transport.Reconnectable)
	err := reconnectable.Reconnect()
	if err == nil {
		t.Fatal("expected error for plain transport")
	}
	if err.Error() != "adaptermux: upstream does not support reconnect" {
		t.Fatalf("unexpected error: %v", err)
	}
}

// --- Compile-time interface satisfaction ---

func TestActiveTransport_InterfaceSatisfaction(t *testing.T) {
	mux := newTestMuxWithUpstream(t, newMockInfoTransport())
	at := mux.ActiveTransport()

	if _, ok := at.(transport.RawTransport); !ok {
		t.Error("ActiveTransport does not implement RawTransport")
	}
	if _, ok := at.(transport.StreamEventReader); !ok {
		t.Error("ActiveTransport does not implement StreamEventReader")
	}
	if _, ok := at.(transport.InfoRequester); !ok {
		t.Error("ActiveTransport does not implement InfoRequester")
	}
	if _, ok := at.(transport.Reconnectable); !ok {
		t.Error("ActiveTransport does not implement Reconnectable")
	}
	if _, ok := at.(interface{ ArbitrationSendsSource() bool }); !ok {
		t.Error("ActiveTransport does not implement ArbitrationSendsSource")
	}
}
