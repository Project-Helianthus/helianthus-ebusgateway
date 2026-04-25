package main

// M6_DISPATCHER_BRIDGE — RED tests for the production raw-frame dispatcher.
//
// These tests fail in the RED commit because rawFrameDispatcher.Invoke
// returns errRawFrameNotImplemented. The IMPL commit replaces the body
// with real bus.Send routing and these tests turn green.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/vaillant/b503session"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

// gatewayWithMockBus builds an *ebusgateway.Gateway whose Bus field
// points at a real *protocol.Bus driven by a stubRawTransport (defined
// in semantic_vaillant_adapter_info_test.go). The bus is started on a
// fresh goroutine so installVaillantB503's production path (which
// requires a non-nil Bus) takes effect.
func gatewayWithMockBus(t *testing.T) *ebusgateway.Gateway {
	t.Helper()
	bus := protocol.NewBus(stubRawTransport{}, protocol.DefaultBusConfig(), 0)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	bus.Run(ctx)
	return &ebusgateway.Gateway{Bus: bus}
}

// gatewaySource is the gateway's eBUS source address. 0x71 per project
// convention (gateway = 113). Used by the dispatcher to populate
// Frame.Source. Defined here as test-package-local; production code
// references its own constant in vaillant_b503_wiring.go after IMPL.
const gatewaySource byte = 0x71

// b503DispatcherMockBus implements b503Bus with a programmable response
// table keyed on the first 2 bytes of the request payload (the §2 family/
// selector prefix). Mirrors the LOCAL_CAPTURE wire shape so the dispatcher
// path is exercised end-to-end with realistic byte streams.
type b503DispatcherMockBus struct {
	mu sync.Mutex

	respByPrefix map[string]*protocol.Frame
	errByPrefix  map[string]error
	defaultErr   error

	calls []protocol.Frame
	// onSend, if set, is invoked synchronously inside Send before
	// returning. Tests use it to inject context cancellation, transport
	// disconnects, or epoch rollovers mid-call.
	onSend func(frame protocol.Frame, mb *b503DispatcherMockBus)
	// blockUntil, if non-nil, makes Send block until the channel closes
	// or ctx fires. Used to model long-running bus turnaround.
	blockUntil <-chan struct{}
}

func newB503DispatcherMockBus() *b503DispatcherMockBus {
	return &b503DispatcherMockBus{
		respByPrefix: make(map[string]*protocol.Frame),
		errByPrefix:  make(map[string]error),
	}
}

func (mb *b503DispatcherMockBus) Send(ctx context.Context, frame protocol.Frame) (*protocol.Frame, error) {
	mb.mu.Lock()
	mb.calls = append(mb.calls, cloneFrame(frame))
	hook := mb.onSend
	block := mb.blockUntil
	mb.mu.Unlock()

	if hook != nil {
		hook(frame, mb)
	}

	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	mb.mu.Lock()
	defer mb.mu.Unlock()
	if len(frame.Data) >= 2 {
		key := string([]byte{frame.Data[0], frame.Data[1]})
		if e, ok := mb.errByPrefix[key]; ok {
			return nil, e
		}
		if r, ok := mb.respByPrefix[key]; ok {
			out := cloneFrame(*r)
			return &out, nil
		}
	}
	if mb.defaultErr != nil {
		return nil, mb.defaultErr
	}
	return nil, errors.New("b503DispatcherMockBus: no canned response for prefix")
}

func cloneFrame(f protocol.Frame) protocol.Frame {
	return protocol.Frame{
		Source:    f.Source,
		Target:    f.Target,
		Primary:   f.Primary,
		Secondary: f.Secondary,
		Data:      append([]byte(nil), f.Data...),
	}
}

func (mb *b503DispatcherMockBus) setResp(prefix [2]byte, data []byte) {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	mb.respByPrefix[string(prefix[:])] = &protocol.Frame{Data: append([]byte(nil), data...)}
}

func (mb *b503DispatcherMockBus) setErr(prefix [2]byte, err error) {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	mb.errByPrefix[string(prefix[:])] = err
}

func (mb *b503DispatcherMockBus) callCount() int {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	return len(mb.calls)
}

func (mb *b503DispatcherMockBus) lastCall() (protocol.Frame, bool) {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	if len(mb.calls) == 0 {
		return protocol.Frame{}, false
	}
	return mb.calls[len(mb.calls)-1], true
}

// newTestDispatcher builds a rawFrameDispatcher backed by a fresh mock
// bus and a fresh Manager.
func newTestDispatcher(t *testing.T) (*rawFrameDispatcher, *b503DispatcherMockBus, *b503session.Manager) {
	t.Helper()
	bus := newB503DispatcherMockBus()
	mgr := b503session.New(
		b503session.TransportKey{AdapterInstanceID: "test", TransportEpoch: 1},
		30*time.Second,
		func(ctx context.Context) (b503session.TransportKey, error) {
			return b503session.TransportKey{}, b503session.ErrTransportDown
		},
	)
	var readMu sync.Mutex
	disp := newRawFrameDispatcher(bus, gatewaySource, &readMu, mgr, 2*time.Second)
	return disp, bus, mgr
}

// --- Read selector dispatch tests (5 tools × byte-stream verification) ---

func TestM6Dispatcher_ErrorsCurrent_RoutesViaBusSend(t *testing.T) {
	disp, bus, _ := newTestDispatcher(t)
	// LOCAL_CAPTURE-shaped 5-slot response with first slot = 0x0119
	// (the worked example from B503.md §5.3).
	bus.setResp([2]byte{0x00, 0x01}, []byte{0x19, 0x01, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})

	resp, err := disp.Invoke(context.Background(), 0x08, []byte{0x00, 0x01})
	if err != nil {
		t.Fatalf("Invoke errors.current = %v; want nil", err)
	}
	if len(resp) != 10 {
		t.Fatalf("len(resp) = %d; want 10", len(resp))
	}
	if resp[0] != 0x19 || resp[1] != 0x01 {
		t.Fatalf("resp[0:2] = %02x %02x; want 19 01", resp[0], resp[1])
	}
	if bus.callCount() != 1 {
		t.Fatalf("bus call count = %d; want 1", bus.callCount())
	}
	frame, _ := bus.lastCall()
	if frame.Primary != 0xB5 || frame.Secondary != 0x03 {
		t.Fatalf("frame PB/SB = %02x/%02x; want b5/03", frame.Primary, frame.Secondary)
	}
	if frame.Source != gatewaySource {
		t.Fatalf("frame.Source = %02x; want %02x", frame.Source, gatewaySource)
	}
	if frame.Target != 0x08 {
		t.Fatalf("frame.Target = %02x; want 08", frame.Target)
	}
	if len(frame.Data) != 2 || frame.Data[0] != 0x00 || frame.Data[1] != 0x01 {
		t.Fatalf("frame.Data = %x; want 00 01", frame.Data)
	}
}

func TestM6Dispatcher_ErrorsHistory_RoutesViaBusSend(t *testing.T) {
	disp, bus, _ := newTestDispatcher(t)
	// Errorhistory request: family=01, selector=01, plus an index byte.
	bus.setResp([2]byte{0x01, 0x01}, []byte{0x03, 0x77, 0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})

	payload := []byte{0x01, 0x01, 0x03}
	resp, err := disp.Invoke(context.Background(), 0x08, payload)
	if err != nil {
		t.Fatalf("Invoke errors.history = %v; want nil", err)
	}
	if len(resp) != 11 {
		t.Fatalf("len(resp) = %d; want 11", len(resp))
	}
	frame, _ := bus.lastCall()
	if len(frame.Data) != 3 {
		t.Fatalf("frame.Data length = %d; want 3 (family+selector+index)", len(frame.Data))
	}
}

func TestM6Dispatcher_ServiceCurrent_RoutesViaBusSend(t *testing.T) {
	disp, bus, _ := newTestDispatcher(t)
	bus.setResp([2]byte{0x00, 0x02}, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})

	resp, err := disp.Invoke(context.Background(), 0x08, []byte{0x00, 0x02})
	if err != nil {
		t.Fatalf("Invoke service.current = %v; want nil", err)
	}
	if len(resp) != 10 {
		t.Fatalf("len(resp) = %d; want 10", len(resp))
	}
	frame, _ := bus.lastCall()
	if frame.Data[0] != 0x00 || frame.Data[1] != 0x02 {
		t.Fatalf("frame.Data prefix = %x; want 00 02", frame.Data[:2])
	}
}

func TestM6Dispatcher_ServiceHistory_RoutesViaBusSend(t *testing.T) {
	disp, bus, _ := newTestDispatcher(t)
	bus.setResp([2]byte{0x01, 0x02}, []byte{0x05, 0x42, 0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})

	payload := []byte{0x01, 0x02, 0x05}
	resp, err := disp.Invoke(context.Background(), 0x08, payload)
	if err != nil {
		t.Fatalf("Invoke service.history = %v; want nil", err)
	}
	if len(resp) != 11 {
		t.Fatalf("len(resp) = %d; want 11", len(resp))
	}
}

func TestM6Dispatcher_LiveMonitor_RoutesViaBusSend(t *testing.T) {
	disp, bus, _ := newTestDispatcher(t)
	// LiveMonitorMain request: family=00, selector=03. Response carries
	// status + function in the first two bytes.
	bus.setResp([2]byte{0x00, 0x03}, []byte{0x01, 0x42, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})

	resp, err := disp.Invoke(context.Background(), 0x08, []byte{0x00, 0x03})
	if err != nil {
		t.Fatalf("Invoke live_monitor = %v; want nil", err)
	}
	if len(resp) != 8 {
		t.Fatalf("len(resp) = %d; want 8", len(resp))
	}
	if resp[0] != 0x01 || resp[1] != 0x42 {
		t.Fatalf("resp[0:2] = %02x %02x; want 01 42", resp[0], resp[1])
	}
}

// --- Reject malformed payload (§12.2 explicit reject) ---

func TestM6Dispatcher_Rejects_PayloadStartingWithNamespaceBytes(t *testing.T) {
	disp, bus, _ := newTestDispatcher(t)
	_, err := disp.Invoke(context.Background(), 0x08, []byte{0xB5, 0x03, 0x00, 0x01})
	if err == nil {
		t.Fatalf("Invoke with payload starting b5 03 = nil; want errRawFrameMalformedPayload")
	}
	if !errors.Is(err, errRawFrameMalformedPayload) {
		t.Fatalf("err = %v; want errors.Is(_, errRawFrameMalformedPayload)", err)
	}
	if bus.callCount() != 0 {
		t.Fatalf("bus.Send was called %d times; want 0 (reject must happen before wire emission)", bus.callCount())
	}
}

// --- Error mapping table (§12.4) ---

func TestM6Dispatcher_TransportDown_FiresOnTransportDisconnect(t *testing.T) {
	disp, bus, mgr := newTestDispatcher(t)
	if _, err := mgr.Enable(context.Background()); err != nil {
		t.Fatalf("mgr.Enable = %v", err)
	}
	bus.setErr([2]byte{0x00, 0x01}, errors.New("ebus: transport closed"))

	_, err := disp.Invoke(context.Background(), 0x08, []byte{0x00, 0x01})
	if err == nil {
		t.Fatalf("Invoke = nil; want TRANSPORT_DOWN-class error")
	}
	if !errors.Is(err, errRawFrameTransportDown) {
		t.Fatalf("err = %v; want errors.Is(_, errRawFrameTransportDown)", err)
	}
	if mgr.IsOwned() {
		t.Fatalf("Manager.IsOwned() = true after transport-down; want false (OnTransportDisconnect should fire)")
	}
}

func TestM6Dispatcher_CtxCanceled_MapsToUpstreamTimeout(t *testing.T) {
	disp, bus, _ := newTestDispatcher(t)
	gate := make(chan struct{})
	bus.blockUntil = gate
	defer close(gate)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	_, err := disp.Invoke(ctx, 0x08, []byte{0x00, 0x01})
	if err == nil {
		t.Fatalf("Invoke under canceled ctx = nil; want UPSTREAM_TIMEOUT-class error")
	}
	if !errors.Is(err, errRawFrameUpstreamTimeout) {
		t.Fatalf("err = %v; want errors.Is(_, errRawFrameUpstreamTimeout)", err)
	}
}

func TestM6Dispatcher_NAKErrors_MapToUpstreamRPCFailed(t *testing.T) {
	disp, bus, _ := newTestDispatcher(t)
	bus.setErr([2]byte{0x00, 0x01}, errors.New("ebus: nak from device 0x08"))

	_, err := disp.Invoke(context.Background(), 0x08, []byte{0x00, 0x01})
	if err == nil {
		t.Fatalf("Invoke under NAK = nil; want UPSTREAM_RPC_FAILED-class error")
	}
	if !errors.Is(err, errRawFrameUpstreamRPCFailed) {
		t.Fatalf("err = %v; want errors.Is(_, errRawFrameUpstreamRPCFailed)", err)
	}
	if errors.Is(err, errRawFrameTransportDown) {
		t.Fatalf("err is misclassified as TRANSPORT_DOWN; NAK is a protocol failure not a transport failure")
	}
}

func TestM6Dispatcher_CRCError_MapsToUpstreamRPCFailed(t *testing.T) {
	disp, bus, _ := newTestDispatcher(t)
	bus.setErr([2]byte{0x00, 0x01}, errors.New("ebus: crc mismatch expected=ab got=cd"))

	_, err := disp.Invoke(context.Background(), 0x08, []byte{0x00, 0x01})
	if err == nil {
		t.Fatalf("Invoke under CRC = nil; want UPSTREAM_RPC_FAILED-class error")
	}
	if !errors.Is(err, errRawFrameUpstreamRPCFailed) {
		t.Fatalf("err = %v; want errors.Is(_, errRawFrameUpstreamRPCFailed)", err)
	}
}

// --- Misconfiguration / safety guards ---

func TestM6Dispatcher_NilBus_ReturnsMisconfigured(t *testing.T) {
	mgr := b503session.New(b503session.TransportKey{}, 30*time.Second, nil)
	disp := newRawFrameDispatcher(nil, gatewaySource, nil, mgr, 0)
	_, err := disp.Invoke(context.Background(), 0x08, []byte{0x00, 0x01})
	if err == nil || !errors.Is(err, errRawFrameMisconfigured) {
		t.Fatalf("Invoke with nil bus = %v; want errRawFrameMisconfigured", err)
	}
}

func TestM6Dispatcher_NilManager_ReturnsMisconfigured(t *testing.T) {
	bus := newB503DispatcherMockBus()
	disp := newRawFrameDispatcher(bus, gatewaySource, nil, nil, 0)
	_, err := disp.Invoke(context.Background(), 0x08, []byte{0x00, 0x01})
	if err == nil || !errors.Is(err, errRawFrameMisconfigured) {
		t.Fatalf("Invoke with nil mgr = %v; want errRawFrameMisconfigured", err)
	}
}

// --- Integration: production installVaillantB503 must inject the real dispatcher ---

// TestM6Dispatcher_InstallVaillantB503_InjectsProductionDispatcher exercises
// the REAL `installVaillantB503` function (the one main.go calls) to assert
// it now wires `*rawFrameDispatcher` instead of `b503StubDispatcher{}`. This
// is the milestone integration test: in RED phase the wiring still injects
// the stub; the IMPL commit replaces the injection and turns this green.
func TestM6Dispatcher_InstallVaillantB503_InjectsProductionDispatcher(t *testing.T) {
	srv, err := mcp.NewServer(emptyMCPRegistry{}, nil)
	if err != nil {
		t.Fatalf("mcp.NewServer = %v", err)
	}
	gw := gatewayWithMockBus(t)
	cfg := &ebusgateway.Config{}
	rt := installVaillantB503(srv, gw, cfg)
	if rt == nil {
		t.Fatalf("installVaillantB503 returned nil")
	}
	if rt.dispatcher == nil {
		t.Fatalf("b503Runtime.dispatcher is nil")
	}
	if _, isStub := rt.dispatcher.(b503StubDispatcher); isStub {
		t.Fatalf("b503Runtime.dispatcher is b503StubDispatcher; want production rawFrameDispatcher (M6)")
	}
	if _, ok := rt.dispatcher.(*rawFrameDispatcher); !ok {
		t.Fatalf("b503Runtime.dispatcher = %T; want *rawFrameDispatcher", rt.dispatcher)
	}
}

// --- Source-level invariants: production must not contain the stub literal ---

// TestM6Dispatcher_NoStubLiteralInProductionWiring asserts that the
// stub-error literal "production raw-frame dispatch not yet wired" no
// longer appears anywhere in production wiring code (M6 acceptance §10).
func TestM6Dispatcher_NoStubLiteralInProductionWiring(t *testing.T) {
	// `go test ./cmd/gateway/...` runs with cwd=cmd/gateway, so plain
	// glob "*.go" finds the right files.
	files, err := filepath.Glob("*.go")
	if err != nil || len(files) == 0 {
		t.Fatalf("filepath.Glob(*.go) = %v / %d files; cannot scan production sources", err, len(files))
	}
	bad := "production raw-frame dispatch not yet wired"
	var hits []string
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		raw, readErr := os.ReadFile(f)
		if readErr != nil {
			continue
		}
		if strings.Contains(string(raw), bad) {
			hits = append(hits, f)
		}
	}
	if len(hits) > 0 {
		t.Fatalf("M6 acceptance §10: stub-error literal %q still present in production: %v", bad, hits)
	}
}

// --- Helpers shared with the truth-table & concurrency suites ---

// installVaillantB503ForTest is a test-only entry point that wires the
// production dispatcher against a mock bus + readMu. It mirrors the
// production installVaillantB503 except the bus is a stub. Used by the
// integration test above and the truth-table tests in
// vaillant_b503_dispatcher_truthtable_test.go.
func installVaillantB503ForTest(srv *mcp.Server) *b503Runtime {
	if srv == nil {
		return nil
	}
	bus := newB503DispatcherMockBus()
	bus.setResp([2]byte{0x00, 0x01}, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})
	mgr := b503session.New(
		b503session.TransportKey{AdapterInstanceID: "test", TransportEpoch: 1},
		30*time.Second,
		func(ctx context.Context) (b503session.TransportKey, error) {
			return b503session.TransportKey{}, b503session.ErrTransportDown
		},
	)
	var readMu sync.Mutex
	disp := newRawFrameDispatcher(bus, gatewaySource, &readMu, mgr, 2*time.Second)
	mcp.RegisterVaillantB503Tools(srv, mcp.VaillantB503Options{
		Dispatcher:     disp,
		SessionManager: mgr,
		DefaultTarget:  defaultVaillantTarget,
	})
	return &b503Runtime{
		mcpServer:  srv,
		manager:    mgr,
		dispatcher: disp,
	}
}
