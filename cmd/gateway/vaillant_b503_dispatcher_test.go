package main

// M6_DISPATCHER_BRIDGE — RED tests for the production raw-frame dispatcher.
//
// These tests fail in the RED commit because rawFrameDispatcher.Invoke
// returns errRawFrameNotImplemented. The IMPL commit replaces the body
// with real bus.Send routing and these tests turn green.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/vaillant/b503session"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
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

// gatewaySource is the admitted eBUS source used by this test dispatcher.
// Production wiring receives the admitted source from startup admission.
const gatewaySource byte = 0x7F

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

// issue851ClassifyBarrierError blocks the first Error call. bus.Send returns
// the value without formatting it, so reaching this barrier proves Invoke has
// already completed its post-Send epoch check and entered error classification.
type issue851ClassifyBarrierError struct {
	once             sync.Once
	classification   chan struct{}
	continueClassify <-chan struct{}
}

func (err *issue851ClassifyBarrierError) Error() string {
	err.once.Do(func() {
		close(err.classification)
		<-err.continueClassify
	})
	return "ebus: transport closed"
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

func TestIssue851B503StaleSendErrorCannotClearUnsettledCleanup(t *testing.T) {
	disp, bus, mgr := newTestDispatcher(t)
	runtime := &b503Runtime{manager: mgr}
	var cleanupWrites atomic.Int32
	mgr.SetCleanupDispatcher(func(context.Context, byte) b503session.DispatchOutcome {
		cleanupWrites.Add(1)
		return b503session.DispatchOutcome{Emitted: true, Native: b503session.NativeACK}
	})
	disp.disconnectIfCurrent = runtime.disconnectIfCurrent
	if _, err := mgr.Enable(context.Background()); err != nil {
		t.Fatalf("old generation Enable() error = %v", err)
	}

	classification := make(chan struct{})
	continueClassify := make(chan struct{})
	defer func() {
		select {
		case <-continueClassify:
		default:
			close(continueClassify)
		}
	}()
	bus.setErr([2]byte{0x00, 0x01}, &issue851ClassifyBarrierError{
		classification:   classification,
		continueClassify: continueClassify,
	})
	invokeDone := make(chan error, 1)
	go func() {
		_, err := disp.Invoke(context.Background(), 0x08, []byte{0x00, 0x01})
		invokeDone <- err
	}()

	select {
	case <-classification:
		// The request from generation 1 passed its end-epoch check. Retire it
		// and install a new issuer before the stale send error is classified.
	case <-time.After(time.Second):
		t.Fatal("Invoke did not reach the post-epoch-check classification barrier")
	}
	runtime.EBusDriverWithdrawn(drivermanager.Correlation{Generation: 1})
	before, ok := mgr.CleanupObligation()
	if !ok {
		t.Fatal("generation-1 withdrawal did not retain cleanup")
	}
	runtime.EBusDriverActivated(drivermanager.Correlation{Generation: 2})
	afterRecovery, ok := mgr.CleanupObligation()
	if !ok || afterRecovery != before {
		t.Fatalf("reconnect mutated cleanup: before=%+v after=%+v present=%v", before, afterRecovery, ok)
	}
	if got := cleanupWrites.Load(); got != 0 {
		t.Fatalf("reconnect cleanup writes = %d, want zero", got)
	}
	var reenableCalls int
	_, err := mgr.EnableOperation(context.Background(), 0, func(context.Context, byte) b503session.DispatchOutcome {
		reenableCalls++
		return b503session.DispatchOutcome{Emitted: true, Native: b503session.NativeACK}
	})
	if !errors.Is(err, b503session.ErrCleanupPending) || reenableCalls != 0 {
		t.Fatalf("re-enable = %v, dispatches=%d; want cleanup pending without write", err, reenableCalls)
	}
	close(continueClassify)

	select {
	case err := <-invokeDone:
		if err == nil || !errors.Is(err, errRawFrameTransportDown) {
			t.Fatalf("stale Invoke error = %v, want errRawFrameTransportDown", err)
		}
	case <-time.After(time.Second):
		t.Fatal("stale Invoke did not finish classification")
	}
	if mgr.IsOwned() {
		t.Fatal("generation-1 send error resurrected caller ownership")
	}
	if got := mgr.TransportKey().TransportEpoch; got != 2 {
		t.Fatalf("recovered transport epoch = %d, want 2", got)
	}
	afterStale, ok := mgr.CleanupObligation()
	if !ok || afterStale.GatewayCleanupAttemptID != before.GatewayCleanupAttemptID {
		t.Fatalf("stale error mutated cleanup = %+v, present=%v", afterStale, ok)
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

func TestIssue552B503OutcomePreCanceledIsNotEmitted(t *testing.T) {
	disp, bus, _ := newTestDispatcher(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	outcome := disp.InvokeB503Outcome(ctx, 0x15, []byte{0x00, 0x03})
	if outcome.Emitted {
		t.Fatal("pre-canceled outcome marked emitted")
	}
	if outcome.Err == nil || !errors.Is(outcome.Err, errRawFrameUpstreamTimeout) {
		t.Fatalf("pre-canceled outcome error = %v", outcome.Err)
	}
	if calls := bus.callCount(); calls != 0 {
		t.Fatalf("pre-canceled bus.Send count = %d, want 0", calls)
	}
}

func TestIssue552B503OutcomeNAKIsEmittedAndNativeNAK(t *testing.T) {
	disp, bus, _ := newTestDispatcher(t)
	bus.setErr([2]byte{0x00, 0x03}, fmt.Errorf("target rejected request: %w", ebuserrors.ErrNACK))
	outcome := disp.InvokeB503Outcome(context.Background(), 0x15, []byte{0x00, 0x03})
	if !outcome.Emitted || outcome.Native != b503session.NativeNAK {
		t.Fatalf("NAK outcome = %+v, want emitted NativeNAK", outcome)
	}
	if outcome.Err == nil || !errors.Is(outcome.Err, errRawFrameUpstreamRPCFailed) {
		t.Fatalf("NAK outcome error = %v", outcome.Err)
	}
	if calls := bus.callCount(); calls != 1 {
		t.Fatalf("NAK bus.Send count = %d, want 1", calls)
	}
}

func TestIssue552B503OutcomeNAKTextWithoutSentinelRemainsAmbiguous(t *testing.T) {
	d, bus, _ := newTestDispatcher(t)
	bus.setErr([2]byte{0x00, 0x03}, errors.New("proxy diagnostic contains NAK text only"))
	outcome := d.InvokeB503Outcome(context.Background(), 0x15, []byte{0x00, 0x03})
	if !outcome.Emitted || outcome.Native != b503session.NativeAmbiguous {
		t.Fatalf("text-only NAK outcome = %+v, want emitted NativeAmbiguous", outcome)
	}
}

func TestIssue552B503OutcomeACKIsNativeEvidenceOnly(t *testing.T) {
	disp, bus, _ := newTestDispatcher(t)
	bus.setResp([2]byte{0x00, 0x03}, []byte{0x01, 0x00})
	outcome := disp.InvokeB503Outcome(context.Background(), 0x15, []byte{0x00, 0x03})
	if outcome.Err != nil || !outcome.Emitted || outcome.Native != b503session.NativeACK {
		t.Fatalf("ACK outcome = %+v, want exact emitted NativeACK", outcome)
	}
	if calls := bus.callCount(); calls != 1 {
		t.Fatalf("ACK bus.Send count = %d, want 1", calls)
	}
}

func TestM6Dispatcher_NAKErrors_MapToUpstreamRPCFailed(t *testing.T) {
	disp, bus, _ := newTestDispatcher(t)
	bus.setErr([2]byte{0x00, 0x01}, fmt.Errorf("device rejected request: %w", ebuserrors.ErrNACK))

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

func TestIssue851B503SourceNotAdmittedDisconnectsOwnerWithoutAdvancingEpoch(t *testing.T) {
	bus := newB503DispatcherMockBus()
	mgr := b503session.New(
		b503session.TransportKey{AdapterInstanceID: "gateway", TransportEpoch: 7},
		30*time.Second,
		nil,
	)
	if _, err := mgr.Enable(context.Background()); err != nil {
		t.Fatalf("Enable() error = %v", err)
	}
	disp := newRawFrameDispatcherWithSourceProvider(bus, func() (byte, bool) {
		return 0, false
	}, nil, mgr, 0)
	disp.disconnectIfCurrent = (&b503Runtime{manager: mgr}).disconnectIfCurrent
	_, err := disp.Invoke(context.Background(), 0x08, []byte{0x00, 0x01})
	if err == nil || !errors.Is(err, errRawFrameSourceNotAdmitted) {
		t.Fatalf("Invoke before admission = %v; want errRawFrameSourceNotAdmitted", err)
	}
	if bus.callCount() != 0 {
		t.Fatalf("bus.Send calls = %d; want 0 before source admission", bus.callCount())
	}
	if mgr.IsOwned() {
		t.Fatal("source-unavailable read retained the old B503 issuer")
	}
	if got := mgr.TransportKey().TransportEpoch; got != 7 {
		t.Fatalf("transport epoch after source-unavailable read = %d, want 7", got)
	}
	// A later lifecycle withdrawal for the same generation is intentionally
	// idempotent: disconnect releases ownership but never manufactures an epoch.
	mgr.OnTransportDisconnect()
	if got := mgr.TransportKey().TransportEpoch; got != 7 {
		t.Fatalf("transport epoch after duplicate disconnect = %d, want 7", got)
	}
}

func TestIssue851B503StaleSourceFallbackCannotClearUnsettledCleanup(t *testing.T) {
	bus := newB503DispatcherMockBus()
	mgr := b503session.New(
		b503session.TransportKey{AdapterInstanceID: "gateway", TransportEpoch: 7},
		30*time.Second,
		nil,
	)
	if _, err := mgr.Enable(context.Background()); err != nil {
		t.Fatalf("old generation Enable() error = %v", err)
	}
	runtime := &b503Runtime{manager: mgr}
	var cleanupWrites atomic.Int32
	mgr.SetCleanupDispatcher(func(context.Context, byte) b503session.DispatchOutcome {
		cleanupWrites.Add(1)
		return b503session.DispatchOutcome{Emitted: true, Native: b503session.NativeACK}
	})
	var reenableErr error
	disp := newRawFrameDispatcherWithSourceProvider(bus, func() (byte, bool) {
		// Invoke already captured epoch 7. Complete the exact lifecycle
		// withdrawal/recovery before returning the stale unavailable result.
		runtime.EBusDriverWithdrawn(drivermanager.Correlation{Generation: 7})
		runtime.EBusDriverActivated(drivermanager.Correlation{Generation: 8})
		_, reenableErr = mgr.EnableOperation(context.Background(), 0, func(context.Context, byte) b503session.DispatchOutcome {
			t.Fatal("cleanup-fenced re-enable reached dispatcher")
			return b503session.DispatchOutcome{}
		})
		return 0, false
	}, nil, mgr, 0)
	disp.disconnectIfCurrent = runtime.disconnectIfCurrent

	_, err := disp.Invoke(context.Background(), 0x08, []byte{0x00, 0x01})
	if err == nil || !errors.Is(err, errRawFrameSourceNotAdmitted) {
		t.Fatalf("Invoke with stale unavailable result = %v, want errRawFrameSourceNotAdmitted", err)
	}
	if !errors.Is(reenableErr, b503session.ErrCleanupPending) {
		t.Fatalf("epoch-8 re-enable = %v, want ErrCleanupPending", reenableErr)
	}
	if mgr.IsOwned() {
		t.Fatal("epoch-7 source fallback resurrected caller ownership")
	}
	if got := mgr.TransportKey().TransportEpoch; got != 8 {
		t.Fatalf("recovered transport epoch = %d, want 8", got)
	}
	if obligation, ok := mgr.CleanupObligation(); !ok || obligation.TransportEpoch != 7 || obligation.LastNative != b503session.NativeAmbiguous || obligation.LastAttemptEpoch != 0 {
		t.Fatalf("epoch-8 cleanup = %+v, present=%v", obligation, ok)
	}
	if got := cleanupWrites.Load(); got != 0 {
		t.Fatalf("epoch-8 recovery cleanup writes = %d, want zero", got)
	}
	if bus.callCount() != 0 {
		t.Fatalf("bus.Send calls = %d, want 0 for unavailable source", bus.callCount())
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
	cfg := &ebusgateway.Config{ScanSource: 0x7F}
	rt := installVaillantB503(srv, gw, cfg, func() (byte, bool) { return 0x7F, true })
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
