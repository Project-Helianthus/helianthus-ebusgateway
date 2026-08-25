package portal

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

// --- Mock Bus ---

type mockExplorerBus struct {
	mu       sync.Mutex
	handler  func(context.Context, protocol.Frame) (*protocol.Frame, error)
	requests []protocol.Frame
}

func (m *mockExplorerBus) Send(ctx context.Context, frame protocol.Frame) (*protocol.Frame, error) {
	m.mu.Lock()
	m.requests = append(m.requests, frame)
	handler := m.handler
	m.mu.Unlock()
	if handler != nil {
		return handler(ctx, frame)
	}
	return nil, fmt.Errorf("no handler configured")
}

// --- Helpers ---

func explorerHandler(bus ExplorerBus) http.Handler {
	return NewHandler(Options{
		GatewayVersion: "test",
		BuildID:        "test",
		ExplorerBus:    bus,
		ExplorerSource: 0xF0,
	})
}

func explorerJSON(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return payload
}

func explorerSSEStates(t *testing.T, body string) []ExplorerScanState {
	t.Helper()
	var states []ExplorerScanState
	for _, line := range strings.Split(body, "\n") {
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var state ExplorerScanState
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &state); err != nil {
			t.Fatalf("unmarshal SSE state: %v; line=%s", err, line)
		}
		states = append(states, state)
	}
	return states
}

func waitExplorerPhase(t *testing.T, store *explorerStore, want string) *ExplorerScanState {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		state := store.getState()
		if state.Phase == want {
			return state
		}
		if state.Phase == ExplorerPhaseDone || state.Phase == ExplorerPhaseError {
			t.Fatalf("scan reached %s while waiting for %s: %#v", state.Phase, want, state)
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for explorer phase %s; last=%#v", want, store.getState())
	return nil
}

type explorerFlushRecorder struct {
	*httptest.ResponseRecorder
	flushed chan struct{}
}

func (rec *explorerFlushRecorder) Flush() {
	rec.ResponseRecorder.Flush()
	select {
	case rec.flushed <- struct{}{}:
	default:
	}
}

type explorerBlockingFlushRecorder struct {
	*httptest.ResponseRecorder
	firstFlushed chan struct{}
	releaseFirst chan struct{}
	once         sync.Once
}

func (rec *explorerBlockingFlushRecorder) Flush() {
	rec.ResponseRecorder.Flush()
	rec.once.Do(func() {
		close(rec.firstFlushed)
		<-rec.releaseFirst
	})
}

// --- Tests ---

func TestExplorerBootstrapCapability(t *testing.T) {
	h := NewHandler(Options{
		GatewayVersion: "test",
		BuildID:        "test",
		ExplorerBus:    &mockExplorerBus{},
		ExplorerSource: 0xF0,
	})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/bootstrap", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; want %d", rec.Code, http.StatusOK)
	}
	payload := explorerJSON(t, rec)
	caps, ok := payload["capabilities"].(map[string]any)
	if !ok {
		t.Fatalf("capabilities missing or wrong type")
	}
	if caps["explorer"] != true {
		t.Fatalf("explorer capability = %v; want true", caps["explorer"])
	}
	endpoints, ok := payload["endpoints"].(map[string]any)
	if !ok {
		t.Fatalf("endpoints missing or wrong type")
	}
	if endpoints["explorer_scans"] != "/portal/api/v1/explorer/scans" {
		t.Fatalf("explorer_scans endpoint = %v; want /portal/api/v1/explorer/scans", endpoints["explorer_scans"])
	}
}

func TestExplorerBootstrapCapability_Disabled(t *testing.T) {
	h := NewHandler(Options{GatewayVersion: "test", BuildID: "test"})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/bootstrap", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	payload := explorerJSON(t, rec)
	caps := payload["capabilities"].(map[string]any)
	if caps["explorer"] != false {
		t.Fatalf("explorer capability = %v; want false when ExplorerBus is nil", caps["explorer"])
	}
}

func TestExplorerScanCurrentIdle(t *testing.T) {
	h := explorerHandler(&mockExplorerBus{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/explorer/scans/current", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; want %d", rec.Code, http.StatusOK)
	}
	payload := explorerJSON(t, rec)
	if payload["phase"] != "idle" {
		t.Fatalf("phase = %v; want idle", payload["phase"])
	}
}

func TestExplorerScanNotAvailable(t *testing.T) {
	h := NewHandler(Options{GatewayVersion: "test", BuildID: "test"})

	// Explorer routes should 404 when ExplorerBus is nil (routes not registered).
	req := httptest.NewRequest(http.MethodGet, "/api/v1/explorer/scans/current", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d; want %d", rec.Code, http.StatusNotFound)
	}
}

func TestExplorerStartScan_MethodNotAllowed(t *testing.T) {
	h := explorerHandler(&mockExplorerBus{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/explorer/scans", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d; want %d", rec.Code, http.StatusMethodNotAllowed)
	}
}

func TestExplorerStartScan_MissingTarget(t *testing.T) {
	h := explorerHandler(&mockExplorerBus{})
	body := `{"kind":"b524"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/explorer/scans", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d; want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestExplorerStartScan_InvalidKind(t *testing.T) {
	h := explorerHandler(&mockExplorerBus{})
	body := `{"kind":"unknown","target":21}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/explorer/scans", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d; want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestExplorerCancelScan_MethodNotAllowed(t *testing.T) {
	h := explorerHandler(&mockExplorerBus{})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/explorer/scans/current", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d; want %d", rec.Code, http.StatusMethodNotAllowed)
	}
}

func TestExplorerCancelScan_NoActiveScan(t *testing.T) {
	h := explorerHandler(&mockExplorerBus{})
	req := httptest.NewRequest(http.MethodDelete, "/api/v1/explorer/scans/current", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; want %d", rec.Code, http.StatusOK)
	}
	payload := explorerJSON(t, rec)
	if payload["status"] != "no_active_scan" {
		t.Fatalf("status = %v; want no_active_scan", payload["status"])
	}
}

func TestExplorerReadOnlyEndpoints_MethodEnforcement(t *testing.T) {
	h := explorerHandler(&mockExplorerBus{})
	readEndpoints := []string{
		"/api/v1/explorer/scans/current/results",
		"/api/v1/explorer/scans/current/stream",
		"/api/v1/explorer/read/b524",
		"/api/v1/explorer/read/b509",
		"/api/v1/explorer/read/scanid",
	}
	for _, path := range readEndpoints {
		req := httptest.NewRequest(http.MethodPost, path, nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("%s POST: status = %d; want %d", path, rec.Code, http.StatusMethodNotAllowed)
		}
	}
}

func TestExplorerResults_Pagination(t *testing.T) {
	h := explorerHandler(&mockExplorerBus{})
	// No results yet — should return empty.
	req := httptest.NewRequest(http.MethodGet, "/api/v1/explorer/scans/current/results?offset=0&limit=10", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; want %d", rec.Code, http.StatusOK)
	}
	payload := explorerJSON(t, rec)
	if int(payload["count"].(float64)) != 0 {
		t.Fatalf("count = %v; want 0", payload["count"])
	}
}

func TestExplorerResults_NegativeOffset(t *testing.T) {
	h := explorerHandler(&mockExplorerBus{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/explorer/scans/current/results?offset=-5&limit=10", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; want %d (should not panic on negative offset)", rec.Code, http.StatusOK)
	}
	payload := explorerJSON(t, rec)
	if int(payload["count"].(float64)) != 0 {
		t.Fatalf("count = %v; want 0 (no results in idle scan)", payload["count"])
	}
}

func TestExplorerResults_MaxPositiveLimitDoesNotOverflow(t *testing.T) {
	h := explorerHandler(&mockExplorerBus{}).(*handler)
	h.explorer.current = &ExplorerScanState{
		Phase: ExplorerPhaseDone,
		Results: []ExplorerRegisterResult{
			{Addr: 1, RawHex: "01"},
			{Addr: 2, RawHex: "02"},
		},
	}
	maxInt := int(^uint(0) >> 1)
	req := httptest.NewRequest(
		http.MethodGet,
		fmt.Sprintf("/api/v1/explorer/scans/current/results?offset=1&limit=%d", maxInt),
		nil,
	)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; want %d", rec.Code, http.StatusOK)
	}
	payload := explorerJSON(t, rec)
	if got := int(payload["count"].(float64)); got != 1 {
		t.Fatalf("count = %d; want 1", got)
	}
	results := payload["results"].([]any)
	result := results[0].(map[string]any)
	if got := int(result["addr"].(float64)); got != 2 {
		t.Fatalf("returned addr = %d; want suffix addr 2", got)
	}
	if got := result["raw_hex"].(string); got != "02" {
		t.Fatalf("returned raw_hex = %q; want 02", got)
	}
}

func TestExplorerSingleB524Read(t *testing.T) {
	bus := &mockExplorerBus{
		handler: func(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
			if frame.Primary != 0xB5 || frame.Secondary != 0x24 {
				return nil, fmt.Errorf("unexpected frame: %02x.%02x", frame.Primary, frame.Secondary)
			}
			// Return a response with 5-byte header [kind, instance, group, addr_lo, addr_hi] + float32 LE payload.
			// Matches parseB524ReadPayload format in semantic_vaillant.go.
			kind := frame.Data[0]
			inst := frame.Data[3]
			group := frame.Data[2]
			addrLo := frame.Data[4]
			addrHi := frame.Data[5]
			// Value = 11.0 as float32 LE
			val := math.Float32bits(11.0)
			data := []byte{
				kind, inst, group, addrLo, addrHi,
				byte(val), byte(val >> 8), byte(val >> 16), byte(val >> 24),
			}
			return &protocol.Frame{
				Source:    frame.Target,
				Target:    frame.Source,
				Primary:   0xB5,
				Secondary: 0x24,
				Data:      data,
			}, nil
		},
	}
	h := explorerHandler(bus)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/explorer/read/b524?target=15&opcode=02&group=01&instance=00&addr=0003", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; want %d", rec.Code, http.StatusOK)
	}
	payload := explorerJSON(t, rec)
	if payload["error"] != nil && payload["error"] != "" {
		t.Fatalf("unexpected error: %v", payload["error"])
	}
	rawHex, ok := payload["raw_hex"].(string)
	if !ok || rawHex == "" {
		t.Fatalf("raw_hex missing or empty: %v", payload["raw_hex"])
	}
	rawBytes, _ := hex.DecodeString(rawHex)
	if len(rawBytes) < 4 {
		t.Fatalf("raw_hex too short: %q (%d bytes)", rawHex, len(rawBytes))
	}
	// Verify the float32 value.
	defFloat, ok := payload["default_float"].(float64)
	if !ok {
		t.Fatalf("default_float missing")
	}
	if math.Abs(defFloat-11.0) > 0.01 {
		t.Fatalf("default_float = %v; want ~11.0", defFloat)
	}
}

func TestExplorerSingleB509Read(t *testing.T) {
	bus := &mockExplorerBus{
		handler: func(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
			if frame.Primary != 0xB5 || frame.Secondary != 0x09 {
				return nil, fmt.Errorf("unexpected frame: %02x.%02x", frame.Primary, frame.Secondary)
			}
			return &protocol.Frame{
				Source:    frame.Target,
				Target:    frame.Source,
				Primary:   0xB5,
				Secondary: 0x09,
				Data:      []byte{0xAB, 0xCD, 0x12, 0x34},
			}, nil
		},
	}
	h := explorerHandler(bus)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/explorer/read/b509?target=15&addr=0028", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; want %d", rec.Code, http.StatusOK)
	}
	payload := explorerJSON(t, rec)
	if payload["raw_hex"] != "abcd1234" {
		t.Fatalf("raw_hex = %v; want abcd1234", payload["raw_hex"])
	}
	if int(payload["raw_len"].(float64)) != 4 {
		t.Fatalf("raw_len = %v; want 4", payload["raw_len"])
	}
}

func TestExplorerSingleB524Read_BusError(t *testing.T) {
	bus := &mockExplorerBus{
		handler: func(_ context.Context, _ protocol.Frame) (*protocol.Frame, error) {
			return nil, fmt.Errorf("bus timeout")
		},
	}
	h := explorerHandler(bus)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/explorer/read/b524?target=15&opcode=02&group=01&instance=00&addr=0003", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; want %d", rec.Code, http.StatusOK)
	}
	payload := explorerJSON(t, rec)
	errMsg, _ := payload["error"].(string)
	if !strings.Contains(errMsg, "bus timeout") {
		t.Fatalf("error = %q; want contains 'bus timeout'", errMsg)
	}
}

func TestExplorerSingleB524Read_MissingTarget(t *testing.T) {
	h := explorerHandler(&mockExplorerBus{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/explorer/read/b524?opcode=02&group=01&instance=00&addr=0003", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d; want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestExplorerSingleB509Read_MissingAddr(t *testing.T) {
	h := explorerHandler(&mockExplorerBus{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/explorer/read/b509?target=15", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d; want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestExplorerB509ScanLifecycle(t *testing.T) {
	bus := &mockExplorerBus{
		handler: func(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
			return &protocol.Frame{
				Source:    frame.Target,
				Target:    frame.Source,
				Primary:   0xB5,
				Secondary: 0x09,
				Data:      []byte{0x01, 0x02, 0x03, 0x04},
			}, nil
		},
	}
	h := explorerHandler(bus)

	// Start B509 scan with small range.
	body := `{"kind":"b509","target":21,"b509_addr_min":0,"b509_addr_max":3}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/explorer/scans", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("start status = %d; want %d; body=%s", rec.Code, http.StatusAccepted, rec.Body.String())
	}

	// Wait for scan to complete (small range, mock bus is instant).
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		req = httptest.NewRequest(http.MethodGet, "/api/v1/explorer/scans/current", nil)
		rec = httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		payload := explorerJSON(t, rec)
		phase := payload["phase"].(string)
		if phase == "done" {
			break
		}
		if phase == "error" || phase == "cancelled" {
			t.Fatalf("scan ended with phase=%s error=%v", phase, payload["error"])
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Verify results.
	req = httptest.NewRequest(http.MethodGet, "/api/v1/explorer/scans/current/results?offset=0&limit=100", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	payload := explorerJSON(t, rec)
	count := int(payload["count"].(float64))
	if count != 4 {
		t.Fatalf("result count = %d; want 4 (addr 0x0000..0x0003)", count)
	}
	results := payload["results"].([]any)
	first := results[0].(map[string]any)
	if first["raw_hex"] != "01020304" {
		t.Fatalf("first result raw_hex = %v; want 01020304", first["raw_hex"])
	}
}

func TestExplorerB524ScanLifecycle(t *testing.T) {
	bus := &mockExplorerBus{
		handler: func(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
			if frame.Primary != 0xB5 || frame.Secondary != 0x24 {
				return nil, fmt.Errorf("unexpected frame")
			}
			// 5-byte header format: [kind, instance, group, addr_lo, addr_hi, data...]
			// Matches parseB524ReadPayload in semantic_vaillant.go.
			kind := frame.Data[0]
			inst := frame.Data[3]
			group := frame.Data[2]
			addrLo := frame.Data[4]
			addrHi := frame.Data[5]

			// Group discovery: return float32 descriptor.
			// addr=0x0000 → descriptor probe.
			if addrLo == 0x00 && addrHi == 0x00 && inst == 0x00 {
				if group == 0x00 {
					// Group 0x00 exists, non-instanced (value=2.0).
					val := math.Float32bits(2.0)
					return &protocol.Frame{
						Source: frame.Target, Target: frame.Source,
						Primary: 0xB5, Secondary: 0x24,
						Data: []byte{kind, inst, group, addrLo, addrHi, byte(val), byte(val >> 8), byte(val >> 16), byte(val >> 24)},
					}, nil
				}
				// All other groups: NaN (end).
				val := math.Float32bits(float32(math.NaN()))
				return &protocol.Frame{
					Source: frame.Target, Target: frame.Source,
					Primary: 0xB5, Secondary: 0x24,
					Data: []byte{kind, inst, group, addrLo, addrHi, byte(val), byte(val >> 8), byte(val >> 16), byte(val >> 24)},
				}, nil
			}

			// Register read: return 4-byte float value.
			val := math.Float32bits(42.5)
			return &protocol.Frame{
				Source: frame.Target, Target: frame.Source,
				Primary: 0xB5, Secondary: 0x24,
				Data: []byte{kind, inst, group, addrLo, addrHi, byte(val), byte(val >> 8), byte(val >> 16), byte(val >> 24)},
			}, nil
		},
	}
	h := explorerHandler(bus)

	// Start B524 scan: group 0x00 to 0x02, register max 0x02.
	body := `{"kind":"b524","target":21,"opcode":2,"group_min":0,"group_max":2,"instance_max":0,"register_max":2}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/explorer/scans", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("start status = %d; want %d; body=%s", rec.Code, http.StatusAccepted, rec.Body.String())
	}

	// Wait for completion.
	deadline := time.Now().Add(5 * time.Second)
	var finalPayload map[string]any
	for time.Now().Before(deadline) {
		req = httptest.NewRequest(http.MethodGet, "/api/v1/explorer/scans/current", nil)
		rec = httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		finalPayload = explorerJSON(t, rec)
		phase := finalPayload["phase"].(string)
		if phase == "done" {
			break
		}
		if phase == "error" || phase == "cancelled" {
			t.Fatalf("scan ended with phase=%s error=%v", phase, finalPayload["error"])
		}
		time.Sleep(50 * time.Millisecond)
	}

	if finalPayload["phase"] != "done" {
		t.Fatalf("phase = %v; want done (timeout?)", finalPayload["phase"])
	}

	// Check groups: only group 0x00 should exist.
	groups, ok := finalPayload["groups"].([]any)
	if !ok || len(groups) == 0 {
		t.Fatalf("groups missing or empty")
	}
	g0 := groups[0].(map[string]any)
	if g0["exists"] != true {
		t.Fatalf("group 0x00 exists = %v; want true", g0["exists"])
	}
	if len(groups) >= 2 {
		g1 := groups[1].(map[string]any)
		if g1["exists"] != false {
			t.Fatalf("group 0x01 exists = %v; want false (NaN descriptor)", g1["exists"])
		}
	}

	// Check results: group 0x00, instance 0x00, registers 0x0000..0x0002 = 3 results.
	results, ok := finalPayload["results"].([]any)
	if !ok {
		t.Fatalf("results missing")
	}
	if len(results) != 3 {
		t.Fatalf("result count = %d; want 3", len(results))
	}
	// result[0] is addr=0x0000 which equals the group descriptor probe (returns 2.0).
	// result[1] is addr=0x0001 which is a normal register read (returns 42.5).
	r1 := results[1].(map[string]any)
	defFloat := r1["default_float"].(float64)
	if math.Abs(defFloat-42.5) > 0.01 {
		t.Fatalf("result[1] default_float = %v; want ~42.5", defFloat)
	}
}

func TestExplorerScanAlreadyRunning(t *testing.T) {
	// Bus that blocks until context is cancelled (no goroutine leak).
	bus := &mockExplorerBus{
		handler: func(ctx context.Context, _ protocol.Frame) (*protocol.Frame, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	h := explorerHandler(bus)

	// Start a scan.
	body := `{"kind":"b509","target":21,"b509_addr_min":0,"b509_addr_max":255}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/explorer/scans", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("first start status = %d; want %d", rec.Code, http.StatusAccepted)
	}

	// Try to start another — should fail with 409 Conflict.
	req = httptest.NewRequest(http.MethodPost, "/api/v1/explorer/scans", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("second start status = %d; want %d (scan already running)", rec.Code, http.StatusConflict)
	}

	// Cancel to clean up.
	req = httptest.NewRequest(http.MethodDelete, "/api/v1/explorer/scans/current", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
}

func TestExplorerSSEStream_InitialTerminalStatesEmitAndClose(t *testing.T) {
	for _, phase := range []string{
		ExplorerPhaseIdle,
		ExplorerPhaseDone,
		ExplorerPhaseCancelled,
		ExplorerPhaseError,
	} {
		t.Run(phase, func(t *testing.T) {
			h := explorerHandler(&mockExplorerBus{}).(*handler)
			h.explorer.current = &ExplorerScanState{
				Phase:          phase,
				Kind:           ExplorerKindB524,
				Target:         0x15,
				Source:         0xF0,
				Opcode:         0x02,
				StartedUTC:     "2026-08-25T00:00:00Z",
				FinishedUTC:    "2026-08-25T00:00:01Z",
				Error:          "fixture-error",
				Results:        []ExplorerRegisterResult{{Addr: 0x1234, RawHex: "aabb"}},
				Progress:       ExplorerProgress{Percent: 100, Description: "fixture-complete"},
				TotalReads:     1,
				CompletedReads: 1,
			}
			ctx, cancel := context.WithCancel(context.Background())
			req := httptest.NewRequest(http.MethodGet, "/api/v1/explorer/scans/current/stream", nil)
			req = req.WithContext(ctx)
			rec := httptest.NewRecorder()
			done := make(chan struct{})
			go func() {
				h.ServeHTTP(rec, req)
				close(done)
			}()

			select {
			case <-done:
				cancel()
			case <-time.After(500 * time.Millisecond):
				cancel()
				<-done
				t.Fatal("initial terminal SSE handler did not close before request cancellation")
			}

			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d; want %d", rec.Code, http.StatusOK)
			}
			if got := rec.Header().Get("Content-Type"); got != "text/event-stream" {
				t.Fatalf("Content-Type = %q; want text/event-stream", got)
			}
			states := explorerSSEStates(t, rec.Body.String())
			if len(states) != 1 {
				t.Fatalf("SSE event count = %d; want 1; body=%s", len(states), rec.Body.String())
			}
			state := states[0]
			if state.Phase != phase || state.Kind != ExplorerKindB524 || state.Target != 0x15 {
				t.Fatalf("initial state = %#v; want phase=%s kind=%s target=0x15", state, phase, ExplorerKindB524)
			}
			if len(state.Results) != 1 || state.Results[0].Addr != 0x1234 || state.Progress.Description != "fixture-complete" {
				t.Fatalf("initial event did not preserve full state: %#v", state)
			}
		})
	}
}

func TestExplorerSSEStream_ActiveToTerminalEmitsAndCloses(t *testing.T) {
	for _, terminal := range []string{
		ExplorerPhaseDone,
		ExplorerPhaseCancelled,
		ExplorerPhaseError,
	} {
		t.Run(terminal, func(t *testing.T) {
			h := explorerHandler(&mockExplorerBus{}).(*handler)
			h.explorer.current = &ExplorerScanState{Phase: ExplorerPhaseRegisterScan}
			rec := &explorerFlushRecorder{
				ResponseRecorder: httptest.NewRecorder(),
				flushed:          make(chan struct{}, 2),
			}
			req := httptest.NewRequest(http.MethodGet, "/api/v1/explorer/scans/current/stream", nil)
			done := make(chan struct{})
			go func() {
				h.ServeHTTP(rec, req)
				close(done)
			}()

			select {
			case <-rec.flushed:
			case <-time.After(2 * time.Second):
				t.Fatal("timed out waiting for initial SSE state")
			}

			h.explorer.mu.Lock()
			h.explorer.current = &ExplorerScanState{Phase: terminal}
			h.explorer.mu.Unlock()
			h.explorer.notifySubscribers()

			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("timed out waiting for terminal SSE closure")
			}

			body := rec.Body.String()
			if !strings.Contains(body, `"phase":"register_scan"`) {
				t.Fatalf("SSE body missing active state: %s", body)
			}
			if !strings.Contains(body, fmt.Sprintf(`"phase":%q`, terminal)) {
				t.Fatalf("SSE body missing terminal %s state: %s", terminal, body)
			}
			if got := strings.Count(body, "data: "); got != 2 {
				t.Fatalf("SSE event count = %d; want 2; body=%s", got, body)
			}
		})
	}
}

func TestExplorerSSEStream_QueuedTerminalSurvivesSuccessorScan(t *testing.T) {
	h := explorerHandler(&mockExplorerBus{}).(*handler)
	h.explorer.scanNext = 1
	h.explorer.current = &ExplorerScanState{
		scanID: 1,
		Phase:  ExplorerPhaseRegisterScan,
		Kind:   ExplorerKindB524,
		Target: 0x15,
	}
	rec := &explorerBlockingFlushRecorder{
		ResponseRecorder: httptest.NewRecorder(),
		firstFlushed:     make(chan struct{}),
		releaseFirst:     make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/explorer/scans/current/stream", nil).WithContext(ctx)
	done := make(chan struct{})
	go func() {
		h.ServeHTTP(rec, req)
		close(done)
	}()

	select {
	case <-rec.firstFlushed:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for initial SSE state")
	}

	terminalA := &ExplorerScanState{
		scanID:      1,
		Phase:       ExplorerPhaseDone,
		Kind:        ExplorerKindB524,
		Target:      0x15,
		FinishedUTC: "2026-08-25T00:00:01Z",
		Results:     []ExplorerRegisterResult{{Addr: 0x1234, RawHex: "aabb"}},
	}
	h.explorer.mu.Lock()
	h.explorer.current = terminalA
	h.explorer.mu.Unlock()
	h.explorer.notifySubscribers(terminalA)

	h.explorer.mu.Lock()
	h.explorer.scanNext = 2
	h.explorer.current = &ExplorerScanState{
		scanID: 2,
		Phase:  ExplorerPhaseRegisterScan,
		Kind:   ExplorerKindB509,
		Target: 0x08,
	}
	h.explorer.mu.Unlock()
	h.explorer.notifySubscribers()
	close(rec.releaseFirst)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("stream did not emit the queued predecessor terminal and close")
	}

	states := explorerSSEStates(t, rec.Body.String())
	if len(states) != 2 {
		t.Fatalf("SSE event count = %d; want initial A plus terminal A; body=%s", len(states), rec.Body.String())
	}
	terminal := states[1]
	if terminal.Phase != ExplorerPhaseDone || terminal.Kind != ExplorerKindB524 || terminal.Target != 0x15 {
		t.Fatalf("terminal event = %#v; want predecessor A done state", terminal)
	}
	if len(terminal.Results) != 1 || terminal.Results[0].Addr != 0x1234 {
		t.Fatalf("terminal event lost predecessor A results: %#v", terminal)
	}
}

func TestExplorerNotifyWithoutSubscribersDoesNotCopyAccumulatedResults(t *testing.T) {
	store := newExplorerStore(&mockExplorerBus{}, 0xF0)
	store.current = &ExplorerScanState{
		scanID:  1,
		Phase:   ExplorerPhaseRegisterScan,
		Results: make([]ExplorerRegisterResult, 1<<16),
	}
	if allocs := testing.AllocsPerRun(20, func() { store.notifySubscribers() }); allocs > 1 {
		t.Fatalf("notify without subscribers allocated %.1f times; want at most lock overhead", allocs)
	}
}

func TestExplorerSSEStream_RealCancellationBeforeAndDuringStream(t *testing.T) {
	newCancellableHandler := func(t *testing.T) (*handler, <-chan struct{}) {
		t.Helper()
		started := make(chan struct{})
		var once sync.Once
		bus := &mockExplorerBus{
			handler: func(ctx context.Context, _ protocol.Frame) (*protocol.Frame, error) {
				once.Do(func() { close(started) })
				<-ctx.Done()
				return nil, ctx.Err()
			},
		}
		return explorerHandler(bus).(*handler), started
	}
	startScan := func(t *testing.T, h *handler, started <-chan struct{}) {
		t.Helper()
		body := `{"kind":"b509","target":21,"b509_addr_min":0,"b509_addr_max":1}`
		req := httptest.NewRequest(http.MethodPost, "/api/v1/explorer/scans", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusAccepted {
			t.Fatalf("start status = %d; want %d; body=%s", rec.Code, http.StatusAccepted, rec.Body.String())
		}
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for cancellable scan worker")
		}
	}
	cancelScan := func(t *testing.T, h *handler) {
		t.Helper()
		req := httptest.NewRequest(http.MethodDelete, "/api/v1/explorer/scans/current", nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"status":"cancelled"`) {
			t.Fatalf("cancel response = status %d body %s", rec.Code, rec.Body.String())
		}
	}

	t.Run("connect_after_cancelled_terminal", func(t *testing.T) {
		h, started := newCancellableHandler(t)
		startScan(t, h, started)
		cancelScan(t, h)
		terminal := waitExplorerPhase(t, h.explorer, ExplorerPhaseCancelled)
		if terminal.Kind != ExplorerKindB509 || terminal.Target != 0x15 {
			t.Fatalf("cancelled terminal lost full scan state: %#v", terminal)
		}

		req := httptest.NewRequest(http.MethodGet, "/api/v1/explorer/scans/current/stream", nil)
		rec := httptest.NewRecorder()
		done := make(chan struct{})
		go func() {
			h.ServeHTTP(rec, req)
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("pre-connected cancelled stream did not emit and close")
		}
		states := explorerSSEStates(t, rec.Body.String())
		if len(states) != 1 || states[0].Phase != ExplorerPhaseCancelled || states[0].Kind != ExplorerKindB509 {
			t.Fatalf("cancelled initial SSE states = %#v; want one full cancelled B509 state", states)
		}
	})

	t.Run("cancel_already_active_stream", func(t *testing.T) {
		h, started := newCancellableHandler(t)
		startScan(t, h, started)
		rec := &explorerFlushRecorder{
			ResponseRecorder: httptest.NewRecorder(),
			flushed:          make(chan struct{}, 2),
		}
		req := httptest.NewRequest(http.MethodGet, "/api/v1/explorer/scans/current/stream", nil)
		done := make(chan struct{})
		go func() {
			h.ServeHTTP(rec, req)
			close(done)
		}()
		select {
		case <-rec.flushed:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for active SSE state")
		}

		cancelScan(t, h)
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("active stream did not emit real cancelled terminal and close")
		}
		states := explorerSSEStates(t, rec.Body.String())
		if len(states) < 2 || states[0].Phase != ExplorerPhaseRegisterScan || states[len(states)-1].Phase != ExplorerPhaseCancelled {
			t.Fatalf("active cancellation SSE states = %#v; want active state(s) ending in cancelled", states)
		}
		terminal := states[len(states)-1]
		if terminal.Kind != ExplorerKindB509 || terminal.Target != 0x15 || terminal.FinishedUTC == "" {
			t.Fatalf("cancelled terminal lost full scan state: %#v", terminal)
		}
	})
}

func TestExtractB524Payload_5ByteHeader(t *testing.T) {
	// 5-byte header format: [kind, instance, group, addr_lo, addr_hi, data...]
	// group at [2], addr at [3:4]
	data := []byte{0x02, 0x00, 0x05, 0x03, 0x00, 0xAA, 0xBB, 0xCC, 0xDD}
	payload := extractB524Payload(data, 0x05, 0x0003)
	if len(payload) != 4 {
		t.Fatalf("payload len = %d; want 4", len(payload))
	}
	if payload[0] != 0xAA || payload[1] != 0xBB || payload[2] != 0xCC || payload[3] != 0xDD {
		t.Fatalf("payload = %v; want [AA BB CC DD]", payload)
	}
}

func TestExtractB524Payload_4ByteHeader(t *testing.T) {
	// 4-byte header format: [kind, group, addr_lo, addr_hi, data...]
	// group at [1], addr at [2:3]
	data := []byte{0x02, 0x05, 0x03, 0x00, 0xAA, 0xBB, 0xCC, 0xDD}
	payload := extractB524Payload(data, 0x05, 0x0003)
	if len(payload) != 4 {
		t.Fatalf("payload len = %d; want 4", len(payload))
	}
	if payload[0] != 0xAA || payload[1] != 0xBB || payload[2] != 0xCC || payload[3] != 0xDD {
		t.Fatalf("payload = %v; want [AA BB CC DD]", payload)
	}
}

func TestExtractB524Payload_Empty(t *testing.T) {
	if payload := extractB524Payload(nil, 0, 0); payload != nil {
		t.Fatalf("nil input should return nil; got %v", payload)
	}
	if payload := extractB524Payload([]byte{}, 0, 0); payload != nil {
		t.Fatalf("empty input should return nil; got %v", payload)
	}
	if payload := extractB524Payload([]byte{0x00}, 0, 0); payload != nil {
		t.Fatalf("single 0x00 should return nil; got %v", payload)
	}
	if payload := extractB524Payload([]byte{0x01, 0x02}, 0, 0); payload != nil {
		t.Fatalf("short input should return nil; got %v", payload)
	}
}

func TestExplorerUnknownRoute(t *testing.T) {
	h := explorerHandler(&mockExplorerBus{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/explorer/nonexistent", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d; want %d", rec.Code, http.StatusNotFound)
	}
}

// Ensure JSON request body parsing works.
func TestExplorerStartScan_InvalidJSON(t *testing.T) {
	h := explorerHandler(&mockExplorerBus{})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/explorer/scans", bytes.NewReader([]byte("not json")))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d; want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestExplorerSendWithRetry_RetriesOnError(t *testing.T) {
	var attempts int
	bus := &mockExplorerBus{
		handler: func(_ context.Context, _ protocol.Frame) (*protocol.Frame, error) {
			attempts++
			if attempts < 3 {
				return nil, fmt.Errorf("bus arbitration lost")
			}
			return &protocol.Frame{Data: []byte{0x01, 0x02}}, nil
		},
	}

	frame := protocol.Frame{Primary: 0xB5, Secondary: 0x24}
	resp, err := explorerSendWithRetry(context.Background(), bus, frame)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d; want 3", attempts)
	}
}

func TestExplorerSendWithRetry_AllFail(t *testing.T) {
	bus := &mockExplorerBus{
		handler: func(_ context.Context, _ protocol.Frame) (*protocol.Frame, error) {
			return nil, fmt.Errorf("bus timeout")
		},
	}

	frame := protocol.Frame{Primary: 0xB5, Secondary: 0x24}
	_, err := explorerSendWithRetry(context.Background(), bus, frame)
	if err == nil {
		t.Fatal("expected error after all retries exhausted")
	}
	if !strings.Contains(err.Error(), "bus timeout") {
		t.Fatalf("error = %q; want contains 'bus timeout'", err)
	}
}

func TestExplorerSendWithRetry_NilResponse(t *testing.T) {
	bus := &mockExplorerBus{
		handler: func(_ context.Context, _ protocol.Frame) (*protocol.Frame, error) {
			return nil, nil
		},
	}

	frame := protocol.Frame{Primary: 0xB5, Secondary: 0x24}
	_, err := explorerSendWithRetry(context.Background(), bus, frame)
	if err == nil {
		t.Fatal("expected error for nil response")
	}
	if !strings.Contains(err.Error(), "empty response") {
		t.Fatalf("error = %q; want contains 'empty response'", err)
	}
}

func TestExplorerSendWithRetry_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var attempts int
	bus := &mockExplorerBus{
		handler: func(_ context.Context, _ protocol.Frame) (*protocol.Frame, error) {
			attempts++
			cancel()
			return nil, fmt.Errorf("bus error")
		},
	}

	frame := protocol.Frame{Primary: 0xB5, Secondary: 0x24}
	_, err := explorerSendWithRetry(ctx, bus, frame)
	if err == nil {
		t.Fatal("expected error")
	}
	if attempts > 2 {
		t.Fatalf("attempts = %d; want <= 2 (should stop on cancelled context)", attempts)
	}
}

func TestExplorerSingleB509Read_0x29Opcode(t *testing.T) {
	bus := &mockExplorerBus{
		handler: func(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
			if frame.Primary != 0xB5 || frame.Secondary != 0x09 {
				return nil, fmt.Errorf("unexpected frame: %02x.%02x", frame.Primary, frame.Secondary)
			}
			// Verify opcode is 0x29.
			if frame.Data[0] != 0x29 {
				return nil, fmt.Errorf("unexpected opcode: 0x%02x; want 0x29", frame.Data[0])
			}
			return &protocol.Frame{
				Source:    frame.Target,
				Target:    frame.Source,
				Primary:   0xB5,
				Secondary: 0x09,
				Data:      []byte{0x11, 0x22, 0x33, 0x44},
			}, nil
		},
	}
	h := explorerHandler(bus)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/explorer/read/b509?target=15&opcode=29&addr=0028", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; want %d", rec.Code, http.StatusOK)
	}
	payload := explorerJSON(t, rec)
	if payload["raw_hex"] != "11223344" {
		t.Fatalf("raw_hex = %v; want 11223344", payload["raw_hex"])
	}

	// Verify the bus received the correct opcode.
	bus.mu.Lock()
	defer bus.mu.Unlock()
	for _, r := range bus.requests {
		if r.Primary == 0xB5 && r.Secondary == 0x09 {
			if r.Data[0] != 0x29 {
				t.Fatalf("bus frame opcode = 0x%02x; want 0x29", r.Data[0])
			}
		}
	}
}

func TestExplorerB509Scan_0x29Opcode(t *testing.T) {
	bus := &mockExplorerBus{
		handler: func(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
			if frame.Primary != 0xB5 || frame.Secondary != 0x09 {
				return nil, fmt.Errorf("unexpected frame: %02x.%02x", frame.Primary, frame.Secondary)
			}
			if frame.Data[0] != 0x29 {
				return nil, fmt.Errorf("unexpected opcode: 0x%02x; want 0x29", frame.Data[0])
			}
			return &protocol.Frame{
				Source:    frame.Target,
				Target:    frame.Source,
				Primary:   0xB5,
				Secondary: 0x09,
				Data:      []byte{0x01, 0x02, 0x03, 0x04},
			}, nil
		},
	}
	h := explorerHandler(bus)

	// opcode 41 decimal = 0x29
	body := `{"kind":"b509","target":21,"opcode":41,"b509_addr_min":0,"b509_addr_max":1}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/explorer/scans", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("start status = %d; want %d; body=%s", rec.Code, http.StatusAccepted, rec.Body.String())
	}

	// Wait for completion.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		req = httptest.NewRequest(http.MethodGet, "/api/v1/explorer/scans/current", nil)
		rec = httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		p := explorerJSON(t, rec)
		phase := p["phase"].(string)
		if phase == "done" {
			break
		}
		if phase == "error" || phase == "cancelled" {
			t.Fatalf("scan ended with phase=%s error=%v", phase, p["error"])
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Verify results: 2 addresses (0x0000, 0x0001).
	req = httptest.NewRequest(http.MethodGet, "/api/v1/explorer/scans/current/results?offset=0&limit=100", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	payload := explorerJSON(t, rec)
	count := int(payload["count"].(float64))
	if count != 2 {
		t.Fatalf("result count = %d; want 2", count)
	}

	// Verify opcode in scan state.
	req = httptest.NewRequest(http.MethodGet, "/api/v1/explorer/scans/current", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	state := explorerJSON(t, rec)
	if int(state["opcode"].(float64)) != 0x29 {
		t.Fatalf("scan state opcode = %v; want 41 (0x29)", state["opcode"])
	}
}

func TestExplorerScanID_Success(t *testing.T) {
	// Build 4 chunks of 8 ASCII bytes each = "2112345678901234567890123456AB"
	serial := "2112345678901234567890123456AB"
	chunks := [][]byte{
		[]byte(serial[0:8]),
		[]byte(serial[8:16]),
		[]byte(serial[16:24]),
		[]byte(serial[24:28]),
	}
	// Pad chunk 3 to 8 bytes.
	for len(chunks[3]) < 8 {
		chunks[3] = append(chunks[3], 0x00)
	}

	callIdx := 0
	bus := &mockExplorerBus{
		handler: func(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
			if frame.Primary != 0xB5 || frame.Secondary != 0x09 {
				return nil, fmt.Errorf("unexpected frame")
			}
			qq := frame.Data[0]
			idx := int(qq) - 0x24
			if idx < 0 || idx >= 4 {
				return nil, fmt.Errorf("unexpected QQ: 0x%02x", qq)
			}
			resp := make([]byte, 9)
			resp[0] = 0x00 // status OK
			copy(resp[1:], chunks[idx])
			callIdx++
			return &protocol.Frame{
				Source:    frame.Target,
				Target:    frame.Source,
				Primary:   0xB5,
				Secondary: 0x09,
				Data:      resp,
			}, nil
		},
	}
	h := explorerHandler(bus)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/explorer/read/scanid?target=15", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; want %d", rec.Code, http.StatusOK)
	}
	payload := explorerJSON(t, rec)
	if payload["error"] != nil && payload["error"] != "" {
		t.Fatalf("unexpected error: %v", payload["error"])
	}
	serialResult, ok := payload["serial"].(string)
	if !ok || serialResult == "" {
		t.Fatalf("serial missing or empty: %v", payload["serial"])
	}
	// The serial "2112345678901234567890123456AB" should be formatted:
	// "21-12-34-5678901234-5678-901234-56"
	// Note: last 2 chars are "AB" but digits-only check fails at position 26 ('A').
	// So it returns raw string.
	// Let's use all-digit serial instead.
	t.Logf("serial = %q", serialResult)
	chunks2 := payload["chunks"].([]any)
	if len(chunks2) != 4 {
		t.Fatalf("chunks count = %d; want 4", len(chunks2))
	}
}

func TestExplorerScanID_Formatted(t *testing.T) {
	// All-digit 28-char serial that gets formatted.
	serial := "2112345678901234567890123456"
	// Pad to 32 bytes.
	padded := serial + "    " // 4 spaces (will be trimmed)
	chunks := [][]byte{
		[]byte(padded[0:8]),
		[]byte(padded[8:16]),
		[]byte(padded[16:24]),
		[]byte(padded[24:32]),
	}

	bus := &mockExplorerBus{
		handler: func(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
			qq := frame.Data[0]
			idx := int(qq) - 0x24
			resp := make([]byte, 9)
			resp[0] = 0x00
			copy(resp[1:], chunks[idx])
			return &protocol.Frame{
				Source:    frame.Target,
				Target:    frame.Source,
				Primary:   0xB5,
				Secondary: 0x09,
				Data:      resp,
			}, nil
		},
	}
	h := explorerHandler(bus)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/explorer/read/scanid?target=15", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	payload := explorerJSON(t, rec)
	serialResult := payload["serial"].(string)
	// Expected: "21-12-34-5678901234-5678-901234-56"
	expected := "21-12-34-5678901234-5678-901234-56"
	if serialResult != expected {
		t.Fatalf("serial = %q; want %q", serialResult, expected)
	}
}

func TestExplorerScanID_NoStatusByte(t *testing.T) {
	// Simulate BASV2-style response: all 9 bytes are serial data (no status prefix).
	// Chunk 0x24 first byte happens to be 0x00 (NUL padding), rest are ASCII.
	// Full serial across 36 bytes: NUL + "21213400202621480082014267N7" + 0xFF padding.
	chunkData := [][]byte{
		{0x00, '2', '1', '2', '1', '3', '4', '0', '0'},       // 0x24: NUL + "21213400"
		{'2', '0', '2', '6', '2', '1', '4', '8', '0'},        // 0x25: "202621480"
		{'0', '8', '2', '0', '1', '4', '2', '6', '7'},        // 0x26: "082014267"
		{'N', '7', 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}, // 0x27: "N7" + padding
	}

	bus := &mockExplorerBus{
		handler: func(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
			idx := int(frame.Data[0]) - 0x24
			return &protocol.Frame{
				Source:    frame.Target,
				Target:    frame.Source,
				Primary:   0xB5,
				Secondary: 0x09,
				Data:      chunkData[idx],
			}, nil
		},
	}
	h := explorerHandler(bus)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/explorer/read/scanid?target=15", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	payload := explorerJSON(t, rec)
	serialResult, _ := payload["serial"].(string)
	// Chunk 0x24 has Data[0]==0x00 but chunks 0x25-0x27 don't, so status-byte path
	// fails.  Fallback concatenates all 36 bytes, trims NUL/0xFF → "21213400202621480082014267N7".
	expected := "21-21-34-0020262148-0082-014267-N7"
	if serialResult != expected {
		t.Fatalf("serial = %q; want %q", serialResult, expected)
	}
	// Error field should be empty (all chunks returned valid data).
	errMsg, _ := payload["error"].(string)
	if errMsg != "" {
		t.Fatalf("unexpected error: %q", errMsg)
	}
}

func TestExplorerScanID_BusError(t *testing.T) {
	bus := &mockExplorerBus{
		handler: func(_ context.Context, _ protocol.Frame) (*protocol.Frame, error) {
			return nil, fmt.Errorf("bus timeout")
		},
	}
	h := explorerHandler(bus)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/explorer/read/scanid?target=15", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; want %d", rec.Code, http.StatusOK)
	}
	payload := explorerJSON(t, rec)
	errMsg, _ := payload["error"].(string)
	if errMsg == "" {
		t.Fatal("expected error in response")
	}
	if !strings.Contains(errMsg, "bus timeout") {
		t.Fatalf("error = %q; want contains 'bus timeout'", errMsg)
	}
}

func TestExplorerScanID_MissingTarget(t *testing.T) {
	h := explorerHandler(&mockExplorerBus{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/explorer/read/scanid", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d; want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestFormatScanIDSerial(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"", ""},
		{"short", "short"},
		{"2112345678901234567890123456", "21-12-34-5678901234-5678-901234-56"},
		{"21123456789012345678901234XX", "21-12-34-5678901234-5678-901234-XX"}, // digits in first 26, last 2 are any chars
		{"211234567890123456789012345678extra", "21-12-34-5678901234-5678-901234-56"},
	}
	for _, tc := range tests {
		got := formatScanIDSerial(tc.input)
		if got != tc.want {
			t.Errorf("formatScanIDSerial(%q) = %q; want %q", tc.input, got, tc.want)
		}
	}
}
