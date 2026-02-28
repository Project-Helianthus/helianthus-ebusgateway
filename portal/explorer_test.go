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

func (m *mockExplorerBus) requestCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.requests)
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

func TestExplorerSSEStream_TerminalState(t *testing.T) {
	h := explorerHandler(&mockExplorerBus{})

	// SSE stream when idle should return immediately with idle state.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/explorer/scans/current/stream", nil)
	req = req.WithContext(ctx)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; want %d", rec.Code, http.StatusOK)
	}
	ct := rec.Header().Get("Content-Type")
	if ct != "text/event-stream" {
		t.Fatalf("Content-Type = %q; want text/event-stream", ct)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"phase":"idle"`) {
		t.Fatalf("SSE body missing idle state: %s", body)
	}
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
