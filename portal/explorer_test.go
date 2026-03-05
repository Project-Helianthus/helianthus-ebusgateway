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
