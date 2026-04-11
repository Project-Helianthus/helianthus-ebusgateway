package adaptermux

import (
	"errors"
	"log"
	"sync"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// testWriter adapts testing.T to io.Writer for log output.
type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Helper()
	w.t.Log(string(p))
	return len(p), nil
}

// testLogger returns a logger that writes to the test log.
func testLogger(t *testing.T) *log.Logger {
	t.Helper()
	return log.New(testWriter{t}, "", 0)
}

// errUnsupportedInfoID is returned by mock transports for INFO IDs
// that the mock doesn't have a response for, matching real adapter
// behavior for unsupported IDs. Distinct from errNotConnected which
// represents a transport-level failure.
var errUnsupportedInfoID = errors.New("mock: unsupported INFO ID")

// mockInfoTransport is a minimal RawTransport that also implements
// InfoRequester for testing the INFO cache without a real adapter.
type mockInfoTransport struct {
	responses map[transport.AdapterInfoID][]byte
	errors    map[transport.AdapterInfoID]error
	mu        sync.Mutex
	calls     []transport.AdapterInfoID // records RequestInfo calls
}

func (m *mockInfoTransport) ReadByte() (byte, error)       { return 0, nil }
func (m *mockInfoTransport) Write(_ []byte) (int, error)   { return 0, nil }
func (m *mockInfoTransport) Close() error                  { return nil }
func (m *mockInfoTransport) StartArbitration(_ byte) error { return nil }

func (m *mockInfoTransport) RequestInfo(id transport.AdapterInfoID) ([]byte, error) {
	m.mu.Lock()
	m.calls = append(m.calls, id)
	m.mu.Unlock()

	if err, ok := m.errors[id]; ok {
		return nil, err
	}
	data, ok := m.responses[id]
	if !ok {
		// Real adapters return an error for unsupported INFO IDs.
		return nil, errUnsupportedInfoID
	}
	return data, nil
}

// mockNoInfoTransport is a RawTransport that does NOT implement
// InfoRequester — used to verify graceful fallback.
type mockNoInfoTransport struct{}

func (m *mockNoInfoTransport) ReadByte() (byte, error)       { return 0, nil }
func (m *mockNoInfoTransport) Write(_ []byte) (int, error)   { return 0, nil }
func (m *mockNoInfoTransport) Close() error                  { return nil }
func (m *mockNoInfoTransport) StartArbitration(_ byte) error { return nil }

// mockArbitrationTransport is a RawTransport that also implements
// ArbitrationSendsSource for testing the active path delegation.
type mockArbitrationTransport struct {
	mockNoInfoTransport
	sendsSource bool
}

func (m *mockArbitrationTransport) ArbitrationSendsSource() bool {
	return m.sendsSource
}

func TestPopulateInfoCache(t *testing.T) {
	mux := &Mux{
		logger:    testLogger(t),
		infoCache: make(map[transport.AdapterInfoID][]byte),
	}

	tr := &mockInfoTransport{
		responses: map[transport.AdapterInfoID][]byte{
			transport.AdapterInfoVersion:     {0x23, 0x01},
			transport.AdapterInfoHardwareID:  {0x10, 0x20, 0x30},
			transport.AdapterInfoHardwareConf: {0x05},
			transport.AdapterInfoTemperature: {0x1C}, // volatile — excluded from cache
			transport.AdapterInfoWiFiRSSI:    {0xE0}, // volatile — excluded from cache
		},
	}

	mux.populateInfoCache(tr)

	mux.infoCacheMu.RLock()
	defer mux.infoCacheMu.RUnlock()

	// Only stable IDs (Version, HardwareID, HardwareConf) should be cached.
	// Volatile IDs (Temperature, WiFiRSSI) are excluded.
	if len(mux.infoCache) != 3 {
		t.Fatalf("infoCache has %d entries, want 3 (volatile IDs excluded)", len(mux.infoCache))
	}

	// Verify version was cached correctly.
	if got := mux.infoCache[transport.AdapterInfoVersion]; len(got) != 2 || got[0] != 0x23 || got[1] != 0x01 {
		t.Fatalf("version cache = %v, want [0x23 0x01]", got)
	}

	// Verify hardware ID was cached.
	if got := mux.infoCache[transport.AdapterInfoHardwareID]; len(got) != 3 {
		t.Fatalf("hardware ID cache = %v, want 3 bytes", got)
	}

	// Verify volatile IDs were NOT cached.
	if _, ok := mux.infoCache[transport.AdapterInfoTemperature]; ok {
		t.Fatal("volatile AdapterInfoTemperature should not be in cache")
	}
	if _, ok := mux.infoCache[transport.AdapterInfoWiFiRSSI]; ok {
		t.Fatal("volatile AdapterInfoWiFiRSSI should not be in cache")
	}
}

func TestPopulateInfoCache_NoInfoRequester(t *testing.T) {
	mux := &Mux{
		logger:    testLogger(t),
		infoCache: make(map[transport.AdapterInfoID][]byte),
	}

	tr := &mockNoInfoTransport{}
	mux.populateInfoCache(tr)

	mux.infoCacheMu.RLock()
	defer mux.infoCacheMu.RUnlock()

	if len(mux.infoCache) != 0 {
		t.Fatalf("infoCache has %d entries, want 0 (transport doesn't support INFO)", len(mux.infoCache))
	}
}

func TestPopulateInfoCache_VersionFails_SkipsAll(t *testing.T) {
	mux := &Mux{
		logger:    testLogger(t),
		infoCache: make(map[transport.AdapterInfoID][]byte),
	}

	tr := &mockInfoTransport{
		responses: map[transport.AdapterInfoID][]byte{
			// HardwareID is present but should NOT be cached when version fails.
			transport.AdapterInfoHardwareID: {0xAA},
		},
		errors: map[transport.AdapterInfoID]error{
			transport.AdapterInfoVersion: errUnsupportedInfoID, // simulate version failure
		},
	}

	mux.populateInfoCache(tr)

	mux.infoCacheMu.RLock()
	defer mux.infoCacheMu.RUnlock()

	// When version query fails with an error, the entire cache
	// population is skipped — adapter does not support INFO.
	if len(mux.infoCache) != 0 {
		t.Fatalf("infoCache has %d entries, want 0 (version failed = skip all)", len(mux.infoCache))
	}
}

func TestCachedInfo_ReturnsCopy(t *testing.T) {
	mux := &Mux{
		logger:    testLogger(t),
		infoCache: make(map[transport.AdapterInfoID][]byte),
	}

	// Populate cache manually.
	mux.infoCacheMu.Lock()
	mux.infoCache[transport.AdapterInfoVersion] = []byte{0x23, 0x01}
	mux.infoCacheMu.Unlock()

	data1, err := mux.CachedInfo(transport.AdapterInfoVersion)
	if err != nil {
		t.Fatalf("CachedInfo error: %v", err)
	}

	// Mutate the returned slice.
	data1[0] = 0xFF

	// Fetch again — must not reflect the mutation.
	data2, err := mux.CachedInfo(transport.AdapterInfoVersion)
	if err != nil {
		t.Fatalf("CachedInfo error: %v", err)
	}

	if data2[0] != 0x23 {
		t.Fatalf("CachedInfo returned shared reference: data2[0]=0x%02X, want 0x23", data2[0])
	}
}

func TestCachedInfo_NotAvailable(t *testing.T) {
	mux := &Mux{
		logger:    testLogger(t),
		infoCache: make(map[transport.AdapterInfoID][]byte),
	}

	_, err := mux.CachedInfo(transport.AdapterInfoVersion)
	if err == nil {
		t.Fatal("CachedInfo should return error when cache is empty")
	}
}

func TestCachedInfo_IDNotCached(t *testing.T) {
	mux := &Mux{
		logger:    testLogger(t),
		infoCache: make(map[transport.AdapterInfoID][]byte),
	}

	mux.infoCacheMu.Lock()
	mux.infoCache[transport.AdapterInfoVersion] = []byte{0x23, 0x01}
	mux.infoCacheMu.Unlock()

	_, err := mux.CachedInfo(transport.AdapterInfoWiFiRSSI)
	if err == nil {
		t.Fatal("CachedInfo should return error for uncached ID")
	}
}

func TestActiveTransport_RequestInfo_UsesCache(t *testing.T) {
	mux := &Mux{
		logger:    testLogger(t),
		infoCache: make(map[transport.AdapterInfoID][]byte),
	}

	// Populate cache.
	mux.infoCacheMu.Lock()
	mux.infoCache[transport.AdapterInfoVersion] = []byte{0x23, 0x01}
	mux.infoCacheMu.Unlock()

	// Set upstream to a transport that does NOT implement InfoRequester.
	// If activeTransport were delegating to upstream, this would fail.
	mux.upstream = &mockNoInfoTransport{}

	at := &activeTransport{mux: mux}

	data, err := at.RequestInfo(transport.AdapterInfoVersion)
	if err != nil {
		t.Fatalf("RequestInfo error: %v", err)
	}
	if len(data) != 2 || data[0] != 0x23 || data[1] != 0x01 {
		t.Fatalf("RequestInfo = %v, want [0x23 0x01]", data)
	}
}

func TestActiveTransport_ArbitrationSendsSource_True(t *testing.T) {
	mux := &Mux{
		logger:    testLogger(t),
		infoCache: make(map[transport.AdapterInfoID][]byte),
	}
	mux.upstream = &mockArbitrationTransport{sendsSource: true}

	at := &activeTransport{mux: mux}

	if !at.ArbitrationSendsSource() {
		t.Fatal("ArbitrationSendsSource = false, want true")
	}
}

func TestActiveTransport_ArbitrationSendsSource_False(t *testing.T) {
	mux := &Mux{
		logger:    testLogger(t),
		infoCache: make(map[transport.AdapterInfoID][]byte),
	}
	mux.upstream = &mockArbitrationTransport{sendsSource: false}

	at := &activeTransport{mux: mux}

	if at.ArbitrationSendsSource() {
		t.Fatal("ArbitrationSendsSource = true, want false")
	}
}

func TestActiveTransport_ArbitrationSendsSource_NilUpstream(t *testing.T) {
	mux := &Mux{
		logger:    testLogger(t),
		infoCache: make(map[transport.AdapterInfoID][]byte),
	}
	mux.upstream = nil

	at := &activeTransport{mux: mux}

	if at.ArbitrationSendsSource() {
		t.Fatal("ArbitrationSendsSource = true with nil upstream, want false")
	}
}

func TestActiveTransport_ArbitrationSendsSource_NoInterface(t *testing.T) {
	mux := &Mux{
		logger:    testLogger(t),
		infoCache: make(map[transport.AdapterInfoID][]byte),
	}
	// Use a transport that does NOT implement ArbitrationSendsSource.
	mux.upstream = &mockNoInfoTransport{}

	at := &activeTransport{mux: mux}

	if at.ArbitrationSendsSource() {
		t.Fatal("ArbitrationSendsSource = true with transport lacking interface, want false")
	}
}
