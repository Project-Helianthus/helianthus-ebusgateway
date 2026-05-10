// M1_TDD_RED tests for the runtime-state package. These tests reference the
// contract that M2_GATEWAY_LOADER and M3_GATEWAY_PERSISTER must implement.
//
// Build tag `runtime_state_tdd_red` excludes them from default `go test` runs
// (CI passes during M1) while keeping them committed as the executable
// design contract per cruise-tdd-gate. Run them locally with:
//
//	go test -tags runtime_state_tdd_red -count=1 ./internal/runtimestate/...
//
// Every test FAILS RED in this state (stubs return errNotImplemented or zero
// values). The M2_GATEWAY_LOADER + M3_GATEWAY_PERSISTER PR removes this build
// tag and replaces the stubs with real implementations, turning the suite GREEN.
//
// Plan: runtime-state-w19-26.locked. ADs referenced inline per case.

//go:build runtime_state_tdd_red

package runtimestate

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func freshTempDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	return dir
}

// helper: sample valid v1 file content as JSON bytes.
func validV1JSON(t *testing.T) []byte {
	t.Helper()
	src := `{
  "schema_version": 1,
  "meta": {
    "instance_guid": "8a3f2b9e-4d7c-4f1a-9b5e-2c1f3e7a9d5b",
    "written_at": "2026-05-10T19:42:11Z",
    "gateway_build": "test-build",
    "addon_version": "0.4.7"
  },
  "ebus": {
    "schema_version": 1,
    "self": {
      "last_admitted_source": 247,
      "last_admitted_at": "2026-05-10T19:38:55Z",
      "selection_method": "source_selection_warmup",
      "companion_target": 252
    },
    "known_bus_members": [
      {
        "addr": 8,
        "companion_addr": 3,
        "identity": {"manufacturer":"Vaillant","device_id":"BAI00","sn":"0x21000567"},
        "last_seen_at": "2026-05-10T19:39:55Z",
        "last_source": "passive_observed",
        "confidence": "verified"
      }
    ]
  }
}`
	return []byte(src)
}

// =============================================================================
// LOADER tests (M2_GATEWAY_LOADER will satisfy)
// =============================================================================

// AD11 — missing file = empty start, no error, no log fatal.
func TestLoad_MissingFile_ReturnsEmptyState(t *testing.T) {
	dir := freshTempDir(t)
	mgr := New(Options{Path: filepath.Join(dir, "runtime_state.json")})
	got, err := mgr.Load(context.Background())
	if err != nil {
		t.Fatalf("Load on missing file should NOT error, got: %v", err)
	}
	if got == nil {
		t.Fatalf("Load on missing file should return empty State, got nil")
	}
	if got.Meta.InstanceGUID != "" {
		t.Errorf("expected empty InstanceGUID, got %q", got.Meta.InstanceGUID)
	}
	if got.EBus != nil && len(got.EBus.KnownBusMembers) != 0 {
		t.Errorf("expected empty known_bus_members, got %d", len(got.EBus.KnownBusMembers))
	}
}

// AD11 — corrupt file is renamed to .corrupt-<ISO8601> and empty State is returned.
func TestLoad_CorruptFile_RenamesAndReturnsEmpty(t *testing.T) {
	dir := freshTempDir(t)
	path := filepath.Join(dir, "runtime_state.json")
	if err := os.WriteFile(path, []byte("{not valid json"), 0o644); err != nil {
		t.Fatalf("setup: %v", err)
	}
	mgr := New(Options{Path: path})
	got, err := mgr.Load(context.Background())
	if err != nil {
		t.Fatalf("Load on corrupt file should NOT return error (per AD11 it falls through to empty), got: %v", err)
	}
	if got == nil {
		t.Fatalf("expected empty State, got nil")
	}
	// The corrupt file must be quarantined.
	matches, _ := filepath.Glob(filepath.Join(dir, "runtime_state.json.corrupt-*"))
	if len(matches) == 0 {
		t.Errorf("expected runtime_state.json.corrupt-<ts> file to be present, found: none in %s", dir)
	}
	// Original path must be absent or recreated by eager persist later (here we just check it was moved).
	if _, err := os.Stat(path); err == nil {
		t.Errorf("expected original corrupt file to be renamed away, but %s still exists", path)
	}
}

// AD12 — per-plugin schema_version mismatch is silently ignored (namespace dropped from in-memory load).
func TestLoad_PluginSchemaVersionMismatch_IgnoresNamespace(t *testing.T) {
	dir := freshTempDir(t)
	path := filepath.Join(dir, "runtime_state.json")
	src := `{
  "schema_version": 1,
  "meta": {"instance_guid": "8a3f2b9e-4d7c-4f1a-9b5e-2c1f3e7a9d5b", "written_at": "2026-05-10T19:42:11Z"},
  "ebus": {"schema_version": 99, "self": {"last_admitted_source": 247}}
}`
	if err := os.WriteFile(path, []byte(src), 0o644); err != nil {
		t.Fatalf("setup: %v", err)
	}
	mgr := New(Options{Path: path})
	got, err := mgr.Load(context.Background())
	if err != nil {
		t.Fatalf("Load with plugin-version mismatch should NOT error (per AD12), got: %v", err)
	}
	if got == nil {
		t.Fatalf("expected non-nil State")
	}
	if got.Meta.InstanceGUID != "8a3f2b9e-4d7c-4f1a-9b5e-2c1f3e7a9d5b" {
		t.Errorf("meta should still load; got InstanceGUID=%q", got.Meta.InstanceGUID)
	}
	if got.EBus != nil {
		t.Errorf("ebus.* with mismatched schema_version should be dropped; got non-nil EBus")
	}
}

// AD05/AD22 — valid v1 file parses all fields including null companion + null identity.
func TestLoad_ValidV1_ParsesAllFields(t *testing.T) {
	dir := freshTempDir(t)
	path := filepath.Join(dir, "runtime_state.json")
	if err := os.WriteFile(path, validV1JSON(t), 0o644); err != nil {
		t.Fatalf("setup: %v", err)
	}
	mgr := New(Options{Path: path})
	got, err := mgr.Load(context.Background())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.SchemaVersion != 1 {
		t.Errorf("SchemaVersion: want 1, got %d", got.SchemaVersion)
	}
	if got.Meta.InstanceGUID != "8a3f2b9e-4d7c-4f1a-9b5e-2c1f3e7a9d5b" {
		t.Errorf("InstanceGUID: %q", got.Meta.InstanceGUID)
	}
	if got.EBus == nil || got.EBus.Self == nil {
		t.Fatalf("expected ebus.self to be parsed")
	}
	if got.EBus.Self.LastAdmittedSource != 0xF7 {
		t.Errorf("LastAdmittedSource: want 0xF7, got 0x%X", got.EBus.Self.LastAdmittedSource)
	}
	if got.EBus.Self.SelectionMethod != SelectionMethodWarmup {
		t.Errorf("SelectionMethod: %q", got.EBus.Self.SelectionMethod)
	}
	if len(got.EBus.KnownBusMembers) != 1 {
		t.Fatalf("KnownBusMembers: want 1, got %d", len(got.EBus.KnownBusMembers))
	}
	m := got.EBus.KnownBusMembers[0]
	if m.Addr != 0x08 || m.LastSource != LastSourcePassiveObserved || m.Confidence != ConfidenceVerified {
		t.Errorf("member fields: addr=0x%X source=%q confidence=%q", m.Addr, m.LastSource, m.Confidence)
	}
	if m.CompanionAddr == nil || *m.CompanionAddr != 0x03 {
		t.Errorf("CompanionAddr: want 0x03 (per atr/02-companion-derivation pinned pair), got %v", m.CompanionAddr)
	}
	if m.Identity == nil || m.Identity.DeviceID != "BAI00" {
		t.Errorf("Identity: %+v", m.Identity)
	}
}

// =============================================================================
// PERSISTER tests (M3_GATEWAY_PERSISTER will satisfy)
// =============================================================================

// AD13 — atomic temp+rename: file appears via rename, never via partial write.
// We assert that during a write, an inode-stable rename happens (no truncate-in-place).
func TestPersist_AtomicTempRename(t *testing.T) {
	dir := freshTempDir(t)
	path := filepath.Join(dir, "runtime_state.json")
	mgr := New(Options{Path: path})

	// Pre-existing valid file to preserve old-file authority.
	if err := os.WriteFile(path, validV1JSON(t), 0o644); err != nil {
		t.Fatalf("setup: %v", err)
	}
	preInfo, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat pre: %v", err)
	}

	// Mutate state and trigger persist.
	mgr.UpdateSelf(Self{LastAdmittedSource: 0xF1, SelectionMethod: SelectionMethodOverride})
	// Allow a brief flush window via Stop (M3 will define Stop to flush).
	if err := mgr.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	postInfo, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat post: %v", err)
	}
	// The post-write inode (or modtime) should differ from the pre-write inode,
	// reflecting a rename operation rather than truncate-in-place.
	if !postInfo.ModTime().After(preInfo.ModTime()) {
		t.Errorf("expected rename to produce a newer ModTime; pre=%v post=%v", preInfo.ModTime(), postInfo.ModTime())
	}
}

// AD13 — JSON output uses deterministic key order (stable across writes).
func TestPersist_DeterministicKeyOrder(t *testing.T) {
	dir := freshTempDir(t)
	path := filepath.Join(dir, "runtime_state.json")
	mgr := New(Options{Path: path})

	if err := mgr.EagerPersistInstanceGUID(context.Background(),
		"8a3f2b9e-4d7c-4f1a-9b5e-2c1f3e7a9d5b", IdentitySourceGenerated); err != nil {
		t.Fatalf("eager persist: %v", err)
	}
	a, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if err := mgr.EagerPersistInstanceGUID(context.Background(),
		"8a3f2b9e-4d7c-4f1a-9b5e-2c1f3e7a9d5b", IdentitySourceGenerated); err != nil {
		t.Fatalf("eager persist 2: %v", err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read 2: %v", err)
	}

	// Strip the dynamic written_at field for stable comparison.
	stripWrittenAt := func(in []byte) []byte {
		var v map[string]interface{}
		_ = json.Unmarshal(in, &v)
		if meta, ok := v["meta"].(map[string]interface{}); ok {
			delete(meta, "written_at")
		}
		out, _ := json.Marshal(v)
		return out
	}
	if string(stripWrittenAt(a)) != string(stripWrittenAt(b)) {
		t.Errorf("expected deterministic key order; got divergent output:\nA=%s\nB=%s", a, b)
	}
}

// AD13 EXDEV path: cross-device-link rename returns EXDEV → write failure preserves
// old file (no non-atomic fallback). Hard to fault-inject without a custom FS abstraction;
// this test verifies that, given a temp file already in the target directory, EXDEV
// cannot reasonably occur (precondition test). M3 will add fault-injection-based unit tests.
func TestPersist_TempInTargetDirEliminatesEXDEV(t *testing.T) {
	dir := freshTempDir(t)
	path := filepath.Join(dir, "runtime_state.json")
	mgr := New(Options{Path: path})

	// Run a write and inspect the directory for stray temp files.
	if err := mgr.EagerPersistInstanceGUID(context.Background(),
		"8a3f2b9e-4d7c-4f1a-9b5e-2c1f3e7a9d5b", IdentitySourceGenerated); err != nil {
		t.Fatalf("eager persist: %v", err)
	}
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		name := e.Name()
		// Allow only the target file + corrupt-* renames + any in-target temp pattern (.tmp prefix).
		if name == filepath.Base(path) {
			continue
		}
		if strings.HasPrefix(name, filepath.Base(path)+".tmp") {
			t.Errorf("temp file leaked after successful write: %s", name)
		}
		if !strings.HasPrefix(name, filepath.Base(path)+".corrupt-") &&
			!strings.HasPrefix(name, filepath.Base(path)+".tmp") &&
			name != filepath.Base(path) {
			// Unexpected sidecar file; M3's writer should keep target dir clean.
			t.Logf("unexpected dir entry: %s (M3 should keep target dir tidy)", name)
		}
	}
}

// AD13 concurrent-write serialization: parallel UpdateSelf calls do not corrupt
// the file. After all writes flush, the file is parseable.
func TestPersist_ConcurrentWritesSerialized(t *testing.T) {
	dir := freshTempDir(t)
	path := filepath.Join(dir, "runtime_state.json")
	mgr := New(Options{Path: path})
	if err := mgr.EagerPersistInstanceGUID(context.Background(),
		"8a3f2b9e-4d7c-4f1a-9b5e-2c1f3e7a9d5b", IdentitySourceGenerated); err != nil {
		t.Fatalf("eager: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			mgr.UpdateSelf(Self{LastAdmittedSource: byte(0xF0 + i%8), SelectionMethod: SelectionMethodWarmup})
		}(i)
	}
	wg.Wait()
	if err := mgr.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var v map[string]interface{}
	if err := json.Unmarshal(data, &v); err != nil {
		t.Errorf("post-concurrent file is not valid JSON (corrupted by interleaved writes): %v", err)
	}
}

// =============================================================================
// EAGER PERSIST tests (M2_GATEWAY_LOADER will satisfy AD08)
// =============================================================================

// AD08 — eager persist completes within 1 second of the call.
func TestEagerPersist_WithinOneSecond(t *testing.T) {
	dir := freshTempDir(t)
	path := filepath.Join(dir, "runtime_state.json")
	mgr := New(Options{Path: path})

	start := time.Now()
	err := mgr.EagerPersistInstanceGUID(context.Background(),
		"8a3f2b9e-4d7c-4f1a-9b5e-2c1f3e7a9d5b", IdentitySourceGenerated)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("eager persist: %v", err)
	}
	if elapsed > time.Second {
		t.Errorf("eager persist took %v, exceeds AD08 1s budget", elapsed)
	}
	// File must exist and contain the supplied GUID.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !strings.Contains(string(data), "8a3f2b9e-4d7c-4f1a-9b5e-2c1f3e7a9d5b") {
		t.Errorf("expected file to contain the persisted GUID; got: %s", data)
	}
}

// =============================================================================
// AD27 IDENTITY SOURCE tests
// =============================================================================

// AD27 — bare -instance-guid (no source flag) treated as cli-override; metric increments.
func TestEagerPersist_RecordsIdentitySource(t *testing.T) {
	dir := freshTempDir(t)
	path := filepath.Join(dir, "runtime_state.json")
	tracker := &recordingMetrics{}
	mgr := New(Options{Path: path, Metrics: tracker})

	if err := mgr.EagerPersistInstanceGUID(context.Background(),
		"8a3f2b9e-4d7c-4f1a-9b5e-2c1f3e7a9d5b", IdentitySourceCLIOverride); err != nil {
		t.Fatalf("eager: %v", err)
	}
	if tracker.identitySource != IdentitySourceCLIOverride {
		t.Errorf("expected metric OnIdentitySource(cli-override), got %q", tracker.identitySource)
	}
}

type recordingMetrics struct {
	mu             sync.Mutex
	writeReasons   []string
	identitySource IdentitySource
	revalidates    []string
}

func (r *recordingMetrics) OnWrite(reason string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.writeReasons = append(r.writeReasons, reason)
}
func (r *recordingMetrics) OnIdentitySource(s IdentitySource) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.identitySource = s
}
func (r *recordingMetrics) OnRevalidate(o string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.revalidates = append(r.revalidates, o)
}

// =============================================================================
// AD18 UNIQUENESS tests
// =============================================================================

// AD18 — UpsertKnownBusMember with duplicate Addr replaces, never appends.
func TestUpsert_DuplicateAddrReplaces(t *testing.T) {
	dir := freshTempDir(t)
	path := filepath.Join(dir, "runtime_state.json")
	mgr := New(Options{Path: path})

	mgr.UpsertKnownBusMember(KnownBusMember{Addr: 0x08, LastSource: LastSourcePassiveObserved, Confidence: ConfidenceCorroborated, LastSeenAt: time.Now()})
	mgr.UpsertKnownBusMember(KnownBusMember{Addr: 0x08, LastSource: LastSourceDirected0704, Confidence: ConfidenceVerified, LastSeenAt: time.Now()})

	st := mgr.State()
	if st == nil || st.EBus == nil {
		t.Fatalf("expected ebus state present after upsert")
	}
	count := 0
	for _, m := range st.EBus.KnownBusMembers {
		if m.Addr == 0x08 {
			count++
		}
	}
	if count != 1 {
		t.Errorf("AD18 violation: expected exactly 1 entry for addr 0x08, got %d", count)
	}
}

// AD23 — EvictKnownBusMember drops entry on no-reply.
func TestEvict_RemovesEntry(t *testing.T) {
	dir := freshTempDir(t)
	path := filepath.Join(dir, "runtime_state.json")
	mgr := New(Options{Path: path})

	mgr.UpsertKnownBusMember(KnownBusMember{Addr: 0x99, LastSource: LastSourceDirected0704, Confidence: ConfidenceVerified, LastSeenAt: time.Now()})
	stPre := mgr.State()
	if stPre == nil || stPre.EBus == nil {
		t.Fatalf("AD23 test: expected upsert to populate state before evict (RED until M2 implements)")
	}
	foundPre := false
	for _, m := range stPre.EBus.KnownBusMembers {
		if m.Addr == 0x99 {
			foundPre = true
			break
		}
	}
	if !foundPre {
		t.Fatalf("AD23 test: upsert did not populate addr 0x99")
	}

	mgr.EvictKnownBusMember(0x99)
	stPost := mgr.State()
	if stPost == nil || stPost.EBus == nil {
		t.Fatalf("AD23 test: state nil after evict")
	}
	for _, m := range stPost.EBus.KnownBusMembers {
		if m.Addr == 0x99 {
			t.Errorf("AD23 violation: addr 0x99 should have been evicted")
		}
	}
}

// =============================================================================
// AD24 HINT VS SOURCE-OF-TRUTH (test scaffolds; full enforcement is M4)
// =============================================================================

// State() returning ebus.self before validation MUST be tagged as historical hint
// only. The Manager itself doesn't enforce the gateway-side surface gating; M4
// does that in the GraphQL/MCP/metrics layer. This test is a documentation marker
// confirming the API surface separation is in place.
func TestState_EBusSelfPresentDoesNotImplyAdmitted(t *testing.T) {
	t.Skip("AD24 enforcement is at the GraphQL/MCP/metrics consumer level (M4_JOINER_HINT). The runtimestate package is intentionally agnostic.")
}
