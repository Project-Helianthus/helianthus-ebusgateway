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
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
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
      },
      {
        "addr": 38,
        "companion_addr": null,
        "identity": null,
        "last_seen_at": "2026-05-10T19:39:48Z",
        "last_source": "passive_observed",
        "confidence": "corroborated"
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
	if len(got.EBus.KnownBusMembers) != 2 {
		t.Fatalf("KnownBusMembers: want 2 (one with non-null identity/companion, one with null both), got %d", len(got.EBus.KnownBusMembers))
	}
	// Member 0: non-null companion + non-null identity (BAI 0x08 ↔ 0x03 pinned pair).
	m0 := got.EBus.KnownBusMembers[0]
	if m0.Addr != 0x08 || m0.LastSource != LastSourcePassiveObserved || m0.Confidence != ConfidenceVerified {
		t.Errorf("member[0] fields: addr=0x%X source=%q confidence=%q", m0.Addr, m0.LastSource, m0.Confidence)
	}
	if m0.CompanionAddr == nil || *m0.CompanionAddr != 0x03 {
		t.Errorf("member[0] CompanionAddr: want 0x03 (per atr/02-companion-derivation pinned pair), got %v", m0.CompanionAddr)
	}
	if m0.Identity == nil || m0.Identity.DeviceID != "BAI00" {
		t.Errorf("member[0] Identity: %+v", m0.Identity)
	}
	// Member 1: null companion + null identity (AD22 optional axis; 0x26 has no
	// valid companion per ATR exception list; identity unknown is permitted at
	// any confidence). A loader that rejects null companion or null identity
	// MUST fail this assertion.
	m1 := got.EBus.KnownBusMembers[1]
	if m1.Addr != 0x26 {
		t.Errorf("member[1] Addr: want 0x26, got 0x%X", m1.Addr)
	}
	if m1.CompanionAddr != nil {
		t.Errorf("member[1] CompanionAddr: want nil (0x26 has no valid companion per ATR exception list), got %v", *m1.CompanionAddr)
	}
	if m1.Identity != nil {
		t.Errorf("member[1] Identity: want nil (AD22 optional regardless of confidence), got %+v", m1.Identity)
	}
	if m1.Confidence != ConfidenceCorroborated {
		t.Errorf("member[1] Confidence: want corroborated, got %q", m1.Confidence)
	}
}

// =============================================================================
// PERSISTER tests (M3_GATEWAY_PERSISTER will satisfy)
// =============================================================================

// AD13 — atomic temp+rename: file appears via rename, never via truncate-in-place.
// Asserts inode REPLACEMENT (different inode number post-write). A rename
// operation creates a NEW inode and unlinks the old one; truncate-in-place
// keeps the same inode and just updates ModTime, which would also pass a
// ModTime-only check. Codex R2 caught this and we now distinguish the two
// behaviors via syscall.Stat_t.Ino on Unix systems.
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
	preInode, ok := inodeOf(preInfo)
	if !ok {
		t.Skip("inode info unavailable on this platform; skipping rename-vs-truncate distinction")
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
	postInode, _ := inodeOf(postInfo)
	if postInode == preInode {
		t.Errorf("AD13 atomic-rename violation: post-write inode (%d) equals pre-write inode (%d). A truncate-in-place rewrite would also pass a ModTime-only check; rename MUST replace the inode. ModTime: pre=%v post=%v",
			postInode, preInode, preInfo.ModTime(), postInfo.ModTime())
	}
}

// inodeOf returns the inode number of a FileInfo on Unix-like systems. On
// platforms where syscall.Stat_t is not available, returns (0, false).
func inodeOf(info os.FileInfo) (uint64, bool) {
	if info == nil {
		return 0, false
	}
	if sys, ok := info.Sys().(*syscall.Stat_t); ok && sys != nil {
		return uint64(sys.Ino), true
	}
	return 0, false
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

	// Strip ONLY the dynamic written_at value from the raw bytes (preserving
	// the surrounding key order) and compare the raw bytes. A naïve approach
	// that round-trips through map[string]interface{} would canonicalize the
	// key order on remarshal and silently mask non-deterministic order in the
	// persister output (Codex R2 caught this). Instead, we replace the
	// written_at value in-place with a fixed sentinel and byte-compare.
	writtenAtRe := regexp.MustCompile(`"written_at"\s*:\s*"[^"]*"`)
	stripWrittenAt := func(in []byte) []byte {
		return writtenAtRe.ReplaceAll(in, []byte(`"written_at":"<SENTINEL>"`))
	}
	stripA := stripWrittenAt(a)
	stripB := stripWrittenAt(b)
	if !bytes.Equal(stripA, stripB) {
		t.Errorf("AD13 deterministic key order violation: post-strip raw bytes differ between two writes (regex-strip preserves surrounding order, so any divergence reflects key-order non-determinism in the persister, not just timestamp drift):\nA=%s\nB=%s", stripA, stripB)
	}
}

// fakeExdevHooks is a FilesystemHooks impl that returns EXDEV from Rename and
// records calls. Used by TestPersist_RenameEXDEV_PreservesOldFile to fault-
// inject the AD13 cross-device-link failure without requiring filesystem
// misconfiguration.
type fakeExdevHooks struct {
	mu          sync.Mutex
	renameCalls int
	unlinkCalls int
	unlinkPaths []string
}

func (f *fakeExdevHooks) Rename(oldpath, newpath string) error {
	f.mu.Lock()
	f.renameCalls++
	f.mu.Unlock()
	return &os.PathError{Op: "rename", Path: oldpath, Err: syscall.EXDEV}
}

func (f *fakeExdevHooks) Unlink(path string) error {
	f.mu.Lock()
	f.unlinkCalls++
	f.unlinkPaths = append(f.unlinkPaths, path)
	f.mu.Unlock()
	return os.Remove(path)
}

// AD13 EXDEV failure path: rename returning EXDEV (cross-device link) MUST be
// treated as a write failure that preserves the OLD file authority, unlinks
// the orphan temp, increments the rename_exdev metric, and leaves no partial
// state on disk. Per AD13 there is NO non-atomic fallback path. Per Codex R2
// P1 (3215158524) this RED contract must be in M1, not deferred.
func TestPersist_RenameEXDEV_PreservesOldFile(t *testing.T) {
	dir := freshTempDir(t)
	path := filepath.Join(dir, "runtime_state.json")

	// Pre-existing valid file establishes "old file authority". After a failed
	// EXDEV rename, this content MUST still be on disk byte-for-byte.
	oldContent := validV1JSON(t)
	if err := os.WriteFile(path, oldContent, 0o644); err != nil {
		t.Fatalf("setup: %v", err)
	}

	hooks := &fakeExdevHooks{}
	tracker := &recordingMetrics{}
	mgr := New(Options{
		Path:    path,
		FsHooks: hooks,
		Metrics: tracker,
	})

	// Trigger a write. M3's persister MUST call FsHooks.Rename, get EXDEV,
	// then FsHooks.Unlink(temp), increment metric reason="rename_exdev",
	// and return without modifying the existing path.
	mgr.UpdateSelf(Self{LastAdmittedSource: 0xF1, SelectionMethod: SelectionMethodOverride})
	if err := mgr.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	// Old file content must be byte-identical (no partial-write artifacts).
	gotContent, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read post-EXDEV: %v", err)
	}
	if !bytes.Equal(gotContent, oldContent) {
		t.Errorf("AD13 EXDEV violation: old-file authority broken. Old content was overwritten or truncated despite EXDEV failure. want=%d bytes got=%d bytes", len(oldContent), len(gotContent))
	}

	// Rename hook must have been called.
	hooks.mu.Lock()
	rc := hooks.renameCalls
	uc := hooks.unlinkCalls
	hooks.mu.Unlock()
	if rc == 0 {
		t.Error("AD13 EXDEV: M3 persister never invoked FsHooks.Rename; can't have triggered the failure path")
	}
	if uc == 0 {
		t.Error("AD13 EXDEV: M3 persister never unlinked the orphan temp file after the failed rename")
	}

	// rename_exdev metric must have been incremented.
	tracker.mu.Lock()
	hasExdev := false
	for _, r := range tracker.writeReasons {
		if r == "rename_exdev" {
			hasExdev = true
			break
		}
	}
	tracker.mu.Unlock()
	if !hasExdev {
		t.Errorf("AD13 EXDEV: metric reason=%q not recorded; got reasons=%v", "rename_exdev", tracker.writeReasons)
	}

	// No partial state on disk: only the original path should exist (no .tmp leftovers).
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if e.Name() == filepath.Base(path) {
			continue
		}
		if strings.HasSuffix(e.Name(), ".tmp") || strings.Contains(e.Name(), ".tmp.") {
			t.Errorf("AD13 EXDEV: orphan temp file left on disk after failed rename: %s", e.Name())
		}
	}
}

// AD13 EXDEV precondition test: in normal operation (no fault injection), the
// temp file is created in the target directory itself, eliminating the EXDEV
// path. Codex R1 P2 (3215038506-equivalent for fail-shape).
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
