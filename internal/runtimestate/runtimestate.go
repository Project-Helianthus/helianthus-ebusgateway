// Package runtimestate is the gateway-owned loader/persister for
// /data/runtime_state.json. Plan: runtime-state-w19-26.locked. v1 namespaces:
// meta + ebus.{self, known_bus_members[]}. See docs-ebus
// architecture/runtime-state.md for the normative schema and contract.
package runtimestate

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// SchemaVersion is the file-format version this package targets.
const SchemaVersion = 1

// EBusSchemaVersion is the v1 ebus namespace version.
const EBusSchemaVersion = 1

// SelectionMethod enumerates the persisted ebus.self.selection_method values
// per AD17 (renamed from join_method by the runtime-state-w19-26 v1.1
// amendment which migrated to the SourceAddressSelection terminology).
type SelectionMethod string

const (
	SelectionMethodWarmup               SelectionMethod = "source_selection_warmup"
	SelectionMethodExplicitValidateOnly SelectionMethod = "explicit_validate_only"
	SelectionMethodEbusdTCPFallback     SelectionMethod = "ebusd-tcp-fallback"
)

// LastSource enumerates the persisted last_source values per AD16.
// Excludes "cached" (load-time provenance preserves original) and
// "directed_07_04_no_reply" (no-reply members are dropped, never persisted).
type LastSource string

const (
	LastSourcePassiveObserved LastSource = "passive_observed"
	LastSourceDirected0704    LastSource = "directed_07_04"
	LastSourceNMEvent         LastSource = "nm_event"
)

// Confidence enumerates the persisted confidence values per AD15.
// Reflects ADDRESS PRESENCE verification only; identity is orthogonal (AD22).
type Confidence string

const (
	ConfidenceVerified     Confidence = "verified"
	ConfidenceCorroborated Confidence = "corroborated"
	ConfidenceUnidentified Confidence = "unidentified"
)

// IdentitySource enumerates the -instance-guid-source flag values per AD27.
type IdentitySource string

const (
	IdentitySourceRuntimeState   IdentitySource = "runtime_state"
	IdentitySourceLegacyMigrated IdentitySource = "legacy_migrated"
	IdentitySourceGenerated      IdentitySource = "generated"
	IdentitySourceCLIOverride    IdentitySource = "cli-override"
)

// State is the in-memory representation of /data/runtime_state.json.
type State struct {
	SchemaVersion int
	Meta          Meta
	EBus          *EBusNamespace
}

// Meta holds the meta namespace.
type Meta struct {
	InstanceGUID string
	WrittenAt    time.Time
	GatewayBuild string
	AddonVersion string
}

// EBusNamespace holds the ebus.* plugin namespace (v1).
type EBusNamespace struct {
	SchemaVersion   int
	Self            *Self
	KnownBusMembers []KnownBusMember
}

// Self holds ebus.self — historical hint only, not the current admitted
// source (AD24).
type Self struct {
	LastAdmittedSource byte
	LastAdmittedAt     time.Time
	SelectionMethod    SelectionMethod
	CompanionTarget    *byte // nil when no valid companion per address-table AD03 bit-pattern rule.
}

// KnownBusMember holds one cached known_bus_members[] entry.
type KnownBusMember struct {
	Addr          byte
	CompanionAddr *byte
	Identity      *Identity
	LastSeenAt    time.Time
	LastSource    LastSource
	Confidence    Confidence
}

// Identity is optional regardless of confidence value (AD22).
type Identity struct {
	Manufacturer string
	DeviceID     string
	SN           string
}

// MetricsHook is the surface used to expose AD13/AD25/AD27 metrics. The
// caller wires this to a Prometheus registry.
type MetricsHook interface {
	// OnWrite is called per persistence attempt with one of:
	// "marshal", "write", "rename", "fsync_temp", "rename_exdev",
	// "parent_fsync_unsupported", "ok".
	OnWrite(reason string)
	// OnIdentitySource is called once at startup with the AD27 source value.
	OnIdentitySource(source IdentitySource)
	// OnRevalidate is called per directed_07_04 outcome during M5 revalidation:
	// "responder", "no_reply", "skipped_passive_refresh".
	OnRevalidate(outcome string)
}

// NopMetrics is a no-op MetricsHook for tests/local.
type NopMetrics struct{}

func (NopMetrics) OnWrite(string)                  {}
func (NopMetrics) OnIdentitySource(IdentitySource) {}
func (NopMetrics) OnRevalidate(string)             {}

// FilesystemHooks is an injection point for fault-tolerant testing of the
// AD13 atomic-write contract. Tests can supply a mock implementation that
// returns specific errno values (EXDEV, EINVAL, ENOSYS, ENOSPC, etc.) to
// exercise every failure path without requiring real filesystem
// misconfiguration. Production code uses DefaultFilesystemHooks{} (see
// fs_hooks.go in the M3_GATEWAY_PERSISTER PR), which delegates to the os
// package.
//
// The persister uses these hooks in this order per atomic-write cycle:
//
//  1. WriteFile(temp, data, perm)   — create+write+close the temp file.
//  2. FsyncFile(temp)               — durably flush temp contents (REQUIRED).
//  3. Rename(temp, final)           — atomic temp→final.
//  4. FsyncDir(parent)              — flush parent directory (BEST-EFFORT).
//  5. Unlink(temp)                  — only if Rename failed (cleanup).
type FilesystemHooks interface {
	// WriteFile writes data to path atomically (create+truncate+write+close).
	// Returns ENOSPC on disk-full, EACCES on permission denied, etc.
	WriteFile(path string, data []byte, perm uint32) error
	// FsyncFile flushes the file at path to durable storage. Failure here
	// means the temp file may not be on disk; persister MUST treat as
	// write failure (reason="fsync_temp") and retain in-memory state.
	FsyncFile(path string) error
	// Rename is called for the temp→final atomic rename. EXDEV indicates
	// cross-device link (precondition violation; should not happen since
	// temp is in target dir). Persister handles EXDEV per AD13:
	// preserve old file, unlink temp, metric reason="rename_exdev".
	Rename(oldpath, newpath string) error
	// FsyncDir flushes directory metadata to make the rename durable. Per
	// AD13 this is BEST-EFFORT — ENOTSUP/EINVAL/EPERM/ENOSYS are SWALLOWED
	// by the persister with metric reason="parent_fsync_unsupported"
	// (distinct from real failures).
	FsyncDir(path string) error
	// Unlink removes the orphan temp file after a failed rename. Cleanup-only.
	Unlink(path string) error
}

// Options configures a Manager.
type Options struct {
	// Path is the runtime_state.json file path. Defaults to /data/runtime_state.json.
	Path string
	// GatewayBuild is the build string written into meta.gateway_build.
	GatewayBuild string
	// AddonVersion is the version string written into meta.addon_version (passed via flag from add-on).
	AddonVersion string
	// PersistInterval is the periodic ticker base interval. Defaults to 15 min.
	PersistInterval time.Duration
	// JitterRange is the symmetric jitter applied to the ticker (±range/2). Defaults to 30s.
	JitterRange time.Duration
	// Logger receives lifecycle log lines. Defaults to slog.Default().
	Logger *slog.Logger
	// Metrics receives counter increments. Defaults to NopMetrics{}.
	Metrics MetricsHook
	// FsHooks is the filesystem-operations injection point. Defaults to nil
	// (M3 implementation falls back to os.Rename / os.Remove). Tests inject
	// mocks here to exercise EXDEV / EINVAL / ENOSYS failure paths per AD13.
	FsHooks FilesystemHooks
}

// Manager coordinates loading, eager persistence, periodic writes, and shutdown
// flushes for /data/runtime_state.json. It is the sole writer (AD07).
type Manager struct {
	opts     Options
	hooks    FilesystemHooks
	mu       sync.Mutex
	state    *State
	dirty    bool
	writeGen uint64 // bumped on every mutation; used by flushIfDirty to detect concurrent updates
	stopCh   chan struct{}
	doneCh   chan struct{}
	started  bool
	stopped  bool
}

// New constructs a Manager. The Manager is not started until Start is called.
func New(opts Options) *Manager {
	if opts.PersistInterval == 0 {
		opts.PersistInterval = 15 * time.Minute
	}
	if opts.JitterRange == 0 {
		opts.JitterRange = 30 * time.Second
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Metrics == nil {
		opts.Metrics = NopMetrics{}
	}
	if opts.Path == "" {
		opts.Path = "/data/runtime_state.json"
	}
	hooks := opts.FsHooks
	if hooks == nil {
		hooks = DefaultFilesystemHooks{}
	}
	return &Manager{
		opts:   opts,
		hooks:  hooks,
		state:  &State{SchemaVersion: SchemaVersion},
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
}

// Load reads the runtime state file. Returns the parsed State, or an empty
// State on missing/corrupt. On corrupt, the file is renamed to
// "<path>.corrupt-<ISO8601>" and a warning is logged. Per-plugin
// schema_version mismatch results in that namespace being dropped from the
// in-memory load (AD12). Never returns a panic; never blocks startup.
func (m *Manager) Load(ctx context.Context) (*State, error) {
	data, err := os.ReadFile(m.opts.Path)
	if err != nil {
		if os.IsNotExist(err) {
			m.opts.Logger.Info("runtime_state: file absent, starting empty", "path", m.opts.Path)
			m.replaceState(&State{SchemaVersion: SchemaVersion})
			return m.State(), nil
		}
		// Permission denied / IO error: log + start empty (never block startup per AD11).
		m.opts.Logger.Warn("runtime_state: read failed, starting empty", "path", m.opts.Path, "error", err)
		m.replaceState(&State{SchemaVersion: SchemaVersion})
		return m.State(), nil
	}

	state, err := unmarshalState(data)
	if err != nil {
		// Corrupt or schema mismatch: quarantine and start empty per AD11.
		quarantine := fmt.Sprintf("%s.corrupt-%s", m.opts.Path, time.Now().UTC().Format("20060102T150405Z"))
		if rerr := m.hooks.Rename(m.opts.Path, quarantine); rerr != nil {
			m.opts.Logger.Warn("runtime_state: corrupt-quarantine rename failed", "from", m.opts.Path, "to", quarantine, "error", rerr)
		} else {
			m.opts.Logger.Warn("runtime_state: corrupt file quarantined", "from", m.opts.Path, "to", quarantine, "parse_error", err)
		}
		m.replaceState(&State{SchemaVersion: SchemaVersion})
		return m.State(), nil
	}

	m.replaceState(state)
	return m.State(), nil
}

// EagerPersistInstanceGUID writes a minimum-valid file containing
// meta.{schema_version, instance_guid, written_at} within ~1s of the call
// (AD08). Closes the crash-before-first-periodic-persist window.
func (m *Manager) EagerPersistInstanceGUID(ctx context.Context, guid string, source IdentitySource) error {
	m.mu.Lock()
	if m.state == nil {
		m.state = &State{SchemaVersion: SchemaVersion}
	}
	m.state.SchemaVersion = SchemaVersion
	m.state.Meta.InstanceGUID = guid
	m.state.Meta.WrittenAt = time.Now().UTC()
	m.state.Meta.GatewayBuild = m.opts.GatewayBuild
	m.state.Meta.AddonVersion = m.opts.AddonVersion
	m.dirty = false // we'll persist now
	snap := *m.state
	m.mu.Unlock()

	m.opts.Metrics.OnIdentitySource(source)

	if err := m.persistLocked(&snap); err != nil {
		return err
	}
	return nil
}

// Start begins the periodic ticker (PersistInterval ± JitterRange/2) and
// the shutdown subscriber. Returns once the goroutines are running.
func (m *Manager) Start(ctx context.Context) error {
	m.mu.Lock()
	if m.started {
		m.mu.Unlock()
		return nil
	}
	m.started = true
	m.mu.Unlock()

	go m.tickerLoop()
	return nil
}

// Stop flushes any pending writes and shuts down the persister goroutines.
// Idempotent.
func (m *Manager) Stop(ctx context.Context) error {
	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		return nil
	}
	m.stopped = true
	started := m.started
	m.mu.Unlock()

	if started {
		close(m.stopCh)
		select {
		case <-m.doneCh:
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
			// Don't deadlock; ticker will exit on next iteration.
		}
	}
	// Final flush of any dirty state.
	m.flushIfDirty()
	return nil
}

// State returns a defensive snapshot of the current in-memory state.
func (m *Manager) State() *State {
	m.mu.Lock()
	defer m.mu.Unlock()
	return cloneState(m.state)
}

// UpdateSelf replaces ebus.self with the given Self and marks state dirty.
// Used after a successful SourceAddressSelection per AD14.
func (m *Manager) UpdateSelf(self Self) {
	m.mu.Lock()
	if m.state == nil {
		m.state = &State{SchemaVersion: SchemaVersion}
	}
	if m.state.EBus == nil {
		m.state.EBus = &EBusNamespace{SchemaVersion: EBusSchemaVersion}
	}
	cp := self
	m.state.EBus.Self = &cp
	m.dirty = true
	m.writeGen++
	m.mu.Unlock()
}

// UpsertKnownBusMember adds or updates an entry in ebus.known_bus_members[].
// Uniqueness on Addr is enforced (AD18); duplicate Addr replaces.
func (m *Manager) UpsertKnownBusMember(member KnownBusMember) {
	m.mu.Lock()
	if m.state == nil {
		m.state = &State{SchemaVersion: SchemaVersion}
	}
	if m.state.EBus == nil {
		m.state.EBus = &EBusNamespace{SchemaVersion: EBusSchemaVersion}
	}
	for i, existing := range m.state.EBus.KnownBusMembers {
		if existing.Addr == member.Addr {
			m.state.EBus.KnownBusMembers[i] = member
			m.dirty = true
			m.writeGen++
			m.mu.Unlock()
			return
		}
	}
	m.state.EBus.KnownBusMembers = append(m.state.EBus.KnownBusMembers, member)
	m.dirty = true
	m.writeGen++
	m.mu.Unlock()
}

// EvictKnownBusMember removes the entry with the given Addr. No-op if absent.
// Used by M5 directed-revalidation on no_reply outcomes (AD23).
func (m *Manager) EvictKnownBusMember(addr byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state == nil || m.state.EBus == nil {
		return
	}
	out := m.state.EBus.KnownBusMembers[:0]
	for _, member := range m.state.EBus.KnownBusMembers {
		if member.Addr != addr {
			out = append(out, member)
		}
	}
	if len(out) != len(m.state.EBus.KnownBusMembers) {
		m.dirty = true
		m.writeGen++
	}
	m.state.EBus.KnownBusMembers = out
}

// --- internal helpers ---

func (m *Manager) replaceState(s *State) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = s
	m.dirty = false
}

func cloneState(s *State) *State {
	if s == nil {
		return nil
	}
	cp := *s
	if s.EBus != nil {
		ebusCopy := *s.EBus
		if s.EBus.Self != nil {
			selfCopy := *s.EBus.Self
			if s.EBus.Self.CompanionTarget != nil {
				v := *s.EBus.Self.CompanionTarget
				selfCopy.CompanionTarget = &v
			}
			ebusCopy.Self = &selfCopy
		}
		members := make([]KnownBusMember, len(s.EBus.KnownBusMembers))
		for i, m := range s.EBus.KnownBusMembers {
			members[i] = m
			if m.CompanionAddr != nil {
				v := *m.CompanionAddr
				members[i].CompanionAddr = &v
			}
			if m.Identity != nil {
				idCopy := *m.Identity
				members[i].Identity = &idCopy
			}
		}
		ebusCopy.KnownBusMembers = members
		cp.EBus = &ebusCopy
	}
	return &cp
}

// flushIfDirty acquires the lock, snapshots state, persists if dirty.
func (m *Manager) flushIfDirty() {
	m.mu.Lock()
	if !m.dirty {
		m.mu.Unlock()
		return
	}
	if m.state == nil {
		m.dirty = false
		m.mu.Unlock()
		return
	}
	m.state.Meta.WrittenAt = time.Now().UTC()
	snap := cloneState(m.state)
	genAtSnapshot := m.writeGen
	m.mu.Unlock()
	// Only clear dirty AFTER a successful persist AND no mutation happened
	// during the persist call. On error (ENOSPC, fsync_temp, etc.) keep
	// dirty=true so the next ticker tick or shutdown retries (Codex R1 P2 —
	// otherwise transient errors would silently lose in-memory mutations).
	if err := m.persistLocked(snap); err != nil {
		m.opts.Logger.Warn("runtime_state: persist failed; keeping dirty for retry", "error", err)
		return
	}
	m.mu.Lock()
	if m.writeGen == genAtSnapshot {
		// No mutation happened during persist; safe to clear dirty.
		m.dirty = false
	}
	// else: a mutation slipped in while we were persisting; dirty stays true
	// and the next flushIfDirty cycle will pick up the new state.
	m.mu.Unlock()
}

// tickerLoop runs the 15-min jittered persist ticker.
func (m *Manager) tickerLoop() {
	defer close(m.doneCh)
	for {
		jitter := time.Duration(0)
		if m.opts.JitterRange > 0 {
			// rand reseeded per call to spread across simultaneous starts.
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			jitter = time.Duration(r.Int63n(int64(m.opts.JitterRange))) - (m.opts.JitterRange / 2)
		}
		next := m.opts.PersistInterval + jitter
		select {
		case <-time.After(next):
			m.flushIfDirty()
		case <-m.stopCh:
			return
		}
	}
}

// persistLocked atomically writes the snapshot to disk per AD13. Caller must
// NOT hold m.mu (we never call back into Manager). The snapshot is treated
// as authoritative for the write.
func (m *Manager) persistLocked(snap *State) error {
	data, err := marshalState(snap)
	if err != nil {
		m.opts.Metrics.OnWrite("marshal")
		return err
	}

	dir := filepath.Dir(m.opts.Path)
	tempName := filepath.Base(m.opts.Path) + ".tmp"
	tempPath := filepath.Join(dir, tempName)

	// Stage 1: WriteFile (creates+writes+closes).
	if err := m.hooks.WriteFile(tempPath, data, 0o644); err != nil {
		m.opts.Metrics.OnWrite("write")
		_ = m.hooks.Unlink(tempPath) // best-effort cleanup
		return err
	}

	// Stage 2: FsyncFile — REQUIRED.
	if err := m.hooks.FsyncFile(tempPath); err != nil {
		m.opts.Metrics.OnWrite("fsync_temp")
		_ = m.hooks.Unlink(tempPath)
		return err
	}

	// Stage 3: Rename temp → final. EXDEV is a precondition violation
	// (temp should always be in target dir); preserve old file per AD13.
	if err := m.hooks.Rename(tempPath, m.opts.Path); err != nil {
		if isExdev(err) {
			m.opts.Metrics.OnWrite("rename_exdev")
			_ = m.hooks.Unlink(tempPath)
			return err
		}
		m.opts.Metrics.OnWrite("rename")
		_ = m.hooks.Unlink(tempPath)
		return err
	}

	// Stage 4: FsyncDir — BEST-EFFORT.
	if err := m.hooks.FsyncDir(dir); err != nil {
		if isParentFsyncSwallowed(err) {
			m.opts.Metrics.OnWrite("parent_fsync_unsupported")
			// Fall through; write succeeded.
		} else {
			m.opts.Metrics.OnWrite("fsync_dir")
			// Don't undo the rename — file is on disk; just record metric.
		}
	}

	m.opts.Metrics.OnWrite("ok")
	return nil
}

// errNotImplemented retained for backward-compat in any test that imports
// the symbol; production Manager methods no longer return it.
var errNotImplemented = errors.New("runtimestate: legacy stub error (no longer used post-M2/M3)")
