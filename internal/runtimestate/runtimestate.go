// Package runtimestate is the gateway-owned loader/persister for
// /data/runtime_state.json. Plan: runtime-state-w19-26.locked. v1 namespaces:
// meta + ebus.{self, known_bus_members[]}. See docs-ebus
// architecture/runtime-state.md for the normative schema and contract.
//
// This file holds the package types + the public Manager surface. The
// implementations are SKELETON (M1_TDD_RED stubs); M2_GATEWAY_LOADER and
// M3_GATEWAY_PERSISTER replace them with real bodies that satisfy the
// contract tests in runtimestate_test.go.
package runtimestate

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// SchemaVersion is the file-format version this package targets.
const SchemaVersion = 1

// EBusSchemaVersion is the v1 ebus namespace version.
const EBusSchemaVersion = 1

// SelectionMethod enumerates the persisted join_method values per AD17.
type SelectionMethod string

const (
	SelectionMethodWarmup           SelectionMethod = "source_selection_warmup"
	SelectionMethodOverride         SelectionMethod = "override"
	SelectionMethodOverrideValidate SelectionMethod = "explicit_validate_only"
	SelectionMethodEbusdTCPFallback SelectionMethod = "ebusd-tcp-fallback"
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
	LastAdmittedAt        time.Time
	SelectionMethod        SelectionMethod
	CompanionTarget   *byte // nil when no valid companion per address-table AD03 bit-pattern rule.
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

func (NopMetrics) OnWrite(string)               {}
func (NopMetrics) OnIdentitySource(IdentitySource) {}
func (NopMetrics) OnRevalidate(string)           {}

// FilesystemHooks is an injection point for fault-tolerant testing of the
// AD13 atomic-write contract. Tests can supply a mock implementation that
// returns specific errno values (EXDEV, EINVAL, ENOSYS, etc.) to exercise
// the failure paths without requiring real filesystem misconfiguration.
// Production code uses DefaultFilesystemHooks{} (see fs_hooks.go in the
// M3_GATEWAY_PERSISTER PR), which delegates to the os package.
type FilesystemHooks interface {
	// Rename is called for the temp→final rename in the AD13 atomic-write
	// sequence. Implementations MUST return *PathError with err = syscall.EXDEV
	// when a cross-device rename is simulated.
	Rename(oldpath, newpath string) error
	// Unlink is called to clean up an orphaned temp file after a failed rename.
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
	opts Options
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
	return &Manager{opts: opts}
}

// errNotImplemented is returned by every method in the M1_TDD_RED skeleton.
// M2_GATEWAY_LOADER and M3_GATEWAY_PERSISTER replace these with real bodies.
var errNotImplemented = errors.New("runtimestate: not implemented (M2/M3 will provide)")

// Load reads the runtime state file. Returns the parsed State, or an empty
// State on missing/corrupt. On corrupt, the file is renamed to
// "<path>.corrupt-<ISO8601>" and a warning is logged. Per-plugin
// schema_version mismatch results in that namespace being dropped from the
// in-memory load (AD12). Never returns a panic; never blocks startup.
func (m *Manager) Load(ctx context.Context) (*State, error) {
	return nil, errNotImplemented
}

// EagerPersistInstanceGUID writes a minimum-valid file containing
// meta.{schema_version, instance_guid, written_at} within ~1s of the call
// (AD08). Closes the crash-before-first-periodic-persist window.
func (m *Manager) EagerPersistInstanceGUID(ctx context.Context, guid string, source IdentitySource) error {
	return errNotImplemented
}

// Start begins the periodic ticker (PersistInterval ± JitterRange/2) and
// the shutdown subscriber. Returns once the goroutines are running.
func (m *Manager) Start(ctx context.Context) error {
	return errNotImplemented
}

// Stop flushes any pending writes and shuts down the persister goroutines.
// Idempotent.
func (m *Manager) Stop(ctx context.Context) error {
	return errNotImplemented
}

// State returns a defensive copy of the current in-memory state. Safe to
// call concurrently with persistence.
func (m *Manager) State() *State {
	return nil
}

// UpdateSelf replaces ebus.self with the given Self and triggers a write.
// Used after a successful SourceAddressSelection per AD14.
func (m *Manager) UpdateSelf(self Self) {
}

// UpsertKnownBusMember adds or updates an entry in ebus.known_bus_members[].
// Uniqueness on Addr is enforced (AD18); duplicate Addr replaces.
func (m *Manager) UpsertKnownBusMember(member KnownBusMember) {
}

// EvictKnownBusMember removes the entry with the given Addr. No-op if absent.
// Used by M5 directed-revalidation on no_reply outcomes (AD23).
func (m *Manager) EvictKnownBusMember(addr byte) {
}
