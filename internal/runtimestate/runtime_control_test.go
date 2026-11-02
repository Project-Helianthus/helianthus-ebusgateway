package runtimestate

// Tests for the G5 runtime_control storage seam (S3.1-S3.6) and the local
// D3/D2 decision mirror. All fixtures are synthetic; driver IDs use generic
// families (fam.*) plus the lifecycle-only ebus.primary registration name.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

const rcGUID = "00000000-0000-4000-8000-0000000000a1"

// rcMaxRevision and rcOverflowRevision are computed, not literal.
var (
	rcMaxRevision      = strconv.FormatUint(math.MaxUint64, 10)
	rcOverflowRevision = rcMaxRevision[:len(rcMaxRevision)-1] + "6"
)

var rcFixedNow = time.Date(2030, 1, 2, 3, 4, 5, 0, time.FixedZone("plus3", 3*3600))

func rcNow() time.Time { return rcFixedNow }

// rcHooks delegates to the production filesystem hooks with injectable
// per-stage failures, an optional one-shot block inside WriteFile and a
// record of staged payloads.
type rcHooks struct {
	DefaultFilesystemHooks
	mu         sync.Mutex
	failWrite  error
	failFsync  error
	failRename error
	failDir    error
	blockNext  chan struct{} // when set, the next WriteFile blocks until closed
	entered    chan struct{}
	writes     [][]byte
}

func (h *rcHooks) set(fn func(*rcHooks)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	fn(h)
}

func (h *rcHooks) WriteFile(path string, data []byte, perm uint32) error {
	h.mu.Lock()
	block, entered, fail := h.blockNext, h.entered, h.failWrite
	h.blockNext = nil
	h.writes = append(h.writes, append([]byte(nil), data...))
	h.mu.Unlock()
	if block != nil {
		if entered != nil {
			close(entered)
		}
		<-block
	}
	if fail != nil {
		return fail
	}
	return h.DefaultFilesystemHooks.WriteFile(path, data, perm)
}

func (h *rcHooks) FsyncFile(path string) error {
	h.mu.Lock()
	fail := h.failFsync
	h.mu.Unlock()
	if fail != nil {
		return fail
	}
	return h.DefaultFilesystemHooks.FsyncFile(path)
}

func (h *rcHooks) Rename(oldpath, newpath string) error {
	h.mu.Lock()
	fail := h.failRename
	h.mu.Unlock()
	if fail != nil {
		return fail
	}
	return h.DefaultFilesystemHooks.Rename(oldpath, newpath)
}

func (h *rcHooks) FsyncDir(path string) error {
	h.mu.Lock()
	fail := h.failDir
	h.mu.Unlock()
	if fail != nil {
		return fail
	}
	return h.DefaultFilesystemHooks.FsyncDir(path)
}

func (h *rcHooks) writeCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.writes)
}

func rcPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "runtime_state.json")
}

func rcWriteBase(t *testing.T, path string) {
	t.Helper()
	body := fmt.Sprintf(`{"schema_version":1,"meta":{"instance_guid":%q,"written_at":"2001-02-03T04:05:06Z","addon_version":"0.0.0-rc"}}`, rcGUID)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write base: %v", err)
	}
}

func rcManager(t *testing.T, path string, hooks *rcHooks) *Manager {
	t.Helper()
	opts := Options{Path: path, GatewayBuild: "rc-build", AddonVersion: "0.0.0-rc", Now: rcNow, ControlWaitBound: 5 * time.Second}
	if hooks != nil {
		opts.FsHooks = hooks
	}
	m := New(opts)
	if _, err := m.Load(context.Background()); err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	return m
}

func rcStop(t *testing.T, m *Manager) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = m.Stop(ctx)
}

func rcCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

var rcDefaultConfigs = []ControlDriverConfig{{DriverID: "ebus.primary", Enabled: true}, {DriverID: "alpha.off", Enabled: false}}

// rcSeeded returns a manager with a seeded namespace: alpha.off STOPPED at 1,
// ebus.primary RUNNING at 2, mark 2, catalog 3.
func rcSeeded(t *testing.T, hooks *rcHooks) (*Manager, string) {
	t.Helper()
	path := rcPath(t)
	rcWriteBase(t, path)
	m := rcManager(t, path, hooks)
	if _, err := m.BootstrapControl(context.Background(), rcDefaultConfigs); err != nil {
		t.Fatalf("BootstrapControl() error = %v", err)
	}
	return m, path
}

func rcRefusal(t *testing.T, err error, code RefusalCode, row int) *ControlRefusal {
	t.Helper()
	var r *ControlRefusal
	if !errors.As(err, &r) {
		t.Fatalf("error = %v, want refusal %s row %d", err, code, row)
	}
	if r.Code != code || r.Row != row {
		t.Fatalf("refusal = %s row %d (%s), want %s row %d", r.Code, r.Row, r.Reason, code, row)
	}
	return r
}

func rcRead(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return data
}

func rcControlJSON(t *testing.T, data []byte) map[string]any {
	t.Helper()
	var root map[string]json.RawMessage
	if err := json.Unmarshal(data, &root); err != nil {
		t.Fatalf("state is not JSON: %v", err)
	}
	raw, ok := root[ControlNamespaceName]
	if !ok {
		return nil
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("runtime_control is not an object: %v", err)
	}
	return out
}

func rcSnapshot(t *testing.T, m *Manager) ControlCatalogView {
	t.Helper()
	view, err := m.ControlSnapshot()
	if err != nil {
		t.Fatalf("ControlSnapshot() error = %v", err)
	}
	return view
}

func rcDriver(t *testing.T, m *Manager, id string) ControlDriverView {
	t.Helper()
	v, err := m.ControlDriver(id)
	if err != nil {
		t.Fatalf("ControlDriver(%s) error = %v", id, err)
	}
	return v
}

func rcDecide(t *testing.T, m *Manager, method ControlMethod, id string, expected uint64) (ControlView, error) {
	t.Helper()
	return m.DecideControl(rcCtx(t), method, ControlRequest{DriverID: id, ExpectedRevision: expected}, nil)
}

// S3.4 / CTL-17 / D2: one-time seed only on genuinely missing state with a
// GUID; disabled seeds STOPPED; durable STOPPED survives reload; a driver
// added after bootstrap has no record and starts STOPPED.
func TestControlBootstrapSeedOnceAndStoppedSurvivesReload(t *testing.T) {
	path := rcPath(t)
	m := rcManager(t, path, nil)
	if got := m.ControlLoad().Disposition; got != ControlLoadMissing {
		t.Fatalf("fresh disposition = %s, want MISSING", got)
	}
	plan, err := m.BootstrapControl(context.Background(), rcDefaultConfigs)
	if !errors.Is(err, ErrControlBootstrapGUID) {
		t.Fatalf("seed without GUID error = %v, want ErrControlBootstrapGUID", err)
	}
	for _, entry := range plan {
		if entry.Action != StartupFailed {
			t.Fatalf("plan without GUID = %+v, want FAILED", entry)
		}
	}
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Fatalf("seed without GUID wrote a file")
	}
	if err := m.EagerPersistInstanceGUID(context.Background(), rcGUID, IdentitySourceRuntimeState); err != nil {
		t.Fatalf("EagerPersistInstanceGUID() error = %v", err)
	}
	plan, err = m.BootstrapControl(context.Background(), rcDefaultConfigs)
	if err != nil {
		t.Fatalf("seed error = %v", err)
	}
	want := []ControlStartup{
		{DriverID: "alpha.off", Action: StartupDisabled, DesiredState: DesiredStopped, DesiredRevision: 1},
		{DriverID: "ebus.primary", Action: StartupStarting, DesiredState: DesiredRunning, DesiredRevision: 2},
	}
	if !reflect.DeepEqual(plan, want) {
		t.Fatalf("seed plan = %+v, want %+v", plan, want)
	}
	snap := rcSnapshot(t, m)
	if snap.IssuedRevisionMark != 2 || snap.CatalogRevision != 3 {
		t.Fatalf("seed mark/catalog = %d/%d, want 2/3", snap.IssuedRevisionMark, snap.CatalogRevision)
	}
	view, err := rcDecide(t, m, ControlStop, "ebus.primary", 2)
	if err != nil || view != (ControlView{DesiredStopped, 3}) {
		t.Fatalf("STOP = %+v, %v; want STOPPED/3", view, err)
	}
	rcStop(t, m)

	// Reload: durable STOPPED survives, no reseed, no start action.
	m2 := rcManager(t, path, nil)
	if got := m2.ControlLoad().Disposition; got != ControlLoadLoaded {
		t.Fatalf("reload disposition = %s, want LOADED", got)
	}
	configs := append(append([]ControlDriverConfig(nil), rcDefaultConfigs...), ControlDriverConfig{DriverID: "fam.added", Enabled: true})
	plan, err = m2.BootstrapControl(context.Background(), configs)
	if err != nil {
		t.Fatalf("reload bootstrap error = %v", err)
	}
	want = []ControlStartup{
		{DriverID: "alpha.off", Action: StartupDisabled, DesiredState: DesiredStopped, DesiredRevision: 1},
		{DriverID: "ebus.primary", Action: StartupStopped, DesiredState: DesiredStopped, DesiredRevision: 3,
			Current: &ControlTransition{Kind: TransitionStop, DesiredRevision: 3, Sequence: 1, Phase: PhasePending}},
		{DriverID: "fam.added", Action: StartupStopped, DesiredState: DesiredStopped, DesiredRevision: 4},
	}
	if !reflect.DeepEqual(plan, want) {
		t.Fatalf("reload plan = %+v\nwant %+v", plan, want)
	}
	if v := rcDriver(t, m2, "fam.added"); v.HasRecord || v.DesiredRevision != 4 {
		t.Fatalf("added driver view = %+v, want no record at mark 4", v)
	}
	rcStop(t, m2)
}

// CTL-17 / CTL-18 / D2: disabled configuration wins over stored RUNNING,
// does not rewrite durable intent and refuses START and RESTART at row 4.
func TestControlDisabledConfigurationWinsWithoutRewritingIntent(t *testing.T) {
	m, path := rcSeeded(t, nil)
	rcStop(t, m)
	before := rcControlJSON(t, rcRead(t, path))
	m2 := rcManager(t, path, nil)
	plan, err := m2.BootstrapControl(context.Background(), []ControlDriverConfig{{DriverID: "ebus.primary", Enabled: false}, {DriverID: "alpha.off", Enabled: false}})
	if err != nil {
		t.Fatalf("bootstrap error = %v", err)
	}
	if plan[1].Action != StartupDisabled || plan[1].DesiredState != DesiredRunning {
		t.Fatalf("disabled plan = %+v, want DISABLED with stored RUNNING", plan[1])
	}
	for _, method := range []ControlMethod{ControlStart, ControlRestart, ControlStop} {
		_, err := rcDecide(t, m2, method, "ebus.primary", 2)
		_ = rcRefusal(t, err, RefusalFailedPrecondition, 4)
	}
	if after := rcControlJSON(t, rcRead(t, path)); !reflect.DeepEqual(before, after) {
		t.Fatalf("disabled configuration rewrote durable intent:\n%v\n%v", before, after)
	}
	rcStop(t, m2)
}

// CTL-04: pre-commit failures (temp write, temp fsync, rename, EXDEV) refuse
// with UNAVAILABLE, change nothing visible, discard staging and are never
// flushed by a later generic write.
func TestControlPreCommitFailuresDiscardStaging(t *testing.T) {
	cases := []struct {
		name   string
		inject func(*rcHooks)
	}{
		{"temp_write", func(h *rcHooks) { h.failWrite = syscall.ENOSPC }},
		{"temp_fsync", func(h *rcHooks) { h.failFsync = syscall.EIO }},
		{"rename", func(h *rcHooks) { h.failRename = syscall.EACCES }},
		{"rename_exdev", func(h *rcHooks) { h.failRename = syscall.EXDEV }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			hooks := &rcHooks{}
			m, path := rcSeeded(t, hooks)
			before := rcRead(t, path)
			hooks.set(tc.inject)
			_, err := rcDecide(t, m, ControlStop, "ebus.primary", 2)
			r := rcRefusal(t, err, RefusalUnavailable, 0)
			if !r.Retryable() {
				t.Fatalf("pre-commit UNAVAILABLE must be retryable")
			}
			if !bytes.Equal(rcRead(t, path), before) {
				t.Fatalf("pre-commit failure changed the state file")
			}
			if _, statErr := os.Stat(path + ".tmp"); !os.IsNotExist(statErr) {
				t.Fatalf("pre-commit failure left the temp file behind")
			}
			snap := rcSnapshot(t, m)
			if snap.IssuedRevisionMark != 2 || snap.CatalogRevision != 3 || snap.Drivers[1].DesiredState != DesiredRunning {
				t.Fatalf("pre-commit failure exposed staging: %+v", snap)
			}
			if !m.ControlAdmissionOpen() {
				t.Fatalf("pre-commit failure must not close admission")
			}
			// A later cache write publishes committed control only.
			hooks.set(func(h *rcHooks) { h.failWrite, h.failFsync, h.failRename = nil, nil, nil })
			m.UpsertKnownBusMember(KnownBusMember{Addr: 0x10, LastSource: LastSourcePassiveObserved, Confidence: ConfidenceCorroborated})
			if err := m.Flush(context.Background()); err != nil {
				t.Fatalf("Flush() error = %v", err)
			}
			ctl := rcControlJSON(t, rcRead(t, path))
			driver := ctl["drivers"].(map[string]any)["ebus.primary"].(map[string]any)
			if ctl["issued_revision_mark"] != "2" || driver["desired_state"] != "RUNNING" {
				t.Fatalf("ticker/cache write published rejected staging: %v", ctl)
			}
			// The revision was never issued: the next accepted decision takes 3.
			view, err := rcDecide(t, m, ControlStop, "ebus.primary", 2)
			if err != nil || view.DesiredRevision != 3 {
				t.Fatalf("retry = %+v, %v; want revision 3", view, err)
			}
			rcStop(t, m)
		})
	}
}

// CTL-04 / CTL-16: a failed parent-directory fsync after a successful rename
// is ambiguous persistence even for a generic-tolerated error. Nothing is
// acknowledged or exposed, admission closes for every driver, readback
// agreement alone does not reopen it and only established directory
// durability does.
func TestControlDirectoryFsyncFailureIsAmbiguousPersistence(t *testing.T) {
	hooks := &rcHooks{}
	m, path := rcSeeded(t, hooks)
	hooks.set(func(h *rcHooks) { h.failDir = syscall.ENOTSUP })
	_, err := rcDecide(t, m, ControlStop, "ebus.primary", 2)
	_ = rcRefusal(t, err, RefusalUnavailable, 3)
	if m.ControlAdmissionOpen() {
		t.Fatalf("admission open after ambiguous persistence")
	}
	if v := rcDriver(t, m, "ebus.primary"); v.DesiredState != DesiredRunning || v.DesiredRevision != 2 {
		t.Fatalf("ambiguous decision exposed: %+v", v)
	}
	if ctl := rcControlJSON(t, rcRead(t, path)); ctl["issued_revision_mark"] != "3" {
		t.Fatalf("expected the renamed bytes on disk, got %v", ctl)
	}
	for _, id := range []string{"ebus.primary", "alpha.off"} {
		_, err := rcDecide(t, m, ControlStart, id, 2)
		_ = rcRefusal(t, err, RefusalUnavailable, 3)
	}
	m.UpsertKnownBusMember(KnownBusMember{Addr: 0x11, LastSource: LastSourcePassiveObserved, Confidence: ConfidenceCorroborated})
	if err := m.Flush(context.Background()); !errors.Is(err, ErrControlReadbackPending) {
		t.Fatalf("generic flush during ambiguity = %v, want deferral", err)
	}
	// Readback agrees with the staged bytes but directory durability fails.
	if err := m.ResolveControlReadback(context.Background()); !errors.Is(err, ErrControlReadbackPending) {
		t.Fatalf("readback without directory durability = %v, want pending", err)
	}
	if m.ControlAdmissionOpen() {
		t.Fatalf("readback agreement alone reopened admission")
	}
	hooks.set(func(h *rcHooks) { h.failDir = nil })
	if err := m.ResolveControlReadback(context.Background()); err != nil {
		t.Fatalf("readback with directory durability = %v", err)
	}
	if !m.ControlAdmissionOpen() {
		t.Fatalf("admission closed after durable readback")
	}
	if v := rcDriver(t, m, "ebus.primary"); v.DesiredState != DesiredStopped || v.DesiredRevision != 3 {
		t.Fatalf("readback view = %+v, want STOPPED/3", v)
	}
	if err := m.Flush(context.Background()); err != nil {
		t.Fatalf("deferred flush after readback = %v", err)
	}
	rcStop(t, m)
}

// CTL-16 power-loss half: the previous mark is restored, the revision first
// believed issued is issued again, and a request holding it is refused.
func TestControlAmbiguousPersistencePowerLossReissuesRevision(t *testing.T) {
	hooks := &rcHooks{}
	m, path := rcSeeded(t, hooks)
	durable := rcRead(t, path)
	hooks.set(func(h *rcHooks) { h.failDir = syscall.EIO })
	_, err := rcDecide(t, m, ControlStop, "ebus.primary", 2)
	_ = rcRefusal(t, err, RefusalUnavailable, 3)
	rcStop(t, m)
	if err := os.WriteFile(path, durable, 0o600); err != nil { // the rename did not survive
		t.Fatalf("simulate power loss: %v", err)
	}
	m2 := rcManager(t, path, nil)
	if _, err := m2.BootstrapControl(context.Background(), rcDefaultConfigs); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	_, err = rcDecide(t, m2, ControlStop, "ebus.primary", 3)
	_ = rcRefusal(t, err, RefusalRevisionConflict, 6)
	view, err := rcDecide(t, m2, ControlStop, "ebus.primary", 2)
	if err != nil || view.DesiredRevision != 3 {
		t.Fatalf("reissue = %+v, %v; want revision 3 issued again", view, err)
	}
	rcStop(t, m2)
}

// CTL-04: a lost acknowledgment after a durable commit is resolved by
// readback; a same-desired retry is a no-op with no key or receipt lookup.
func TestControlLostAcknowledgmentResolvedByReadback(t *testing.T) {
	m, path := rcSeeded(t, nil)
	if _, err := rcDecide(t, m, ControlStop, "ebus.primary", 2); err != nil {
		t.Fatal(err)
	}
	rcStop(t, m) // response lost; process restarts
	hooks := &rcHooks{}
	m2 := rcManager(t, path, hooks)
	if _, err := m2.BootstrapControl(context.Background(), rcDefaultConfigs); err != nil {
		t.Fatal(err)
	}
	v := rcDriver(t, m2, "ebus.primary")
	if v.DesiredState != DesiredStopped || v.DesiredRevision != 3 || v.Current == nil || v.Current.Kind != TransitionStop {
		t.Fatalf("readback = %+v, want STOPPED/3 with typed STOP retained", v)
	}
	writes := hooks.writeCount()
	view, err := rcDecide(t, m2, ControlStop, "ebus.primary", 3)
	if err != nil || view != (ControlView{DesiredStopped, 3}) || hooks.writeCount() != writes {
		t.Fatalf("same-desired retry = %+v, %v, writes %d->%d", view, err, writes, hooks.writeCount())
	}
	rcStop(t, m2)
}

// Generic namespaces keep the best-effort directory fsync.
func TestControlGenericWritesKeepBestEffortDirectoryFsync(t *testing.T) {
	hooks := &rcHooks{}
	m, _ := rcSeeded(t, hooks)
	hooks.set(func(h *rcHooks) { h.failDir = syscall.ENOTSUP })
	m.UpsertKnownBusMember(KnownBusMember{Addr: 0x12, LastSource: LastSourcePassiveObserved, Confidence: ConfidenceCorroborated})
	if err := m.Flush(context.Background()); err != nil {
		t.Fatalf("generic flush with tolerated directory fsync error = %v", err)
	}
	if !m.ControlAdmissionOpen() {
		t.Fatalf("generic write closed control admission")
	}
	rcStop(t, m)
}

// S3.5 / D2: missing, corrupt, unreadable, unsupported and locked never
// collapse; failed loads never seed and preserve the bytes.
func TestControlLoadDispositionsNeverCollapse(t *testing.T) {
	base := `"schema_version":1,"meta":{"instance_guid":"` + rcGUID + `","written_at":"2001-02-03T04:05:06Z"}`
	type tc struct {
		name    string
		control string
		want    ControlLoadDisposition
	}
	namespaceCases := []tc{
		{"unsupported_version", `{"schema_version":2,"future":{"z":1,"a":[1,2]}}`, ControlLoadUnsupported},
		{"version_as_string", `{"schema_version":"1","issued_revision_mark":"0","catalog_revision":"1","drivers":{}}`, ControlLoadCorrupt},
		{"leading_zero_mark", `{"schema_version":1,"issued_revision_mark":"01","catalog_revision":"1","drivers":{}}`, ControlLoadCorrupt},
		{"mark_missing_with_records", `{"schema_version":1,"catalog_revision":"2","drivers":{"ebus.primary":{"desired_state":"RUNNING","desired_revision":"1","lifecycle_sequence":"0","current":null,"reserved":null}}}`, ControlLoadCorrupt},
		{"catalog_zero", `{"schema_version":1,"issued_revision_mark":"0","catalog_revision":"0","drivers":{}}`, ControlLoadCorrupt},
		{"unknown_member", `{"schema_version":1,"issued_revision_mark":"0","catalog_revision":"1","drivers":{},"instance_guid":"x"}`, ControlLoadCorrupt},
		{"duplicate_member", `{"schema_version":1,"issued_revision_mark":"0","issued_revision_mark":"5","catalog_revision":"1","drivers":{}}`, ControlLoadCorrupt},
		{"not_an_object", `[]`, ControlLoadCorrupt},
		{"recovery_digest_mismatch", `{"schema_version":1,"issued_revision_mark":"3","catalog_revision":"1","drivers":{},"recovery":{"format":"` + ControlRecoveryFormat + `","driver_ids":[],"max_desired_revision":"1","payload_sha256":"` + strings.Repeat("0", 64) + `","payload":"AA=="}}`, ControlLoadCorrupt},
	}
	for _, c := range namespaceCases {
		t.Run(c.name, func(t *testing.T) {
			path := rcPath(t)
			body := "{" + base + `,"zeta_plugin":{"k":1},"runtime_control":` + c.control + "}"
			if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
			m := rcManager(t, path, nil)
			status := m.ControlLoad()
			if status.Disposition != c.want || !status.Failed() {
				t.Fatalf("disposition = %+v, want failed %s", status, c.want)
			}
			plan, err := m.BootstrapControl(context.Background(), rcDefaultConfigs)
			if err != nil {
				t.Fatalf("bootstrap on failed load returned %v", err)
			}
			for _, entry := range plan {
				if entry.Action != StartupFailed {
					t.Fatalf("failed load produced %+v, want FAILED and no seed", entry)
				}
			}
			_, err = rcDecide(t, m, ControlStart, "ebus.primary", 1)
			_ = rcRefusal(t, err, RefusalFailedPrecondition, 3)
			if err := m.Flush(context.Background()); err != nil { // build metadata differs
				t.Fatalf("Flush() error = %v", err)
			}
			var root map[string]json.RawMessage
			if err := json.Unmarshal(rcRead(t, path), &root); err != nil {
				t.Fatal(err)
			}
			if !rcJSONEqual(t, root[ControlNamespaceName], []byte(c.control)) {
				t.Fatalf("namespace bytes not preserved:\n%s\nwant %s", root[ControlNamespaceName], c.control)
			}
			if !rcJSONEqual(t, root["zeta_plugin"], []byte(`{"k":1}`)) {
				t.Fatalf("unknown namespace not preserved")
			}
			if m.State().Meta.InstanceGUID != rcGUID {
				t.Fatalf("control load changed the GUID")
			}
			rcStop(t, m)
		})
	}

	t.Run("missing_file_without_material_is_missing", func(t *testing.T) {
		m := rcManager(t, rcPath(t), nil)
		if got := m.ControlLoad().Disposition; got != ControlLoadMissing {
			t.Fatalf("got %s", got)
		}
		rcStop(t, m)
	})
	t.Run("missing_namespace_with_quarantine_copy_fails_closed", func(t *testing.T) {
		path := rcPath(t)
		rcWriteBase(t, path)
		if err := os.WriteFile(path+controlQuarantineInfix+"20300101T000000Z", []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		m := rcManager(t, path, nil)
		if got := m.ControlLoad().Disposition; got != ControlLoadMissingWithMaterial {
			t.Fatalf("got %s, want MISSING_WITH_MATERIAL", got)
		}
		plan, _ := m.BootstrapControl(context.Background(), rcDefaultConfigs)
		if plan[0].Action != StartupFailed || rcControlJSON(t, rcRead(t, path)) != nil {
			t.Fatalf("seeded despite quarantine material")
		}
		rcStop(t, m)
	})
	t.Run("corrupt_file_quarantines_exact_bytes_and_never_seeds", func(t *testing.T) {
		path := rcPath(t)
		corrupt := []byte(`{"schema_version":1,"meta":` + "\x00garbage")
		if err := os.WriteFile(path, corrupt, 0o600); err != nil {
			t.Fatal(err)
		}
		m := rcManager(t, path, nil)
		if got := m.ControlLoad().Disposition; got != ControlLoadCorrupt {
			t.Fatalf("got %s", got)
		}
		copies, _ := filepath.Glob(path + corruptQuarantineInfix + "*")
		if len(copies) != 1 || !bytes.Equal(rcRead(t, copies[0]), corrupt) {
			t.Fatalf("quarantine copies = %v, want one exact copy", copies)
		}
		if m.State().Meta.InstanceGUID != "" {
			t.Fatalf("a GUID was invented")
		}
		plan, _ := m.BootstrapControl(context.Background(), rcDefaultConfigs)
		if plan[0].Action != StartupFailed {
			t.Fatalf("corrupt load seeded")
		}
		// The add-on supplies the GUID; the next start still fails closed.
		if err := m.EagerPersistInstanceGUID(context.Background(), rcGUID, IdentitySourceRuntimeState); err != nil {
			t.Fatal(err)
		}
		rcStop(t, m)
		m2 := rcManager(t, path, nil)
		if got := m2.ControlLoad().Disposition; got != ControlLoadMissingWithMaterial {
			t.Fatalf("restart after corrupt quarantine = %s, want MISSING_WITH_MATERIAL", got)
		}
		rcStop(t, m2)
	})
	t.Run("second_corruption_never_replaces_a_quarantine_copy", func(t *testing.T) {
		path := rcPath(t)
		for i, body := range []string{"{first", "{second"} {
			if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
			m := rcManager(t, path, nil)
			rcStop(t, m)
			copies, _ := filepath.Glob(path + corruptQuarantineInfix + "*")
			if len(copies) != i+1 {
				t.Fatalf("copies after corruption %d = %v", i+1, copies)
			}
		}
	})
	t.Run("unreadable_file_blocks_persistence", func(t *testing.T) {
		path := rcPath(t)
		if err := os.Mkdir(path, 0o700); err != nil { // reading a directory fails
			t.Fatal(err)
		}
		m := rcManager(t, path, nil)
		if got := m.ControlLoad().Disposition; got != ControlLoadUnreadable {
			t.Fatalf("got %s, want UNREADABLE", got)
		}
		if err := m.EagerPersistInstanceGUID(context.Background(), rcGUID, IdentitySourceRuntimeState); !errors.Is(err, ErrStatePersistBlocked) {
			t.Fatalf("persist over unreadable bytes = %v, want blocked", err)
		}
		if info, err := os.Stat(path); err != nil || !info.IsDir() {
			t.Fatalf("unreadable path was replaced")
		}
		rcStop(t, m)
	})
	t.Run("store_lock_held_by_another_writer", func(t *testing.T) {
		path := rcPath(t)
		rcWriteBase(t, path)
		lock, err := os.OpenFile(path+".lock", os.O_RDWR|os.O_CREATE, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := lock.Close(); err != nil {
				t.Errorf("close store lock: %v", err)
			}
		}()
		if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
			t.Fatal(err)
		}
		before := rcRead(t, path)
		m := rcManager(t, path, nil)
		if got := m.ControlLoad().Disposition; got != ControlLoadLocked {
			t.Fatalf("got %s, want LOCKED", got)
		}
		if err := m.Flush(context.Background()); !errors.Is(err, ErrStatePersistBlocked) {
			t.Fatalf("flush while another writer holds the lock = %v", err)
		}
		if !bytes.Equal(rcRead(t, path), before) {
			t.Fatalf("locked store was written")
		}
		rcStop(t, m)
	})
}

// A failed sidecar open must never allow generic or control persistence.
func TestControlStoreLockOpenFailureBlocksPersistence(t *testing.T) {
	path := rcPath(t)
	rcWriteBase(t, path)
	before := rcRead(t, path)
	if err := os.Mkdir(path+".lock", 0o700); err != nil {
		t.Fatal(err)
	}
	m := rcManager(t, path, nil)
	defer rcStop(t, m)
	if got := m.ControlLoad().Disposition; got != ControlLoadLocked {
		t.Fatalf("got %s, want LOCKED", got)
	}
	if err := m.EagerPersistInstanceGUID(context.Background(), rcGUID, IdentitySourceRuntimeState); !errors.Is(err, ErrStatePersistBlocked) {
		t.Fatalf("persist without a lock = %v, want blocked", err)
	}
	plan, err := m.BootstrapControl(context.Background(), rcDefaultConfigs)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range plan {
		if entry.Action != StartupFailed {
			t.Fatalf("bootstrap without a lock = %+v", plan)
		}
	}
	if !bytes.Equal(rcRead(t, path), before) {
		t.Fatal("store changed without owning its lock")
	}
}

func TestControlStoreLockNonContentionFailure(t *testing.T) {
	path := rcPath(t)
	m := New(Options{Path: path})
	var fd int
	err := m.acquireStoreLock(func(openFD, flags int) error {
		fd = openFD
		if flags != unix.LOCK_EX|unix.LOCK_NB {
			t.Fatalf("flock flags = %d", flags)
		}
		return unix.EIO
	})
	if !errors.Is(err, unix.EIO) || m.lockFile != nil {
		t.Fatalf("failed flock = %v, lock file = %v", err, m.lockFile)
	}
	if _, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); !errors.Is(err, unix.EBADF) {
		t.Fatalf("failed flock descriptor was not closed: %v", err)
	}
	if err := m.acquireStoreLock(unix.Flock); err != nil {
		t.Fatalf("reacquire after failed flock: %v", err)
	}
	m.releaseStoreLock()
}

func rcJSONEqual(t *testing.T, a, b []byte) bool {
	t.Helper()
	var ca, cb bytes.Buffer
	if err := json.Compact(&ca, a); err != nil {
		return false
	}
	if err := json.Compact(&cb, b); err != nil {
		return false
	}
	return bytes.Equal(ca.Bytes(), cb.Bytes())
}

// S3.5 / CTL-14: an invalid known driver record fails only that driver while
// an independent driver stays usable; its bytes are preserved.
func TestControlInvalidDriverRecordFailsOnlyThatDriver(t *testing.T) {
	path := rcPath(t)
	broken := `{"desired_state":"PAUSED","desired_revision":"2","lifecycle_sequence":"0","current":null,"reserved":null}`
	body := `{"schema_version":1,"meta":{"instance_guid":"` + rcGUID + `","written_at":"2001-02-03T04:05:06Z"},` +
		`"runtime_control":{"schema_version":1,"issued_revision_mark":"2","catalog_revision":"3","drivers":{` +
		`"ebus.primary":{"desired_state":"RUNNING","desired_revision":"1","lifecycle_sequence":"0","current":null,"reserved":null},` +
		`"fam.broken":` + broken + `}}}`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	m := rcManager(t, path, nil)
	if got := m.ControlLoad().Disposition; got != ControlLoadLoaded {
		t.Fatalf("disposition = %s, want LOADED", got)
	}
	plan, err := m.BootstrapControl(context.Background(), []ControlDriverConfig{{DriverID: "ebus.primary", Enabled: true}, {DriverID: "fam.broken", Enabled: true}})
	if err != nil {
		t.Fatal(err)
	}
	if plan[0].Action != StartupStarting || plan[1].Action != StartupFailed {
		t.Fatalf("plan = %+v, want STARTING and FAILED", plan)
	}
	if v := rcDriver(t, m, "fam.broken"); !v.Failed {
		t.Fatalf("broken driver view = %+v, want Failed", v)
	}
	_, err = rcDecide(t, m, ControlStart, "fam.broken", 2)
	_ = rcRefusal(t, err, RefusalFailedPrecondition, 3)
	if view, err := rcDecide(t, m, ControlStop, "ebus.primary", 1); err != nil || view.DesiredRevision != 3 {
		t.Fatalf("independent driver decision = %+v, %v", view, err)
	}
	var file struct {
		Control struct {
			Drivers map[string]json.RawMessage `json:"drivers"`
		} `json:"runtime_control"`
	}
	if err := json.Unmarshal(rcRead(t, path), &file); err != nil {
		t.Fatal(err)
	}
	raw := file.Control.Drivers["fam.broken"]
	if !rcJSONEqual(t, raw, []byte(broken)) {
		t.Fatalf("broken record bytes not preserved: %s", raw)
	}
	rcStop(t, m)
}

// CTL-05 / CTL-15 / refusal row 1: closed request decoding.
func TestControlParseRequestIsClosed(t *testing.T) {
	invalid := []string{
		`{"DriverID":"ebus.primary","ExpectedRevision":"0"}`,
		`{"DriverID":"ebus.primary","ExpectedRevision":"01"}`,
		`{"DriverID":"ebus.primary","ExpectedRevision":"+1"}`,
		`{"DriverID":"ebus.primary","ExpectedRevision":" 1"}`,
		`{"DriverID":"ebus.primary","ExpectedRevision":"1x"}`,
		`{"DriverID":"ebus.primary","ExpectedRevision":2}`,
		`{"DriverID":"ebus.primary","ExpectedRevision":"` + rcOverflowRevision + `"}`,
		`{"DriverID":"ebus.primary"}`,
		`{"DriverID":"ebus.primary","ExpectedRevision":"2","idempotency_key":"k"}`,
		`{"DriverID":"ebus.primary","ExpectedRevision":"2","request_id":"r"}`,
		`{"DriverID":"ebus.primary","DriverID":"ebus.primary","ExpectedRevision":"2"}`,
		`{"DriverID":"ebus.primary","ExpectedRevision":"2"} {}`,
		`{"DriverID":7,"ExpectedRevision":"2"}`,
		`[]`,
	}
	for _, body := range invalid {
		_, err := ParseControlRequest([]byte(body))
		var r *ControlRefusal
		if !errors.As(err, &r) || r.Code != RefusalInvalidArgument || r.Row != 1 {
			t.Errorf("ParseControlRequest(%s) = %v, want INVALID_ARGUMENT row 1", body, err)
		}
	}
	req, err := ParseControlRequest([]byte(`{"ExpectedRevision":"` + rcMaxRevision + `","DriverID":"Not.Canonical"}`))
	if err != nil || req.ExpectedRevision != math.MaxUint64 || req.DriverID != "Not.Canonical" {
		t.Fatalf("valid request = %+v, %v", req, err)
	}
	if fields := reflect.TypeOf(ControlRequest{}).NumField(); fields != 2 {
		t.Fatalf("ControlRequest has %d fields, want exactly DriverID and ExpectedRevision", fields)
	}
}

// CTL-03 / CTL-06 / CTL-07 / CTL-11 / CTL-14 / CTL-18: refusal order, no-ops
// and bounded typed slots, each refusal leaving durable state unchanged.
func TestControlDecisionRefusalOrderNoOpsAndSlots(t *testing.T) {
	hooks := &rcHooks{}
	m, path := rcSeeded(t, hooks)
	noDeadline := context.Background()
	expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	unchanged := func(t *testing.T, before []byte) {
		t.Helper()
		if !bytes.Equal(rcRead(t, path), before) {
			t.Fatalf("refusal mutated durable state")
		}
	}
	before := rcRead(t, path)
	steps := []struct {
		name   string
		ctx    context.Context
		method ControlMethod
		id     string
		rev    uint64
		code   RefusalCode
		row    int
	}{
		{"zero_revision_beats_everything", noDeadline, ControlStart, "Bad.ID", 0, RefusalInvalidArgument, 1},
		{"unknown_method", noDeadline, "PAUSE", "ebus.primary", 2, RefusalInvalidArgument, 1},
		{"noncanonical_before_deadline", noDeadline, ControlStart, "Ebus.primary", 2, RefusalNotFound, 2},
		{"unregistered", expired, ControlStart, "fam.unknown", 2, RefusalNotFound, 2},
		{"disabled_before_deadline", expired, ControlStart, "alpha.off", 1, RefusalFailedPrecondition, 4},
		{"no_finite_deadline", noDeadline, ControlStart, "ebus.primary", 2, RefusalInvalidArgument, 5},
		{"expired_before_conflict", expired, ControlStop, "ebus.primary", 1, RefusalDeadlineExceeded, 5},
	}
	for _, s := range steps {
		_, err := m.DecideControl(s.ctx, s.method, ControlRequest{DriverID: s.id, ExpectedRevision: s.rev}, nil)
		if r := rcRefusal(t, err, s.code, s.row); r.View != nil {
			t.Fatalf("%s: unexpected view", s.name)
		}
	}
	unchanged(t, before)
	writes := hooks.writeCount()
	// Stale expectation conflicts even for a same-desired request.
	_, err := rcDecide(t, m, ControlStart, "ebus.primary", 1)
	if r := rcRefusal(t, err, RefusalRevisionConflict, 6); r.View == nil || *r.View != (ControlView{DesiredRunning, 2}) {
		t.Fatalf("conflict view = %+v", r.View)
	}
	// Same-desired START is a no-op: no write, no counter.
	if view, err := rcDecide(t, m, ControlStart, "ebus.primary", 2); err != nil || view != (ControlView{DesiredRunning, 2}) {
		t.Fatalf("no-op = %+v, %v", view, err)
	}
	if hooks.writeCount() != writes {
		t.Fatalf("refusal or no-op wrote state")
	}
	unchanged(t, before)
	// STOP occupies the current slot; RESTART in desired STOPPED refuses at
	// row 6 before any capacity check.
	if view, err := rcDecide(t, m, ControlStop, "ebus.primary", 2); err != nil || view.DesiredRevision != 3 {
		t.Fatalf("STOP = %+v, %v", view, err)
	}
	_, err = rcDecide(t, m, ControlRestart, "ebus.primary", 3)
	_ = rcRefusal(t, err, RefusalFailedPrecondition, 6)
	// STOP during STOPPING is a no-op; START takes the reserved slot.
	if view, err := rcDecide(t, m, ControlStop, "ebus.primary", 3); err != nil || view.DesiredRevision != 3 {
		t.Fatalf("STOP during STOPPING = %+v, %v", view, err)
	}
	if view, err := rcDecide(t, m, ControlStart, "ebus.primary", 3); err != nil || view.DesiredRevision != 4 {
		t.Fatalf("START during STOPPING = %+v, %v", view, err)
	}
	v := rcDriver(t, m, "ebus.primary")
	if v.Current == nil || v.Current.Kind != TransitionStop || v.Reserved == nil || v.Reserved.Kind != TransitionStart ||
		v.Reserved.DesiredRevision != 4 || v.Reserved.Sequence != 2 {
		t.Fatalf("slots = %+v / %+v", v.Current, v.Reserved)
	}
	// Both slots occupied: changing requests are busy, same-desired is a no-op.
	writes = hooks.writeCount()
	before = rcRead(t, path)
	_, err = rcDecide(t, m, ControlStop, "ebus.primary", 4)
	_ = rcRefusal(t, err, RefusalTransitionBusy, 8)
	_, err = rcDecide(t, m, ControlRestart, "ebus.primary", 4)
	_ = rcRefusal(t, err, RefusalTransitionBusy, 8)
	if view, err := rcDecide(t, m, ControlStart, "ebus.primary", 4); err != nil || view.DesiredRevision != 4 {
		t.Fatalf("same-desired during occupied slots = %+v, %v", view, err)
	}
	if hooks.writeCount() != writes {
		t.Fatalf("busy or no-op wrote state")
	}
	unchanged(t, before)
	// Complete the current work, promote the reserved work, then settle it.
	// RESTART takes the current slot; further changing requests are busy.
	if err := m.CommitControlWork(rcCtx(t), "ebus.primary", v.Reserved, nil); err != nil {
		t.Fatalf("reserved promotion = %v", err)
	}
	if got := rcDriver(t, m, "ebus.primary"); !reflect.DeepEqual(got.Current, v.Reserved) || got.Reserved != nil {
		t.Fatalf("reserved promotion lost work: %+v", got)
	}
	if err := m.CommitControlWork(rcCtx(t), "ebus.primary", nil, nil); err != nil {
		t.Fatalf("CommitControlWork() = %v", err)
	}
	if view, err := rcDecide(t, m, ControlRestart, "ebus.primary", 4); err != nil || view != (ControlView{DesiredRunning, 5}) {
		t.Fatalf("RESTART = %+v, %v", view, err)
	}
	_, err = rcDecide(t, m, ControlRestart, "ebus.primary", 5)
	_ = rcRefusal(t, err, RefusalTransitionBusy, 8)
	// A planner may refuse unsafe admission at row 7.
	quarantine := func(ControlPlanInput) (ControlPlan, *ControlRefusal) {
		return ControlPlan{}, refuse(RefusalFailedPrecondition, 7, "unresolved close obligation")
	}
	_, err = m.DecideControl(rcCtx(t), ControlStop, ControlRequest{DriverID: "ebus.primary", ExpectedRevision: 5}, quarantine)
	_ = rcRefusal(t, err, RefusalFailedPrecondition, 7)
	rcStop(t, m)
}

// CTL-19: one namespace-wide serialization point; a waiting request re-reads
// the mark; a bounded-wait timeout answers REVISION_CONFLICT only when the
// expectation is conclusively stale and TRANSITION_BUSY otherwise.
func TestControlSerializationPointRereadsTheMark(t *testing.T) {
	setup := func(t *testing.T) (*Manager, *rcHooks, uint64) {
		hooks := &rcHooks{}
		path := rcPath(t)
		rcWriteBase(t, path)
		m := rcManager(t, path, hooks)
		if _, err := m.BootstrapControl(context.Background(), []ControlDriverConfig{{DriverID: "fam.a", Enabled: true}}); err != nil {
			t.Fatal(err)
		}
		if _, err := m.BootstrapControl(context.Background(), []ControlDriverConfig{{DriverID: "fam.a", Enabled: true}, {DriverID: "fam.b", Enabled: true}}); err != nil {
			t.Fatal(err)
		}
		// fam.a STOP takes 3, so H=3 is both fam.a's revision and the mark
		// that fam.b (no record) reads.
		if v, err := rcDecide(t, m, ControlStop, "fam.a", 1); err != nil || v.DesiredRevision != 3 {
			t.Fatalf("setup STOP = %+v, %v", v, err)
		}
		return m, hooks, 3
	}
	type result struct {
		view ControlView
		err  error
	}
	race := func(t *testing.T, m *Manager, hooks *rcHooks, first, second ControlRequest) (result, result) {
		release, entered := make(chan struct{}), make(chan struct{})
		hooks.set(func(h *rcHooks) { h.blockNext, h.entered = release, entered })
		var wg sync.WaitGroup
		var err1, err2 error
		var v1, v2 ControlView
		wg.Add(1)
		go func() {
			defer wg.Done()
			v1, err1 = m.DecideControl(rcCtx(t), ControlStart, first, nil)
		}()
		<-entered // first holds the serialization point inside its write
		wg.Add(1)
		started := make(chan struct{})
		go func() {
			defer wg.Done()
			close(started)
			v2, err2 = m.DecideControl(rcCtx(t), ControlStart, second, nil)
		}()
		<-started
		time.Sleep(50 * time.Millisecond) // second is past validation and waiting
		close(release)
		wg.Wait()
		return result{view: v1, err: err1}, result{view: v2, err: err2}
	}

	t.Run("same_driver_without_record_exactly_one_accepted", func(t *testing.T) {
		m, hooks, h := setup(t)
		req := ControlRequest{DriverID: "fam.b", ExpectedRevision: h}
		first, second := race(t, m, hooks, req, req)
		if first.err != nil || first.view.DesiredRevision != h+1 {
			t.Fatalf("first = %+v, %v", first.view, first.err)
		}
		if r := rcRefusal(t, second.err, RefusalRevisionConflict, 6); r.View == nil || r.View.DesiredRevision != h+1 {
			t.Fatalf("second view = %+v", r.View)
		}
		if snap := rcSnapshot(t, m); snap.IssuedRevisionMark != h+1 {
			t.Fatalf("mark = %d, want %d (advanced once)", snap.IssuedRevisionMark, h+1)
		}
		rcStop(t, m)
	})
	t.Run("recorded_driver_first_then_driver_without_record_conflicts", func(t *testing.T) {
		m, hooks, h := setup(t)
		first, second := race(t, m, hooks, ControlRequest{DriverID: "fam.a", ExpectedRevision: h}, ControlRequest{DriverID: "fam.b", ExpectedRevision: h})
		if first.err != nil || first.view.DesiredRevision != h+1 {
			t.Fatalf("fam.a = %+v, %v", first.view, first.err)
		}
		_ = rcRefusal(t, second.err, RefusalRevisionConflict, 6)
		if snap := rcSnapshot(t, m); snap.IssuedRevisionMark != h+1 {
			t.Fatalf("mark = %d", snap.IssuedRevisionMark)
		}
		rcStop(t, m)
	})
	t.Run("driver_without_record_first_then_recorded_driver_keeps_its_own_revision", func(t *testing.T) {
		m, hooks, h := setup(t)
		first, second := race(t, m, hooks, ControlRequest{DriverID: "fam.b", ExpectedRevision: h}, ControlRequest{DriverID: "fam.a", ExpectedRevision: h})
		// fam.a's own desired revision is still h, so its comparison matches;
		// it is decided against the re-read mark and takes h+2, never h+1.
		if first.err != nil || first.view.DesiredRevision != h+1 || second.err != nil || second.view.DesiredRevision != h+2 {
			t.Fatalf("results = %+v %v / %+v %v", first.view, first.err, second.view, second.err)
		}
		rcStop(t, m)
	})
	t.Run("bounded_wait_timeout", func(t *testing.T) {
		m, hooks, h := setup(t)
		m.opts.ControlWaitBound = 100 * time.Millisecond
		release, entered := make(chan struct{}), make(chan struct{})
		hooks.set(func(h *rcHooks) { h.blockNext, h.entered = release, entered })
		done := make(chan error, 1)
		go func() {
			_, err := m.DecideControl(rcCtx(t), ControlStart, ControlRequest{DriverID: "fam.a", ExpectedRevision: h}, nil)
			done <- err
		}()
		<-entered
		_, err := rcDecide(t, m, ControlStart, "fam.b", h-1)
		if r := rcRefusal(t, err, RefusalRevisionConflict, 6); r.View == nil || r.View.DesiredRevision != h {
			t.Fatalf("stale waiter view = %+v", r.View)
		}
		_, err = rcDecide(t, m, ControlStart, "fam.b", h)
		if r := rcRefusal(t, err, RefusalTransitionBusy, 8); r.View != nil {
			t.Fatalf("TRANSITION_BUSY reported a comparison outcome")
		}
		// Same-desired would-be no-op past its deadline: DEADLINE_EXCEEDED.
		short, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()
		_, err = m.DecideControl(short, ControlStop, ControlRequest{DriverID: "fam.b", ExpectedRevision: h}, nil)
		_ = rcRefusal(t, err, RefusalDeadlineExceeded, 5)
		if snap := rcSnapshot(t, m); snap.IssuedRevisionMark != h {
			t.Fatalf("waiters advanced the mark: %d", snap.IssuedRevisionMark)
		}
		close(release)
		if err := <-done; err != nil {
			t.Fatalf("holder = %v", err)
		}
		rcStop(t, m)
	})
}

// S3.2: concurrent cache and control writes serialize through the sole
// writer; every acknowledged decision survives and no revision repeats.
func TestControlConcurrentCacheAndControlWrites(t *testing.T) {
	path := rcPath(t)
	rcWriteBase(t, path)
	m := rcManager(t, path, nil)
	ids := []string{"fam.a", "fam.b", "fam.c", "fam.d"}
	var configs []ControlDriverConfig
	for _, id := range ids {
		configs = append(configs, ControlDriverConfig{DriverID: id, Enabled: true})
	}
	if _, err := m.BootstrapControl(context.Background(), configs); err != nil {
		t.Fatal(err)
	}
	const rounds = 8
	var wg sync.WaitGroup
	var mu sync.Mutex
	issued := map[uint64]string{}
	final := map[string]ControlView{}
	errs := make(chan error, 64)
	for _, id := range ids {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			view, err := m.ControlDriver(id)
			if err != nil {
				errs <- err
				return
			}
			rev := view.DesiredRevision
			for i := 0; i < rounds; i++ {
				method := ControlStop
				if i%2 == 1 {
					method = ControlStart
				}
				if err := m.CommitControlWork(rcCtx(t), id, nil, nil); err != nil {
					errs <- err
					return
				}
				v, err := m.DecideControl(rcCtx(t), method, ControlRequest{DriverID: id, ExpectedRevision: rev}, nil)
				if err != nil {
					errs <- fmt.Errorf("%s round %d: %w", id, i, err)
					return
				}
				mu.Lock()
				if prior, dup := issued[v.DesiredRevision]; dup {
					errs <- fmt.Errorf("revision %d issued to %s and %s", v.DesiredRevision, prior, id)
				}
				issued[v.DesiredRevision] = id
				final[id] = v
				mu.Unlock()
				rev = v.DesiredRevision
			}
		}(id)
	}
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 40; i++ {
				m.RefreshKnownBusMemberPresence(byte(w*64+i), rcFixedNow, LastSourcePassiveObserved)
				if err := m.Flush(context.Background()); err != nil {
					errs <- err
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	rcStop(t, m)
	m2 := rcManager(t, path, nil)
	if _, err := m2.BootstrapControl(context.Background(), configs); err != nil {
		t.Fatal(err)
	}
	snap := rcSnapshot(t, m2)
	if want := uint64(len(ids) + len(ids)*rounds); snap.IssuedRevisionMark != want {
		t.Fatalf("mark = %d, want %d", snap.IssuedRevisionMark, want)
	}
	for _, d := range snap.Drivers {
		if f := final[d.DriverID]; d.DesiredRevision != f.DesiredRevision || d.DesiredState != f.DesiredState {
			t.Fatalf("%s durable %s/%d, acknowledged %+v", d.DriverID, d.DesiredState, d.DesiredRevision, f)
		}
	}
	if got := len(m2.State().EBus.KnownBusMembers); got != 80 {
		t.Fatalf("cache members = %d, want 80", got)
	}
	rcStop(t, m2)
}

func rcCounterFixture(t *testing.T, mark, catalog, rev, seq string) (*Manager, string) {
	t.Helper()
	path := rcPath(t)
	body := `{"schema_version":1,"meta":{"instance_guid":"` + rcGUID + `","written_at":"2001-02-03T04:05:06Z"},` +
		`"runtime_control":{"schema_version":1,"issued_revision_mark":"` + mark + `","catalog_revision":"` + catalog + `","drivers":{` +
		`"ebus.primary":{"desired_state":"RUNNING","desired_revision":"` + rev + `","lifecycle_sequence":"` + seq + `","current":null,"reserved":null}}}}`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	m := rcManager(t, path, nil)
	if got := m.ControlLoad().Disposition; got != ControlLoadLoaded {
		t.Fatalf("fixture disposition = %s (%s)", got, m.ControlLoad().Detail)
	}
	return m, path
}

// Counter bounds: every increment refuses COUNTER_EXHAUSTED before mutation;
// no-ops still answer at the maximum.
func TestControlCounterBounds(t *testing.T) {
	max := rcMaxRevision
	cases := []struct {
		name                    string
		mark, catalog, rev, seq string
	}{
		{"mark", max, "5", max, "0"},
		{"catalog", "7", max, "7", "0"},
		{"lifecycle_sequence", "7", "5", "7", max},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m, path := rcCounterFixture(t, c.mark, c.catalog, c.rev, c.seq)
			if _, err := m.BootstrapControl(context.Background(), []ControlDriverConfig{{DriverID: "ebus.primary", Enabled: true}}); err != nil {
				t.Fatal(err)
			}
			before := rcRead(t, path)
			rev, _ := ParseControlRevision(c.rev, false)
			if view, err := rcDecide(t, m, ControlStart, "ebus.primary", rev); err != nil || view.DesiredRevision != rev {
				t.Fatalf("no-op at bound = %+v, %v", view, err)
			}
			_, err := rcDecide(t, m, ControlStop, "ebus.primary", rev)
			_ = rcRefusal(t, err, RefusalCounterExhausted, 10)
			if !bytes.Equal(rcRead(t, path), before) {
				t.Fatalf("exhaustion mutated state")
			}
			rcStop(t, m)
		})
	}
	t.Run("registration_mark_overflow", func(t *testing.T) {
		m, path := rcCounterFixture(t, max, "5", max, "0")
		before := rcRead(t, path)
		_, err := m.BootstrapControl(context.Background(), []ControlDriverConfig{{DriverID: "ebus.primary", Enabled: true}, {DriverID: "fam.new", Enabled: true}})
		_ = rcRefusal(t, err, RefusalCounterExhausted, 10)
		if !bytes.Equal(rcRead(t, path), before) {
			t.Fatalf("registration overflow mutated state")
		}
		rcStop(t, m)
	})
	t.Run("catalog_advance_overflow", func(t *testing.T) {
		m, _ := rcCounterFixture(t, "7", max, "7", "0")
		_, err := m.AdvanceControlCatalog(rcCtx(t))
		_ = rcRefusal(t, err, RefusalCounterExhausted, 10)
		rcStop(t, m)
	})
	t.Run("catalog_advance_persists_before_view", func(t *testing.T) {
		m, path := rcCounterFixture(t, "7", "5", "7", "0")
		next, err := m.AdvanceControlCatalog(rcCtx(t))
		if err != nil || next != 6 || rcControlJSON(t, rcRead(t, path))["catalog_revision"] != "6" {
			t.Fatalf("advance = %d, %v", next, err)
		}
		rcStop(t, m)
	})
	if _, ok := ParseControlRevision(max, false); !ok {
		t.Fatalf("maximum revision rejected")
	}
	if _, ok := ParseControlRevision(rcOverflowRevision, true); ok {
		t.Fatalf("revision above the maximum accepted")
	}
}

// Row 9: record and byte limits refuse RESOURCE_EXHAUSTED before acceptance.
func TestControlStorageLimits(t *testing.T) {
	t.Run("record_limit", func(t *testing.T) {
		path := rcPath(t)
		var records []string
		for i := 0; i < MaxControlDriverRecords; i++ {
			records = append(records, fmt.Sprintf(`"fam.r%03d":{"desired_state":"STOPPED","desired_revision":"1","lifecycle_sequence":"1","current":{"kind":"STOP","desired_revision":"1","sequence":"1","phase":"CLEANUP","epoch":"","generation":"0","native_close_pending":true,"semantic_cleanup_pending":false,"attempts_used":0,"deadline_utc":""},"reserved":null}`, i))
		}
		body := `{"schema_version":1,"meta":{"instance_guid":"` + rcGUID + `","written_at":"2001-02-03T04:05:06Z"},` +
			`"runtime_control":{"schema_version":1,"issued_revision_mark":"200","catalog_revision":"1","drivers":{` + strings.Join(records, ",") + `}}}`
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		m := rcManager(t, path, nil)
		// Records with retained work are not removed although unconfigured.
		if _, err := m.BootstrapControl(context.Background(), []ControlDriverConfig{{DriverID: "fam.new", Enabled: true}}); err != nil {
			t.Fatal(err)
		}
		if got := len(rcControlJSON(t, rcRead(t, path))["drivers"].(map[string]any)); got != MaxControlDriverRecords {
			t.Fatalf("records = %d, retained work was removed", got)
		}
		_, err := rcDecide(t, m, ControlStart, "fam.new", 201)
		_ = rcRefusal(t, err, RefusalResourceExhausted, 9)
		if _, err := m.BootstrapControl(context.Background(), make([]ControlDriverConfig, MaxControlDriverRecords+1)); !errors.Is(err, ErrControlInvalidConfig) {
			t.Fatalf("over-limit registration = %v", err)
		}
		rcStop(t, m)
	})
	t.Run("byte_limit", func(t *testing.T) {
		m, path := rcSeeded(t, nil)
		saved := maxControlNamespaceBytes
		defer func() { maxControlNamespaceBytes = saved }()
		maxControlNamespaceBytes = 600
		before := rcRead(t, path)
		err := m.CommitControlRecovery(rcCtx(t), &ControlRecovery{Payload: bytes.Repeat([]byte{0xAB}, 1024)})
		_ = rcRefusal(t, err, RefusalResourceExhausted, 9)
		if !bytes.Equal(rcRead(t, path), before) {
			t.Fatalf("byte-limit refusal mutated state")
		}
		rcStop(t, m)
	})
}

// CTL-13: shared mark across drivers, removal and re-add as mark-only writes,
// held requests conflict, removal is blocked by in-flight work or D5 material.
func TestControlRemovalAndReAddAreMarkOnlyWrites(t *testing.T) {
	path := rcPath(t)
	rcWriteBase(t, path)
	both := []ControlDriverConfig{{DriverID: "fam.a", Enabled: true}, {DriverID: "fam.b", Enabled: true}}
	m := rcManager(t, path, nil)
	if _, err := m.BootstrapControl(context.Background(), both); err != nil {
		t.Fatal(err)
	}
	// Interleaved decisions each take mark + 1.
	va, _ := rcDecide(t, m, ControlStop, "fam.a", 1)
	vb, _ := rcDecide(t, m, ControlStop, "fam.b", 2)
	if va.DesiredRevision != 3 || vb.DesiredRevision != 4 {
		t.Fatalf("interleaved = %d/%d", va.DesiredRevision, vb.DesiredRevision)
	}
	if err := m.CommitControlWork(rcCtx(t), "fam.b", nil, nil); err != nil {
		t.Fatal(err)
	}
	heldOld, heldMark := uint64(4), uint64(4)
	rcStop(t, m)

	m = rcManager(t, path, nil) // fam.b removed from configuration
	if _, err := m.BootstrapControl(context.Background(), both[:1]); err != nil {
		t.Fatal(err)
	}
	if snap := rcSnapshot(t, m); snap.IssuedRevisionMark != 5 {
		t.Fatalf("removal mark = %d, want 5", snap.IssuedRevisionMark)
	}
	if _, ok := rcControlJSON(t, rcRead(t, path))["drivers"].(map[string]any)["fam.b"]; ok {
		t.Fatalf("removed record still stored")
	}
	rcStop(t, m)

	m = rcManager(t, path, nil) // fam.b re-added
	if _, err := m.BootstrapControl(context.Background(), both); err != nil {
		t.Fatal(err)
	}
	v := rcDriver(t, m, "fam.b")
	if v.HasRecord || v.DesiredState != DesiredStopped || v.DesiredRevision != 6 {
		t.Fatalf("re-added view = %+v, want STOPPED at mark 6", v)
	}
	for _, held := range []uint64{heldOld, heldMark, 5} {
		_, err := rcDecide(t, m, ControlStart, "fam.b", held)
		_ = rcRefusal(t, err, RefusalRevisionConflict, 6)
	}
	if view, err := rcDecide(t, m, ControlStop, "fam.b", 6); err != nil || view != (ControlView{DesiredStopped, 6}) {
		t.Fatalf("STOP at the mark = %+v, %v; want no-op", view, err)
	}
	if view, err := rcDecide(t, m, ControlStart, "fam.b", 6); err != nil || view.DesiredRevision != 7 {
		t.Fatalf("START at the mark = %+v, %v", view, err)
	}
	// fam.a holds in-flight STOP work: unconfiguring it does not remove it.
	// fam.b is referenced by D5 material: not removed either.
	if err := m.CommitControlWork(rcCtx(t), "fam.b", nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := m.CommitControlRecovery(rcCtx(t), &ControlRecovery{DriverIDs: []string{"fam.b"}, MaxDesiredRevision: 7, Payload: []byte("d5")}); err != nil {
		t.Fatal(err)
	}
	rcStop(t, m)
	m = rcManager(t, path, nil)
	if _, err := m.BootstrapControl(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	drivers := rcControlJSON(t, rcRead(t, path))["drivers"].(map[string]any)
	if _, ok := drivers["fam.a"]; !ok {
		t.Fatalf("driver with in-flight work was removed")
	}
	if _, ok := drivers["fam.b"]; !ok {
		t.Fatalf("driver referenced by D5 material was removed")
	}
	rcStop(t, m)
}

// Byte-stable UTC serialization: fixed clocks give identical bytes, written_at
// is UTC even for a non-UTC clock, and load followed by re-encode is stable.
func TestControlSerializationIsByteStableUTC(t *testing.T) {
	produce := func(t *testing.T) []byte {
		m, path := rcSeeded(t, nil)
		if _, err := rcDecide(t, m, ControlStop, "ebus.primary", 2); err != nil {
			t.Fatal(err)
		}
		deadline := time.Date(2030, 1, 2, 0, 0, 0, 0, time.UTC)
		work := &ControlTransition{Kind: TransitionStop, DesiredRevision: 3, Sequence: 1, Phase: PhaseCleanup, Epoch: "epoch-1", Generation: 2, NativeClosePending: true, AttemptsUsed: 3, DeadlineUTC: deadline}
		if err := m.CommitControlWork(rcCtx(t), "ebus.primary", work, nil); err != nil {
			t.Fatal(err)
		}
		if err := m.CommitControlRecovery(rcCtx(t), &ControlRecovery{DriverIDs: []string{"ebus.primary"}, MaxDesiredRevision: 3, Payload: []byte{0, 1, 2, 0xfe, 0xff}}); err != nil {
			t.Fatal(err)
		}
		rcStop(t, m)
		return rcRead(t, path)
	}
	a, b := produce(t), produce(t)
	if !bytes.Equal(a, b) {
		t.Fatalf("serialization not byte-stable:\n%s\n%s", a, b)
	}
	if !bytes.Contains(a, []byte(`"written_at": "2030-01-02T00:04:05Z"`)) {
		t.Fatalf("written_at not normalized to UTC:\n%s", a)
	}
	state, _, err := unmarshalState(a)
	if err != nil {
		t.Fatal(err)
	}
	state.Meta.GatewayBuild, state.Meta.AddonVersion = "rc-build", "0.0.0-rc"
	again, err := marshalState(state)
	if err != nil || !bytes.Equal(again, a) {
		t.Fatalf("load/re-encode drifted (%v):\n%s\n%s", err, a, again)
	}
	const wantControl = `{"schema_version":1,"issued_revision_mark":"3","catalog_revision":"4","drivers":{` +
		`"alpha.off":{"desired_state":"STOPPED","desired_revision":"1","lifecycle_sequence":"0","current":null,"reserved":null},` +
		`"ebus.primary":{"desired_state":"STOPPED","desired_revision":"3","lifecycle_sequence":"1","current":{"kind":"STOP","desired_revision":"3","sequence":"1","phase":"CLEANUP","epoch":"epoch-1","generation":"2","native_close_pending":true,"semantic_cleanup_pending":false,"attempts_used":3,"deadline_utc":"2030-01-02T00:00:00Z"},"reserved":null}},` +
		`"recovery":{"format":"` + ControlRecoveryFormat + `","driver_ids":["ebus.primary"],"max_desired_revision":"3","payload_sha256":"%s","payload":"AAEC/v8="}}`
	var root map[string]json.RawMessage
	_ = json.Unmarshal(a, &root)
	digest := recoveryToDTO(&ControlRecovery{Payload: []byte{0, 1, 2, 0xfe, 0xff}}).PayloadSHA256
	if !rcJSONEqual(t, root[ControlNamespaceName], []byte(fmt.Sprintf(wantControl, digest))) {
		t.Fatalf("namespace bytes drifted:\n%s", root[ControlNamespaceName])
	}
}

// Upgrade -> downgrade -> upgrade: a pre-control build keeps runtime_control
// opaquely (sorted-map rewrite) and the re-upgrade restores intent, the D5
// payload, the GUID and unknown namespaces. An old-format file without the
// namespace is genuinely missing and seeds once.
func TestControlUpgradeDowngradeUpgradePreservesIntentAndRecovery(t *testing.T) {
	m, path := rcSeeded(t, nil)
	if _, err := rcDecide(t, m, ControlStop, "ebus.primary", 2); err != nil {
		t.Fatal(err)
	}
	payload := []byte("synthetic-d5-recovery\x00\x01")
	if err := m.CommitControlRecovery(rcCtx(t), &ControlRecovery{DriverIDs: []string{"ebus.primary"}, MaxDesiredRevision: 3, Payload: payload}); err != nil {
		t.Fatal(err)
	}
	rcStop(t, m)
	// Downgrade: a build without control support keeps every unknown
	// namespace as opaque JSON and rewrites the file through a sorted map.
	var root map[string]json.RawMessage
	if err := json.Unmarshal(rcRead(t, path), &root); err != nil {
		t.Fatal(err)
	}
	root["zeta_plugin"] = json.RawMessage(`{"v":1}`)
	downgraded, err := json.MarshalIndent(root, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, downgraded, 0o600); err != nil {
		t.Fatal(err)
	}
	// Re-upgrade.
	m2 := rcManager(t, path, nil)
	if got := m2.ControlLoad().Disposition; got != ControlLoadLoaded {
		t.Fatalf("re-upgrade disposition = %s", got)
	}
	plan, err := m2.BootstrapControl(context.Background(), rcDefaultConfigs)
	if err != nil {
		t.Fatal(err)
	}
	if plan[1].Action != StartupStopped || plan[1].DesiredRevision != 3 {
		t.Fatalf("intent lost across downgrade: %+v", plan[1])
	}
	if rec := m2.ControlRecoveryPayload(); rec == nil || !bytes.Equal(rec.Payload, payload) {
		t.Fatalf("recovery payload lost: %+v", rec)
	}
	if err := m2.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	var after map[string]json.RawMessage
	_ = json.Unmarshal(rcRead(t, path), &after)
	if !rcJSONEqual(t, after["zeta_plugin"], []byte(`{"v":1}`)) || m2.State().Meta.InstanceGUID != rcGUID {
		t.Fatalf("unknown namespace or GUID not preserved")
	}
	rcStop(t, m2)

	t.Run("old_format_without_namespace_seeds_once", func(t *testing.T) {
		path := rcPath(t)
		if err := os.WriteFile(path, validV1JSON(t), 0o600); err != nil {
			t.Fatal(err)
		}
		m := rcManager(t, path, nil)
		if got := m.ControlLoad().Disposition; got != ControlLoadMissing {
			t.Fatalf("old format = %s, want MISSING", got)
		}
		if _, err := m.BootstrapControl(context.Background(), rcDefaultConfigs); err != nil {
			t.Fatal(err)
		}
		if got := len(m.State().EBus.KnownBusMembers); got != 2 {
			t.Fatalf("ebus cache lost on seed: %d", got)
		}
		rcStop(t, m)
	})
}

// No receipt, key, principal, request digest or completed-operation store:
// the encoded namespace carries only the allowed durable members.
func TestControlNamespaceHoldsNoReceiptKeyOrPrincipalStore(t *testing.T) {
	m, path := rcSeeded(t, nil)
	if _, err := rcDecide(t, m, ControlStop, "ebus.primary", 2); err != nil {
		t.Fatal(err)
	}
	if err := m.CommitControlRecovery(rcCtx(t), &ControlRecovery{MaxDesiredRevision: 3, Payload: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	rcStop(t, m)
	var keys []string
	var walk func(v any)
	walk = func(v any) {
		switch x := v.(type) {
		case map[string]any:
			for k, child := range x {
				keys = append(keys, k)
				walk(child)
			}
		case []any:
			for _, child := range x {
				walk(child)
			}
		}
	}
	walk(rcControlJSON(t, rcRead(t, path)))
	sort.Strings(keys)
	for _, k := range keys {
		lower := strings.ToLower(k)
		for _, banned := range []string{"receipt", "idempotency", "principal", "request", "operation", "completed", "authority", "key"} {
			if strings.Contains(lower, banned) {
				t.Errorf("namespace member %q looks like a forbidden store (%s)", k, banned)
			}
		}
	}
}

// The namespace cannot alter the GUID, meta or add-on markers; shutdown never
// changes durable intent; a control load failure never fails Load.
func TestControlNamespaceCannotAlterIdentityOrShutdownIntent(t *testing.T) {
	m, path := rcSeeded(t, nil)
	metaBefore := func() json.RawMessage {
		var root map[string]json.RawMessage
		_ = json.Unmarshal(rcRead(t, path), &root)
		var meta map[string]any
		_ = json.Unmarshal(root["meta"], &meta)
		delete(meta, "written_at")
		out, _ := json.Marshal(meta)
		return out
	}
	before := metaBefore()
	if _, err := rcDecide(t, m, ControlStop, "ebus.primary", 2); err != nil {
		t.Fatal(err)
	}
	if got := metaBefore(); !bytes.Equal(got, before) {
		t.Fatalf("control transaction changed meta: %s -> %s", before, got)
	}
	ctlBefore := rcControlJSON(t, rcRead(t, path))
	m.UpsertKnownBusMember(KnownBusMember{Addr: 0x20, LastSource: LastSourcePassiveObserved, Confidence: ConfidenceCorroborated})
	rcStop(t, m) // shutdown flush
	if got := rcControlJSON(t, rcRead(t, path)); !reflect.DeepEqual(got, ctlBefore) {
		t.Fatalf("shutdown changed durable intent")
	}
	if got := metaBefore(); !bytes.Equal(got, before) {
		t.Fatalf("shutdown changed meta")
	}
}

// CTL-12 cross-check: the offline reset's output loads, reads STOPPED at
// H + 1, advances the catalog to C + 1, keeps D5 byte-identical, and a held
// pre-repair START conflicts while a fresh START with the read-back revision
// proceeds.
func TestControlOfflineResetOutputLoadsAndFencesHeldRequests(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	script, err := filepath.Abs(filepath.Join("..", "..", "scripts", "reset_runtime_control.py"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(script); err != nil {
		t.Fatalf("reset script missing: %v", err)
	}
	m, path := rcSeeded(t, nil)
	if _, err := rcDecide(t, m, ControlStop, "ebus.primary", 2); err != nil {
		t.Fatal(err)
	}
	payload := []byte("retained-d5")
	if err := m.CommitControlRecovery(rcCtx(t), &ControlRecovery{DriverIDs: []string{"ebus.primary"}, MaxDesiredRevision: 3, Payload: payload}); err != nil {
		t.Fatal(err)
	}
	rcStop(t, m)
	original := rcRead(t, path)
	cmd := exec.Command(python, "-I", script, "--state-file", path, "--namespace", "runtime_control", "--desired", "STOPPED", "--driver-id", "ebus.primary", "--apply", "--gateway-stopped")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("reset failed: %v\n%s", err, out)
	}
	copies, _ := filepath.Glob(path + controlQuarantineInfix + "*")
	if len(copies) != 1 || !bytes.Equal(rcRead(t, copies[0]), original) {
		t.Fatalf("quarantine copy = %v, want exact original bytes", copies)
	}
	m2 := rcManager(t, path, nil)
	if got := m2.ControlLoad().Disposition; got != ControlLoadLoaded {
		t.Fatalf("reset output disposition = %s (%s)", got, m2.ControlLoad().Detail)
	}
	plan, err := m2.BootstrapControl(context.Background(), []ControlDriverConfig{{DriverID: "ebus.primary", Enabled: true}})
	if err != nil {
		t.Fatal(err)
	}
	if plan[0].Action != StartupStopped || plan[0].DesiredRevision != 4 || plan[0].Current != nil {
		t.Fatalf("reset plan = %+v, want STOPPED at H+1=4 with no active work", plan[0])
	}
	snap := rcSnapshot(t, m2)
	if snap.CatalogRevision != 5 || snap.IssuedRevisionMark != 4 {
		t.Fatalf("reset mark/catalog = %d/%d, want 4/5", snap.IssuedRevisionMark, snap.CatalogRevision)
	}
	if rec := m2.ControlRecoveryPayload(); rec == nil || !bytes.Equal(rec.Payload, payload) {
		t.Fatalf("D5 recovery changed by reset")
	}
	for _, held := range []uint64{1, 2, 3} {
		_, err := rcDecide(t, m2, ControlStart, "ebus.primary", held)
		_ = rcRefusal(t, err, RefusalRevisionConflict, 6)
	}
	if view, err := rcDecide(t, m2, ControlStart, "ebus.primary", 4); err != nil || view.DesiredRevision != 5 {
		t.Fatalf("fresh START = %+v, %v", view, err)
	}
	rcStop(t, m2)
}

// Registration stays private until its mark-only write is durable, including
// while the write is blocked and after any pre-commit failure or overflow.
func TestControlRegistrationWaitsForDurableFence(t *testing.T) {
	for _, failure := range []string{"write", "fsync", "rename", "overflow"} {
		t.Run(failure, func(t *testing.T) {
			hooks := &rcHooks{}
			m, path := rcSeeded(t, hooks)
			defer rcStop(t, m)
			if failure == "overflow" {
				m.mu.Lock()
				m.state.control.mark = math.MaxUint64
				m.mu.Unlock()
			}
			before := rcRead(t, path)
			old := rcSnapshot(t, m)
			block, entered := make(chan struct{}), make(chan struct{})
			hooks.set(func(h *rcHooks) {
				h.blockNext, h.entered = block, entered
				switch failure {
				case "write":
					h.failWrite = syscall.ENOSPC
				case "fsync":
					h.failFsync = syscall.EIO
				case "rename":
					h.failRename = syscall.EACCES
				}
			})
			configs := append(append([]ControlDriverConfig(nil), rcDefaultConfigs...), ControlDriverConfig{DriverID: "fam.new", Enabled: true})
			done := make(chan error, 1)
			ctx := rcCtx(t)
			go func() {
				_, err := m.BootstrapControl(ctx, configs)
				done <- err
			}()
			if failure != "overflow" {
				select {
				case <-entered:
				case <-time.After(5 * time.Second):
					t.Fatal("registration write did not enter")
				}
				if got := rcSnapshot(t, m); !reflect.DeepEqual(got, old) {
					t.Errorf("registration exposed before commit: %+v", got)
				}
				close(block)
			}
			if err := <-done; err == nil {
				t.Fatal("registration unexpectedly succeeded")
			}
			if got := rcSnapshot(t, m); !reflect.DeepEqual(got, old) {
				t.Fatalf("failed registration exposed: %+v", got)
			}
			_, err := rcDecide(t, m, ControlStart, "fam.new", old.IssuedRevisionMark)
			_ = rcRefusal(t, err, RefusalNotFound, 2)
			if !bytes.Equal(before, rcRead(t, path)) {
				t.Fatal("failed registration changed durable bytes")
			}
			if failure != "overflow" {
				hooks.set(func(h *rcHooks) { h.failWrite, h.failFsync, h.failRename = nil, nil, nil })
				if _, err := m.BootstrapControl(rcCtx(t), configs); err != nil {
					t.Fatal(err)
				}
				if got := rcDriver(t, m, "fam.new"); got.DesiredRevision != old.IssuedRevisionMark+1 {
					t.Fatalf("registration retry lacks new fence: %+v", got)
				}
			}
		})
	}
}

func TestControlWorkRejectsObsoleteSlots(t *testing.T) {
	hooks := &rcHooks{}
	m, path := rcSeeded(t, hooks)
	defer rcStop(t, m)
	if _, err := rcDecide(t, m, ControlStop, "ebus.primary", 2); err != nil {
		t.Fatal(err)
	}
	old := rcDriver(t, m, "ebus.primary").Current
	if _, err := rcDecide(t, m, ControlStart, "ebus.primary", 3); err != nil {
		t.Fatal(err)
	}
	v := rcDriver(t, m, "ebus.primary")
	before, writes := rcRead(t, path), hooks.writeCount()
	if err := m.CommitControlWork(rcCtx(t), "ebus.primary", old, nil); !errors.Is(err, ErrControlInvalidWork) {
		t.Fatalf("discard newer reserved slot = %v", err)
	}
	if hooks.writeCount() != writes || !bytes.Equal(before, rcRead(t, path)) {
		t.Fatal("rejected update wrote state")
	}
	// Promotion and phase updates of the accepted identity remain valid.
	promoted := cloneTransition(v.Reserved)
	promoted.Phase = PhaseActive
	if err := m.CommitControlWork(rcCtx(t), "ebus.primary", promoted, nil); err != nil {
		t.Fatal(err)
	}
	before, writes = rcRead(t, path), hooks.writeCount()
	// The accepted identity (kind, desired revision, sequence) is immutable:
	// a later write may only advance or clear it.
	wrongRevision, wrongKind, swappedKind, wrongSequence := cloneTransition(promoted), cloneTransition(promoted), cloneTransition(promoted), cloneTransition(promoted)
	wrongRevision.DesiredRevision--
	wrongKind.Kind = TransitionRestart
	swappedKind.Kind = TransitionStop
	wrongSequence.Sequence++
	for _, stale := range []*ControlTransition{old, wrongRevision, wrongKind, swappedKind, wrongSequence} {
		if err := m.CommitControlWork(rcCtx(t), "ebus.primary", stale, nil); !errors.Is(err, ErrControlInvalidWork) {
			t.Fatalf("obsolete identity accepted: %+v: %v", stale, err)
		}
	}
	if hooks.writeCount() != writes || !bytes.Equal(before, rcRead(t, path)) {
		t.Fatal("obsolete update changed state")
	}
	if got := rcDriver(t, m, "ebus.primary"); !reflect.DeepEqual(got.Current, promoted) || got.Reserved != nil {
		t.Fatalf("obsolete update replaced accepted work: %+v", got)
	}
}

func TestControlMissingReadbackCannotResurrectIntent(t *testing.T) {
	hooks := &rcHooks{}
	m, path := rcSeeded(t, hooks)
	defer rcStop(t, m)
	hooks.set(func(h *rcHooks) { h.failDir = syscall.EIO })
	_, err := rcDecide(t, m, ControlStop, "ebus.primary", 2)
	_ = rcRefusal(t, err, RefusalUnavailable, 3)
	// Simulate readback of a valid state file whose namespace has vanished.
	rcWriteBase(t, path)
	before := rcRead(t, path)
	hooks.set(func(h *rcHooks) { h.failDir = nil })
	if err := m.ResolveControlReadback(rcCtx(t)); err == nil {
		t.Fatal("missing namespace readback accepted")
	}
	if m.ControlAdmissionOpen() || m.ControlLoad().Disposition != ControlLoadCorrupt {
		t.Fatal("missing namespace did not fail closed")
	}
	writes := hooks.writeCount()
	m.UpsertKnownBusMember(KnownBusMember{Addr: 0x12, LastSource: LastSourcePassiveObserved, Confidence: ConfidenceCorroborated})
	if err := m.Flush(rcCtx(t)); !errors.Is(err, ErrStatePersistBlocked) {
		t.Fatalf("flush after missing readback = %v", err)
	}
	if err := m.ResolveControlReadback(rcCtx(t)); err != nil {
		t.Fatal(err)
	}
	if hooks.writeCount() != writes || !bytes.Equal(before, rcRead(t, path)) {
		t.Fatal("cache flush resurrected old control intent")
	}
	_, err = rcDecide(t, m, ControlStart, "ebus.primary", 2)
	_ = rcRefusal(t, err, RefusalFailedPrecondition, 3)
}

// rcHoldStoreLock takes the store lock through an independent descriptor, as
// another gateway process or the offline reset would.
func rcHoldStoreLock(t *testing.T, path string) *os.File {
	t.Helper()
	lock, err := os.OpenFile(path+".lock", os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = lock.Close()
		t.Fatalf("store lock not acquirable: %v", err)
	}
	return lock
}

// rcStoreLockFree reports whether an independent descriptor can take the lock.
func rcStoreLockFree(t *testing.T, path string) bool {
	t.Helper()
	lock, err := os.OpenFile(path+".lock", os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lock.Close() }()
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		if !errors.Is(err, unix.EWOULDBLOCK) {
			t.Fatalf("flock: %v", err)
		}
		return false
	}
	_ = unix.Flock(int(lock.Fd()), unix.LOCK_UN)
	return true
}

// A store lock that cannot be acquired fails control closed for admission and
// writes (LOCKED, nothing accepted, no write, no seed) while the readable
// file stays available read-only: the GUID, the eBUS cache and the
// last-known-good control namespace are never collapsed into "missing" and
// never overwritten later.
func TestControlStoreLockUnavailableKeepsReadableStateReadOnly(t *testing.T) {
	for _, cause := range []string{"held_by_another_writer", "sidecar_cannot_be_opened"} {
		t.Run(cause, func(t *testing.T) {
			m, path := rcSeeded(t, nil)
			m.UpsertKnownBusMember(KnownBusMember{Addr: 0x15, LastSource: LastSourcePassiveObserved, Confidence: ConfidenceCorroborated})
			if err := m.Flush(rcCtx(t)); err != nil {
				t.Fatal(err)
			}
			if _, err := rcDecide(t, m, ControlStop, "ebus.primary", 2); err != nil {
				t.Fatal(err)
			}
			rcStop(t, m)
			switch cause {
			case "held_by_another_writer":
				lock := rcHoldStoreLock(t, path)
				defer func() { _ = lock.Close() }()
			case "sidecar_cannot_be_opened":
				if err := os.Remove(path + ".lock"); err != nil && !os.IsNotExist(err) {
					t.Fatal(err)
				}
				if err := os.Mkdir(path+".lock", 0o700); err != nil {
					t.Fatal(err)
				}
			}
			before := rcRead(t, path)
			hooks := &rcHooks{}
			m2 := rcManager(t, path, hooks)
			status := m2.ControlLoad()
			if status.Disposition != ControlLoadLocked || !strings.Contains(status.Detail, "store lock unavailable") {
				t.Fatalf("load = %+v, want LOCKED", status)
			}
			if m2.ControlAdmissionOpen() {
				t.Fatal("admission open without the store lock")
			}
			// Generic namespaces keep their last-known-good in-memory values.
			st := m2.State()
			if st.Meta.InstanceGUID != rcGUID || st.EBus == nil || len(st.EBus.KnownBusMembers) != 1 || st.EBus.KnownBusMembers[0].Addr != 0x15 {
				t.Fatalf("readable state collapsed: %+v", st)
			}
			plan, err := m2.BootstrapControl(context.Background(), rcDefaultConfigs)
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range plan {
				if entry.Action != StartupFailed {
					t.Fatalf("startup without the store lock = %+v", plan)
				}
			}
			// The read path serves the last-known-good namespace read-only.
			snap := rcSnapshot(t, m2)
			if !snap.ReadOnly || snap.IssuedRevisionMark != 3 || snap.CatalogRevision != 4 || len(snap.Drivers) != 2 {
				t.Fatalf("read-only snapshot = %+v", snap)
			}
			v := rcDriver(t, m2, "ebus.primary")
			if !v.ReadOnly || v.Failed || !v.HasRecord || v.DesiredState != DesiredStopped || v.DesiredRevision != 3 || v.Current == nil {
				t.Fatalf("read-only driver view = %+v", v)
			}
			// Every admission and write path fails closed.
			_, err = rcDecide(t, m2, ControlStart, "ebus.primary", 3)
			_ = rcRefusal(t, err, RefusalFailedPrecondition, 3)
			if _, err := m2.AdvanceControlCatalog(rcCtx(t)); err == nil {
				t.Fatal("catalog advanced without the store lock")
			}
			if err := m2.CommitControlWork(rcCtx(t), "ebus.primary", nil, nil); err == nil {
				t.Fatal("work committed without the store lock")
			}
			if err := m2.CommitControlRecovery(rcCtx(t), nil); err == nil {
				t.Fatal("recovery committed without the store lock")
			}
			m2.UpsertKnownBusMember(KnownBusMember{Addr: 0x26, LastSource: LastSourcePassiveObserved, Confidence: ConfidenceCorroborated})
			if err := m2.Flush(rcCtx(t)); !errors.Is(err, ErrStatePersistBlocked) {
				t.Fatalf("flush without the store lock = %v", err)
			}
			if err := m2.EagerPersistInstanceGUID(context.Background(), rcGUID, IdentitySourceRuntimeState); !errors.Is(err, ErrStatePersistBlocked) {
				t.Fatalf("eager persist without the store lock = %v", err)
			}
			rcStop(t, m2)
			if hooks.writeCount() != 0 || !bytes.Equal(rcRead(t, path), before) {
				t.Fatal("store written without its lock")
			}
		})
	}
	t.Run("corrupt_file_is_not_quarantined", func(t *testing.T) {
		path := rcPath(t)
		if err := os.WriteFile(path, []byte(`{"schema_version":1,`), 0o600); err != nil {
			t.Fatal(err)
		}
		lock := rcHoldStoreLock(t, path)
		defer func() { _ = lock.Close() }()
		before := rcRead(t, path)
		m := rcManager(t, path, nil)
		defer rcStop(t, m)
		if got := m.ControlLoad(); got.Disposition != ControlLoadLocked || !strings.Contains(got.Detail, "corrupt") {
			t.Fatalf("load = %+v", got)
		}
		if copies, _ := filepath.Glob(path + corruptQuarantineInfix + "*"); len(copies) != 0 || !bytes.Equal(rcRead(t, path), before) {
			t.Fatalf("corrupt file renamed without the lock: %v", copies)
		}
		if _, err := m.ControlSnapshot(); err == nil {
			t.Fatal("snapshot served without a readable namespace")
		}
	})
	t.Run("missing_file_is_not_seeded", func(t *testing.T) {
		path := rcPath(t)
		lock := rcHoldStoreLock(t, path)
		defer func() { _ = lock.Close() }()
		m := rcManager(t, path, nil)
		defer rcStop(t, m)
		if got := m.ControlLoad().Disposition; got != ControlLoadLocked {
			t.Fatalf("got %s, want LOCKED", got)
		}
		if _, err := m.BootstrapControl(context.Background(), rcDefaultConfigs); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("seeded without the lock: %v", err)
		}
	})
}

// Stop waits for an in-flight control transaction (dirty=false, so no final
// flush) before releasing the store lock, and closes write admission.
func TestControlStopWaitsForInFlightControlTransaction(t *testing.T) {
	hooks := &rcHooks{}
	m, path := rcSeeded(t, hooks)
	block, entered := make(chan struct{}), make(chan struct{})
	hooks.set(func(h *rcHooks) { h.blockNext, h.entered = block, entered })
	advanced := make(chan error, 1)
	go func() {
		_, err := m.AdvanceControlCatalog(context.Background())
		advanced <- err
	}()
	<-entered
	stopped := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		stopped <- m.Stop(ctx)
	}()
	select {
	case err := <-stopped:
		t.Fatalf("Stop returned during an in-flight control transaction: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	if rcStoreLockFree(t, path) {
		t.Fatal("store lock released before the in-flight rename")
	}
	close(block)
	if err := <-advanced; err != nil {
		t.Fatalf("in-flight transaction = %v", err)
	}
	if err := <-stopped; err != nil {
		t.Fatalf("Stop = %v", err)
	}
	if !rcStoreLockFree(t, path) {
		t.Fatal("store lock still held after Stop")
	}
	if got := rcControlJSON(t, rcRead(t, path))["catalog_revision"]; got != "4" {
		t.Fatalf("catalog after the in-flight commit = %v, want 4", got)
	}
	// Write admission is closed after the lock is released.
	before, writes := rcRead(t, path), hooks.writeCount()
	_, err := m.AdvanceControlCatalog(rcCtx(t))
	_ = rcRefusal(t, err, RefusalUnavailable, 3)
	m.UpsertKnownBusMember(KnownBusMember{Addr: 0x31, LastSource: LastSourcePassiveObserved, Confidence: ConfidenceCorroborated})
	if err := m.Flush(rcCtx(t)); !errors.Is(err, ErrStatePersistBlocked) {
		t.Fatalf("flush after Stop = %v", err)
	}
	if hooks.writeCount() != writes || !bytes.Equal(before, rcRead(t, path)) {
		t.Fatal("write after Stop released the lock")
	}
}

// The store lock is released on every shutdown path, including a canceled
// context, so a restart does not see a stale LOCKED disposition.
func TestControlStopReleasesStoreLockOnCanceledContext(t *testing.T) {
	t.Run("canceled_before_stop", func(t *testing.T) {
		m, path := rcSeeded(t, nil)
		if err := m.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_ = m.Stop(ctx)
		if !rcStoreLockFree(t, path) {
			t.Fatal("store lock held after a canceled Stop")
		}
		if err := m.Stop(context.Background()); err != nil {
			t.Fatalf("second Stop = %v", err)
		}
		m2 := rcManager(t, path, nil)
		defer rcStop(t, m2)
		if got := m2.ControlLoad().Disposition; got != ControlLoadLoaded {
			t.Fatalf("restart disposition = %s, want LOADED", got)
		}
	})
	t.Run("canceled_during_in_flight_write", func(t *testing.T) {
		hooks := &rcHooks{}
		m, path := rcSeeded(t, hooks)
		block, entered := make(chan struct{}), make(chan struct{})
		hooks.set(func(h *rcHooks) { h.blockNext, h.entered = block, entered })
		advanced := make(chan error, 1)
		go func() {
			_, err := m.AdvanceControlCatalog(context.Background())
			advanced <- err
		}()
		<-entered
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		if err := m.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Stop with an expiring context = %v", err)
		}
		if rcStoreLockFree(t, path) {
			t.Fatal("store lock released before the in-flight rename")
		}
		close(block)
		if err := <-advanced; err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(5 * time.Second)
		for !rcStoreLockFree(t, path) {
			if time.Now().After(deadline) {
				t.Fatal("store lock never released after a canceled Stop")
			}
			time.Sleep(5 * time.Millisecond)
		}
		m2 := rcManager(t, path, nil)
		defer rcStop(t, m2)
		if got := m2.ControlLoad().Disposition; got != ControlLoadLoaded {
			t.Fatalf("restart disposition = %s, want LOADED", got)
		}
		if snap := rcSnapshot(t, m2); snap.CatalogRevision != 4 {
			t.Fatalf("restart catalog = %d, want the in-flight commit 4", snap.CatalogRevision)
		}
	})
}

// The namespace byte limit is enforced before catalog, registration and seed
// commits: one byte over refuses RESOURCE_EXHAUSTED with no write, exactly at
// the limit commits.
func TestControlByteLimitBeforeCatalogAndBootstrapCommits(t *testing.T) {
	saved := maxControlNamespaceBytes
	defer func() { maxControlNamespaceBytes = saved }()
	encodedSize := func(m *Manager) int {
		m.mu.Lock()
		defer m.mu.Unlock()
		raw, err := m.state.control.encode()
		if err != nil {
			t.Fatal(err)
		}
		return len(raw)
	}
	t.Run("catalog", func(t *testing.T) {
		m, path := rcCounterFixture(t, "9", "9", "9", "0")
		defer rcStop(t, m)
		size := encodedSize(m)
		maxControlNamespaceBytes = size // "9" -> "10" grows the namespace by one byte
		before := rcRead(t, path)
		_, err := m.AdvanceControlCatalog(rcCtx(t))
		_ = rcRefusal(t, err, RefusalResourceExhausted, 9)
		if !bytes.Equal(rcRead(t, path), before) {
			t.Fatal("over-limit catalog commit persisted")
		}
		maxControlNamespaceBytes = size + 1
		if next, err := m.AdvanceControlCatalog(rcCtx(t)); err != nil || next != 10 {
			t.Fatalf("catalog at the limit = %d, %v", next, err)
		}
		if got := encodedSize(m); got != maxControlNamespaceBytes {
			t.Fatalf("committed size = %d, want exactly the limit %d", got, maxControlNamespaceBytes)
		}
	})
	t.Run("registration", func(t *testing.T) {
		m, path := rcCounterFixture(t, "9", "9", "9", "0")
		defer rcStop(t, m)
		maxControlNamespaceBytes = encodedSize(m)
		before := rcRead(t, path)
		_, err := m.BootstrapControl(context.Background(), []ControlDriverConfig{{DriverID: "ebus.primary", Enabled: true}, {DriverID: "fam.new", Enabled: true}})
		_ = rcRefusal(t, err, RefusalResourceExhausted, 9)
		if !bytes.Equal(rcRead(t, path), before) {
			t.Fatal("over-limit registration persisted")
		}
	})
	t.Run("seed", func(t *testing.T) {
		path := rcPath(t)
		rcWriteBase(t, path)
		m := rcManager(t, path, nil)
		defer rcStop(t, m)
		maxControlNamespaceBytes = 16
		before := rcRead(t, path)
		_, err := m.BootstrapControl(context.Background(), rcDefaultConfigs)
		_ = rcRefusal(t, err, RefusalResourceExhausted, 9)
		if !bytes.Equal(rcRead(t, path), before) || m.ControlLoad().Disposition != ControlLoadMissing {
			t.Fatal("over-limit seed persisted")
		}
	})
}

// Cross-implementation check: the offline reset either refuses with the
// original bytes intact or emits a file the Go loader loads as LOADED. Each
// refused base is one the loader itself rejects.
func TestControlOfflineResetNeverEmitsAStateTheLoaderRejects(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	script, err := filepath.Abs(filepath.Join("..", "..", "scripts", "reset_runtime_control.py"))
	if err != nil {
		t.Fatal(err)
	}
	recovery := func(id string) string {
		payload := []byte("d5")
		sum := sha256.Sum256(payload)
		return fmt.Sprintf(`,"recovery":{"format":%q,"driver_ids":[%q],"max_desired_revision":"1","payload_sha256":%q,"payload":%q}`,
			ControlRecoveryFormat, id, hex.EncodeToString(sum[:]), base64.StdEncoding.EncodeToString(payload))
	}
	base := func(ebus, rec string) string {
		return `{"schema_version":1,"meta":{"instance_guid":"` + rcGUID + `","written_at":"2001-02-03T04:05:06Z"}` + ebus +
			`,"runtime_control":{"schema_version":1,"issued_revision_mark":"1","catalog_revision":"1","drivers":{}` + rec + `}}`
	}
	member := func(addr string) string {
		return `{"addr":` + addr + `,"last_seen_at":"","last_source":"passive_observed","confidence":"corroborated"}`
	}
	cases := []struct {
		name  string
		body  string
		valid bool
	}{
		{"valid", base(`,"ebus":{"schema_version":1,"known_bus_members":[`+member("8")+`]}`, recovery("ebus.primary")), true},
		{"d5_id_128_bytes", base("", recovery(strings.Repeat("a", 128))), true},
		{"d5_id_129_bytes", base("", recovery(strings.Repeat("a", 129))), false},
		{"member_addr_256", base(`,"ebus":{"schema_version":1,"known_bus_members":[`+member("256")+`]}`, ""), false},
		{"member_addr_duplicate", base(`,"ebus":{"schema_version":1,"known_bus_members":[`+member("8")+`,`+member("8")+`]}`, ""), false},
		{"self_source_300", base(`,"ebus":{"schema_version":1,"self":{"last_admitted_source":300,"last_admitted_at":"","selection_method":"","companion_target":null}}`, ""), false},
		{"companion_negative", base(`,"ebus":{"schema_version":1,"known_bus_members":[{"addr":8,"companion_addr":-1}]}`, ""), false},
		{"meta_wrong_type", strings.Replace(base("", ""), `"written_at":"2001-02-03T04:05:06Z"`, `"written_at":7`, 1), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// The loader's own verdict on the original bytes (on a copy, since
			// loading a corrupt file quarantines it).
			probe := rcPath(t)
			if err := os.WriteFile(probe, []byte(tc.body), 0o600); err != nil {
				t.Fatal(err)
			}
			pm := rcManager(t, probe, nil)
			loaderAccepts := pm.ControlLoad().Disposition == ControlLoadLoaded
			rcStop(t, pm)
			if loaderAccepts != tc.valid {
				t.Fatalf("loader verdict = %v, want %v (%+v)", loaderAccepts, tc.valid, pm.ControlLoad())
			}
			path := rcPath(t)
			if err := os.WriteFile(path, []byte(tc.body), 0o600); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(python, "-I", script, "--state-file", path, "--namespace", "runtime_control", "--desired", "STOPPED", "--driver-id", "ebus.primary", "--apply", "--gateway-stopped")
			out, runErr := cmd.CombinedOutput()
			if !tc.valid {
				var exitErr *exec.ExitError
				if !errors.As(runErr, &exitErr) || exitErr.ExitCode() != 3 {
					t.Fatalf("reset = %v, want FAILED_PRECONDITION exit 3\n%s", runErr, out)
				}
				if !bytes.Equal(rcRead(t, path), []byte(tc.body)) {
					t.Fatal("refused reset changed the original bytes")
				}
				return
			}
			if runErr != nil {
				t.Fatalf("reset = %v\n%s", runErr, out)
			}
			m := rcManager(t, path, nil)
			defer rcStop(t, m)
			if got := m.ControlLoad(); got.Disposition != ControlLoadLoaded {
				t.Fatalf("reset output load = %+v", got)
			}
		})
	}
}

// Stop's final flush takes the writer slot with the stop context: with dirty
// cache state and a blocked in-flight writer, Stop returns the context error
// without flushing, and the store lock is released once the writer finishes.
func TestControlStopDirtyFinalFlushHonorsContext(t *testing.T) {
	hooks := &rcHooks{}
	m, path := rcSeeded(t, hooks)
	block, entered := make(chan struct{}), make(chan struct{})
	hooks.set(func(h *rcHooks) { h.blockNext, h.entered = block, entered })
	advanced := make(chan error, 1)
	go func() {
		_, err := m.AdvanceControlCatalog(context.Background())
		advanced <- err
	}()
	<-entered
	m.UpsertKnownBusMember(KnownBusMember{Addr: 0x31, LastSource: LastSourcePassiveObserved, Confidence: ConfidenceCorroborated})
	writes := hooks.writeCount()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	stopped := make(chan error, 1)
	go func() { stopped <- m.Stop(ctx) }()
	select {
	case err := <-stopped:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Stop = %v, want the context error", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Stop blocked past its deadline in the final flush")
	}
	if rcStoreLockFree(t, path) {
		t.Fatal("store lock released before the in-flight rename")
	}
	if m.ControlAdmissionOpen() {
		t.Fatal("admission open after a cancelled Stop")
	}
	close(block)
	if err := <-advanced; err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for !rcStoreLockFree(t, path) {
		if time.Now().After(deadline) {
			t.Fatal("store lock never released")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if hooks.writeCount() != writes {
		t.Fatal("cancelled Stop still flushed the dirty cache")
	}
}

// A Stop whose context already ended skips the final flush even when the
// ticker never started (startup-error cleanup after Load).
func TestControlStopCanceledBeforeStartSkipsFlush(t *testing.T) {
	hooks := &rcHooks{}
	m, path := rcSeeded(t, hooks)
	m.UpsertKnownBusMember(KnownBusMember{Addr: 0x31, LastSource: LastSourcePassiveObserved, Confidence: ConfidenceCorroborated})
	writes := hooks.writeCount()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() { done <- m.Stop(ctx) }()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Stop = %v, want context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Stop with a cancelled context did not return promptly")
	}
	if !rcStoreLockFree(t, path) {
		t.Fatal("store lock held after Stop")
	}
	if hooks.writeCount() != writes {
		t.Fatal("cancelled Stop flushed the dirty cache")
	}
	if m.ControlAdmissionOpen() {
		t.Fatal("admission open after Stop")
	}
}

// ControlAdmissionOpen is false once the manager is stopped, after a normal
// Stop and while a cancelled Stop still waits for an in-flight writer.
func TestControlAdmissionClosedAfterStop(t *testing.T) {
	m, _ := rcSeeded(t, nil)
	if !m.ControlAdmissionOpen() {
		t.Fatal("admission closed before Stop")
	}
	if err := m.Stop(rcCtx(t)); err != nil {
		t.Fatal(err)
	}
	if m.ControlAdmissionOpen() {
		t.Fatal("admission open after Stop")
	}
}

// A stale update of a stored identity cannot regress durable progress.
func TestControlWorkRejectsProgressRegression(t *testing.T) {
	hooks := &rcHooks{}
	m, path := rcSeeded(t, hooks)
	defer rcStop(t, m)
	if _, err := rcDecide(t, m, ControlStop, "ebus.primary", 2); err != nil {
		t.Fatal(err)
	}
	base := cloneTransition(rcDriver(t, m, "ebus.primary").Current)
	deadline := rcFixedNow.UTC().Add(time.Hour)
	progressed := cloneTransition(base)
	progressed.Phase, progressed.AttemptsUsed, progressed.DeadlineUTC = PhaseCleanup, 2, deadline
	if err := m.CommitControlWork(rcCtx(t), "ebus.primary", progressed, nil); err != nil {
		t.Fatal(err)
	}
	// Forward updates stay accepted: same phase, more attempts, earlier deadline.
	forward := cloneTransition(progressed)
	forward.AttemptsUsed, forward.DeadlineUTC = 3, deadline.Add(-time.Minute)
	if err := m.CommitControlWork(rcCtx(t), "ebus.primary", forward, nil); err != nil {
		t.Fatalf("forward update refused: %v", err)
	}
	before, writes := rcRead(t, path), hooks.writeCount()
	mut := func(fn func(*ControlTransition)) *ControlTransition {
		c := cloneTransition(forward)
		fn(c)
		return c
	}
	for name, stale := range map[string]*ControlTransition{
		"phase_back":      mut(func(c *ControlTransition) { c.Phase = PhasePending }),
		"phase_active":    mut(func(c *ControlTransition) { c.Phase = PhaseActive }),
		"attempts_lower":  mut(func(c *ControlTransition) { c.AttemptsUsed = 1 }),
		"deadline_extend": mut(func(c *ControlTransition) { c.DeadlineUTC = deadline.Add(time.Hour) }),
		"deadline_remove": mut(func(c *ControlTransition) { c.DeadlineUTC = time.Time{} }),
		"fresh_pending":   base,
	} {
		if err := m.CommitControlWork(rcCtx(t), "ebus.primary", stale, nil); !errors.Is(err, ErrControlInvalidWork) {
			t.Fatalf("%s accepted: %v", name, err)
		}
	}
	if hooks.writeCount() != writes || !bytes.Equal(before, rcRead(t, path)) {
		t.Fatal("refused regression changed the on-disk bytes")
	}
}

// Admission is closed after Stop: every decision, including the same-desired
// no-op and a stale revision, and every sibling write entry point is refused
// UNAVAILABLE at row 3, and nothing is written.
func TestControlDecisionsRefusedAfterStop(t *testing.T) {
	for _, mode := range []string{"normal_stop", "canceled_stop"} {
		t.Run(mode, func(t *testing.T) {
			hooks := &rcHooks{}
			m, path := rcSeeded(t, hooks)
			drv := rcDriver(t, m, "ebus.primary")
			if drv.DesiredState != DesiredRunning {
				t.Fatalf("seeded desired = %v, want RUNNING", drv.DesiredState)
			}
			rev := drv.DesiredRevision
			ctx := rcCtx(t)
			if mode == "canceled_stop" {
				c, cancel := context.WithCancel(context.Background())
				cancel()
				ctx = c
			}
			_ = m.Stop(ctx)
			before, writes := rcRead(t, path), hooks.writeCount()
			for name, call := range map[string]func() error{
				"start_matching_noop": func() error { _, err := rcDecide(t, m, ControlStart, "ebus.primary", rev); return err },
				"stop_matching":       func() error { _, err := rcDecide(t, m, ControlStop, "ebus.primary", rev); return err },
				"restart_matching":    func() error { _, err := rcDecide(t, m, ControlRestart, "ebus.primary", rev); return err },
				"start_stale":         func() error { _, err := rcDecide(t, m, ControlStart, "ebus.primary", rev+7); return err },
				"commit_work": func() error {
					return m.CommitControlWork(rcCtx(t), "ebus.primary", nil, nil)
				},
				"advance_catalog": func() error { _, err := m.AdvanceControlCatalog(rcCtx(t)); return err },
				"commit_recovery": func() error { return m.CommitControlRecovery(rcCtx(t), nil) },
			} {
				_ = rcRefusal(t, call(), RefusalUnavailable, 3)
				if hooks.writeCount() != writes || !bytes.Equal(before, rcRead(t, path)) {
					t.Fatalf("%s changed the on-disk state", name)
				}
			}
			if m.ControlAdmissionOpen() {
				t.Fatal("admission open after Stop")
			}
		})
	}
}

// rcWriteLoaded writes a LOADED namespace with the given drivers object body.
func rcWriteLoaded(t *testing.T, path string, mark, catalog, drivers string) {
	t.Helper()
	body := `{"schema_version":1,"meta":{"instance_guid":"` + rcGUID + `","written_at":"2001-02-03T04:05:06Z"},` +
		`"runtime_control":{"schema_version":1,"issued_revision_mark":"` + mark + `","catalog_revision":"` + catalog +
		`","drivers":{` + drivers + `}}}`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

// Removing a registered record-less driver (and re-adding it) is a mark-only
// write: the issued-revision mark and catalog revision advance, the advance is
// durable, and the snapshot stops listing the removed driver.
func TestControlRecordlessDriverRemovalAndReAddAdvanceMark(t *testing.T) {
	path := rcPath(t)
	rcWriteLoaded(t, path, "0", "1", "")
	m := rcManager(t, path, nil)
	defer rcStop(t, m)
	ghost := []ControlDriverConfig{{DriverID: "fam.ghost", Enabled: true}}
	if _, err := m.BootstrapControl(context.Background(), ghost); err != nil {
		t.Fatal(err)
	}
	snap := rcSnapshot(t, m)
	if len(snap.Drivers) != 1 || snap.IssuedRevisionMark != 1 || snap.CatalogRevision != 2 {
		t.Fatalf("after register = %+v, want 1 driver, mark 1, catalog 2", snap)
	}
	// Re-registering the same set in-process is not a removal or an addition.
	if _, err := m.BootstrapControl(context.Background(), ghost); err != nil {
		t.Fatal(err)
	}
	if got := rcSnapshot(t, m); got.IssuedRevisionMark != 1 || got.CatalogRevision != 2 {
		t.Fatalf("idempotent re-register moved counters: %+v", got)
	}

	if _, err := m.BootstrapControl(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	snap = rcSnapshot(t, m)
	if len(snap.Drivers) != 0 || snap.IssuedRevisionMark != 2 || snap.CatalogRevision != 3 {
		t.Fatalf("after removal = %+v, want no drivers, mark 2, catalog 3", snap)
	}
	ns := rcControlJSON(t, rcRead(t, path))
	if ns["issued_revision_mark"] != "2" || ns["catalog_revision"] != "3" {
		t.Fatalf("removal not durable: mark=%v catalog=%v", ns["issued_revision_mark"], ns["catalog_revision"])
	}

	if _, err := m.BootstrapControl(context.Background(), ghost); err != nil {
		t.Fatal(err)
	}
	snap = rcSnapshot(t, m)
	if len(snap.Drivers) != 1 || snap.IssuedRevisionMark != 3 || snap.CatalogRevision != 4 {
		t.Fatalf("after re-add = %+v, want mark 3, catalog 4", snap)
	}
	ns = rcControlJSON(t, rcRead(t, path))
	if ns["issued_revision_mark"] != "3" || ns["catalog_revision"] != "4" {
		t.Fatalf("re-add not durable: mark=%v catalog=%v", ns["issued_revision_mark"], ns["catalog_revision"])
	}
}

// Admission is enforced at the serialization point: requests that queued on
// the writer semaphore before Stop began are refused once Stop has started,
// while the writer admitted before shutdown still finishes.
func TestControlQueuedWritesRefusedAfterStopBegins(t *testing.T) {
	hooks := &rcHooks{}
	m, path := rcSeeded(t, hooks)
	block, entered := make(chan struct{}), make(chan struct{})
	hooks.set(func(h *rcHooks) { h.blockNext, h.entered = block, entered })
	inflight := make(chan error, 1)
	go func() {
		_, err := m.AdvanceControlCatalog(context.Background())
		inflight <- err
	}()
	<-entered

	queued := map[string]func() error{
		"decide_stop": func() error {
			_, err := m.DecideControl(rcCtx(t), ControlStop, ControlRequest{DriverID: "ebus.primary", ExpectedRevision: 2}, nil)
			return err
		},
		"advance_catalog": func() error { _, err := m.AdvanceControlCatalog(rcCtx(t)); return err },
		"commit_work":     func() error { return m.CommitControlWork(rcCtx(t), "ebus.primary", nil, nil) },
		"commit_recovery": func() error {
			return m.CommitControlRecovery(rcCtx(t), &ControlRecovery{DriverIDs: []string{"ebus.primary"}, MaxDesiredRevision: 2, Payload: []byte("d5")})
		},
		"bootstrap": func() error {
			_, err := m.BootstrapControl(rcCtx(t), append([]ControlDriverConfig{{DriverID: "fam.late", Enabled: true}}, rcDefaultConfigs...))
			return err
		},
		"resolve_readback": func() error { return m.ResolveControlReadback(rcCtx(t)) },
	}
	results := make(map[string]chan error, len(queued))
	for name, call := range queued {
		ch := make(chan error, 1)
		results[name] = ch
		go func(call func() error) { ch <- call() }(call)
	}
	time.Sleep(200 * time.Millisecond) // let every request queue behind the writer

	stopped := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		stopped <- m.Stop(ctx)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		m.mu.Lock()
		isStopped := m.stopped
		m.mu.Unlock()
		if isStopped {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Stop never started")
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(block)
	if err := <-inflight; err != nil {
		t.Fatalf("writer admitted before shutdown = %v", err)
	}
	for name, ch := range results {
		_ = rcRefusal(t, <-ch, RefusalUnavailable, 3)
		_ = name
	}
	if err := <-stopped; err != nil {
		t.Fatalf("Stop = %v", err)
	}
	ns := rcControlJSON(t, rcRead(t, path))
	if ns["catalog_revision"] != "4" || ns["issued_revision_mark"] != "2" || ns["recovery"] != nil {
		t.Fatalf("queued request reached disk: %+v", ns)
	}
	if drivers := ns["drivers"].(map[string]any); len(drivers) != 2 {
		t.Fatalf("queued bootstrap registered a driver: %+v", drivers)
	}
	if _, err := m.ControlDriver("fam.late"); err == nil {
		t.Fatal("queued bootstrap published a registration")
	}
}

func rcTransitionJSON(kind, rev, seq string) string {
	return `{"kind":"` + kind + `","desired_revision":"` + rev + `","sequence":"` + seq +
		`","phase":"PENDING","epoch":"","generation":"0","native_close_pending":false,"semantic_cleanup_pending":false,"attempts_used":0,"deadline_utc":""}`
}

// The current slot must hold the older transition: a record whose current
// sequence is above (or equal to) the reserved sequence fails only that driver.
func TestControlRejectsReversedAndDuplicateTransitionSequences(t *testing.T) {
	for name, tc := range map[string]struct{ cur, res string }{
		"reversed":  {"2", "1"},
		"duplicate": {"2", "2"},
	} {
		t.Run(name, func(t *testing.T) {
			path := rcPath(t)
			bad := `{"desired_state":"RUNNING","desired_revision":"2","lifecycle_sequence":"2","current":` +
				rcTransitionJSON("STOP", "1", tc.cur) + `,"reserved":` + rcTransitionJSON("START", "2", tc.res) + `}`
			rcWriteLoaded(t, path, "2", "3",
				`"ebus.primary":{"desired_state":"RUNNING","desired_revision":"1","lifecycle_sequence":"0","current":null,"reserved":null},"fam.bad":`+bad)
			m := rcManager(t, path, nil)
			defer rcStop(t, m)
			if got := m.ControlLoad().Disposition; got != ControlLoadLoaded {
				t.Fatalf("disposition = %s, want LOADED", got)
			}
			plan, err := m.BootstrapControl(context.Background(), []ControlDriverConfig{{DriverID: "ebus.primary", Enabled: true}, {DriverID: "fam.bad", Enabled: true}})
			if err != nil {
				t.Fatal(err)
			}
			if plan[0].Action != StartupStarting || plan[1].Action != StartupFailed {
				t.Fatalf("plan = %+v, want only fam.bad FAILED", plan)
			}
			if v := rcDriver(t, m, "fam.bad"); !v.Failed || v.Current != nil || v.Reserved != nil {
				t.Fatalf("bad driver exposed transitions: %+v", v)
			}
		})
	}
}

// CommitControlWork cannot produce a layout where the current slot holds the
// newer transition.
func TestControlWorkCannotProduceReversedSlots(t *testing.T) {
	hooks := &rcHooks{}
	m, path := rcSeeded(t, hooks)
	defer rcStop(t, m)
	if _, err := rcDecide(t, m, ControlStop, "ebus.primary", 2); err != nil {
		t.Fatal(err)
	}
	if _, err := rcDecide(t, m, ControlStart, "ebus.primary", 3); err != nil {
		t.Fatal(err)
	}
	v := rcDriver(t, m, "ebus.primary")
	if v.Current == nil || v.Reserved == nil || v.Current.Sequence >= v.Reserved.Sequence {
		t.Fatalf("decision layout = %+v / %+v, want current older than reserved", v.Current, v.Reserved)
	}
	before, writes := rcRead(t, path), hooks.writeCount()
	for name, args := range map[string][2]*ControlTransition{
		"swap":                                 {v.Reserved, v.Current},
		"newer_as_current_with_older_reserved": {v.Reserved, cloneTransition(v.Current)},
	} {
		if err := m.CommitControlWork(rcCtx(t), "ebus.primary", args[0], args[1]); !errors.Is(err, ErrControlInvalidWork) {
			t.Fatalf("%s accepted: %v", name, err)
		}
	}
	if hooks.writeCount() != writes || !bytes.Equal(before, rcRead(t, path)) {
		t.Fatal("rejected reversed layout wrote state")
	}
	// The direct validator also refuses it.
	reversed := ControlDriverRecord{DesiredRevision: v.DesiredRevision, LifecycleSequence: v.Reserved.Sequence, Current: v.Reserved, Reserved: v.Current}
	if reason := validateRecordSlots(reversed); !strings.Contains(reason, "does not precede") {
		t.Fatalf("validateRecordSlots(reversed) = %q", reason)
	}
}

// Every per-record invariant the loader validates fails only that driver
// closed, preserves its bytes, and leaves the sibling record usable.
func TestControlLoaderRejectsEachInvalidRecordInvariant(t *testing.T) {
	tr := func(mut ...[2]string) string {
		fields := map[string]string{
			"kind": `"STOP"`, "desired_revision": `"1"`, "sequence": `"1"`, "phase": `"PENDING"`, "epoch": `""`,
			"generation": `"0"`, "native_close_pending": `false`, "semantic_cleanup_pending": `false`,
			"attempts_used": `0`, "deadline_utc": `""`,
		}
		for _, kv := range mut {
			fields[kv[0]] = kv[1]
		}
		order := []string{"kind", "desired_revision", "sequence", "phase", "epoch", "generation", "native_close_pending", "semantic_cleanup_pending", "attempts_used", "deadline_utc"}
		parts := make([]string, 0, len(order))
		for _, k := range order {
			if v, ok := fields[k]; ok && v != "" {
				parts = append(parts, `"`+k+`":`+v)
			}
		}
		return "{" + strings.Join(parts, ",") + "}"
	}
	rec := func(state, rev, lifecycle, cur, res string) string {
		return `{"desired_state":` + state + `,"desired_revision":` + rev + `,"lifecycle_sequence":` + lifecycle + `,"current":` + cur + `,"reserved":` + res + `}`
	}
	okState, okRev, okSeq := `"RUNNING"`, `"2"`, `"3"`
	cases := map[string]string{
		"state_unknown":           rec(`"PAUSED"`, okRev, okSeq, "null", "null"),
		"state_not_string":        rec(`1`, okRev, okSeq, "null", "null"),
		"revision_zero":           rec(okState, `"0"`, okSeq, "null", "null"),
		"revision_leading_zero":   rec(okState, `"02"`, okSeq, "null", "null"),
		"revision_above_mark":     rec(okState, `"3"`, okSeq, "null", "null"),
		"revision_not_string":     rec(okState, `2`, okSeq, "null", "null"),
		"sequence_leading_zero":   rec(okState, okRev, `"03"`, "null", "null"),
		"sequence_not_string":     rec(okState, okRev, `3`, "null", "null"),
		"unknown_member":          `{"desired_state":"RUNNING","desired_revision":"2","lifecycle_sequence":"3","current":null,"reserved":null,"extra":1}`,
		"missing_member":          `{"desired_state":"RUNNING","desired_revision":"2","lifecycle_sequence":"3","current":null}`,
		"reserved_without_cur":    rec(okState, okRev, okSeq, "null", tr()),
		"transition_kind":         rec(okState, okRev, okSeq, tr([2]string{"kind", `"PAUSE"`}), "null"),
		"transition_phase":        rec(okState, okRev, okSeq, tr([2]string{"phase", `"DONE"`}), "null"),
		"transition_rev_zero":     rec(okState, okRev, okSeq, tr([2]string{"desired_revision", `"0"`}), "null"),
		"transition_seq_zero":     rec(okState, okRev, okSeq, tr([2]string{"sequence", `"0"`}), "null"),
		"transition_gen_format":   rec(okState, okRev, okSeq, tr([2]string{"generation", `"00"`}), "null"),
		"transition_epoch":        rec(okState, okRev, okSeq, tr([2]string{"epoch", `"bad epoch"`}), "null"),
		"transition_epoch_long":   rec(okState, okRev, okSeq, tr([2]string{"epoch", `"` + strings.Repeat("a", 257) + `"`}), "null"),
		"transition_bool":         rec(okState, okRev, okSeq, tr([2]string{"native_close_pending", `"false"`}), "null"),
		"transition_attempts":     rec(okState, okRev, okSeq, tr([2]string{"attempts_used", `9`}), "null"),
		"transition_attempts_ln":  rec(okState, okRev, okSeq, tr([2]string{"attempts_used", `-1`}), "null"),
		"transition_deadline":     rec(okState, okRev, okSeq, tr([2]string{"deadline_utc", `"2030-01-01T00:00:00+03:00"`}), "null"),
		"transition_deadline_fmt": rec(okState, okRev, okSeq, tr([2]string{"deadline_utc", `"tomorrow"`}), "null"),
		"transition_extra":        rec(okState, okRev, okSeq, `{"kind":"STOP"}`, "null"),
		"transition_rev_above":    rec(okState, okRev, okSeq, tr([2]string{"desired_revision", `"3"`}), "null"),
		"transition_seq_above":    rec(okState, okRev, `"1"`, tr([2]string{"sequence", `"2"`}), "null"),
		"slots_duplicate_seq":     rec(okState, okRev, okSeq, tr(), tr([2]string{"kind", `"START"`})),
		"slots_reversed_seq":      rec(okState, okRev, okSeq, tr([2]string{"sequence", `"2"`}), tr([2]string{"kind", `"START"`})),
	}
	// Positive control: the builders produce a record the loader accepts.
	t.Run("valid_ordered_slots_load", func(t *testing.T) {
		path := rcPath(t)
		rcWriteLoaded(t, path, "2", "3", `"fam.ok":`+rec(okState, okRev, okSeq, tr(), tr([2]string{"kind", `"START"`}, [2]string{"sequence", `"2"`})))
		m := rcManager(t, path, nil)
		defer rcStop(t, m)
		m.mu.Lock()
		_, ok := m.state.control.records["fam.ok"]
		m.mu.Unlock()
		if !ok {
			t.Fatal("valid ordered slots rejected by the loader")
		}
	})
	for name, bad := range cases {
		t.Run(name, func(t *testing.T) {
			path := rcPath(t)
			rcWriteLoaded(t, path, "2", "3",
				`"ebus.primary":{"desired_state":"RUNNING","desired_revision":"1","lifecycle_sequence":"0","current":null,"reserved":null},"fam.bad":`+bad)
			m := rcManager(t, path, nil)
			defer rcStop(t, m)
			if got := m.ControlLoad().Disposition; got != ControlLoadLoaded {
				t.Fatalf("disposition = %s, want LOADED", got)
			}
			plan, err := m.BootstrapControl(context.Background(), []ControlDriverConfig{{DriverID: "ebus.primary", Enabled: true}, {DriverID: "fam.bad", Enabled: true}})
			if err != nil {
				t.Fatal(err)
			}
			if plan[0].Action != StartupStarting || plan[1].Action != StartupFailed {
				t.Fatalf("plan = %+v, want only fam.bad FAILED", plan)
			}
			if v := rcDriver(t, m, "fam.bad"); !v.Failed {
				t.Fatalf("bad record not failed: %+v", v)
			}
		})
	}

	t.Run("noncanonical_driver_id", func(t *testing.T) {
		path := rcPath(t)
		rcWriteLoaded(t, path, "2", "3",
			`"ebus.primary":`+rec(`"RUNNING"`, `"1"`, `"0"`, "null", "null")+`,"Bad.ID":`+rec(`"RUNNING"`, `"1"`, `"0"`, "null", "null"))
		m := rcManager(t, path, nil)
		defer rcStop(t, m)
		if got := m.ControlLoad().Disposition; got != ControlLoadLoaded {
			t.Fatalf("disposition = %s, want LOADED", got)
		}
		m.mu.Lock()
		_, failed := m.state.control.failed["Bad.ID"]
		_, kept := m.state.control.records["ebus.primary"]
		m.mu.Unlock()
		if !failed || !kept {
			t.Fatalf("noncanonical id failed=%v sibling kept=%v", failed, kept)
		}
	})
}

func p09Unchanged(t *testing.T, m *Manager, path string, hooks *rcHooks, before []byte, snapshot ControlCatalogView, writes int) {
	t.Helper()
	if !bytes.Equal(before, rcRead(t, path)) || hooks.writeCount() != writes {
		t.Fatal("refused update changed durable bytes or attempted a write")
	}
	if got := rcSnapshot(t, m); !reflect.DeepEqual(got, snapshot) {
		t.Fatalf("refused update changed catalog, desired revision or issued mark: before=%+v after=%+v", snapshot, got)
	}
}

func TestP09PlanRequiresNewRequestedKind(t *testing.T) {
	for _, method := range []ControlMethod{ControlStart, ControlStop, ControlRestart} {
		for _, layout := range []string{"empty", "retained_only", "wrong_kind", "retained_requested_new_wrong_kind", "new_requested", "retained_and_new_requested"} {
			t.Run(string(method)+"/"+layout, func(t *testing.T) {
				path := rcPath(t)
				state := "RUNNING"
				if method == ControlStart {
					state = "STOPPED"
				}
				rcWriteLoaded(t, path, "1", "2", `"fam.primary":{"desired_state":"`+state+`","desired_revision":"1","lifecycle_sequence":"1","current":`+
					rcTransitionJSON(string(method), "1", "1")+`,"reserved":null}`)
				hooks := &rcHooks{}
				m := rcManager(t, path, hooks)
				defer rcStop(t, m)
				if _, err := m.BootstrapControl(rcCtx(t), []ControlDriverConfig{{DriverID: "fam.primary", Enabled: true}}); err != nil {
					t.Fatal(err)
				}
				before, snapshot, writes := rcRead(t, path), rcSnapshot(t, m), hooks.writeCount()
				wrong := TransitionStop
				if method == ControlStop {
					wrong = TransitionStart
				}
				fresh := func(kind TransitionKind) *ControlTransition {
					return &ControlTransition{Kind: kind, Phase: PhasePending}
				}
				_, err := m.DecideControl(rcCtx(t), method, ControlRequest{DriverID: "fam.primary", ExpectedRevision: 1}, func(in ControlPlanInput) (ControlPlan, *ControlRefusal) {
					switch layout {
					case "empty":
						return ControlPlan{}, nil
					case "retained_only":
						return ControlPlan{Current: in.Record.Current}, nil
					case "wrong_kind":
						return ControlPlan{Current: fresh(wrong)}, nil
					case "retained_requested_new_wrong_kind":
						return ControlPlan{Current: in.Record.Current, Reserved: fresh(wrong)}, nil
					case "retained_and_new_requested":
						return ControlPlan{Current: in.Record.Current, Reserved: fresh(TransitionKind(method))}, nil
					default:
						return ControlPlan{Current: fresh(TransitionKind(method))}, nil
					}
				})
				valid := layout == "new_requested" || layout == "retained_and_new_requested"
				if !valid {
					_ = rcRefusal(t, err, RefusalTransitionBusy, 8)
					p09Unchanged(t, m, path, hooks, before, snapshot, writes)
					return
				}
				if err != nil {
					t.Fatalf("valid requested allocation refused: %v", err)
				}
				got := rcDriver(t, m, "fam.primary")
				work := got.Current
				if layout == "retained_and_new_requested" {
					work = got.Reserved
				}
				if work == nil || work.Kind != TransitionKind(method) || work.Sequence != 2 || work.DesiredRevision != 2 ||
					got.DesiredRevision != 2 || rcSnapshot(t, m).IssuedRevisionMark != 2 || rcSnapshot(t, m).CatalogRevision != 3 {
					t.Fatalf("requested allocation or counters incorrect: %+v / %+v", got, work)
				}
			})
		}
	}
}

func TestP09WorkProgression(t *testing.T) {
	for _, name := range []string{
		"epoch_replace_native_pending", "epoch_replace_semantic_pending", "epoch_remove", "epoch_remove_pair",
		"generation_decrease", "generation_remove", "native_reactivate", "semantic_reactivate",
		"identity_kind", "identity_revision", "identity_sequence",
	} {
		t.Run(name, func(t *testing.T) {
			path := rcPath(t)
			rcWriteLoaded(t, path, "3", "4", `"fam.primary":{"desired_state":"RUNNING","desired_revision":"3","lifecycle_sequence":"2","current":`+
				rcTransitionJSON("START", "3", "1")+`,"reserved":null}`)
			hooks := &rcHooks{}
			m := rcManager(t, path, hooks)
			defer rcStop(t, m)
			if _, err := m.BootstrapControl(rcCtx(t), []ControlDriverConfig{{DriverID: "fam.primary", Enabled: true}}); err != nil {
				t.Fatal(err)
			}
			stored := rcDriver(t, m, "fam.primary").Current
			stored.Epoch, stored.Generation, stored.Phase = "p09.epoch-one", 2, PhaseActive
			stored.NativeClosePending = name == "epoch_replace_native_pending" || name == "native_reactivate"
			stored.SemanticCleanupPending = name == "epoch_replace_semantic_pending" || name == "semantic_reactivate"
			if err := m.CommitControlWork(rcCtx(t), "fam.primary", stored, nil); err != nil {
				t.Fatalf("valid initial assignment refused: %v", err)
			}
			if name == "native_reactivate" || name == "semantic_reactivate" {
				stored.NativeClosePending, stored.SemanticCleanupPending = false, false
				if err := m.CommitControlWork(rcCtx(t), "fam.primary", stored, nil); err != nil {
					t.Fatalf("valid obligation clearing refused: %v", err)
				}
			}
			next := cloneTransition(stored)
			switch name {
			case "epoch_replace_native_pending", "epoch_replace_semantic_pending":
				next.Epoch = "p09.epoch-two"
			case "epoch_remove":
				next.Epoch = ""
			case "epoch_remove_pair":
				next.Epoch, next.Generation = "", 0
			case "generation_decrease":
				next.Generation = 1
			case "generation_remove":
				next.Generation = 0
			case "native_reactivate":
				next.NativeClosePending = true
			case "semantic_reactivate":
				next.SemanticCleanupPending = true
			case "identity_kind":
				next.Kind = TransitionRestart
			case "identity_revision":
				next.DesiredRevision = 2
			case "identity_sequence":
				next.Sequence = 2
			}
			// Both candidates are individually well formed; only progression
			// from the durable baseline makes the update invalid.
			if err := validateTransition(next); err != nil {
				t.Fatalf("candidate is not a valid transition: %v", err)
			}
			before, snapshot, writes := rcRead(t, path), rcSnapshot(t, m), hooks.writeCount()
			if err := m.CommitControlWork(rcCtx(t), "fam.primary", next, nil); !errors.Is(err, ErrControlInvalidWork) {
				t.Fatalf("invalid progression accepted: %v", err)
			}
			p09Unchanged(t, m, path, hooks, before, snapshot, writes)
		})
	}
}

func TestP09ValidCorrelationProgression(t *testing.T) {
	m, _ := rcSeeded(t, nil)
	defer rcStop(t, m)
	if _, err := rcDecide(t, m, ControlRestart, "ebus.primary", 2); err != nil {
		t.Fatal(err)
	}
	base := rcDriver(t, m, "ebus.primary").Current
	commit := func(next *ControlTransition) {
		t.Helper()
		if err := m.CommitControlWork(rcCtx(t), "ebus.primary", next, nil); err != nil {
			t.Fatalf("valid progression refused: %v", err)
		}
		if got := rcDriver(t, m, "ebus.primary").Current; !reflect.DeepEqual(got, next) {
			t.Fatalf("work not stored: got %+v want %+v", got, next)
		}
	}
	assigned := cloneTransition(base)
	assigned.Epoch, assigned.Generation = "p09.epoch-one", 1
	commit(assigned)
	active := cloneTransition(assigned)
	active.Phase, active.NativeClosePending, active.SemanticCleanupPending = PhaseActive, true, true
	commit(active)
	retry := cloneTransition(active)
	retry.AttemptsUsed, retry.Generation = 1, 2
	commit(retry)
	settled := cloneTransition(retry)
	settled.NativeClosePending, settled.SemanticCleanupPending = false, false
	commit(settled)
	replacement := cloneTransition(settled)
	replacement.Epoch, replacement.Generation = "p09.epoch-two", 1
	replacement.NativeClosePending, replacement.SemanticCleanupPending = true, true
	commit(replacement)
}

func TestP09ReservedMustSurviveClearing(t *testing.T) {
	hooks := &rcHooks{}
	m, path := rcSeeded(t, hooks)
	defer rcStop(t, m)
	if _, err := rcDecide(t, m, ControlStop, "ebus.primary", 2); err != nil {
		t.Fatal(err)
	}
	if _, err := rcDecide(t, m, ControlStart, "ebus.primary", 3); err != nil {
		t.Fatal(err)
	}
	stored := rcDriver(t, m, "ebus.primary")
	if stored.Current == nil || stored.Reserved == nil {
		t.Fatal("valid baseline needs two slots")
	}
	before, snapshot, writes := rcRead(t, path), rcSnapshot(t, m), hooks.writeCount()
	if err := m.CommitControlWork(rcCtx(t), "ebus.primary", nil, nil); !errors.Is(err, ErrControlInvalidWork) {
		t.Fatalf("nil/nil discarded reserved work: %v", err)
	}
	p09Unchanged(t, m, path, hooks, before, snapshot, writes)
	if err := m.CommitControlWork(rcCtx(t), "ebus.primary", stored.Reserved, nil); err != nil {
		t.Fatalf("valid reserved promotion refused: %v", err)
	}
	promoted := rcDriver(t, m, "ebus.primary")
	if !reflect.DeepEqual(promoted.Current, stored.Reserved) || promoted.Reserved != nil {
		t.Fatalf("promotion did not preserve reserved work: %+v", promoted)
	}
	if err := m.CommitControlWork(rcCtx(t), "ebus.primary", nil, nil); err != nil {
		t.Fatalf("terminal clearing without reserved work refused: %v", err)
	}
}
