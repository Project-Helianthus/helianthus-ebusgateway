package main

// M71 follow-up (issue #1360): the eeBUS operator MCP socket location is
// bounded and degrades instead of failing the gateway.
//
//   - -eebus-operator-socket-dir names the directory; the socket path it implies
//     must fit sockaddr_un, and an unusable explicit directory fails the start.
//   - The default sibling <state-root>-operator moves to a bounded temp-directory
//     location when it is too long to dial or its parent is not writable.
//   - An endpoint that cannot be served at the default location is logged and
//     reported as degraded eeBUS readiness; the gateway keeps running.

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"syscall"
	"testing"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/eebusadmin"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
)

var m71FallbackName = regexp.MustCompile(`^helianthus-eebus-op-[0-9]+-[0-9a-f]{8}$`)

// m71TempDirSeam makes the fallback's temp directory a short private
// directory and returns it. It is registered before the process starts, so it
// is restored after the process stopped.
func m71TempDirSeam(t *testing.T) string {
	t.Helper()
	dir := m71Base(t)
	original := eebusOperatorTempDir
	eebusOperatorTempDir = func() string { return dir }
	t.Cleanup(func() { eebusOperatorTempDir = original })
	return dir
}

// m71CreatableSeam replaces the writability probe of the default location.
func m71CreatableSeam(t *testing.T, probe func(string) bool) {
	t.Helper()
	original := eebusOperatorDirectoryCreatable
	eebusOperatorDirectoryCreatable = probe
	t.Cleanup(func() { eebusOperatorDirectoryCreatable = original })
}

// m71EndpointStateReset restores the process-wide endpoint state after a test.
func m71EndpointStateReset(t *testing.T) {
	t.Helper()
	eebusOperatorEndpointDown.Store(false)
	t.Cleanup(func() { eebusOperatorEndpointDown.Store(false) })
}

// m71FallbackFor is the fallback socket the rules choose for a state root
// whose default location is unusable, under tempDir.
func m71FallbackFor(t *testing.T, tempDir, stateRoot string) string {
	t.Helper()
	got := eebusOperatorFallbackSocket(mcp.EEBusV1OperatorSocketPath(stateRoot))
	directory := filepath.Dir(got)
	if filepath.Dir(directory) != tempDir || !m71FallbackName.MatchString(filepath.Base(directory)) || filepath.Base(got) != mcp.EEBusV1OperatorSocketName {
		t.Fatalf("fallback socket %q is not <temp dir>/helianthus-eebus-op-<uid>-<id>/operator-mcp.sock", got)
	}
	return got
}

// m71AssertFallbackRun checks a settled process whose operator socket is at
// the fallback: the sibling was never created, the fallback was logged exactly
// once with its reason and no path, and eeBUS is RUNNING and ready.
func m71AssertFallbackRun(t *testing.T, p *m71Process, status runtimecontrol.DriverStatus, logs *m71LogBuffer, stateRoot, tempDir, reason string) {
	t.Helper()
	want := m71FallbackFor(t, tempDir, stateRoot)
	m71AssertRunningWithOperatorSocketAt(t, p, status, stateRoot, want)
	if _, err := os.Lstat(stateRoot + "-operator"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the default sibling was created although the fallback applies: %v", err)
	}
	text := logs.String()
	lines := m71Lines(text, m71FallbackLine)
	if len(lines) != 1 || !strings.HasSuffix(lines[0], "reason="+reason+"; using a bounded location in the temp directory") {
		t.Fatalf("fallback log lines = %q; want one ending reason=%s", lines, reason)
	}
	if lines := m71Lines(text, m71UnavailableLine); len(lines) != 0 {
		t.Fatalf("an unavailable-endpoint line was logged for a served fallback: %q", lines)
	}
	for _, path := range []string{filepath.Dir(stateRoot), stateRoot, tempDir, want} {
		if strings.Contains(text, path) {
			t.Fatalf("log contains a path:\n%s", text)
		}
	}
	if readiness := projectGatewayReadiness(nil, (&managedSidecarBindings{sidecars: p.sidecars}).eebusLifecycleSnapshot()); readiness.EEBusReadiness != "READY" || readiness.EEBusDegradedReason != "" {
		t.Fatalf("eeBUS readiness = %s/%s; want READY (a served fallback is not degraded)", readiness.EEBusReadiness, readiness.EEBusDegradedReason)
	}
}

// A volume mounted directly at the state root leaves its parent unwritable,
// so the sibling cannot be created. The socket moves to the fallback, the
// gateway starts and the eeBUS runtime reaches RUNNING.
func TestM71UnwritableStateParentUsesTheBoundedFallback(t *testing.T) {
	m71EndpointStateReset(t)
	logs := m71CaptureLog(t)
	_, stateRoot := m71StateRoot(t)
	tempDir := m71TempDirSeam(t)
	m71CreatableSeam(t, func(string) bool { return false })
	p, status := m71Start(t, stateRoot)
	m71AssertFallbackRun(t, p, status, logs, stateRoot, tempDir, "parent_not_writable")
}

// The same on the filesystem: the parent of the state root is read-only for
// this user. A root user could write there, so the case needs another user.
func TestM71ReadOnlyStateParentOnTheFilesystemUsesTheBoundedFallback(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("a root user can write to a read-only directory")
	}
	m71EndpointStateReset(t)
	logs := m71CaptureLog(t)
	base, stateRoot := m71StateRoot(t)
	tempDir := m71TempDirSeam(t)
	if err := os.Chmod(base, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(base, 0o700) })
	p, status := m71Start(t, stateRoot)
	m71AssertFallbackRun(t, p, status, logs, stateRoot, tempDir, "parent_not_writable")
}

// A long custom state root derives a sibling address that clients cannot dial;
// the socket moves to the fallback although the parent is writable.
func TestM71OverlongDefaultStateRootUsesTheBoundedFallback(t *testing.T) {
	m71EndpointStateReset(t)
	logs := m71CaptureLog(t)
	base := m71Base(t)
	stateRoot := filepath.Join(base, strings.Repeat("r", 100), "eebus")
	if err := os.MkdirAll(stateRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if derived := mcp.EEBusV1OperatorSocketPath(stateRoot); len(derived) <= mcp.EEBusV1OperatorSocketPathLimit {
		t.Fatalf("fixture sibling address has %d bytes; want more than %d", len(derived), mcp.EEBusV1OperatorSocketPathLimit)
	}
	tempDir := m71TempDirSeam(t)
	p, status := m71Start(t, stateRoot)
	m71AssertFallbackRun(t, p, status, logs, stateRoot, tempDir, "path_too_long")
}

// An endpoint that cannot be served at the default location, fallback
// included, is logged by its category and reported as degraded eeBUS
// readiness; the gateway starts and the eeBUS runtime stays RUNNING.
func TestM71UnavailableOperatorEndpointDegradesInsteadOfFailingTheGateway(t *testing.T) {
	m71EndpointStateReset(t)
	logs := m71CaptureLog(t)
	base, stateRoot := m71StateRoot(t)
	p, status := m71StartWith(t, stateRoot, m71Options{operatorError: func(socketPath string) error {
		return &os.PathError{Op: "mkdir", Path: socketPath, Err: syscall.EACCES}
	}})
	lifecycle := (&managedSidecarBindings{sidecars: p.sidecars}).eebusLifecycleSnapshot()
	if status.ObservedState != drivermanager.ObservedRunning || status.Reason != drivermanager.ReasonNone {
		t.Fatalf("eebus.primary = %s/%s; want RUNNING/NONE although the operator endpoint is down", status.ObservedState, status.Reason)
	}
	if rejections := p.storeRejections(); len(rejections) != 0 {
		t.Fatalf("eeBUS store rejected state root entries %q", rejections)
	}
	if sockets := p.operatorSockets(); len(sockets) != 1 {
		t.Fatalf("operator endpoint attempts = %q; want one (the default location)", sockets)
	}
	text := logs.String()
	lines := m71Lines(text, m71UnavailableLine)
	if len(lines) != 1 || !strings.HasSuffix(lines[0], "cause="+syscall.EACCES.Error()+"; the gateway continues without it") {
		t.Fatalf("unavailable log lines = %q; want one with the errno category only", lines)
	}
	if strings.Contains(text, base) {
		t.Fatalf("log contains a path:\n%s", text)
	}
	readiness := projectGatewayReadiness(nil, lifecycle)
	if readiness.EEBusReadiness != string(eebusadmin.EEBusReadinessDegraded) || readiness.EEBusDegradedReason != string(eebusadmin.EEBusDegradedReasonAdminBoundaryUnavailable) {
		t.Fatalf("eeBUS readiness = %s/%s; want DEGRADED/ADMIN_BOUNDARY_UNAVAILABLE", readiness.EEBusReadiness, readiness.EEBusDegradedReason)
	}
	if readiness.ProcessReadiness != string(eebusadmin.ProcessReadinessReady) || readiness.HTTPReadiness != "READY" {
		t.Fatalf("process/HTTP readiness = %s/%s; the gateway itself stays ready", readiness.ProcessReadiness, readiness.HTTPReadiness)
	}
}

// The admin status handler reports the same readiness, read at request time.
func TestM71AdminStatusReflectsTheOperatorEndpointState(t *testing.T) {
	m71EndpointStateReset(t)
	ready := eebusadmin.ReadinessV1{ProcessReadiness: eebusadmin.ProcessReadinessReady, EEBusReadiness: eebusadmin.EEBusReadinessReady}
	handler := unavailableEEBusAdminHandler(ready)
	readinessOf := func() map[string]any {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/admin/eebus/v1/status", nil))
		var envelope struct {
			Data struct {
				Readiness map[string]any `json:"readiness"`
			} `json:"data"`
		}
		if err := json.Unmarshal(recorder.Body.Bytes(), &envelope); err != nil {
			t.Fatalf("decode admin status: %v\n%s", err, recorder.Body.String())
		}
		return envelope.Data.Readiness
	}
	if got := readinessOf(); got["eebus_readiness"] != "READY" || got["eebus_degraded_reason"] != nil {
		t.Fatalf("admin readiness with the endpoint up = %v; want READY", got)
	}
	eebusOperatorEndpointDown.Store(true)
	if got := readinessOf(); got["eebus_readiness"] != "DEGRADED" || got["eebus_degraded_reason"] != "ADMIN_BOUNDARY_UNAVAILABLE" {
		t.Fatalf("admin readiness with the endpoint down = %v; want DEGRADED/ADMIN_BOUNDARY_UNAVAILABLE", got)
	}
	// Only a ready eeBUS degrades; a disabled or already degraded one is reported as is.
	for _, readiness := range []eebusadmin.ReadinessV1{
		{ProcessReadiness: eebusadmin.ProcessReadinessReady, EEBusReadiness: eebusadmin.EEBusReadinessDisabled},
		{ProcessReadiness: eebusadmin.ProcessReadinessReady, EEBusReadiness: eebusadmin.EEBusReadinessDegraded, EEBusDegradedReason: eebusadmin.EEBusDegradedReasonLocalIdentityUnavailable},
	} {
		if got := eebusReadinessWithOperatorEndpoint(readiness); got != readiness {
			t.Fatalf("readiness %+v changed to %+v by the operator endpoint state", readiness, got)
		}
	}
}

type m71Closer struct{ closed int }

func (c *m71Closer) Close() error { c.closed++; return nil }

// openEEBusOperatorEndpoint: a configured directory that cannot be used fails;
// the default location degrades.
func TestM71OpenOperatorEndpointFailsOnlyForAConfiguredDirectory(t *testing.T) {
	original := startEEBusOperatorEndpoint
	t.Cleanup(func() { startEEBusOperatorEndpoint = original })
	var starts []string
	var startErr error
	endpoint := &m71Closer{}
	startEEBusOperatorEndpoint = func(_ *mcp.Server, _ context.Context, socketPath string) (io.Closer, error) {
		starts = append(starts, socketPath)
		if startErr != nil {
			return nil, startErr
		}
		return endpoint, nil
	}
	m71CreatableSeam(t, func(string) bool { return true })
	denied := &os.PathError{Op: "mkdir", Path: "/synthetic/operator", Err: syscall.EACCES}
	overlong := "/" + strings.Repeat("d", 110)

	for _, test := range []struct {
		name      string
		stateRoot string
		explicit  string
		startErr  error
		wantPath  string
		wantErr   string
		wantDown  bool
		wantNil   bool
	}{
		{name: "default served", stateRoot: "/data/eebus", wantPath: "/data/eebus-operator/operator-mcp.sock"},
		{name: "default without a state root", wantPath: "/data/eebus-operator/operator-mcp.sock"},
		{name: "default start failure degrades", stateRoot: "/data/eebus", startErr: denied, wantPath: "/data/eebus-operator/operator-mcp.sock", wantNil: true, wantDown: true},
		{name: "configured directory served", stateRoot: "/data/eebus", explicit: "/run/helianthus/op", wantPath: "/run/helianthus/op/operator-mcp.sock"},
		{name: "configured start failure fails", stateRoot: "/data/eebus", explicit: "/run/helianthus/op", startErr: denied, wantPath: "/run/helianthus/op/operator-mcp.sock", wantErr: "permission denied"},
		{name: "configured over-long directory fails before starting", stateRoot: "/data/eebus", explicit: overlong, wantErr: "AF_UNIX limit"},
		{name: "configured relative directory fails before starting", explicit: "run/op", wantErr: "must be absolute"},
		{name: "configured directory inside the state root fails before starting", stateRoot: "/data/eebus", explicit: "/data/eebus/operator", wantErr: "outside the eeBUS state root"},
	} {
		t.Run(test.name, func(t *testing.T) {
			m71EndpointStateReset(t)
			logs := m71CaptureLog(t)
			starts, startErr, endpoint.closed = nil, test.startErr, 0
			got, err := openEEBusOperatorEndpoint(context.Background(), nil, test.stateRoot, test.explicit)
			switch {
			case test.wantErr != "":
				if err == nil || !strings.Contains(err.Error(), test.wantErr) || got != nil {
					t.Fatalf("endpoint, error = %v, %v; want an error containing %q", got, err, test.wantErr)
				}
				if eebusOperatorEndpointDown.Load() {
					t.Fatal("a configured directory that cannot be used marked the endpoint degraded; it fails the start instead")
				}
			case test.wantNil:
				if err != nil || got != nil {
					t.Fatalf("endpoint, error = %v, %v; want nil, nil (degraded)", got, err)
				}
			default:
				if err != nil || got != io.Closer(endpoint) {
					t.Fatalf("endpoint, error = %v, %v; want the started endpoint", got, err)
				}
			}
			if eebusOperatorEndpointDown.Load() != test.wantDown {
				t.Fatalf("endpoint down = %v; want %v", eebusOperatorEndpointDown.Load(), test.wantDown)
			}
			var wantStarts []string
			if test.wantPath != "" {
				wantStarts = []string{test.wantPath}
			}
			if !reflect.DeepEqual(starts, wantStarts) {
				t.Fatalf("endpoint starts = %q; want %q", starts, wantStarts)
			}
			if test.wantDown {
				lines := m71Lines(logs.String(), m71UnavailableLine)
				if len(lines) != 1 || strings.Contains(lines[0], "/synthetic") || !strings.Contains(lines[0], "cause=permission denied") {
					t.Fatalf("unavailable log lines = %q; want one with the category only", lines)
				}
			}
		})
	}

	t.Run("recovery clears the degraded state", func(t *testing.T) {
		m71EndpointStateReset(t)
		m71CaptureLog(t)
		starts, startErr = nil, denied
		if got, err := openEEBusOperatorEndpoint(context.Background(), nil, "/data/eebus", ""); got != nil || err != nil || !eebusOperatorEndpointDown.Load() {
			t.Fatalf("failed default start = %v, %v, down=%v; want nil, nil, down", got, err, eebusOperatorEndpointDown.Load())
		}
		startErr = nil
		if got, err := openEEBusOperatorEndpoint(context.Background(), nil, "/data/eebus", ""); got != io.Closer(endpoint) || err != nil || eebusOperatorEndpointDown.Load() {
			t.Fatalf("served default start = %v, %v, down=%v; want the endpoint, up", got, err, eebusOperatorEndpointDown.Load())
		}
	})
}

// The control plane fails the start for a configured directory that cannot be
// used, before it binds the TCP listener.
func TestM71ControlPlaneFailsForAnOverlongConfiguredOperatorDirectory(t *testing.T) {
	m71EndpointStateReset(t)
	operator, listen := startEEBusOperatorEndpoint, listenHTTPControlPlane
	t.Cleanup(func() { startEEBusOperatorEndpoint, listenHTTPControlPlane = operator, listen })
	startEEBusOperatorEndpoint = func(*mcp.Server, context.Context, string) (io.Closer, error) {
		t.Error("the operator endpoint started for an over-long configured directory")
		return &m71Closer{}, nil
	}
	listenHTTPControlPlane = func(string, string) (net.Listener, error) {
		t.Error("the TCP listener opened although the operator directory is unusable")
		return nil, errors.New("unexpected listen")
	}
	cfg := ebusgateway.DefaultConfig()
	cfg.HTTPAddr = "127.0.0.1:0"
	cfg.EEBusConfig.OperatorSocketDir = "/" + strings.Repeat("d", 110)
	server, _, err := startHTTPControlPlaneListener(context.Background(), cfg, http.NewServeMux(), nil, nil, &msp05bRuntime{})
	if server != nil || err == nil || !strings.Contains(err.Error(), "start eeBUS operator MCP endpoint") || !strings.Contains(err.Error(), "AF_UNIX limit") {
		t.Fatalf("start = %v, %v; want the operator endpoint error", server, err)
	}
	if strings.Contains(err.Error(), "ddddd") {
		t.Fatalf("error carries the configured path: %v", err)
	}
}

// The location rules for the default and the configured directory.
func TestM71OperatorSocketLocationRules(t *testing.T) {
	const addon = "/data/eebus-operator/operator-mcp.sock"
	tempDir := "/tmp/m71-rules"
	original := eebusOperatorTempDir
	t.Cleanup(func() { eebusOperatorTempDir = original })
	eebusOperatorTempDir = func() string { return tempDir }
	creatable := true
	m71CreatableSeam(t, func(string) bool { return creatable })
	limit := mcp.EEBusV1OperatorSocketPathLimit
	longRoot := "/" + strings.Repeat("r", 100)
	exactDir := "/" + strings.Repeat("e", limit-len("/"+mcp.EEBusV1OperatorSocketName)-1)

	t.Run("the add-on default is the unchanged sibling", func(t *testing.T) {
		for _, root := range []string{"/data/eebus", "", "relative", "/"} {
			got, err := resolveEEBusOperatorSocket(root, "")
			if err != nil || got != (eebusOperatorSocketLocation{Path: addon}) {
				t.Errorf("resolve(%q) = %+v, %v; want %q without fallback", root, got, err, addon)
			}
		}
	})
	t.Run("another writable state root derives its sibling", func(t *testing.T) {
		got, err := resolveEEBusOperatorSocket("/var/lib/helianthus/eebus", "")
		if want := "/var/lib/helianthus/eebus-operator/operator-mcp.sock"; err != nil || got != (eebusOperatorSocketLocation{Path: want}) {
			t.Fatalf("resolve = %+v, %v; want %q", got, err, want)
		}
	})
	t.Run("an unwritable parent falls back", func(t *testing.T) {
		creatable = false
		t.Cleanup(func() { creatable = true })
		got, err := resolveEEBusOperatorSocket("/data/eebus", "")
		if err != nil || got.Explicit || got.Fallback != eebusOperatorFallbackParentNotWritable || filepath.Dir(filepath.Dir(got.Path)) != tempDir {
			t.Fatalf("resolve = %+v, %v; want the fallback under %s for parent_not_writable", got, err, tempDir)
		}
		if !m71FallbackName.MatchString(filepath.Base(filepath.Dir(got.Path))) || filepath.Base(got.Path) != mcp.EEBusV1OperatorSocketName {
			t.Fatalf("fallback path %q has the wrong shape", got.Path)
		}
		if mcp.ValidateEEBusV1OperatorSocketPath(got.Path) != nil {
			t.Fatalf("fallback path %q exceeds the AF_UNIX limit", got.Path)
		}
	})
	t.Run("an over-long default falls back whatever the parent allows", func(t *testing.T) {
		got, err := resolveEEBusOperatorSocket(longRoot, "")
		if err != nil || got.Fallback != eebusOperatorFallbackPathTooLong || mcp.ValidateEEBusV1OperatorSocketPath(got.Path) != nil {
			t.Fatalf("resolve = %+v, %v; want a bounded fallback for path_too_long", got, err)
		}
	})
	t.Run("a default exactly at the limit is kept", func(t *testing.T) {
		// root + "-operator/operator-mcp.sock" is exactly limit bytes.
		root := "/" + strings.Repeat("s", limit-len("/-operator/"+mcp.EEBusV1OperatorSocketName))
		derived := mcp.EEBusV1OperatorSocketPath(root)
		if len(derived) != limit {
			t.Fatalf("fixture address has %d bytes; want %d", len(derived), limit)
		}
		got, err := resolveEEBusOperatorSocket(root, "")
		if err != nil || got != (eebusOperatorSocketLocation{Path: derived}) {
			t.Fatalf("resolve = %+v, %v; want the sibling kept at the limit", got, err)
		}
		got, err = resolveEEBusOperatorSocket(root+"x", "")
		if err != nil || got.Fallback != eebusOperatorFallbackPathTooLong {
			t.Fatalf("resolve one byte over = %+v, %v; want the fallback", got, err)
		}
	})
	t.Run("every default location has its own fallback directory", func(t *testing.T) {
		creatable = false
		t.Cleanup(func() { creatable = true })
		first, _ := resolveEEBusOperatorSocket("/data/eebus", "")
		again, _ := resolveEEBusOperatorSocket("/data/eebus", "")
		other, _ := resolveEEBusOperatorSocket("/srv/eebus", "")
		if first.Path != again.Path || first.Path == other.Path {
			t.Fatalf("fallbacks = %q, %q, %q; want a stable per-location directory", first.Path, again.Path, other.Path)
		}
	})
	t.Run("a temp directory without room uses /tmp", func(t *testing.T) {
		creatable = false
		t.Cleanup(func() { creatable = true })
		eebusOperatorTempDir = func() string { return "/" + strings.Repeat("t", 100) }
		t.Cleanup(func() { eebusOperatorTempDir = func() string { return tempDir } })
		got, err := resolveEEBusOperatorSocket("/data/eebus", "")
		if err != nil || filepath.Dir(filepath.Dir(got.Path)) != "/tmp" || mcp.ValidateEEBusV1OperatorSocketPath(got.Path) != nil {
			t.Fatalf("resolve = %+v, %v; want a fallback under /tmp", got, err)
		}
	})
	t.Run("a relative temp directory uses /tmp", func(t *testing.T) {
		creatable = false
		t.Cleanup(func() { creatable = true })
		eebusOperatorTempDir = func() string { return "relative/tmp" }
		t.Cleanup(func() { eebusOperatorTempDir = func() string { return tempDir } })
		got, err := resolveEEBusOperatorSocket("/data/eebus", "")
		if err != nil || filepath.Dir(filepath.Dir(got.Path)) != "/tmp" {
			t.Fatalf("resolve = %+v, %v; want a fallback under /tmp", got, err)
		}
	})
	t.Run("a configured directory is used as is, without probing or fallback", func(t *testing.T) {
		m71CreatableSeam(t, func(string) bool { t.Error("the writability probe ran for a configured directory"); return false })
		got, err := resolveEEBusOperatorSocket("/data/eebus", "/run/helianthus/op")
		if want := "/run/helianthus/op/operator-mcp.sock"; err != nil || got != (eebusOperatorSocketLocation{Path: want, Explicit: true}) {
			t.Fatalf("resolve = %+v, %v; want %q", got, err, want)
		}
		got, err = resolveEEBusOperatorSocket("/data/eebus", exactDir)
		if err != nil || len(got.Path) != limit {
			t.Fatalf("resolve at the limit = %+v, %v (%d bytes); want accepted", got, err, len(got.Path))
		}
	})
	t.Run("a configured directory that cannot be used is rejected", func(t *testing.T) {
		for _, test := range []struct{ dir, want string }{
			{exactDir + "x", "AF_UNIX limit"},
			{"/" + strings.Repeat("d", 300), "AF_UNIX limit"},
			{"relative/op", "must be absolute"},
			{"/run/../op", "traversal"},
			{"/run/op\x00", "NUL"},
			{"/", "filesystem root"},
			{"/data/eebus", "outside the eeBUS state root"},
			{"/data/eebus/op", "outside the eeBUS state root"},
		} {
			got, err := resolveEEBusOperatorSocket("/data/eebus", test.dir)
			if err == nil || !strings.Contains(err.Error(), test.want) || !got.Explicit || got.Path != "" {
				t.Errorf("resolve(%q) = %+v, %v; want a rejection containing %q", test.dir, got, err, test.want)
			}
			if err != nil && len(test.dir) > 8 && strings.Contains(err.Error(), test.dir) {
				t.Errorf("rejection of %q carries the path: %v", test.dir, err)
			}
		}
	})
	t.Run("a sibling-named directory is not inside the state root", func(t *testing.T) {
		got, err := resolveEEBusOperatorSocket("/data/eebus", "/data/eebus-operator")
		if want := addon; err != nil || got.Path != want || !got.Explicit {
			t.Fatalf("resolve = %+v, %v; want %q", got, err, want)
		}
	})
}

// The writability probe: an existing directory, or an existing writable
// ancestor, passes; a file, a link or a read-only ancestor does not.
func TestM71DirectoryCreatableProbe(t *testing.T) {
	base := m71Base(t)
	if err := os.Mkdir(filepath.Join(base, "existing"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base, "file"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(base, "existing"), filepath.Join(base, "link")); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		dir  string
		want bool
	}{
		{"existing directory", filepath.Join(base, "existing"), true},
		{"missing under a writable directory", filepath.Join(base, "missing"), true},
		{"several levels missing", filepath.Join(base, "a", "b", "c"), true},
		{"a file in the way", filepath.Join(base, "file"), false},
		{"below a file", filepath.Join(base, "file", "child"), false},
		{"a link in the way", filepath.Join(base, "link"), false},
		{"below a link", filepath.Join(base, "link", "child"), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := eebusDirectoryCreatable(test.dir); got != test.want {
				t.Fatalf("eebusDirectoryCreatable = %v; want %v", got, test.want)
			}
		})
	}
	if os.Geteuid() == 0 {
		t.Log("read-only ancestors are skipped for a root user")
		return
	}
	readOnly := filepath.Join(base, "readonly")
	if err := os.Mkdir(readOnly, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(readOnly, 0o700) })
	if eebusDirectoryCreatable(filepath.Join(readOnly, "operator")) {
		t.Fatal("a directory below a read-only ancestor was reported creatable")
	}
	if !eebusDirectoryCreatable(readOnly) {
		t.Fatal("an existing read-only directory needs no creation and was reported unusable")
	}
	closed := filepath.Join(base, "closed")
	if err := os.Mkdir(closed, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(closed, 0o700) })
	if eebusDirectoryCreatable(filepath.Join(closed, "operator")) {
		t.Fatal("a directory below an untraversable ancestor was reported creatable")
	}
}

func m71ParseOperatorFlags(args ...string) (ebusgateway.Config, error) {
	cfg := ebusgateway.DefaultConfig()
	fs := flag.NewFlagSet("operator-dir", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	bindFlags(fs, &cfg)
	return cfg, fs.Parse(args)
}

func TestM71OperatorSocketDirFlag(t *testing.T) {
	limit := mcp.EEBusV1OperatorSocketPathLimit
	exactDir := "/" + strings.Repeat("e", limit-len("/"+mcp.EEBusV1OperatorSocketName)-1)

	t.Run("unset keeps the default", func(t *testing.T) {
		cfg, err := m71ParseOperatorFlags("-eebus-enabled", "-eebus-state-root", "/data/eebus")
		if err != nil || cfg.EEBusConfig.OperatorSocketDir != "" || ebusgateway.DefaultConfig().EEBusConfig.OperatorSocketDir != "" {
			t.Fatalf("OperatorSocketDir = %q, %v; want the default (empty)", cfg.EEBusConfig.OperatorSocketDir, err)
		}
	})
	t.Run("a valid directory is cleaned and kept", func(t *testing.T) {
		for value, want := range map[string]string{
			" /run/helianthus/op ":       "/run/helianthus/op",
			"/run//helianthus/./op/":     "/run/helianthus/op",
			exactDir:                     exactDir,
			"/data/eebus-operator":       "/data/eebus-operator",
			"/var/lib/helianthus/op-dir": "/var/lib/helianthus/op-dir",
		} {
			cfg, err := m71ParseOperatorFlags("-eebus-operator-socket-dir", value)
			if err != nil || cfg.EEBusConfig.OperatorSocketDir != want {
				t.Errorf("-eebus-operator-socket-dir %q = %q, %v; want %q", value, cfg.EEBusConfig.OperatorSocketDir, err, want)
			}
		}
	})
	t.Run("the last occurrence wins and an empty value restores the default", func(t *testing.T) {
		cfg, err := m71ParseOperatorFlags("-eebus-operator-socket-dir", "/run/a", "-eebus-operator-socket-dir", "/run/b")
		if err != nil || cfg.EEBusConfig.OperatorSocketDir != "/run/b" {
			t.Fatalf("OperatorSocketDir = %q, %v; want /run/b", cfg.EEBusConfig.OperatorSocketDir, err)
		}
		for _, empty := range []string{"", "  "} {
			cfg, err = m71ParseOperatorFlags("-eebus-operator-socket-dir", "/run/a", "-eebus-operator-socket-dir", empty)
			if err != nil || cfg.EEBusConfig.OperatorSocketDir != "" {
				t.Fatalf("OperatorSocketDir after %q = %q, %v; want the default", empty, cfg.EEBusConfig.OperatorSocketDir, err)
			}
		}
	})
	t.Run("an unusable directory is rejected without changing the configuration", func(t *testing.T) {
		for name, test := range map[string]struct{ value, want string }{
			"one byte over the limit": {exactDir + "x", "AF_UNIX limit"},
			"far over the limit":      {"/" + strings.Repeat("d", 400), "AF_UNIX limit"},
			"relative":                {"run/op", "must be absolute"},
			"traversal":               {"/run/../op", "traversal"},
			"NUL":                     {"/run/op\x00", "NUL"},
			"filesystem root":         {"/", "filesystem root"},
		} {
			cfg, err := m71ParseOperatorFlags("-eebus-operator-socket-dir", "/run/keep", "-eebus-operator-socket-dir", test.value)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Errorf("%s: error = %v; want one containing %q", name, err, test.want)
			}
			if cfg.EEBusConfig.OperatorSocketDir != "/run/keep" {
				t.Errorf("%s: a rejected value changed the configuration to %q", name, cfg.EEBusConfig.OperatorSocketDir)
			}
		}
	})
	t.Run("the directory is independent of the eeBUS runtime configuration", func(t *testing.T) {
		// A disabled eeBUS configuration with an operator directory is still
		// inert: the operator endpoint does not depend on the sidecar.
		if _, err := mapEEBusRuntimeConfig(ebusgateway.EEBusConfig{OperatorSocketDir: "/run/helianthus/op"}, nil); err != nil {
			t.Fatalf("disabled eeBUS with an operator directory is rejected: %v", err)
		}
	})
}

// The fallback name is stable for the user and the default location.
func TestM71FallbackNameShape(t *testing.T) {
	original := eebusOperatorTempDir
	t.Cleanup(func() { eebusOperatorTempDir = original })
	eebusOperatorTempDir = func() string { return "/tmp" }
	got := eebusOperatorFallbackSocket("/data/eebus-operator/operator-mcp.sock")
	if want := fmt.Sprintf("/tmp/helianthus-eebus-op-%d-", os.Geteuid()); !strings.HasPrefix(got, want) || !m71FallbackName.MatchString(filepath.Base(filepath.Dir(got))) {
		t.Fatalf("fallback = %q; want %s<id>/operator-mcp.sock", got, want)
	}
}
