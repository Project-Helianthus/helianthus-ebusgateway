package runtimecontrol

// Tests for the S1/D3 desired-state service over the merged runtime_control
// store. Every fixture is synthetic: generic family driver IDs (fam.*),
// fixed incarnations, injected lifecycle executors and recovery adapters,
// and a temporary state file. Nothing here opens an endpoint.

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimestate"
)

const svcGUID = "00000000-0000-4000-8000-0000000000c1"

// --- bridge from the merged store to the Store seam (test-only wiring) ---

type managerStore struct{ m *runtimestate.Manager }

func toRS(t *Transition) *runtimestate.ControlTransition {
	if t == nil {
		return nil
	}
	return &runtimestate.ControlTransition{
		Kind: runtimestate.TransitionKind(t.Kind), DesiredRevision: uint64(t.DesiredRevision),
		Sequence: uint64(t.Sequence), Phase: runtimestate.TransitionPhase(t.Phase), Epoch: string(t.Epoch),
		Generation: uint64(t.Generation), NativeClosePending: t.NativeClosePending,
		SemanticCleanupPending: t.SemanticCleanupPending, AttemptsUsed: t.AttemptsUsed, DeadlineUTC: t.DeadlineUTC,
	}
}

func fromRS(t *runtimestate.ControlTransition) *Transition {
	if t == nil {
		return nil
	}
	return &Transition{
		Kind: TransitionKind(t.Kind), DesiredRevision: DesiredRevision(t.DesiredRevision),
		Sequence: LifecycleSequence(t.Sequence), Phase: TransitionPhase(t.Phase), Epoch: SourceEpoch(t.Epoch),
		Generation: DriverGeneration(t.Generation), NativeClosePending: t.NativeClosePending,
		SemanticCleanupPending: t.SemanticCleanupPending, AttemptsUsed: t.AttemptsUsed, DeadlineUTC: t.DeadlineUTC,
	}
}

func convertErr(err error) error {
	var r *runtimestate.ControlRefusal
	if errors.As(err, &r) {
		out := &Refusal{Code: RefusalCode(r.Code), Row: r.Row, Reason: r.Reason}
		if r.View != nil {
			out.View = &DesiredView{DesiredState: DesiredState(r.View.DesiredState), DesiredRevision: DesiredRevision(r.View.DesiredRevision)}
		}
		return out
	}
	return err
}

func fromRecord(v runtimestate.ControlDriverView) ControlRecord {
	return ControlRecord{
		DriverID: DriverID(v.DriverID), Enabled: v.Enabled, Failed: v.Failed, ReadOnly: v.ReadOnly,
		HasRecord: v.HasRecord, DesiredState: DesiredState(v.DesiredState),
		DesiredRevision: DesiredRevision(v.DesiredRevision), Current: fromRS(v.Current), Reserved: fromRS(v.Reserved),
	}
}

func (s *managerStore) Decide(ctx context.Context, method Method, req Request, planner Planner) (DesiredView, error) {
	view, err := s.m.DecideControl(ctx, runtimestate.ControlMethod(method),
		runtimestate.ControlRequest{DriverID: string(req.DriverID), ExpectedRevision: uint64(req.ExpectedRevision)},
		func(in runtimestate.ControlPlanInput) (runtimestate.ControlPlan, *runtimestate.ControlRefusal) {
			plan, refusal := planner(PlanInput{Method: Method(in.Method), DriverID: DriverID(in.DriverID),
				HasRecord: in.HasRecord, Current: fromRS(in.Record.Current), Reserved: fromRS(in.Record.Reserved)})
			if refusal != nil {
				return runtimestate.ControlPlan{}, &runtimestate.ControlRefusal{Code: runtimestate.RefusalCode(refusal.Code), Row: refusal.Row, Reason: refusal.Reason}
			}
			return runtimestate.ControlPlan{Current: toRS(plan.Current), Reserved: toRS(plan.Reserved)}, nil
		})
	if err != nil {
		return DesiredView{}, convertErr(err)
	}
	return DesiredView{DesiredState: DesiredState(view.DesiredState), DesiredRevision: DesiredRevision(view.DesiredRevision)}, nil
}

func (s *managerStore) CommitWork(ctx context.Context, id DriverID, current, reserved *Transition) error {
	return convertErr(s.m.CommitControlWork(ctx, string(id), toRS(current), toRS(reserved)))
}

func (s *managerStore) AdvanceCatalog(ctx context.Context) (CatalogRevision, error) {
	v, err := s.m.AdvanceControlCatalog(ctx)
	return CatalogRevision(v), convertErr(err)
}

func (s *managerStore) CommitRecovery(ctx context.Context, m *RecoveryMaterial) error {
	if m == nil {
		return convertErr(s.m.CommitControlRecovery(ctx, nil))
	}
	ids := make([]string, 0, len(m.DriverIDs))
	for _, id := range m.DriverIDs {
		ids = append(ids, string(id))
	}
	return convertErr(s.m.CommitControlRecovery(ctx, &runtimestate.ControlRecovery{DriverIDs: ids,
		MaxDesiredRevision: uint64(m.MaxDesiredRevision), Payload: append([]byte(nil), m.Payload...)}))
}

func (s *managerStore) Recovery() *RecoveryMaterial {
	r := s.m.ControlRecoveryPayload()
	if r == nil {
		return nil
	}
	out := &RecoveryMaterial{MaxDesiredRevision: DesiredRevision(r.MaxDesiredRevision), Payload: r.Payload}
	for _, id := range r.DriverIDs {
		out.DriverIDs = append(out.DriverIDs, DriverID(id))
	}
	return out
}

func (s *managerStore) Catalog() (ControlCatalog, error) {
	v, err := s.m.ControlSnapshot()
	if err != nil {
		return ControlCatalog{}, convertErr(err)
	}
	out := ControlCatalog{ReadOnly: v.ReadOnly, IssuedMark: DesiredRevision(v.IssuedRevisionMark), CatalogRevision: CatalogRevision(v.CatalogRevision)}
	for _, d := range v.Drivers {
		out.Drivers = append(out.Drivers, fromRecord(d))
	}
	return out, nil
}

func (s *managerStore) Driver(id DriverID) (ControlRecord, error) {
	v, err := s.m.ControlDriver(string(id))
	if err != nil {
		return ControlRecord{}, convertErr(err)
	}
	return fromRecord(v), nil
}

func (s *managerStore) AdmissionOpen() bool { return s.m.ControlAdmissionOpen() }

func (s *managerStore) ResolveReadback(ctx context.Context) error {
	return convertErr(s.m.ResolveControlReadback(ctx))
}

// --- filesystem hooks with an injectable block and failures ---

type blockHooks struct {
	runtimestate.DefaultFilesystemHooks
	mu         sync.Mutex
	blockNext  chan struct{}
	entered    chan struct{}
	failRename error
	// failDirSync makes the next N parent-directory fsyncs fail.
	failDirSync int
	writes      int
}

func (h *blockHooks) FsyncDir(path string) error {
	h.mu.Lock()
	fail := h.failDirSync > 0
	if fail {
		h.failDirSync--
	}
	h.mu.Unlock()
	if fail {
		return errors.New("synthetic directory fsync failure")
	}
	return h.DefaultFilesystemHooks.FsyncDir(path)
}

func (h *blockHooks) WriteFile(path string, data []byte, perm uint32) error {
	h.mu.Lock()
	block, entered := h.blockNext, h.entered
	h.blockNext, h.entered = nil, nil
	h.writes++
	h.mu.Unlock()
	if block != nil {
		close(entered)
		<-block
	}
	return h.DefaultFilesystemHooks.WriteFile(path, data, perm)
}

func (h *blockHooks) Rename(oldpath, newpath string) error {
	h.mu.Lock()
	fail := h.failRename
	h.mu.Unlock()
	if fail != nil {
		return fail
	}
	return h.DefaultFilesystemHooks.Rename(oldpath, newpath)
}

func (h *blockHooks) arm() (entered, release chan struct{}) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.blockNext, h.entered = make(chan struct{}), make(chan struct{})
	return h.entered, h.blockNext
}

// --- fakes ---

type orderLog struct {
	mu      sync.Mutex
	entries []string
}

func (l *orderLog) add(entry string) {
	if l == nil {
		return
	}
	l.mu.Lock()
	l.entries = append(l.entries, entry)
	l.mu.Unlock()
}

func (l *orderLog) list() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.entries...)
}

type execCall struct {
	Op   string
	Corr LifecycleCorrelation
}

type fakeExec struct {
	id    DriverID
	log   *orderLog
	mu    sync.Mutex
	calls []execCall
	start func(ctx context.Context, c LifecycleCorrelation) error
	stop  func(ctx context.Context, c LifecycleCorrelation) StopOutcome
}

func (f *fakeExec) Start(ctx context.Context, c LifecycleCorrelation) error {
	f.mu.Lock()
	f.calls = append(f.calls, execCall{"start", c})
	fn := f.start
	f.mu.Unlock()
	f.log.add("start:" + string(f.id))
	if fn != nil {
		return fn(ctx, c)
	}
	return nil
}

func (f *fakeExec) Stop(ctx context.Context, c LifecycleCorrelation) StopOutcome {
	f.mu.Lock()
	f.calls = append(f.calls, execCall{"stop", c})
	fn := f.stop
	f.mu.Unlock()
	f.log.add("stop:" + string(f.id))
	if fn != nil {
		return fn(ctx, c)
	}
	return StopOutcome{NativeClosed: true, SemanticCleaned: true}
}

func (f *fakeExec) setStart(fn func(context.Context, LifecycleCorrelation) error) {
	f.mu.Lock()
	f.start = fn
	f.mu.Unlock()
}

func (f *fakeExec) setStop(fn func(context.Context, LifecycleCorrelation) StopOutcome) {
	f.mu.Lock()
	f.stop = fn
	f.mu.Unlock()
}

func (f *fakeExec) snapshot() []execCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]execCall(nil), f.calls...)
}

func (f *fakeExec) count(op string) int {
	n := 0
	for _, c := range f.snapshot() {
		if c.Op == op {
			n++
		}
	}
	return n
}

type retryErr struct{ msg string }

func (e retryErr) Error() string   { return e.msg }
func (e retryErr) Retryable() bool { return true }

type fakeRecovery struct {
	log        *orderLog
	mu         sync.Mutex
	restoreErr error
	restored   []*RecoveryMaterial
	secure     func(id DriverID, t Transition) (*RecoveryMaterial, error)
	secured    []Transition
}

func (r *fakeRecovery) Restore(_ context.Context, m *RecoveryMaterial) error {
	r.log.add("restore")
	r.mu.Lock()
	defer r.mu.Unlock()
	r.restored = append(r.restored, cloneMaterial(m))
	return r.restoreErr
}

func (r *fakeRecovery) Secure(_ context.Context, id DriverID, t Transition) (*RecoveryMaterial, error) {
	r.log.add("secure:" + string(id))
	r.mu.Lock()
	fn := r.secure
	r.secured = append(r.secured, t)
	r.mu.Unlock()
	if fn != nil {
		return fn(id, t)
	}
	return nil, nil
}

func (r *fakeRecovery) securedFor(id DriverID) []Transition {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Transition(nil), r.secured...)
}

type errSink struct {
	mu   sync.Mutex
	errs []error
}

func (s *errSink) add(err error) {
	s.mu.Lock()
	s.errs = append(s.errs, err)
	s.mu.Unlock()
}

func (s *errSink) list() []error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]error(nil), s.errs...)
}

// --- fixture ---

type svcFixture struct {
	t     *testing.T
	path  string
	hooks *blockHooks
	m     *runtimestate.Manager
	ms    *managerStore
	store Store
	svc   *Service
	execs map[DriverID]*fakeExec
	rec   *fakeRecovery
	errs  *errSink
	log   *orderLog
}

type svcOpts struct {
	drivers     []runtimestate.ControlDriverConfig
	wrap        func(Store) Store
	opts        func(*Options)
	state       func(t *testing.T, path string)
	waitBound   time.Duration
	incarnation string
	noStart     bool
	recovery    *fakeRecovery
	setup       func(*svcFixture)
}

func writeBase(t *testing.T, path string) {
	t.Helper()
	body := fmt.Sprintf(`{"schema_version":1,"meta":{"instance_guid":%q,"written_at":"2001-02-03T04:05:06Z"}}`, svcGUID)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func openManager(t *testing.T, path string, hooks *blockHooks, waitBound time.Duration, drivers []runtimestate.ControlDriverConfig) *runtimestate.Manager {
	t.Helper()
	if waitBound == 0 {
		waitBound = 2 * time.Second
	}
	m := runtimestate.New(runtimestate.Options{Path: path, FsHooks: hooks, ControlWaitBound: waitBound, Logger: quietLogger()})
	if _, err := m.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := m.BootstrapControl(context.Background(), drivers); err != nil {
		t.Fatalf("BootstrapControl: %v", err)
	}
	return m
}

func newFixture(t *testing.T, fo svcOpts) *svcFixture {
	t.Helper()
	path := filepath.Join(t.TempDir(), "runtime_state.json")
	if fo.state != nil {
		fo.state(t, path)
	} else {
		writeBase(t, path)
	}
	hooks := &blockHooks{}
	m := openManager(t, path, hooks, fo.waitBound, fo.drivers)
	f := &svcFixture{t: t, path: path, hooks: hooks, m: m, ms: &managerStore{m: m}, execs: map[DriverID]*fakeExec{},
		errs: &errSink{}, log: &orderLog{}}
	f.store = f.ms
	if fo.wrap != nil {
		f.store = fo.wrap(f.ms)
	}
	f.rec = fo.recovery
	if f.rec == nil {
		f.rec = &fakeRecovery{}
	}
	f.rec.log = f.log
	var cfgs []DriverConfig
	for _, d := range fo.drivers {
		e := &fakeExec{id: DriverID(d.DriverID), log: f.log}
		f.execs[DriverID(d.DriverID)] = e
		cfgs = append(cfgs, DriverConfig{DriverID: DriverID(d.DriverID), Executor: e})
	}
	inc := fo.incarnation
	if inc == "" {
		inc = "inc1"
	}
	opts := Options{Store: f.store, Recovery: f.rec, Drivers: cfgs, Incarnation: inc,
		RetryDelay: func(int) time.Duration { return 2 * time.Millisecond }, OnError: f.errs.add}
	if fo.opts != nil {
		fo.opts(&opts)
	}
	svc, err := New(opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	f.svc = svc
	t.Cleanup(func() {
		f.svc.Close()
		_ = f.m.Stop(context.Background())
	})
	if fo.setup != nil {
		fo.setup(f)
	}
	if !fo.noStart {
		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
	}
	return f
}

func enabled(ids ...string) []runtimestate.ControlDriverConfig {
	out := make([]runtimestate.ControlDriverConfig, 0, len(ids))
	for _, id := range ids {
		out = append(out, runtimestate.ControlDriverConfig{DriverID: id, Enabled: true})
	}
	return out
}

func deadlineCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func (f *svcFixture) status(id DriverID) DriverStatus {
	f.t.Helper()
	st, err := f.svc.Get(id)
	if err != nil {
		f.t.Fatalf("Get(%s): %v", id, err)
	}
	return st
}

func (f *svcFixture) record(id DriverID) ControlRecord {
	f.t.Helper()
	rec, err := f.ms.Driver(id)
	if err != nil {
		f.t.Fatalf("Driver(%s): %v", id, err)
	}
	return rec
}

func (f *svcFixture) waitObserved(id DriverID, state drivermanager.ObservedState) {
	f.t.Helper()
	waitFor(f.t, fmt.Sprintf("%s observed %s", id, state), func() bool { return f.status(id).ObservedState == state })
}

func (f *svcFixture) waitIdle(id DriverID) {
	f.t.Helper()
	waitFor(f.t, fmt.Sprintf("%s slots empty", id), func() bool {
		rec := f.record(id)
		return rec.Current == nil && rec.Reserved == nil
	})
}

func (f *svcFixture) waitSettled(id DriverID, state drivermanager.ObservedState) {
	f.t.Helper()
	f.waitIdle(id)
	f.waitObserved(id, state)
}

func body(id DriverID, rev DesiredRevision) []byte {
	return []byte(fmt.Sprintf(`{"DriverID":%q,"ExpectedRevision":"%d"}`, id, rev))
}

func (f *svcFixture) decide(method Method, id DriverID) (DesiredView, error) {
	f.t.Helper()
	return f.svc.Control(deadlineCtx(f.t), method, body(id, f.status(id).DesiredRevision))
}

func (f *svcFixture) mustDecide(method Method, id DriverID) DesiredView {
	f.t.Helper()
	view, err := f.decide(method, id)
	if err != nil {
		f.t.Fatalf("%s %s: %v", method, id, err)
	}
	return view
}

// quiesce waits until no store write happened for a short interval, so an
// armed filesystem block catches the next deliberate write.
func (f *svcFixture) quiesce() {
	f.t.Helper()
	count := func() int {
		f.hooks.mu.Lock()
		defer f.hooks.mu.Unlock()
		return f.hooks.writes
	}
	waitFor(f.t, "store writes to quiesce", func() bool {
		before := count()
		time.Sleep(50 * time.Millisecond)
		return count() == before
	})
}

func (f *svcFixture) fileBytes() []byte {
	f.t.Helper()
	data, err := os.ReadFile(f.path)
	if err != nil {
		f.t.Fatal(err)
	}
	return data
}

// fileDriver decodes one driver record straight from the state file.
func (f *svcFixture) fileDriver(id DriverID) map[string]any {
	f.t.Helper()
	var doc struct {
		Control struct {
			Drivers map[string]map[string]any `json:"drivers"`
		} `json:"runtime_control"`
	}
	if err := json.Unmarshal(f.fileBytes(), &doc); err != nil {
		f.t.Fatal(err)
	}
	return doc.Control.Drivers[string(id)]
}

func (f *svcFixture) fileDesired(id DriverID) string {
	state, _ := f.fileDriver(id)["desired_state"].(string)
	return state
}

func (f *svcFixture) fileCurrentKind(id DriverID) string {
	current, _ := f.fileDriver(id)["current"].(map[string]any)
	kind, _ := current["kind"].(string)
	return kind
}

// refusalOf asserts the refusal code and row and returns the refusal as a
// value (the value type is not an error, so callers may ignore it).
func refusalOf(t *testing.T, err error, code RefusalCode, row int) Refusal {
	t.Helper()
	var r *Refusal
	if !errors.As(err, &r) {
		t.Fatalf("error = %v, want refusal %s", err, code)
	}
	if r.Code != code || r.Row != row {
		t.Fatalf("refusal = %s row %d (%s), want %s row %d", r.Code, r.Row, r.Reason, code, row)
	}
	return *r
}

// blockingStart returns a start function that blocks until release is
// closed or the attempt context ends, reporting entry on entered.
func blockingStart(entered chan<- LifecycleCorrelation, release <-chan struct{}, lateResult error) func(context.Context, LifecycleCorrelation) error {
	return func(ctx context.Context, c LifecycleCorrelation) error {
		entered <- c
		select {
		case <-release:
			return lateResult
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// --- configuration and request shape ---

// TestServiceConfigValidationRejectsWithoutEffects covers invalid service
// configuration: every refusal happens before any store access, executor or
// recovery call, and a driver set that differs from the store's visible set
// refuses Start without writing.
func TestServiceConfigValidationRejectsWithoutEffects(t *testing.T) {
	f := newFixture(t, svcOpts{drivers: enabled("fam.alpha", "fam.beta"), noStart: true})
	before := f.fileBytes()
	exec := &fakeExec{id: "fam.alpha"}
	var nilExec *fakeExec
	var nilStore *managerStore
	good := func() Options {
		return Options{Store: f.store, Recovery: f.rec, Incarnation: "inc1",
			Drivers: []DriverConfig{{DriverID: "fam.alpha", Executor: exec}}}
	}
	many := make([]DriverConfig, MaxServiceDrivers+1)
	for i := range many {
		many[i] = DriverConfig{DriverID: DriverID(fmt.Sprintf("fam.d%d", i)), Executor: exec}
	}
	cases := map[string]func(*Options){
		"nil store":           func(o *Options) { o.Store = nil },
		"typed nil store":     func(o *Options) { o.Store = nilStore },
		"nil recovery":        func(o *Options) { o.Recovery = nil },
		"empty incarnation":   func(o *Options) { o.Incarnation = "" },
		"spaced incarnation":  func(o *Options) { o.Incarnation = "inc 1" },
		"long incarnation":    func(o *Options) { o.Incarnation = strings.Repeat("a", maxIncarnationBytes+1) },
		"no drivers":          func(o *Options) { o.Drivers = nil },
		"too many drivers":    func(o *Options) { o.Drivers = many },
		"noncanonical id":     func(o *Options) { o.Drivers[0].DriverID = "Fam.Alpha" },
		"duplicate id":        func(o *Options) { o.Drivers = append(o.Drivers, o.Drivers[0]) },
		"nil executor":        func(o *Options) { o.Drivers[0].Executor = nil },
		"typed nil executor":  func(o *Options) { o.Drivers[0].Executor = nilExec },
		"attempts above 8":    func(o *Options) { o.MaxAttempts = DefaultMaxAttempts + 1 },
		"negative budget":     func(o *Options) { o.OperationBudget = -time.Second },
		"budget above 30s":    func(o *Options) { o.OperationBudget = DefaultOperationBudget + time.Nanosecond },
		"negative max":        func(o *Options) { o.MaxAttempts = -1 },
		"id above 128 bytes":  func(o *Options) { o.Drivers[0].DriverID = DriverID("a" + strings.Repeat("b", 128)) },
		"control identifiers": func(o *Options) { o.Drivers[0].DriverID = "fam_alpha" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			opts := good()
			mutate(&opts)
			if _, err := New(opts); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("New error = %v, want ErrInvalidConfig", err)
			}
		})
	}
	// A configured set that differs from the store's visible drivers.
	svc, err := New(good())
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Start(context.Background()); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Start error = %v, want ErrInvalidConfig", err)
	}
	svc.Close()
	if len(exec.snapshot()) != 0 || len(f.rec.securedFor("")) != 0 || len(f.log.list()) != 0 {
		t.Fatalf("configuration refusal invoked an executor or recovery adapter: %v", f.log.list())
	}
	if after := f.fileBytes(); string(after) != string(before) {
		t.Fatalf("configuration refusal changed durable state")
	}
	if _, err := svc.Decide(deadlineCtx(t), MethodStop, Request{DriverID: "fam.alpha", ExpectedRevision: 1}); err == nil {
		t.Fatalf("a closed service accepted a decision")
	}
}

// TestServiceParseRequestMatchesStoreDecoder checks the service's closed
// decoder (row 1) against the merged store's decoder on the same corpus.
func TestServiceParseRequestMatchesStoreDecoder(t *testing.T) {
	maxRev := strconv.FormatUint(math.MaxUint64, 10)
	overflow := maxRev[:len(maxRev)-1] + "6"
	corpus := []string{
		`{"DriverID":"fam.alpha","ExpectedRevision":"7"}`,
		` { "ExpectedRevision" : "7" , "DriverID" : "fam.alpha" } `,
		`{"DriverID":"fam.alpha","ExpectedRevision":"` + maxRev + `"}`,
		`{"DriverID":"fam.alpha","ExpectedRevision":"` + overflow + `"}`,
		`{"DriverID":"fam.alpha","ExpectedRevision":"0"}`,
		`{"DriverID":"fam.alpha","ExpectedRevision":"01"}`,
		`{"DriverID":"fam.alpha","ExpectedRevision":"+1"}`,
		`{"DriverID":"fam.alpha","ExpectedRevision":" 1"}`,
		`{"DriverID":"fam.alpha","ExpectedRevision":"1x"}`,
		`{"DriverID":"fam.alpha","ExpectedRevision":"-1"}`,
		`{"DriverID":"fam.alpha","ExpectedRevision":""}`,
		`{"DriverID":"fam.alpha","ExpectedRevision":1}`,
		`{"DriverID":"fam.alpha","ExpectedRevision":"1"}`,
		`{"DriverID":7,"ExpectedRevision":"1"}`,
		`{"DriverID":null,"ExpectedRevision":"1"}`,
		`{"DriverID":["fam.alpha"],"ExpectedRevision":"1"}`,
		`{"DriverID":{"x":"y"},"ExpectedRevision":"1"}`,
		`{"DriverID":"fam.alpha"}`,
		`{"ExpectedRevision":"1"}`,
		`{"DriverID":"fam.alpha","ExpectedRevision":"1","idempotency_key":"k"}`,
		`{"DriverID":"fam.alpha","ExpectedRevision":"1","request_id":"r"}`,
		`{"DriverID":"fam.alpha","DriverID":"fam.beta","ExpectedRevision":"1"}`,
		`{"DriverID":"fam.alpha","ExpectedRevision":"1","ExpectedRevision":"1"}`,
		`{"DriverID":"fam.alpha","ExpectedRevision":"1"}{}`,
		`{"DriverID":"fam.alpha","ExpectedRevision":"1"} x`,
		`{"DriverID":"fam.alpha","ExpectedRevision":"1",}`,
		`{"driverid":"fam.alpha","ExpectedRevision":"1"}`,
		`[]`, `null`, `""`, ``, `{}`, `{`,
	}
	for _, input := range corpus {
		want, wantErr := runtimestate.ParseControlRequest([]byte(input))
		got, err := ParseRequest([]byte(input))
		if (wantErr == nil) != (err == nil) {
			t.Fatalf("%q: service error %v, store error %v", input, err, wantErr)
		}
		if err != nil {
			refusalOf(t, err, RefusalInvalidArgument, 1)
			continue
		}
		if string(got.DriverID) != want.DriverID || uint64(got.ExpectedRevision) != want.ExpectedRevision {
			t.Fatalf("%q: service %+v, store %+v", input, got, want)
		}
	}
}

type stateProbe struct {
	file   string
	view   Catalog
	cursor uint64
	calls  int
}

func (f *svcFixture) probe() stateProbe {
	calls := 0
	for _, e := range f.execs {
		calls += len(e.snapshot())
	}
	return stateProbe{file: string(f.fileBytes()), view: f.svc.List(), cursor: f.svc.events.cursor(), calls: calls}
}

func (f *svcFixture) assertUnchanged(what string, before stateProbe) {
	f.t.Helper()
	after := f.probe()
	if after.file != before.file {
		f.t.Fatalf("%s changed durable state", what)
	}
	if !reflect.DeepEqual(after.view, before.view) {
		f.t.Fatalf("%s changed the view: %+v -> %+v", what, before.view, after.view)
	}
	if after.cursor != before.cursor {
		f.t.Fatalf("%s emitted an event", what)
	}
	if after.calls != before.calls {
		f.t.Fatalf("%s invoked an executor", what)
	}
}

// TestServiceRefusalsLeaveStateUnchanged drives one request per refusal the
// service can reach (invalid ID, malformed request, disabled, deadline,
// stale revision, RESTART in STOPPED) and requires no durable, view, event
// or executor change (CTL-02/03/05/14/15/18).
func TestServiceRefusalsLeaveStateUnchanged(t *testing.T) {
	drivers := []runtimestate.ControlDriverConfig{
		{DriverID: "fam.alpha", Enabled: true}, {DriverID: "fam.beta", Enabled: false}, {DriverID: "fam.gamma", Enabled: true},
	}
	f := newFixture(t, svcOpts{drivers: drivers})
	f.waitSettled("fam.alpha", drivermanager.ObservedRunning)
	f.waitSettled("fam.gamma", drivermanager.ObservedRunning)
	f.mustDecide(MethodStop, "fam.gamma")
	f.waitSettled("fam.gamma", drivermanager.ObservedStopped)
	if st := f.status("fam.beta"); st.ObservedState != drivermanager.ObservedDisabled || f.execs["fam.beta"].count("start") != 0 {
		t.Fatalf("disabled driver = %+v", st)
	}
	alpha := f.status("fam.alpha")
	expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	cases := []struct {
		name   string
		call   func() error
		code   RefusalCode
		row    int
		verify func(Refusal)
	}{
		{"malformed body", func() error {
			_, err := f.svc.Control(deadlineCtx(t), MethodStop, []byte(`{"DriverID":"fam.alpha","ExpectedRevision":"01"}`))
			return err
		}, RefusalInvalidArgument, 1, nil},
		{"extra member", func() error {
			_, err := f.svc.Control(deadlineCtx(t), MethodStop, []byte(`{"DriverID":"fam.alpha","ExpectedRevision":"1","idempotency_key":"k"}`))
			return err
		}, RefusalInvalidArgument, 1, nil},
		{"unknown method", func() error {
			_, err := f.svc.Control(deadlineCtx(t), Method("PAUSE"), body("fam.alpha", alpha.DesiredRevision))
			return err
		}, RefusalInvalidArgument, 1, nil},
		{"unregistered id", func() error {
			_, err := f.svc.Control(deadlineCtx(t), MethodStart, body("fam.delta", alpha.DesiredRevision))
			return err
		}, RefusalNotFound, 2, nil},
		{"noncanonical id", func() error {
			_, err := f.svc.Control(deadlineCtx(t), MethodStart, body("Fam.Alpha", alpha.DesiredRevision))
			return err
		}, RefusalNotFound, 2, nil},
		{"disabled", func() error {
			_, err := f.svc.Control(deadlineCtx(t), MethodStart, body("fam.beta", f.status("fam.beta").DesiredRevision))
			return err
		}, RefusalFailedPrecondition, 4, nil},
		{"no deadline", func() error {
			_, err := f.svc.Control(context.Background(), MethodStop, body("fam.alpha", alpha.DesiredRevision))
			return err
		}, RefusalInvalidArgument, 5, nil},
		{"expired deadline", func() error {
			_, err := f.svc.Control(expired, MethodStop, body("fam.alpha", alpha.DesiredRevision))
			return err
		}, RefusalDeadlineExceeded, 5, nil},
		{"stale changing", func() error {
			_, err := f.svc.Control(deadlineCtx(t), MethodStart, body("fam.gamma", f.status("fam.gamma").DesiredRevision-1))
			return err
		}, RefusalRevisionConflict, 6, func(r Refusal) {
			gamma := f.status("fam.gamma")
			if r.View == nil || *r.View != (DesiredView{DesiredStopped, gamma.DesiredRevision}) {
				t.Fatalf("conflict view = %+v", r.View)
			}
		}},
		{"stale same-desired", func() error {
			_, err := f.svc.Control(deadlineCtx(t), MethodStart, body("fam.alpha", alpha.DesiredRevision+1))
			return err
		}, RefusalRevisionConflict, 6, nil},
		{"restart while stopped", func() error {
			_, err := f.svc.Control(deadlineCtx(t), MethodRestart, body("fam.gamma", f.status("fam.gamma").DesiredRevision))
			return err
		}, RefusalFailedPrecondition, 6, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := f.probe()
			r := refusalOf(t, tc.call(), tc.code, tc.row)
			if tc.verify != nil {
				tc.verify(r)
			}
			f.assertUnchanged(tc.name, before)
		})
	}
}

// TestServiceCounterOverflowRejectsWithoutStateChange stores the mark at the
// unsigned 64-bit maximum: a changing START refuses COUNTER_EXHAUSTED with no
// durable, view or event change.
func TestServiceCounterOverflowRejectsWithoutStateChange(t *testing.T) {
	maxRev := strconv.FormatUint(math.MaxUint64, 10)
	f := newFixture(t, svcOpts{drivers: enabled("fam.alpha"), state: func(t *testing.T, path string) {
		body := fmt.Sprintf(`{"schema_version":1,"meta":{"instance_guid":%q,"written_at":"2001-02-03T04:05:06Z"},`+
			`"runtime_control":{"schema_version":1,"issued_revision_mark":%q,"catalog_revision":"5","drivers":{`+
			`"fam.alpha":{"desired_state":"STOPPED","desired_revision":%q,"lifecycle_sequence":"0","current":null,"reserved":null}}}}`,
			svcGUID, maxRev, maxRev)
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}})
	f.waitObserved("fam.alpha", drivermanager.ObservedStopped)
	before := f.probe()
	_, err := f.svc.Control(deadlineCtx(t), MethodStart, body("fam.alpha", math.MaxUint64))
	refusalOf(t, err, RefusalCounterExhausted, 10)
	f.assertUnchanged("overflow", before)
	// A same-desired STOP still answers as a no-op without any counter.
	view, err := f.svc.Control(deadlineCtx(t), MethodStop, body("fam.alpha", math.MaxUint64))
	if err != nil || view != (DesiredView{DesiredStopped, math.MaxUint64}) {
		t.Fatalf("no-op at the maximum = %+v, %v", view, err)
	}
	f.assertUnchanged("no-op at the maximum", before)
}

// --- same-desired no-ops (CTL-06, CTL-11) ---

// TestServiceSameDesiredNoOpsKeepRetryAndQuarantine: START while desired
// RUNNING in BACKOFF (untyped and typed) and FAILED, and STOP while desired
// STOPPED with an unconfirmed close, return desired/revision only and change
// nothing: no revision, mark, slot, budget, observation, event or wake.
func TestServiceSameDesiredNoOpsKeepRetryAndQuarantine(t *testing.T) {
	f := newFixture(t, svcOpts{
		drivers: enabled("fam.backoff", "fam.failed", "fam.quar", "fam.typed"),
		opts:    func(o *Options) { o.RetryDelay = func(int) time.Duration { return time.Hour } },
		setup: func(f *svcFixture) {
			f.execs["fam.backoff"].setStart(func(context.Context, LifecycleCorrelation) error { return retryErr{"busy"} })
		},
	})
	f.waitSettled("fam.typed", drivermanager.ObservedRunning)
	f.waitSettled("fam.failed", drivermanager.ObservedRunning)
	f.waitSettled("fam.quar", drivermanager.ObservedRunning)
	// Typed BACKOFF: STOP, then a START that keeps failing retryably.
	f.mustDecide(MethodStop, "fam.typed")
	f.waitSettled("fam.typed", drivermanager.ObservedStopped)
	f.execs["fam.typed"].setStart(func(context.Context, LifecycleCorrelation) error { return retryErr{"busy"} })
	f.mustDecide(MethodStart, "fam.typed")
	f.waitObserved("fam.typed", drivermanager.ObservedBackoff)
	// FAILED: a terminal start failure after STOP/START.
	f.mustDecide(MethodStop, "fam.failed")
	f.waitSettled("fam.failed", drivermanager.ObservedStopped)
	f.execs["fam.failed"].setStart(func(context.Context, LifecycleCorrelation) error { return errors.New("terminal") })
	f.mustDecide(MethodStart, "fam.failed")
	f.waitSettled("fam.failed", drivermanager.ObservedFailed)
	// Quarantine: STOP whose native close cannot be proven.
	f.execs["fam.quar"].setStop(func(context.Context, LifecycleCorrelation) StopOutcome { return StopOutcome{SemanticCleaned: true} })
	f.mustDecide(MethodStop, "fam.quar")
	waitFor(t, "quarantine", func() bool { return f.status("fam.quar").SafetyQuarantined })
	// Untyped BACKOFF: the seeded driver's process-start attempt fails.
	f.waitObserved("fam.backoff", drivermanager.ObservedBackoff)
	if rec := f.record("fam.backoff"); rec.Current != nil {
		t.Fatalf("untyped attempt persisted typed work: %+v", rec.Current)
	}

	typedSlot := f.record("fam.typed").Current
	if typedSlot == nil || typedSlot.AttemptsUsed != 1 || typedSlot.Kind != KindStart {
		t.Fatalf("typed backoff slot = %+v", typedSlot)
	}
	before := f.probe()
	catalogBefore, _ := f.ms.Catalog()
	for _, tc := range []struct {
		id     DriverID
		method Method
		state  DesiredState
	}{
		{"fam.backoff", MethodStart, DesiredRunning},
		{"fam.typed", MethodStart, DesiredRunning},
		{"fam.failed", MethodStart, DesiredRunning},
		{"fam.quar", MethodStop, DesiredStopped},
	} {
		rev := f.status(tc.id).DesiredRevision
		var wg sync.WaitGroup
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				view, err := f.svc.Control(deadlineCtx(t), tc.method, body(tc.id, rev))
				if err != nil || view != (DesiredView{tc.state, rev}) {
					t.Errorf("%s %s no-op = %+v, %v", tc.method, tc.id, view, err)
				}
			}()
		}
		wg.Wait()
	}
	f.assertUnchanged("same-desired no-ops", before)
	catalogAfter, _ := f.ms.Catalog()
	if catalogAfter.IssuedMark != catalogBefore.IssuedMark || catalogAfter.CatalogRevision != catalogBefore.CatalogRevision {
		t.Fatalf("no-op moved the mark or catalog: %+v -> %+v", catalogBefore, catalogAfter)
	}
	if slot := f.record("fam.typed").Current; !reflect.DeepEqual(slot, typedSlot) {
		t.Fatalf("no-op changed the retry budget: %+v -> %+v", typedSlot, slot)
	}
	// Quarantine blocks admission, never cleared by a request.
	_, err := f.decide(MethodStart, "fam.quar")
	refusalOf(t, err, RefusalFailedPrecondition, 7)
	_, err = f.decide(MethodRestart, "fam.quar")
	refusalOf(t, err, RefusalFailedPrecondition, 6)
	if st := f.status("fam.quar"); !st.SafetyQuarantined || st.Reason != drivermanager.ReasonCloseUnconfirmed {
		t.Fatalf("quarantine cleared: %+v", st)
	}
	if slot := f.record("fam.quar").Current; slot == nil || slot.Kind != KindStop || !slot.NativeClosePending {
		t.Fatalf("unconfirmed close did not retain its cleanup obligation: %+v", slot)
	}
}

// --- acceptance durability and ordering ---

type gateStore struct {
	Store
	mu      sync.Mutex
	gate    bool
	reached chan struct{}
	release chan struct{}
	ops     *orderLog
}

func (g *gateStore) Decide(ctx context.Context, m Method, r Request, p Planner) (DesiredView, error) {
	view, err := g.Store.Decide(ctx, m, r, p)
	g.mu.Lock()
	gate, reached, release := g.gate, g.reached, g.release
	g.gate = false
	g.mu.Unlock()
	if gate {
		close(reached)
		<-release
	}
	return view, err
}

func (g *gateStore) CommitWork(ctx context.Context, id DriverID, cur, res *Transition) error {
	err := g.Store.CommitWork(ctx, id, cur, res)
	if err == nil && cur == nil && res == nil {
		g.ops.add("prune:" + string(id))
	}
	return err
}

func (g *gateStore) CommitRecovery(ctx context.Context, m *RecoveryMaterial) error {
	err := g.Store.CommitRecovery(ctx, m)
	if err == nil {
		g.ops.add("recovery")
	}
	return err
}

func (g *gateStore) arm() (reached, release chan struct{}) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.gate, g.reached, g.release = true, make(chan struct{}), make(chan struct{})
	return g.reached, g.release
}

// TestServiceAcceptanceIsDurableBeforePublishWakeAndEvent holds the service
// right after the store's durable commit: the decision and its typed slot are
// already on disk while the view, the event log and the reconciler are still
// untouched. Releasing it publishes, emits and wakes, and the response is the
// committed desired state and revision, incremented exactly once.
func TestServiceAcceptanceIsDurableBeforePublishWakeAndEvent(t *testing.T) {
	gate := &gateStore{ops: &orderLog{}}
	f := newFixture(t, svcOpts{drivers: enabled("fam.alpha"), wrap: func(s Store) Store { gate.Store = s; return gate }})
	f.waitSettled("fam.alpha", drivermanager.ObservedRunning)
	f.mustDecide(MethodStop, "fam.alpha")
	f.waitSettled("fam.alpha", drivermanager.ObservedStopped)
	before := f.status("fam.alpha")
	markBefore, _ := f.ms.Catalog()
	starts := f.execs["fam.alpha"].count("start")
	cursor := f.svc.events.cursor()

	reached, release := gate.arm()
	type result struct {
		view DesiredView
		err  error
	}
	done := make(chan result, 1)
	go func() {
		view, err := f.svc.Control(deadlineCtx(t), MethodStart, body("fam.alpha", before.DesiredRevision))
		done <- result{view, err}
	}()
	<-reached
	rec := f.record("fam.alpha")
	if rec.DesiredState != DesiredRunning || rec.DesiredRevision != markBefore.IssuedMark+1 ||
		rec.Current == nil || rec.Current.Kind != KindStart || rec.Current.Phase != PhasePending {
		t.Fatalf("decision not durable before notification: %+v", rec)
	}
	if got := f.fileDesired("fam.alpha"); got != "RUNNING" {
		t.Fatalf("committed decision missing from the state file: %q", got)
	}
	time.Sleep(20 * time.Millisecond)
	if st := f.status("fam.alpha"); st != before {
		t.Fatalf("view exposed before the service resumed: %+v", st)
	}
	if f.svc.events.cursor() != cursor || f.execs["fam.alpha"].count("start") != starts {
		t.Fatalf("event or wake before the service resumed")
	}
	close(release)
	res := <-done
	if res.err != nil || res.view != (DesiredView{DesiredRunning, markBefore.IssuedMark + 1}) {
		t.Fatalf("accepted response = %+v, %v", res.view, res.err)
	}
	page := f.svc.EventsSince(cursor)
	if len(page.Events) == 0 || page.Events[0].Kind != EventDesiredAccepted || page.Events[0].DesiredRevision != res.view.DesiredRevision {
		t.Fatalf("first event after acceptance = %+v", page.Events)
	}
	f.waitSettled("fam.alpha", drivermanager.ObservedRunning)
	if got := f.execs["fam.alpha"].count("start"); got != starts+1 {
		t.Fatalf("accepted START executed %d times, want 1", got-starts)
	}
}

// TestServiceObservedTransitionsNeverBumpDesiredRevision: every observed
// transition advances only the catalog revision; desired revision and the
// mark keep the accepted value.
func TestServiceObservedTransitionsNeverBumpDesiredRevision(t *testing.T) {
	f := newFixture(t, svcOpts{drivers: enabled("fam.alpha"),
		opts: func(o *Options) {}})
	f.waitSettled("fam.alpha", drivermanager.ObservedRunning)
	stop := f.mustDecide(MethodStop, "fam.alpha")
	f.waitSettled("fam.alpha", drivermanager.ObservedStopped)
	failures := 2
	var mu sync.Mutex
	f.execs["fam.alpha"].setStart(func(context.Context, LifecycleCorrelation) error {
		mu.Lock()
		defer mu.Unlock()
		if failures > 0 {
			failures--
			return retryErr{"warming"}
		}
		return nil
	})
	cursor := f.svc.events.cursor()
	accepted := f.mustDecide(MethodStart, "fam.alpha")
	if accepted.DesiredRevision != stop.DesiredRevision+1 {
		t.Fatalf("acceptance incremented the revision by %d", accepted.DesiredRevision-stop.DesiredRevision)
	}
	catalogAtAcceptance, _ := f.ms.Catalog()
	f.waitSettled("fam.alpha", drivermanager.ObservedRunning)
	catalog, _ := f.ms.Catalog()
	if catalog.IssuedMark != accepted.DesiredRevision || f.record("fam.alpha").DesiredRevision != accepted.DesiredRevision {
		t.Fatalf("observed transitions moved desired revision or mark: %+v", catalog)
	}
	observed := 0
	for _, ev := range f.svc.EventsSince(cursor).Events {
		if ev.Kind == EventObservedChanged {
			observed++
			if ev.DesiredRevision != accepted.DesiredRevision {
				t.Fatalf("observed event carries desired revision %d", ev.DesiredRevision)
			}
		}
	}
	if observed < 4 || catalog.CatalogRevision < catalogAtAcceptance.CatalogRevision+CatalogRevision(observed) {
		t.Fatalf("observed=%d catalog %d -> %d", observed, catalogAtAcceptance.CatalogRevision, catalog.CatalogRevision)
	}
	if got := f.status("fam.alpha"); got.Revision != f.svc.List().Revision || got.DesiredRevision != accepted.DesiredRevision {
		t.Fatalf("view = %+v", got)
	}
}

// TestServiceListSnapshotsAreCoherentUnderRaces runs decisions on four
// drivers while readers list concurrently: every list is sorted, carries one
// revision on every entry, revisions never go backwards for a reader, and a
// revision always shows the same content to every reader (CTL-01).
func TestServiceListSnapshotsAreCoherentUnderRaces(t *testing.T) {
	ids := []DriverID{"fam.d", "fam.b", "fam.c", "fam.a"}
	f := newFixture(t, svcOpts{drivers: enabled("fam.d", "fam.b", "fam.c", "fam.a")})
	for _, id := range ids {
		f.waitSettled(id, drivermanager.ObservedRunning)
	}
	stop := make(chan struct{})
	var seen sync.Map
	var readers, writers sync.WaitGroup
	for r := 0; r < 4; r++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			var last CatalogRevision
			for {
				select {
				case <-stop:
					return
				default:
				}
				view := f.svc.List()
				if view.Revision < last {
					t.Errorf("revision went backwards: %d -> %d", last, view.Revision)
					return
				}
				last = view.Revision
				if !sort.SliceIsSorted(view.Drivers, func(i, j int) bool { return view.Drivers[i].DriverID < view.Drivers[j].DriverID }) || len(view.Drivers) != 4 {
					t.Errorf("unsorted or incomplete list: %+v", view.Drivers)
					return
				}
				for _, entry := range view.Drivers {
					if entry.Revision != view.Revision {
						t.Errorf("entry revision %d in view %d", entry.Revision, view.Revision)
						return
					}
				}
				if prior, loaded := seen.LoadOrStore(view.Revision, view.Drivers); loaded && !reflect.DeepEqual(prior, view.Drivers) {
					t.Errorf("revision %d shows two contents", view.Revision)
					return
				}
			}
		}()
	}
	for _, id := range ids {
		writers.Add(1)
		go func(id DriverID) {
			defer writers.Done()
			for i := 0; i < 6; i++ {
				method := MethodStop
				if f.status(id).DesiredState == DesiredStopped {
					method = MethodStart
				}
				if _, err := f.svc.Control(deadlineCtx(t), method, body(id, f.status(id).DesiredRevision)); err != nil {
					var r *Refusal
					if !errors.As(err, &r) || (r.Code != RefusalTransitionBusy && r.Code != RefusalRevisionConflict) {
						t.Errorf("%s %s: %v", method, id, err)
						return
					}
				}
				time.Sleep(time.Millisecond)
			}
		}(id)
	}
	writers.Wait()
	close(stop)
	readers.Wait()
	for _, id := range ids {
		f.waitIdle(id)
	}
}

// --- concurrency, capacity and restart ordering ---

func raceDecisions(t *testing.T, f *svcFixture, id DriverID, methods []Method, rev DesiredRevision) ([]DesiredView, []error) {
	t.Helper()
	views := make([]DesiredView, len(methods))
	errs := make([]error, len(methods))
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i, m := range methods {
		wg.Add(1)
		go func(i int, m Method) {
			defer wg.Done()
			<-start
			views[i], errs[i] = f.svc.Control(deadlineCtx(t), m, body(id, rev))
		}(i, m)
	}
	close(start)
	wg.Wait()
	return views, errs
}

func countAccepted(t *testing.T, views []DesiredView, errs []error, rev DesiredRevision) int {
	t.Helper()
	accepted := 0
	for i, err := range errs {
		if err == nil {
			if views[i].DesiredRevision == rev {
				t.Fatalf("a changing request returned the old revision")
			}
			accepted++
			continue
		}
		refusalOf(t, err, RefusalRevisionConflict, 6)
	}
	return accepted
}

// TestServiceConcurrentRequestsObeyConflictAndCapacity: simultaneous
// changing requests carrying the same expected revision accept exactly one
// (the rest conflict); simultaneous same-desired requests are all no-ops;
// RESTART races accept one; STOP in flight makes RESTART FAILED_PRECONDITION,
// START takes the reserved slot and any further changing request is
// TRANSITION_BUSY (CTL-07, CTL-11, CTL-19 at the service).
func TestServiceConcurrentRequestsObeyConflictAndCapacity(t *testing.T) {
	f := newFixture(t, svcOpts{drivers: enabled("fam.alpha")})
	id := DriverID("fam.alpha")
	f.waitSettled(id, drivermanager.ObservedRunning)

	rev := f.status(id).DesiredRevision
	views, errs := raceDecisions(t, f, id, []Method{MethodRestart, MethodRestart, MethodRestart}, rev)
	if n := countAccepted(t, views, errs, rev); n != 1 {
		t.Fatalf("RESTART race accepted %d", n)
	}
	f.waitSettled(id, drivermanager.ObservedRunning)

	rev = f.status(id).DesiredRevision
	views, errs = raceDecisions(t, f, id, []Method{MethodStop, MethodRestart, MethodStop}, rev)
	if n := countAccepted(t, views, errs, rev); n != 1 {
		t.Fatalf("STOP/RESTART race accepted %d", n)
	}
	f.waitIdle(id)
	if f.status(id).DesiredState == DesiredRunning {
		f.mustDecide(MethodStop, id)
	}
	f.waitSettled(id, drivermanager.ObservedStopped)

	rev = f.status(id).DesiredRevision
	views, errs = raceDecisions(t, f, id, []Method{MethodStop, MethodStop, MethodStop, MethodStop}, rev)
	for i, err := range errs {
		if err != nil || views[i] != (DesiredView{DesiredStopped, rev}) {
			t.Fatalf("same-desired race: %+v %v", views[i], err)
		}
	}

	// Capacity during STOPPING.
	f.mustDecide(MethodStart, id)
	f.waitSettled(id, drivermanager.ObservedRunning)
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	f.execs[id].setStop(func(context.Context, LifecycleCorrelation) StopOutcome {
		once.Do(func() { close(entered) })
		<-release
		return StopOutcome{NativeClosed: true, SemanticCleaned: true}
	})
	f.mustDecide(MethodStop, id)
	<-entered
	f.waitObserved(id, drivermanager.ObservedStopping)
	_, err := f.decide(MethodRestart, id)
	refusalOf(t, err, RefusalFailedPrecondition, 6)
	if v, err := f.decide(MethodStop, id); err != nil || v.DesiredState != DesiredStopped {
		t.Fatalf("STOP during STOPPING = %+v, %v", v, err)
	}
	startView := f.mustDecide(MethodStart, id)
	rec := f.record(id)
	if rec.Current == nil || rec.Current.Kind != KindStop || rec.Reserved == nil || rec.Reserved.Kind != KindStart ||
		rec.Reserved.DesiredRevision != startView.DesiredRevision {
		t.Fatalf("START during STOPPING did not take the reserved slot: %+v", rec)
	}
	for _, m := range []Method{MethodRestart, MethodStop} {
		_, err := f.decide(m, id)
		refusalOf(t, err, RefusalTransitionBusy, 8)
	}
	if v, err := f.decide(MethodStart, id); err != nil || v != startView {
		t.Fatalf("same-desired START with both slots occupied = %+v, %v", v, err)
	}
	close(release)
	f.waitSettled(id, drivermanager.ObservedRunning)
}

type fencingCase struct {
	name    string
	untyped bool
	backoff bool
	method  Method
}

// TestServiceStopOrRestartDuringStartingOrBackoffFencesAndKeepsCleanup is
// CTL-08: STARTING and BACKOFF, each with a typed START and with an untyped
// process-start attempt. STOP or RESTART takes the reserved slot (the untyped
// case synthesizes the cancellation as the current slot with the attempt's
// correlation and obligations), fences the old attempt, keeps its cleanup
// obligations until a Stop with the old correlation settles them, never
// publishes a late result, refuses a further changing request as
// TRANSITION_BUSY, and loses no accepted RESTART.
func TestServiceStopOrRestartDuringStartingOrBackoffFencesAndKeepsCleanup(t *testing.T) {
	for _, tc := range []fencingCase{
		{"typed starting stop", false, false, MethodStop},
		{"typed starting restart", false, false, MethodRestart},
		{"typed backoff stop", false, true, MethodStop},
		{"typed backoff restart", false, true, MethodRestart},
		{"untyped starting stop", true, false, MethodStop},
		{"untyped starting restart", true, false, MethodRestart},
		{"untyped backoff stop", true, true, MethodStop},
		{"untyped backoff restart", true, true, MethodRestart},
	} {
		t.Run(tc.name, func(t *testing.T) { runFencingCase(t, tc) })
	}
}

func runFencingCase(t *testing.T, tc fencingCase) {
	id := DriverID("fam.alpha")
	entered := make(chan LifecycleCorrelation, 16)
	var attemptCtx atomic.Value
	failing := func(ctx context.Context, c LifecycleCorrelation) error {
		entered <- c
		return retryErr{"warming"}
	}
	blocking := func(ctx context.Context, c LifecycleCorrelation) error {
		attemptCtx.Store(ctx)
		entered <- c
		<-ctx.Done()
		return nil // a success reported only after the attempt was fenced
	}
	pick := blocking
	if tc.backoff {
		pick = failing
	}
	f := newFixture(t, svcOpts{
		drivers: enabled(string(id)),
		opts:    func(o *Options) { o.RetryDelay = func(int) time.Duration { return time.Hour } },
		setup: func(f *svcFixture) {
			if tc.untyped {
				f.execs[id].setStart(pick)
			}
		},
	})
	stopRelease := make(chan struct{})
	if !tc.untyped {
		f.waitSettled(id, drivermanager.ObservedRunning)
		f.mustDecide(MethodStop, id)
		f.waitSettled(id, drivermanager.ObservedStopped)
		f.execs[id].setStart(pick)
		f.mustDecide(MethodStart, id)
	}
	old := <-entered
	if tc.backoff {
		f.waitObserved(id, drivermanager.ObservedBackoff)
	} else {
		f.waitObserved(id, drivermanager.ObservedStarting)
	}
	before := f.record(id)
	// Hold the cleanup Stop so the slot layout can be inspected.
	stopEntered := make(chan LifecycleCorrelation, 4)
	f.execs[id].setStop(func(_ context.Context, c LifecycleCorrelation) StopOutcome {
		stopEntered <- c
		<-stopRelease
		return StopOutcome{NativeClosed: true, SemanticCleaned: true}
	})
	f.execs[id].setStart(nil)
	cursor := f.svc.events.cursor()
	view := f.mustDecide(tc.method, id)

	rec := f.record(id)
	if rec.Reserved == nil || rec.Reserved.Kind != TransitionKind(tc.method) || rec.Reserved.DesiredRevision != view.DesiredRevision {
		t.Fatalf("%s did not take the reserved slot: %+v", tc.method, rec)
	}
	if tc.untyped {
		cur := rec.Current
		if before.Current != nil || cur == nil || cur.Kind != KindCancel || cur.Epoch != old.Epoch ||
			cur.Generation != old.Generation || !cur.NativeClosePending || !cur.SemanticCleanupPending || cur.DeadlineUTC.IsZero() {
			t.Fatalf("untyped cancellation not synthesized with the attempt's correlation: %+v (attempt %+v)", cur, old)
		}
	} else {
		cur := rec.Current
		if cur == nil || !sameIdentity(cur, before.Current) || cur.Epoch != old.Epoch || !cur.NativeClosePending || !cur.SemanticCleanupPending {
			t.Fatalf("typed START slot or its obligations changed: %+v -> %+v", before.Current, cur)
		}
	}
	// Any further changing request while both slots are occupied is busy.
	other := MethodStart
	if view.DesiredState == DesiredRunning {
		other = MethodStop
	}
	_, err := f.decide(other, id)
	refusalOf(t, err, RefusalTransitionBusy, 8)

	cleanup := <-stopEntered
	if cleanup.Epoch != old.Epoch || cleanup.Generation != old.Generation {
		t.Fatalf("cleanup used correlation %+v, want the fenced attempt's %+v", cleanup, old)
	}
	if !tc.backoff {
		if ctx, _ := attemptCtx.Load().(context.Context); ctx == nil || ctx.Err() == nil {
			t.Fatalf("fenced attempt context not canceled")
		}
	}
	close(stopRelease)
	want := drivermanager.ObservedStopped
	if tc.method == MethodRestart {
		want = drivermanager.ObservedRunning
	}
	f.waitSettled(id, want)
	for _, ev := range f.svc.EventsSince(cursor).Events {
		if ev.Kind == EventObservedChanged && ev.ObservedState == drivermanager.ObservedRunning && tc.method == MethodStop {
			t.Fatalf("fenced attempt published RUNNING after acceptance")
		}
	}
	if tc.method == MethodRestart {
		calls := f.execs[id].snapshot()
		last := calls[len(calls)-1]
		if last.Op != "start" || last.Corr.Epoch == old.Epoch || last.Corr.Generation != 1 {
			t.Fatalf("accepted RESTART did not start a new epoch: %+v", last)
		}
	}
}

// TestServiceEveryAcceptedRestartExecutesInOrder: each accepted RESTART in
// RUNNING takes a new revision and performs exactly one stop-then-start with
// a new epoch, in acceptance order; a RESTART while one executes is
// TRANSITION_BUSY; an old-revision retry conflicts and a fresh-revision retry
// after settlement is another restart (CTL-07, CTL-10).
func TestServiceEveryAcceptedRestartExecutesInOrder(t *testing.T) {
	id := DriverID("fam.alpha")
	f := newFixture(t, svcOpts{drivers: enabled(string(id))})
	f.waitSettled(id, drivermanager.ObservedRunning)
	var epochs []SourceEpoch
	epochs = append(epochs, f.execs[id].snapshot()[0].Corr.Epoch)
	for i := 0; i < 3; i++ {
		prior := f.status(id).DesiredRevision
		calls := len(f.execs[id].snapshot())
		view := f.mustDecide(MethodRestart, id)
		if view.DesiredState != DesiredRunning || view.DesiredRevision <= prior {
			t.Fatalf("RESTART view = %+v", view)
		}
		f.waitSettled(id, drivermanager.ObservedRunning)
		got := f.execs[id].snapshot()[calls:]
		if len(got) != 2 || got[0].Op != "stop" || got[0].Corr.Epoch != epochs[len(epochs)-1] || got[1].Op != "start" {
			t.Fatalf("RESTART %d executed %+v", i, got)
		}
		for _, e := range epochs {
			if got[1].Corr.Epoch == e {
				t.Fatalf("RESTART reused epoch %s", e)
			}
		}
		epochs = append(epochs, got[1].Corr.Epoch)
		_, err := f.svc.Control(deadlineCtx(t), MethodRestart, body(id, prior))
		refusalOf(t, err, RefusalRevisionConflict, 6)
	}
	// A RESTART while another RESTART executes is busy, never coalesced.
	entered := make(chan LifecycleCorrelation, 1)
	release := make(chan struct{})
	f.execs[id].setStart(blockingStart(entered, release, nil))
	f.mustDecide(MethodRestart, id)
	<-entered
	_, err := f.decide(MethodRestart, id)
	refusalOf(t, err, RefusalTransitionBusy, 8)
	close(release)
	f.waitSettled(id, drivermanager.ObservedRunning)
}

// TestServiceResultsAreNotReceipts: a decision result encodes exactly
// desired_state and desired_revision, and no result, view, event or
// diagnostic type carries an operation handle, receipt, key, principal,
// authority or endpoint field.
func TestServiceResultsAreNotReceipts(t *testing.T) {
	raw, err := json.Marshal(DesiredView{DesiredState: DesiredRunning, DesiredRevision: 42})
	if err != nil {
		t.Fatal(err)
	}
	var members map[string]any
	if err := json.Unmarshal(raw, &members); err != nil {
		t.Fatal(err)
	}
	if len(members) != 2 || members["desired_state"] != "RUNNING" || members["desired_revision"] != "42" {
		t.Fatalf("result = %s", raw)
	}
	forbidden := []string{"receipt", "idempoten", "principal", "authoriz", "authority", "permission", "actor",
		"token", "credential", "operation", "sequence", "endpoint", "address", "host", "url", "payload", "error"}
	visited := map[reflect.Type]bool{}
	var walk func(path string, typ reflect.Type)
	walk = func(path string, typ reflect.Type) {
		for typ.Kind() == reflect.Pointer || typ.Kind() == reflect.Slice || typ.Kind() == reflect.Map {
			typ = typ.Elem()
		}
		if visited[typ] || typ.Kind() != reflect.Struct {
			return
		}
		visited[typ] = true
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			name := strings.ToLower(field.Name)
			for _, word := range forbidden {
				if strings.Contains(name, word) {
					t.Fatalf("%s.%s is a forbidden field", path, field.Name)
				}
			}
			walk(path+"."+field.Name, field.Type)
		}
	}
	// The event shape is pinned: categorical state plus the lifecycle
	// correlation (epoch, generation) of S6.1, and no operation handle.
	var eventFields []string
	for i := 0; i < reflect.TypeOf(Event{}).NumField(); i++ {
		eventFields = append(eventFields, reflect.TypeOf(Event{}).Field(i).Name)
	}
	wantFields := []string{"Cursor", "Kind", "DriverID", "CatalogRevision", "DesiredState", "DesiredRevision",
		"ObservedState", "Reason", "SafetyQuarantined", "Epoch", "Generation"}
	if !reflect.DeepEqual(eventFields, wantFields) {
		t.Fatalf("event fields = %v, want %v", eventFields, wantFields)
	}
	if reflect.TypeOf(Event{}.Epoch) != reflect.TypeOf(SourceEpoch("")) || reflect.TypeOf(Event{}.Generation) != reflect.TypeOf(DriverGeneration(0)) {
		t.Fatalf("event correlation fields use the wrong identity types")
	}
	for _, typ := range []reflect.Type{
		reflect.TypeOf(DesiredView{}), reflect.TypeOf(Catalog{}), reflect.TypeOf(DriverStatus{}),
		reflect.TypeOf(Event{}), reflect.TypeOf(EventPage{}), reflect.TypeOf(Diagnostic{}), reflect.TypeOf(Request{}),
	} {
		walk(typ.Name(), typ)
	}
}

// TestServiceCancellationAfterAcceptanceKeepsWork: cancelling the caller's
// context after acceptance neither retracts the decision nor cancels the
// persisted work, which runs under the service's own lifetime and bounded
// budget; an exhausted budget ends the work FAILED with desired intact.
func TestServiceCancellationAfterAcceptanceKeepsWork(t *testing.T) {
	id := DriverID("fam.alpha")
	f := newFixture(t, svcOpts{drivers: enabled(string(id), "fam.beta"),
		opts: func(o *Options) { o.OperationBudget = 300 * time.Millisecond }})
	f.waitSettled(id, drivermanager.ObservedRunning)
	f.mustDecide(MethodStop, id)
	f.waitSettled(id, drivermanager.ObservedStopped)
	entered := make(chan LifecycleCorrelation, 1)
	release := make(chan struct{})
	var startCtx atomic.Value
	f.execs[id].setStart(func(ctx context.Context, c LifecycleCorrelation) error {
		startCtx.Store(ctx)
		entered <- c
		<-release
		return nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	view, err := f.svc.Control(ctx, MethodStart, body(id, f.status(id).DesiredRevision))
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	<-entered
	if c := startCtx.Load().(context.Context); c.Err() != nil {
		t.Fatalf("caller cancellation reached the persisted work")
	}
	close(release)
	f.waitSettled(id, drivermanager.ObservedRunning)
	if rec := f.record(id); rec.DesiredRevision != view.DesiredRevision || rec.DesiredState != DesiredRunning {
		t.Fatalf("cancellation retracted intent: %+v", rec)
	}

	// Bounded budget: a START that keeps failing retryably ends FAILED.
	beta := DriverID("fam.beta")
	f.waitSettled(beta, drivermanager.ObservedRunning)
	f.mustDecide(MethodStop, beta)
	f.waitSettled(beta, drivermanager.ObservedStopped)
	f.execs[beta].setStart(func(context.Context, LifecycleCorrelation) error { return retryErr{"never"} })
	startsBefore := f.execs[beta].count("start")
	accepted := f.mustDecide(MethodStart, beta)
	f.waitSettled(beta, drivermanager.ObservedFailed)
	if st := f.status(beta); st.Reason != drivermanager.ReasonRetryExhausted || st.DesiredState != DesiredRunning || st.DesiredRevision != accepted.DesiredRevision {
		t.Fatalf("exhausted START = %+v", st)
	}
	starts := f.execs[beta].count("start") - startsBefore
	if starts == 0 || starts > DefaultMaxAttempts {
		t.Fatalf("attempts = %d, want within the bound", starts)
	}
}

// --- isolation between drivers ---

// Documented responsiveness bound: list and get read an atomically published
// view and never wait for a store write, a reconciler or a decision, so each
// read returns within readBound even while another driver blocks. A decision
// for another driver waits at most the store's bounded serialization wait.
const readBound = 50 * time.Millisecond

func assertReadsResponsive(t *testing.T, f *svcFixture, ids ...DriverID) {
	t.Helper()
	for i := 0; i < 50; i++ {
		start := time.Now()
		f.svc.List()
		for _, id := range ids {
			if _, err := f.svc.Get(id); err != nil {
				t.Fatal(err)
			}
		}
		if elapsed := time.Since(start); elapsed > readBound {
			t.Fatalf("list/get took %v, above the documented %v bound", elapsed, readBound)
		}
	}
}

// TestServiceTwoDriversStayResponsiveWhileOneBlocks: a reconciliation that
// blocks in driver A's executor, a terminal failure in A, and a store write
// for A blocked inside the filesystem each leave list/get responsive within
// the documented bound; B keeps deciding and reconciling (its decision waits
// only the bounded serialization wait while A's write holds the store).
func TestServiceTwoDriversStayResponsiveWhileOneBlocks(t *testing.T) {
	a, b := DriverID("fam.a"), DriverID("fam.b")
	cycleB := func(t *testing.T, f *svcFixture) {
		start := time.Now()
		f.mustDecide(MethodStop, b)
		f.waitSettled(b, drivermanager.ObservedStopped)
		f.mustDecide(MethodStart, b)
		f.waitSettled(b, drivermanager.ObservedRunning)
		if elapsed := time.Since(start); elapsed > 3*time.Second {
			t.Fatalf("driver B took %v while A was blocked", elapsed)
		}
	}
	t.Run("blocked reconciliation", func(t *testing.T) {
		f := newFixture(t, svcOpts{drivers: enabled(string(a), string(b))})
		f.waitSettled(a, drivermanager.ObservedRunning)
		f.waitSettled(b, drivermanager.ObservedRunning)
		f.mustDecide(MethodStop, a)
		f.waitSettled(a, drivermanager.ObservedStopped)
		entered := make(chan LifecycleCorrelation, 1)
		f.execs[a].setStart(blockingStart(entered, make(chan struct{}), nil))
		f.mustDecide(MethodStart, a)
		<-entered
		assertReadsResponsive(t, f, a, b)
		cycleB(t, f)
		assertReadsResponsive(t, f, a, b)
		if st := f.status(a); st.ObservedState != drivermanager.ObservedStarting {
			t.Fatalf("blocked driver = %+v", st)
		}
	})
	t.Run("failure", func(t *testing.T) {
		f := newFixture(t, svcOpts{drivers: enabled(string(a), string(b)), setup: func(f *svcFixture) {
			f.execs[a].setStart(func(context.Context, LifecycleCorrelation) error { return errors.New("terminal") })
		}})
		f.waitObserved(a, drivermanager.ObservedFailed)
		f.waitSettled(b, drivermanager.ObservedRunning)
		cycleB(t, f)
		assertReadsResponsive(t, f, a, b)
	})
	t.Run("blocked persistence", func(t *testing.T) {
		const waitBound = 300 * time.Millisecond
		f := newFixture(t, svcOpts{drivers: enabled(string(a), string(b)), waitBound: waitBound})
		f.waitSettled(a, drivermanager.ObservedRunning)
		f.waitSettled(b, drivermanager.ObservedRunning)
		f.quiesce()
		entered, release := f.hooks.arm()
		done := make(chan error, 1)
		go func() {
			_, err := f.svc.Control(deadlineCtx(t), MethodStop, body(a, f.status(a).DesiredRevision))
			done <- err
		}()
		<-entered
		assertReadsResponsive(t, f, a, b)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		start := time.Now()
		_, err := f.svc.Control(ctx, MethodStop, body(b, f.status(b).DesiredRevision))
		elapsed := time.Since(start)
		refusalOf(t, err, RefusalTransitionBusy, 8)
		if elapsed > waitBound+250*time.Millisecond {
			t.Fatalf("B's decision waited %v, above the bounded wait %v", elapsed, waitBound)
		}
		assertReadsResponsive(t, f, a, b)
		close(release)
		if err := <-done; err != nil {
			t.Fatalf("A's blocked STOP: %v", err)
		}
		f.waitSettled(a, drivermanager.ObservedStopped)
		cycleB(t, f)
	})
}

// --- recovery from in-flight state only ---

func transitionJSON(kind TransitionKind, rev, seq uint64, phase TransitionPhase, epoch string, gen uint64, native, semantic bool, attempts int, deadline string) string {
	return fmt.Sprintf(`{"kind":%q,"desired_revision":"%d","sequence":"%d","phase":%q,"epoch":%q,"generation":"%d",`+
		`"native_close_pending":%t,"semantic_cleanup_pending":%t,"attempts_used":%d,"deadline_utc":%q}`,
		kind, rev, seq, phase, epoch, gen, native, semantic, attempts, deadline)
}

func recordJSON(state DesiredState, rev, lifecycle uint64, current string) string {
	if current == "" {
		current = "null"
	}
	return fmt.Sprintf(`{"desired_state":%q,"desired_revision":"%d","lifecycle_sequence":"%d","current":%s,"reserved":null}`,
		state, rev, lifecycle, current)
}

func recoveryJSON(ids []string, maxRev uint64, payload []byte) string {
	sum := sha256.Sum256(payload)
	quoted, _ := json.Marshal(ids)
	return fmt.Sprintf(`{"format":%q,"driver_ids":%s,"max_desired_revision":"%d","payload_sha256":%q,"payload":%q}`,
		runtimestate.ControlRecoveryFormat, quoted, maxRev, hex.EncodeToString(sum[:]), base64.StdEncoding.EncodeToString(payload))
}

func controlState(mark, catalog uint64, drivers map[string]string, recovery string) func(*testing.T, string) {
	return func(t *testing.T, path string) {
		t.Helper()
		ids := make([]string, 0, len(drivers))
		for id := range drivers {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		var parts []string
		for _, id := range ids {
			parts = append(parts, fmt.Sprintf("%q:%s", id, drivers[id]))
		}
		rec := ""
		if recovery != "" {
			rec = `,"recovery":` + recovery
		}
		body := fmt.Sprintf(`{"schema_version":1,"meta":{"instance_guid":%q,"written_at":"2001-02-03T04:05:06Z"},`+
			`"runtime_control":{"schema_version":1,"issued_revision_mark":"%d","catalog_revision":"%d","drivers":{%s}%s}}`,
			svcGUID, mark, catalog, strings.Join(parts, ","), rec)
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

// TestServiceRecoveryUsesOnlyInFlightState restarts over committed in-flight
// work: D5 material is restored before any executor call; a PENDING START
// starts under a new epoch; an ACTIVE RESTART left by an earlier incarnation
// settles its historical correlation and performs its one replacement; a
// terminal STOP is only pruned; desired RUNNING without work gets the untyped
// process-start attempt and desired STOPPED without work stays untouched.
// Only the pruned slots are secured, and no completed-operation history
// exists to consult.
func TestServiceRecoveryUsesOnlyInFlightState(t *testing.T) {
	far := "2099-01-01T00:00:00Z"
	payload := []byte("synthetic-d5-material")
	drivers := map[string]string{
		"fam.a": recordJSON(DesiredRunning, 1, 1, transitionJSON(KindStart, 1, 1, PhasePending, "", 0, false, false, 0, "")),
		"fam.b": recordJSON(DesiredRunning, 2, 4, transitionJSON(KindRestart, 2, 4, PhaseActive, "old.fam.b.1", 3, true, true, 1, far)),
		"fam.c": recordJSON(DesiredStopped, 3, 2, transitionJSON(KindStop, 3, 2, PhaseCleanup, "old.fam.c.1", 1, false, false, 0, far)),
		"fam.d": recordJSON(DesiredRunning, 4, 0, ""),
		"fam.e": recordJSON(DesiredStopped, 5, 0, ""),
	}
	f := newFixture(t, svcOpts{
		drivers:     enabled("fam.a", "fam.b", "fam.c", "fam.d", "fam.e"),
		state:       controlState(5, 9, drivers, recoveryJSON([]string{"fam.b"}, 2, payload)),
		incarnation: "inc2",
	})
	for id, want := range map[DriverID]drivermanager.ObservedState{
		"fam.a": drivermanager.ObservedRunning, "fam.b": drivermanager.ObservedRunning, "fam.c": drivermanager.ObservedStopped,
		"fam.d": drivermanager.ObservedRunning, "fam.e": drivermanager.ObservedStopped,
	} {
		f.waitSettled(id, want)
	}
	entries := f.log.list()
	if len(entries) == 0 || entries[0] != "restore" {
		t.Fatalf("D5 restore did not precede executor work: %v", entries)
	}
	if len(f.rec.restored) != 1 || string(f.rec.restored[0].Payload) != string(payload) {
		t.Fatalf("restored material = %+v", f.rec.restored)
	}
	want := map[DriverID][]execCall{
		"fam.a": {{"start", LifecycleCorrelation{DriverID: "fam.a", Epoch: "inc2.fam.a.1", Generation: 1, Sequence: 1}}},
		"fam.b": {
			{"stop", LifecycleCorrelation{DriverID: "fam.b", Epoch: "old.fam.b.1", Generation: 3, Sequence: 4}},
			{"start", LifecycleCorrelation{DriverID: "fam.b", Epoch: "inc2.fam.b.1", Generation: 1, Sequence: 4}},
		},
		"fam.c": nil,
		"fam.d": {{"start", LifecycleCorrelation{DriverID: "fam.d", Epoch: "inc2.fam.d.1", Generation: 1}}},
		"fam.e": nil,
	}
	for id, calls := range want {
		if got := f.execs[id].snapshot(); !reflect.DeepEqual(got, calls) {
			t.Fatalf("%s executor calls = %+v, want %+v", id, got, calls)
		}
	}
	securedIDs := map[DriverID]int{}
	for _, entry := range f.log.list() {
		if id, ok := strings.CutPrefix(entry, "secure:"); ok {
			securedIDs[DriverID(id)]++
		}
	}
	if !reflect.DeepEqual(securedIDs, map[DriverID]int{"fam.a": 1, "fam.b": 1, "fam.c": 1}) {
		t.Fatalf("secured slots = %v", securedIDs)
	}
	for _, tr := range f.rec.securedFor("") {
		if !terminal(&tr) {
			t.Fatalf("secured a non-terminal slot: %+v", tr)
		}
	}
	if m := f.ms.Recovery(); m == nil || string(m.Payload) != string(payload) {
		t.Fatalf("committed D5 material changed: %+v", m)
	}
	for _, id := range f.svc.order {
		if len(f.svc.Diagnostics(id)) > 1 {
			t.Fatalf("diagnostics carried history across the restart")
		}
	}
}

// TestServiceRestartRecoveredAfterProcessLoss is CTL-09: process memory is
// lost while an accepted RESTART is ACTIVE; the new incarnation settles the
// historical correlation and performs the retained replacement under a new
// epoch before dropping the slot, although desired already says RUNNING.
func TestServiceRestartRecoveredAfterProcessLoss(t *testing.T) {
	id := DriverID("fam.alpha")
	f := newFixture(t, svcOpts{drivers: enabled(string(id))})
	f.waitSettled(id, drivermanager.ObservedRunning)
	entered := make(chan LifecycleCorrelation, 1)
	f.execs[id].setStart(blockingStart(entered, make(chan struct{}), nil))
	view := f.mustDecide(MethodRestart, id)
	active := <-entered
	held := f.record(id).Current
	if held == nil || held.Kind != KindRestart || held.Phase != PhaseActive || held.Epoch != active.Epoch {
		t.Fatalf("RESTART slot = %+v", held)
	}
	f.svc.Close()
	if after := f.record(id).Current; !reflect.DeepEqual(after, held) {
		t.Fatalf("closing the service rewrote in-flight work: %+v -> %+v", held, after)
	}
	if err := f.m.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}

	m2 := openManager(t, f.path, &blockHooks{}, 0, enabled(string(id)))
	exec2 := &fakeExec{id: id}
	svc2, err := New(Options{Store: &managerStore{m: m2}, Recovery: &fakeRecovery{}, Incarnation: "inc2",
		Drivers: []DriverConfig{{DriverID: id, Executor: exec2}}, RetryDelay: func(int) time.Duration { return 2 * time.Millisecond }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		svc2.Close()
		_ = m2.Stop(context.Background())
	})
	if err := svc2.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "replacement after process loss", func() bool {
		st, _ := svc2.Get(id)
		rec, _ := m2.ControlDriver(string(id))
		return st.ObservedState == drivermanager.ObservedRunning && rec.Current == nil
	})
	calls := exec2.snapshot()
	if len(calls) != 2 || calls[0].Op != "stop" || calls[0].Corr.Epoch != active.Epoch || calls[0].Corr.Generation != active.Generation ||
		calls[1].Op != "start" || !strings.HasPrefix(string(calls[1].Corr.Epoch), "inc2.") || calls[1].Corr.Generation != 1 {
		t.Fatalf("recovered RESTART executed %+v", calls)
	}
	if st, _ := svc2.Get(id); st.DesiredRevision != view.DesiredRevision {
		t.Fatalf("recovery changed desired revision: %+v", st)
	}
}

// --- pruning, D5 material and diagnostics ---

// TestServicePrunesOnlyAfterRecoveryMaterialSecured: a terminal slot stays
// durable while the recovery adapter cannot secure its material; once it
// can, the material is committed before the slot is pruned. Diagnostics stay
// a bounded sanitized in-memory ring.
func TestServicePrunesOnlyAfterRecoveryMaterialSecured(t *testing.T) {
	id := DriverID("fam.alpha")
	sentinel := errors.New("synthetic secure rejection")
	var allow atomic.Bool
	rec := &fakeRecovery{secure: func(_ DriverID, tr Transition) (*RecoveryMaterial, error) {
		if !allow.Load() {
			return nil, fmt.Errorf("wrapped: %w", sentinel)
		}
		return &RecoveryMaterial{DriverIDs: []DriverID{id}, MaxDesiredRevision: tr.DesiredRevision,
			Payload: []byte(fmt.Sprintf("d5-%d", tr.Sequence))}, nil
	}}
	gate := &gateStore{ops: &orderLog{}}
	f := newFixture(t, svcOpts{drivers: enabled(string(id)), recovery: rec,
		wrap: func(s Store) Store { gate.Store = s; return gate }})
	f.waitObserved(id, drivermanager.ObservedRunning)
	f.mustDecide(MethodStop, id)
	waitFor(t, "a secure failure", func() bool {
		for _, err := range f.errs.list() {
			var se *SemanticError
			if errors.As(err, &se) && se.Op == "secure" && errors.Is(err, sentinel) && se.DriverID == id {
				return true
			}
		}
		return false
	})
	slot := f.record(id).Current
	if slot == nil || !terminal(slot) || slot.Kind != KindStop {
		t.Fatalf("terminal slot pruned before its material was secured: %+v", slot)
	}
	if got := f.fileCurrentKind(id); got != "STOP" {
		t.Fatalf("terminal slot missing from durable state while unsecured: %q", got)
	}
	allow.Store(true)
	f.waitSettled(id, drivermanager.ObservedStopped)
	if m := f.ms.Recovery(); m == nil || string(m.Payload) != fmt.Sprintf("d5-%d", slot.Sequence) {
		t.Fatalf("secured material = %+v", m)
	}
	ops := gate.ops.list()
	if len(ops) < 2 || ops[len(ops)-2] != "recovery" || ops[len(ops)-1] != "prune:"+string(id) {
		t.Fatalf("material not committed before pruning: %v", ops)
	}

	// Bounded sanitized diagnostics.
	f.execs[id].setStart(func(context.Context, LifecycleCorrelation) error {
		return errors.New("refused by endpoint 192.0.2.7:4444 with secret=abc")
	})
	f.mustDecide(MethodStart, id)
	f.waitSettled(id, drivermanager.ObservedFailed)
	f.execs[id].setStart(nil)
	for i := 0; i < 17; i++ {
		if f.status(id).DesiredState == DesiredStopped {
			f.mustDecide(MethodStart, id)
		} else {
			f.mustDecide(MethodRestart, id)
		}
		f.waitSettled(id, drivermanager.ObservedRunning)
		f.mustDecide(MethodStop, id)
		f.waitSettled(id, drivermanager.ObservedStopped)
	}
	diags := f.svc.Diagnostics(id)
	if len(diags) != MaxDiagnostics {
		t.Fatalf("diagnostics = %d, want the %d bound", len(diags), MaxDiagnostics)
	}
	for _, d := range diags {
		if len(d.Summary) > MaxDiagnosticBytes || strings.Contains(d.Summary, "192.0.2") || strings.Contains(d.Summary, "secret") {
			t.Fatalf("unsanitized diagnostic %q", d.Summary)
		}
	}
	fresh, err := New(Options{Store: f.store, Recovery: rec, Incarnation: "inc3",
		Drivers: []DriverConfig{{DriverID: id, Executor: f.execs[id]}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(fresh.Diagnostics(id)) != 0 {
		t.Fatalf("diagnostics survived a new service instance")
	}
}

// --- events ---

// TestServiceEventsAreCommittedBoundedAndResyncable: refusals and a
// pre-commit write failure emit nothing; accepted decisions and observed
// transitions emit events that reference a published catalog revision; the
// ring is bounded and a reader behind the window is told to resync.
func TestServiceEventsAreCommittedBoundedAndResyncable(t *testing.T) {
	id := DriverID("fam.alpha")
	f := newFixture(t, svcOpts{drivers: enabled(string(id)), opts: func(o *Options) { o.EventCapacity = 8 }})
	f.waitSettled(id, drivermanager.ObservedRunning)
	cursor := f.svc.events.cursor()
	view := f.svc.List()
	running := f.execs[id].snapshot()[0].Corr

	f.hooks.mu.Lock()
	f.hooks.failRename = errors.New("synthetic rename refusal")
	f.hooks.mu.Unlock()
	_, err := f.decide(MethodStop, id)
	refusalOf(t, err, RefusalUnavailable, 0)
	f.hooks.mu.Lock()
	f.hooks.failRename = nil
	f.hooks.mu.Unlock()
	if f.svc.events.cursor() != cursor || !reflect.DeepEqual(f.svc.List(), view) {
		t.Fatalf("pre-commit failure emitted an event or changed the view")
	}

	notify := f.svc.EventNotify()
	accepted := f.mustDecide(MethodStop, id)
	select {
	case <-notify:
	case <-time.After(5 * time.Second):
		t.Fatalf("no event notification")
	}
	f.waitSettled(id, drivermanager.ObservedStopped)
	page := f.svc.EventsSince(cursor)
	if page.Resync || len(page.Events) < 2 || page.Events[0].Kind != EventDesiredAccepted ||
		page.Events[0].DesiredRevision != accepted.DesiredRevision || page.Events[0].DesiredState != DesiredStopped {
		t.Fatalf("events after acceptance = %+v", page)
	}
	last := page.Events[len(page.Events)-1]
	if last.Kind != EventObservedChanged || last.ObservedState != drivermanager.ObservedStopped {
		t.Fatalf("last event = %+v", last)
	}
	// Every event of this STOP names the withdrawn generation's correlation.
	for _, ev := range page.Events {
		if ev.Epoch != running.Epoch || ev.Generation != running.Generation {
			t.Fatalf("event correlation = %s/%d, want %s/%d", ev.Epoch, ev.Generation, running.Epoch, running.Generation)
		}
	}
	// A later START carries the new attempt's epoch and generation 1.
	startCursor := f.svc.events.cursor()
	f.mustDecide(MethodStart, id)
	f.waitSettled(id, drivermanager.ObservedRunning)
	calls := f.execs[id].snapshot()
	started := calls[len(calls)-1].Corr
	sawRunning := false
	for _, ev := range f.svc.EventsSince(startCursor).Events {
		if ev.Kind == EventObservedChanged && ev.ObservedState == drivermanager.ObservedRunning {
			sawRunning = true
			if ev.Epoch != started.Epoch || ev.Generation != 1 || ev.Epoch == running.Epoch {
				t.Fatalf("RUNNING event correlation = %s/%d, want new epoch %s/1", ev.Epoch, ev.Generation, started.Epoch)
			}
		}
	}
	if !sawRunning {
		t.Fatalf("no RUNNING event after START")
	}
	f.mustDecide(MethodStop, id)
	f.waitSettled(id, drivermanager.ObservedStopped)
	for i, ev := range page.Events {
		if ev.Cursor != cursor+uint64(i)+1 || ev.CatalogRevision == 0 || ev.CatalogRevision > f.svc.List().Revision {
			t.Fatalf("event %d = %+v", i, ev)
		}
	}
	if got := f.svc.List(); got.EventCursor != f.svc.events.cursor() {
		t.Fatalf("view cursor %d, log cursor %d", got.EventCursor, f.svc.events.cursor())
	}
	for i := 0; i < 3; i++ {
		f.mustDecide(MethodStart, id)
		f.waitSettled(id, drivermanager.ObservedRunning)
		f.mustDecide(MethodStop, id)
		f.waitSettled(id, drivermanager.ObservedStopped)
	}
	newest := f.svc.events.cursor()
	if p := f.svc.EventsSince(cursor); !p.Resync || len(p.Events) != 0 || p.Next != newest {
		t.Fatalf("reader behind the window = %+v", p)
	}
	if p := f.svc.EventsSince(newest - 3); p.Resync || len(p.Events) != 3 {
		t.Fatalf("reader inside the window = %+v", p)
	}
	if p := f.svc.EventsSince(newest); p.Resync || len(p.Events) != 0 {
		t.Fatalf("current reader = %+v", p)
	}
	if p := f.svc.EventsSince(newest + 5); !p.Resync {
		t.Fatalf("future cursor not resynced")
	}
}

// --- error identity ---

// TestServiceErrorIdentitySeparation: recovery-adapter (semantic registry)
// failures keep their original error identity inside SemanticError and never
// become lifecycle refusals; executor failures are LifecycleError; request
// refusals are Refusal. An unresolved restore withholds the driver FAILED and
// refuses admission (row 7) while an unaffected driver runs.
func TestServiceErrorIdentitySeparation(t *testing.T) {
	sentinel := errors.New("synthetic semantic rejection")
	payload := []byte("synthetic-d5")
	drivers := map[string]string{
		"fam.alpha": recordJSON(DesiredRunning, 1, 0, ""),
		"fam.beta":  recordJSON(DesiredRunning, 2, 0, ""),
	}
	f := newFixture(t, svcOpts{drivers: enabled("fam.alpha", "fam.beta"), noStart: true,
		state:    controlState(2, 3, drivers, recoveryJSON([]string{"fam.alpha"}, 1, payload)),
		recovery: &fakeRecovery{restoreErr: fmt.Errorf("restore: %w", sentinel)}})
	err := f.svc.Start(context.Background())
	var se *SemanticError
	var refusal *Refusal
	var le *LifecycleError
	if !errors.Is(err, sentinel) || !errors.As(err, &se) || se.DriverID != "fam.alpha" || se.Op != "restore" {
		t.Fatalf("restore error = %v", err)
	}
	if errors.As(err, &refusal) || errors.As(err, &le) {
		t.Fatalf("semantic error converted into a lifecycle error: %v", err)
	}
	f.waitSettled("fam.beta", drivermanager.ObservedRunning)
	if st := f.status("fam.alpha"); st.ObservedState != drivermanager.ObservedFailed || st.Reason != drivermanager.ReasonDependencyUnavailable {
		t.Fatalf("unrestored driver = %+v", st)
	}
	if f.execs["fam.alpha"].count("start") != 0 {
		t.Fatalf("unrestored driver was started")
	}
	_, err = f.decide(MethodRestart, "fam.alpha")
	refusalOf(t, err, RefusalFailedPrecondition, 7)
	if errors.As(err, &se) {
		t.Fatalf("refusal carries a semantic error")
	}

	execSentinel := errors.New("synthetic executor failure")
	f.mustDecide(MethodStop, "fam.beta")
	f.waitSettled("fam.beta", drivermanager.ObservedStopped)
	f.execs["fam.beta"].setStart(func(context.Context, LifecycleCorrelation) error { return fmt.Errorf("start: %w", execSentinel) })
	f.mustDecide(MethodStart, "fam.beta")
	f.waitSettled("fam.beta", drivermanager.ObservedFailed)
	found := false
	for _, e := range f.errs.list() {
		if errors.Is(e, execSentinel) {
			found = true
			if !errors.As(e, &le) || le.DriverID != "fam.beta" || le.Kind != KindStart || errors.As(e, &se) {
				t.Fatalf("executor error identity = %#v", e)
			}
		}
	}
	if !found {
		t.Fatalf("executor failure not reported")
	}
}

// --- planner, plan validation and work-update guard units ---

// TestServicePlanValidationRequiresRequestedTransition closes the planner
// residual on the service side: an accepted changing request must enqueue
// exactly one new transition of the requested kind and keep stored work.
func TestServicePlanValidationRequiresRequestedTransition(t *testing.T) {
	stored := &Transition{Kind: KindStart, DesiredRevision: 4, Sequence: 2, Phase: PhaseActive, Epoch: "inc1.fam.a.1", Generation: 1}
	newT := func(kind TransitionKind) *Transition { return &Transition{Kind: kind, Phase: PhasePending} }
	cancel := &Transition{Kind: KindCancel, Phase: PhasePending, Epoch: "inc1.fam.a.2", Generation: 1, NativeClosePending: true}
	modified := *stored
	modified.Generation = 9
	cases := []struct {
		name string
		in   PlanInput
		plan Plan
		ok   bool
	}{
		{"current request", PlanInput{Method: MethodStop}, Plan{Current: newT(KindStop)}, true},
		{"empty plan", PlanInput{Method: MethodStop}, Plan{}, false},
		{"wrong kind", PlanInput{Method: MethodStop}, Plan{Current: newT(KindStart)}, false},
		{"two requested", PlanInput{Method: MethodStop}, Plan{Current: newT(KindStop), Reserved: newT(KindStop)}, false},
		{"reserved only", PlanInput{Method: MethodStop}, Plan{Reserved: newT(KindStop)}, false},
		{"not pending", PlanInput{Method: MethodStop}, Plan{Current: &Transition{Kind: KindStop, Phase: PhaseActive}}, false},
		{"preassigned revision", PlanInput{Method: MethodStop}, Plan{Current: &Transition{Kind: KindStop, Phase: PhasePending, DesiredRevision: 3}}, false},
		{"retained only", PlanInput{Method: MethodStop, Current: stored}, Plan{Current: stored}, false},
		{"behind stored", PlanInput{Method: MethodStop, Current: stored}, Plan{Current: stored, Reserved: newT(KindStop)}, true},
		{"stored dropped", PlanInput{Method: MethodStop, Current: stored}, Plan{Current: newT(KindStop)}, false},
		{"stored modified", PlanInput{Method: MethodStop, Current: stored}, Plan{Current: &modified, Reserved: newT(KindStop)}, false},
		{"cancel synthesized", PlanInput{Method: MethodRestart}, Plan{Current: cancel, Reserved: newT(KindRestart)}, true},
		{"cancel alone", PlanInput{Method: MethodRestart}, Plan{Current: cancel}, false},
		{"cancel reserved", PlanInput{Method: MethodRestart}, Plan{Current: newT(KindRestart), Reserved: cancel}, false},
		{"cancel behind stored", PlanInput{Method: MethodStop, Current: stored}, Plan{Current: stored, Reserved: cancel}, false},
	}
	for _, tc := range cases {
		if err := validatePlan(tc.in, tc.plan); (err == nil) != tc.ok {
			t.Fatalf("%s: validatePlan error = %v, want ok=%v", tc.name, err, tc.ok)
		}
	}
}

// TestServicePlannerSlotRules covers the planner's placement and refusals
// without the store: pruning and occupied slots are TRANSITION_BUSY,
// quarantine and unresolved recovery refuse admission at row 7 but allow STOP.
func TestServicePlannerSlotRules(t *testing.T) {
	svc, err := New(Options{Store: &managerStore{}, Recovery: &fakeRecovery{}, Incarnation: "inc1",
		Drivers: []DriverConfig{{DriverID: "fam.a", Executor: &fakeExec{}}}})
	if err != nil {
		t.Fatal(err)
	}
	d := svc.drivers["fam.a"]
	plan := func(in PlanInput) (Plan, *Refusal, bool, bool) {
		var planned, fence bool
		p, r := svc.planner(d, &planned, &fence)(in)
		if planned {
			d.deciding--
		}
		return p, r, planned, fence
	}
	start := &Transition{Kind: KindStart, DesiredRevision: 2, Sequence: 1, Phase: PhaseActive, Epoch: "e.1", Generation: 1, NativeClosePending: true, SemanticCleanupPending: true}
	restart := &Transition{Kind: KindRestart, DesiredRevision: 3, Sequence: 2, Phase: PhaseActive}
	doneRestart := &Transition{Kind: KindRestart, DesiredRevision: 3, Sequence: 2, Phase: PhaseCleanup}
	if p, r, _, fence := plan(PlanInput{Method: MethodStop, Current: start}); r != nil || p.Reserved == nil || p.Reserved.Kind != KindStop || !fence {
		t.Fatalf("STOP behind START = %+v %v fence=%v", p, r, fence)
	}
	if _, r, _, _ := plan(PlanInput{Method: MethodStop, Current: restart}); r == nil || r.Code != RefusalTransitionBusy || r.Row != 8 {
		t.Fatalf("request during executing RESTART = %v", r)
	}
	if p, r, _, _ := plan(PlanInput{Method: MethodStop, Current: doneRestart}); r != nil || p.Reserved == nil {
		t.Fatalf("request behind a terminal RESTART = %+v %v", p, r)
	}
	if _, r, _, _ := plan(PlanInput{Method: MethodStart, Current: start, Reserved: &Transition{Kind: KindStop, DesiredRevision: 3, Sequence: 2, Phase: PhasePending}}); r == nil || r.Code != RefusalTransitionBusy {
		t.Fatalf("both slots occupied = %v", r)
	}
	d.untyped = &attempt{corr: LifecycleCorrelation{DriverID: "fam.a", Epoch: "inc1.fam.a.1", Generation: 2}, cancel: func() {}}
	if p, r, _, fence := plan(PlanInput{Method: MethodRestart}); r != nil || p.Current == nil || p.Current.Kind != KindCancel ||
		p.Current.Generation != 2 || p.Reserved.Kind != KindRestart || !fence {
		t.Fatalf("RESTART during untyped attempt = %+v %v", p, r)
	}
	d.untyped = nil
	d.pruning = true
	if _, r, planned, _ := plan(PlanInput{Method: MethodStart, Current: doneRestart}); r == nil || r.Code != RefusalTransitionBusy || planned {
		t.Fatalf("decision during an uncommitted prune = %v", r)
	}
	if p, r, _, _ := plan(PlanInput{Method: MethodStop}); r != nil || p.Current == nil {
		t.Fatalf("decision after the prune committed = %+v %v", p, r)
	}
	d.pruning = false
	for _, flag := range []*bool{&d.quarantined, &d.recoveryBlocked} {
		*flag = true
		for _, m := range []Method{MethodStart, MethodRestart} {
			if _, r, _, _ := plan(PlanInput{Method: m}); r == nil || r.Code != RefusalFailedPrecondition || r.Row != 7 {
				t.Fatalf("%s while blocked = %v", m, r)
			}
		}
		if _, r, _, _ := plan(PlanInput{Method: MethodStop}); r != nil {
			t.Fatalf("STOP while blocked refused: %v", r)
		}
		*flag = false
	}
}

// TestServiceWorkUpdateGuard closes the correlation and obligation residuals
// for every work update the service writes. Each case varies one field of a
// stored transition; the comment names the mutation of checkWorkUpdate it
// catches.
func TestServiceWorkUpdateGuard(t *testing.T) {
	deadline := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	stored := Transition{Kind: KindStart, DesiredRevision: 5, Sequence: 3, Phase: PhaseActive, Epoch: "inc1.fam.a.1",
		Generation: 2, NativeClosePending: true, SemanticCleanupPending: true, AttemptsUsed: 2, DeadlineUTC: deadline}
	cleared := stored
	cleared.NativeClosePending, cleared.SemanticCleanupPending = false, false
	pending := Transition{Kind: KindStop, DesiredRevision: 6, Sequence: 4, Phase: PhasePending}
	with := func(base Transition, fn func(*Transition)) Transition { fn(&base); return base }
	cases := []struct {
		name   string
		stored Transition
		next   Transition
		ok     bool
	}{
		{"unchanged", stored, stored, true},
		// identity: dropping any sameIdentity comparison lets a stale worker rewrite another transition.
		{"kind", stored, with(stored, func(t *Transition) { t.Kind = KindRestart }), false},
		{"desired revision", stored, with(stored, func(t *Transition) { t.DesiredRevision = 6 }), false},
		{"sequence", stored, with(stored, func(t *Transition) { t.Sequence = 4 }), false},
		// phase: dropping the rank check lets a stale update move CLEANUP back to ACTIVE.
		{"phase backwards", stored, with(stored, func(t *Transition) { t.Phase = PhasePending }), false},
		{"phase forwards", stored, with(stored, func(t *Transition) { t.Phase = PhaseCleanup }), true},
		{"phase unknown", stored, with(stored, func(t *Transition) { t.Phase = "PAUSED" }), false},
		// attempts and deadline: dropping either check renews the bounded budget.
		{"attempts lowered", stored, with(stored, func(t *Transition) { t.AttemptsUsed = 1 }), false},
		{"attempts raised", stored, with(stored, func(t *Transition) { t.AttemptsUsed = 3 }), true},
		{"deadline extended", stored, with(stored, func(t *Transition) { t.DeadlineUTC = deadline.Add(time.Second) }), false},
		{"deadline removed", stored, with(stored, func(t *Transition) { t.DeadlineUTC = time.Time{} }), false},
		{"deadline shortened", stored, with(stored, func(t *Transition) { t.DeadlineUTC = deadline.Add(-time.Second) }), true},
		// epoch: allowing removal or early replacement resumes cleanup against the wrong correlation.
		{"epoch removed", stored, with(stored, func(t *Transition) { t.Epoch = "" }), false},
		{"epoch replaced with obligations pending", stored, with(stored, func(t *Transition) { t.Epoch = "inc2.fam.a.1" }), false},
		{"epoch replaced after obligations cleared", cleared, with(cleared, func(t *Transition) {
			t.Epoch = "inc2.fam.a.1"
			t.Generation = 1
			t.NativeClosePending = true
			t.SemanticCleanupPending = true
		}), true},
		// generation: allowing a decrease reuses a fenced generation.
		{"generation lowered", stored, with(stored, func(t *Transition) { t.Generation = 1 }), false},
		{"generation raised", stored, with(stored, func(t *Transition) { t.Generation = 3 }), true},
		// obligations: allowing false->true repeats a completed native close or semantic cleanup.
		{"native close re-activated", with(stored, func(t *Transition) { t.NativeClosePending = false }), stored, false},
		{"semantic cleanup re-activated", with(stored, func(t *Transition) { t.SemanticCleanupPending = false }), stored, false},
		{"native close cleared", stored, with(stored, func(t *Transition) { t.NativeClosePending = false }), true},
		{"semantic cleanup cleared", stored, with(stored, func(t *Transition) { t.SemanticCleanupPending = false }), true},
		// initial assignment at PENDING -> ACTIVE incurs both obligations.
		{"initial assignment", pending, with(pending, func(t *Transition) {
			t.Phase = PhaseActive
			t.Epoch, t.Generation = "inc1.fam.a.2", 1
			t.NativeClosePending, t.SemanticCleanupPending = true, true
			t.DeadlineUTC = deadline
		}), true},
	}
	for _, tc := range cases {
		s, n := tc.stored, tc.next
		if err := checkWorkUpdate(&s, &n); (err == nil) != tc.ok {
			t.Fatalf("%s: checkWorkUpdate error = %v, want ok=%v", tc.name, err, tc.ok)
		}
	}
}

// --- review round: durability of withdrawal, bounded cleanup, races ---

// hookStore wraps a Store with test-controlled faults.
type hookStore struct {
	Store
	failAdvance atomic.Bool
	driverHook  atomic.Pointer[func(DriverID) ControlRecord]
}

func (h *hookStore) AdvanceCatalog(ctx context.Context) (CatalogRevision, error) {
	if h.failAdvance.Load() {
		return 0, errors.New("synthetic advance failure")
	}
	return h.Store.AdvanceCatalog(ctx)
}

func (h *hookStore) Driver(id DriverID) (ControlRecord, error) {
	rec, err := h.Store.Driver(id)
	if fn := h.driverHook.Swap(nil); fn != nil && err == nil {
		(*fn)(id)
	}
	return rec, err
}

// TestServiceRestartPersistsPredecessorBeforeStop: while the executor is
// withdrawing the admitted generation for a RESTART, the durable slot already
// carries that generation's correlation and both cleanup obligations.
func TestServiceRestartPersistsPredecessorBeforeStop(t *testing.T) {
	id := DriverID("fam.alpha")
	f := newFixture(t, svcOpts{drivers: enabled(string(id))})
	f.waitSettled(id, drivermanager.ObservedRunning)
	live := f.execs[id].snapshot()[0].Corr
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	f.execs[id].setStop(func(context.Context, LifecycleCorrelation) StopOutcome {
		once.Do(func() { close(entered) })
		<-release
		return StopOutcome{NativeClosed: true, SemanticCleaned: true}
	})
	f.mustDecide(MethodRestart, id)
	<-entered
	cur := f.record(id).Current
	if cur == nil || cur.Kind != KindRestart || cur.Phase != PhaseActive || cur.Epoch != live.Epoch ||
		cur.Generation != live.Generation || !cur.NativeClosePending || !cur.SemanticCleanupPending || cur.DeadlineUTC.IsZero() {
		t.Fatalf("slot during withdrawal = %+v, want the predecessor correlation with both obligations", cur)
	}
	close(release)
	f.waitSettled(id, drivermanager.ObservedRunning)
}

// TestServiceCleanupStopIsBounded: a context-respecting executor cannot block
// cleanup past the operation budget, and an unfinished semantic cleanup is
// retried only a bounded number of times per series.
func TestServiceCleanupStopIsBounded(t *testing.T) {
	id := DriverID("fam.alpha")
	f := newFixture(t, svcOpts{drivers: enabled(string(id)),
		opts: func(o *Options) { o.OperationBudget = 100 * time.Millisecond; o.MaxAttempts = 3 }})
	f.waitSettled(id, drivermanager.ObservedRunning)
	var hasDeadline atomic.Bool
	hasDeadline.Store(true)
	f.execs[id].setStop(func(ctx context.Context, _ LifecycleCorrelation) StopOutcome {
		if _, ok := ctx.Deadline(); !ok {
			hasDeadline.Store(false)
		}
		<-ctx.Done()
		return StopOutcome{NativeClosed: true}
	})
	f.mustDecide(MethodStop, id)
	waitFor(t, "cleanup stalled", func() bool { return f.status(id).ObservedState == drivermanager.ObservedFailed })
	time.Sleep(300 * time.Millisecond)
	if !hasDeadline.Load() {
		t.Fatal("Stop ran without a deadline")
	}
	// The first Stop consumed the whole nonrenewable budget, so no retry
	// follows; the attempt limit (3) bounds a series with budget left.
	if n := f.execs[id].count("stop"); n != 1 {
		t.Fatalf("semantic cleanup ran %d times, want 1 once the deadline is spent", n)
	}
	if cur := f.record(id).Current; cur == nil || !cur.SemanticCleanupPending || cur.NativeClosePending {
		t.Fatalf("unfinished cleanup not retained: %+v", cur)
	}
}

// TestServiceStopBeforeUntypedInstallClosesGeneration: a STOP accepted after
// the reconciler read its (empty) slots but before the process-start attempt
// was installed must prevent that attempt from starting at all.
func TestServiceStopBeforeUntypedInstallClosesGeneration(t *testing.T) {
	id := DriverID("fam.alpha")
	var hs *hookStore
	reached, release := make(chan struct{}), make(chan struct{})
	hook := func(DriverID) ControlRecord {
		close(reached)
		<-release
		return ControlRecord{}
	}
	f := newFixture(t, svcOpts{drivers: enabled(string(id)), noStart: true,
		wrap: func(s Store) Store { hs = &hookStore{Store: s}; hs.driverHook.Store(&hook); return hs }})
	if err := f.svc.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-reached
	f.mustDecide(MethodStop, id)
	close(release)
	f.waitSettled(id, drivermanager.ObservedStopped)
	if n := f.execs[id].count("start"); n != 0 {
		t.Fatalf("a start ran after the STOP was accepted (%d)", n)
	}
}

// TestServiceLateStartSuccessIsNotAdmitted: a START that returns nil after
// its attempt deadline is withdrawn and never published RUNNING, for the
// typed and the process-start path.
func TestServiceLateStartSuccessIsNotAdmitted(t *testing.T) {
	late := func(ctx context.Context, _ LifecycleCorrelation) error {
		<-ctx.Done()
		return nil
	}
	assertNeverRunning := func(t *testing.T, f *svcFixture, id DriverID, stopsWant int) {
		waitFor(t, "failed", func() bool { return f.status(id).ObservedState == drivermanager.ObservedFailed })
		if st := f.status(id); st.Reason != drivermanager.ReasonRetryExhausted {
			t.Fatalf("status = %+v", st)
		}
		waitFor(t, "late generation withdrawn", func() bool { return f.execs[id].count("stop") >= stopsWant })
		if st := f.status(id); st.ObservedState == drivermanager.ObservedRunning {
			t.Fatalf("late success published RUNNING: %+v", st)
		}
	}
	id := DriverID("fam.alpha")
	t.Run("untyped", func(t *testing.T) {
		f := newFixture(t, svcOpts{drivers: enabled(string(id)),
			opts:  func(o *Options) { o.OperationBudget = 100 * time.Millisecond },
			setup: func(f *svcFixture) { f.execs[id].setStart(late) }})
		assertNeverRunning(t, f, id, 1)
	})
	t.Run("typed", func(t *testing.T) {
		f := newFixture(t, svcOpts{drivers: enabled(string(id)),
			opts: func(o *Options) { o.OperationBudget = 200 * time.Millisecond }})
		f.waitSettled(id, drivermanager.ObservedRunning)
		f.execs[id].setStart(late)
		f.mustDecide(MethodRestart, id)
		assertNeverRunning(t, f, id, 2)
	})
}

// TestServiceFailedObservationPublicationIsRetried: a transient catalog
// failure neither loses the transition nor its event; both appear in order
// once the store recovers, with no further action.
func TestServiceFailedObservationPublicationIsRetried(t *testing.T) {
	id := DriverID("fam.alpha")
	var hs *hookStore
	f := newFixture(t, svcOpts{drivers: enabled(string(id)),
		wrap: func(s Store) Store { hs = &hookStore{Store: s}; return hs }})
	f.waitSettled(id, drivermanager.ObservedRunning)
	cursor := f.svc.List().EventCursor
	hs.failAdvance.Store(true)
	f.mustDecide(MethodStop, id)
	f.waitIdle(id)
	time.Sleep(20 * time.Millisecond)
	if st := f.status(id); st.ObservedState == drivermanager.ObservedStopped {
		t.Fatalf("transition published despite failing catalog: %+v", st)
	}
	hs.failAdvance.Store(false)
	f.waitObserved(id, drivermanager.ObservedStopped)
	page := f.svc.EventsSince(cursor)
	last := page.Events[len(page.Events)-1]
	if page.Resync || last.Kind != EventObservedChanged || last.ObservedState != drivermanager.ObservedStopped {
		t.Fatalf("events after recovery = %+v", page)
	}
	for i := 1; i < len(page.Events); i++ {
		if page.Events[i].Cursor != page.Events[i-1].Cursor+1 || page.Events[i].CatalogRevision < page.Events[i-1].CatalogRevision {
			t.Fatalf("events out of order: %+v", page.Events)
		}
	}
}

// TestServiceEventsFollowTheirView: a reader woken by an event always finds a
// list at least as new as the event's catalog revision.
func TestServiceEventsFollowTheirView(t *testing.T) {
	id := DriverID("fam.alpha")
	f := newFixture(t, svcOpts{drivers: enabled(string(id))})
	f.waitSettled(id, drivermanager.ObservedRunning)
	stop := make(chan struct{})
	bad := make(chan string, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		cursor := f.svc.List().EventCursor
		for {
			notify := f.svc.EventNotify()
			page := f.svc.EventsSince(cursor)
			for _, e := range page.Events {
				if got := f.svc.List(); got.Revision < e.CatalogRevision || got.EventCursor < e.Cursor {
					select {
					case bad <- fmt.Sprintf("event %+v seen before list %d/%d", e, got.Revision, got.EventCursor):
					default:
					}
				}
			}
			cursor = page.Next
			select {
			case <-notify:
			case <-stop:
				return
			}
		}
	}()
	for i := 0; i < 20; i++ {
		f.mustDecide(MethodRestart, id)
		f.waitSettled(id, drivermanager.ObservedRunning)
	}
	close(stop)
	<-done
	select {
	case msg := <-bad:
		t.Fatal(msg)
	default:
	}
}

// TestServiceFatalStartLeavesServiceNotRunning: a Start refused by startup
// validation accepts no decision afterwards.
func TestServiceFatalStartLeavesServiceNotRunning(t *testing.T) {
	a, b := DriverID("fam.alpha"), DriverID("fam.beta")
	f := newFixture(t, svcOpts{drivers: enabled(string(a), string(b)), noStart: true,
		opts: func(o *Options) { o.Drivers = o.Drivers[:1] }})
	if err := f.svc.Start(context.Background()); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Start error = %v", err)
	}
	before := f.probe()
	_, err := f.svc.Control(deadlineCtx(t), MethodStart, body(a, 1))
	refusalOf(t, err, RefusalUnavailable, 3)
	f.assertUnchanged("decision after fatal start", before)
}

// TestServiceInitialViewIsRetried: a transient failure while publishing the
// first view is retried instead of leaving the constructor's empty catalog.
func TestServiceInitialViewIsRetried(t *testing.T) {
	id := DriverID("fam.alpha")
	var hs *hookStore
	f := newFixture(t, svcOpts{drivers: []runtimestate.ControlDriverConfig{{DriverID: string(id)}}, noStart: true,
		wrap: func(s Store) Store { hs = &hookStore{Store: s}; return hs }})
	hs.failAdvance.Store(true)
	if err := f.svc.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := len(f.svc.List().Drivers); n != 0 {
		t.Fatalf("view published despite failing catalog: %d", n)
	}
	hs.failAdvance.Store(false)
	waitFor(t, "initial view", func() bool { return len(f.svc.List().Drivers) == 1 })
}

// TestServiceUntypedLateSuccessKeepsUnfinishedCleanupBlocking: a process-start
// success after its deadline whose withdrawal closes natively but leaves
// semantic cleanup unfinished is not discarded: the driver is FAILED with a
// CLEANUP_STALLED diagnostic and a later RESTART is refused at row 7. The
// late withdrawal stays bounded by the attempt's original deadline: its one
// Stop receives an already-expired context and is never retried with a fresh
// budget.
func TestServiceUntypedLateSuccessKeepsUnfinishedCleanupBlocking(t *testing.T) {
	id := DriverID("fam.alpha")
	var renewed atomic.Bool
	f := newFixture(t, svcOpts{drivers: enabled(string(id)),
		opts: func(o *Options) { o.OperationBudget = 100 * time.Millisecond; o.MaxAttempts = 3 },
		setup: func(f *svcFixture) {
			f.execs[id].setStart(func(ctx context.Context, _ LifecycleCorrelation) error {
				<-ctx.Done()
				return nil
			})
			f.execs[id].setStop(func(ctx context.Context, _ LifecycleCorrelation) StopOutcome {
				if ctx.Err() == nil {
					renewed.Store(true)
				}
				return StopOutcome{NativeClosed: true}
			})
		}})
	waitFor(t, "failed", func() bool { return f.status(id).ObservedState == drivermanager.ObservedFailed })
	waitFor(t, "cleanup stalled diagnostic", func() bool { return hasDiagnostic(f.svc.Diagnostics(id), "CLEANUP_STALLED") })
	time.Sleep(200 * time.Millisecond)
	if n := f.execs[id].count("stop"); n != 1 {
		t.Fatalf("late withdrawal ran Stop %d times, want 1 bounded by the original deadline", n)
	}
	if renewed.Load() {
		t.Fatal("late withdrawal Stop received a renewed deadline")
	}
	_, err := f.decide(MethodRestart, id)
	refusalOf(t, err, RefusalFailedPrecondition, 7)
}

func hasDiagnostic(diags []Diagnostic, outcome string) bool {
	for _, d := range diags {
		if d.Outcome == outcome {
			return true
		}
	}
	return false
}

// TestServiceAcceptedEventCarriesObservedState: DESIRED_ACCEPTED reports the
// driver's observed state at acceptance, not an empty one.
func TestServiceAcceptedEventCarriesObservedState(t *testing.T) {
	id := DriverID("fam.alpha")
	f := newFixture(t, svcOpts{drivers: enabled(string(id))})
	f.waitSettled(id, drivermanager.ObservedRunning)
	cursor := f.svc.events.cursor()
	f.mustDecide(MethodStop, id)
	for _, e := range f.svc.EventsSince(cursor).Events {
		if e.Kind == EventDesiredAccepted {
			if e.ObservedState == "" {
				t.Fatalf("accepted event has no observed state: %+v", e)
			}
			return
		}
	}
	t.Fatal("no accepted event")
}

// TestServiceCleanupAttemptsArePersisted: each cleanup Stop is counted in the
// slot's persisted AttemptsUsed so process recovery cannot renew it.
func TestServiceCleanupAttemptsArePersisted(t *testing.T) {
	id := DriverID("fam.alpha")
	f := newFixture(t, svcOpts{drivers: enabled(string(id)),
		opts: func(o *Options) { o.MaxAttempts = 3 }})
	f.waitSettled(id, drivermanager.ObservedRunning)
	f.execs[id].setStop(func(context.Context, LifecycleCorrelation) StopOutcome {
		return StopOutcome{NativeClosed: true}
	})
	f.mustDecide(MethodStop, id)
	waitFor(t, "cleanup stalled", func() bool { return f.status(id).ObservedState == drivermanager.ObservedFailed })
	if cur := f.record(id).Current; cur == nil || cur.AttemptsUsed != 3 {
		t.Fatalf("persisted cleanup attempts = %+v, want 3", cur)
	}
}

// TestServiceCleanupRetryNeverRenewsExpiredDeadline: once the persisted
// deadline has passed, an unfinished semantic cleanup is not retried with a
// fresh budget; the obligation stays durable.
func TestServiceCleanupRetryNeverRenewsExpiredDeadline(t *testing.T) {
	id := DriverID("fam.alpha")
	f := newFixture(t, svcOpts{drivers: enabled(string(id)),
		opts: func(o *Options) { o.OperationBudget = 100 * time.Millisecond }})
	f.waitSettled(id, drivermanager.ObservedRunning)
	f.execs[id].setStop(func(context.Context, LifecycleCorrelation) StopOutcome {
		time.Sleep(150 * time.Millisecond) // outlives the slot's deadline
		return StopOutcome{NativeClosed: true}
	})
	f.mustDecide(MethodStop, id)
	waitFor(t, "cleanup stalled", func() bool { return f.status(id).ObservedState == drivermanager.ObservedFailed })
	time.Sleep(400 * time.Millisecond)
	if n := f.execs[id].count("stop"); n != 1 {
		t.Fatalf("Stop ran %d times, want 1 (no fresh budget after the deadline)", n)
	}
	if cur := f.record(id).Current; cur == nil || !cur.SemanticCleanupPending {
		t.Fatalf("unfinished cleanup not retained: %+v", cur)
	}
}

// TestServiceDeferredObservationsMatchTheirView: transitions queued while
// publication fails are coalesced per driver, so no replayed event reports an
// observation the view at its catalog revision no longer shows.
func TestServiceDeferredObservationsMatchTheirView(t *testing.T) {
	id := DriverID("fam.alpha")
	var hs *hookStore
	f := newFixture(t, svcOpts{drivers: enabled(string(id)),
		wrap: func(s Store) Store { hs = &hookStore{Store: s}; return hs }})
	f.waitSettled(id, drivermanager.ObservedRunning)
	cursor := f.svc.List().EventCursor
	hs.failAdvance.Store(true)
	f.mustDecide(MethodStop, id)
	f.waitIdle(id)
	f.mustDecide(MethodStart, id)
	f.waitIdle(id)
	hs.failAdvance.Store(false)
	f.waitObserved(id, drivermanager.ObservedRunning)
	view := f.svc.List()
	for _, e := range f.svc.EventsSince(cursor).Events {
		if e.Kind == EventObservedChanged && e.CatalogRevision == view.Revision && e.ObservedState != f.status(id).ObservedState {
			t.Fatalf("event %+v reports an observation the view at its revision does not show", e)
		}
	}
}

// TestServicePendingEventOverflowForcesResync: events discarded before they
// received a cursor cannot leave a silent gap; readers positioned before it
// are told to resynchronise.
func TestServicePendingEventOverflowForcesResync(t *testing.T) {
	id := DriverID("fam.alpha")
	var hs *hookStore
	f := newFixture(t, svcOpts{drivers: enabled(string(id)),
		wrap: func(s Store) Store { hs = &hookStore{Store: s}; return hs }})
	f.waitSettled(id, drivermanager.ObservedRunning)
	cursor := f.svc.List().EventCursor
	hs.failAdvance.Store(true)
	for i := 0; i < maxPendingEvents+1; i++ {
		f.svc.deferPublication(&Event{Kind: EventDesiredAccepted, DriverID: id})
	}
	hs.failAdvance.Store(false)
	waitFor(t, "pending events published", func() bool { return f.svc.EventsSince(cursor).Next > cursor })
	if page := f.svc.EventsSince(cursor); !page.Resync {
		t.Fatalf("overflow left a silent gap: %+v", page)
	}
}

// --- author round: readback resolver, nonrenewable cleanup, views, cursor ---

// TestServiceResolvesAmbiguousCommitWithoutFurtherRequest: a decision whose
// commit is ambiguous (directory fsync failure after the rename) closes
// admission. The service runs the bounded readback itself: with no further
// request, admission reopens, the adopted decision appears in List and the
// reconciler executes it, within the documented bound.
func TestServiceResolvesAmbiguousCommitWithoutFurtherRequest(t *testing.T) {
	id := DriverID("fam.alpha")
	f := newFixture(t, svcOpts{drivers: enabled(string(id))})
	f.waitSettled(id, drivermanager.ObservedRunning)
	before := f.status(id)
	f.quiesce()
	f.hooks.mu.Lock()
	f.hooks.failDirSync = 1
	f.hooks.mu.Unlock()
	start := time.Now()
	_, err := f.decide(MethodStop, id)
	refusalOf(t, err, RefusalUnavailable, 3)
	waitFor(t, "ambiguous decision adopted without a further request", func() bool {
		st := f.status(id)
		return f.ms.AdmissionOpen() && st.DesiredState == DesiredStopped && st.DesiredRevision > before.DesiredRevision &&
			st.ObservedState == drivermanager.ObservedStopped
	})
	// Bound: the first readback runs at once; the series is at most
	// MaxAttempts readbacks spaced by RetryDelay (2 ms here).
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("ambiguous commit resolved after %v, beyond the bound", elapsed)
	}
	if n := f.execs[id].count("stop"); n != 1 {
		t.Fatalf("adopted STOP executed %d times, want 1", n)
	}
	if got := f.fileDesired(id); got != string(DesiredStopped) {
		t.Fatalf("durable desired = %q", got)
	}
}

// TestServiceExpiredCleanupDeadlineIsNeverRenewed: an ACTIVE slot recovered
// after its persisted deadline gets no fresh budget. Its one cleanup Stop
// receives an already-expired context; the unfinished obligation ends FAILED
// with CLEANUP_STALLED, blocks START and RESTART (row 7), is never retried
// and keeps the original deadline durably.
func TestServiceExpiredCleanupDeadlineIsNeverRenewed(t *testing.T) {
	id := DriverID("fam.alpha")
	past := "2001-01-01T00:00:00Z"
	var renewed atomic.Bool
	f := newFixture(t, svcOpts{
		drivers: enabled(string(id)),
		state: controlState(1, 3, map[string]string{
			string(id): recordJSON(DesiredRunning, 1, 1, transitionJSON(KindStart, 1, 1, PhaseActive, "old.fam.alpha.1", 1, true, true, 1, past)),
		}, ""),
		incarnation: "inc2",
		setup: func(f *svcFixture) {
			f.execs[id].setStop(func(ctx context.Context, _ LifecycleCorrelation) StopOutcome {
				if ctx.Err() == nil {
					renewed.Store(true)
				}
				return StopOutcome{NativeClosed: true}
			})
		},
	})
	waitFor(t, "cleanup stalled", func() bool {
		return f.status(id).ObservedState == drivermanager.ObservedFailed && hasDiagnostic(f.svc.Diagnostics(id), "CLEANUP_STALLED")
	})
	time.Sleep(200 * time.Millisecond)
	if renewed.Load() {
		t.Fatal("cleanup Stop received a renewed deadline")
	}
	if n := f.execs[id].count("stop"); n != 1 {
		t.Fatalf("expired cleanup ran Stop %d times, want 1", n)
	}
	if n := f.execs[id].count("start"); n != 0 {
		t.Fatalf("a start ran over unfinished cleanup (%d)", n)
	}
	cur := f.record(id).Current
	want, _ := time.Parse(time.RFC3339, past)
	if cur == nil || !cur.SemanticCleanupPending || cur.NativeClosePending || !cur.DeadlineUTC.Equal(want) {
		t.Fatalf("slot after stalled cleanup = %+v, want the obligation and the original deadline", cur)
	}
	_, err := f.decide(MethodRestart, id)
	refusalOf(t, err, RefusalFailedPrecondition, 7)
}

// TestServiceCleanupAttemptChargedBeforeStop: the cleanup attempt is durable
// before the executor's Stop runs, so process loss during Stop cannot grant
// an extra attempt on recovery.
func TestServiceCleanupAttemptChargedBeforeStop(t *testing.T) {
	id := DriverID("fam.alpha")
	f := newFixture(t, svcOpts{drivers: enabled(string(id))})
	f.waitSettled(id, drivermanager.ObservedRunning)
	entered, release := make(chan struct{}), make(chan struct{})
	var once, releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock) // runs before the fixture's Close, even after a failure
	f.execs[id].setStop(func(context.Context, LifecycleCorrelation) StopOutcome {
		once.Do(func() { close(entered) })
		<-release
		return StopOutcome{NativeClosed: true, SemanticCleaned: true}
	})
	f.mustDecide(MethodStop, id)
	<-entered
	cur := f.record(id).Current
	if cur == nil || cur.AttemptsUsed != 1 || !cur.NativeClosePending || !cur.SemanticCleanupPending {
		t.Fatalf("slot while Stop runs = %+v, want the attempt already charged with both obligations pending", cur)
	}
	unblock()
	f.waitSettled(id, drivermanager.ObservedStopped)
}

// roStore reports the committed catalog as read-only (a LOCKED or
// persistence-blocked restart).
type roStore struct{ Store }

func (r roStore) Catalog() (ControlCatalog, error) {
	c, err := r.Store.Catalog()
	c.ReadOnly = true
	return c, err
}

// TestServiceReadOnlyViewKeepsStoredRevisions: a read-only catalog at Start
// withholds every driver as FAILED but keeps the stored catalog revision and
// each stored desired revision unchanged, never zero.
func TestServiceReadOnlyViewKeepsStoredRevisions(t *testing.T) {
	a, b := DriverID("fam.a"), DriverID("fam.b")
	f := newFixture(t, svcOpts{
		drivers: enabled(string(a), string(b)),
		state: controlState(7, 11, map[string]string{
			string(a): recordJSON(DesiredRunning, 5, 0, ""),
			string(b): recordJSON(DesiredStopped, 7, 0, ""),
		}, ""),
		wrap:    func(s Store) Store { return roStore{s} },
		noStart: true,
	})
	stored, err := f.ms.Catalog()
	if err != nil || stored.CatalogRevision == 0 {
		t.Fatalf("stored catalog = %+v, %v", stored, err)
	}
	err = f.svc.Start(context.Background())
	refusalOf(t, err, RefusalFailedPrecondition, 3)
	view := f.svc.List()
	if view.Revision != stored.CatalogRevision {
		t.Fatalf("read-only view revision = %d, want stored %d", view.Revision, stored.CatalogRevision)
	}
	want := map[DriverID]DesiredRevision{}
	for _, rec := range stored.Drivers {
		want[rec.DriverID] = rec.DesiredRevision
	}
	for _, e := range view.Drivers {
		if e.Revision != stored.CatalogRevision || e.DesiredRevision != want[e.DriverID] || e.DesiredRevision == 0 ||
			e.ObservedState != drivermanager.ObservedFailed {
			t.Fatalf("read-only entry = %+v, want stored revisions %d/%d", e, stored.CatalogRevision, want[e.DriverID])
		}
	}
}

// TestServicePublishedCursorEventsExist: a reader that resyncs at a published
// EventCursor always finds that cursor's events, deterministically at the
// event log and under concurrent publications.
func TestServicePublishedCursorEventsExist(t *testing.T) {
	t.Run("log", func(t *testing.T) {
		l := newEventLog(8)
		got := make(chan EventPage, 1)
		l.publish([]Event{{Kind: EventObservedChanged}, {Kind: EventObservedChanged}}, false, func(cursor uint64) {
			// A reader that saw the exposed cursor reads right away; it
			// must wait for the publication and then find both events.
			go func() { got <- l.since(cursor - 2) }()
		})
		page := <-got
		if page.Resync || len(page.Events) != 2 || page.Next != 2 {
			t.Fatalf("resync at the exposed cursor = %+v", page)
		}
	})
	t.Run("service", func(t *testing.T) {
		id := DriverID("fam.alpha")
		f := newFixture(t, svcOpts{drivers: enabled(string(id))})
		f.waitSettled(id, drivermanager.ObservedRunning)
		stop := make(chan struct{})
		bad := make(chan string, 1)
		done := make(chan struct{})
		go func() {
			defer close(done)
			for {
				select {
				case <-stop:
					return
				default:
				}
				view := f.svc.List()
				if view.EventCursor == 0 {
					continue
				}
				page := f.svc.EventsSince(view.EventCursor - 1)
				if page.Resync || len(page.Events) == 0 || page.Events[0].Cursor != view.EventCursor {
					select {
					case bad <- fmt.Sprintf("cursor %d published before its event: %+v", view.EventCursor, page):
					default:
					}
					return
				}
			}
		}()
		for i := 0; i < 15; i++ {
			f.mustDecide(MethodRestart, id)
			f.waitSettled(id, drivermanager.ObservedRunning)
		}
		close(stop)
		<-done
		select {
		case msg := <-bad:
			t.Fatal(msg)
		default:
		}
	})
}

// TestServicePublishCoalescesSupersededObservations: when an earlier
// observation's publication failed and a newer observation of the same
// driver is published successfully, the superseded event is not stamped with
// the newer view's revision.
func TestServicePublishCoalescesSupersededObservations(t *testing.T) {
	id := DriverID("fam.alpha")
	var hs *hookStore
	f := newFixture(t, svcOpts{drivers: enabled(string(id)),
		wrap: func(s Store) Store { hs = &hookStore{Store: s}; return hs }})
	f.waitSettled(id, drivermanager.ObservedRunning)
	cursor := f.svc.List().EventCursor
	d := f.svc.drivers[id]
	hs.failAdvance.Store(true)
	stopping := observation{state: drivermanager.ObservedStopping, reason: drivermanager.ReasonStopRequested}
	if f.svc.stageObservation(id, stopping) {
		f.svc.commitObserved(d, stopping)
	}
	stopped := observation{state: drivermanager.ObservedStopped, reason: drivermanager.ReasonOperatorStopped}
	f.svc.stageObservation(id, stopped)
	hs.failAdvance.Store(false)
	f.svc.commitObserved(d, stopped)
	f.waitObserved(id, drivermanager.ObservedStopped)
	page := f.svc.EventsSince(cursor)
	sawStopped := false
	for _, e := range page.Events {
		if e.Kind != EventObservedChanged {
			continue
		}
		if e.ObservedState == drivermanager.ObservedStopping {
			t.Fatalf("superseded STOPPING event emitted with revision %d: %+v", e.CatalogRevision, page.Events)
		}
		sawStopped = sawStopped || e.ObservedState == drivermanager.ObservedStopped
	}
	if page.Resync || !sawStopped {
		t.Fatalf("events after publication = %+v", page)
	}
}

// TestServiceSynthesizedCancelInheritsUntypedBudget: a STOP accepted during an
// untyped BACKOFF synthesizes the cancellation with the attempt's own
// nonrenewable deadline and attempts already used, never a fresh budget.
func TestServiceSynthesizedCancelInheritsUntypedBudget(t *testing.T) {
	id := DriverID("fam.alpha")
	entered := make(chan LifecycleCorrelation, 16)
	f := newFixture(t, svcOpts{
		drivers: enabled(string(id)),
		opts:    func(o *Options) { o.RetryDelay = func(int) time.Duration { return time.Hour } },
		setup: func(f *svcFixture) {
			f.execs[id].setStart(func(ctx context.Context, c LifecycleCorrelation) error {
				entered <- c
				return retryErr{"warming"}
			})
		},
	})
	<-entered
	f.waitObserved(id, drivermanager.ObservedBackoff)
	time.Sleep(60 * time.Millisecond)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	f.execs[id].setStop(func(ctx context.Context, _ LifecycleCorrelation) StopOutcome {
		<-release
		return StopOutcome{NativeClosed: true, SemanticCleaned: true}
	})
	decided := time.Now()
	f.mustDecide(MethodStop, id)
	cur := f.record(id).Current
	if cur == nil || cur.Kind != KindCancel {
		t.Fatalf("cancellation not synthesized: %+v", cur)
	}
	if cur.AttemptsUsed < 1 {
		t.Fatalf("cancellation restarted the attempt count: %+v", cur)
	}
	if !cur.DeadlineUTC.Before(decided.Add(f.svc.budget).Add(-30 * time.Millisecond)) {
		t.Fatalf("cancellation got a renewed deadline %v (accepted %v)", cur.DeadlineUTC, decided)
	}
}

// TestServiceAmbiguousStopFencesUntypedBackoffAfterReadback: a STOP whose commit
// is ambiguous during an untyped BACKOFF is adopted by the readback; the
// attempt must not open another generation, and the cleanup closes the
// generation the cancellation slot names.
func TestServiceAmbiguousStopFencesUntypedBackoffAfterReadback(t *testing.T) {
	id := DriverID("fam.alpha")
	var mu sync.Mutex
	var started []LifecycleCorrelation
	var stopped []LifecycleCorrelation
	entered := make(chan struct{}, 16)
	f := newFixture(t, svcOpts{
		drivers: enabled(string(id)),
		opts:    func(o *Options) { o.RetryDelay = func(int) time.Duration { return 150 * time.Millisecond } },
		setup: func(f *svcFixture) {
			f.execs[id].setStart(func(ctx context.Context, c LifecycleCorrelation) error {
				mu.Lock()
				started = append(started, c)
				mu.Unlock()
				entered <- struct{}{}
				return retryErr{"warming"}
			})
			f.execs[id].setStop(func(ctx context.Context, c LifecycleCorrelation) StopOutcome {
				mu.Lock()
				stopped = append(stopped, c)
				mu.Unlock()
				return StopOutcome{NativeClosed: true, SemanticCleaned: true}
			})
		},
	})
	<-entered
	f.waitObserved(id, drivermanager.ObservedBackoff)
	f.hooks.mu.Lock()
	f.hooks.failDirSync = 1
	f.hooks.mu.Unlock()
	_, err := f.decide(MethodStop, id)
	refusalOf(t, err, RefusalUnavailable, 3)
	waitFor(t, "adopted STOP cleaned up", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(stopped) > 0 && f.status(id).DesiredState == DesiredStopped
	})
	time.Sleep(400 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	last := started[len(started)-1]
	for _, c := range stopped {
		if c.Epoch != last.Epoch || c.Generation != last.Generation {
			t.Fatalf("cleanup closed %+v but the last opened generation is %+v", c, last)
		}
	}
	if len(started) != 1 {
		t.Fatalf("attempt opened %d generations after the adopted STOP", len(started))
	}
}

// TestServiceSettleObligationsNeverRunsStopWithoutPersistedAttempt: a slot
// whose persisted attempts are spent runs no uncharged Stop, and a START that
// keeps failing leaves one persisted attempt for its cleanup.
func TestServiceSettleObligationsNeverRunsStopWithoutPersistedAttempt(t *testing.T) {
	id := DriverID("fam.alpha")
	f := newFixture(t, svcOpts{drivers: enabled(string(id)),
		opts: func(o *Options) { o.MaxAttempts = 3 }})
	f.waitSettled(id, drivermanager.ObservedRunning)
	stops := f.execs[id].count("stop")
	spent := Transition{Kind: KindStart, Phase: PhaseCleanup, Epoch: "e", Generation: 1,
		NativeClosePending: true, SemanticCleanupPending: true, AttemptsUsed: 3}
	next, err := f.svc.settleObligations(f.svc.drivers[id], spent)
	if err != nil || !next.NativeClosePending || !next.SemanticCleanupPending || next.AttemptsUsed != 3 {
		t.Fatalf("spent slot settled = %+v, %v", next, err)
	}
	if n := f.execs[id].count("stop"); n != stops {
		t.Fatalf("uncharged Stop ran %d times", n-stops)
	}

	f.mustDecide(MethodStop, id)
	f.waitSettled(id, drivermanager.ObservedStopped)
	f.execs[id].setStart(func(context.Context, LifecycleCorrelation) error { return retryErr{"never"} })
	startsBefore := f.execs[id].count("start")
	f.mustDecide(MethodStart, id)
	f.waitSettled(id, drivermanager.ObservedFailed)
	if n := f.execs[id].count("start") - startsBefore; n != 2 {
		t.Fatalf("START attempts = %d, want 2 with one attempt reserved for cleanup", n)
	}
}

// TestServiceAdoptedAmbiguousDecisionEmitsDesiredAccepted: a decision whose
// commit was ambiguous and later adopted by the readback is not silently
// absent from the event stream.
func TestServiceAdoptedAmbiguousDecisionEmitsDesiredAccepted(t *testing.T) {
	id := DriverID("fam.alpha")
	f := newFixture(t, svcOpts{drivers: enabled(string(id))})
	f.waitSettled(id, drivermanager.ObservedRunning)
	before := f.status(id)
	cursor := f.svc.List().EventCursor
	f.quiesce()
	f.hooks.mu.Lock()
	f.hooks.failDirSync = 1
	f.hooks.mu.Unlock()
	_, err := f.decide(MethodStop, id)
	refusalOf(t, err, RefusalUnavailable, 3)
	waitFor(t, "adopted decision event", func() bool {
		for _, e := range f.svc.EventsSince(cursor).Events {
			if e.Kind == EventDesiredAccepted && e.DriverID == id &&
				e.DesiredState == DesiredStopped && e.DesiredRevision == before.DesiredRevision+1 {
				return true
			}
		}
		return false
	})
}

// TestServiceSupersededPendingDesiredEventIsNotStampedWithLaterView: a pending
// DESIRED_ACCEPTED that a later desired transition superseded before its view
// published is not stamped with that view; readers are told to resync.
func TestServiceSupersededPendingDesiredEventIsNotStampedWithLaterView(t *testing.T) {
	id := DriverID("fam.alpha")
	var hs *hookStore
	f := newFixture(t, svcOpts{drivers: enabled(string(id)),
		wrap: func(s Store) Store { hs = &hookStore{Store: s}; return hs }})
	f.waitSettled(id, drivermanager.ObservedRunning)
	f.mustDecide(MethodStop, id)
	f.waitSettled(id, drivermanager.ObservedStopped)
	cur := f.status(id)
	cursor := f.svc.List().EventCursor
	hs.failAdvance.Store(true)
	f.svc.deferPublication(&Event{Kind: EventDesiredAccepted, DriverID: id,
		DesiredState: DesiredRunning, DesiredRevision: cur.DesiredRevision - 1})
	f.svc.deferPublication(&Event{Kind: EventDesiredAccepted, DriverID: id,
		DesiredState: DesiredStopped, DesiredRevision: cur.DesiredRevision})
	hs.failAdvance.Store(false)
	waitFor(t, "pending events published", func() bool { return f.svc.EventsSince(cursor).Next > cursor })
	page := f.svc.EventsSince(cursor)
	if !page.Resync {
		t.Fatalf("superseded event left a silent gap: %+v", page)
	}
	for _, e := range page.Events {
		if e.Kind == EventDesiredAccepted && e.DesiredRevision != cur.DesiredRevision {
			t.Fatalf("superseded event stamped with a later view: %+v", e)
		}
	}
}

// --- author round 5: one rule for superseding decisions and their readback ---

// readbackStore wraps a Store: an armed readback gate holds ResolveReadback, and
// an armed afterDecide hook runs once after a store Decide that succeeded.
type readbackStore struct {
	Store
	readback    atomic.Pointer[chan struct{}]
	afterDecide atomic.Pointer[func()]
}

func (g *readbackStore) ResolveReadback(ctx context.Context) error {
	if gate := g.readback.Load(); gate != nil {
		select {
		case <-*gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return g.Store.ResolveReadback(ctx)
}

func (g *readbackStore) Decide(ctx context.Context, method Method, req Request, planner Planner) (DesiredView, error) {
	view, err := g.Store.Decide(ctx, method, req, planner)
	if err == nil {
		if fn := g.afterDecide.Swap(nil); fn != nil {
			(*fn)()
		}
	}
	return view, err
}

func gatedFixture(t *testing.T, ids ...string) (*svcFixture, *readbackStore) {
	t.Helper()
	var g *readbackStore
	f := newFixture(t, svcOpts{drivers: enabled(ids...),
		wrap: func(s Store) Store { g = &readbackStore{Store: s}; return g }})
	return f, g
}

// heldStart is a typed START whose one Start call blocks until result
// delivers its outcome or the attempt's context ends.
type heldStart struct {
	entered chan struct{}
	result  chan error
	mu      sync.Mutex
	ctx     context.Context
	corr    LifecycleCorrelation
}

func (h *heldStart) attempt() (context.Context, LifecycleCorrelation) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.ctx, h.corr
}

// typedStartInFlight stops the driver, accepts a typed START and returns
// once its Start call is executing and the store is quiet.
func (f *svcFixture) typedStartInFlight(id DriverID) *heldStart {
	f.t.Helper()
	f.waitSettled(id, drivermanager.ObservedRunning)
	f.mustDecide(MethodStop, id)
	f.waitSettled(id, drivermanager.ObservedStopped)
	h := &heldStart{entered: make(chan struct{}), result: make(chan error, 1)}
	var once sync.Once
	f.execs[id].setStart(func(ctx context.Context, c LifecycleCorrelation) error {
		h.mu.Lock()
		h.ctx, h.corr = ctx, c
		h.mu.Unlock()
		once.Do(func() { close(h.entered) })
		select {
		case err := <-h.result:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	f.mustDecide(MethodStart, id)
	<-h.entered
	f.quiesce()
	return h
}

// ambiguousStop decides a STOP whose commit is ambiguous (directory fsync
// failure after the rename) while the readback is held at the gate. It
// returns the gate and the state file as committed before the STOP.
func (f *svcFixture) ambiguousStop(g *readbackStore, id DriverID) (gate chan struct{}, committed []byte) {
	f.t.Helper()
	committed = f.fileBytes()
	gate = make(chan struct{})
	g.readback.Store(&gate)
	f.hooks.mu.Lock()
	f.hooks.failDirSync = 1
	f.hooks.mu.Unlock()
	_, err := f.decide(MethodStop, id)
	refusalOf(f.t, err, RefusalUnavailable, 3)
	return gate, committed
}

// rejectOnReadback makes the pending readback find the committed namespace:
// the STOP did not persist.
func (f *svcFixture) rejectOnReadback(committed []byte) {
	f.t.Helper()
	if err := os.WriteFile(f.path, committed, 0o600); err != nil {
		f.t.Fatal(err)
	}
}

func (f *svcFixture) waitDecisionSettled(id DriverID) {
	f.t.Helper()
	d := f.svc.drivers[id]
	waitFor(f.t, "held decision settled", func() bool {
		d.mu.Lock()
		defer d.mu.Unlock()
		return f.ms.AdmissionOpen() && d.ambiguous == nil && d.deciding == 0
	})
}

func driverEvents(f *svcFixture, cursor uint64, id DriverID) []Event {
	var out []Event
	for _, e := range f.svc.EventsSince(cursor).Events {
		if e.DriverID == id {
			out = append(out, e)
		}
	}
	return out
}

func countOutcome(f *svcFixture, id DriverID, outcome string) int {
	n := 0
	for _, d := range f.svc.Diagnostics(id) {
		if strings.Contains(d.Summary, "outcome="+outcome) {
			n++
		}
	}
	return n
}

// assertAcceptedThenCanceled checks the adopted outcome: exactly one
// DESIRED_ACCEPTED for the STOP, published before every observation the
// cancellation produced, and exactly one cancellation.
func assertAcceptedThenCanceled(t *testing.T, f *svcFixture, cursor uint64, id DriverID, stopsBefore int) {
	t.Helper()
	events := driverEvents(f, cursor, id)
	final := f.status(id)
	accepted, firstObserved := -1, -1
	for i, e := range events {
		switch {
		case e.Kind == EventDesiredAccepted:
			if accepted >= 0 || e.DesiredState != DesiredStopped || e.DesiredRevision != final.DesiredRevision {
				t.Fatalf("acceptance events %+v, want exactly one for the STOP", events)
			}
			accepted = i
		case e.Kind == EventObservedChanged && firstObserved < 0:
			firstObserved = i
		}
		if e.Kind == EventObservedChanged && e.ObservedState == drivermanager.ObservedRunning {
			t.Fatalf("superseded START was admitted: %+v", events)
		}
	}
	if accepted < 0 || firstObserved < 0 || firstObserved < accepted {
		t.Fatalf("events %+v: acceptance must precede the cancellation", events)
	}
	if n := f.execs[id].count("stop") - stopsBefore; n != 1 {
		t.Fatalf("cancellation ran Stop %d times, want 1", n)
	}
	if n := countOutcome(f, id, "CANCELED"); n != 1 {
		t.Fatalf("CANCELED diagnostics = %d, want 1", n)
	}
}

// TestServiceRejectedSupersedingStopResumesTypedStart: a STOP behind an
// executing typed START whose commit is ambiguous does not cancel the START
// while unresolved. When the readback finds the committed namespace (the STOP
// did not persist), the START resumes with its attempt, budget and deadline
// unchanged and completes; no Stop runs and no event names the STOP.
func TestServiceRejectedSupersedingStopResumesTypedStart(t *testing.T) {
	id := DriverID("fam.alpha")
	f, g := gatedFixture(t, string(id))
	h := f.typedStartInFlight(id)
	starts, stops := f.execs[id].count("start"), f.execs[id].count("stop")
	slot := *f.record(id).Current
	before := f.status(id)
	cursor := f.svc.List().EventCursor
	gate, committed := f.ambiguousStop(g, id)
	f.rejectOnReadback(committed)
	ctx, corr := h.attempt()
	if ctx.Err() != nil {
		t.Fatal("START canceled while its superseding STOP was unresolved")
	}
	close(gate)
	f.waitDecisionSettled(id)
	if rec := f.record(id); rec.Reserved != nil || rec.Current == nil || *rec.Current != slot {
		t.Fatalf("START slot after the rejected STOP = %+v, want unchanged %+v", rec.Current, slot)
	}
	if ctx.Err() != nil {
		t.Fatal("rejected STOP canceled the START")
	}
	h.result <- nil
	f.waitSettled(id, drivermanager.ObservedRunning)
	st := f.status(id)
	if st.DesiredState != DesiredRunning || st.DesiredRevision != before.DesiredRevision {
		t.Fatalf("desired after rejection = %s@%d, want %s@%d", st.DesiredState, st.DesiredRevision, before.DesiredState, before.DesiredRevision)
	}
	if n := f.execs[id].count("start"); n != starts {
		t.Fatalf("START made %d further attempts after the rejection", n-starts)
	}
	if n := f.execs[id].count("stop"); n != stops {
		t.Fatalf("rejected STOP ran Stop %d times", n-stops)
	}
	d := f.svc.drivers[id]
	d.mu.Lock()
	live := d.live
	d.mu.Unlock()
	if live == nil || live.corr != corr {
		t.Fatalf("admitted generation %+v, want the resumed attempt %+v", live, corr)
	}
	for _, e := range driverEvents(f, cursor, id) {
		if e.Kind == EventDesiredAccepted {
			t.Fatalf("rejected STOP left an event: %+v", e)
		}
	}
	if n := countOutcome(f, id, "CANCELED"); n != 0 {
		t.Fatalf("rejected STOP canceled the START (%d diagnostics)", n)
	}
}

// TestServiceAdoptedSupersedingStopAcceptsBeforeCanceling: when the readback
// adopts an ambiguous STOP behind an executing typed START, its
// DESIRED_ACCEPTED is published at adoption (never earlier, never twice) and
// only then is the START canceled, exactly once. A decision on another driver
// moved the namespace mark past this driver's revision, so adoption is read
// from the committed record, not from expected+1.
func TestServiceAdoptedSupersedingStopAcceptsBeforeCanceling(t *testing.T) {
	alpha, beta := DriverID("fam.alpha"), DriverID("fam.beta")
	f, g := gatedFixture(t, string(alpha), string(beta))
	f.waitSettled(alpha, drivermanager.ObservedRunning)
	h := f.typedStartInFlight(beta)
	f.mustDecide(MethodStop, alpha)
	f.waitSettled(alpha, drivermanager.ObservedStopped)
	f.quiesce()
	before := f.status(beta)
	stops := f.execs[beta].count("stop")
	cursor := f.svc.List().EventCursor
	gate, _ := f.ambiguousStop(g, beta)
	time.Sleep(50 * time.Millisecond)
	if ctx, _ := h.attempt(); ctx.Err() != nil {
		t.Fatal("START canceled before the STOP was adopted")
	}
	if n := f.execs[beta].count("stop"); n != stops {
		t.Fatal("cancellation ran before the STOP was adopted")
	}
	if events := driverEvents(f, cursor, beta); len(events) != 0 {
		t.Fatalf("events before adoption: %+v", events)
	}
	close(gate)
	f.waitSettled(beta, drivermanager.ObservedStopped)
	if final := f.status(beta); final.DesiredRevision <= before.DesiredRevision+1 {
		t.Fatalf("test premise: adopted revision %d should skip past %d+1", final.DesiredRevision, before.DesiredRevision)
	}
	assertAcceptedThenCanceled(t, f, cursor, beta, stops)
	if ctx, _ := h.attempt(); ctx.Err() == nil {
		t.Fatal("adopted STOP left the START attempt running")
	}
}

// TestServiceReadbackOutcomeRacesStartExecution: the typed START's Start
// succeeds while the superseding STOP is still unresolved. The success is not
// admitted while the decision is held; the readback outcome then decides:
// adopted cancels the START (acceptance first, one cancellation, never
// RUNNING), rejected admits the same attempt.
func TestServiceReadbackOutcomeRacesStartExecution(t *testing.T) {
	for _, adopted := range []bool{true, false} {
		name := map[bool]string{true: "adopted", false: "rejected"}[adopted]
		t.Run(name, func(t *testing.T) {
			id := DriverID("fam.alpha")
			f, g := gatedFixture(t, string(id))
			h := f.typedStartInFlight(id)
			starts, stops := f.execs[id].count("start"), f.execs[id].count("stop")
			cursor := f.svc.List().EventCursor
			gate, committed := f.ambiguousStop(g, id)
			h.result <- nil
			time.Sleep(50 * time.Millisecond)
			d := f.svc.drivers[id]
			d.mu.Lock()
			live := d.live
			d.mu.Unlock()
			if live != nil || f.status(id).ObservedState == drivermanager.ObservedRunning {
				t.Fatal("START admitted while its superseding STOP was unresolved")
			}
			if !adopted {
				f.rejectOnReadback(committed)
			}
			close(gate)
			if adopted {
				f.waitSettled(id, drivermanager.ObservedStopped)
				assertAcceptedThenCanceled(t, f, cursor, id, stops)
				return
			}
			f.waitSettled(id, drivermanager.ObservedRunning)
			_, corr := h.attempt()
			d.mu.Lock()
			live = d.live
			d.mu.Unlock()
			if live == nil || live.corr != corr || f.execs[id].count("start") != starts || f.execs[id].count("stop") != stops {
				t.Fatalf("rejected STOP: live=%+v corr=%+v starts+%d stops+%d", live, corr,
					f.execs[id].count("start")-starts, f.execs[id].count("stop")-stops)
			}
			for _, e := range driverEvents(f, cursor, id) {
				if e.Kind == EventDesiredAccepted {
					t.Fatalf("rejected STOP left an event: %+v", e)
				}
			}
		})
	}
}

// TestServiceCommittedSupersedingStopHoldsCancelUntilAccepted: a STOP behind
// an executing typed START is committed, but Decide has not yet returned from
// the store. The START's Start returns meanwhile and the reserved slot is
// visible; the reconciler must not cancel (no Stop, no observation) until
// the acceptance is published.
func TestServiceCommittedSupersedingStopHoldsCancelUntilAccepted(t *testing.T) {
	id := DriverID("fam.alpha")
	f, g := gatedFixture(t, string(id))
	h := f.typedStartInFlight(id)
	stops := f.execs[id].count("stop")
	cursor := f.svc.List().EventCursor
	committedCh, release := make(chan struct{}), make(chan struct{})
	hook := func() {
		close(committedCh)
		<-release
	}
	g.afterDecide.Store(&hook)
	done := make(chan error, 1)
	go func() {
		_, err := f.decide(MethodStop, id)
		done <- err
	}()
	<-committedCh
	if rec := f.record(id); rec.Reserved == nil {
		t.Fatal("test premise: the STOP should be committed into the reserved slot")
	}
	h.result <- errors.New("start failed")
	time.Sleep(100 * time.Millisecond)
	if n := f.execs[id].count("stop"); n != stops {
		t.Fatal("cancellation ran before the acceptance was published")
	}
	if events := driverEvents(f, cursor, id); len(events) != 0 {
		t.Fatalf("events before the acceptance: %+v", events)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("STOP: %v", err)
	}
	f.waitSettled(id, drivermanager.ObservedStopped)
	assertAcceptedThenCanceled(t, f, cursor, id, stops)
}
