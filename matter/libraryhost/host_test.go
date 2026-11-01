package libraryhost

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"go/parser"
	"go/token"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/matter"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/evse"
)

var (
	aa17Selection   = Selection{Tag: "tag-a", Target: "target-a", Profile: "profile-a", Evidence: "evidence-a"}
	aa17Generations = Generations{Source: 3, Binding: 5, NativeDriver: 7}
)

// aa17Rig counts every port call, so zero-allocation claims read a real count.
type aa17Rig struct {
	mu            sync.Mutex
	generations   Generations
	captureErr    error
	captureHook   func(context.Context) error
	capture       Capture
	captures      int
	qualifies     int
	requests      []Request
	rowsErr       error
	mutateRows    func([]Row) []Row
	class         BindingClass
	classCalls    int
	news          int
	newErr        error
	newNil        bool
	newPartial    bool
	onNew         func(*Session)
	sessions      []*Session
	closes        int
	closeErr      error
	closeHook     func(context.Context) error
	tokens        int
	tokenOverride func() (string, error)
}

func aa17NewRig() *aa17Rig {
	return &aa17Rig{generations: aa17Generations, class: ClassSyntheticFixture}
}

func (r *aa17Rig) Capture(ctx context.Context) (Capture, error) {
	r.mu.Lock()
	r.captures++
	hook, err, out := r.captureHook, r.captureErr, r.capture
	out.Generations = r.generations
	r.mu.Unlock()
	if hook != nil {
		if hookErr := hook(ctx); hookErr != nil {
			return Capture{}, hookErr
		}
	}
	return out, err
}

func (r *aa17Rig) Qualify(_ context.Context, request Request) ([]Row, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.qualifies++
	r.requests = append(r.requests, request)
	if r.rowsErr != nil {
		return nil, r.rowsErr
	}
	rows := make([]Row, 0, len(requiredRows))
	for _, kind := range []RowKind{RowTag, RowBuild, RowRuntime, RowProfile, RowAdmission} {
		rows = append(rows, Row{Kind: kind, Verdict: VerdictPass, Origin: r.class, Selection: request.Selection, Generations: request.Generations, Token: request.Token})
	}
	if r.mutateRows != nil {
		rows = r.mutateRows(rows)
	}
	return rows, nil
}

func (r *aa17Rig) Class() BindingClass {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.classCalls++
	return r.class
}

func (r *aa17Rig) New(_ context.Context, session *Session) (Binding, error) {
	r.mu.Lock()
	r.news++
	r.sessions = append(r.sessions, session)
	onNew, err, returnNil, partial := r.onNew, r.newErr, r.newNil, r.newPartial
	r.mu.Unlock()
	if onNew != nil {
		onNew(session)
	}
	if partial && err != nil {
		return r, err
	}
	if err != nil || returnNil {
		return nil, err
	}
	return r, nil
}

func (r *aa17Rig) Close(ctx context.Context) error {
	r.mu.Lock()
	r.closes++
	hook, err := r.closeHook, r.closeErr
	r.mu.Unlock()
	if hook != nil {
		return hook(ctx)
	}
	return err
}

func (r *aa17Rig) token() (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.tokenOverride != nil {
		return r.tokenOverride()
	}
	r.tokens++
	return "token-" + strconv.Itoa(r.tokens), nil
}

func (r *aa17Rig) counts() (captures, qualifies, classCalls, news, closes int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.captures, r.qualifies, r.classCalls, r.news, r.closes
}

func (r *aa17Rig) session(i int) *Session {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.sessions[i]
}

func (r *aa17Rig) config() Config {
	return Config{Enabled: true, Selection: aa17Selection, Factory: r, Qualifier: r, Snapshots: r, NewToken: r.token}
}

func aa17RequireReason(t *testing.T, err error, want Reason) {
	t.Helper()
	var unavailable *UnavailableError
	if !errors.As(err, &unavailable) || !errors.Is(err, ErrUnavailable) || unavailable.Reason != want {
		t.Fatalf("start error = %v, want unavailable reason %q", err, want)
	}
}

func aa17RequireReleased(t *testing.T, host *Host) {
	t.Helper()
	status := host.Status()
	if status.OwnedWork != 0 || status.Resources != 0 {
		t.Fatalf("owned work or resources remain: %+v", status)
	}
}

// aa17WaitState spins without a timer: the state change is the synchronization.
func aa17WaitState(host *Host, want State) {
	for host.Status().State != want {
		runtime.Gosched()
	}
}

func TestAA17ZeroConfigAndDisabledAllocateNothing(t *testing.T) {
	if (Config{}).Enabled {
		t.Fatal("option default must be false")
	}
	rig := aa17NewRig()
	cfg := rig.config()
	cfg.Enabled = false
	host := New(cfg)
	ctx := context.Background()
	for _, step := range []func(context.Context) error{host.Start, host.Restart, host.Stop} {
		if err := step(ctx); err != nil {
			t.Fatalf("disabled lifecycle step: %v", err)
		}
	}
	if c, q, k, n, cl := rig.counts(); c+q+k+n+cl != 0 || rig.tokens != 0 {
		t.Fatalf("disabled touched ports: capture=%d qualify=%d class=%d new=%d close=%d tokens=%d", c, q, k, n, cl, rig.tokens)
	}
	if status := host.Status(); status.State != StateDisabled || status.Reason != "" {
		t.Fatalf("status=%+v", status)
	}

	// Flag is the only difference from a positive control that does launch.
	control := aa17NewRig()
	enabled := New(control.config())
	if err := enabled.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, _, news, _ := control.counts(); news != 1 {
		t.Fatalf("positive control news=%d", news)
	}
	if err := enabled.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestAA17MissingPortsAndSelectionStayUnavailable(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*Config)
		want   Reason
	}{
		{"factory", func(c *Config) { c.Factory = nil }, ReasonFactoryMissing},
		{"qualifier", func(c *Config) { c.Qualifier = nil }, ReasonQualifierMissing},
		{"snapshots", func(c *Config) { c.Snapshots = nil }, ReasonSnapshotMissing},
		{"tag", func(c *Config) { c.Selection.Tag = "" }, ReasonSelectionIncomplete},
		{"target", func(c *Config) { c.Selection.Target = "" }, ReasonSelectionIncomplete},
		{"profile", func(c *Config) { c.Selection.Profile = "" }, ReasonSelectionIncomplete},
		{"evidence", func(c *Config) { c.Selection.Evidence = "" }, ReasonSelectionIncomplete},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rig := aa17NewRig()
			cfg := rig.config()
			tc.mutate(&cfg)
			host := New(cfg)
			aa17RequireReason(t, host.Start(context.Background()), tc.want)
			if _, _, classCalls, news, _ := rig.counts(); news != 0 || classCalls != 0 {
				t.Fatalf("factory touched: class=%d new=%d", classCalls, news)
			}
			status := host.Status()
			if status.State != StateUnavailable || status.Reason != tc.want {
				t.Fatalf("status=%+v", status)
			}
		})
	}
}

func TestAA17EveryRowAndIdentityMustPassBeforeLaunch(t *testing.T) {
	onEach := func(kind RowKind, change func(*Row)) func([]Row) []Row {
		return func(rows []Row) []Row {
			for i := range rows {
				if rows[i].Kind == kind {
					change(&rows[i])
				}
			}
			return rows
		}
	}
	type rowCase struct {
		name   string
		mutate func([]Row) []Row
		want   Reason
	}
	cases := []rowCase{
		{"no-rows", func([]Row) []Row { return nil }, ReasonEvidenceAbsent},
		{"duplicate-row", func(rows []Row) []Row { return append(rows, rows[0]) }, ReasonEvidenceAmbiguous},
		{"extra-row", func(rows []Row) []Row {
			extra := rows[0]
			extra.Kind = "extra"
			return append(rows, extra)
		}, ReasonEvidenceAmbiguous},
	}
	for _, kind := range requiredRows {
		kind := kind
		cases = append(cases,
			rowCase{"missing-" + string(kind), func(rows []Row) []Row {
				kept := rows[:0]
				for _, row := range rows {
					if row.Kind != kind {
						kept = append(kept, row)
					}
				}
				return kept
			}, ReasonEvidenceIncomplete},
			rowCase{"failed-" + string(kind), onEach(kind, func(r *Row) { r.Verdict = VerdictFail }), ReasonEvidenceNotPassed},
			rowCase{"unknown-" + string(kind), onEach(kind, func(r *Row) { r.Verdict = VerdictUnknown }), ReasonEvidenceNotPassed},
			rowCase{"zero-verdict-" + string(kind), onEach(kind, func(r *Row) { r.Verdict = "" }), ReasonEvidenceNotPassed},
			rowCase{"selected-tag-" + string(kind), onEach(kind, func(r *Row) { r.Selection.Tag = "other" }), ReasonSelectionMismatch},
			rowCase{"selected-target-" + string(kind), onEach(kind, func(r *Row) { r.Selection.Target = "other" }), ReasonSelectionMismatch},
			rowCase{"selected-profile-" + string(kind), onEach(kind, func(r *Row) { r.Selection.Profile = "other" }), ReasonSelectionMismatch},
			rowCase{"selected-evidence-" + string(kind), onEach(kind, func(r *Row) { r.Selection.Evidence = "other" }), ReasonSelectionMismatch},
			rowCase{"source-generation-" + string(kind), onEach(kind, func(r *Row) { r.Generations.Source++ }), ReasonGenerationMismatch},
			rowCase{"binding-generation-" + string(kind), onEach(kind, func(r *Row) { r.Generations.Binding++ }), ReasonGenerationMismatch},
			rowCase{"driver-generation-" + string(kind), onEach(kind, func(r *Row) { r.Generations.NativeDriver++ }), ReasonGenerationMismatch},
			rowCase{"token-" + string(kind), onEach(kind, func(r *Row) { r.Token += "-old" }), ReasonTokenMismatch},
			rowCase{"origin-" + string(kind), onEach(kind, func(r *Row) { r.Origin = ClassProduction }), ReasonOriginMismatch},
		)
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := aa17NewRig()
			rig.mutateRows = tc.mutate
			host := New(rig.config())
			aa17RequireReason(t, host.Start(context.Background()), tc.want)
			if _, _, _, news, _ := rig.counts(); news != 0 {
				t.Fatalf("launched before all rows passed: news=%d", news)
			}
			if status := host.Status(); status.State != StateUnavailable || status.Epoch != 0 {
				t.Fatalf("status=%+v", status)
			}
			aa17RequireReleased(t, host)
		})
	}
}

func TestAA17RowsAreBoundToTheExactRequest(t *testing.T) {
	rig := aa17NewRig()
	host := New(rig.config())
	ctx := context.Background()
	if err := host.Start(ctx); err != nil {
		t.Fatal(err)
	}
	want := Request{Selection: aa17Selection, Generations: aa17Generations, Token: "token-1"}
	if !reflect.DeepEqual(rig.requests, []Request{want}) {
		t.Fatalf("requests=%+v", rig.requests)
	}
	wantStamp := Stamp{Selection: aa17Selection, Generations: aa17Generations, Epoch: 1, Token: "token-1"}
	if got := rig.session(0).Stamp(); got != wantStamp {
		t.Fatalf("stamp=%+v", got)
	}
	if err := host.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestAA17PortFailuresAndClassBoundaryStayUnavailable(t *testing.T) {
	failure := errors.New("synthetic port failure")
	for _, tc := range []struct {
		name   string
		mutate func(*aa17Rig)
		want   Reason
	}{
		{"snapshot", func(r *aa17Rig) { r.captureErr = failure }, ReasonSnapshotFailed},
		{"source-generation-zero", func(r *aa17Rig) { r.generations.Source = 0 }, ReasonGenerationsInvalid},
		{"binding-generation-zero", func(r *aa17Rig) { r.generations.Binding = 0 }, ReasonGenerationsInvalid},
		{"driver-generation-zero", func(r *aa17Rig) { r.generations.NativeDriver = 0 }, ReasonGenerationsInvalid},
		{"qualifier", func(r *aa17Rig) { r.rowsErr = failure }, ReasonQualifierFailed},
		{"token-error", func(r *aa17Rig) { r.tokenOverride = func() (string, error) { return "", failure } }, ReasonTokenFailed},
		{"token-empty", func(r *aa17Rig) { r.tokenOverride = func() (string, error) { return "", nil } }, ReasonTokenFailed},
		{"unknown-class", func(r *aa17Rig) { r.class = "other" }, ReasonOriginMismatch},
		{"fixture-evidence-production-binding", func(r *aa17Rig) {
			r.class = ClassProduction
			r.mutateRows = func(rows []Row) []Row {
				for i := range rows {
					rows[i].Origin = ClassSyntheticFixture
				}
				return rows
			}
		}, ReasonOriginMismatch},
		{"production-evidence-fixture-binding", func(r *aa17Rig) {
			r.mutateRows = func(rows []Row) []Row {
				for i := range rows {
					rows[i].Origin = ClassProduction
				}
				return rows
			}
		}, ReasonOriginMismatch},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rig := aa17NewRig()
			tc.mutate(rig)
			host := New(rig.config())
			aa17RequireReason(t, host.Start(context.Background()), tc.want)
			if _, _, _, news, _ := rig.counts(); news != 0 {
				t.Fatalf("news=%d", news)
			}
		})
	}

	t.Run("cancelled-before-launch", func(t *testing.T) {
		rig := aa17NewRig()
		ctx, cancel := context.WithCancel(context.Background())
		rig.captureHook = func(context.Context) error { cancel(); return nil }
		host := New(rig.config())
		aa17RequireReason(t, host.Start(ctx), ReasonCancelled)
		if _, _, _, news, _ := rig.counts(); news != 0 {
			t.Fatalf("news=%d", news)
		}
	})
}

func TestAA17FactoryFailureDrainsItsTasks(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*aa17Rig)
	}{
		{"error", func(r *aa17Rig) { r.newErr = errors.New("synthetic factory failure") }},
		{"nil-binding", func(r *aa17Rig) { r.newNil = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rig := aa17NewRig()
			tc.mutate(rig)
			joined := make(chan struct{})
			rig.onNew = func(session *Session) {
				if err := session.Go(func(ctx context.Context) {
					<-ctx.Done()
					close(joined)
				}); err != nil {
					t.Error(err)
				}
			}
			host := New(rig.config())
			aa17RequireReason(t, host.Start(context.Background()), ReasonFactoryFailed)
			select {
			case <-joined:
			default:
				t.Fatal("factory task was not joined")
			}
			aa17RequireReleased(t, host)
			if err := rig.session(0).Go(func(context.Context) { t.Error("task ran") }); !errors.Is(err, ErrStaleCallback) {
				t.Fatalf("late task err=%v", err)
			}
		})
	}
}

func TestAA17CallbackStampVariesEveryFieldIndependently(t *testing.T) {
	rig := aa17NewRig()
	host := New(rig.config())
	ctx := context.Background()
	if err := host.Start(ctx); err != nil {
		t.Fatal(err)
	}
	session := rig.session(0)
	exact := session.Stamp()
	ran := 0
	callback := func(context.Context) error { ran++; return nil }
	if err := session.Deliver(exact, callback); err != nil || ran != 1 {
		t.Fatalf("positive control err=%v ran=%d", err, ran)
	}
	for _, tc := range []struct {
		name   string
		change func(*Stamp)
	}{
		{"tag", func(s *Stamp) { s.Selection.Tag = "other" }},
		{"target", func(s *Stamp) { s.Selection.Target = "other" }},
		{"profile", func(s *Stamp) { s.Selection.Profile = "other" }},
		{"evidence", func(s *Stamp) { s.Selection.Evidence = "other" }},
		{"source", func(s *Stamp) { s.Generations.Source++ }},
		{"binding", func(s *Stamp) { s.Generations.Binding++ }},
		{"driver", func(s *Stamp) { s.Generations.NativeDriver++ }},
		{"epoch", func(s *Stamp) { s.Epoch++ }},
		{"token", func(s *Stamp) { s.Token += "-old" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stamp := exact
			tc.change(&stamp)
			before := host.Status().StaleRejected
			if err := session.Deliver(stamp, callback); !errors.Is(err, ErrStaleCallback) {
				t.Fatalf("err=%v", err)
			}
			if ran != 1 {
				t.Fatalf("stale callback ran: %d", ran)
			}
			status := host.Status()
			if status.StaleRejected != before+1 || status.OwnedWork != 0 {
				t.Fatalf("stale bookkeeping: %+v", status)
			}
		})
	}
	if err := host.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestAA17RestartRejectsEveryOldRoute(t *testing.T) {
	rig := aa17NewRig()
	aa17InstallProjection(t, rig)
	host := New(rig.config())
	ctx := context.Background()
	if err := host.Start(ctx); err != nil {
		t.Fatal(err)
	}
	old := rig.session(0)
	oldStamp := old.Stamp()
	if err := host.Restart(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, _, news, closes := rig.counts(); news != 2 || closes != 1 {
		t.Fatalf("restart news=%d closes=%d", news, closes)
	}
	fresh := rig.session(1)
	freshStamp := fresh.Stamp()
	if freshStamp.Epoch != oldStamp.Epoch+1 || freshStamp.Token == oldStamp.Token {
		t.Fatalf("restart reused identity: old=%+v new=%+v", oldStamp, freshStamp)
	}
	ran := 0
	callback := func(context.Context) error { ran++; return nil }
	for _, tc := range []struct {
		name    string
		session *Session
		stamp   Stamp
	}{
		{"old-session-old-stamp", old, oldStamp},
		{"old-session-new-stamp", old, freshStamp},
		{"new-session-old-stamp", fresh, oldStamp},
	} {
		if err := tc.session.Deliver(tc.stamp, callback); !errors.Is(err, ErrStaleCallback) {
			t.Fatalf("%s err=%v", tc.name, err)
		}
	}
	if err := old.Go(func(context.Context) { ran++ }); !errors.Is(err, ErrStaleCallback) {
		t.Fatalf("old task err=%v", err)
	}
	if _, err := old.Project(ctx); !errors.Is(err, ErrStaleCallback) {
		t.Fatalf("old projection err=%v", err)
	}
	if ran != 0 {
		t.Fatalf("stale work ran %d times", ran)
	}
	if err := fresh.Deliver(freshStamp, callback); err != nil || ran != 1 {
		t.Fatalf("positive control err=%v ran=%d", err, ran)
	}
	if _, err := fresh.Project(ctx); err != nil {
		t.Fatalf("positive projection: %v", err)
	}
	if err := host.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestAA17RestartOfDisabledHostStaysDisabled(t *testing.T) {
	rig := aa17NewRig()
	cfg := rig.config()
	cfg.Enabled = false
	host := New(cfg)
	if err := host.Restart(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, _, _, news, _ := rig.counts(); news != 0 {
		t.Fatalf("news=%d", news)
	}
}

func TestAA17ReusedTokenIsRefused(t *testing.T) {
	rig := aa17NewRig()
	rig.tokenOverride = func() (string, error) { return "same", nil }
	host := New(rig.config())
	ctx := context.Background()
	if err := host.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := host.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	aa17RequireReason(t, host.Start(ctx), ReasonTokenReused)
	if _, _, _, news, _ := rig.counts(); news != 1 {
		t.Fatalf("news=%d", news)
	}
}

func TestAA17DuplicateStartCannotAllocateASecondBinding(t *testing.T) {
	rig := aa17NewRig()
	host := New(rig.config())
	ctx := context.Background()
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- host.Start(ctx)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, _, _, news, _ := rig.counts(); news != 1 {
		t.Fatalf("news=%d", news)
	}
	if err := host.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if err := host.Stop(ctx); err != nil {
		t.Fatalf("repeated stop: %v", err)
	}
	if _, _, _, _, closes := rig.counts(); closes != 1 {
		t.Fatalf("closes=%d", closes)
	}
}

func TestAA17StopFencesDrainsThenReleases(t *testing.T) {
	rig := aa17NewRig()
	host := New(rig.config())
	ctx := context.Background()
	if err := host.Start(ctx); err != nil {
		t.Fatal(err)
	}
	session := rig.session(0)
	stamp := session.Stamp()
	entered, finish := make(chan struct{}), make(chan struct{})
	deliverDone := make(chan error, 1)
	go func() {
		deliverDone <- session.Deliver(stamp, func(context.Context) error {
			close(entered)
			<-finish
			return nil
		})
	}()
	<-entered
	stopDone := make(chan error, 1)
	go func() { stopDone <- host.Stop(ctx) }()
	aa17WaitState(host, StateStopping)

	// Fenced for new work while the admitted callback still holds the release.
	if err := session.Deliver(stamp, func(context.Context) error { t.Error("admitted during stop"); return nil }); !errors.Is(err, ErrStaleCallback) {
		t.Fatalf("err=%v", err)
	}
	if status := host.Status(); status.OwnedWork != 1 || status.Resources != 1 {
		t.Fatalf("drain status=%+v", status)
	}
	if _, _, _, _, closes := rig.counts(); closes != 0 {
		t.Fatalf("released before drain: closes=%d", closes)
	}
	select {
	case err := <-stopDone:
		t.Fatalf("stop returned before drain: %v", err)
	default:
	}
	close(finish)
	if err := <-deliverDone; err != nil {
		t.Fatal(err)
	}
	if err := <-stopDone; err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, closes := rig.counts(); closes != 1 {
		t.Fatalf("closes=%d", closes)
	}
	aa17RequireReleased(t, host)
	if state := host.Status().State; state != StateStopped {
		t.Fatalf("state=%s", state)
	}
}

func TestAA17UnquiescedTaskQuarantinesAndBlocksRestart(t *testing.T) {
	rig := aa17NewRig()
	host := New(rig.config())
	ctx := context.Background()
	if err := host.Start(ctx); err != nil {
		t.Fatal(err)
	}
	session := rig.session(0)
	stamp := session.Stamp()
	release, joined := make(chan struct{}), make(chan struct{})
	if err := session.Go(func(context.Context) {
		<-release
		close(joined)
	}); err != nil {
		t.Fatal(err)
	}
	stopCtx, stopCancel := context.WithCancel(ctx)
	stopDone := make(chan error, 1)
	go func() { stopDone <- host.Stop(stopCtx) }()
	aa17WaitState(host, StateStopping)
	stopCancel()
	if err := <-stopDone; !errors.Is(err, ErrQuiesce) || !errors.Is(err, context.Canceled) {
		t.Fatalf("stop err=%v", err)
	}
	status := host.Status()
	if status.State != StateQuarantined || status.OwnedWork != 1 || status.Resources != 1 {
		t.Fatalf("status=%+v", status)
	}
	if _, _, _, _, closes := rig.counts(); closes != 0 {
		t.Fatalf("released undrained generation: closes=%d", closes)
	}
	if err := host.Start(ctx); !errors.Is(err, ErrQuarantined) {
		t.Fatalf("start err=%v", err)
	}
	blockedRestart, restartCancel := context.WithCancel(ctx)
	restartDone := make(chan error, 1)
	go func() { restartDone <- host.Restart(blockedRestart) }()
	aa17WaitState(host, StateStopping)
	restartCancel()
	if err := <-restartDone; !errors.Is(err, ErrQuiesce) {
		t.Fatalf("restart err=%v", err)
	}
	if _, _, _, news, _ := rig.counts(); news != 1 {
		t.Fatalf("quarantine allowed allocation: news=%d", news)
	}
	if err := session.Deliver(stamp, func(context.Context) error { t.Error("ran"); return nil }); !errors.Is(err, ErrStaleCallback) {
		t.Fatalf("deliver err=%v", err)
	}

	close(release)
	<-joined
	if err := host.Stop(ctx); err != nil {
		t.Fatalf("resumed stop: %v", err)
	}
	aa17RequireReleased(t, host)
	if err := host.Start(ctx); err != nil {
		t.Fatalf("start after quiesce: %v", err)
	}
	if got := rig.session(1).Stamp(); got.Epoch != stamp.Epoch+1 || got.Token == stamp.Token {
		t.Fatalf("stamp=%+v old=%+v", got, stamp)
	}
	if err := host.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestAA17CloseFailureAndHangStayQuarantined(t *testing.T) {
	t.Run("error-then-retry", func(t *testing.T) {
		rig := aa17NewRig()
		failure := errors.New("synthetic close failure")
		rig.closeErr = failure
		host := New(rig.config())
		ctx := context.Background()
		if err := host.Start(ctx); err != nil {
			t.Fatal(err)
		}
		if err := host.Stop(ctx); !errors.Is(err, ErrQuiesce) || !errors.Is(err, failure) {
			t.Fatalf("err=%v", err)
		}
		if status := host.Status(); status.State != StateQuarantined || status.Resources != 1 {
			t.Fatalf("status=%+v", status)
		}
		rig.mu.Lock()
		rig.closeErr = nil
		rig.mu.Unlock()
		if err := host.Stop(ctx); err != nil {
			t.Fatal(err)
		}
		aa17RequireReleased(t, host)
	})
	t.Run("hang-is-bounded-and-not-reinvoked", func(t *testing.T) {
		rig := aa17NewRig()
		unblock := make(chan struct{})
		rig.closeHook = func(context.Context) error { <-unblock; return nil }
		host := New(rig.config())
		ctx := context.Background()
		if err := host.Start(ctx); err != nil {
			t.Fatal(err)
		}
		budget, cancel := context.WithCancel(ctx)
		stopDone := make(chan error, 1)
		go func() { stopDone <- host.Stop(budget) }()
		for {
			if _, _, _, _, closes := rig.counts(); closes == 1 {
				break
			}
			runtime.Gosched()
		}
		cancel()
		if err := <-stopDone; !errors.Is(err, ErrQuiesce) {
			t.Fatalf("err=%v", err)
		}
		close(unblock)
		if err := host.Stop(ctx); err != nil {
			t.Fatal(err)
		}
		if _, _, _, _, closes := rig.counts(); closes != 1 {
			t.Fatalf("closes=%d", closes)
		}
		aa17RequireReleased(t, host)
	})
}

func TestAA17ThreeCyclesLeaveNothingOwned(t *testing.T) {
	rig := aa17NewRig()
	aa17InstallProjection(t, rig)
	joined := 0
	var mu sync.Mutex
	rig.onNew = func(session *Session) {
		if err := session.Go(func(ctx context.Context) {
			<-ctx.Done()
			mu.Lock()
			joined++
			mu.Unlock()
		}); err != nil {
			t.Error(err)
		}
	}
	host := New(rig.config())
	ctx := context.Background()
	seen := map[string]struct{}{}
	for cycle := 1; cycle <= 3; cycle++ {
		if err := host.Start(ctx); err != nil {
			t.Fatal(err)
		}
		session := rig.session(cycle - 1)
		if _, err := session.Project(ctx); err != nil {
			t.Fatal(err)
		}
		if status := host.Status(); status.State != StateRunning || status.Resources != 1 || status.OwnedWork != 1 || status.Class != ClassSyntheticFixture {
			t.Fatalf("cycle %d running status=%+v", cycle, status)
		}
		seen[session.Stamp().Token] = struct{}{}
		if got := session.Stamp().Epoch; got != uint64(cycle) {
			t.Fatalf("epoch=%d", got)
		}
		if err := host.Stop(ctx); err != nil {
			t.Fatal(err)
		}
		aa17RequireReleased(t, host)
		mu.Lock()
		got := joined
		mu.Unlock()
		if got != cycle {
			t.Fatalf("joined=%d want %d", got, cycle)
		}
		if _, _, _, news, closes := rig.counts(); news != cycle || closes != cycle {
			t.Fatalf("news=%d closes=%d", news, closes)
		}
	}
	if len(seen) != 3 {
		t.Fatalf("tokens=%v", seen)
	}

	absent := aa17NewRig()
	absent.mutateRows = func([]Row) []Row { return nil }
	unavailable := New(absent.config())
	for cycle := 0; cycle < 3; cycle++ {
		aa17RequireReason(t, unavailable.Start(ctx), ReasonEvidenceAbsent)
		if err := unavailable.Stop(ctx); err != nil {
			t.Fatal(err)
		}
		aa17RequireReleased(t, unavailable)
	}
	if _, _, _, news, closes := absent.counts(); news != 0 || closes != 0 {
		t.Fatalf("accidental SDK admission: news=%d closes=%d", news, closes)
	}
}

func TestAA17ProjectUsesTheAA03Projector(t *testing.T) {
	rig := aa17NewRig()
	aa17InstallProjection(t, rig)
	want, err := matter.Project161(rig.capture.Snapshot, rig.capture.Evaluation)
	if err != nil {
		t.Fatal(err)
	}
	if len(want.Document.Attributes) != 1 || want.Document.Attributes[0].Value.Milliampere != "3200" {
		t.Fatalf("control attributes=%+v", want.Document.Attributes)
	}
	host := New(rig.config())
	ctx := context.Background()
	if err := host.Start(ctx); err != nil {
		t.Fatal(err)
	}
	session := rig.session(0)
	got, err := session.Project(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) || got.Document.TargetVersion != matter.TargetVersion161 {
		t.Fatalf("projection=%+v", got)
	}

	for _, tc := range []struct {
		name   string
		change func(*Generations)
	}{
		{"source", func(g *Generations) { g.Source++ }},
		{"binding", func(g *Generations) { g.Binding++ }},
		{"driver", func(g *Generations) { g.NativeDriver++ }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rig.mu.Lock()
			tc.change(&rig.generations)
			rig.mu.Unlock()
			defer func() {
				rig.mu.Lock()
				rig.generations = aa17Generations
				rig.mu.Unlock()
			}()
			if _, err := session.Project(ctx); !errors.Is(err, ErrGenerationChanged) {
				t.Fatalf("err=%v", err)
			}
		})
	}

	failure := errors.New("synthetic capture failure")
	rig.mu.Lock()
	rig.captureErr = failure
	rig.mu.Unlock()
	if _, err := session.Project(ctx); !errors.Is(err, ErrSnapshotUnavailable) || !errors.Is(err, failure) {
		t.Fatalf("capture err=%v", err)
	}
	rig.mu.Lock()
	rig.captureErr = nil
	rig.capture.Evaluation.EvaluationDigest = "sha256:invalid"
	rig.mu.Unlock()
	if _, err := session.Project(ctx); err == nil {
		t.Fatal("projector refusal was not surfaced")
	}
	if err := host.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestAA17StopCancelsAndWithholdsAnInFlightProjection(t *testing.T) {
	for _, tc := range []struct {
		name string
		exit func(context.Context) error
	}{
		{"capture-returns-a-value", func(context.Context) error { return nil }},
		{"capture-returns-its-cancellation", func(capture context.Context) error { return capture.Err() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rig := aa17NewRig()
			aa17InstallProjection(t, rig)
			host := New(rig.config())
			ctx := context.Background()
			if err := host.Start(ctx); err != nil {
				t.Fatal(err)
			}
			session := rig.session(0)
			entered := make(chan struct{})
			rig.mu.Lock()
			rig.captureHook = func(capture context.Context) error {
				close(entered)
				<-capture.Done()
				return tc.exit(capture)
			}
			rig.mu.Unlock()
			result := make(chan error, 1)
			go func() {
				_, err := session.Project(ctx)
				result <- err
			}()
			<-entered
			if err := host.Stop(ctx); err != nil {
				t.Fatal(err)
			}
			if err := <-result; !errors.Is(err, ErrStaleCallback) {
				t.Fatalf("err=%v", err)
			}
			aa17RequireReleased(t, host)
		})
	}
}

func aa17DisallowedImports(t *testing.T, source any) []string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "host.go", source, parser.ImportsOnly)
	if err != nil {
		t.Fatal(err)
	}
	allowed := map[string]bool{
		"context": true, "crypto/rand": true, "encoding/hex": true, "errors": true, "fmt": true, "sync": true, "time": true,
		"github.com/Project-Helianthus/helianthus-ebusgateway/matter": true,
		"github.com/Project-Helianthus/helianthus-semreg/semreg/v1":   true,
	}
	var disallowed []string
	for _, spec := range file.Imports {
		if path := strings.Trim(spec.Path.Value, `"`); !allowed[path] {
			disallowed = append(disallowed, path)
		}
	}
	return disallowed
}

func TestAA17PackageImportsNoSDKTransportOrVendorDecoding(t *testing.T) {
	if got := aa17DisallowedImports(t, nil); len(got) != 0 {
		t.Fatalf("unexpected imports %v", got)
	}
	for _, path := range []string{"net/http", "example.invalid/go-fabric", "example.invalid/vendor/decoder"} {
		source := "package libraryhost\nimport _ \"" + path + "\"\n"
		if got := aa17DisallowedImports(t, source); len(got) != 1 || got[0] != path {
			t.Fatalf("control %s reported %v", path, got)
		}
	}
}

func aa17InstallProjection(t *testing.T, rig *aa17Rig) {
	t.Helper()
	digest := sha256.Sum256([]byte("public synthetic AA17 current evidence"))
	evidence := semreg.EvidenceRef{Owner: "owner.test", Kind: "evidence.test", Digest: semreg.Digest(fmt.Sprintf("sha256:%x", digest)), Contract: "test.evidence/v1", Access: semreg.EvidenceAccessPublic, Redaction: semreg.RedactionNone}
	at := func(n semreg.Int64) semreg.TimePoint {
		return semreg.TimePoint{UnixNanoseconds: n, ClockID: "clock.utc", UncertaintyNS: "0"}
	}
	mono := func(n semreg.Uint64) semreg.MonotonicPoint {
		return semreg.MonotonicPoint{ClockEpochID: "clock-epoch:aa17", Nanoseconds: n}
	}
	binding := semreg.NativeBindingID("binding:aa17:1")
	epoch := semreg.SourceEpochID("epoch:aa17:1")
	generation := semreg.Uint64("1")
	source := semreg.SourceID("source:aa17:1")
	phase := "l1"
	value := semreg.Value{Kind: semreg.ValueQuantity, Quantity: &semreg.Quantity{Number: semreg.Decimal{Coefficient: "32", Exponent10: -1}, Unit: "unit.ampere"}}
	candidate := semreg.FactCandidate{
		CandidateID: "candidate:aa17:l1",
		Key: semreg.FactKey{PackID: "helianthus.pack.evse", PackVersion: "1.0.0", FactID: "evse.ac.current",
			Dimensions: []semreg.Dimension{{ID: "evse.dimension.phase", Value: semreg.Value{Kind: semreg.ValueText, Text: &phase}}}},
		Value:           &value,
		Quality:         semreg.Quality{Assertion: semreg.AssertionObserved, Qualification: semreg.QualificationQualified, Promotion: semreg.PromotionPromoted, Validity: semreg.ValidityGood, Availability: semreg.AvailabilityAvailable, Freshness: semreg.FreshnessFresh, Reasons: []semreg.DefinitionID{}},
		Times:           semreg.Times{ReceivedAt: at("100"), ReceiptMonotonic: mono("100"), EvaluatedAt: at("100"), EvaluateMonotonic: mono("100")},
		FreshnessPolicy: semreg.FreshnessPolicy{PolicyID: "policy.aa17.current", Version: "1.0.0", FreshForNS: "1000", RetainForNS: "2000", MaxWallUncertaintyNS: "0"},
		BindingID:       &binding, SourceEpochID: &epoch, DriverGeneration: &generation,
		Origin:   semreg.OriginRef{OriginID: "origin:aa17:1", Kind: semreg.OriginNativeObservation, SourceID: &source, SourceEpochID: &epoch, BindingID: &binding, Evidence: []semreg.EvidenceRef{evidence}},
		Evidence: []semreg.EvidenceRef{evidence}, Revision: "1",
	}
	batch := semreg.PublicationBatch{
		Contract: semreg.ContractKernelV1, BatchID: "batch:aa17:1", AssetID: "asset:aa17:1", SourceID: source, SourceEpochID: epoch,
		DriverGeneration: generation, Sequence: "1", ExpectedSemanticRevision: "0", ObservedAt: at("100"),
		SourceUpserts:         []semreg.SourceDescriptor{{SourceID: source, SourceEpochID: epoch, ProtocolID: "protocol.test", ProfileID: "profile.test", ProfileVersion: "1", RegistryEvidence: evidence, StartedAt: at("90"), State: semreg.SourceCurrent, Revision: "1"}},
		SourceRetirements:     []semreg.SourceEpochID{},
		BindingUpserts:        []semreg.NativeBinding{{BindingID: binding, AssetID: "asset:aa17:1", SourceID: source, SourceEpochID: epoch, DriverGeneration: generation, NativeResource: evidence, State: semreg.BindingCurrent, Revision: "1"}},
		IdentityLinkUpserts:   []semreg.IdentityLink{{AssetID: "asset:aa17:1", BindingID: binding, State: semreg.LinkQualified, Basis: []semreg.EvidenceRef{evidence}, Revision: "1"}},
		FactUpserts:           []semreg.FactCandidate{candidate},
		FactWithdrawals:       []semreg.CandidateID{},
		ServiceUpserts:        []semreg.ServiceInstance{},
		ServiceWithdrawals:    []semreg.ServiceInstanceID{},
		CapabilityUpserts:     []semreg.CapabilityInstance{},
		CapabilityWithdrawals: []semreg.CapabilityInstanceID{},
		GenerationFences:      []semreg.GenerationFence{},
	}
	var err error
	if batch.BatchDigest, err = batch.ComputedDigest(); err != nil {
		t.Fatal(err)
	}
	kernel, err := semreg.NewPublicationKernel("asset:aa17:1", evse.New())
	if err != nil {
		t.Fatal(err)
	}
	snapshot, _, err := kernel.Apply(batch, mono("100"))
	if err != nil {
		t.Fatal(err)
	}
	evaluation, err := semreg.EvaluateSnapshot(snapshot, semreg.EvaluationContext{EvaluatedAt: at("150"), EvaluateMonotonic: mono("150")})
	if err != nil {
		t.Fatal(err)
	}
	rig.capture = Capture{Snapshot: snapshot, Evaluation: evaluation}
}

func TestAA17GenerationChangeDuringQualificationIsFenced(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*Generations)
	}{
		{"source", func(g *Generations) { g.Source++ }},
		{"binding", func(g *Generations) { g.Binding++ }},
		{"native-driver", func(g *Generations) { g.NativeDriver++ }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rig := aa17NewRig()
			rig.mutateRows = func(rows []Row) []Row {
				rig.generations = aa17Generations
				tc.mutate(&rig.generations)
				return rows
			}
			host := New(rig.config())
			aa17RequireReason(t, host.Start(context.Background()), ReasonGenerationMismatch)
			if _, _, _, news, _ := rig.counts(); news != 0 {
				t.Fatalf("factory constructed %d times after a generation change", news)
			}
			aa17RequireReleased(t, host)
		})
	}
}

func TestAA17PartialConstructionIsClosedAndDrained(t *testing.T) {
	rig := aa17NewRig()
	rig.newErr = errors.New("synthetic partial construction")
	rig.newPartial = true
	host := New(rig.config())
	aa17RequireReason(t, host.Start(context.Background()), ReasonFactoryFailed)
	if _, _, _, news, closes := rig.counts(); news != 1 || closes != 1 {
		t.Fatalf("news=%d closes=%d, want 1 and 1", news, closes)
	}
	aa17RequireReleased(t, host)
	rig.newErr = nil
	rig.newPartial = false
	if err := host.Start(context.Background()); err != nil {
		t.Fatalf("restart after released partial construction: %v", err)
	}
	if err := host.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestAA17CancelOnSecondCaptureNeverConstructs(t *testing.T) {
	rig := aa17NewRig()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rig.captureHook = func(context.Context) error {
		rig.mu.Lock()
		second := rig.captures == 2
		rig.mu.Unlock()
		if second {
			cancel()
		}
		return nil
	}
	host := New(rig.config())
	aa17RequireReason(t, host.Start(ctx), ReasonCancelled)
	if _, _, _, news, _ := rig.counts(); news != 0 {
		t.Fatalf("news=%d", news)
	}
	if status := host.Status(); status.State == StateRunning || status.Resources != 0 {
		t.Fatalf("status=%+v", status)
	}
}

func TestAA17CancelDuringConstructionIsNotPublishedAsRunning(t *testing.T) {
	rig := aa17NewRig()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rig.onNew = func(*Session) { cancel() }
	host := New(rig.config())
	aa17RequireReason(t, host.Start(ctx), ReasonCancelled)
	if _, _, _, news, closes := rig.counts(); news != 1 || closes != 1 {
		t.Fatalf("news=%d closes=%d", news, closes)
	}
	status := host.Status()
	if status.State == StateRunning || status.Resources != 0 || status.OwnedWork != 0 {
		t.Fatalf("status=%+v", status)
	}
}

func TestAA17TokenReplayAcrossLifecyclesIsRefused(t *testing.T) {
	rig := aa17NewRig()
	sequence := []string{"A", "B", "A"}
	rig.tokenOverride = func() (string, error) {
		token := sequence[0]
		sequence = sequence[1:]
		return token, nil
	}
	host := New(rig.config())
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if err := host.Start(ctx); err != nil {
			t.Fatal(err)
		}
		if err := host.Stop(ctx); err != nil {
			t.Fatal(err)
		}
	}
	aa17RequireReason(t, host.Start(ctx), ReasonTokenReused)
	if _, _, _, news, _ := rig.counts(); news != 2 {
		t.Fatalf("news=%d", news)
	}
}

func TestAA17CancelledStartWithHungCloseQuarantinesWithinBudget(t *testing.T) {
	rig := aa17NewRig()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rig.onNew = func(*Session) { cancel() }
	rig.closeHook = func(c context.Context) error {
		<-c.Done()
		return c.Err()
	}
	cfg := rig.config()
	cfg.CleanupBudget = 20 * time.Millisecond
	host := New(cfg)
	err := host.Start(ctx)
	if !errors.Is(err, ErrQuiesce) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v", err)
	}
	if status := host.Status(); status.State != StateQuarantined || status.Resources != 1 {
		t.Fatalf("status=%+v", status)
	}
	if err := host.Start(context.Background()); !errors.Is(err, ErrQuarantined) {
		t.Fatalf("restart err=%v", err)
	}
}

func TestAA17PartialConstructionUnderCancelledStartGetsFreshCleanupBudget(t *testing.T) {
	rig := aa17NewRig()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rig.newErr = errors.New("synthetic partial construction")
	rig.newPartial = true
	rig.onNew = func(*Session) { cancel() }
	rig.closeHook = func(c context.Context) error { return c.Err() }
	host := New(rig.config())
	aa17RequireReason(t, host.Start(ctx), ReasonFactoryFailed)
	if _, _, _, news, closes := rig.counts(); news != 1 || closes != 1 {
		t.Fatalf("news=%d closes=%d, want 1 and 1", news, closes)
	}
	aa17RequireReleased(t, host)
}

func TestAA17PartialConstructionWithHungCloseQuarantinesWithinBudget(t *testing.T) {
	rig := aa17NewRig()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rig.newErr = errors.New("synthetic partial construction")
	rig.newPartial = true
	rig.onNew = func(*Session) { cancel() }
	rig.closeHook = func(c context.Context) error {
		<-c.Done()
		return c.Err()
	}
	cfg := rig.config()
	cfg.CleanupBudget = 20 * time.Millisecond
	host := New(cfg)
	err := host.Start(ctx)
	if !errors.Is(err, ErrQuiesce) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v", err)
	}
	if status := host.Status(); status.State != StateQuarantined || status.Resources != 1 {
		t.Fatalf("status=%+v", status)
	}
}

// Catches: a Stop that times out on the operation lock leaves a start in
// flight unfenced, so a binding launches after shutdown returned.
func TestAA17StopFencesAStartHoldingTheOperationLock(t *testing.T) {
	rig := aa17NewRig()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	rig.captureHook = func(context.Context) error {
		once.Do(func() { close(entered) })
		<-release
		return nil
	}
	host := New(rig.config())
	startDone := make(chan error, 1)
	go func() { startDone <- host.Start(context.Background()) }()
	<-entered
	expired, cancel := context.WithCancel(context.Background())
	cancel()
	if err := host.Stop(expired); !errors.Is(err, context.Canceled) {
		t.Fatalf("stop err=%v", err)
	}
	close(release)
	aa17RequireReason(t, <-startDone, ReasonCancelled)
	if _, _, _, news, _ := rig.counts(); news != 0 {
		t.Fatalf("binding launched after stop: news=%d", news)
	}
	aa17RequireReleased(t, host)
	// Positive control: a start entered after the stop is not fenced.
	rig.mu.Lock()
	rig.captureHook = nil
	rig.mu.Unlock()
	if err := host.Start(context.Background()); err != nil {
		t.Fatalf("later start: %v", err)
	}
	if err := host.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// Catches: a buffered error from an expired Close attempt is taken as the
// result of the follow-up Stop, which then quarantines without calling Close.
func TestAA17StopRetriesCloseAfterAnExpiredAttempt(t *testing.T) {
	rig := aa17NewRig()
	returned := make(chan struct{})
	var mu sync.Mutex
	calls := 0
	rig.closeHook = func(ctx context.Context) error {
		mu.Lock()
		calls++
		first := calls == 1
		mu.Unlock()
		if !first {
			return nil
		}
		<-ctx.Done()
		defer close(returned)
		return ctx.Err()
	}
	host := New(rig.config())
	if err := host.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	first, cancel := context.WithCancel(context.Background())
	stopDone := make(chan error, 1)
	go func() { stopDone <- host.Stop(first) }()
	aa17WaitState(host, StateStopping)
	cancel()
	if err := <-stopDone; !errors.Is(err, ErrQuiesce) {
		t.Fatalf("first stop err=%v", err)
	}
	<-returned
	if err := host.Stop(context.Background()); err != nil {
		t.Fatalf("follow-up stop: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls < 2 {
		t.Fatalf("Close not retried under the current budget: calls=%d", calls)
	}
	aa17RequireReleased(t, host)
}

// Catches: a Stop whose context is already expired loses the race for the
// operation lock without fencing the running generation, so callbacks stay
// admissible and a later Start reports success after shutdown failed.
func TestAA17ExpiredStopStillFencesTheRunningGeneration(t *testing.T) {
	rig := aa17NewRig()
	host := New(rig.config())
	if err := host.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	session := rig.session(0)
	stamp := session.Stamp()
	// Positive control: the callback is admitted while running.
	if err := session.Deliver(stamp, func(context.Context) error { return nil }); err != nil {
		t.Fatalf("control: %v", err)
	}
	expired, cancel := context.WithCancel(context.Background())
	cancel()
	// Hold the operation lock so the expired Stop cannot win it.
	if err := host.acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := host.Stop(expired); !errors.Is(err, context.Canceled) {
		t.Fatalf("stop err=%v", err)
	}
	host.release()
	if err := session.Deliver(stamp, func(context.Context) error { t.Error("admitted after stop"); return nil }); !errors.Is(err, ErrStaleCallback) {
		t.Fatalf("deliver err=%v", err)
	}
	aa17RequireReason(t, host.Start(context.Background()), ReasonCancelled)
	// A later Stop with a live budget completes the same teardown.
	if err := host.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	aa17RequireReleased(t, host)
}
